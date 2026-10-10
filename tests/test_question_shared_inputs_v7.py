"""Shared work reduces repeated reads, never fresh permission or delivery guards."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory.derived.model import DerivedError
from agent_memory.derived.question_inputs import QuestionInputWork, SharedQuestionInputs
from agent_memory.operations.worker_tasks import WorkerQueueError

store = project.store
QUESTIONS = ("owner", "status", "commitments", "risks")


async def batch_setup(engine, scope, clock, *, policies=False):
    svc = runtime(engine, scope, clock)
    for predicate, value in (("project.owner", "Alice"), ("project.status", "active")):
        item = await project.stage(svc.admission, scope, identity=predicate,
                                   predicate=predicate, value=value)
        await project.qualify(svc.admission, *item)
    from agent_memory.operations.refresh_policy import RefreshPolicy
    for i, question in enumerate(QUESTIONS):
        options = {"refresh_policy": RefreshPolicy(mode="on_demand", retry_seconds=31 + i)} if policies else {}
        await register(svc, question, **options)
    clock[0] += timedelta(microseconds=100)
    leases = []
    for question in QUESTIONS:
        receipt = await svc.request("project-a:" + question, actor=ACTOR, dedupe_key=question)
        lease = await svc.queue.claim("batch", lease_seconds=30, target_id=receipt["target_id"])
        assert lease
        leases.append(lease)
    svc.input_work = QuestionInputWork()
    return svc, leases


def test_shared_snapshot_and_publication_are_oracle_equal_and_reduce_work(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases = await batch_setup(engine, scope, clock)
            tasks = [lease.task for lease in leases]
            snapshots = await svc.snapshot_many(tasks)
            assert svc.input_work.qualified_census_builds == 1
            assert svc.input_work.qualified_census_reuses == 3
            assert svc.input_work.candidate_census_reads == 1
            assert svc.input_work.candidate_census_reuses == 11
            assert len({s["census"].snapshot.id for s in snapshots}) == 4
            # Compare complete per-job snapshots with the independently rebuilt
            # path; only operational invalidation counters are out of contract.
            serial = [await svc.snapshot(task) for task in tasks]
            assert snapshots == serial
            prepared = [svc.prepare(s) for s in snapshots]
            for snapshot, output in zip(snapshots, prepared):
                oracle = project.full_project_question(svc.admission.contract,
                    snapshot["census"].snapshot, snapshot["question"])
                assert output["result_metadata"]["snapshot_id"] == oracle.snapshot_id
            svc.input_work = QuestionInputWork()
            await svc.publish_many(tasks, snapshots, prepared)
            assert svc.input_work.qualified_census_builds == 1
            assert svc.input_work.qualified_census_reuses == 3
            for lease in leases:
                await svc.queue.complete(lease)
            svc.input_work = QuestionInputWork()
            answers = await svc.read_many(["project-a:" + q for q in QUESTIONS], actor=ACTOR)
            assert svc.input_work.candidate_census_reads == 1
            assert svc.input_work.source_proof_reads > 0
            assert svc.input_work.authorization_checks > 4
            assert all(a["model_calls"] == 0 for a in answers)
            assert [a["answer_status"] for a in answers] == ["resolved", "resolved", "empty", "empty"]
            # Returned payload mutation cannot change owned inputs or siblings.
            snapshots[0]["census"].grants[0]["revoked"] = True
            assert not snapshots[1]["census"].grants[0].get("revoked")

    asyncio.run(run())


def test_incompatible_policy_does_not_share_qualified_census(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases = await batch_setup(engine, scope, clock, policies=True)
            await svc.snapshot_many([lease.task for lease in leases])
            assert svc.input_work.qualified_census_builds == 4
            assert svc.input_work.qualified_census_reuses == 0

    asyncio.run(run())


@pytest.mark.parametrize("change", ["context", "host", "grant", "frontier", "epoch"])
def test_shared_inputs_recheck_controls_and_frontier(store, monkeypatch, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases = await batch_setup(engine, scope, clock)
            original = svc._snapshot_in_uow
            calls = []
            async def capture(uow, task, **kwargs):
                value = await original(uow, task, **kwargs)
                calls.append(True)
                if len(calls) == 1:
                    if change == "context":
                        svc.context = replace(svc.context, revision="changed")
                    elif change == "host":
                        svc.admission.memberships["a"] = replace(svc.admission.memberships["a"],
                                                                 registry_revision="changed")
                    elif change == "grant":
                        grant = await uow.derived_get(scope, "grant", "project.owner")
                        grant["revoked"] = True
                        await uow.derived_put(scope, "grant", "project.owner", grant)
                    elif change == "frontier":
                        key = next(iter(value["proof"]["query_generations"]))
                        old = await uow.derived_get(scope, "barrier", key) or {"generation": 0}
                        await uow.derived_put(scope, "barrier", key, {"generation": old["generation"] + 1})
                    else:
                        original_epoch = uow.retention_epoch
                        async def changed_epoch(scope):
                            return await original_epoch(scope) + 1
                        uow.retention_epoch = changed_epoch
                return value
            monkeypatch.setattr(svc, "_snapshot_in_uow", capture)
            with pytest.raises((DerivedError, WorkerQueueError)):
                await svc.snapshot_many([lease.task for lease in leases])
            assert svc.input_work.qualified_census_reuses == 0

    asyncio.run(run())


def test_batch_output_tamper_rolls_back_every_publication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases = await batch_setup(engine, scope, clock)
            tasks = [lease.task for lease in leases]
            snapshots = await svc.snapshot_many(tasks)
            prepared = [svc.prepare(s) for s in snapshots]
            prepared[-1]["trace"]["groups_evaluated"] += 1
            with pytest.raises(DerivedError, match="output_invalid"):
                await svc.publish_many(tasks, snapshots, prepared)
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "question_head")
                assert not await uow.derived_records(scope, "question_content")

    asyncio.run(run())


def test_shared_owner_cannot_cross_transactions(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            async with engine.repository.unit_of_work() as uow:
                shared = SharedQuestionInputs(uow, QuestionInputWork())
            async with engine.repository.unit_of_work() as uow:
                with pytest.raises(DerivedError, match="transaction_mismatch"):
                    shared.check(uow)
    asyncio.run(run())


def test_advancing_clock_preserves_real_header_and_qualified_reuse(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases = await batch_setup(engine, scope, clock)
            def advancing():
                clock[0] += timedelta(microseconds=1)
                return clock[0]
            svc.clock = svc.admission.clock = svc.queue.clock = advancing
            tasks = [lease.task for lease in leases]
            snapshots = await svc.snapshot_many(tasks)
            assert svc.input_work.candidate_census_reads == 1
            assert svc.input_work.qualified_census_builds == 1
            svc.input_work = QuestionInputWork()
            await svc.publish_many(tasks, snapshots, [svc.prepare(s) for s in snapshots])
            assert svc.input_work.candidate_census_reads == 1
            assert svc.input_work.qualified_census_builds == 1
            assert svc.input_work.qualified_census_reuses == 3
            for lease in leases:
                await svc.queue.complete(lease)
            svc.input_work = QuestionInputWork()
            await svc.read_many(["project-a:" + q for q in QUESTIONS], actor=ACTOR)
            assert svc.input_work.candidate_census_reads == 1
            assert svc.input_work.candidate_census_reuses >= 4
            assert svc.input_work.authorization_checks >= 8
    asyncio.run(run())


@pytest.mark.parametrize("change", ["grant_expiry", "source_erasure"])
def test_shared_census_cannot_bypass_fresh_source_and_time_checks(store, monkeypatch, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases = await batch_setup(engine, scope, clock)
            original = svc._snapshot_in_uow
            calls = []
            async def capture(uow, task, **kwargs):
                value = await original(uow, task, **kwargs)
                calls.append(True)
                if len(calls) == 1:
                    if change == "grant_expiry":
                        row = await uow.derived_get(scope, "grant", "project.owner")
                        row["expires_at"] = clock[0].isoformat()
                        await uow.derived_put(scope, "grant", "project.owner", row)
                    else:
                        actual = uow.derived_project_source_proof
                        async def missing(scope, key):
                            return None if key == "project.owner" else await actual(scope, key)
                        uow.derived_project_source_proof = missing
                return value
            monkeypatch.setattr(svc, "_snapshot_in_uow", capture)
            with pytest.raises(DerivedError, match="expired|source_unavailable"):
                await svc.snapshot_many([lease.task for lease in leases])
            assert svc.input_work.qualified_census_reuses == 0
    asyncio.run(run())


@pytest.mark.parametrize("change", ["grant", "head", "expiry", "host"])
def test_final_batch_guard_rechecks_earlier_answers(store, monkeypatch, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases = await batch_setup(engine, scope, clock)
            tasks = [lease.task for lease in leases]
            snapshots = await svc.snapshot_many(tasks)
            await svc.publish_many(tasks, snapshots, [svc.prepare(s) for s in snapshots])
            for lease in leases:
                await svc.queue.complete(lease)
            original = svc._read_in_uow
            calls = []
            async def read(uow, question_id, **kwargs):
                result = await original(uow, question_id, **kwargs)
                calls.append(True)
                if len(calls) == 4:
                    if change == "grant":
                        row = await uow.derived_get(scope, "grant", "project.owner")
                        row["revoked"] = True
                        await uow.derived_put(scope, "grant", "project.owner", row)
                    elif change == "head":
                        key = snapshots[0]["definition"]["facet_id"]
                        row = await uow.derived_get(scope, "question_head", key)
                        row["content_sha256"] = "0" * 64
                        await uow.derived_put(scope, "question_head", key, row)
                    elif change == "expiry":
                        clock[0] = svc.context.expires_at
                    else:
                        svc.admission.memberships["a"] = replace(svc.admission.memberships["a"],
                                                                 registry_revision="late-change")
                return result
            monkeypatch.setattr(svc, "_read_in_uow", read)
            with pytest.raises(DerivedError):
                await svc.read_many(["project-a:" + q for q in QUESTIONS], actor=ACTOR)
    asyncio.run(run())


def test_full_qualified_compatibility_binds_every_coordinate(store):
    from copy import deepcopy
    from types import SimpleNamespace
    from agent_memory.derived.question_inputs import compatibility
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases = await batch_setup(engine, scope, clock)
            snapshots = await svc.snapshot_many([lease.task for lease in leases])
            definition = snapshots[0]["definition"]
            expected = compatibility(svc, definition, clock[0])
            assert compatibility(svc, snapshots[1]["definition"], clock[0]) == expected
            paths = [("project_id",), ("contract_fingerprint",), ("registration_fingerprint",),
                     ("context", "revision"), ("purpose",), ("readers",),
                     ("publication_request_ids",), ("instance", "time", "mode"),
                     ("instance", "definition", "source_basis"),
                     ("instance", "definition", "refresh_policy", "id")]
            for path in paths:
                changed = deepcopy(definition)
                row = changed["spec"]
                for name in path[:-1]:
                    row = row[name]
                row[path[-1]] = ["other"] if isinstance(row[path[-1]], list) else "other"
                assert compatibility(svc, changed, clock[0]) != expected, path
            assert compatibility(svc, definition, clock[0] + timedelta(microseconds=1)) != expected
            for field, value in (("principal", "other"), ("authority_id", "other"),
                                 ("authority_min_version", 99), ("policy", {"changed": True}),
                                 ("projection_policy", {"changed": True})):
                admission = SimpleNamespace(**vars(svc.admission))
                setattr(admission, field, value)
                changed = SimpleNamespace(**vars(svc))
                changed.admission = admission
                assert compatibility(changed, definition, clock[0]) != expected, field
            changed = SimpleNamespace(**vars(svc))
            changed.scope = replace(scope, tenant_id="other")
            assert compatibility(changed, definition, clock[0]) != expected
    asyncio.run(run())


def test_publication_basis_and_project_keep_separate_censuses(store):
    from agent_memory.derived.question_model import SourceBasis
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases = await batch_setup(engine, scope, clock)
            await register(svc, question_id="owner-publication",
                source_basis=SourceBasis.PUBLICATION_MANIFEST,
                publication_request_ids=("request:project.owner", "request:project.status"))
            await svc.register("other-project", "project-b", "owner", readers=(ACTOR,))
            for name in ("owner-publication", "other-project"):
                receipt = await svc.request(name, actor=ACTOR, dedupe_key=name)
                leases.append(await svc.queue.claim("batch", lease_seconds=30,
                                                    target_id=receipt["target_id"]))
            assert all(leases)
            svc.input_work = QuestionInputWork()
            snapshots = await svc.snapshot_many([lease.task for lease in leases])
            assert svc.input_work.qualified_census_builds == 3
            assert svc.input_work.qualified_census_reuses == 3
            assert snapshots[-2]["census"].snapshot.coverage.source_basis == "publication_manifest"
            assert snapshots[-1]["census"].snapshot.project_id == "project-b"
    asyncio.run(run())
