"""T14 bounded retention against SQLite and live PostgreSQL, without model calls."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_purge_restore import backup_copy, erase, replay, restorer
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.derived.question_gc import COLLECTIBLE_KINDS, QuestionRetentionPolicy
from agent_memory.operations.refresh_policy import RefreshPolicy

store = project.store
POLICY = QuestionRetentionPolicy("host:retain-current-unreferenced/1")


async def background(svc, clock):
    clock[0] += timedelta(seconds=31)
    lease = await svc.queue.claim("background", lease_seconds=30)
    assert lease is not None
    await svc.queue.apply(lease.task)
    await svc.queue.complete(lease)
    return await svc.read("project-a:owner", actor=ACTOR)


async def seed(engine, scope, clock):
    svc = runtime(engine, scope, clock)
    item = await project.stage(svc.admission, scope)
    await project.qualify(svc.admission, *item)
    await register(svc, refresh_policy=RefreshPolicy(mode="on_change", max_age_seconds=60))
    return svc, item, await background(svc, clock)


async def revalidate(svc, clock, version):
    await svc.grant(
        ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=version
    )
    return await background(svc, clock)


async def census(svc):
    async with svc.repository.unit_of_work() as uow:
        return {kind: await uow.derived_records(svc.scope, kind) for kind in COLLECTIBLE_KINDS}


def test_background_cycles_reclaim_complete_groups_and_keep_original_generation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, first = await seed(engine, scope, clock)
            manifests, deleted = [], []
            for version in range(1, 13):
                current = await revalidate(svc, clock, version)
                manifests.append(current["generation_manifest"])
                result = await svc.collect_garbage(POLICY)
                deleted.extend(result["deleted"])
                assert result["receipt_retention"] == "indefinite"
                assert await svc.read("project-a:owner", actor=ACTOR) == current
                rows = await census(svc)
                assert len(rows["question_content"]) == 1
                assert len(rows["question_certificate"]) == 1
                assert len(rows["job"]) == len(rows["refresh_execution"]) == 1
                assert len(rows["refresh_publication"]) == 1
            assert len(deleted) == 12 * 4
            assert all(m == first["generation_manifest"] for m in manifests)
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "coverage_request")
                assert not await uow.derived_records(scope, "request")
                header = await uow.derived_get(scope, "question_head", first["instance_id"])
                assert header["generation_proof"]["sources"][0]["grant_version"] == 1
                assert header["proof"]["sources"][0]["grant_version"] == 13

    asyncio.run(run())


def test_explicit_receipt_stays_complete_after_refresh_and_gc(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            await register(svc)
            receipt = await svc.request("project-a:owner", actor=ACTOR, dedupe_key="finite")
            first = await background(svc, clock)
            await svc.queue.configure(
                first["instance_id"], RefreshPolicy(mode="on_change", max_age_seconds=60)
            )
            second = await background(svc, clock)
            clock[0] += timedelta(seconds=31)
            before = await svc.queue.status(receipt["target_id"], actor=ACTOR)
            result = await svc.collect_garbage(POLICY)
            assert before["complete"]
            assert await svc.queue.status(receipt["target_id"], actor=ACTOR) == before
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(
                    scope, "question_certificate", first["certificate_revision_id"]
                )
            assert result["retained"]["question_certificate"] == 2
            assert not result["deleted"]
            assert await svc.read("project-a:owner", actor=ACTOR) == second

    asyncio.run(run())


@pytest.mark.parametrize("hold", ["object", "instance"])
def test_explicit_retention_holds_transitive_proof_and_small_batch_never_dangles(store, hold):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, first = await seed(engine, scope, clock)
            await revalidate(svc, clock, 1)
            policy = replace(
                POLICY,
                **{
                    "retain_ids" if hold == "object" else "retain_instances": (
                        first["certificate_revision_id"]
                        if hold == "object"
                        else first["instance_id"],
                    )
                },
            )
            held = await svc.collect_garbage(policy)
            # A hold on the cert preserves the cert, but does not claim that an
            # unreferenced background execution is itself a permanent receipt.
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(
                    scope, "question_certificate", first["certificate_revision_id"]
                )
            assert held["roots"]["host_retention"]
            await revalidate(svc, clock, 2)
            before = await census(svc)
            limited = await svc.collect_garbage(replace(policy, max_delete=1))
            # Any deleted singleton has no retained incoming references. Full
            # background proof cycles require a larger atomic component budget.
            assert limited["reason"] == (
                "question_gc_batch_limit" if hold == "object" else "question_gc_references_retained"
            )
            after = await census(svc)
            for kind, rows in before.items():
                for row in rows:
                    if (kind, row["identity"]) not in limited["deleted"]:
                        assert row in after[kind]
            released = await svc.collect_garbage(POLICY)
            assert released["deleted"]
            assert len((await census(svc))["question_certificate"]) == 1

    asyncio.run(run())


@pytest.mark.parametrize("limit", ["max_records", "max_edges", "max_bytes"])
def test_incomplete_census_never_deletes_and_returns_stable_reason(store, limit):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, _ = await seed(engine, scope, clock)
            await revalidate(svc, clock, 1)
            before = await census(svc)
            policy = replace(POLICY, **{limit: 1})
            result = await svc.collect_garbage(policy)
            assert result["state"] == "deferred"
            assert result["reason"] == "question_gc_census_limit"
            assert result["deleted"] == [] and await census(svc) == before
            assert await svc.collect_garbage(policy) == result

    asyncio.run(run())


@pytest.mark.parametrize("damage", ["kind", "candidate"])
def test_unknown_kind_or_invalid_candidate_defers_without_partial_deletion(store, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, _ = await seed(engine, scope, clock)
            await revalidate(svc, clock, 1)
            async with engine.repository.unit_of_work() as uow:
                await uow.derived_put(
                    scope,
                    "future_reference" if damage == "kind" else "question_certificate",
                    "unsupported",
                    {"schema": "future/9"},
                )
            before = await census(svc)
            result = await svc.collect_garbage(POLICY)
            assert (
                result["reason"]
                == "question_gc_" + ("kind" if damage == "kind" else "record") + "_unsupported"
            )
            assert not result["deleted"] and await census(svc) == before

    asyncio.run(run())


def test_scope_isolation_and_erase_journal_replay_after_gc(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, item, first = await seed(engine, scope, clock)
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                second = await revalidate(svc, clock, 1)
                assert (await svc.collect_garbage(POLICY))["deleted"]
                other = replace(scope, session_id="untouched")
                async with engine.repository.unit_of_work() as uow:
                    await uow.derived_put(
                        other, "question_certificate", "unknown", {"secret": "other"}
                    )
                await erase(kernel, scope, item[0].id)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                from agent_memory.consolidation.admission_runtime import AdmissionEngine

                cloned = runtime(AdmissionEngine(backup), scope, clock)
                for current in (svc, cloned):
                    tombstones = await census(current)
                    result = await current.collect_garbage(POLICY)
                    assert not result["deleted"]
                    assert await census(current) == tombstones
                    assert "private-project-marker" not in json.dumps(tombstones)
                    assert (await restorer(current.repository, scope, clock).export())[
                        "checkpoint"
                    ] == deletion["checkpoint"]
                    with pytest.raises(DerivedError):
                        await current.read("project-a:owner", actor=ACTOR)
                async with engine.repository.unit_of_work() as uow:
                    assert (await uow.derived_get(other, "question_certificate", "unknown"))[
                        "secret"
                    ] == "other"
                assert first["generation_manifest"] == second["generation_manifest"]

    asyncio.run(run())


def test_claimed_successor_and_original_baseline_are_pinned_until_publish(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, first = await seed(engine, scope, clock)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            clock[0] += timedelta(seconds=31)
            lease = await svc.queue.claim("in-flight", lease_seconds=30)
            snapshot = await svc.snapshot(lease.task)
            before = await census(svc)
            result = await svc.collect_garbage(POLICY)
            assert not result["deleted"]
            assert await census(svc) == before
            await svc.publish(lease.task, snapshot, svc.prepare(snapshot))
            await svc.queue.complete(lease)
            assert (await svc.collect_garbage(POLICY))["deleted"]
            answer = await svc.read("project-a:owner", actor=ACTOR)
            assert answer["generation_manifest"] == first["generation_manifest"]

    asyncio.run(run())


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_delete", True),
        ("max_records", 0),
        ("max_edges", 262145),
        ("max_bytes", 268435457),
        ("retain_ids", ["mutable"]),
    ],
)
def test_invalid_host_policy_is_rejected(field, value):
    with pytest.raises((DerivedError, ValueError, TypeError)):
        QuestionRetentionPolicy("host", **{field: value})


@pytest.mark.parametrize("kind", ["question_content", "question_certificate"])
def test_real_production_capacity_failure_recovers_without_discarding_receipts(store, kind):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, first = await seed(engine, scope, clock)
            from agent_memory.derived.question_model import QuestionCertificate, QuestionContent

            async with engine.repository.unit_of_work() as uow:
                row = (await uow.derived_records(scope, kind))[0]["payload"]
                original = (
                    QuestionContent if kind == "question_content" else QuestionCertificate
                ).from_payload(row)
                # Valid obsolete immutable revisions at the actual 4096-row
                # production boundary; no receipt or proof pointer is removed.
                for n in range(4095):
                    historic = (
                        replace(original, structure={"historical_revision": n})
                        if kind == "question_content"
                        else replace(
                            original, validated_at=clock[0] + timedelta(microseconds=n + 1)
                        )
                    )
                    await uow.derived_put(scope, kind, historic.id, historic.payload())
            changed = await project.stage(svc.admission, scope, identity="new-owner", value="Bob")
            await project.qualify(svc.admission, *changed)
            clock[0] += timedelta(seconds=31)
            lease = await svc.queue.claim("capacity-recovery", lease_seconds=30)
            with pytest.raises(DerivedError, match=kind + "_capacity"):
                await svc.queue.apply(lease.task)
            result = await svc.collect_garbage(replace(POLICY, max_delete=4096))
            assert len(result["deleted"]) == 4095
            assert all(node[0] == kind for node in result["deleted"])
            await svc.queue.apply(lease.task)
            await svc.queue.complete(lease)
            answer = await svc.read("project-a:owner", actor=ACTOR)
            assert answer["answer_status"] == "contested"
            assert answer["content_revision_id"] != first["content_revision_id"]
            assert len((await census(svc))[kind]) == 2

    asyncio.run(run())


def test_collector_owns_host_policy_before_first_await(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, first = await seed(engine, scope, clock)
            await revalidate(svc, clock, 1)
            policy = replace(POLICY, retain_ids=(first["certificate_revision_id"],))
            original = svc._open

            async def mutate(uow):
                object.__setattr__(policy, "retain_ids", ())
                return await original(uow)

            svc._open = mutate
            await svc.collect_garbage(policy)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(
                    scope, "question_certificate", first["certificate_revision_id"]
                )

    asyncio.run(run())


def test_independent_full_generation_reclaims_obsolete_content_without_rewriting_lineage(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            public = await project.stage(svc.admission, scope, identity="public")
            await project.qualify(svc.admission, *public)
            private = await project.stage(
                svc.admission,
                scope,
                identity="source",
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="private",
            )
            await project.qualify(svc.admission, *private)
            await register(svc, refresh_policy=RefreshPolicy(mode="on_change", max_age_seconds=60))
            first = await background(svc, clock)
            await svc.admission.replace_membership(
                private[2], expected_version=4, membership_id="promise-b"
            )
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",), revoked=True),
                expected_version=1,
            )
            second = await background(svc, clock)
            assert second["compute_mode"] == "full"
            assert first["content_revision_id"] != second["content_revision_id"]
            result = await svc.collect_garbage(POLICY)
            assert ("question_content", first["content_revision_id"]) in result["deleted"]
            assert ("question_certificate", first["certificate_revision_id"]) in result["deleted"]
            assert len(result["deleted"]) == 5
            assert await svc.read("project-a:owner", actor=ACTOR) == second
            assert {
                ref["id"]
                for ref in second["generation_manifest"]["inputs"]
                if ref["kind"] == "source"
            } == {"public"}

    asyncio.run(run())


def test_completion_acknowledgement_remains_supported_after_lease_until(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            await register(svc, refresh_policy=RefreshPolicy(mode="on_change", max_age_seconds=120))
            lease = await svc.queue.claim("late-ack", lease_seconds=5)
            await svc.queue.apply(lease.task)
            await svc.queue.configure(
                lease.task.payload["unit"]["facet_id"],
                RefreshPolicy(mode="on_change", max_age_seconds=120),
            )
            await background(svc, clock)
            result = await svc.collect_garbage(POLICY)
            assert not result["deleted"]
            assert result["roots"]["lease_fence"]
            await svc.queue.complete(lease)

    asyncio.run(run())


def test_gc_is_host_only_requires_backend_contract_and_preserves_clock_barrier(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, _ = await seed(engine, scope, clock)
            await revalidate(svc, clock, 1)
            from agent_memory.mcp import MCPRequestContext

            with pytest.raises(DerivedError, match="invalid_question_request"):
                await svc.call(
                    "collect_garbage", {"policy_id": "model"}, MCPRequestContext(scope, actor=ACTOR)
                )
            cls = type(engine.repository.unit_of_work())
            with monkeypatch.context() as patch:
                patch.setattr(cls, "question_gc_contract", "future/9")
                with pytest.raises(DerivedError, match="question_gc_backend_unsupported"):
                    await svc.collect_garbage(POLICY)
            before = await census(svc)
            clock[0] -= timedelta(seconds=1)
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                await svc.collect_garbage(POLICY)
            assert await census(svc) == before

    asyncio.run(run())


def test_cross_connection_collector_sees_reference_committed_while_waiting_for_lock(store):
    import threading

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, first = await seed(engine, scope, clock)
            await revalidate(svc, clock, 1)
            repo = engine.repository
            ready, start, entered, finished = (threading.Event() for _ in range(4))
            outcomes, errors = [], []

            def other_connection():
                async def collect():
                    from agent_memory.consolidation.admission_runtime import AdmissionEngine

                    other = (
                        type(repo).from_dsn(repo.pool.conninfo, max_size=2)
                        if hasattr(repo, "pool")
                        else type(repo)(repo._path)
                    )
                    try:
                        await other.initialize()
                        service = runtime(AdmissionEngine(other), scope, clock)
                        ready.set()
                        await asyncio.to_thread(start.wait, 10)
                        entered.set()
                        outcomes.append(await service.collect_garbage(POLICY))
                    finally:
                        if hasattr(other, "close"):
                            await other.close()

                try:
                    asyncio.run(collect())
                except BaseException as error:
                    errors.append(error)
                finally:
                    ready.set()
                    finished.set()

            worker = threading.Thread(target=other_connection)
            worker.start()
            try:
                assert await asyncio.to_thread(ready.wait, 10)
                assert not errors
                async with repo.unit_of_work() as uow:
                    await uow.lock_admission_scope(scope)
                    start.set()
                    assert await asyncio.to_thread(entered.wait, 10)
                    await asyncio.sleep(0.05)
                    assert not finished.is_set()
                    await uow.derived_put(
                        scope,
                        "model_flight",
                        "racing-reference",
                        {
                            "state": "running",
                            "parents": ["derived:" + first["certificate_revision_id"]],
                        },
                    )
                assert await asyncio.to_thread(finished.wait, 10)
                assert not errors
                assert ("question_certificate", first["certificate_revision_id"]) not in outcomes[
                    0
                ]["deleted"]
                async with repo.unit_of_work() as uow:
                    assert await uow.derived_get(
                        scope, "question_certificate", first["certificate_revision_id"]
                    )
            finally:
                start.set()
                await asyncio.to_thread(worker.join, 10)
                assert not worker.is_alive()

    asyncio.run(run())


@pytest.mark.parametrize("reason", ["kind", "record", "census_limit"])
def test_early_deferred_plan_reports_full_tombstone_capacity(reason):
    from datetime import UTC, datetime

    from agent_memory.derived.question_gc import plan
    from agent_memory.domain import MemoryScope

    rows = [
        dict(kind="question_content", identity=f"erased-{n}", payload={"state": "erased"})
        for n in range(4096)
    ]
    policy = POLICY
    if reason == "kind":
        rows.append(dict(kind="future", identity="future", payload={}))
    elif reason == "record":
        rows.append(dict(kind="question_head", identity="future", payload={"schema": "future/9"}))
    else:
        policy = replace(policy, max_edges=1)
    result = plan(
        MemoryScope("gc-report"),
        policy,
        dict(rows=rows, edges=[], reservations=[]),
        datetime(2026, 10, 1, tzinfo=UTC),
    )
    assert result["state"] == "deferred"
    assert result["reason"] == "question_gc_" + reason + (
        "_unsupported" if reason != "census_limit" else ""
    )
    assert result["remaining"]["question_content"] == 4096
    assert result["at_capacity"] == ["question_content"]
    assert not result["deleted"]
