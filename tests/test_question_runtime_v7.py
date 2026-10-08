"""Real dual-provider project QuestionView lifecycle, no model or mock proof authority."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_purge_restore import backup_copy, erase, replay, restorer

from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.derived.question_model import QuestionContext, SourceBasis
from agent_memory.derived.question_service import QuestionService
from agent_memory.retrieval.question_router import QuestionRouter

store = project.store
ACTOR = "host:alice"


def runtime(engine, scope, clock, **options):
    admission = project.service(engine, scope, clock)
    context = QuestionContext("host", "context/1", {}, clock[0] + timedelta(days=10))
    return QuestionService(admission, context, **options)


async def register(svc, question="owner", *, question_id=None, **options):
    return await svc.register(
        question_id or "project-a:" + question,
        "project-a",
        question,
        readers=(ACTOR,),
        **options,
    )


async def fresh(svc, clock, question="owner", *, question_id=None, dedupe="read"):
    clock[0] += timedelta(microseconds=100)
    return await svc.answer(question_id or "project-a:" + question, actor=ACTOR, dedupe_key=dedupe)


async def lease_snapshot(svc, clock, question="owner", *, dedupe="request"):
    clock[0] += timedelta(microseconds=100)
    receipt = await svc.request("project-a:" + question, actor=ACTOR, dedupe_key=dedupe)
    lease = await svc.queue.claim("test-worker", lease_seconds=30)
    assert lease
    snapshot = await svc.snapshot(lease.task)
    return receipt, lease, snapshot, svc.prepare(snapshot)


def test_actual_registration_review_publish_guarded_read_and_singleflight(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            registered = await register(svc, aliases=("Who owns project A?",))
            result = await fresh(svc, clock)
            assert result["answer_status"] == "resolved"
            assert result["result"]["rows"][0]["fields"][0]["known_values"] == ["Alice"]
            assert result["availability_status"] == "valid" and result["model_calls"] == 0
            assert result["citations"] and result["processing_references"]
            assert result["instance_id"] == registered["instance_id"]
            again = await svc.answer("project-a:owner", actor=ACTOR, dedupe_key="second")
            assert again["content_revision_id"] == result["content_revision_id"]
            assert await svc.queue.claim("unnecessary-background", lease_seconds=30) is None
            async with engine.repository.unit_of_work() as uow:
                jobs = await uow.derived_records(scope, "job")
                contents = await uow.derived_records(scope, "question_content")
                assert len(jobs) == len(contents) == 1
                assert jobs[0]["payload"]["status"] == "completed"
            router = QuestionRouter(svc)
            assert (await router.route("  WHO owns PROJECT a? ", actor=ACTOR))[
                "route"
            ] == "question"
            assert (await router.route("Who owns another project?", actor=ACTOR))[
                "route"
            ] == "abstain"
            conflict = await router.route(
                "project-a:owner", actor=ACTOR, parameters={"project_id": "project-b"}
            )
            assert conflict["reason"] == "question_parameter_conflict"
            await register(svc, "status", aliases=("Who owns project A?",))
            assert (await router.route("Who owns project A?", actor=ACTOR))[
                "reason"
            ] == "ambiguous_question"
            await register(svc, "risks", aliases=("project-a:owner",))
            # An explicit registered ID cannot be shadowed by another alias.
            assert (await router.route("project-a:owner", actor=ACTOR))["question_id"] == (
                "project-a:owner"
            )

    asyncio.run(run())


def test_all_four_templates_explicit_unknown_contested_and_incomplete(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            for question in ("owner", "status", "commitments", "risks"):
                await register(svc, question)
                result = await fresh(svc, clock, question, dedupe=question)
                expected = "empty" if question in {"commitments", "risks"} else "unknown"
                assert result["answer_status"] == expected
            first = await project.stage(svc.admission, scope, identity="alice")
            second = await project.stage(svc.admission, scope, identity="bob", value="Bob")
            await project.qualify(svc.admission, *first)
            pending = await fresh(svc, clock, dedupe="pending")
            assert pending["answer_status"] == "incomplete"
            inputs = pending["generation_manifest"]["inputs"]
            assert {i["id"] for i in inputs if i["kind"] == "source"} == {"alice", "bob"}
            await project.qualify(svc.admission, *second)
            contested = await fresh(svc, clock, dedupe="contested")
            assert contested["answer_status"] == "contested"
            assert len(contested["result"]["rows"][0]["fields"][0]["candidates"]) == 2

    asyncio.run(run())


@pytest.mark.parametrize("change", ["candidate", "grant", "context", "membership", "authority"])
def test_compute_publish_cas_rejects_every_changed_authority_input(store, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            _, lease, snapshot, prepared = await lease_snapshot(svc, clock)
            if change == "candidate":
                await project.stage(svc.admission, scope, identity="late", value="Bob")
            elif change == "grant":
                await svc.grant(
                    ProcessingGrant(item[0].id, (ACTOR,), ("project_questions",), revoked=True),
                    expected_version=1,
                )
            elif change == "context":
                svc.context = replace(svc.context, revision="context/2")
            elif change == "membership":
                svc.admission.memberships["a"] = replace(
                    project.MEMBERSHIPS[0], registry_revision="registry/2"
                )
            else:
                svc.admission.authorities[project.AUTHORITY.source_id] = replace(
                    project.AUTHORITY, kind="document"
                )
            with pytest.raises(DerivedError):
                await svc.publish(lease.task, snapshot, prepared)
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "question_content")
                assert not await uow.derived_records(scope, "question_head")
                assert not await uow.derived_records(scope, "refresh_publication")

    asyncio.run(run())


def test_forged_snapshot_output_cannot_reuse_real_input_metadata(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            _, lease, snapshot, _ = await lease_snapshot(svc, clock)
            census = snapshot["census"]
            fact = census.snapshot.facts[0]
            forged_fact = replace(fact.fact, value="Mallory")
            forged_qualification = replace(
                fact.qualification, target_sha256=forged_fact.fingerprint
            )
            forged = replace(fact, fact=forged_fact, qualification=forged_qualification)
            snapshot["census"] = replace(census, snapshot=replace(census.snapshot, facts=(forged,)))
            with pytest.raises(DerivedError, match="input_changed"):
                await svc.publish(lease.task, snapshot, svc.prepare(snapshot))

    asyncio.run(run())


def test_pending_and_rejected_processing_sources_remain_guarded_and_denied_prebody(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            good = await project.stage(svc.admission, scope)
            bad = await project.stage(
                svc.admission, scope, identity="rejected", value="private-reject"
            )
            await project.qualify(svc.admission, *good)
            await svc.admission.reject(
                bad[2], expected_version=2, review_id="reject", reasons=("unsupported",)
            )
            await register(svc)
            result = await fresh(svc, clock)
            assert result["answer_status"] == "resolved"
            assert {r["source_event_id"] for r in result["processing_references"]} == {
                "source",
                "rejected",
            }
            await svc.grant(
                ProcessingGrant("rejected", (ACTOR,), ("project_questions",), revoked=True),
                expected_version=1,
            )
            original_uow = engine.repository.unit_of_work

            class GuardUow:
                def __init__(self):
                    self.inner = original_uow()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_get(self, scope, kind, identity):
                    assert kind != "question_content", (
                        "Denied original processing input loaded answer body"
                    )
                    return await self.uow.derived_get(scope, kind, identity)

                async def get_source_event(self, *args):
                    raise AssertionError("Denied read loaded source body")

                async def get_admission_record(self, *args):
                    raise AssertionError("Denied read loaded candidate body")

            engine.repository.unit_of_work = GuardUow
            try:
                with pytest.raises(DerivedError, match="processing_denied"):
                    await svc.read("project-a:owner", actor=ACTOR)
            finally:
                engine.repository.unit_of_work = original_uow

    asyncio.run(run())


def test_finite_manifest_target_is_real_closed_and_lease_fenced(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(
                svc,
                source_basis=SourceBasis.PUBLICATION_MANIFEST,
                publication_request_ids=("request:source",),
            )
            receipt, lease, snapshot, prepared = await lease_snapshot(svc, clock)
            forged = replace(lease.task, payload={**lease.task.payload, "fence": "wrong"})
            with pytest.raises(Exception, match="stale"):
                await svc.publish(forged, snapshot, prepared)
            assert not (await svc.queue.status(receipt["target_id"], actor=ACTOR))["complete"]
            await svc.publish(lease.task, snapshot, prepared)
            assert (await svc.queue.status(receipt["target_id"], actor=ACTOR))["complete"]
            result = await svc.read("project-a:owner", actor=ACTOR)
            assert result["coverage"]["publication_closed"] is True
            assert result["coverage"]["publication_manifest_digest"]
            await project.stage(svc.admission, scope, identity="outside-manifest")
            with pytest.raises(DerivedError, match="publication_target_incomplete"):
                await svc.read("project-a:owner", actor=ACTOR)

    asyncio.run(run())


def test_full_response_budget_and_expiry_never_truncate(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc, max_output_bytes=1200)
            with pytest.raises(DerivedError, match="output_capacity"):
                await fresh(svc, clock)
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "question_content")
            await register(svc, question_id="large", max_output_bytes=262144)
            await fresh(svc, clock, question_id="large", dedupe="large")
            clock[0] = svc.context.expires_at
            with pytest.raises(DerivedError, match="context_expired|time_coverage_expired"):
                await svc.read("large", actor=ACTOR)

    asyncio.run(run())


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_question_erasure_and_real_old_backup_replay_scrub_complete_generation(
    store, tmp_path, all_in_scope
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope, value="private-question-marker")
            await project.qualify(svc.admission, *item)
            svc = QuestionService(
                svc.admission,
                replace(svc.context, attributes={"sensitive": "private-context-marker"}),
            )
            await register(
                svc, question_id="private-question-label", aliases=("private-alias-marker",)
            )
            await fresh(svc, clock, question_id="private-question-label")
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, item[0].id, all_in_scope=all_in_scope)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        for kind in (
                            "question_content",
                            "question_certificate",
                            "question_head",
                            "question_registration",
                            "job",
                            "coverage_request",
                            "refresh_execution",
                            "refresh_publication",
                        ):
                            rows = await uow.derived_records(scope, kind)
                            encoded = json.dumps(rows)
                            assert "private-question-marker" not in encoded
                            assert "private-project-marker" not in encoded
                            assert "private-question-label" not in encoded
                            assert "private-context-marker" not in encoded
                            assert "private-alias-marker" not in encoded
                with pytest.raises(DerivedError, match="question_erased"):
                    await svc.read("private-question-label", actor=ACTOR)

    asyncio.run(run())


def test_nonempty_full_status_commitments_risks_with_unknown_state_and_qualifiers(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            specs = (
                ("project.status", "paused", "project-a", "a"),
                ("commitment.promisor", "Alice", "promise-1", "promise-a"),
                ("commitment.action", "ship milestone", "promise-1", "promise-a"),
                ("commitment.state", "open", "promise-1", "promise-a"),
                ("commitment.deadline", project.base.at(1).isoformat(), "promise-1", "promise-a"),
                ("risk.label", "supplier delayed", "promise-1", "promise-a"),
                ("risk.state", "open", "promise-1", "promise-a"),
            )
            for index, (predicate, value, subject, membership) in enumerate(specs):
                item = await project.stage(
                    svc.admission,
                    scope,
                    identity=f"fact-{index}",
                    predicate=predicate,
                    value=value,
                    subject=subject,
                    membership=membership,
                )
                await project.qualify(svc.admission, *item)
            for question in ("status", "commitments", "risks"):
                await register(svc, question, overdue_only=question == "commitments")
                result = await fresh(svc, clock, question, dedupe=question)
                assert result["answer_status"] == "resolved"
                assert result["result"]["matched_ids"]
                assert result["citations"]
                for entry in result["result"]["rows"]:
                    for field in entry["fields"]:
                        for candidate in field["candidates"]:
                            assert candidate["qualification"]["field_evidence"]
                            assert candidate["fact"]["conditions"] == []
            # No completed record does not manufacture a known open commitment.
            unknown = await project.stage(
                svc.admission,
                scope,
                identity="unknown-state",
                predicate="commitment.state",
                value="unknown",
                subject="promise-1",
                membership="promise-a",
            )
            await project.qualify(svc.admission, *unknown)
            result = await fresh(svc, clock, "commitments", dedupe="unknown")
            assert result["answer_status"] == "unknown"
            assert result["result"]["rows"][0]["matches"] is None

    asyncio.run(run())


def test_time_and_context_changed_while_body_loads_fail_final_delivery_guard(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            await fresh(svc, clock)
            original_uow = engine.repository.unit_of_work

            class ExpiringUow:
                def __init__(self):
                    self.inner = original_uow()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_get(self, scope, kind, identity):
                    value = await self.uow.derived_get(scope, kind, identity)
                    if kind == "question_content":
                        clock[0] = svc.context.expires_at
                    return value

            engine.repository.unit_of_work = ExpiringUow
            try:
                with pytest.raises(DerivedError, match="time_coverage_expired|context_expired"):
                    await svc.read("project-a:owner", actor=ACTOR)
            finally:
                engine.repository.unit_of_work = original_uow
            with pytest.raises(DerivedError, match="historical_unsupported"):
                await svc.read("project-a:owner", actor=ACTOR, valid_at=project.base.at(1))

    asyncio.run(run())


def test_durable_clock_rollback_cannot_revive_expired_question_body(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            expires = clock[0] + timedelta(seconds=10)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",), expires_at=expires),
                expected_version=1,
            )
            await register(svc)
            receipt, lease, snapshot, prepared = await lease_snapshot(svc, clock)
            await svc.publish(lease.task, snapshot, prepared)
            assert (await svc.read("project-a:owner", actor=ACTOR))[
                "availability_status"
            ] == "valid"
            clock[0] = expires + timedelta(seconds=10)
            with pytest.raises(DerivedError, match="expired"):
                await svc.read("project-a:owner", actor=ACTOR)
            clock[0] = expires - timedelta(seconds=8)
            # A restarted facade shares the database-wide committed wall floor.
            restarted = QuestionService(svc.admission, svc.context)
            with pytest.raises(DerivedError, match="clock_discontinuity"):
                await restarted.read("project-a:owner", actor=ACTOR)
            status = await restarted.queue.status(receipt["target_id"], actor=ACTOR)
            assert status["complete"] is True and status["current_ready"] is None

    asyncio.run(run())


def test_direct_read_work_respects_shared_running_quota_and_existing_lease(store):
    from agent_memory.operations.refresh_policy import RefreshLimits

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(
                engine, scope, clock, limits=RefreshLimits(global_running=1, tenant_running=1)
            )
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            await register(svc, "status")
            _, lease, snapshot, prepared = await lease_snapshot(svc, clock)
            pending = await svc.answer(
                "project-a:status", actor=ACTOR, dedupe_key="status", max_steps=1
            )
            assert pending["answer_status"] is None
            assert pending["refresh_status"] == "deferred"
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "question_content")
                running = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(),
                    tenant_id=scope.tenant_id,
                    instance_key=prepared["content"]["instance"]["definition"]["id"],
                )
                assert running["global_running"] == 1
            await svc.publish(lease.task, snapshot, prepared)
            clock[0] += timedelta(seconds=3)
            answer = await svc.answer("project-a:status", actor=ACTOR, dedupe_key="status")
            assert answer["answer_status"] == "unknown"
            assert await svc.queue.claim("duplicate", lease_seconds=30) is None

    asyncio.run(run())


def test_source_document_revision_cannot_publish_or_deliver_old_question(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            old = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *old)
            await register(svc)
            target, lease, snapshot, prepared = await lease_snapshot(svc, clock)
            event, draft = project.inputs(scope, identity="new-source", value="Bob")
            receipt = await svc.admission.stage_source(
                event,
                (draft,),
                source_authority_id=project.AUTHORITY.source_id,
                request_id="revision",
                membership_ids=("a",),
                base_event_id=old[0].id,
                expected_revision=1,
            )
            await project.grant(engine.repository, scope, event.id)
            await project.qualify(svc.admission, event, draft, receipt.candidate_ids[0])
            with pytest.raises(DerivedError, match="snapshot_changed"):
                await svc.publish(lease.task, snapshot, prepared)
            # Superseded responsibility is retained and later fulfilled by one
            # compatible full successor; it cannot be marked complete by failure.
            await svc.queue.fail(lease, DerivedError("derived_snapshot_changed"))
            clock[0] += timedelta(seconds=3)
            current = await fresh(svc, clock, dedupe="successor")
            assert current["result"]["rows"][0]["fields"][0]["known_values"] == ["Bob"]
            assert (await svc.queue.status(target["target_id"], actor=ACTOR))["complete"] is True

    asyncio.run(run())


def test_publish_owns_all_caller_inputs_before_waiting_for_transaction(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            _, lease, snapshot, prepared = await lease_snapshot(svc, clock)
            entering, release = asyncio.Event(), asyncio.Event()
            original_uow = engine.repository.unit_of_work

            class WaitingUow:
                def __init__(self):
                    self.inner = original_uow()

                async def __aenter__(self):
                    entering.set()
                    await release.wait()
                    return await self.inner.__aenter__()

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

            engine.repository.unit_of_work = WaitingUow
            task = asyncio.create_task(svc.publish(lease.task, snapshot, prepared))
            try:
                await entering.wait()
                lease.task.payload["fence"] = "caller-mutated"
                snapshot["proof"]["sources"].clear()
                prepared["content"]["structure"]["result"]["rows"].clear()
                release.set()
                result = await task
                assert result["outcome"] == "applied"
            finally:
                release.set()
                engine.repository.unit_of_work = original_uow
            answer = await svc.read("project-a:owner", actor=ACTOR)
            assert answer["result"]["rows"][0]["fields"][0]["known_values"] == ["Alice"]

    asyncio.run(run())


def test_question_read_rechecks_clock_after_final_storage_await(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            await fresh(svc, clock)
            clock[0] += timedelta(seconds=10)
            original = engine.repository.unit_of_work
            state = {"stats_written": False, "changed": False}

            class RewindingUow:
                def __init__(self):
                    self.inner = original()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_put(self, scope, kind, key, value):
                    result = await self.uow.derived_put(scope, kind, key, value)
                    if kind == "refresh_policy":
                        state["stats_written"] = True
                    return result

                async def refresh_scheduler_observe_clock(self, scope, *, now):
                    result = await self.uow.refresh_scheduler_observe_clock(scope, now=now)
                    if state["stats_written"] and not state["changed"]:
                        clock[0] -= timedelta(seconds=1)
                        state["changed"] = True
                    return result

            engine.repository.unit_of_work = RewindingUow
            try:
                with pytest.raises(DerivedError, match="clock_discontinuity"):
                    await svc.read("project-a:owner", actor=ACTOR)
                assert state["changed"]
            finally:
                engine.repository.unit_of_work = original

    asyncio.run(run())


def test_failed_question_publication_preserves_observed_expiry_across_restart(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            svc = QuestionService(
                svc.admission, replace(svc.context, expires_at=clock[0] + timedelta(seconds=10))
            )
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            _, lease, snapshot, prepared = await lease_snapshot(svc, clock)
            previous_time = clock[0]
            original = engine.repository.unit_of_work

            class ExpiringUow:
                def __init__(self):
                    self.inner = original()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_put(self, scope, kind, key, value):
                    result = await self.uow.derived_put(scope, kind, key, value)
                    if kind == "question_head":
                        clock[0] = svc.context.expires_at
                    return result

            engine.repository.unit_of_work = ExpiringUow
            try:
                with pytest.raises(DerivedError, match="expired"):
                    await svc.publish(lease.task, snapshot, prepared)
            finally:
                engine.repository.unit_of_work = original
            async with engine.repository.unit_of_work() as uow:
                for kind in ("question_content", "question_certificate", "question_head"):
                    assert not await uow.derived_records(scope, kind)
            clock[0] = previous_time + timedelta(seconds=1)
            restarted = QuestionService(project.service(engine, scope, clock), svc.context)
            with pytest.raises(DerivedError, match="clock_discontinuity"):
                await restarted.read("project-a:owner", actor=ACTOR)

    asyncio.run(run())
