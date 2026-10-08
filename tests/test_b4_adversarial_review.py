"""Independent adversarial B4 review regressions."""

import asyncio
from dataclasses import replace

import pytest
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, lease_snapshot, register, runtime

from agent_memory.conditions import Condition, ContextAttribute
from agent_memory.derived.model import DerivedError, digest
from agent_memory.serialization import to_jsonable

store = project.store


def test_publish_rejects_caller_question_not_bound_to_registered_definition(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            for options in (
                {},
                {"identity": "status", "predicate": "project.status", "value": "active"},
            ):
                item = await project.stage(svc.admission, scope, **options)
                await project.qualify(svc.admission, *item)
            await register(svc, "owner")
            _, lease, snapshot, _ = await lease_snapshot(svc, clock)
            snapshot["question"] = "status"
            prepared = svc.prepare(snapshot)
            assert prepared["content"]["structure"]["result"]["question"] == "status"
            with pytest.raises(DerivedError):
                await svc.publish(lease.task, snapshot, prepared)

    asyncio.run(run())


def test_publish_rejects_caller_context_not_bound_to_registered_context(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            conditions = (Condition("eq", "region", "US"),)
            item = await project.stage(svc.admission, scope, conditions=("only in US",))
            await project.qualify(svc.admission, *item, conditions=conditions)
            await register(svc)
            _, lease, snapshot, original = await lease_snapshot(svc, clock)
            assert original["content"]["answer_status"] == "unknown"
            # Reconstruct only the pure ID and context from the returned copy;
            # no caller needs to mutate storage or obtain additional source data.
            census = snapshot["census"]
            context = replace(
                census.snapshot.context, attributes=(ContextAttribute("region", "US", "forged"),)
            )
            identity = "project-snapshot:" + digest(
                {
                    "context": to_jsonable(context),
                    "versions": list(census.candidate_versions),
                    "grants": {grant["source_id"]: grant for grant in census.grants},
                    "source_proofs": list(census.source_proofs),
                    "contract": svc.admission.contract.fingerprint,
                    "registration": svc.admission.registration_fingerprint,
                }
            )
            snapshot["census"] = replace(
                census, snapshot=replace(census.snapshot, id=identity, context=context)
            )
            prepared = svc.prepare(snapshot)
            assert prepared["content"]["answer_status"] == "resolved"
            with pytest.raises(DerivedError):
                await svc.publish(lease.task, snapshot, prepared)

    asyncio.run(run())


def test_inherited_removed_source_expiry_is_part_of_delta_time_coverage(store):
    from datetime import datetime, timedelta

    from test_question_runtime_v7 import fresh

    from agent_memory.derived.model import ProcessingGrant

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            public = await project.stage(svc.admission, scope, identity="public")
            private = await project.stage(
                svc.admission,
                scope,
                identity="private",
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="private",
            )
            for item in (public, private):
                await project.qualify(svc.admission, *item)
            expiry = clock[0] + timedelta(seconds=10)
            await svc.grant(
                ProcessingGrant("private", (ACTOR,), ("project_questions",), expires_at=expiry),
                expected_version=1,
            )
            await register(svc)
            first = await fresh(svc, clock)
            assert datetime.fromisoformat(first["valid_until"]) == expiry
            await svc.admission.replace_membership(
                private[2], expected_version=4, membership_id="promise-b"
            )
            second = await fresh(svc, clock, dedupe="moved")
            inherited = {
                i["id"] for i in second["generation_manifest"]["inputs"] if i["kind"] == "source"
            }
            assert "private" in inherited
            assert datetime.fromisoformat(second["valid_until"]) <= expiry

    asyncio.run(run())


def test_inherited_removed_source_expiry_during_final_guard_never_delivers(store):
    from datetime import timedelta

    from test_question_runtime_v7 import fresh

    from agent_memory.derived.model import ProcessingGrant

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            public = await project.stage(svc.admission, scope, identity="public")
            private = await project.stage(
                svc.admission,
                scope,
                identity="private",
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="private",
            )
            for item in (public, private):
                await project.qualify(svc.admission, *item)
            expiry = clock[0] + timedelta(seconds=10)
            await svc.grant(
                ProcessingGrant("private", (ACTOR,), ("project_questions",), expires_at=expiry),
                expected_version=1,
            )
            await register(svc)
            await fresh(svc, clock)
            await svc.admission.replace_membership(
                private[2], expected_version=4, membership_id="promise-b"
            )
            second = await fresh(svc, clock, dedupe="moved")
            assert "private" in {
                i["id"] for i in second["generation_manifest"]["inputs"] if i["kind"] == "source"
            }
            original = engine.repository.unit_of_work
            loaded = [False]

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

                async def derived_get(self, scope, kind, key):
                    value = await self.uow.derived_get(scope, kind, key)
                    if kind == "question_content":
                        loaded[0] = True
                    if loaded[0] and kind == "grant" and key == "private":
                        clock[0] = expiry
                    return value

            engine.repository.unit_of_work = ExpiringUow
            try:
                with pytest.raises(DerivedError):
                    await svc.read("project-a:owner", actor=ACTOR)
                assert loaded[0]
            finally:
                engine.repository.unit_of_work = original

    asyncio.run(run())


@pytest.mark.parametrize("damage", ["missing", "gap", "corrupt_baseline", "rollover"])
def test_integrated_unprovable_optimization_rebuilds_after_restart(store, damage):
    from test_question_runtime_v7 import fresh

    from agent_memory.derived import subscriptions
    from agent_memory.derived.model import ProcessingGrant
    from agent_memory.derived.question_delta import LOG_LIMIT
    from agent_memory.derived.question_service import QuestionService

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            first = await fresh(svc, clock)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            async with engine.repository.unit_of_work() as uow:
                await svc._open(uow)
                head = await uow.derived_get(scope, "question_head", first["instance_id"])
                if damage == "corrupt_baseline":
                    state = await uow.derived_get(
                        scope, "question_delta_state", first["instance_id"]
                    )
                    state["rows"] = {}
                    await uow.derived_put(
                        scope, "question_delta_state", first["instance_id"], state
                    )
                else:
                    for key, old_sequence in head["proof"]["query_generations"].items():
                        barrier = await uow.derived_get(scope, "barrier", key)
                        if not barrier or barrier["generation"] == old_sequence:
                            continue
                        if damage == "rollover":
                            for _ in range(LOG_LIMIT + 1):
                                await subscriptions.bump(uow, scope, key)
                        else:
                            log = await uow.derived_get(scope, "question_change_log", key)
                            if damage == "missing":
                                log = {}
                            else:
                                log["entries"] = [
                                    entry
                                    for entry in log["entries"]
                                    if entry["sequence"] != old_sequence + 1
                                ]
                                log["sha256"] = digest(
                                    {k: v for k, v in log.items() if k != "sha256"}
                                )
                            await uow.derived_put(scope, "question_change_log", key, log)
            restarted = QuestionService(project.service(engine, scope, clock), svc.context)
            second = await fresh(restarted, clock, dedupe="rebuild")
            assert second["compute_mode"] == "full"
            assert second["compute_trace"]["fallback_reason"] in {
                "missing_baseline",
                "change_log_gap",
            }
            assert second["result"]["rows"] == first["result"]["rows"]
            async with engine.repository.unit_of_work() as uow:
                state = await uow.derived_get(scope, "question_delta_state", first["instance_id"])
                head = await uow.derived_get(scope, "question_head", first["instance_id"])
                assert state["query_generations"] == head["proof"]["query_generations"]
                assert digest(state) == head["delta_state_sha256"]

    asyncio.run(run())


def test_delta_state_and_pending_page_validation_are_scrubbed_from_real_backup(store, tmp_path):
    import json

    from test_purge_restore import backup_copy, erase, replay, restorer
    from test_question_runtime_v7 import fresh

    from agent_memory.derived.model import ProcessingGrant
    from agent_memory.derived.question_erasure import QUESTION_KINDS

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope, value="adversarial-private-marker")
            await project.qualify(svc.admission, *item)
            await register(svc)
            await fresh(svc, clock)
            await svc.pages.register(
                "adversarial-private-page", ("project-a:owner",), readers=(ACTOR,)
            )
            await svc.pages.publish("adversarial-private-page", actor=ACTOR)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            await fresh(svc, clock, dedupe="pending-proof")
            async with engine.repository.unit_of_work() as uow:
                states = await uow.derived_records(scope, "question_delta_state")
                validations = await uow.derived_records(scope, "question_page_validation")
                assert states and "adversarial-private-marker" in json.dumps(states)
                assert any(r["payload"]["state"] == "validation_pending" for r in validations)
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, item[0].id)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        for kind in QUESTION_KINDS:
                            rows = await uow.derived_records(scope, kind)
                            assert "adversarial-private" not in json.dumps(rows)
                            assert all(r["payload"]["state"] == "erased" for r in rows)

    asyncio.run(run())


def test_bounded_page_validation_does_not_starve_healthy_pages_behind_blocked_page(store):
    from test_question_page_validation_v7 import setup
    from test_question_runtime_v7 import fresh

    from agent_memory.derived.model import ProcessingGrant

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, count=2)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            await fresh(svc, clock, dedupe="two-pending-pages")
            blocked, healthy = sorted(("page:0", "page:1"), key=svc.pages.key)
            await svc.pages.register(
                blocked,
                ("project-a:owner",),
                readers=(ACTOR,),
                expected_generation=1,
                max_output_bytes=1024,
            )
            first = await svc.pages.validate_pending(actor=ACTOR, max_pages=1)
            assert first == [
                {
                    "page_id": blocked,
                    "state": "validation_pending",
                    "reason": "question_output_capacity",
                }
            ]
            second = await svc.pages.validate_pending(actor=ACTOR, max_pages=1)
            assert second == [{"page_id": healthy, "state": "valid", "rebuild": "proof_reuse"}]
            assert (await svc.pages.read(healthy, actor=ACTOR))["availability_status"] == "valid"
            with pytest.raises(DerivedError):
                await svc.pages.read(blocked, actor=ACTOR)

    asyncio.run(run())


def test_answer_regenerates_independently_after_original_private_source_is_revoked(store):
    from test_question_runtime_v7 import fresh

    from agent_memory.derived.model import ProcessingGrant

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            public = await project.stage(svc.admission, scope, identity="public")
            private = await project.stage(
                svc.admission,
                scope,
                identity="private",
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="adversarial-private-action",
            )
            for item in (public, private):
                await project.qualify(svc.admission, *item)
            await register(svc)
            first = await fresh(svc, clock)
            await svc.admission.replace_membership(
                private[2], expected_version=4, membership_id="promise-b"
            )
            await svc.grant(
                ProcessingGrant("private", (ACTOR,), ("project_questions",), revoked=True),
                expected_version=1,
            )
            second = await fresh(svc, clock, dedupe="independent-public")
            assert second["compute_mode"] == "full"
            assert second["compute_trace"]["fallback_reason"] == "missing_baseline"
            assert second["content_revision_id"] != first["content_revision_id"]
            assert {
                r["id"] for r in second["generation_manifest"]["inputs"] if r["kind"] == "source"
            } == {"public"}
            assert "adversarial-private-action" not in str(second)
            async with engine.repository.unit_of_work() as uow:
                old = await uow.derived_get(scope, "question_content", first["content_revision_id"])
                assert old["generation_manifest"] == first["generation_manifest"]

    asyncio.run(run())


def test_answer_cannot_recover_by_processing_a_current_revoked_source(store):
    from test_question_runtime_v7 import fresh

    from agent_memory.derived.model import ProcessingGrant

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope, identity="private")
            await project.qualify(svc.admission, *item)
            await register(svc)
            first = await fresh(svc, clock)
            await svc.grant(
                ProcessingGrant("private", (ACTOR,), ("project_questions",), revoked=True),
                expected_version=1,
            )
            original = engine.repository.unit_of_work
            bodies = []

            class ObserveBodies:
                def __init__(self):
                    self.inner = original()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_get(self, scope, kind, key):
                    if kind in {"question_content", "question_delta_state"}:
                        bodies.append(kind)
                    return await self.uow.derived_get(scope, kind, key)

                async def get_source_event(self, scope, key):
                    bodies.append("source")
                    return await self.uow.get_source_event(scope, key)

            engine.repository.unit_of_work = ObserveBodies
            try:
                result = await fresh(svc, clock, dedupe="still-private")
                assert result["availability_status"] == "stale"
                assert result["answer_status"] is None
                assert "result" not in result and "citations" not in result
                assert not bodies
            finally:
                engine.repository.unit_of_work = original
            async with engine.repository.unit_of_work() as uow:
                contents = await uow.derived_records(scope, "question_content")
                assert len(contents) == 1
                assert contents[0]["identity"] == first["content_revision_id"]

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["snapshot", "read", "page_read", "page_publish"])
@pytest.mark.parametrize("change", ["grant_expiry", "context", "host"])
def test_every_cached_body_has_its_own_before_and_after_guard(store, operation, change):
    from datetime import timedelta

    from test_question_runtime_v7 import fresh

    from agent_memory.derived.model import ProcessingGrant

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            expiry = clock[0] + timedelta(seconds=10)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",), expires_at=expiry),
                expected_version=1,
            )
            await register(svc)
            await fresh(svc, clock)
            if operation.startswith("page_"):
                await svc.pages.register("page", ("project-a:owner",), readers=(ACTOR,))
                await svc.pages.publish("page", actor=ACTOR)
            if operation == "snapshot":
                await svc.grant(
                    ProcessingGrant("source", (ACTOR,), ("project_questions",), expires_at=expiry),
                    expected_version=2,
                )
                await svc.request("project-a:owner", actor=ACTOR, dedupe_key="cached-boundary")
                lease = await svc.queue.claim("worker", lease_seconds=30)
            original = engine.repository.unit_of_work
            first = "question_page_content" if operation.startswith("page_") else "question_content"
            bodies, fired = [], [False]

            class ChangingBody:
                def __init__(self):
                    self.inner = original()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_get(self, scope, kind, key):
                    if fired[0] and kind in {
                        "question_content",
                        "question_certificate",
                        "question_delta_state",
                        "question_page_content",
                        "question_page_certificate",
                        "question_page_block",
                    }:
                        bodies.append(kind)
                    value = await self.uow.derived_get(scope, kind, key)
                    if kind == first and not fired[0]:
                        fired[0] = True
                        if change == "grant_expiry":
                            clock[0] = expiry
                        elif change == "context":
                            svc.context = replace(svc.context, revision="changed-during-body")
                        else:
                            svc.admission.memberships["a"] = replace(
                                project.MEMBERSHIPS[0], registry_revision="changed-during-body"
                            )
                    return value

            engine.repository.unit_of_work = ChangingBody
            try:
                with pytest.raises(DerivedError):
                    if operation == "snapshot":
                        await svc.snapshot(lease.task)
                    elif operation == "read":
                        await svc.read("project-a:owner", actor=ACTOR)
                    elif operation == "page_read":
                        await svc.pages.read("page", actor=ACTOR)
                    else:
                        await svc.pages.publish("page", actor=ACTOR)
                assert fired[0]
                assert not bodies, "A later cached body loaded after controls changed"
            finally:
                engine.repository.unit_of_work = original

    asyncio.run(run())
