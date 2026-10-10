"""Caller-owned snapshots cannot authorize different registered question inputs."""

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
                {}, {"identity": "status", "predicate": "project.status", "value": "active"}
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
            context = replace(census.snapshot.context,
                              attributes=(ContextAttribute("region", "US", "forged"),))
            identity = "project-snapshot:" + digest({
                "context": to_jsonable(context),
                "versions": list(census.candidate_versions),
                "grants": {grant["source_id"]: grant for grant in census.grants},
                "source_proofs": list(census.source_proofs),
                "contract": svc.admission.contract.fingerprint,
                "registration": svc.admission.registration_fingerprint,
            })
            snapshot["census"] = replace(census, snapshot=replace(census.snapshot,
                id=identity, context=context))
            prepared = svc.prepare(snapshot)
            assert prepared["content"]["answer_status"] == "resolved"
            with pytest.raises(DerivedError):
                await svc.publish(lease.task, snapshot, prepared)
    asyncio.run(run())



@pytest.mark.parametrize("change", ["context", "grant", "host"])
@pytest.mark.parametrize("boundary", ["metadata", "between_sources", "before_candidates"])
def test_snapshot_rechecks_current_controls_before_each_source_body(
    store, monkeypatch, change, boundary
):
    from datetime import timedelta

    from agent_memory.derived.model import ProcessingGrant
    from agent_memory.derived.question_service import QuestionService

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            expiry = clock[0] + timedelta(seconds=10)
            if change == "context":
                svc = QuestionService(svc.admission, replace(svc.context, expires_at=expiry))
            for key in (("first",) if boundary == "before_candidates" else ("first", "second")):
                item = await project.stage(svc.admission, scope, identity=key)
                await project.qualify(svc.admission, *item)
                if change == "grant":
                    await svc.grant(ProcessingGrant(key, (ACTOR,), ("project_questions",),
                                                   expires_at=expiry), expected_version=1)
            await register(svc)
            await svc.request("project-a:owner", actor=ACTOR, dedupe_key="snapshot")
            lease = await svc.queue.claim("snapshot", lease_seconds=30)
            previous = clock[0]
            cls = type(engine.repository.unit_of_work())
            original_grants = svc.admission._grants
            original_source = cls.get_source_event
            calls, headers = [], []

            def change_control():
                if change == "host":
                    membership = svc.admission.memberships["a"]
                    svc.admission.memberships["a"] = replace(
                        membership, registry_revision="host-replaced"
                    )
                else:
                    clock[0] = expiry

            async def grants(uow, *args):
                value = await original_grants(uow, *args)
                headers.append(True)
                # First current-control read is the metadata proof. Second is
                # the semantic bridge; immutable candidate headers now share
                # one scan, but current grant/time checks must remain fresh.
                if boundary == "metadata" and len(headers) == 2:
                    change_control()
                return value

            async def source(uow, scope, key):
                calls.append(key)
                if boundary == "metadata" or len(calls) > 1:
                    pytest.fail("Source body loaded after current input authority changed")
                value = await original_source(uow, scope, key)
                change_control()
                return value

            with monkeypatch.context() as patch:
                patch.setattr(svc.admission, "_grants", grants)
                patch.setattr(cls, "get_source_event", source)
                if boundary == "before_candidates":
                    async def forbidden_candidate(*args, **kwargs):
                        pytest.fail("Candidate body loaded after current input authority changed")
                    patch.setattr(cls, "get_admission_record", forbidden_candidate)
                with pytest.raises(DerivedError, match="expired|registration_changed"):
                    await svc.snapshot(lease.task)
            assert calls == ([] if boundary == "metadata" else ["first"])
            if change != "host":
                clock[0] = previous + timedelta(seconds=1)
                restarted = QuestionService(project.service(engine, scope, clock), svc.context)
                with pytest.raises(DerivedError, match="clock_discontinuity"):
                    await restarted.read("project-a:owner", actor=ACTOR)

    asyncio.run(run())
