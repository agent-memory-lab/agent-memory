import asyncio
import json
from datetime import timedelta
from hashlib import sha256

import pytest
import test_atom_admission as base

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.verification_tools import LocalRecordVerifier
from agent_memory.domain import ForgetMode, ForgetRequest, PredicateSpec, SourceAuthority
from agent_memory.operations.domain_verification import (
    KIND,
    DomainVerificationQueue,
    VerificationFinding,
    VerificationToolSpec,
    admission_publisher,
)
from agent_memory.retrieval.model_contracts import ModelError

store = base.store


async def setup(engine, scope, clock, tmp_path, *, value="Shanghai"):
    policy = AdmissionPolicy([PredicateSpec("city", allow_self_report=False)])
    user = SourceAuthority("alice-login", subjects=("alice",), predicates=("city",))
    event = base.source(scope)
    receipt = await engine.admit(event, (base.atom(),), authority=user, policy=policy)
    row = await engine.repository.admission_record(scope, receipt.candidate_ids[0])
    issuer = SourceAuthority("city-registry", "tool_observation", ("alice",), ("city",))
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "schema": "authoritative-domain-records/1",
                "issuer": issuer.source_id,
                "records": [
                    {
                        "subject_id": "alice",
                        "predicate": "city",
                        "value": value,
                        "valid_from": base.at(1).isoformat(),
                        "valid_to": None,
                        "recorded_at": base.at(1).isoformat(),
                    }
                ],
            }
        )
    )
    tool = LocalRecordVerifier(
        path,
        expected_sha256=sha256(path.read_bytes()).hexdigest(),
        spec=VerificationToolSpec("registry", "1", issuer, ("alice",), ("city",)),
    )

    async def authorize(*_):
        return True

    queue = DomainVerificationQueue(
        engine.repository,
        scope,
        (tool,),
        authorize=authorize,
        publisher=admission_publisher(engine, policy),
        clock=lambda: clock[0],
        timeout_seconds=5,
    )
    identity = await queue.schedule(
        row["id"], row["version"], tool_id="registry", request_id="check-city"
    )
    return queue, identity, row, event, tool


@pytest.mark.parametrize("value,expected", [("Hangzhou", "supported"), ("Shanghai", "refuted")])
def test_authoritative_result_and_task_commit_together(store, tmp_path, value, expected):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            queue, identity, row, event, tool = await setup(
                engine, scope, clock, tmp_path, value=value
            )
            assert await queue.run_once("worker") == expected
            updated = await engine.repository.admission_record(scope, row["id"])
            assert updated["payload"]["action"] == (
                "ACCEPT" if expected == "supported" else "REJECT"
            )
            async with engine.repository.unit_of_work() as uow:
                state = await uow.derived_get(scope, KIND, identity)
                assert state["state"] == "completed" and state["disposition"] == expected

    asyncio.run(run())


def test_erasure_and_expired_lease_cannot_publish(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            queue, identity, row, event, tool = await setup(engine, scope, clock, tmp_path)
            lease = await queue.claim("worker", lease_seconds=10)
            finding = await tool.verify(row)
            clock[0] += timedelta(seconds=11)
            with pytest.raises(ModelError, match="lease_fenced"):
                await queue.publish(lease, finding)
            await kernel.forget(ForgetRequest(scope, (event.id,), mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, KIND, identity) == {"state": "erased"}
            assert await queue.claim("after-erase", lease_seconds=10) is None

    asyncio.run(run())


def test_unknown_does_not_reject_and_two_workers_do_not_share_lease(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            queue, identity, row, event, tool = await setup(engine, scope, clock, tmp_path)
            leases = await asyncio.gather(
                queue.claim("a", lease_seconds=10), queue.claim("b", lease_seconds=10)
            )
            assert sum(value is not None for value in leases) == 1
            lease = next(value for value in leases if value)
            assert await queue.publish(lease, VerificationFinding("unknown")) == "unknown"
            current = await engine.repository.admission_record(scope, row["id"])
            assert (
                current["payload"]["action"] == "PENDING_VERIFICATION"
                and current["version"] == row["version"]
            )

    asyncio.run(run())


def test_publisher_failure_rolls_back_facts_and_task_completion(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            queue, identity, row, event, tool = await setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            original = queue.publisher

            async def broken(uow, candidate, finding, spec):
                await original(uow, candidate, finding, spec)
                raise RuntimeError("private authority response")

            queue.publisher = broken
            with pytest.raises(ModelError, match="verification_tool_failed"):
                await queue.run_once("worker", lease_seconds=10)
            current = await engine.repository.admission_record(scope, row["id"])
            assert (
                current["version"] == row["version"]
                and current["payload"]["action"] == "PENDING_VERIFICATION"
            )
            async with engine.repository.unit_of_work() as uow:
                state = await uow.derived_get(scope, KIND, identity)
                assert state["state"] == "running"
                assert state["reason"] == "verification_tool_failed"
                assert state["publication_attempt"]["state"] == "fenced"
                assert state["lease_until"] == (clock[0] + timedelta(seconds=10)).isoformat()
            queue.publisher = original
            assert await queue.claim("too-early", lease_seconds=10) is None
            clock[0] += timedelta(seconds=10)
            assert await queue.run_once("new-lease", lease_seconds=10) == "supported"

    asyncio.run(run())


def test_expiry_during_publisher_rolls_back_actual_fact_commit(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            queue, identity, row, event, tool = await setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            original = queue.publisher

            async def late(uow, candidate, finding, spec):
                await original(uow, candidate, finding, spec)
                clock[0] += timedelta(seconds=31)

            queue.publisher = late
            with pytest.raises(ModelError, match="commit_fenced"):
                await queue.run_once("late-worker")
            current = await engine.repository.admission_record(scope, row["id"])
            assert (
                current["version"] == row["version"]
                and current["payload"]["action"] == "PENDING_VERIFICATION"
            )

    asyncio.run(run())


@pytest.mark.parametrize("disposition", ["supported", "refuted"])
def test_native_project_tool_review_uses_atomic_field_publication(store, disposition):
    import test_project_admission_v7 as project

    from agent_memory.derived.model import ProcessingGrant
    from agent_memory.operations.domain_verification import project_publisher

    async def run():
        async with store() as (engine, kernel, scope, clock):
            admission = project.service(engine, scope, clock)
            event, draft, candidate_id = await project.stage(admission, scope)
            row = await engine.repository.admission_record(scope, candidate_id)
            tool_spec = VerificationToolSpec(
                "project-system",
                "1",
                project.AUTHORITY,
                project.AUTHORITY.subjects,
                project.AUTHORITY.predicates,
            )
            witness = base.source(scope, "Authoritative owner is Alice", identity="witness")
            from dataclasses import replace

            witness = replace(witness, metadata={"lifecycle": {"origin": "tool"}})
            finding = VerificationFinding(
                disposition, witness, witness.content, base.at(1), None, project.FIELDS
            )

            class Tool:
                spec = tool_spec

                async def verify(self, candidate):
                    return finding

            async def accept(uow, evidence):
                if await uow.get_source_event(scope, evidence.id) is None:
                    await uow.append_event(evidence)
                grant = ProcessingGrant(
                    evidence.id, ("host:alice",), ("project_questions",)
                ).payload()
                await uow.derived_put(scope, "grant", evidence.id, {**grant, "version": 1})
                return True

            async def authorize(*_):
                return True

            queue = DomainVerificationQueue(
                engine.repository,
                scope,
                (Tool(),),
                authorize=authorize,
                publisher=project_publisher(admission, accept_evidence=accept),
                clock=lambda: clock[0],
                timeout_seconds=5,
            )
            await queue.schedule(
                candidate_id, row["version"], tool_id=tool_spec.id, request_id="project-review"
            )
            assert await queue.run_once("project-worker") == disposition
            updated = await engine.repository.admission_record(scope, candidate_id)
            assert updated["payload"]["project_candidate"]["review"]["disposition"] == (
                "qualified" if disposition == "supported" else "rejected"
            )
            if disposition == "supported":
                answer = project.answer(await project.snapshot(admission, clock))
                assert answer.status.value == "resolved"

    asyncio.run(run())


def test_typed_conditional_tool_keeps_original_condition_and_context(store):
    import test_contextual_memory as contextual_test

    from agent_memory.conditions import Condition
    from agent_memory.consolidation.qualification import ContextualMemory
    from agent_memory.domain import MemoryEvent
    from agent_memory.operations.domain_verification import contextual_publisher

    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, draft, identity = await contextual_test.candidate(engine, scope)
            witness = MemoryEvent(
                scope,
                "tool",
                "Only project A: Alice lives in Hangzhou",
                occurred_at=clock[0],
                metadata={"lifecycle": {"origin": "tool"}},
            )
            spec = VerificationToolSpec("system", "1", base.TOOL, ("alice",), ("city",))
            fields = ("subject_id", "predicate", "value", "valid_from", "conditions")
            finding = VerificationFinding(
                "supported",
                witness,
                witness.content,
                base.at(1),
                None,
                fields,
                conditions=(Condition("eq", "project", "A"),),
            )

            class Tool:
                async def verify(self, candidate):
                    return finding

            tool = Tool()
            tool.spec = spec

            async def accept(uow, source):
                await uow.append_event(source)
                return True

            async def authorize(*_):
                return True

            contextual = ContextualMemory(engine, scope, principal="host:alice")
            queue = DomainVerificationQueue(
                engine.repository,
                scope,
                (tool,),
                authorize=authorize,
                publisher=contextual_publisher(
                    contextual, base.POLICY, contextual_test.POLICY, accept_evidence=accept
                ),
                clock=lambda: clock[0],
                timeout_seconds=5,
            )
            await queue.schedule(identity, 1, tool_id="system", request_id="conditional")
            assert await queue.run_once("worker") == "supported"
            row = await engine.repository.admission_record(scope, identity)
            assert row["payload"]["action"] == "PENDING_VERIFICATION"
            assert row["payload"]["qualification"]["conditions"][0]["value"] == "A"
            assert row["payload"]["draft"]["conditions"] == ["Only in this project"]

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["final_authorization", "completion_write"])
@pytest.mark.parametrize("disposition", ["supported", "unknown"])
def test_final_guard_rolls_back_expiry_after_last_async_boundary(
    store, tmp_path, monkeypatch, boundary, disposition
):
    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            lease = await queue.claim("worker", lease_seconds=10)
            finding = (
                await tool.verify(row)
                if disposition == "supported"
                else VerificationFinding("unknown")
            )
            if boundary == "final_authorization":
                calls = []

                async def late_authorize(*_):
                    calls.append(True)
                    if len(calls) == 3:
                        clock[0] += timedelta(seconds=10)
                    return True

                queue.authorize = late_authorize
            else:
                async with engine.repository.unit_of_work() as uow:
                    cls = type(uow)
                put = cls.derived_put

                async def late_completion(uow, target, kind, key, payload):
                    await put(uow, target, kind, key, payload)
                    if kind == KIND and key == identity and payload.get("state") == "completed":
                        clock[0] += timedelta(seconds=10)

                monkeypatch.setattr(cls, "derived_put", late_completion)
            with pytest.raises(ModelError, match="commit_fenced"):
                await queue.publish(lease, finding)
            current = await engine.repository.admission_record(scope, row["id"])
            assert current["version"] == row["version"]
            assert current["payload"]["action"] == "PENDING_VERIFICATION"
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, KIND, identity))["state"] == "running"

    asyncio.run(run())


@pytest.mark.parametrize("change", ["authority", "tool"])
def test_completion_write_cannot_outlive_current_host_guard(store, tmp_path, monkeypatch, change):
    from dataclasses import replace

    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            allowed = [True]

            async def authorize(*_):
                return allowed[0]

            queue.authorize = authorize
            lease = await queue.claim("worker", lease_seconds=10)
            finding = await tool.verify(row)
            async with engine.repository.unit_of_work() as uow:
                cls = type(uow)
            put = cls.derived_put

            async def changed_completion(uow, target, kind, key, payload):
                await put(uow, target, kind, key, payload)
                if kind == KIND and key == identity and payload.get("state") == "completed":
                    if change == "authority":
                        allowed[0] = False
                    else:
                        tool.spec = replace(tool.spec, version="changed")

            monkeypatch.setattr(cls, "derived_put", changed_completion)
            with pytest.raises(ModelError, match="commit_fenced"):
                await queue.publish(lease, finding)
            current = await engine.repository.admission_record(scope, row["id"])
            assert current["version"] == row["version"]
            assert current["payload"]["action"] == "PENDING_VERIFICATION"
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, KIND, identity))["state"] == "running"

    asyncio.run(run())


def test_final_authorization_clock_observation_survives_restart(store, tmp_path):
    from agent_memory.derived.model import DerivedError

    async def run():
        async with store() as (engine, _, scope, clock):
            queue, _, _, _, _ = await setup(engine, scope, clock, tmp_path)
            lease = await queue.claim("worker", lease_seconds=10)
            calls = []

            async def advance_during_final_authorize(*_):
                calls.append(True)
                if len(calls) == 3:
                    clock[0] += timedelta(seconds=5)
                return True

            queue.authorize = advance_during_final_authorize
            assert await queue.publish(lease, VerificationFinding("unknown")) == "unknown"
            clock[0] -= timedelta(seconds=1)
            restarted = DomainVerificationQueue(
                engine.repository,
                scope,
                tuple(queue.tools.values()),
                authorize=queue.authorize,
                publisher=queue.publisher,
                clock=lambda: clock[0],
                timeout_seconds=5,
            )
            with pytest.raises(DerivedError, match="clock_discontinuity"):
                await restarted.claim("restarted", lease_seconds=10)

    asyncio.run(run())


def test_failed_expiry_observation_cannot_reopen_original_lease_after_clock_rollback(
    store, tmp_path
):
    from agent_memory.derived.model import DerivedError

    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            lease = await queue.claim("worker", lease_seconds=10)
            finding = await tool.verify(row)
            calls = []

            async def expire_during_final_authorize(*_):
                calls.append(True)
                if len(calls) == 3:
                    clock[0] += timedelta(seconds=11)
                return True

            queue.authorize = expire_during_final_authorize
            with pytest.raises(ModelError, match="commit_fenced"):
                await queue.publish(lease, finding)
            clock[0] -= timedelta(seconds=10)
            with pytest.raises(DerivedError, match="clock_discontinuity"):
                await queue.publish(lease, finding)
            current = await engine.repository.admission_record(scope, row["id"])
            assert current["version"] == row["version"]
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, KIND, identity))["state"] == "running"

    asyncio.run(run())


@pytest.mark.parametrize("disposition", ["supported", "unknown"])
def test_final_clock_write_revocation_fences_publication(store, tmp_path, monkeypatch, disposition):
    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            lease = await queue.claim("worker", lease_seconds=10)
            finding = (
                await tool.verify(row)
                if disposition == "supported"
                else VerificationFinding("unknown")
            )
            state = {"allowed": True, "calls": 0, "revoked": False}

            async def authorize(*_):
                state["calls"] += 1
                return state["allowed"]

            queue.authorize = authorize
            async with engine.repository.unit_of_work() as uow:
                cls = type(uow)
            observe = cls.refresh_scheduler_observe_clock

            async def revoke_at_final_clock(uow, requested_scope, *, now):
                result = await observe(uow, requested_scope, now=now)
                if state["calls"] == 2:
                    state["allowed"] = False
                    state["revoked"] = True
                return result

            monkeypatch.setattr(cls, "refresh_scheduler_observe_clock", revoke_at_final_clock)
            with pytest.raises(ModelError, match="commit_fenced"):
                await queue.publish(lease, finding)
            assert state["revoked"]
            current = await engine.repository.admission_record(scope, row["id"])
            assert current["version"] == row["version"]
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, KIND, identity))["state"] == "running"

    asyncio.run(run())


def test_successful_checkpoint_superseded_by_newer_floor_is_not_publication_failure(
    store, tmp_path, monkeypatch
):
    from agent_memory.derived.model import DerivedError

    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            lease = await queue.claim("worker", lease_seconds=10)
            finding = await tool.verify(row)
            barrier = queue._clock_barrier
            calls = []

            async def concurrent_newer_floor(sample):
                calls.append(True)
                if len(calls) == 2:
                    await barrier(lambda: clock[0] + timedelta(seconds=5))
                return await barrier(sample)

            monkeypatch.setattr(queue, "_clock_barrier", concurrent_newer_floor)
            assert await queue.publish(lease, finding) == "supported"
            assert (await queue.backlog())["clock_persistence_error"] is None
            current = await engine.repository.admission_record(scope, row["id"])
            assert current["payload"]["action"] == "ACCEPT"
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, KIND, identity))["state"] == "completed"
            with pytest.raises(DerivedError, match="clock_discontinuity"):
                await queue.claim("clock-behind", lease_seconds=10)

    asyncio.run(run())


@pytest.mark.parametrize("disposition", ["supported", "unknown"])
def test_failed_success_checkpoint_reports_committed_degradation_and_blocks_more_work(
    store, tmp_path, monkeypatch, disposition
):
    from agent_memory.derived.model import DerivedError

    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            lease = await queue.claim("worker", lease_seconds=10)
            finding = (
                await tool.verify(row)
                if disposition == "supported"
                else VerificationFinding("unknown")
            )
            auth_calls = []

            async def late_authorize(*_):
                auth_calls.append(True)
                if len(auth_calls) == 3:
                    clock[0] += timedelta(seconds=5)
                return True

            queue.authorize = late_authorize
            barrier = queue._clock_barrier
            calls, failing = [], [True]

            async def fail_checkpoint(sample):
                calls.append(True)
                if len(calls) >= 2 and failing[0]:
                    raise RuntimeError("private provider detail")
                return await barrier(sample)

            monkeypatch.setattr(queue, "_clock_barrier", fail_checkpoint)
            assert await queue.publish(lease, finding) == (
                f"committed_{disposition}_clock_checkpoint_pending"
            )
            final_clock = clock[0]
            clock[0] -= timedelta(seconds=4)
            restarted = DomainVerificationQueue(
                engine.repository,
                scope,
                tuple(queue.tools.values()),
                authorize=queue.authorize,
                publisher=queue.publisher,
                clock=lambda: clock[0],
                timeout_seconds=5,
            )
            with pytest.raises(ModelError, match="lease_fenced"):
                await restarted.publish(lease, finding)
            clock[0] = final_clock
            metrics = await queue.backlog()
            assert metrics["clock_persistence_error"] == "verification_clock_checkpoint_failed"
            assert "private provider detail" not in str(metrics)
            current = await engine.repository.admission_record(scope, row["id"])
            assert current["payload"]["action"] == (
                "ACCEPT" if disposition == "supported" else "PENDING_VERIFICATION"
            )
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, KIND, identity))["state"] == "completed"
            with pytest.raises(ModelError, match="checkpoint_unavailable"):
                await queue.claim("blocked", lease_seconds=10)
            with pytest.raises(ModelError, match="checkpoint_unavailable"):
                await queue.schedule(
                    row["id"], row["version"], tool_id="registry", request_id="new"
                )
            failing[0] = False
            assert await queue.claim("recovered", lease_seconds=10) is None
            assert (await queue.backlog())["clock_persistence_error"] is None
            clock[0] -= timedelta(seconds=1)
            with pytest.raises(DerivedError, match="clock_discontinuity"):
                await queue.claim("rollback-after-recovery", lease_seconds=10)

    asyncio.run(run())
