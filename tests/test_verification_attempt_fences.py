"""Durable one-attempt-per-lease fencing under checkpoint/storage interruption."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_domain_verification as base

from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.operations.domain_verification import (
    KIND,
    DomainVerificationQueue,
    VerificationFinding,
)
from agent_memory.retrieval.model_contracts import ModelError

store = base.store


def restart(queue, clock, *, repository=None, scope=None):
    async def allowed(*_):
        return True

    return DomainVerificationQueue(
        repository or queue.repository,
        scope or queue.scope,
        tuple(queue.tools.values()),
        authorize=allowed,
        publisher=queue.publisher,
        clock=lambda: clock[0],
        timeout_seconds=5,
    )


@pytest.mark.parametrize("disposition", ["supported", "unknown"])
def test_failed_checkpoint_poison_survives_restart_and_needs_trusted_recovery(
    store, tmp_path, monkeypatch, disposition
):
    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await base.setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            lease = await queue.claim("worker", lease_seconds=10)
            finding = (
                await tool.verify(row)
                if disposition == "supported"
                else VerificationFinding("unknown")
            )
            auth_calls = []

            async def expire(*_):
                auth_calls.append(True)
                if len(auth_calls) == 3:
                    clock[0] += timedelta(seconds=11)
                return True

            queue.authorize = expire
            barrier, calls = queue._clock_barrier, []

            async def unavailable_checkpoint(sample):
                calls.append(True)
                if len(calls) == 2:
                    raise RuntimeError("checkpoint storage unavailable")
                return await barrier(sample)

            monkeypatch.setattr(queue, "_clock_barrier", unavailable_checkpoint)
            with pytest.raises(ModelError, match="commit_fenced_recovery_required"):
                await queue.publish(lease, finding)
            async with engine.repository.unit_of_work() as uow:
                task = await uow.derived_get(scope, KIND, identity)
                assert task["state"] == "running"
                assert task["publication_attempt"]["state"] == "pending"
                assert task["lease_until"] == lease["task"]["lease_until"]
            clock[0] -= timedelta(seconds=10)
            recovered = restart(queue, clock)
            with pytest.raises(ModelError, match="recovery_required"):
                await recovered.publish(lease, finding)
            with pytest.raises(ModelError, match="recovery_required"):
                await recovered.schedule(
                    row["id"], row["version"], tool_id="registry", request_id="bypass"
                )
            # Expiry alone cannot clear uncertain publication or a lost clock sample.
            clock[0] += timedelta(seconds=20)
            with pytest.raises(ModelError, match="recovery_required"):
                await recovered.claim("unsafe-new-lease", lease_seconds=10)
            assert (await recovered.backlog())["publication_attempts_pending"] == 1
            # This explicit host assertion covers the interrupted attempt. It
            # may precede a later, healthy current clock without causing rollback.
            floor = clock[0] - timedelta(seconds=1)
            assert await recovered.recover_publication(lease, trusted_clock_at=floor) == "recovered"
            next_lease = await recovered.claim("recovered", lease_seconds=10)
            assert next_lease["task"]["token"] != lease["task"]["token"]
            assert "publication_attempt" not in next_lease["task"]
            with pytest.raises(ModelError, match="recovery_fenced"):
                await recovered.recover_publication(lease, trusted_clock_at=clock[0])
            assert await recovered.publish(next_lease, finding) == disposition
            with pytest.raises(ModelError, match="lease_fenced"):
                await recovered.publish(next_lease, finding)
            assert await recovered.claim("terminal", lease_seconds=10) is None

    asyncio.run(run())


@pytest.mark.parametrize("disposition", ["supported", "unknown"])
def test_concurrent_duplicate_publication_consumes_only_one_attempt(store, tmp_path, disposition):
    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await base.setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            lease = await queue.claim("worker", lease_seconds=10)
            finding = (
                await tool.verify(row)
                if disposition == "supported"
                else VerificationFinding("unknown")
            )
            other = restart(queue, clock)
            results = await asyncio.gather(
                queue.publish(lease, finding), other.publish(lease, finding), return_exceptions=True
            )
            assert results.count(disposition) == 1
            assert sum(isinstance(result, ModelError) for result in results) == 1
            current = await engine.repository.admission_record(scope, row["id"])
            assert current["version"] == row["version"] + (disposition == "supported")
            async with engine.repository.unit_of_work() as uow:
                task = await uow.derived_get(scope, KIND, identity)
                assert task["state"] == "completed"
                assert task["publication_attempt"]["state"] == "completed"

    asyncio.run(run())


def test_crash_after_marker_survives_actual_backup_and_erasure(store, tmp_path, monkeypatch):
    from test_purge_restore import backup_copy

    async def run():
        async with store() as (engine, kernel, scope, clock):
            queue, identity, row, event, tool = await base.setup(engine, scope, clock, tmp_path)
            lease = await queue.claim("worker", lease_seconds=10)
            begin = queue._begin_publication

            async def interrupt_after_durable_marker(*args):
                await begin(*args)
                raise RuntimeError("process interrupted after durable marker")

            monkeypatch.setattr(queue, "_begin_publication", interrupt_after_durable_marker)
            with pytest.raises(RuntimeError, match="interrupted"):
                await queue.publish(lease, VerificationFinding("unknown"))
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                clock[0] += timedelta(seconds=11)
                restored = restart(queue, clock, repository=backup)
                with pytest.raises(ModelError, match="recovery_required"):
                    await restored.claim("restored", lease_seconds=10)
            assert (await engine.repository.admission_record(scope, row["id"]))["version"] == 1
            await kernel.forget(ForgetRequest(scope, (event.id,), mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, KIND, identity) == {"state": "erased"}
            assert (await restart(queue, clock).backlog())["publication_attempts_pending"] == 0

    asyncio.run(run())


@pytest.mark.parametrize("after_write", [False, True])
def test_marker_write_failure_never_starts_publisher(store, tmp_path, monkeypatch, after_write):
    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await base.setup(
                engine, scope, clock, tmp_path, value="Hangzhou"
            )
            lease = await queue.claim("worker", lease_seconds=10)
            finding = await tool.verify(row)
            async with engine.repository.unit_of_work() as uow:
                cls = type(uow)
            put = cls.derived_put

            async def broken_mark(uow, target, kind, key, payload):
                if (
                    kind == KIND
                    and payload.get("publication_attempt", {}).get("state") == "pending"
                ):
                    if after_write:
                        await put(uow, target, kind, key, payload)
                    raise RuntimeError("attempt marker write failed")
                await put(uow, target, kind, key, payload)

            monkeypatch.setattr(cls, "derived_put", broken_mark)
            with pytest.raises(RuntimeError, match="marker write"):
                await queue.publish(lease, finding)
            current = await engine.repository.admission_record(scope, row["id"])
            assert current["version"] == row["version"]
            async with engine.repository.unit_of_work() as uow:
                assert "publication_attempt" not in await uow.derived_get(scope, KIND, identity)
            monkeypatch.setattr(cls, "derived_put", put)
            assert await queue.publish(lease, finding) == "supported"

    asyncio.run(run())


def test_run_once_keeps_attempt_deadline_when_checkpoint_fails(store, tmp_path, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, _, _, _ = await base.setup(engine, scope, clock, tmp_path)
            barrier, calls = queue._clock_barrier, []

            async def fail_publisher(*_):
                raise RuntimeError("publisher interrupted")

            async def fail_checkpoint(sample):
                calls.append(True)
                if len(calls) == 2:
                    raise RuntimeError("checkpoint unavailable")
                return await barrier(sample)

            monkeypatch.setattr(queue, "_publish", fail_publisher)
            monkeypatch.setattr(queue, "_clock_barrier", fail_checkpoint)
            with pytest.raises(ModelError, match="recovery_required"):
                await queue.run_once("worker", lease_seconds=10)
            async with engine.repository.unit_of_work() as uow:
                task = await uow.derived_get(scope, KIND, identity)
                assert task["state"] == "running" and task["token"]
                assert task["lease_until"] == (clock[0] + timedelta(seconds=10)).isoformat()
                assert task["publication_attempt"]["state"] == "pending"
            clock[0] += timedelta(seconds=3)
            assert await restart(queue, clock).claim("not-retry", lease_seconds=10) is None
            clock[0] += timedelta(seconds=8)
            with pytest.raises(ModelError, match="recovery_required"):
                await restart(queue, clock).claim("not-reclaimable", lease_seconds=10)

    asyncio.run(run())


@pytest.mark.parametrize("changed", ["authority", "tool", "candidate", "scope", "early_clock"])
def test_explicit_recovery_preserves_current_guards(store, tmp_path, changed):
    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, row, _, tool = await base.setup(engine, scope, clock, tmp_path)
            lease = await queue.claim("worker", lease_seconds=10)
            await queue._begin_publication(lease, queue.clock)
            clock[0] += timedelta(seconds=11)
            floor = clock[0]
            if changed == "authority":

                async def denied(*_):
                    return False

                queue.authorize = denied
            elif changed == "tool":
                tool.spec = replace(tool.spec, version="changed")
            elif changed == "candidate":
                async with engine.repository.unit_of_work() as uow:
                    await uow.save_admission_record(
                        scope, row["id"], row["event_id"], row["slot_key"], row["payload"], 1
                    )
            elif changed == "scope":
                queue = restart(queue, clock, scope=replace(scope, user_id="other"))
            else:
                floor -= timedelta(seconds=2)
            with pytest.raises((ModelError, ValueError)):
                await queue.recover_publication(lease, trusted_clock_at=floor)
            async with engine.repository.unit_of_work() as uow:
                task = await uow.derived_get(scope, KIND, identity)
                assert task["publication_attempt"]["state"] == "pending"

    asyncio.run(run())


def test_failed_recovery_write_leaves_durable_pending_marker(store, tmp_path, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            queue, identity, _, _, _ = await base.setup(engine, scope, clock, tmp_path)
            lease = await queue.claim("worker", lease_seconds=10)

            async def interrupted(*_):
                raise RuntimeError("interrupted publisher")

            async def recovery_unavailable(*_):
                raise RuntimeError("recovery write unavailable")

            monkeypatch.setattr(queue, "_publish", interrupted)
            monkeypatch.setattr(queue, "_fence_publication_attempt", recovery_unavailable)
            with pytest.raises(ModelError, match="commit_fenced_recovery_required"):
                await queue.publish(lease, VerificationFinding("unknown"))
            async with engine.repository.unit_of_work() as uow:
                task = await uow.derived_get(scope, KIND, identity)
                assert task["publication_attempt"]["state"] == "pending"
            clock[0] += timedelta(seconds=11)
            recovered = restart(queue, clock)
            with pytest.raises(ModelError, match="recovery_required"):
                await recovered.claim("blocked", lease_seconds=10)
            result = await recovered.recover_publication(lease, trusted_clock_at=clock[0])
            assert result == "recovered"
            new = await recovered.claim("new-lease", lease_seconds=10)
            assert await recovered.publish(new, VerificationFinding("unknown")) == "unknown"

    asyncio.run(run())
