"""Publication time belongs to the durable lease, including final audit awaits."""

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta

import pytest
import test_source_omission_audit as audit

from agent_memory.operations.publication_batches import PublicationPolicy
from agent_memory.operations.retention import RetentionError
from agent_memory.operations.worker_tasks import WorkerQueueError

store = audit.store


@pytest.mark.parametrize("batched", [False, True])
@pytest.mark.parametrize("boundary", ["write", "audit"])
@pytest.mark.parametrize("change", ["lease", "queue_config", "handler_config"])
def test_final_completion_fences_lease_and_host_configuration(
    store, monkeypatch, batched, boundary, change
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, _, host, pipeline, queue, runner = await audit.durable_setup(
                engine, scope, clock,
                publication_policy=PublicationPolicy(1) if batched else None,
            )
            lease = await queue.claim("completion-worker", lease_seconds=30)
            handler = runner._handlers["memory.extract"]
            cls = type(engine.repository.unit_of_work())
            original = cls.retention_update
            written, changed = [], []

            def change_control():
                if changed:
                    return
                changed.append(True)
                if change == "lease":
                    clock[0] = datetime.fromisoformat(written[0]["lease_until"])
                elif change == "queue_config":
                    queue.configuration_sha256 = "f" * 64
                else:
                    pipeline.generator.version = "changed-after-publication"

            async def late(uow, item_scope, request_id, row):
                result = await original(uow, item_scope, request_id, row)
                if row["status"] == "completed":
                    written.append(row)
                    if boundary == "write":
                        change_control()
                return result

            def authorize(uow):
                if uow is not None and written and boundary == "audit":
                    change_control()

            host.callback = authorize
            monkeypatch.setattr(cls, "retention_update", late)
            with pytest.raises((RetentionError, WorkerQueueError, ValueError)):
                await handler(lease.task, lambda value: queue.checkpoint(lease, value))
            assert written and changed
            result = await queue.status("audit-request")
            assert result["status"] != "completed" and not result["l1_decided"]
            async with engine.repository.unit_of_work() as uow:
                head = await uow.retention_head_get(scope, "interpretation", "durable-audit")
                if batched:
                    assert head["payload"]["publication_closed"] is False
                else:
                    assert head is None
                    assert not await uow.list_admission_records(scope)
    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["write", "audit"])
def test_partial_batch_expiry_rolls_back_the_current_batch(store, monkeypatch, boundary):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, _, host, _, queue, runner = await audit.durable_setup(
                engine, scope, clock, publication_policy=PublicationPolicy(1)
            )
            lease = await queue.claim("partial-worker", lease_seconds=30)
            async with engine.repository.unit_of_work() as uow:
                before = await uow.retention_get(scope, "request", lease.task.id)
            handler = runner._handlers["memory.extract"]
            cls = type(engine.repository.unit_of_work())
            original = cls.retention_update
            written = []

            async def late(uow, item_scope, request_id, row):
                result = await original(uow, item_scope, request_id, row)
                if row.get("publication_manifest", {}).get("publications"):
                    written.append(row)
                    if boundary == "write":
                        clock[0] = datetime.fromisoformat(row["lease_until"])
                return result

            def authorize(uow):
                if uow is not None and written and boundary == "audit":
                    clock[0] = datetime.fromisoformat(written[0]["lease_until"])

            host.callback = authorize
            monkeypatch.setattr(cls, "retention_update", late)
            with pytest.raises(WorkerQueueError, match="lease"):
                await handler(lease.task, lambda value: queue.checkpoint(lease, value))
            assert written
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.list_admission_records(scope)
                assert await uow.retention_head_get(
                    scope, "interpretation", "durable-audit"
                ) is None
                row = await uow.retention_get(scope, "request", "audit-request")
                assert row["status"] == "running"
                assert row["publication_manifest"] == before["publication_manifest"]
    asyncio.run(run())


@pytest.mark.parametrize("batched", [False, True])
@pytest.mark.parametrize("display", ["original", "none", "future"])
def test_publication_honors_renewed_durable_lease_and_allows_late_acknowledgement(
    store, batched, display
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, _, _, _, queue, runner = await audit.durable_setup(
                engine, scope, clock,
                publication_policy=PublicationPolicy(1) if batched else None,
            )
            lease = await queue.claim("renewed-worker", lease_seconds=30)
            task = lease.task
            if display != "original":
                task = replace(task, lease_expires_at=(
                    None if display == "none" else clock[0] + timedelta(days=365)
                ))
            async with engine.repository.unit_of_work() as uow:
                row = await uow.retention_get(scope, "request", task.id)
                row["lease_until"] = (clock[0] + timedelta(seconds=60)).isoformat()
                await uow.retention_update(scope, task.id, row)
            clock[0] += timedelta(seconds=31)
            await runner._handlers["memory.extract"](
                task, lambda value: queue.checkpoint(lease, value)
            )
            clock[0] += timedelta(seconds=60)
            await queue.complete(lease)
            assert (await queue.status("audit-request"))["status"] == "completed"
    asyncio.run(run())
