"""Batch commit leases come from fenced durable rows, never task display fields."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
from test_question_shared_inputs_v7 import batch_setup
import test_project_admission_v7 as project

from agent_memory.operations.worker_tasks import WorkerQueueError

store = project.store


async def captured(engine, scope, clock):
    service, leases = await batch_setup(engine, scope, clock)
    tasks = [lease.task for lease in leases]
    snapshots = await service.snapshot_many(tasks)
    return service, leases, tasks, snapshots, [service.prepare(s) for s in snapshots]


async def no_publication(repository, scope):
    async with repository.unit_of_work() as uow:
        for kind in ("question_head", "question_content", "question_certificate",
                     "refresh_publication"):
            assert not await uow.derived_records(scope, kind)
        assert all(r["payload"]["status"] == "running"
                   for r in await uow.derived_records(scope, "refresh_execution"))


@pytest.mark.parametrize("batched", [False, True])
def test_heartbeat_renewed_durable_lease_accepts_original_task(store, batched):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases, tasks, snapshots, prepared = await captured(engine, scope, clock)
            old_expiry = tasks[0].lease_expires_at
            clock[0] += timedelta(seconds=20)
            for lease in leases:
                assert await svc.queue.heartbeat(lease, lease_seconds=30) is None
            clock[0] += timedelta(seconds=11)
            assert clock[0] > old_expiry
            if batched:
                await svc.publish_many(tasks, snapshots, prepared)
            else:
                for task, snapshot, output in zip(tasks, snapshots, prepared):
                    await svc.publish(task, snapshot, output)
            for lease in leases:
                await svc.queue.complete(lease)
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "refresh_publication")) == 4
    asyncio.run(run())


@pytest.mark.parametrize("supplied_expiry", ["original", "none", "future"])
@pytest.mark.parametrize("boundary", ["last_publication", "final_guard", "last_lease_read"])
def test_final_batch_expiry_uses_durable_bounds_and_rolls_back(
    store, monkeypatch, supplied_expiry, boundary
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases, tasks, snapshots, prepared = await captured(engine, scope, clock)
            expiry = tasks[0].lease_expires_at
            if supplied_expiry != "original":
                value = None if supplied_expiry == "none" else expiry + timedelta(days=365)
                tasks = [replace(task, lease_expires_at=value) for task in tasks]
            method = {"last_publication": "_publish_in_uow", "final_guard": "_guard_batch",
                      "last_lease_read": "_batch_lease_deadlines"}[boundary]
            original = getattr(svc, method)
            calls = []
            async def cross_boundary(*args, **kwargs):
                result = await original(*args, **kwargs)
                calls.append(True)
                if boundary != "last_publication" or len(calls) == len(tasks):
                    clock[0] = expiry
                return result
            monkeypatch.setattr(svc, method, cross_boundary)
            with pytest.raises(WorkerQueueError, match="stale"):
                await svc.publish_many(tasks, snapshots, prepared)
            await no_publication(engine.repository, scope)
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["job", "refresh_execution"])
@pytest.mark.parametrize("field", ["lease_until", "expires_at"])
def test_each_durable_limit_is_checked_after_last_await(store, monkeypatch, kind, field):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases, tasks, snapshots, prepared = await captured(engine, scope, clock)
            deadline = clock[0] + timedelta(seconds=1)
            original = svc._batch_lease_deadlines
            async def read_limits(uow, task_values):
                key = tasks[0].id if kind == "job" else tasks[0].payload["refresh_execution"]
                row = await uow.derived_get(scope, kind, key)
                row[field] = deadline.isoformat()
                await uow.derived_put(scope, kind, key, row)
                result = await original(uow, task_values)
                clock[0] = deadline
                return result
            monkeypatch.setattr(svc, "_batch_lease_deadlines", read_limits)
            with pytest.raises(WorkerQueueError, match="stale"):
                await svc.publish_many(tasks, snapshots, prepared)
            await no_publication(engine.repository, scope)
    asyncio.run(run())


@pytest.mark.parametrize("field", ["fence", "generation", "unit_id", "unit", "epoch",
                                   "adapter_key", "id", "status", "schema"])
def test_final_execution_coordinates_are_bound_to_authenticated_job(
    store, monkeypatch, field
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases, tasks, snapshots, prepared = await captured(engine, scope, clock)
            original = svc._batch_lease_deadlines
            async def corrupt_execution(uow, task_values):
                key = tasks[0].payload["refresh_execution"]
                get = uow.derived_get
                async def corrupted(item_scope, kind, identity):
                    row = await get(item_scope, kind, identity)
                    if kind == "refresh_execution" and identity == key:
                        row[field] = (row[field] + 1 if field in {"generation", "epoch"}
                                      else "forged")
                    return row
                uow.derived_get = corrupted
                try:
                    return await original(uow, task_values)
                finally:
                    uow.derived_get = get
            monkeypatch.setattr(svc, "_batch_lease_deadlines", corrupt_execution)
            with pytest.raises(WorkerQueueError, match="stale"):
                await svc.publish_many(tasks, snapshots, prepared)
            await no_publication(engine.repository, scope)
    asyncio.run(run())
