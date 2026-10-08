"""Q7-29: actual process death, fresh-process recovery, and shared SQL admission."""

import asyncio
import json
import os
import signal
import sys
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import test_atom_admission as base
from test_derived_observations import setup

from agent_memory.derived import FacetDefinition, ObservationService
from agent_memory.domain import MemoryScope
from agent_memory.operations.refresh_demand import (
    ObservationRefreshProcessor,
    RefreshDemandQueue,
    record_dirty,
)
from agent_memory.operations.refresh_policy import RefreshLimits, RefreshPolicy
from agent_memory.operations.worker_tasks import (
    WorkerLease,
    WorkerQueueError,
    WorkerTask,
    WorkerTaskStatus,
)
from agent_memory.serialization import to_jsonable

store = base.store
CHILD = Path(__file__).parent / "fixtures" / "refresh_crash_child.py"
pytestmark = pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="requires real SIGKILL")


def scheduler(service, clock, **kwargs):
    return RefreshDemandQueue(
        (ObservationRefreshProcessor(service),), clock=lambda: clock[0], **kwargs
    )


def restore_lease(value):
    task = dict(value["task"])
    task["scope"] = MemoryScope(**task["scope"])
    task["status"] = WorkerTaskStatus(task["status"])
    for key in ("next_attempt_at", "created_at", "updated_at", "lease_expires_at"):
        if task[key] is not None:
            task[key] = datetime.fromisoformat(task[key])
    return WorkerLease(WorkerTask(**task), value["token"])


@asynccontextmanager
async def child(repository, scope, clock, tmp_path, mode, **extra):
    config = dict(
        backend="postgres" if hasattr(repository, "pool") else "sqlite",
        database=(
            repository.pool.conninfo if hasattr(repository, "pool") else str(repository._path)
        ),
        scope=to_jsonable(scope),
        clock=clock[0].isoformat(),
        mode=mode,
        **extra,
    )
    path = tmp_path / f"refresh-child-{uuid4().hex}.json"
    path.write_text(json.dumps(config))
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(CHILD),
        str(path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parent)},
    )
    try:
        yield process
    finally:
        if process.returncode is None:
            process.kill()
        await asyncio.wait_for(process.wait(), timeout=10)
        process.stdin.close()
        await process.stdin.wait_closed()


async def event(process, expected):
    line = await asyncio.wait_for(process.stdout.readline(), timeout=30)
    if not line:
        error = await asyncio.wait_for(process.stderr.read(), timeout=5)
        pytest.fail(f"refresh process exited before {expected}: {error.decode()}")
    value = json.loads(line)
    assert value["event"] == expected, value
    return value


async def release(process):
    process.stdin.write(b"continue\n")
    await process.stdin.drain()


async def kill(process):
    process.kill()
    assert await asyncio.wait_for(process.wait(), timeout=10) == -signal.SIGKILL


async def clean_exit(process):
    assert await asyncio.wait_for(process.wait(), timeout=10) == 0, (
        await process.stderr.read()
    ).decode()


async def records(repository, scope, kind):
    async with repository.unit_of_work() as uow:
        return [row["payload"] for row in await uow.derived_records(scope, kind)]


async def drain(repository, scope, clock, tmp_path, **extra):
    async with child(repository, scope, clock, tmp_path, "drain", **extra) as process:
        result = await event(process, "drained")
        await clean_exit(process)
        return result["leases"]


@pytest.mark.parametrize("phase", ["before_publication_commit", "after_publication_commit"])
@pytest.mark.parametrize("successor", [False, True], ids=["single", "successor"])
def test_sigkill_at_publish_commit_recovers_exact_responsibility(store, tmp_path, phase, successor):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            repository = engine.repository
            service, _, _ = await setup(engine, kernel, scope, clock)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            first = await queue.request("language", dedupe_key="first", actor="alice")
            required = set(first["target"]["required_frontier"]["units"])
            receipts = [first]
            async with child(repository, scope, clock, tmp_path, phase) as process:
                old = restore_lease((await event(process, "claimed"))["lease"])
                execution_id = old.task.payload["refresh_execution"]
                execution = (await records(repository, scope, "refresh_execution"))[0]
                assert set(execution["claimed"]) == required
                if successor:
                    # Even identical census content cannot discharge an obligation
                    # arriving after immutable claim. The hook is the real UoW API.
                    async with repository.unit_of_work() as uow:
                        definition = await uow.derived_get(scope, "definition", "language")
                        definition["dirty"] = True
                        await uow.derived_put(scope, "definition", "language", definition)
                        dirty = await record_dirty(
                            uow, scope, definition, at=clock[0], reason="another_trigger"
                        )
                    assert required < set(dirty["requested"])
                    receipts.append(
                        await queue.request("language", dedupe_key="later", actor="alice")
                    )
                requested = set(receipts[-1]["target"]["required_frontier"]["units"])
                await release(process)
                await event(process, "boundary")
                await kill(process)

            committed = phase == "after_publication_commit"
            publications = await records(repository, scope, "refresh_publication")
            revisions = await records(repository, scope, "revision")
            assert len(publications) == len(revisions) == int(committed)
            if committed:
                assert publications[0]["id"] == execution_id
                assert set(publications[0]["claimed"]) == required
            committed_snapshot = json.dumps(publications, sort_keys=True)
            demand = (await records(repository, scope, "refresh_demand"))[0]
            assert set(demand["requested"]) == requested - (required if committed else set())
            assert (await queue.status(first["target_id"], actor="alice"))["complete"] == committed
            if successor:
                status = await queue.status(receipts[-1]["target_id"], actor="alice")
                assert not status["complete"]
                assert demand["due_at"] is not None
            async with repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                assert definition["dirty"] == (successor or not committed)
                usage = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(),
                    tenant_id=scope.tenant_id,
                    instance_key=demand["instance_key"],
                )
                assert usage["global_running"] == int(not committed)

            # Only this new interpreter performs recovery: it receives no lost
            # notification, old lease, in-memory receipt, or completion callback.
            clock[0] += timedelta(seconds=6)
            recovered = await drain(repository, scope, clock, tmp_path)
            assert len(recovered) == int(successor or not committed)
            for receipt in receipts:
                status = await queue.status(receipt["target_id"], actor="alice")
                fixed = receipt["target"]["required_frontier"]
                assert status["required"] == fixed
                assert set(status["covered"]) == set(fixed["units"])
                assert status["complete"] and status["state"] == "completed"
            publications = await records(repository, scope, "refresh_publication")
            assert len(publications) == 1 + int(committed and successor)
            assert set().union(*(set(row["claimed"]) for row in publications)) == requested
            assert sum(len(row["claimed"]) for row in publications) == len(requested)
            if committed:
                original = [row for row in publications if row["id"] == execution_id]
                assert json.dumps(original, sort_keys=True) == committed_snapshot
            elif successor:
                assert execution_id not in {row["id"] for row in publications}
            assert len(await records(repository, scope, "revision")) == len(publications)
            demand = (await records(repository, scope, "refresh_demand"))[0]
            assert demand["requested"] == [] and demand["active_execution"] is None
            assert demand["status"] == "idle"
            async with repository.unit_of_work() as uow:
                usage = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(),
                    tenant_id=scope.tenant_id,
                    instance_key=demand["instance_key"],
                )
                assert usage["global_running"] == 0
            view = await service.read("language", actor="alice")
            assert view["body"]["blocks"][0]["value"] == "zh-CN"
            # Another process restart is inert, including the commit-before-ack case.
            assert await drain(repository, scope, clock, tmp_path) == []
            assert await records(repository, scope, "refresh_publication") == publications

    asyncio.run(run())


def test_killed_owner_fence_cannot_publish_heartbeat_fail_or_complete_after_reclaim(
    store, tmp_path
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            receipt = await queue.request("language", dedupe_key="fixed", actor="alice")
            async with child(engine.repository, scope, clock, tmp_path, "after_claim") as process:
                old = restore_lease((await event(process, "claimed"))["lease"])
                await event(process, "boundary")
                snapshot = await service.snapshot(old.task)
                prepared = service.prepare(snapshot)
                await kill(process)
            assert await queue.claim("too-early", lease_seconds=60) is None
            clock[0] += timedelta(seconds=6)
            fresh = scheduler(service, clock)
            current = await fresh.claim("reclaimed", lease_seconds=60)
            assert current is not None and current.token != old.token
            assert current.task.payload["generation"] > old.task.payload["generation"]
            for operation in (
                lambda: service.publish(old.task, snapshot, prepared),
                lambda: fresh.heartbeat(old, lease_seconds=60),
                lambda: fresh.fail(old, RuntimeError("late failure")),
                lambda: fresh.complete(old),
            ):
                with pytest.raises(WorkerQueueError, match="stale"):
                    await operation()
            assert not (await fresh.status(receipt["target_id"], actor="alice"))["complete"]
            assert await records(engine.repository, scope, "refresh_publication") == []
            await fresh.apply(current.task)
            await fresh.complete(current)
            assert (await fresh.status(receipt["target_id"], actor="alice"))["complete"]
            assert len(await records(engine.repository, scope, "refresh_publication")) == 1

    asyncio.run(run())


def test_independent_running_host_polls_durable_work_without_notification(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(mode="on_demand"))
            async with child(
                engine.repository, scope, clock, tmp_path, "host_without_notification"
            ) as process:
                await event(process, "idle")
                receipt = await queue.request("language", dedupe_key="no-hint", actor="alice")
                await event(process, "completed")
                await clean_exit(process)
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            assert len(await records(engine.repository, scope, "refresh_publication")) == 1

    asyncio.run(run())


def test_future_due_survives_host_sigkill_and_runs_only_after_durable_deadline(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            repository = engine.repository
            service, _, _ = await setup(engine, kernel, scope, clock)
            queue = scheduler(service, clock)
            pending = await queue.configure(
                "language", RefreshPolicy(debounce_seconds=20, max_wait_seconds=30)
            )
            assert pending["due_at"] == (clock[0] + timedelta(seconds=20)).isoformat()
            async with child(
                repository, scope, clock, tmp_path, "host_without_notification"
            ) as process:
                await event(process, "idle")
                await kill(process)
            assert await records(repository, scope, "refresh_demand") == [pending]
            clock[0] += timedelta(seconds=19)
            assert await drain(repository, scope, clock, tmp_path) == []
            assert await records(repository, scope, "refresh_demand") == [pending]
            assert await records(repository, scope, "refresh_publication") == []
            async with repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, "definition", "language"))["dirty"]
            clock[0] += timedelta(seconds=1)
            assert len(await drain(repository, scope, clock, tmp_path)) == 1
            publications = await records(repository, scope, "refresh_publication")
            assert len(publications) == 1
            assert set(publications[0]["claimed"]) == set(pending["requested"])
            assert (await records(repository, scope, "refresh_demand"))[0]["requested"] == []
            assert await drain(repository, scope, clock, tmp_path) == []

    asyncio.run(run())


def test_fresh_process_host_rejects_backward_clock_until_durable_high_water_catches_up(
    store, tmp_path
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            repository = engine.repository
            service, _, _ = await setup(engine, kernel, scope, clock)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            receipt = await queue.request("language", dedupe_key="clock-target", actor="alice")
            pending = (await records(repository, scope, "refresh_demand"))[0]
            clock[0] += timedelta(seconds=20)
            high_water = clock[0]
            async with child(repository, scope, clock, tmp_path, "after_initialize") as process:
                await event(process, "boundary")
                await kill(process)

            # Both old clocks are already past due_at: an empty due selection
            # cannot accidentally make this guard test pass. Each probe has a
            # fresh interpreter and a fresh host's empty in-memory clock history.
            for seconds_behind in (10, 1):
                clock[0] = high_water - timedelta(seconds=seconds_behind)
                assert datetime.fromisoformat(pending["due_at"]) < clock[0]
                async with child(repository, scope, clock, tmp_path, "host_clock_probe") as process:
                    probe = await event(process, "health")
                    await clean_exit(process)
                assert probe["health"] == dict(
                    state="degraded",
                    clock_healthy=False,
                    last_poll_at=clock[0].isoformat(),
                    reason="refresh_clock_discontinuity",
                )
                assert probe["result"] is None
                assert await records(repository, scope, "refresh_execution") == []
                assert await records(repository, scope, "refresh_publication") == []
                assert await records(repository, scope, "refresh_demand") == [pending]
                status = await queue.status(receipt["target_id"], actor="alice")
                assert not status["complete"] and status["covered"] == []
                async with repository.unit_of_work() as uow:
                    usage = await uow.refresh_scheduler_usage(
                        now=clock[0].isoformat(),
                        tenant_id=scope.tenant_id,
                        instance_key=pending["instance_key"],
                    )
                assert usage["global_running"] == 0

            clock[0] = high_water
            async with child(repository, scope, clock, tmp_path, "host_clock_probe") as process:
                probe = await event(process, "health")
                await clean_exit(process)
            assert probe["health"]["state"] == "running"
            assert probe["health"]["clock_healthy"] is True
            assert probe["health"]["reason"] is None
            assert probe["result"]["claimed"] == probe["result"]["completed"] == 1
            assert probe["result"]["failed"] == 0
            status = await queue.status(receipt["target_id"], actor="alice")
            assert status["complete"]
            assert set(status["covered"]) == set(receipt["target"]["required_frontier"]["units"])
            assert len(await records(repository, scope, "refresh_publication")) == 1
            assert await drain(repository, scope, clock, tmp_path) == []

    asyncio.run(run())


def test_two_independent_scheduler_processes_compete_for_last_shared_quota(store, tmp_path):
    async def run():
        async with store() as (engine, _, scope, clock):
            repository = engine.repository
            scopes = (scope, replace(scope, tenant_id="other-tenant"))
            limits = RefreshLimits(global_running=1, tenant_running=1)
            queues, receipts = [], []
            for item_scope in scopes:
                service = ObservationService(
                    repository, item_scope, base.POLICY, clock=lambda: clock[0]
                )
                await service.register(FacetDefinition("language", "alice", readers=("alice",)))
                queue = scheduler(service, clock, limits=limits)
                await queue.configure("language", RefreshPolicy())
                receipts.append(await queue.request("language", dedupe_key="race", actor="alice"))
                queues.append(queue)
            async with (
                child(
                    repository,
                    scopes[0],
                    clock,
                    tmp_path,
                    "claim_gate",
                    worker="process-one",
                    limits=limits.payload(),
                ) as one,
                child(
                    repository,
                    scopes[1],
                    clock,
                    tmp_path,
                    "claim_gate",
                    worker="process-two",
                    limits=limits.payload(),
                ) as two,
            ):
                await asyncio.gather(event(one, "waiting"), event(two, "waiting"))
                await asyncio.gather(release(one), release(two))
                result = await asyncio.gather(event(one, "claimed"), event(two, "claimed"))
                await asyncio.gather(clean_exit(one), clean_exit(two))
            assert sum(row["lease"] is not None for row in result) == 1
            winner = next(index for index, row in enumerate(result) if row["lease"] is not None)
            loser = 1 - winner
            async with repository.unit_of_work() as uow:
                usage = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(), tenant_id=scope.tenant_id, instance_key="unused"
                )
            assert usage["global_running"] == 1
            loser_status = await queues[loser].status(receipts[loser]["target_id"], actor="alice")
            assert not loser_status["complete"] and loser_status["state"] == "deferred"
            assert loser_status["reason"] == "refresh_running_quota"
            lease = restore_lease(result[winner]["lease"])
            await queues[winner].apply(lease.task)
            await queues[winner].complete(lease)
            clock[0] += timedelta(seconds=3)
            recovered = await drain(
                repository, scopes[loser], clock, tmp_path, limits=limits.payload()
            )
            assert len(recovered) == 1
            for queue, receipt in zip(queues, receipts, strict=True):
                assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]

    asyncio.run(run())
