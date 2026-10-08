"""Independent refresh worker; the parent sends a real SIGKILL at SQL boundaries."""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import test_atom_admission as base

from agent_memory.derived import ObservationService
from agent_memory.domain import MemoryScope
from agent_memory.operations.refresh_demand import ObservationRefreshProcessor, RefreshDemandQueue
from agent_memory.operations.refresh_host import RefreshHost
from agent_memory.operations.refresh_policy import RefreshLimits
from agent_memory.serialization import to_jsonable


def emit(event, **values):
    print(json.dumps(dict(event=event, **to_jsonable(values))), flush=True)


async def pause():
    emit("boundary")
    await asyncio.Event().wait()


async def gate():
    # stdin is only a deterministic test barrier, never a scheduler notification.
    command = await asyncio.to_thread(sys.stdin.readline)
    if command != "continue\n":
        raise RuntimeError("parent did not release process barrier")


async def main(config):
    if config["backend"] == "postgres":
        from agent_memory_postgres.repository import PostgresMemoryRepository

        repository = PostgresMemoryRepository.from_dsn(config["database"], max_size=2)
    else:
        from agent_memory.sqlite import SQLiteMemoryRepository

        repository = SQLiteMemoryRepository(config["database"])
    await repository.initialize()
    scope = MemoryScope(**config["scope"])
    now = datetime.fromisoformat(config["clock"])
    service = ObservationService(repository, scope, base.POLICY, clock=lambda: now)
    queue = RefreshDemandQueue(
        (ObservationRefreshProcessor(service),),
        clock=lambda: now,
        limits=RefreshLimits(**config.get("limits", {})),
    )
    try:
        mode = config["mode"]
        if mode == "host_clock_probe":
            # Let the actual host handle initialize() at a rolled-back wall clock.
            # No in-memory high water is supplied to this fresh interpreter.
            host = RefreshHost(queue, worker_id="restarted-clock", poll_seconds=0.02)
            original_run_once = host.run_once

            async def report_poll():
                result = await original_run_once()
                emit("health", health=host.health, result=result)
                host.stop()
                return result

            host.run_once = report_poll
            await host.run()
            return
        await queue.initialize()
        if mode == "after_initialize":
            await pause()
        if mode == "drain":
            leases = []
            for _ in range(8):
                lease = await queue.claim("restarted-process", lease_seconds=60)
                if lease is None:
                    emit("drained", leases=leases)
                    return
                await queue.apply(lease.task)
                await queue.complete(lease)
                leases.append(lease)
            raise AssertionError("finite test workload did not drain")
        if mode == "claim_gate":
            emit("waiting")
            await gate()
            lease = await queue.claim(config["worker"], lease_seconds=60)
            emit("claimed", lease=lease)
            return
        if mode == "host_without_notification":
            host = RefreshHost(queue, worker_id="poll-only", poll_seconds=0.02)
            original_claim, original_complete = queue.claim, queue.complete
            reported_idle = False

            async def claim(*args, **kwargs):
                nonlocal reported_idle
                lease = await original_claim(*args, **kwargs)
                if lease is None and not reported_idle:
                    reported_idle = True
                    emit("idle")
                return lease

            async def complete(lease):
                await original_complete(lease)
                emit("completed", lease=lease)
                host.stop()

            queue.claim, queue.complete = claim, complete
            await host.run()
            return

        lease = await queue.claim("crashed-process", lease_seconds=5)
        if lease is None:
            raise AssertionError("crash fixture did not claim its work")
        emit("claimed", lease=lease)
        if mode == "after_claim":
            await pause()
        await gate()
        unit_type = type(repository.unit_of_work())
        original_put, original_exit = unit_type.derived_put, unit_type.__aexit__

        async def put(uow, item_scope, kind, key, row):
            await original_put(uow, item_scope, kind, key, row)
            if kind == "refresh_publication":
                # Mark this exact content/head/coverage publication transaction.
                uow._refresh_publication_boundary = True

        async def at_commit(uow, *args):
            publishing = args[0] is None and getattr(uow, "_refresh_publication_boundary", False)
            if publishing and mode == "before_publication_commit":
                await pause()
            result = await original_exit(uow, *args)
            if publishing and mode == "after_publication_commit":
                await pause()
            return result

        unit_type.derived_put, unit_type.__aexit__ = put, at_commit
        await queue.apply(lease.task)
        raise AssertionError("publication did not reach requested COMMIT boundary")
    finally:
        close = getattr(repository, "close", None)
        if close is not None:
            await close()


if __name__ == "__main__":
    asyncio.run(main(json.loads(Path(sys.argv[1]).read_text())))
