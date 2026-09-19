"""Acceptance tests for the bounded consolidation worker runtime."""

import asyncio

import pytest

from agent_memory.domain import MemoryScope
from agent_memory.sqlite_worker_queue import SQLiteWorkerQueue
from agent_memory.worker_runtime import BoundedWorker
from agent_memory.worker_tasks import WorkerLimits, WorkerTaskStatus, WorkerQueueError


SCOPE = MemoryScope("tenant-a", session_id="session-a")


def test_sqlite_queue_is_idempotent_and_persists_checkpoint(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        limits = WorkerLimits(max_pending_per_scope=4, max_pending_global=8)
        first = SQLiteWorkerQueue(database, limits=limits)
        await first.initialize()
        task_id = await first.enqueue("same-key", SCOPE, "memory.consolidate", {"event_id": "event-1"})
        duplicate_id = await first.enqueue("same-key", SCOPE, "memory.consolidate", {"event_id": "other"})
        assert duplicate_id == task_id

        lease = await first.claim("worker-a", lease_seconds=5)
        assert lease is not None
        assert lease.task.status is WorkerTaskStatus.LEASED
        await first.checkpoint(lease, {"phase": "episode", "offset": 2})

        restarted = SQLiteWorkerQueue(database, limits=limits)
        await restarted.initialize()
        second_lease = await restarted.claim("worker-b", lease_seconds=5)
        assert second_lease is None
        await first.complete(lease)
        stats = await restarted.stats()
        assert stats.completed == 1
        assert stats.pending == 0

    asyncio.run(scenario())


def test_queue_capacity_is_scoped_and_failed_single_attempt_becomes_dead(tmp_path) -> None:
    async def scenario() -> None:
        queue = SQLiteWorkerQueue(
            tmp_path / "memory.db",
            limits=WorkerLimits(
                max_pending_per_scope=1,
                max_pending_global=2,
                max_attempts=1,
            ),
        )
        await queue.initialize()
        await queue.enqueue("first", SCOPE, "memory.consolidate", {})
        with pytest.raises(WorkerQueueError, match="scope worker capacity"):
            await queue.enqueue("second", SCOPE, "memory.consolidate", {})

        lease = await queue.claim("worker-a", lease_seconds=5)
        assert lease is not None
        await queue.fail(lease, RuntimeError("bad payload"))
        stats = await queue.stats()
        assert stats.dead == 1
        assert stats.pending == 0

    asyncio.run(scenario())


def test_bounded_worker_completes_success_and_records_failure(tmp_path) -> None:
    async def scenario() -> None:
        queue = SQLiteWorkerQueue(
            tmp_path / "memory.db",
            limits=WorkerLimits(max_batch_size=2, max_concurrency=2),
        )
        await queue.initialize()
        await queue.enqueue("success", SCOPE, "success", {})
        await queue.enqueue("failure", SCOPE, "failure", {})
        seen: list[str] = []

        async def success(task, checkpoint):
            seen.append(task.task_key)
            await checkpoint({"done": True})

        async def failure(task, checkpoint):
            raise ValueError("expected failure")

        worker = BoundedWorker(
            queue,
            {"success": success, "failure": failure},
            worker_id="worker-a",
        )
        result = await worker.run_batch()
        assert result.claimed == 2
        assert result.completed == 1
        assert result.failed == 1
        assert seen == ["success"] or seen == ["failure"]
        stats = await queue.stats()
        assert stats.completed == 1
        assert stats.pending == 1

    asyncio.run(scenario())


def test_worker_pause_and_stop_are_bounded(tmp_path) -> None:
    async def scenario() -> None:
        queue = SQLiteWorkerQueue(tmp_path / "memory.db")
        await queue.initialize()
        await queue.enqueue("paused", SCOPE, "noop", {})
        worker = BoundedWorker(queue, {"noop": lambda task, checkpoint: asyncio.sleep(0)}, worker_id="worker-a")
        worker.pause()
        paused_result = await worker.run_batch()
        assert paused_result.idle is True
        assert (await queue.stats()).pending == 1

        worker.resume()
        stop = asyncio.Event()
        stop.set()
        await worker.run_forever(stop, idle_seconds=0.01)
        result = await worker.run_batch()
        assert result.completed == 1

    asyncio.run(scenario())
