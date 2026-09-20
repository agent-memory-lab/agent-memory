"""T39 fault, race, isolation, atomicity, and resource acceptance tests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from time import perf_counter
import tracemalloc

import pytest

from agent_memory import (
    ArtifactStatus,
    ConsolidationResult,
    Episode,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryQuery,
    MemoryScope,
    Provenance,
    build_local_kernel,
)
from agent_memory.sqlite_worker_queue import SQLiteWorkerQueue
from agent_memory.worker_runtime import BoundedWorker
from agent_memory.worker_tasks import WorkerLimits, WorkerTaskStatus


NOW = datetime(2026, 9, 20, tzinfo=timezone.utc)
SCOPE = MemoryScope("tenant", session_id="session")


def test_duplicate_enqueue_crash_recovery_locking_and_dead_letter(tmp_path, monkeypatch):
    async def scenario():
        from agent_memory import sqlite_worker_queue as queue_module

        clock = [NOW]
        monkeypatch.setattr(queue_module, "_now", lambda: clock[0])
        path = tmp_path / "worker.db"
        limits = WorkerLimits(max_attempts=2, max_pending_per_scope=4)
        queues = tuple(SQLiteWorkerQueue(path, limits=limits) for _ in range(4))
        await queues[0].initialize()

        task_ids = await asyncio.gather(
            *(
                queue.enqueue(
                    "same-task", SCOPE, "memory.consolidate", {"event_id": "event-1"}
                )
                for queue in queues
                for _ in range(4)
            )
        )
        assert len(set(task_ids)) == 1

        first = await queues[0].claim("worker-a", lease_seconds=5)
        assert first is not None
        await queues[0].checkpoint(first, {"phase": "episode", "offset": 1})

        clock[0] += timedelta(seconds=6)
        recovered = await queues[1].claim("worker-b", lease_seconds=5)
        assert recovered is not None
        assert recovered.task.id == first.task.id
        assert recovered.task.attempts == 2
        assert recovered.task.checkpoint == {"phase": "episode", "offset": 1}

        await queues[1].fail(recovered, RuntimeError("repeatable failure"))
        stats = await queues[2].stats()
        assert stats.dead == 1
        assert stats.pending == stats.leased == 0

    asyncio.run(scenario())


def test_forget_cancels_running_task_and_prevents_regeneration(tmp_path):
    async def scenario():
        queue = SQLiteWorkerQueue(tmp_path / "worker.db")
        await queue.initialize()
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=queue)
        await kernel.initialize()
        source = MemoryEvent(SCOPE, "tool.completed", "source evidence", id="source")
        await kernel.ingest_event(source)

        started = asyncio.Event()
        release = asyncio.Event()

        async def handler(task, checkpoint):
            started.set()
            await release.wait()
            episode = Episode(
                id="late-episode",
                scope=task.scope,
                observation="late regeneration",
                action="should not commit",
                outcome="forgotten",
                lesson="forgotten evidence cannot regenerate memory",
                status=ArtifactStatus.CANDIDATE,
                provenance=Provenance(source_event_ids=(source.id,)),
            )
            await kernel.commit_consolidation(
                ConsolidationResult(episodes=(episode,))
            )

        worker = BoundedWorker(
            queue,
            {"memory.consolidate": handler},
            worker_id="worker-a",
            limits=WorkerLimits(max_batch_size=1, max_concurrency=1),
        )
        running = asyncio.create_task(worker.run_batch(max_tasks=1))
        await started.wait()
        await kernel.forget(
            ForgetRequest(SCOPE, memory_ids=(source.id,), mode=ForgetMode.ERASE)
        )
        release.set()
        result = await running

        assert result.cancelled == 1
        assert result.completed == result.failed == 0
        assert (await queue.stats()).cancelled == 1
        bundle = await kernel.retrieve(MemoryQuery(SCOPE, "late regeneration"))
        assert all(item.id != "late-episode" for item in bundle.episodes)

    asyncio.run(scenario())


def test_forget_and_evidence_validation_are_scope_isolated(tmp_path):
    async def scenario():
        queue = SQLiteWorkerQueue(tmp_path / "worker.db")
        await queue.initialize()
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=queue)
        await kernel.initialize()
        left = MemoryScope("tenant", session_id="left")
        right = MemoryScope("tenant", session_id="right")
        left_event = MemoryEvent(left, "event", "left", id="left-event")
        right_event = MemoryEvent(right, "event", "right", id="right-event")
        await kernel.ingest_event(left_event)
        await kernel.ingest_event(right_event)

        await kernel.forget(
            ForgetRequest(left, memory_ids=(left_event.id,), mode=ForgetMode.ERASE)
        )
        stats = await queue.stats()
        assert stats.cancelled == 1
        assert stats.pending == 1

        forged = Episode(
            id="forged",
            scope=left,
            observation="cross scope",
            action="copy evidence",
            outcome="forbidden",
            lesson="must fail closed",
            provenance=Provenance(source_event_ids=(right_event.id,)),
        )
        with pytest.raises(ValueError, match="source evidence"):
            await kernel.commit_consolidation(ConsolidationResult(episodes=(forged,)))

        remaining = await queue.claim("worker", lease_seconds=5)
        assert remaining is not None
        assert remaining.task.scope == right

    asyncio.run(scenario())


def test_consolidation_commit_is_idempotent_and_rolls_back_as_one_unit(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        source = MemoryEvent(SCOPE, "event", "source", id="source")
        await kernel.ingest_event(source)
        stable = Episode(
            id="stable",
            scope=SCOPE,
            observation="stable episode",
            action="commit",
            outcome="ok",
            lesson="idempotent",
            provenance=Provenance(source_event_ids=(source.id,)),
        )
        result = ConsolidationResult(episodes=(stable,))
        await kernel.commit_consolidation(result)
        await kernel.commit_consolidation(result)

        valid = Episode(
            id="must-rollback",
            scope=SCOPE,
            observation="should rollback",
            action="batch",
            outcome="partial",
            lesson="atomic",
            provenance=Provenance(source_event_ids=(source.id,)),
        )
        invalid = Episode(
            id="missing-source",
            scope=SCOPE,
            observation="invalid",
            action="batch",
            outcome="invalid",
            lesson="atomic",
            provenance=Provenance(source_event_ids=("missing",)),
        )
        with pytest.raises(ValueError, match="source evidence"):
            await kernel.commit_consolidation(
                ConsolidationResult(episodes=(valid, invalid))
            )

        bundle = await kernel.retrieve(MemoryQuery(SCOPE, "episode rollback stable"))
        episode_ids = {item.id for item in bundle.episodes}
        assert "stable" in episode_ids
        assert "must-rollback" not in episode_ids

    asyncio.run(scenario())


def test_fixed_load_hot_path_and_background_peak_are_bounded(tmp_path):
    async def scenario():
        queue = SQLiteWorkerQueue(
            tmp_path / "worker.db",
            limits=WorkerLimits(
                max_pending_per_scope=64,
                max_pending_global=64,
                max_batch_size=32,
                max_concurrency=4,
            ),
        )
        await queue.initialize()
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=queue)
        await kernel.initialize()

        tracemalloc.start()
        hot_started = perf_counter()
        for index in range(32):
            await kernel.ingest_event(
                MemoryEvent(SCOPE, "load", f"event-{index}", id=f"event-{index}")
            )
        hot_seconds = perf_counter() - hot_started
        _, hot_peak = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()

        async def noop(task, checkpoint):
            await checkpoint({"event_id": task.payload["event_id"]})

        worker = BoundedWorker(
            queue,
            {"memory.consolidate": noop},
            worker_id="resource-worker",
            limits=WorkerLimits(max_batch_size=32, max_concurrency=4),
        )
        background_started = perf_counter()
        result = await worker.run_batch(max_tasks=32)
        background_seconds = perf_counter() - background_started
        _, background_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert result.completed == 32
        assert hot_seconds > 0 and background_seconds > 0
        assert hot_peak < 16 * 1024 * 1024
        assert background_peak < 16 * 1024 * 1024

    asyncio.run(scenario())
