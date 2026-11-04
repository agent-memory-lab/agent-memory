"""Execution slots, lease handoffs, and structured worker shutdown."""

import asyncio
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
import sqlite3
import threading

import pytest

from agent_memory.domain import MemoryScope
from agent_memory.operations import sqlite_worker_queue as queue_module
from agent_memory.operations.sqlite_worker_queue import SQLiteWorkerQueue
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.operations.worker_tasks import WorkerLimits, WorkerQueueError


SCOPE = MemoryScope("worker-scheduling")
NOW = datetime(2026, 11, 4, tzinfo=timezone.utc)


def run(scenario):
    asyncio.run(asyncio.wait_for(scenario(), timeout=15))


def test_pending_work_is_not_leased_and_cancelled_work_never_starts(tmp_path):
    async def scenario():
        limits = WorkerLimits(max_batch_size=3, max_concurrency=1)
        queue = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        await queue.initialize()
        for key in ("first", "cancel", "last"):
            identity = await queue.enqueue(key, SCOPE, "work", {})
            if key == "cancel":
                cancelled_id = identity
        started, release = asyncio.Event(), asyncio.Event()
        seen = []

        async def handler(task, checkpoint):
            seen.append(task.task_key)
            if task.task_key == "first":
                started.set()
                await release.wait()

        worker = BoundedWorker(queue, {"work": handler}, worker_id="one", limits=limits)
        batch = asyncio.create_task(worker.run_batch())
        await started.wait()
        stats = await queue.stats()
        assert (stats.leased, stats.pending) == (1, 2)
        assert await queue.cancel(cancelled_id, SCOPE)
        release.set()
        result = await batch
        assert seen == ["first", "last"]
        assert (result.claimed, result.completed, result.cancelled) == (2, 2, 0)
        assert (await queue.stats()).cancelled == 1
        with closing(sqlite3.connect(tmp_path / "queue.db")) as connection:
            assert connection.execute(
                "SELECT attempts FROM agent_memory_worker_tasks WHERE id=?", (cancelled_id,)
            ).fetchone() == (0,)

    run(scenario)


def test_waiting_tasks_do_not_expire_and_execute_again_on_another_worker(tmp_path, monkeypatch):
    async def scenario():
        clock = [NOW]
        monkeypatch.setattr(queue_module, "_now", lambda: clock[0])
        limits = WorkerLimits(
            max_batch_size=3, max_concurrency=1, lease_seconds=5, task_timeout_seconds=4,
        )
        queue = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        other = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        await queue.initialize()
        for key in ("first", "second", "third"):
            await queue.enqueue(key, SCOPE, "work", {})
            clock[0] += timedelta(microseconds=1)
        starts = [asyncio.Event(), asyncio.Event()]
        releases = [asyncio.Event(), asyncio.Event()]
        seen = []

        async def slow(task, checkpoint):
            seen.append(("slow", task.task_key))
            index = {"first": 0, "second": 1}[task.task_key]
            starts[index].set()
            await releases[index].wait()

        async def fast(task, checkpoint):
            seen.append(("fast", task.task_key))

        worker = BoundedWorker(queue, {"work": slow}, worker_id="slow", limits=limits)
        competitor = BoundedWorker(other, {"work": fast}, worker_id="fast", limits=limits)
        batch = asyncio.create_task(worker.run_batch())
        await starts[0].wait()
        clock[0] = NOW + timedelta(seconds=4)
        releases[0].set()
        await starts[1].wait()
        clock[0] = NOW + timedelta(seconds=6)
        result = await competitor.run_batch()
        assert result.completed == 1
        releases[1].set()
        assert (await batch).completed == 2
        assert seen == [("slow", "first"), ("slow", "second"), ("fast", "third")]
        assert (await queue.stats()).completed == 3

    run(scenario)


@pytest.mark.parametrize("change", ["cancel", "expire", "replace"])
def test_lease_invalidated_during_claim_handoff_does_not_enter_handler(
    tmp_path, monkeypatch, change,
):
    async def scenario():
        clock = [NOW]
        monkeypatch.setattr(queue_module, "_now", lambda: clock[0])
        queue = SQLiteWorkerQueue(tmp_path / "queue.db")
        other = SQLiteWorkerQueue(tmp_path / "queue.db")
        await queue.initialize()
        identity = await queue.enqueue("one", SCOPE, "work", {})
        claim = queue.claim
        replacements = []

        async def interrupted_claim(*args, **kwargs):
            lease = await claim(*args, **kwargs)
            if change == "cancel":
                assert await other.cancel(identity, SCOPE)
            else:
                clock[0] += timedelta(seconds=6)
                if change == "replace":
                    replacements.append(await other.claim("new", lease_seconds=5))
            return lease

        monkeypatch.setattr(queue, "claim", interrupted_claim)
        seen = []

        async def handler(task, checkpoint):
            seen.append(task.id)

        limits = WorkerLimits(
            max_batch_size=1, max_concurrency=1, lease_seconds=5, task_timeout_seconds=4,
        )
        result = await BoundedWorker(
            queue, {"work": handler}, worker_id="old", limits=limits,
        ).run_batch()
        assert not seen
        assert (result.claimed, result.cancelled, result.failed) == (1, 1, 0)
        if replacements:
            await other.complete(replacements[0])
            assert (await queue.stats()).completed == 1

    run(scenario)


@pytest.mark.parametrize("action", ["validate_lease", "checkpoint", "complete", "fail"])
def test_expired_lease_is_fenced_before_any_reclaim(tmp_path, monkeypatch, action):
    async def scenario():
        clock = [NOW]
        monkeypatch.setattr(queue_module, "_now", lambda: clock[0])
        queue = SQLiteWorkerQueue(tmp_path / "queue.db")
        await queue.initialize()
        await queue.enqueue("one", SCOPE, "work", {})
        lease = await queue.claim("one", lease_seconds=5)
        clock[0] += timedelta(seconds=5)
        arguments = {"checkpoint": ({"late": True},), "fail": (RuntimeError("late"),)}
        with pytest.raises(WorkerQueueError) as error:
            await getattr(queue, action)(lease, *arguments.get(action, ()))
        assert error.value.code == "stale_lease"
        assert (await queue.stats()).expired_leases == 1
        replacement = await queue.claim("new", lease_seconds=5)
        assert replacement.token != lease.token
        await queue.complete(replacement)

    run(scenario)


def test_cancellation_drains_threaded_claim_and_returns_lease_without_running(tmp_path, monkeypatch):
    async def scenario():
        queue = SQLiteWorkerQueue(tmp_path / "queue.db")
        await queue.initialize()
        await queue.enqueue("one", SCOPE, "work", {})
        selected, release = threading.Event(), threading.Event()
        original = queue._claim_sync

        def delayed_claim(*args):
            lease = original(*args)
            selected.set()
            assert release.wait(10)
            return lease

        monkeypatch.setattr(queue, "_claim_sync", delayed_claim)
        seen = []

        async def handler(task, checkpoint):
            seen.append(task.id)

        worker = BoundedWorker(queue, {"work": handler}, worker_id="cancelled")
        batch = asyncio.create_task(worker.run_batch(max_tasks=1))
        try:
            assert await asyncio.to_thread(selected.wait, 10)
            batch.cancel()
            await asyncio.sleep(0)
            batch.cancel()
            await asyncio.sleep(0)
            assert not batch.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await batch
        stats = await queue.stats()
        assert not seen
        assert (stats.leased, stats.pending) == (0, 1)
        # No background claim or failure can mutate state after batch shutdown.
        assert all(task is asyncio.current_task() for task in asyncio.all_tasks())

    run(scenario)


def test_repeated_cancellation_waits_for_handler_cleanup_and_does_not_claim_more(tmp_path):
    async def scenario():
        limits = WorkerLimits(max_batch_size=3, max_concurrency=1)
        queue = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        await queue.initialize()
        for key in ("one", "two", "three"):
            await queue.enqueue(key, SCOPE, "work", {})
        started, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        finished = asyncio.Event()
        seen = []

        async def handler(task, checkpoint):
            seen.append(task.task_key)
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()
                finished.set()

        worker = BoundedWorker(queue, {"work": handler}, worker_id="one", limits=limits)
        batch = asyncio.create_task(worker.run_batch())
        await started.wait()
        batch.cancel()
        await cleaning.wait()
        batch.cancel()
        await asyncio.sleep(0)
        assert not batch.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await batch
        assert finished.is_set()
        assert seen == ["one"]
        assert (await queue.stats()).leased == 0
        assert (await queue.stats()).pending == 3
        assert all(task is asyncio.current_task() for task in asyncio.all_tasks())

    run(scenario)


def test_lane_failure_cancels_and_drains_sibling_claims(tmp_path, monkeypatch):
    async def scenario():
        limits = WorkerLimits(max_batch_size=3, max_concurrency=2)
        queue = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        await queue.initialize()
        for key in ("one", "two", "three"):
            await queue.enqueue(key, SCOPE, "work", {})
        claim = queue.claim
        first_claimed, release = asyncio.Event(), asyncio.Event()
        calls, seen = 0, []

        async def interrupted_claim(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                await first_claimed.wait()
                release.set()
                raise RuntimeError("claim infrastructure failed")
            lease = await claim(*args, **kwargs)
            first_claimed.set()
            await release.wait()
            # Keep the durable lease in hand until the sibling failure cancels us.
            await asyncio.sleep(0.01)
            return lease

        monkeypatch.setattr(queue, "claim", interrupted_claim)

        async def handler(task, checkpoint):
            seen.append(task.task_key)

        worker = BoundedWorker(queue, {"work": handler}, worker_id="one", limits=limits)
        with pytest.raises(RuntimeError, match="claim infrastructure failed"):
            await worker.run_batch()
        assert calls == 2 and not seen
        assert (await queue.stats()).leased == 0
        assert (await queue.stats()).pending == 3
        assert all(task is asyncio.current_task() for task in asyncio.all_tasks())

    run(scenario)


def test_simultaneous_batches_share_execution_capacity_and_keep_batch_bound(tmp_path):
    async def scenario():
        limits = WorkerLimits(max_batch_size=4, max_concurrency=2)
        queue = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        await queue.initialize()
        for index in range(8):
            await queue.enqueue(str(index), SCOPE, "work", {})
        active, maximum = 0, 0
        seen = []

        async def handler(task, checkpoint):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            seen.append(task.id)
            await asyncio.sleep(0.01)
            active -= 1

        worker = BoundedWorker(queue, {"work": handler}, worker_id="one", limits=limits)
        results = await asyncio.gather(worker.run_batch(), worker.run_batch())
        assert [result.completed for result in results] == [4, 4]
        assert maximum == 2
        assert len(Counter(seen)) == 8

    run(scenario)


def test_legacy_execution_adapter_needs_no_validation_or_empty_checkpoint(tmp_path):
    async def scenario():
        queue = SQLiteWorkerQueue(tmp_path / "queue.db")
        await queue.initialize()
        await queue.enqueue("one", SCOPE, "work", {})

        class LegacyAdapter:
            claim, complete, fail = queue.claim, queue.complete, queue.fail

            async def checkpoint(self, lease, value):
                raise AssertionError("adapter does not support checkpoints")

        async def handler(task, checkpoint):
            pass

        worker = BoundedWorker(LegacyAdapter(), {"work": handler}, worker_id="legacy")
        assert (await worker.run_batch()).completed == 1

    run(scenario)


@pytest.mark.parametrize("action", ["checkpoint", "complete"])
def test_cancelled_threaded_write_finishes_before_lease_cleanup(tmp_path, monkeypatch, action):
    async def scenario():
        queue = SQLiteWorkerQueue(tmp_path / "queue.db")
        await queue.initialize()
        await queue.enqueue("one", SCOPE, "work", {})
        writing, release = threading.Event(), threading.Event()
        original = queue._update_lease_sync
        writes = []

        def delayed_write(lease, operation, value, error):
            writes.append(operation)
            if operation == action:
                writing.set()
                assert release.wait(10)
            return original(lease, operation, value, error)

        monkeypatch.setattr(queue, "_update_lease_sync", delayed_write)
        seen = []

        async def handler(task, checkpoint):
            if action == "checkpoint":
                await checkpoint({"done": True})
            seen.append(task.id)

        worker = BoundedWorker(queue, {"work": handler}, worker_id="one")
        batch = asyncio.create_task(worker.run_batch(max_tasks=1))
        try:
            assert await asyncio.to_thread(writing.wait, 10)
            batch.cancel()
            await asyncio.sleep(0)
            batch.cancel()
            await asyncio.sleep(0)
            assert writes == [action]
            assert not batch.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await batch
        assert writes == [action, "fail"]
        stats = await queue.stats()
        assert stats.leased == 0
        if action == "checkpoint":
            assert not seen and stats.pending == 1
        else:
            assert len(seen) == 1 and stats.completed == 1
        assert all(task is asyncio.current_task() for task in asyncio.all_tasks())

    run(scenario)


def test_timeout_is_a_task_failure_and_frees_the_next_slot(tmp_path):
    async def scenario():
        limits = WorkerLimits(
            max_batch_size=2, max_concurrency=1, max_attempts=1, task_timeout_seconds=1,
        )
        queue = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        await queue.initialize()
        await queue.enqueue("slow", SCOPE, "work", {})
        await queue.enqueue("fast", SCOPE, "work", {})
        cancelled = asyncio.Event()

        async def handler(task, checkpoint):
            if task.task_key == "slow":
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        worker = BoundedWorker(queue, {"work": handler}, worker_id="one", limits=limits)
        result = await worker.run_batch()
        assert (result.claimed, result.failed, result.completed) == (2, 1, 1)
        assert cancelled.is_set()
        assert (await queue.stats()).dead == 1
        assert (await queue.stats()).leased == 0

    run(scenario)


def test_failed_cancellation_cleanup_preserves_cancellation_and_expiry_recovery(
    tmp_path, monkeypatch, caplog,
):
    async def scenario():
        clock = [NOW]
        monkeypatch.setattr(queue_module, "_now", lambda: clock[0])
        limits = WorkerLimits(
            max_batch_size=1, max_concurrency=1, lease_seconds=5, task_timeout_seconds=4,
        )
        queue = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        await queue.initialize()
        await queue.enqueue("one", SCOPE, "work", {})
        started = asyncio.Event()

        async def handler(task, checkpoint):
            started.set()
            await asyncio.Event().wait()

        async def broken_fail(lease, error):
            raise RuntimeError("database unavailable")

        monkeypatch.setattr(queue, "fail", broken_fail)
        worker = BoundedWorker(queue, {"work": handler}, worker_id="old", limits=limits)
        batch = asyncio.create_task(worker.run_batch())
        await started.wait()
        batch.cancel()
        with pytest.raises(asyncio.CancelledError):
            await batch
        assert "cleanup failed (RuntimeError)" in caplog.text
        assert (await queue.stats()).leased == 1
        clock[0] += timedelta(seconds=5)
        recovered = await queue.claim("restarted", lease_seconds=5)
        assert recovered.task.attempts == 2
        await queue.complete(recovered)
        assert (await queue.stats()).completed == 1
        assert all(task is asyncio.current_task() for task in asyncio.all_tasks())

    run(scenario)


@pytest.mark.parametrize("timeout", [5, 6, 30])
def test_generic_worker_rejects_timeout_that_can_outlive_lease(tmp_path, timeout):
    queue = SQLiteWorkerQueue(tmp_path / "queue.db")
    limits = WorkerLimits(lease_seconds=5, task_timeout_seconds=timeout)
    with pytest.raises(ValueError, match="shorter than the lease"):
        BoundedWorker(queue, {"work": lambda task, checkpoint: asyncio.sleep(0)},
                      worker_id="unsafe", limits=limits)


def test_lease_renewal_is_an_explicit_host_contract(tmp_path):
    queue = SQLiteWorkerQueue(tmp_path / "queue.db")
    with pytest.raises(TypeError, match="lease_renewal_managed must be a bool"):
        BoundedWorker(queue, {"work": lambda task, checkpoint: asyncio.sleep(0)},
                      worker_id="invalid", lease_renewal_managed="yes")


def test_caller_cancellation_during_timeout_drains_cleanup_and_stops_claims(tmp_path):
    async def scenario():
        limits = WorkerLimits(max_batch_size=2, max_concurrency=1, task_timeout_seconds=1)
        queue = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        await queue.initialize()
        await queue.enqueue("one", SCOPE, "work", {})
        await queue.enqueue("two", SCOPE, "work", {})
        cleaning, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
        seen = []

        async def handler(task, checkpoint):
            seen.append(task.task_key)
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()
                finished.set()

        worker = BoundedWorker(queue, {"work": handler}, worker_id="one", limits=limits)
        batch = asyncio.create_task(worker.run_batch())
        await cleaning.wait()  # The handler timed out, but cleanup is still live.
        batch.cancel()
        await asyncio.sleep(0)
        batch.cancel()
        await asyncio.sleep(0)
        assert not batch.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await batch
        assert finished.is_set() and seen == ["one"]
        assert (await queue.stats()).pending == 2
        assert (await queue.stats()).leased == 0
        assert all(task is asyncio.current_task() for task in asyncio.all_tasks())

    run(scenario)


def test_failure_ack_error_drains_running_sibling_before_returning(tmp_path, monkeypatch):
    async def scenario():
        limits = WorkerLimits(max_batch_size=3, max_concurrency=2)
        queue = SQLiteWorkerQueue(tmp_path / "queue.db", limits=limits)
        await queue.initialize()
        for key in ("bad", "running", "untouched"):
            await queue.enqueue(key, SCOPE, "work", {})
        running, cleaning = asyncio.Event(), asyncio.Event()
        release, finished = asyncio.Event(), asyncio.Event()
        seen = []
        fail = queue.fail

        async def broken_ack(lease, error):
            if lease.task.task_key == "bad":
                raise RuntimeError("failure acknowledgement unavailable")
            return await fail(lease, error)

        monkeypatch.setattr(queue, "fail", broken_ack)

        async def handler(task, checkpoint):
            seen.append(task.task_key)
            if task.task_key == "bad":
                await running.wait()
                raise ValueError("processing failed")
            running.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()
                finished.set()

        worker = BoundedWorker(queue, {"work": handler}, worker_id="one", limits=limits)
        batch = asyncio.create_task(worker.run_batch())
        await cleaning.wait()
        assert not batch.done()
        release.set()
        with pytest.raises(RuntimeError, match="failure acknowledgement unavailable"):
            await batch
        assert finished.is_set() and set(seen) == {"bad", "running"}
        stats = await queue.stats()
        assert (stats.leased, stats.pending) == (1, 2)  # Failed ack recovers via lease expiry.
        assert all(task is asyncio.current_task() for task in asyncio.all_tasks())

    run(scenario)
