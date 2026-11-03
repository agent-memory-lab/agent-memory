"""Cross-instance worker lease writes must not overwrite newer queue state."""

import asyncio
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import threading

import pytest

from agent_memory.domain import ForgetRequest, MemoryScope
from agent_memory.operations import sqlite_worker_queue as queue_module
from agent_memory.operations.sqlite_worker_queue import SQLiteWorkerQueue
from agent_memory.operations.worker_tasks import WorkerQueueError


SCOPE = MemoryScope("tenant-a", session_id="session-a")
NOW = datetime(2026, 11, 3, tzinfo=timezone.utc)


def _task_row(path, task_id):
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        return dict(connection.execute(
            "SELECT * FROM agent_memory_worker_tasks WHERE id=?", (task_id,)
        ).fetchone())


async def _write_lease(queue, lease, action):
    if action == "checkpoint":
        await queue.checkpoint(lease, {"owner": "old-worker"})
    elif action == "complete":
        await queue.complete(lease)
    else:
        await queue.fail(lease, RuntimeError("old worker failure"))


@pytest.mark.parametrize("action", ["checkpoint", "complete", "fail"])
@pytest.mark.parametrize("interleaving", ["cancel", "forget", "replace"])
def test_lease_write_rechecks_identity_after_concurrent_change(
    tmp_path, monkeypatch, action, interleaving,
):
    async def scenario():
        clock = [NOW]
        monkeypatch.setattr(queue_module, "_now", lambda: clock[0])
        path = tmp_path / "worker.db"
        first, second = SQLiteWorkerQueue(path), SQLiteWorkerQueue(path)
        await first.initialize()
        task_id = await first.enqueue("task", SCOPE, "noop", {"event_id": "source"})
        lease = await first.claim("old-worker", lease_seconds=5)
        assert lease is not None
        await first.checkpoint(lease, {"owner": "original"})
        selected = threading.Event()
        resume = threading.Event()
        original_connection = first._connection

        class PausedCursor:
            def __init__(self, cursor):
                self.cursor = cursor

            def fetchone(self):
                row = self.cursor.fetchone()
                self.cursor.close()
                selected.set()
                assert resume.wait(10), "lease write was not released"
                return row

        class PausedConnection:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, sql, parameters=()):
                cursor = self.connection.execute(sql, parameters)
                if sql.startswith("SELECT") and "lease_token=?" in sql:
                    return PausedCursor(cursor)
                return cursor

        @contextmanager
        def paused_connection():
            with original_connection() as connection:
                yield PausedConnection(connection)

        monkeypatch.setattr(first, "_connection", paused_connection)
        old_write = asyncio.create_task(_write_lease(first, lease, action))
        replacement = None
        try:
            assert await asyncio.to_thread(selected.wait, 10), "lease was not selected"
            # Finish the independent writer before resuming the stale UPDATE.
            if interleaving == "cancel":
                assert await second.cancel(task_id, SCOPE)
            elif interleaving == "forget":
                assert await second.cancel_forget(
                    ForgetRequest(SCOPE, memory_ids=("source",))
                ) == 1
            else:
                clock[0] += timedelta(seconds=6)
                replacement = await second.claim("new-worker", lease_seconds=5)
                assert replacement is not None and replacement.token != lease.token
                await second.checkpoint(replacement, {"owner": "new-worker"})
            expected = _task_row(path, task_id)
        finally:
            resume.set()
        with pytest.raises(WorkerQueueError) as stale:
            await old_write
        assert stale.value.code == "stale_lease"
        assert _task_row(path, task_id) == expected
        if replacement is not None:
            # Rejecting the old owner must not invalidate the current lease.
            await second.complete(replacement)
            assert (await second.stats()).completed == 1
        else:
            assert (await second.stats()).cancelled == 1
            assert await second.claim("other-worker", lease_seconds=5) is None

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["checkpoint", "complete", "fail"])
@pytest.mark.parametrize("max_attempts", [1, 3])
def test_current_lease_writes_preserve_success_and_retry_behavior(
    tmp_path, monkeypatch, action, max_attempts,
):
    async def scenario():
        monkeypatch.setattr(queue_module, "_now", lambda: NOW)
        path = tmp_path / "worker.db"
        queue = SQLiteWorkerQueue(path)
        await queue.initialize()
        task_id = await queue.enqueue("task", SCOPE, "noop", {}, max_attempts=max_attempts)
        lease = await queue.claim("worker", lease_seconds=5)
        assert lease is not None
        await _write_lease(queue, lease, action)
        row = _task_row(path, task_id)
        assert row["attempts"] == 1
        if action == "checkpoint":
            assert row["status"] == "leased"
            assert row["lease_token"] == lease.token
            assert json.loads(row["checkpoint_json"]) == {"owner": "old-worker"}
            await queue.complete(lease)
        else:
            assert row["leased_by"] is row["lease_token"] is row["lease_expires_at"] is None
            if action == "complete":
                assert row["status"] == "completed"
                assert row["last_error_code"] is None
            else:
                assert row["status"] == ("dead" if max_attempts == 1 else "pending")
                assert row["last_error_code"] == "RuntimeError"
                assert datetime.fromisoformat(row["next_attempt_at"]) == NOW + timedelta(seconds=4)
            with pytest.raises(WorkerQueueError) as stale:
                await _write_lease(queue, lease, action)
            assert stale.value.code == "stale_lease"

    asyncio.run(scenario())
