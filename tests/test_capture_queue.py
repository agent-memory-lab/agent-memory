"""Durable capture admission and recovery contracts."""

from __future__ import annotations

import asyncio
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
import sqlite3
from types import SimpleNamespace

import pytest

from agent_memory import CapturePlan
from agent_memory.capture_artifacts import FileCaptureArtifactStore
from agent_memory.capture_policy import CaptureSanitizer
from agent_memory.capture_queue import (
    CaptureQueueError,
    CaptureQueueLimits,
    SQLiteCaptureQueue,
)
from agent_memory.domain import MemoryScope
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin


def _run(coro):
    return asyncio.run(coro)


def _event(scope: MemoryScope, event_id: str, *, large: bool = False) -> LifecycleEvent:
    return LifecycleEvent(
        scope=scope,
        event_id=event_id,
        event_type=LifecycleEventType.TOOL_COMPLETED,
        origin=LifecycleOrigin.TOOL,
        occurred_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        run_id="run-1",
        content="Authorization: Bearer " + "A" * 24,
        payload={
            "api_key": "secretvalue12345678",
            "result": "x" * (9_000 if large else 8),
        },
    )


def _scope(tenant: str) -> MemoryScope:
    return MemoryScope(tenant, user_id=f"user-{tenant}", session_id="session-1")


def test_public_capture_plan_and_redacted_artifact(tmp_path):
    scope = _scope("a")
    store = FileCaptureArtifactStore(tmp_path / "artifacts")
    sanitizer = CaptureSanitizer(artifact_store=store)
    plan = sanitizer.plan(_event(scope, "large", large=True))
    assert isinstance(plan, CapturePlan)
    assert plan.artifact_id is not None
    assert plan.event.payload["api_key"] == "[REDACTED]"
    assert "Bearer " + "A" * 24 not in plan.event.content
    with pytest.raises(FileNotFoundError):
        _run(store.get(plan.artifact_id, scope=scope))
    _run(sanitizer.materialize(plan))
    artifact = _run(store.get(plan.artifact_id, scope=scope))
    assert b"secretvalue12345678" not in artifact
    with pytest.raises(ValueError):
        _run(store.get(plan.artifact_id, scope=_scope("b")))


def test_queue_scope_capacity_duplicate_and_conflict(tmp_path):
    limits = CaptureQueueLimits(max_pending_per_scope=1, max_pending_global=2)
    queue = SQLiteCaptureQueue(tmp_path / "capture.db", sanitizer=CaptureSanitizer(), limits=limits)
    a, b = _scope("a"), _scope("b")
    first = _event(a, "first")
    receipt = _run(queue.enqueue(first))
    assert receipt.status == "pending"
    assert _run(queue.enqueue(first)).duplicate
    with pytest.raises(CaptureQueueError) as conflict:
        _run(queue.enqueue(replace(first, content="different content")))
    assert conflict.value.code == "event_conflict"
    with pytest.raises(CaptureQueueError) as full:
        _run(queue.enqueue(_event(a, "second")))
    assert full.value.code == "queue_full"
    assert queue.status(b, "first") is None
    assert _run(queue.enqueue(_event(b, "other"))).status == "pending"
    with pytest.raises(CaptureQueueError, match="full"):
        _run(queue.enqueue(_event(_scope("c"), "third")))


def test_capacity_denial_never_writes_an_artifact(tmp_path):
    scope = _scope("a")
    store = FileCaptureArtifactStore(tmp_path / "artifacts")
    sanitizer = CaptureSanitizer(artifact_store=store)
    queue = SQLiteCaptureQueue(
        tmp_path / "capture.db",
        sanitizer=sanitizer,
        limits=CaptureQueueLimits(max_pending_per_scope=1, max_pending_global=1),
    )
    _run(queue.enqueue(_event(scope, "first")))
    event = _event(scope, "denied", large=True)
    reference = sanitizer.plan(event).artifact_id
    with pytest.raises(CaptureQueueError, match="full"):
        _run(queue.enqueue(event))
    with pytest.raises(FileNotFoundError):
        _run(store.get(reference, scope=scope))


def test_queue_persists_only_sanitized_evidence(tmp_path):
    path = tmp_path / "capture.db"
    scope = _scope("a")
    queue = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer())
    _run(queue.enqueue(_event(scope, "safe")))
    with closing(sqlite3.connect(path)) as connection:
        serialized = connection.execute("SELECT event_json FROM capture_queue").fetchone()[0]
    assert "secretvalue12345678" not in serialized
    assert "Bearer " + "A" * 24 not in serialized
    assert "[REDACTED]" in serialized


def test_expired_lease_recovery_and_stale_ack(tmp_path, monkeypatch):
    from agent_memory import capture_queue as queue_module

    clock = [1_000_000.0]
    monkeypatch.setattr(queue_module.time, "time", lambda: clock[0])
    scope = _scope("a")
    path = tmp_path / "capture.db"
    limits = CaptureQueueLimits(max_pending_per_scope=1, max_pending_global=1, lease_seconds=1)
    first = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer(), limits=limits)
    second = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer(), limits=limits)
    _run(first.enqueue(_event(scope, "leased")))
    old = _run(first.claim(scope=scope))
    assert old is not None and old.attempt == 1
    assert _run(second.claim(scope=scope)) is None
    clock[0] += 2
    current = _run(second.claim(scope=scope))
    assert current is not None and current.attempt == 2
    with pytest.raises(CaptureQueueError) as stale:
        _run(first.ack(old, SimpleNamespace(event_id="stored")))
    assert stale.value.code == "stale_lease"
    _run(second.ack(current, SimpleNamespace(event_id="stored")))
    assert first.status(scope, "leased").status == "done"


def test_provider_failure_retries_then_dead_letters(tmp_path, monkeypatch):
    from agent_memory import capture_queue as queue_module

    clock = [1_000_000.0]
    monkeypatch.setattr(queue_module.time, "time", lambda: clock[0])
    scope = _scope("a")
    queue = SQLiteCaptureQueue(
        tmp_path / "capture.db",
        sanitizer=CaptureSanitizer(),
        limits=CaptureQueueLimits(max_attempts=2),
    )
    _run(queue.enqueue(_event(scope, "failure")))

    class BrokenProvider:
        async def ingest_event(self, event):
            raise RuntimeError("provider failed")

    with pytest.raises(RuntimeError):
        _run(queue.process_one(BrokenProvider(), scope=scope))
    assert queue.status(scope, "failure").status == "pending"
    assert _run(queue.claim(scope=scope)) is None
    clock[0] += 3
    with pytest.raises(RuntimeError):
        _run(queue.process_one(BrokenProvider(), scope=scope))
    assert queue.status(scope, "failure").status == "dead"


def test_worker_ingests_once_and_acknowledges(tmp_path):
    scope = _scope("a")
    path = tmp_path / "capture.db"
    first = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer())
    second = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer())
    _run(first.enqueue(_event(scope, "processed")))
    seen = []

    class Provider:
        async def ingest_event(self, event):
            seen.append(event)
            return SimpleNamespace(event_id="stored-1", duplicate=False)

    receipt = _run(second.process_one(Provider(), scope=scope))
    assert receipt.status == "done"
    assert first.status(scope, "processed").status == "done"
    assert _run(first.process_one(Provider(), scope=scope)) is None
    assert len(seen) == 1
    assert seen[0].metadata["lifecycle"]["event_id"] == "processed"
    assert seen[0].metadata["lifecycle"]["payload"]["api_key"] == "[REDACTED]"


def test_interrupted_staging_discards_orphan_artifact(tmp_path, monkeypatch):
    from agent_memory import capture_queue as queue_module

    clock = [1_000_000.0]
    monkeypatch.setattr(queue_module.time, "time", lambda: clock[0])
    scope = _scope("a")
    store = FileCaptureArtifactStore(tmp_path / "artifacts")
    sanitizer = CaptureSanitizer(artifact_store=store)
    queue = SQLiteCaptureQueue(
        tmp_path / "capture.db", sanitizer=sanitizer,
        limits=CaptureQueueLimits(stage_seconds=1),
    )
    plan = sanitizer.plan(_event(scope, "interrupted", large=True))
    assert queue._reserve(plan).status == "staged"
    _run(sanitizer.materialize(plan))
    clock[0] += 2
    assert _run(queue.recover_staged()) == 1
    assert queue.status(scope, "interrupted").status == "dead"
    with pytest.raises(FileNotFoundError):
        _run(store.get(plan.artifact_id, scope=scope))


@pytest.mark.parametrize("scoped", [False, True])
def test_drain_skips_multiple_expired_exhausted_leases(tmp_path, monkeypatch, scoped):
    from agent_memory.capture import queue as queue_module

    clock = [1_000_000.0]
    monkeypatch.setattr(queue_module.time, "time", lambda: clock[0])
    scope = _scope("a")
    path = tmp_path / "capture.db"
    limits = CaptureQueueLimits(max_attempts=1, lease_seconds=1)
    queue = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer(), limits=limits)
    for event_id in ("exhausted-1", "exhausted-2"):
        _run(queue.enqueue(_event(scope, event_id)))
        assert _run(queue.claim(scope=scope)) is not None
    for event_id in ("ready-1", "ready-2"):
        _run(queue.enqueue(_event(scope, event_id)))
    clock[0] += 2
    restarted = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer(), limits=limits)
    seen = []

    class Provider:
        async def ingest_event(self, event):
            seen.append(event.metadata["lifecycle"]["event_id"])
            return SimpleNamespace(event_id=event.id, duplicate=False)

    async def drain():
        receipts = []
        while (receipt := await restarted.process_one(
            Provider(), scope=scope if scoped else None,
        )) is not None:
            receipts.append(receipt.event_id)
        return receipts

    assert _run(drain()) == ["ready-1", "ready-2"]
    assert seen == ["ready-1", "ready-2"]
    with closing(sqlite3.connect(path)) as connection:
        rows = connection.execute(
            "SELECT status, last_error, lease_token, lease_until FROM capture_queue "
            "WHERE event_id LIKE 'exhausted-%' ORDER BY id"
        ).fetchall()
    assert rows == [("dead", "attempts_exhausted", None, None)] * 2
    assert queue.status(scope, "ready-1").status == "done"
    assert queue.status(scope, "ready-2").status == "done"


def test_exhausted_cleanup_preserves_other_scopes_and_unready_work(tmp_path, monkeypatch):
    from agent_memory.capture import queue as queue_module

    clock = [1_000_000.0]
    monkeypatch.setattr(queue_module.time, "time", lambda: clock[0])
    a, b = _scope("a"), _scope("b")
    path = tmp_path / "capture.db"
    initial_limits = CaptureQueueLimits(max_attempts=2, lease_seconds=10, retry_seconds=10)
    first = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer(), limits=initial_limits)
    _run(first.enqueue(_event(a, "live-lease")))
    initial = _run(first.claim(scope=a))
    assert initial is not None
    _run(first.fail(initial))
    _run(first.enqueue(_event(b, "other-expired")))
    assert _run(first.claim(scope=b)) is not None
    clock[0] += 11
    live = _run(first.claim(scope=a))
    assert live is not None and live.event.event_id == "live-lease" and live.attempt == 2
    _run(first.enqueue(_event(a, "future-retry")))
    retry = _run(first.claim(scope=a))
    assert retry is not None and retry.event.event_id == "future-retry"
    _run(first.fail(retry))
    _run(first.enqueue(_event(a, "ready")))
    # Lowering the attempt limit must not retire live leases or future retries.
    second = SQLiteCaptureQueue(
        path, sanitizer=CaptureSanitizer(),
        limits=CaptureQueueLimits(max_attempts=1, lease_seconds=10, retry_seconds=10),
    )
    lease = _run(second.claim(scope=a))
    assert lease is not None and lease.event.event_id == "ready"
    assert second.status(b, "other-expired").status == "leased"
    assert second.status(a, "live-lease").status == "leased"
    assert second.status(a, "future-retry").status == "pending"
    clock[0] += 11
    assert _run(second.claim(scope=a)) is None
    assert second.status(a, "live-lease").status == "dead"
    assert second.status(a, "future-retry").status == "dead"
    assert second.status(a, "ready").status == "dead"
    assert second.status(b, "other-expired").status == "leased"
    assert _run(second.claim(scope=b)) is None
    assert second.status(b, "other-expired").status == "dead"


@pytest.mark.parametrize("action", ["enqueue", "claim", "ack", "fail", "recover_staged"])
def test_async_capture_transactions_do_not_block_loop_on_writer_lock(tmp_path, action):
    import threading
    import time

    async def scenario():
        path = tmp_path / "capture.db"
        scope = _scope("async")
        queue = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer())
        await queue.enqueue(_event(scope, "first"))
        lease = await queue.claim() if action in {"ack", "fail"} else None
        acquired, release = threading.Event(), threading.Event()

        def hold_writer():
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("BEGIN IMMEDIATE")
                acquired.set()
                # The timeout prevents an old blocking implementation deadlocking
                # its event loop, while the separate thread still releases SQLite.
                release.wait(2)
                connection.rollback()

        holder = asyncio.create_task(asyncio.to_thread(hold_writer))
        assert await asyncio.to_thread(acquired.wait, 5)
        operations = {
            "enqueue": lambda: queue.enqueue(_event(scope, "second")),
            "claim": queue.claim,
            "ack": lambda: queue.ack(lease, SimpleNamespace(event_id="stored")),
            "fail": lambda: queue.fail(lease),
            "recover_staged": queue.recover_staged,
        }
        task = asyncio.create_task(operations[action]())
        try:
            start, ticks = time.monotonic(), 0
            while time.monotonic() - start < 0.12:
                ticks += 1
                await asyncio.sleep(0.005)
            assert ticks >= 8
            assert not task.done()
        finally:
            release.set()
            await holder
            await task

    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ["begin", "commit", "rollback", "close"])
def test_cancelled_capture_transaction_drains_before_returning(tmp_path, monkeypatch, phase):
    import threading

    from agent_memory.capture import queue as queue_module

    async def scenario():
        path = tmp_path / "capture.db"
        scope = _scope("cancel")
        queue = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer())
        started, release = threading.Event(), threading.Event()
        calls = []
        original_connect = sqlite3.connect

        class ObservedConnection(sqlite3.Connection):
            def record(self, action):
                calls.append((action, threading.get_ident()))
                if action == phase:
                    started.set()
                    assert release.wait(5), "transaction was not released"

            def execute(self, sql, parameters=()):
                self.record("begin" if sql == "BEGIN IMMEDIATE" else "execute")
                if phase == "rollback" and sql.lstrip().startswith("INSERT"):
                    raise sqlite3.OperationalError("injected write failure")
                return super().execute(sql, parameters)

            def commit(self):
                self.record("commit")
                return super().commit()

            def rollback(self):
                self.record("rollback")
                return super().rollback()

            def close(self):
                self.record("close")
                return super().close()

        def connect(*args, **kwargs):
            calls.append(("connect", threading.get_ident()))
            return original_connect(*args, **kwargs, factory=ObservedConnection)

        monkeypatch.setattr(queue_module.sqlite3, "connect", connect)
        task = asyncio.create_task(queue.enqueue(_event(scope, "cancelled")))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            for _ in range(3):
                task.cancel()
                await asyncio.sleep(0.01)
                assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert calls[-1][0] == "close"
        threads = {thread for _, thread in calls}
        assert len(threads) == 1 and threading.get_ident() not in threads
        with closing(original_connect(path)) as connection:
            count = connection.execute("SELECT count(*) FROM capture_queue").fetchone()[0]
            assert count == (0 if phase == "rollback" else 1)
            # No transaction is left running after cancellation propagates.
            connection.execute("BEGIN IMMEDIATE")
            connection.rollback()

    asyncio.run(scenario())


def test_cancelled_staging_recovery_finishes_orphan_cleanup(tmp_path, monkeypatch):
    from agent_memory.capture import queue as queue_module

    async def scenario():
        scope = _scope("recovery-cancel")
        store = FileCaptureArtifactStore(tmp_path / "artifacts")
        sanitizer = CaptureSanitizer(artifact_store=store)
        queue = SQLiteCaptureQueue(tmp_path / "capture.db", sanitizer=sanitizer)
        plan = sanitizer.plan(_event(scope, "staged", large=True))
        queue._reserve(plan)
        await sanitizer.materialize(plan)
        monkeypatch.setattr(queue_module.time, "time", lambda: 10_000_000_000.0)
        started, release = asyncio.Event(), asyncio.Event()
        discard = store.discard

        async def blocked_discard(artifact_id, *, scope):
            started.set()
            await release.wait()
            await discard(artifact_id, scope=scope)

        monkeypatch.setattr(store, "discard", blocked_discard)
        task = asyncio.create_task(queue.recover_staged())
        await asyncio.wait_for(started.wait(), 2)
        try:
            for _ in range(3):
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert queue.status(scope, "staged").status == "dead"
        with pytest.raises(FileNotFoundError):
            await store.get(plan.artifact_id, scope=scope)

    asyncio.run(scenario())


@pytest.mark.parametrize("fail_artifact", [False, True])
def test_staging_transition_runs_off_loop(tmp_path, monkeypatch, fail_artifact):
    import threading

    async def scenario():
        scope = _scope("staging")
        store = FileCaptureArtifactStore(tmp_path / "artifacts")
        sanitizer = CaptureSanitizer(artifact_store=store)
        queue = SQLiteCaptureQueue(tmp_path / "capture.db", sanitizer=sanitizer)
        owner_threads = []
        finish = queue._finish_staging

        def record_finish(*args, **kwargs):
            owner_threads.append(threading.get_ident())
            return finish(*args, **kwargs)

        async def broken_materialize(plan):
            raise OSError("artifact unavailable")

        monkeypatch.setattr(queue, "_finish_staging", record_finish)
        if fail_artifact:
            monkeypatch.setattr(sanitizer, "materialize", broken_materialize)
            with pytest.raises(OSError, match="artifact unavailable"):
                await queue.enqueue(_event(scope, "large", large=True))
            assert queue.status(scope, "large").status == "dead"
        else:
            receipt = await queue.enqueue(_event(scope, "large", large=True))
            assert receipt.status == "pending"
        assert len(owner_threads) == 1
        assert owner_threads[0] != threading.get_ident()

    asyncio.run(scenario())
