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
