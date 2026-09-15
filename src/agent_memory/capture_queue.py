from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
import time
from typing import Iterator
from uuid import uuid4

from .capture_policy import CapturePlan, CaptureSanitizer
from .domain import IngestResult, MemoryScope
from .lifecycle import LifecycleEvent
from .ports import MemoryProvider
from .serialization import to_jsonable


class CaptureQueueError(ValueError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class CaptureQueueLimits:
    max_pending_per_scope: int = 128
    max_pending_global: int = 4_096
    max_attempts: int = 3
    lease_seconds: int = 30
    retry_seconds: int = 2
    stage_seconds: int = 120

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 86_400:
                raise CaptureQueueError(f"invalid capture queue limit {name}", code="invalid_limit")
        if self.max_pending_per_scope > self.max_pending_global:
            raise CaptureQueueError("per-scope capacity exceeds global capacity", code="invalid_limit")


@dataclass(frozen=True, slots=True)
class CaptureReceipt:
    queue_id: int
    event_id: str
    status: str
    duplicate: bool = False


@dataclass(frozen=True, slots=True)
class CaptureLease:
    queue_id: int
    event: LifecycleEvent
    token: str
    attempt: int


class SQLiteCaptureQueue:
    """Optional durable, scoped capture admission and at-least-once processing."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        sanitizer: CaptureSanitizer,
        limits: CaptureQueueLimits | None = None,
    ) -> None:
        self._path = Path(database_path)
        self._sanitizer = sanitizer
        self._limits = limits or CaptureQueueLimits()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS capture_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    partition_key TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    scope_json TEXT NOT NULL,
                    event_json TEXT,
                    artifact_id TEXT,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    available_at REAL NOT NULL,
                    lease_until REAL,
                    lease_token TEXT,
                    last_error TEXT,
                    result_event_id TEXT,
                    UNIQUE (partition_key, event_id)
                )"""
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS capture_queue_ready_idx "
                "ON capture_queue (status, available_at, id)"
            )

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _reserve(self, plan: CapturePlan) -> CaptureReceipt:
        event = plan.event
        scope_key = event.scope.partition_key()
        with self._connection() as connection:
            existing = connection.execute(
                "SELECT id, status, content_hash FROM capture_queue "
                "WHERE partition_key=? AND event_id=?",
                (scope_key, event.event_id),
            ).fetchone()
            if existing is not None:
                if existing["content_hash"] != event.content_hash:
                    raise CaptureQueueError(
                        "event ID reused with different sanitized content",
                        code="event_conflict",
                    )
                return CaptureReceipt(existing["id"], event.event_id, existing["status"], True)
            active = ("staged", "pending", "leased")
            placeholders = ",".join("?" for _ in active)
            scoped = connection.execute(
                "SELECT count(*) FROM capture_queue WHERE partition_key=? "
                f"AND status IN ({placeholders})",
                (scope_key, *active),
            ).fetchone()[0]
            global_count = connection.execute(
                f"SELECT count(*) FROM capture_queue WHERE status IN ({placeholders})",
                active,
            ).fetchone()[0]
            if scoped >= self._limits.max_pending_per_scope or global_count >= self._limits.max_pending_global:
                raise CaptureQueueError("capture queue is full", code="queue_full")
            status = "staged" if plan.artifact_id is not None else "pending"
            cursor = connection.execute(
                """INSERT INTO capture_queue
                   (partition_key, event_id, content_hash, scope_json, event_json,
                    artifact_id, status, available_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    scope_key,
                    event.event_id,
                    event.content_hash,
                    json.dumps(to_jsonable(event.scope), sort_keys=True),
                    json.dumps(event.to_dict(), sort_keys=True),
                    plan.artifact_id,
                    status,
                    time.time(),
                ),
            )
            return CaptureReceipt(cursor.lastrowid, event.event_id, status)

    async def enqueue(self, event: LifecycleEvent) -> CaptureReceipt:
        plan = self._sanitizer.plan(event)
        receipt = self._reserve(plan)
        if receipt.duplicate or plan.artifact_id is None:
            return receipt
        try:
            await self._sanitizer.materialize(plan)
        except Exception:
            with self._connection() as connection:
                connection.execute(
                    "UPDATE capture_queue SET status='dead', last_error='artifact_write_failed' "
                    "WHERE id=? AND status='staged'",
                    (receipt.queue_id,),
                )
            raise
        with self._connection() as connection:
            connection.execute(
                "UPDATE capture_queue SET status='pending' WHERE id=? AND status='staged'",
                (receipt.queue_id,),
            )
        return CaptureReceipt(receipt.queue_id, receipt.event_id, "pending")

    async def recover_staged(self) -> int:
        """Mark interrupted staging and remove deterministic orphan references."""

        with self._connection() as connection:
            cutoff = time.time() - self._limits.stage_seconds
            rows = connection.execute(
                "SELECT id, artifact_id, scope_json FROM capture_queue "
                "WHERE status='staged' AND available_at<=?",
                (cutoff,),
            ).fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE capture_queue SET status='dead', last_error='staging_interrupted' "
                    "WHERE id=? AND status='staged'",
                    (row["id"],),
                )
        store = self._sanitizer.artifact_store
        for row in rows:
            if store is not None and row["artifact_id"] is not None:
                scope = MemoryScope(**json.loads(row["scope_json"]))
                await store.discard(row["artifact_id"], scope=scope)
        return len(rows)

    async def claim(self, *, scope: MemoryScope | None = None) -> CaptureLease | None:
        with self._connection() as connection:
            now = time.time()
            scope_filter = " AND partition_key=?" if scope is not None else ""
            arguments = (now, now, scope.partition_key()) if scope is not None else (now, now)
            row = connection.execute(
                "SELECT * FROM capture_queue WHERE "
                "((status='pending' AND available_at<=?) "
                "OR (status='leased' AND lease_until<=?))"
                + scope_filter
                + " ORDER BY id LIMIT 1",
                arguments,
            ).fetchone()
            if row is None:
                return None
            if row["attempts"] >= self._limits.max_attempts:
                connection.execute(
                    "UPDATE capture_queue SET status='dead', last_error='attempts_exhausted', "
                    "lease_token=NULL, lease_until=NULL WHERE id=?",
                    (row["id"],),
                )
                return None
            token = str(uuid4())
            connection.execute(
                "UPDATE capture_queue SET status='leased', attempts=attempts+1, "
                "lease_token=?, lease_until=? WHERE id=?",
                (token, now + self._limits.lease_seconds, row["id"]),
            )
            trusted_scope = MemoryScope(**json.loads(row["scope_json"]))
            event = LifecycleEvent.from_dict(
                json.loads(row["event_json"]),
                trusted_scope=trusted_scope,
                allow_host_feedback=True,
            )
            return CaptureLease(row["id"], event, token, row["attempts"] + 1)

    async def ack(self, lease: CaptureLease, result: IngestResult) -> None:
        with self._connection() as connection:
            cursor = connection.execute(
                "UPDATE capture_queue SET status='done', result_event_id=?, "
                "event_json=NULL, lease_token=NULL, lease_until=NULL "
                "WHERE id=? AND status='leased' AND lease_token=?",
                (result.event_id, lease.queue_id, lease.token),
            )
            if cursor.rowcount != 1:
                raise CaptureQueueError("capture lease is no longer active", code="stale_lease")

    async def fail(self, lease: CaptureLease, *, error_code: str = "provider_failure") -> None:
        if not isinstance(error_code, str) or not error_code.isidentifier():
            raise CaptureQueueError("invalid sanitized failure code", code="invalid_error_code")
        with self._connection() as connection:
            status = "dead" if lease.attempt >= self._limits.max_attempts else "pending"
            cursor = connection.execute(
                "UPDATE capture_queue SET status=?, available_at=?, last_error=?, "
                "lease_token=NULL, lease_until=NULL "
                "WHERE id=? AND status='leased' AND lease_token=?",
                (
                    status,
                    time.time() + self._limits.retry_seconds,
                    error_code,
                    lease.queue_id,
                    lease.token,
                ),
            )
            if cursor.rowcount != 1:
                raise CaptureQueueError("capture lease is no longer active", code="stale_lease")

    async def process_one(
        self,
        provider: MemoryProvider,
        *,
        scope: MemoryScope | None = None,
    ) -> CaptureReceipt | None:
        lease = await self.claim(scope=scope)
        if lease is None:
            return None
        try:
            result = await provider.ingest_event(lease.event.to_memory_event())
        except Exception:
            await self.fail(lease)
            raise
        await self.ack(lease, result)
        return CaptureReceipt(lease.queue_id, lease.event.event_id, "done", result.duplicate)

    def status(self, scope: MemoryScope, event_id: str) -> CaptureReceipt | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT id, status FROM capture_queue WHERE partition_key=? AND event_id=?",
                (scope.partition_key(), event_id),
            ).fetchone()
            return CaptureReceipt(row["id"], event_id, row["status"]) if row else None
