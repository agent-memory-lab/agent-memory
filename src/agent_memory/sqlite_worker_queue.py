"""Durable SQLite implementation of the provider-neutral worker queue."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping
from uuid import uuid4

from .domain import IngestResult, MemoryEvent, MemoryScope
from .serialization import to_jsonable
from .worker_tasks import (
    WorkerLease,
    WorkerLimits,
    WorkerQueueError,
    WorkerQueueStats,
    WorkerTask,
    WorkerTaskStatus,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


class SQLiteWorkerQueue:
    """At-least-once local queue isolated from the Agent request hot path."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        limits: WorkerLimits | None = None,
    ) -> None:
        self._path = str(database_path)
        self._limits = limits or WorkerLimits()
        self._write_lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS agent_memory_worker_tasks (
                    id TEXT PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    user_id TEXT,
                    agent_id TEXT,
                    workspace_id TEXT,
                    session_id TEXT,
                    task_key TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    max_attempts INTEGER NOT NULL,
                    next_attempt_at TEXT NOT NULL,
                    leased_by TEXT,
                    lease_token TEXT,
                    lease_expires_at TEXT,
                    checkpoint_json TEXT NOT NULL,
                    last_error_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(partition_key, task_key)
                );
                CREATE INDEX IF NOT EXISTS agent_memory_worker_claim_idx
                ON agent_memory_worker_tasks(status, next_attempt_at, created_at);
                CREATE INDEX IF NOT EXISTS agent_memory_worker_scope_idx
                ON agent_memory_worker_tasks(partition_key, status);
                """
            )

    async def enqueue_event(self, event: MemoryEvent, result: IngestResult) -> str:
        return await self.enqueue(
            f"event:{event.id}:consolidate",
            event.scope,
            "memory.consolidate",
            {
                "event_id": event.id,
                "claim_ids": tuple(result.accepted_ids),
            },
        )

    async def enqueue(
        self,
        task_key: str,
        scope: MemoryScope,
        task_type: str,
        payload: Mapping[str, Any],
        *,
        max_attempts: int | None = None,
    ) -> str:
        async with self._write_lock:
            return await asyncio.to_thread(
                self._enqueue_sync,
                task_key,
                scope,
                task_type,
                payload,
                max_attempts,
            )

    def _enqueue_sync(
        self,
        task_key: str,
        scope: MemoryScope,
        task_type: str,
        payload: Mapping[str, Any],
        max_attempts: int | None,
    ) -> str:
        if not isinstance(task_key, str) or not task_key.strip() or len(task_key) > 256:
            raise ValueError("task_key must contain 1 to 256 characters")
        if not isinstance(scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope")
        if not isinstance(task_type, str) or not task_type.strip() or len(task_type) > 128:
            raise ValueError("task_type must contain 1 to 128 characters")
        if not isinstance(payload, Mapping):
            raise TypeError("payload must be a mapping")
        attempts_limit = max_attempts or self._limits.max_attempts
        if type(attempts_limit) is not int or not 1 <= attempts_limit <= 100:
            raise ValueError("max_attempts must be between 1 and 100")
        payload_json = json.dumps(to_jsonable(dict(payload)), ensure_ascii=False, separators=(",", ":"))
        if len(payload_json.encode("utf-8")) > 256_000:
            raise ValueError("worker payload exceeds 256000 bytes")
        now = _now()
        partition_key = scope.partition_key()
        with self._connection() as connection:
            existing = connection.execute(
                "SELECT id FROM agent_memory_worker_tasks WHERE partition_key=? AND task_key=?",
                (partition_key, task_key),
            ).fetchone()
            if existing is not None:
                return existing["id"]
            active = (WorkerTaskStatus.PENDING, WorkerTaskStatus.LEASED)
            global_count = connection.execute(
                "SELECT COUNT(*) FROM agent_memory_worker_tasks WHERE status IN (?, ?)",
                active,
            ).fetchone()[0]
            scope_count = connection.execute(
                "SELECT COUNT(*) FROM agent_memory_worker_tasks WHERE partition_key=? AND status IN (?, ?)",
                (partition_key, *active),
            ).fetchone()[0]
            if global_count >= self._limits.max_pending_global:
                raise WorkerQueueError("global worker capacity exceeded", code="global_capacity")
            if scope_count >= self._limits.max_pending_per_scope:
                raise WorkerQueueError("scope worker capacity exceeded", code="scope_capacity")
            task_id = str(uuid4())
            connection.execute(
                """
                INSERT INTO agent_memory_worker_tasks (
                    id, partition_key, tenant_id, namespace, user_id, agent_id,
                    workspace_id, session_id, task_key, task_type, payload_json,
                    status, attempts, max_attempts, next_attempt_at, checkpoint_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, '{}', ?, ?)
                """,
                (
                    task_id,
                    partition_key,
                    scope.tenant_id,
                    scope.namespace,
                    scope.user_id,
                    scope.agent_id,
                    scope.workspace_id,
                    scope.session_id,
                    task_key,
                    task_type,
                    payload_json,
                    WorkerTaskStatus.PENDING,
                    attempts_limit,
                    _iso(now),
                    _iso(now),
                    _iso(now),
                ),
            )
            return task_id

    async def claim(self, worker_id: str, *, lease_seconds: int) -> WorkerLease | None:
        async with self._write_lock:
            return await asyncio.to_thread(self._claim_sync, worker_id, lease_seconds)

    def _claim_sync(self, worker_id: str, lease_seconds: int) -> WorkerLease | None:
        if not isinstance(worker_id, str) or not worker_id.strip() or len(worker_id) > 128:
            raise ValueError("worker_id must contain 1 to 128 characters")
        if type(lease_seconds) is not int or not 5 <= lease_seconds <= 86_400:
            raise ValueError("lease_seconds must be between 5 and 86400")
        now = _now()
        expires = datetime.fromtimestamp(now.timestamp() + lease_seconds, timezone.utc)
        token = str(uuid4())
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE agent_memory_worker_tasks
                SET status=?, leased_by=NULL, lease_token=NULL, lease_expires_at=NULL,
                    next_attempt_at=?, updated_at=?
                WHERE status=? AND lease_expires_at<=? AND attempts<max_attempts
                """,
                (
                    WorkerTaskStatus.PENDING,
                    _iso(now),
                    _iso(now),
                    WorkerTaskStatus.LEASED,
                    _iso(now),
                ),
            )
            connection.execute(
                """
                UPDATE agent_memory_worker_tasks
                SET status=?, leased_by=NULL, lease_token=NULL, lease_expires_at=NULL,
                    updated_at=?
                WHERE status=? AND lease_expires_at<=? AND attempts>=max_attempts
                """,
                (WorkerTaskStatus.DEAD, _iso(now), WorkerTaskStatus.LEASED, _iso(now)),
            )
            row = connection.execute(
                """
                SELECT * FROM agent_memory_worker_tasks
                WHERE status=? AND next_attempt_at<=?
                ORDER BY next_attempt_at, created_at, id LIMIT 1
                """,
                (WorkerTaskStatus.PENDING, _iso(now)),
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            connection.execute(
                """
                UPDATE agent_memory_worker_tasks
                SET status=?, attempts=attempts+1, leased_by=?, lease_token=?,
                    lease_expires_at=?, updated_at=? WHERE id=? AND status=?
                """,
                (
                    WorkerTaskStatus.LEASED,
                    worker_id,
                    token,
                    _iso(expires),
                    _iso(now),
                    row["id"],
                    WorkerTaskStatus.PENDING,
                ),
            )
            leased = connection.execute(
                "SELECT * FROM agent_memory_worker_tasks WHERE id=?", (row["id"],)
            ).fetchone()
            connection.commit()
            return WorkerLease(self._task_from_row(leased), token)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    async def checkpoint(self, lease: WorkerLease, value: Mapping[str, Any]) -> None:
        await self._update_lease(lease, "checkpoint", value=value)

    async def complete(self, lease: WorkerLease) -> None:
        await self._update_lease(lease, "complete")

    async def fail(self, lease: WorkerLease, error: BaseException) -> None:
        await self._update_lease(lease, "fail", error=error)

    async def _update_lease(
        self,
        lease: WorkerLease,
        action: str,
        *,
        value: Mapping[str, Any] | None = None,
        error: BaseException | None = None,
    ) -> None:
        if not isinstance(lease, WorkerLease):
            raise TypeError("lease must be a WorkerLease")
        async with self._write_lock:
            await asyncio.to_thread(self._update_lease_sync, lease, action, value, error)

    def _update_lease_sync(
        self,
        lease: WorkerLease,
        action: str,
        value: Mapping[str, Any] | None,
        error: BaseException | None,
    ) -> None:
        now = _now()
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM agent_memory_worker_tasks WHERE id=? AND status=? AND lease_token=?",
                (lease.task.id, WorkerTaskStatus.LEASED, lease.token),
            ).fetchone()
            if row is None:
                raise WorkerQueueError("worker lease is no longer active", code="stale_lease")
            if action == "checkpoint":
                if not isinstance(value, Mapping):
                    raise TypeError("checkpoint must be a mapping")
                encoded = json.dumps(to_jsonable(dict(value)), ensure_ascii=False, separators=(",", ":"))
                if len(encoded.encode("utf-8")) > 64_000:
                    raise ValueError("checkpoint exceeds 64000 bytes")
                connection.execute(
                    "UPDATE agent_memory_worker_tasks SET checkpoint_json=?, updated_at=? WHERE id=?",
                    (encoded, _iso(now), lease.task.id),
                )
                return
            if action == "complete":
                connection.execute(
                    """
                    UPDATE agent_memory_worker_tasks SET status=?, leased_by=NULL,
                    lease_token=NULL, lease_expires_at=NULL, last_error_code=NULL,
                    updated_at=? WHERE id=?
                    """,
                    (WorkerTaskStatus.COMPLETED, _iso(now), lease.task.id),
                )
                return
            if action != "fail" or error is None:
                raise ValueError("unsupported worker lease action")
            terminal = row["attempts"] >= row["max_attempts"]
            delay = min(3_600, self._limits.retry_base_seconds * (2 ** min(row["attempts"], 10)))
            retry_at = datetime.fromtimestamp(now.timestamp() + delay, timezone.utc)
            connection.execute(
                """
                UPDATE agent_memory_worker_tasks SET status=?, next_attempt_at=?,
                leased_by=NULL, lease_token=NULL, lease_expires_at=NULL,
                last_error_code=?, updated_at=? WHERE id=?
                """,
                (
                    WorkerTaskStatus.DEAD if terminal else WorkerTaskStatus.PENDING,
                    _iso(retry_at),
                    type(error).__name__[:128],
                    _iso(now),
                    lease.task.id,
                ),
            )

    async def cancel(self, task_id: str, scope: MemoryScope) -> bool:
        async with self._write_lock:
            return await asyncio.to_thread(self._cancel_sync, task_id, scope)

    def _cancel_sync(self, task_id: str, scope: MemoryScope) -> bool:
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_memory_worker_tasks SET status=?, leased_by=NULL,
                lease_token=NULL, lease_expires_at=NULL, updated_at=?
                WHERE id=? AND partition_key=? AND status IN (?, ?)
                """,
                (
                    WorkerTaskStatus.CANCELLED,
                    _iso(_now()),
                    task_id,
                    scope.partition_key(),
                    WorkerTaskStatus.PENDING,
                    WorkerTaskStatus.LEASED,
                ),
            )
            return cursor.rowcount == 1

    async def stats(self) -> WorkerQueueStats:
        return await asyncio.to_thread(self._stats_sync)

    def _stats_sync(self) -> WorkerQueueStats:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM agent_memory_worker_tasks GROUP BY status"
            ).fetchall()
            counts = {row["status"]: row["count"] for row in rows}
            expired = connection.execute(
                "SELECT COUNT(*) FROM agent_memory_worker_tasks WHERE status=? AND lease_expires_at<=?",
                (WorkerTaskStatus.LEASED, _iso(_now())),
            ).fetchone()[0]
        return WorkerQueueStats(
            pending=counts.get(WorkerTaskStatus.PENDING, 0),
            leased=counts.get(WorkerTaskStatus.LEASED, 0),
            completed=counts.get(WorkerTaskStatus.COMPLETED, 0),
            dead=counts.get(WorkerTaskStatus.DEAD, 0),
            cancelled=counts.get(WorkerTaskStatus.CANCELLED, 0),
            expired_leases=expired,
        )

    @staticmethod
    def _task_from_row(row: sqlite3.Row) -> WorkerTask:
        scope = MemoryScope(
            row["tenant_id"],
            namespace=row["namespace"],
            user_id=row["user_id"],
            agent_id=row["agent_id"],
            workspace_id=row["workspace_id"],
            session_id=row["session_id"],
        )
        return WorkerTask(
            id=row["id"],
            task_key=row["task_key"],
            scope=scope,
            task_type=row["task_type"],
            payload=json.loads(row["payload_json"]),
            status=WorkerTaskStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            next_attempt_at=datetime.fromisoformat(row["next_attempt_at"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            leased_by=row["leased_by"],
            lease_expires_at=(
                datetime.fromisoformat(row["lease_expires_at"])
                if row["lease_expires_at"]
                else None
            ),
            checkpoint=json.loads(row["checkpoint_json"]),
            last_error_code=row["last_error_code"],
        )
