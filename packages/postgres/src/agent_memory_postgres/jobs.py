from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Awaitable, Callable, Mapping
from uuid import uuid4

from agent_memory import IngestResult, MemoryEvent, MemoryScope
from agent_memory.serialization import to_jsonable


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    DEAD = "dead"


@dataclass(frozen=True, slots=True)
class ConsolidationJob:
    id: str
    job_key: str
    scope: MemoryScope
    job_type: str
    payload: Mapping[str, Any]
    status: JobStatus
    attempts: int
    max_attempts: int
    next_attempt_at: datetime
    leased_by: str | None = None
    lease_expires_at: datetime | None = None


JobHandler = Callable[[ConsolidationJob], Awaitable[None]]


class PostgresConsolidationQueue:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def enqueue_event(self, event: MemoryEvent, result: IngestResult) -> str:
        return await self.enqueue(
            event.scope,
            job_key=f"event:{event.id}:consolidate",
            job_type="memory.consolidate",
            payload={
                "event_id": event.id,
                "claim_ids": result.claim_ids,
                "state_delta_ids": result.state_delta_ids,
            },
        )

    async def enqueue(
        self,
        scope: MemoryScope,
        *,
        job_key: str,
        job_type: str,
        payload: Mapping[str, Any],
        max_attempts: int = 5,
    ) -> str:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        job_id = str(uuid4())
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                """
                INSERT INTO agent_memory_consolidation_jobs (
                    id, job_key, partition_key, tenant_id, namespace, user_id,
                    agent_id, workspace_id, session_id, job_type, payload_json, max_attempts
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
                )
                ON CONFLICT (job_key) DO UPDATE SET job_key = excluded.job_key
                RETURNING id
                """,
                (
                    job_id,
                    job_key,
                    scope.partition_key(),
                    scope.tenant_id,
                    scope.namespace,
                    scope.user_id,
                    scope.agent_id,
                    scope.workspace_id,
                    scope.session_id,
                    job_type,
                    json.dumps(to_jsonable(payload), ensure_ascii=False),
                    max_attempts,
                ),
            )
            return (await cursor.fetchone())["id"]

    async def claim(self, worker_id: str, *, lease_seconds: int = 60) -> ConsolidationJob | None:
        if lease_seconds < 5:
            raise ValueError("lease_seconds must be at least 5")
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    WITH candidate AS (
                        SELECT id
                        FROM agent_memory_consolidation_jobs
                        WHERE (
                            status = 'pending' AND next_attempt_at <= now()
                        ) OR (
                            status = 'running' AND lease_expires_at < now()
                        )
                        ORDER BY next_attempt_at, created_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE agent_memory_consolidation_jobs AS job
                    SET status = 'running',
                        attempts = attempts + 1,
                        leased_by = %s,
                        lease_expires_at = now() + make_interval(secs => %s),
                        updated_at = now()
                    FROM candidate
                    WHERE job.id = candidate.id
                    RETURNING job.*
                    """,
                    (worker_id, lease_seconds),
                )
                row = await cursor.fetchone()
                return self._job_from_row(row) if row else None

    async def complete(self, job: ConsolidationJob, worker_id: str) -> None:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE agent_memory_consolidation_jobs
                SET status = 'completed', leased_by = NULL, lease_expires_at = NULL,
                    last_error = NULL, updated_at = now()
                WHERE id = %s AND status = 'running' AND leased_by = %s
                """,
                (job.id, worker_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("job lease was lost before completion")

    async def fail(self, job: ConsolidationJob, worker_id: str, error: BaseException) -> None:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE agent_memory_consolidation_jobs
                SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
                    next_attempt_at = now() + make_interval(
                        secs => LEAST(3600, CAST(power(2, LEAST(attempts, 10)) AS integer))
                    ),
                    leased_by = NULL,
                    lease_expires_at = NULL,
                    last_error = %s,
                    updated_at = now()
                WHERE id = %s AND status = 'running' AND leased_by = %s
                """,
                (str(error)[:4000], job.id, worker_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("job lease was lost before failure recording")

    @staticmethod
    def _job_from_row(row: Mapping[str, Any]) -> ConsolidationJob:
        payload = row["payload_json"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return ConsolidationJob(
            id=row["id"],
            job_key=row["job_key"],
            scope=MemoryScope(
                tenant_id=row["tenant_id"],
                namespace=row["namespace"],
                user_id=row["user_id"],
                agent_id=row["agent_id"],
                workspace_id=row["workspace_id"],
                session_id=row["session_id"],
            ),
            job_type=row["job_type"],
            payload=payload,
            status=JobStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            next_attempt_at=row["next_attempt_at"],
            leased_by=row["leased_by"],
            lease_expires_at=row["lease_expires_at"],
        )


class ConsolidationWorker:
    def __init__(
        self,
        queue: PostgresConsolidationQueue,
        handlers: Mapping[str, JobHandler],
        *,
        worker_id: str,
        lease_seconds: int = 60,
    ) -> None:
        self._queue = queue
        self._handlers = dict(handlers)
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds

    async def run_once(self) -> bool:
        job = await self._queue.claim(self._worker_id, lease_seconds=self._lease_seconds)
        if job is None:
            return False
        try:
            handler = self._handlers[job.job_type]
            await handler(job)
        except Exception as error:
            await self._queue.fail(job, self._worker_id, error)
        else:
            await self._queue.complete(job, self._worker_id)
        return True

    async def run_forever(
        self,
        stop: asyncio.Event,
        *,
        idle_seconds: float = 1.0,
    ) -> None:
        while not stop.is_set():
            processed = await self.run_once()
            if not processed:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=idle_seconds)
                except TimeoutError:
                    pass
