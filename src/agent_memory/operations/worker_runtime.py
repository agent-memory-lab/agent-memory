"""Bounded async worker runner shared by local and remote queue adapters."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Mapping

from .worker_tasks import (
    WorkerLease,
    WorkerLimits,
    WorkerQueue,
    WorkerQueueError,
    WorkerTaskHandler,
    validate_handlers,
)


@dataclass(frozen=True, slots=True)
class WorkerBatchResult:
    claimed: int
    completed: int
    failed: int
    idle: bool
    cancelled: int = 0


class BoundedWorker:
    """Execute bounded batches; queue leases remain the source of truth."""

    def __init__(
        self,
        queue: WorkerQueue,
        handlers: Mapping[str, WorkerTaskHandler],
        *,
        worker_id: str,
        limits: WorkerLimits | None = None,
    ) -> None:
        if not isinstance(worker_id, str) or not worker_id.strip() or len(worker_id) > 128:
            raise ValueError("worker_id must contain 1 to 128 characters")
        self._queue = queue
        self._handlers = validate_handlers(handlers)
        self._worker_id = worker_id
        self._limits = limits or WorkerLimits()
        self._paused = asyncio.Event()
        self._paused.set()

    @property
    def paused(self) -> bool:
        return not self._paused.is_set()

    def pause(self) -> None:
        self._paused.clear()

    def resume(self) -> None:
        self._paused.set()

    async def run_once(self) -> bool:
        result = await self.run_batch(max_tasks=1)
        return result.completed + result.failed > 0

    async def run_batch(self, *, max_tasks: int | None = None) -> WorkerBatchResult:
        if self.paused:
            return WorkerBatchResult(0, 0, 0, True)
        requested = max_tasks if max_tasks is not None else self._limits.max_batch_size
        if type(requested) is not int or not 1 <= requested <= self._limits.max_batch_size:
            raise ValueError("max_tasks is outside the configured batch limit")
        leases: list[WorkerLease] = []
        for _ in range(requested):
            lease = await self._queue.claim(
                self._worker_id,
                lease_seconds=self._limits.lease_seconds,
            )
            if lease is None:
                break
            leases.append(lease)
        if not leases:
            return WorkerBatchResult(0, 0, 0, True)

        semaphore = asyncio.Semaphore(self._limits.max_concurrency)

        async def execute(lease: WorkerLease) -> str:
            async with semaphore:
                try:
                    handler = self._handlers[lease.task.task_type]

                    async def checkpoint(value):
                        await self._queue.checkpoint(lease, value)

                    await asyncio.wait_for(
                        handler(lease.task, checkpoint),
                        timeout=self._limits.task_timeout_seconds,
                    )
                    try:
                        await self._queue.complete(lease)
                    except WorkerQueueError as error:
                        if error.code == "stale_lease":
                            return "cancelled"
                        raise
                    return "completed"
                except BaseException as error:
                    if isinstance(error, (KeyboardInterrupt, SystemExit)):
                        raise
                    try:
                        await self._queue.fail(lease, error)
                    except WorkerQueueError as queue_error:
                        if queue_error.code == "stale_lease":
                            return "cancelled"
                        raise
                    return "failed"

        outcomes = await asyncio.gather(*(execute(lease) for lease in leases))
        completed = outcomes.count("completed")
        cancelled = outcomes.count("cancelled")
        return WorkerBatchResult(
            claimed=len(leases),
            completed=completed,
            failed=outcomes.count("failed"),
            idle=False,
            cancelled=cancelled,
        )

    async def run_forever(
        self,
        stop: asyncio.Event,
        *,
        idle_seconds: float = 1.0,
    ) -> None:
        if not isinstance(stop, asyncio.Event):
            raise TypeError("stop must be an asyncio.Event")
        if not isinstance(idle_seconds, (int, float)) or not 0.01 <= idle_seconds <= 300:
            raise ValueError("idle_seconds must be between 0.01 and 300")
        while not stop.is_set():
            if self.paused:
                await self._wait_or_stop(stop, idle_seconds)
                continue
            result = await self.run_batch()
            if result.idle:
                await self._wait_or_stop(stop, idle_seconds)

    @staticmethod
    async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except TimeoutError:
            pass
