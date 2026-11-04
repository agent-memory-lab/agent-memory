"""Bounded async worker runner shared by local and remote queue adapters."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass

from .worker_tasks import (
    WorkerExecutionQueue,
    WorkerFailureDisposition,
    WorkerLease,
    WorkerLimits,
    WorkerQueueError,
    WorkerTaskHandler,
    validate_handlers,
)


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WorkerBatchResult:
    claimed: int
    completed: int
    failed: int
    idle: bool
    cancelled: int = 0
    deferred: int = 0


async def _settle(operation):
    """Drain owned work even if shutdown receives repeated cancellation requests."""
    while True:
        try:
            return await asyncio.shield(operation)
        except asyncio.CancelledError:
            if operation.done():
                return operation.result()


async def _queue_call(awaitable):
    # Cancelling an await does not stop a SQLite to_thread write. Keep ownership
    # until it finishes, before releasing the slot or starting lease cleanup.
    operation = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        await _settle(asyncio.gather(operation, return_exceptions=True))
        raise


async def _run_handler(awaitable, *, timeout):
    # Timeout and caller cancellation must not independently cancel the same
    # handler while its finally block is draining. Own its task explicitly.
    operation = asyncio.create_task(awaitable)
    try:
        return await asyncio.wait_for(asyncio.shield(operation), timeout=timeout)
    finally:
        if not operation.done():
            operation.cancel()
        await _settle(asyncio.gather(operation, return_exceptions=True))
        # Cancellation during timeout cleanup takes precedence over reporting a
        # retryable timeout, and must not let the lane claim another task.
        if asyncio.current_task().cancelling():
            raise asyncio.CancelledError


class BoundedWorker:
    """Execute bounded batches without holding leases for waiting work.

    Queues may implement the optional validate_lease hook to reject stale work
    immediately before handler entry. Existing four-method execution adapters
    remain compatible and retain their own publication fences and heartbeats.
    Delivery is at least once: cancellation cannot retract side effects already
    started. Handlers must fence transactional publication and use idempotency
    keys for external effects; a pre-start check is not an exactly-once promise.
    Without host-managed renewal the task timeout must be shorter than the lease.
    Set lease_renewal_managed only when the host renews while the handler runs and
    cancels/fences publication if renewal fails, as RefreshHost does.
    """

    def __init__(
        self,
        queue: WorkerExecutionQueue,
        handlers: Mapping[str, WorkerTaskHandler],
        *,
        worker_id: str,
        limits: WorkerLimits | None = None,
        lease_renewal_managed: bool = False,
    ) -> None:
        if not isinstance(worker_id, str) or not worker_id.strip() or len(worker_id) > 128:
            raise ValueError("worker_id must contain 1 to 128 characters")
        self._queue = queue
        self._handlers = validate_handlers(handlers)
        self._worker_id = worker_id
        self._limits = limits or WorkerLimits()
        if type(lease_renewal_managed) is not bool:
            raise TypeError("lease_renewal_managed must be a bool")
        if (
            not lease_renewal_managed
            and self._limits.task_timeout_seconds >= self._limits.lease_seconds
        ):
            raise ValueError("task timeout must be shorter than the lease without managed renewal")
        # Share slots across simultaneous calls on this worker, not just a batch.
        self._slots = asyncio.Semaphore(self._limits.max_concurrency)
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
        return result.completed + result.failed + result.deferred > 0

    async def _cancel_lease(self, lease: WorkerLease, error: asyncio.CancelledError) -> None:
        # Fail is token-fenced. If cancellation raced with completion, deletion,
        # or another owner, stale_lease must never change that newer state.
        outcome = await _settle(
            asyncio.gather(self._queue.fail(lease, error), return_exceptions=True)
        )
        failure = outcome[0]
        if isinstance(failure, BaseException) and not (
            isinstance(failure, WorkerQueueError) and failure.code == "stale_lease"
        ):
            # Keep cancellation as the caller-visible outcome. A failed cleanup
            # remains recoverable through lease expiry, not a detached coroutine.
            error.add_note(f"Worker lease cleanup failed: {type(failure).__name__}")
            _LOGGER.warning(
                "Worker lease cleanup failed (%s); recovery requires lease expiry",
                type(failure).__name__,
            )

    async def _claim(self) -> WorkerLease | None:
        operation = asyncio.create_task(
            self._queue.claim(self._worker_id, lease_seconds=self._limits.lease_seconds)
        )
        try:
            return await asyncio.shield(operation)
        except asyncio.CancelledError as error:
            outcome = await _settle(asyncio.gather(operation, return_exceptions=True))
            if isinstance(outcome[0], WorkerLease):
                await self._cancel_lease(outcome[0], error)
            raise

    async def _execute(self, lease: WorkerLease) -> str:
        try:
            handler = self._handlers[lease.task.task_type]

            async def invoke():
                validate = getattr(self._queue, "validate_lease", None)
                if validate is not None:
                    await _queue_call(validate(lease))

                async def checkpoint(value):
                    await _queue_call(self._queue.checkpoint(lease, value))

                await handler(lease.task, checkpoint)

            await _run_handler(invoke(), timeout=self._limits.task_timeout_seconds)
            await _queue_call(self._queue.complete(lease))
            return "completed"
        except asyncio.CancelledError as error:
            await self._cancel_lease(lease, error)
            raise
        except Exception as error:
            if isinstance(error, WorkerQueueError) and error.code == "stale_lease":
                return "cancelled"
            try:
                disposition = await _queue_call(self._queue.fail(lease, error))
            except asyncio.CancelledError as cancelled:
                await self._cancel_lease(lease, cancelled)
                raise
            except WorkerQueueError as queue_error:
                if queue_error.code == "stale_lease":
                    return "cancelled"
                raise
            return "deferred" if disposition == WorkerFailureDisposition.DEFERRED else "failed"

    async def run_batch(self, *, max_tasks: int | None = None) -> WorkerBatchResult:
        if self.paused:
            return WorkerBatchResult(0, 0, 0, True)
        requested = max_tasks if max_tasks is not None else self._limits.max_batch_size
        if type(requested) is not int or not 1 <= requested <= self._limits.max_batch_size:
            raise ValueError("max_tasks is outside the configured batch limit")
        remaining, claimed = requested, 0
        exhausted = False
        outcomes: list[str] = []

        async def lane():
            nonlocal remaining, claimed, exhausted
            while True:
                async with self._slots:
                    if remaining == 0 or exhausted or self.paused:
                        return
                    remaining -= 1
                    lease = await self._claim()
                    if lease is None:
                        exhausted = True
                        return
                    claimed += 1
                    outcomes.append(await self._execute(lease))

        lanes = [
            asyncio.create_task(lane())
            for _ in range(min(requested, self._limits.max_concurrency))
        ]
        joined = asyncio.gather(*lanes)
        try:
            await asyncio.shield(joined)
        except BaseException:
            # gather does not stop siblings on a queue/handler infrastructure
            # failure. Cancel and drain them, including in-flight durable claims.
            for task in lanes:
                if not task.done() and not task.cancelling():
                    task.cancel()
            await _settle(asyncio.gather(*lanes, joined, return_exceptions=True))
            raise
        return WorkerBatchResult(
            claimed=claimed,
            completed=outcomes.count("completed"),
            failed=outcomes.count("failed"),
            idle=claimed == 0,
            cancelled=outcomes.count("cancelled"),
            deferred=outcomes.count("deferred"),
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
