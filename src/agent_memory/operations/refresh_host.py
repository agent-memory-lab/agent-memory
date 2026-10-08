"""Bounded host for durable refresh: hints, lease liveness, and graceful drain."""

import asyncio
import time
from dataclasses import dataclass

from ..derived.model import DerivedError, timestamp
from .worker_runtime import BoundedWorker
from .worker_tasks import WorkerLease, WorkerLimits


@dataclass(frozen=True, slots=True)
class RefreshHealth:
    state: str
    clock_healthy: bool
    last_poll_at: str | None
    reason: str | None


class RefreshHost:
    def __init__(
        self,
        queue,
        *,
        worker_id,
        limits=None,
        poll_seconds=1.0,
        heartbeat_seconds=None,
        monotonic=time.monotonic,
        clock_tolerance_seconds=5.0,
    ):
        self.queue = queue
        self.limits = limits or WorkerLimits()
        if not 0.01 <= poll_seconds <= 300 or not 0 <= clock_tolerance_seconds <= 300:
            raise ValueError("invalid refresh host timings")
        heartbeat_seconds = heartbeat_seconds or self.limits.lease_seconds / 3
        if not 0 < heartbeat_seconds < self.limits.lease_seconds:
            raise ValueError("heartbeat must precede lease expiry")
        self.poll_seconds, self.heartbeat_seconds = poll_seconds, heartbeat_seconds
        self.monotonic, self.clock_tolerance_seconds = monotonic, clock_tolerance_seconds
        self._last_wall, self._last_mono = None, None
        self._stop, self._notify = asyncio.Event(), asyncio.Event()
        self._initialized = False
        self._health = RefreshHealth("stopped", True, None, None)
        self.worker = BoundedWorker(
            queue,
            {p.task_type: self._apply for p in queue.processors.values()},
            worker_id=worker_id,
            limits=self.limits,
        )

    @property
    def health(self):
        return self._health

    def notify(self):
        self._notify.set()  # This hint can be lost; indexed polling remains authoritative.

    def stop(self):
        self._initialized = False
        self.queue.stopping = True
        self._stop.set()
        self._notify.set()

    def _clock_ok(self):
        wall, mono = timestamp(self.queue.clock()), self.monotonic()
        okay = self._last_wall is None or (
            wall >= self._last_wall
            and abs((wall - self._last_wall).total_seconds() - (mono - self._last_mono))
            <= self.clock_tolerance_seconds
        )
        # Retain a wall-clock high water mark until rollback catches up.
        self._last_wall = max(wall, self._last_wall) if self._last_wall is not None else wall
        self._last_mono = mono
        self._health = RefreshHealth(
            "running" if okay else "degraded",
            okay,
            wall.isoformat(),
            None if okay else "refresh_clock_discontinuity",
        )
        return okay

    async def _apply(self, task, checkpoint):
        lease = WorkerLease(task, task.payload["fence"])
        work = asyncio.create_task(self.queue.apply(task, checkpoint))

        async def renew():
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                if not self._clock_ok():
                    raise DerivedError("refresh_clock_discontinuity")
                await self.queue.heartbeat(lease, lease_seconds=self.limits.lease_seconds)

        heartbeat = asyncio.create_task(renew())
        try:
            done, _ = await asyncio.wait((work, heartbeat), return_when=asyncio.FIRST_COMPLETED)
            if work in done:
                # Publication and a queued renewal can finish together. Let the
                # worker verify committed coverage rather than discard success
                # merely because the now-completed lease no longer renews.
                return await work
            if heartbeat in done:
                # A failed lease renewal cannot leave a processor publishing unsafely.
                await heartbeat
                raise DerivedError("refresh_heartbeat_stopped")
            return await work
        finally:
            heartbeat.cancel()
            if not work.done():
                work.cancel()
            await asyncio.gather(work, heartbeat, return_exceptions=True)

    async def run_once(self):
        if self._stop.is_set():
            return None
        if not self._initialized:
            try:
                await self.queue.initialize()
            except DerivedError as error:
                if error.code != "refresh_clock_discontinuity":
                    raise
                self._health = RefreshHealth(
                    "degraded", False, self.queue.clock().isoformat(), error.code
                )
                return None
            if self._stop.is_set():
                self.queue.stopping = True
                return None
            self._initialized = True
        if not self._clock_ok():
            return None
        # Claim only available execution slots, not an entire batch waiting on a
        # semaphore while already holding scarce leases and database reservations.
        try:
            return await self.worker.run_batch(max_tasks=self.limits.max_concurrency)
        except DerivedError as error:
            if error.code != "refresh_clock_discontinuity":
                raise
            self._initialized = False
            self._health = RefreshHealth(
                "degraded", False, self.queue.clock().isoformat(), error.code
            )
            return None

    async def run(self):
        # Initialization must succeed on this run before health can become
        # running. Retrying also clears a reused queue's stopped flag only after
        # the durable wall floor and backend contracts pass.
        self._initialized = False
        self._stop.clear()
        try:
            while not self._stop.is_set():
                self._notify.clear()
                result = await self.run_once()
                if result is None or result.idle:
                    try:
                        await asyncio.wait_for(self._notify.wait(), timeout=self.poll_seconds)
                    except TimeoutError:
                        pass
        finally:
            self.queue.stopping = True
            self._health = RefreshHealth(
                "stopped",
                self._health.clock_healthy,
                self._health.last_poll_at,
                self._health.reason,
            )
