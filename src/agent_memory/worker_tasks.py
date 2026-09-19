"""Provider-neutral contracts for bounded, recoverable background work."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol, Sequence

from .domain import MemoryScope


class WorkerTaskStatus(StrEnum):
    PENDING = "pending"
    LEASED = "leased"
    COMPLETED = "completed"
    DEAD = "dead"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class WorkerLimits:
    max_pending_per_scope: int = 128
    max_pending_global: int = 4_096
    max_attempts: int = 3
    lease_seconds: int = 60
    retry_base_seconds: int = 2
    max_batch_size: int = 8
    max_concurrency: int = 2
    task_timeout_seconds: int = 30

    def __post_init__(self) -> None:
        values = {
            "max_pending_per_scope": (self.max_pending_per_scope, 1, 100_000),
            "max_pending_global": (self.max_pending_global, 1, 1_000_000),
            "max_attempts": (self.max_attempts, 1, 100),
            "lease_seconds": (self.lease_seconds, 5, 86_400),
            "retry_base_seconds": (self.retry_base_seconds, 1, 3_600),
            "max_batch_size": (self.max_batch_size, 1, 256),
            "max_concurrency": (self.max_concurrency, 1, 64),
            "task_timeout_seconds": (self.task_timeout_seconds, 1, 86_400),
        }
        for name, (value, minimum, maximum) in values.items():
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(f"{name} must be between {minimum} and {maximum}")
        if self.max_concurrency > self.max_batch_size:
            raise ValueError("max_concurrency cannot exceed max_batch_size")
        if self.max_pending_per_scope > self.max_pending_global:
            raise ValueError("per-scope capacity cannot exceed global capacity")


@dataclass(frozen=True, slots=True)
class WorkerTask:
    id: str
    task_key: str
    scope: MemoryScope
    task_type: str
    payload: Mapping[str, Any]
    status: WorkerTaskStatus
    attempts: int
    max_attempts: int
    next_attempt_at: datetime
    created_at: datetime
    updated_at: datetime
    leased_by: str | None = None
    lease_expires_at: datetime | None = None
    checkpoint: Mapping[str, Any] = field(default_factory=dict)
    last_error_code: str | None = None


@dataclass(frozen=True, slots=True)
class WorkerLease:
    task: WorkerTask
    token: str


@dataclass(frozen=True, slots=True)
class WorkerQueueStats:
    pending: int
    leased: int
    completed: int
    dead: int
    cancelled: int
    expired_leases: int


class WorkerQueueError(RuntimeError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


class WorkerQueue(Protocol):
    async def initialize(self) -> None: ...

    async def enqueue(
        self,
        task_key: str,
        scope: MemoryScope,
        task_type: str,
        payload: Mapping[str, Any],
        *,
        max_attempts: int | None = None,
    ) -> str: ...

    async def claim(self, worker_id: str, *, lease_seconds: int) -> WorkerLease | None: ...

    async def checkpoint(self, lease: WorkerLease, value: Mapping[str, Any]) -> None: ...

    async def complete(self, lease: WorkerLease) -> None: ...

    async def fail(self, lease: WorkerLease, error: BaseException) -> None: ...

    async def cancel(self, task_id: str, scope: MemoryScope) -> bool: ...

    async def stats(self) -> WorkerQueueStats: ...


TaskCheckpoint = Callable[[Mapping[str, Any]], Awaitable[None]]
WorkerTaskHandler = Callable[[WorkerTask, TaskCheckpoint], Awaitable[None]]


def validate_handlers(
    handlers: Mapping[str, WorkerTaskHandler],
) -> dict[str, WorkerTaskHandler]:
    if not isinstance(handlers, Mapping) or not handlers:
        raise ValueError("handlers must be a non-empty mapping")
    validated: dict[str, WorkerTaskHandler] = {}
    for task_type, handler in handlers.items():
        if not isinstance(task_type, str) or not task_type.strip() or len(task_type) > 128:
            raise ValueError("task type must contain 1 to 128 characters")
        if not callable(handler):
            raise TypeError("worker handler must be callable")
        validated[task_type] = handler
    return validated
