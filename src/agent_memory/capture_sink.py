"""Small, host-injected capture port and two optional local implementations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .capture_policy import CapturePolicyError, CaptureSanitizer
from .capture_queue import CaptureQueueError, SQLiteCaptureQueue
from .lifecycle import LifecycleEvent
from .ports import MemoryProvider


class CaptureError(ValueError):
    def __init__(self, message: str, *, code: str, field: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.field = field


@dataclass(frozen=True, slots=True)
class CaptureSubmission:
    event_id: str
    status: str
    duplicate: bool = False
    queue_id: int | None = None
    provider_event_id: str | None = None


class CaptureSink(Protocol):
    """The host selects a sink; the sink consumes trusted, sanitized evidence.

    Implementations must keep evidence within event.scope, redact before
    persistence and return an accurate admission/completion status. SDK and
    framework adapters never grant scope or feedback authority to payloads.
    """

    async def submit(self, event: LifecycleEvent) -> CaptureSubmission: ...


class QueuedCaptureSink:
    """Durable admission; Provider ingestion occurs only in a separate worker."""

    def __init__(self, queue: SQLiteCaptureQueue) -> None:
        self.queue = queue

    async def submit(self, event: LifecycleEvent) -> CaptureSubmission:
        try:
            receipt = await self.queue.enqueue(event)
        except CaptureQueueError as error:
            raise CaptureError(str(error), code=error.code) from error
        except CapturePolicyError as error:
            raise CaptureError(str(error), code="capture_rejected", field=error.field) from error
        except Exception as error:
            raise CaptureError("capture storage failed", code="capture_storage_failed") from error
        return CaptureSubmission(
            event_id=receipt.event_id,
            status=receipt.status,
            duplicate=receipt.duplicate,
            queue_id=receipt.queue_id,
        )


class DirectCaptureSink:
    """Explicit synchronous alternative without the queue's crash recovery."""

    def __init__(self, provider: MemoryProvider, sanitizer: CaptureSanitizer) -> None:
        self.provider = provider
        self.sanitizer = sanitizer

    async def submit(self, event: LifecycleEvent) -> CaptureSubmission:
        try:
            result = await self.sanitizer.ingest(event, self.provider)
        except CapturePolicyError as error:
            raise CaptureError(str(error), code="capture_rejected", field=error.field) from error
        except Exception as error:
            raise CaptureError("capture storage failed", code="capture_storage_failed") from error
        return CaptureSubmission(
            event_id=event.event_id,
            status="done",
            duplicate=result.duplicate,
            provider_event_id=result.event_id,
        )
