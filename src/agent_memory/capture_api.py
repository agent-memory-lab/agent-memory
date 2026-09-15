"""One identity-bound event parser for embedded and MCP capture surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .capture_sink import CaptureError, CaptureSink, CaptureSubmission
from .domain import MemoryScope
from .lifecycle import LifecycleEvent, LifecycleEventError


async def submit_capture(
    data: Mapping[str, Any],
    *,
    sink: CaptureSink,
    scope: MemoryScope,
    actor: str,
) -> CaptureSubmission:
    if not isinstance(data, Mapping):
        raise LifecycleEventError("capture event must be an object", field="event")
    envelope = dict(data)
    envelope["actor"] = actor
    lifecycle = LifecycleEvent.from_dict(envelope, trusted_scope=scope)
    submission = await sink.submit(lifecycle)
    if not isinstance(submission, CaptureSubmission) or submission.event_id != lifecycle.event_id:
        raise CaptureError("capture sink returned an invalid receipt", code="capture_sink_contract")
    return submission
