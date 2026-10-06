"""One identity-bound event parser for embedded and MCP capture surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..domain import MemoryScope
from ..lifecycle import LifecycleEvent, LifecycleEventError
from .profile import (
    CAPTURE_METADATA_KEY,
    CaptureProfile,
    HostCaptureObservation,
    annotate_capture,
)
from .sink import CaptureError, CaptureSink, CaptureSubmission


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
    if CAPTURE_METADATA_KEY in lifecycle.payload:
        raise LifecycleEventError("capture metadata requires a trusted host adapter", field="payload")
    submission = await sink.submit(lifecycle)
    if not isinstance(submission, CaptureSubmission) or submission.event_id != lifecycle.event_id:
        raise CaptureError("capture sink returned an invalid receipt", code="capture_sink_contract")
    return submission


async def submit_profiled_capture(
    data: Mapping[str, Any],
    *,
    sink: CaptureSink,
    scope: MemoryScope,
    actor: str,
    profile: CaptureProfile,
    observation: HostCaptureObservation,
) -> CaptureSubmission:
    """Opt-in host API; profile/observation must not be decoded from tool input."""
    if not isinstance(data, Mapping):
        raise LifecycleEventError("capture event must be an object", field="event")
    envelope = {**data, "actor": actor}
    lifecycle = LifecycleEvent.from_dict(envelope, trusted_scope=scope)
    annotated = annotate_capture(lifecycle, profile, observation)
    submission = await sink.submit(annotated)
    if not isinstance(submission, CaptureSubmission) or submission.event_id != lifecycle.event_id:
        raise CaptureError("capture sink returned an invalid receipt", code="capture_sink_contract")
    return submission
