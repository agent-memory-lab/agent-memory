"""Host-authored capture coverage, independent of model extraction coverage.

Profiles and observations come from an installed host adapter, never a model
or incoming tool arguments. The transport entry point rejects this namespace.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from ..lifecycle import CAPTURE_METADATA_KEY, LifecycleEvent, LifecycleEventType, LifecycleOrigin


def _identifier(value, name):
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ValueError(f"{name} must be a nonempty bounded string")
    return value


def _tuple(values, name, maximum=128):
    if not isinstance(values, (list, tuple)) or len(values) > maximum:
        raise ValueError(f"{name} must be a bounded sequence")
    values = tuple(_identifier(value, name) for value in values)
    if len(set(values)) != len(values):
        raise ValueError(f"{name} must be unique")
    return values


@dataclass(frozen=True, slots=True)
class CaptureProfile:
    profile_id: str
    version: str
    host: str
    host_version: str
    event_types: tuple[str, ...]
    origins: tuple[str, ...]
    content_types: tuple[str, ...]
    sanitization_policy: str
    allowed_omissions: tuple[str, ...] = ()

    def __post_init__(self):
        for name in ("profile_id", "version", "host", "host_version", "sanitization_policy"):
            _identifier(getattr(self, name), name)
        for name in ("event_types", "origins", "content_types", "allowed_omissions"):
            values = _tuple(getattr(self, name), name)
            if name != "allowed_omissions" and not values:
                raise ValueError(f"{name} cannot be empty")
            object.__setattr__(self, name, values)
        for value in self.event_types:
            LifecycleEventType(value)
        for value in self.origins:
            LifecycleOrigin(value)
        if not set(self.content_types) <= {"text", "tool_result"}:
            raise ValueError("this capture profile version supports text and tool_result only")


@dataclass(frozen=True, slots=True)
class CaptureOmission:
    field: str
    reason: str

    def __post_init__(self):
        _identifier(self.field, "field")
        _identifier(self.reason, "reason")


@dataclass(frozen=True, slots=True)
class HostCaptureObservation:
    """Evidence supplied out of band by the trusted host for ONE event.

    capture_complete is True/False/None; None means the host cannot assess it.
    An injected memory is context, never independent confirmation.
    """

    host_event_id: str
    message_id: str
    source_revision_id: str
    source_family: str
    event_type: LifecycleEventType
    origin: LifecycleOrigin
    occurred_at: datetime
    content_types: tuple[str, ...]
    capture_complete: bool | None = None
    omissions: tuple[CaptureOmission, ...] = ()
    memory_injection: bool = False
    parent_revision_ids: tuple[str, ...] = ()
    parent_receipt_ids: tuple[str, ...] = ()

    def __post_init__(self):
        for name in ("host_event_id", "message_id", "source_revision_id", "source_family"):
            _identifier(getattr(self, name), name)
        object.__setattr__(self, "event_type", LifecycleEventType(self.event_type))
        object.__setattr__(self, "origin", LifecycleOrigin(self.origin))
        if not isinstance(self.occurred_at, datetime) or self.occurred_at.utcoffset() is None:
            raise ValueError("host occurrence time must include a timezone")
        for name in ("content_types", "parent_revision_ids", "parent_receipt_ids"):
            object.__setattr__(self, name, _tuple(getattr(self, name), name))
        if self.capture_complete is not None and type(self.capture_complete) is not bool:
            raise ValueError("capture_complete must be true, false or None")
        if type(self.memory_injection) is not bool:
            raise ValueError("memory_injection must be a boolean")
        if not isinstance(self.omissions, (tuple, list)) or len(self.omissions) > 128:
            raise ValueError("omissions must be a bounded sequence")
        if any(not isinstance(value, CaptureOmission) for value in self.omissions):
            raise TypeError("invalid omission")
        object.__setattr__(self, "omissions", tuple(self.omissions))
        if len({value.field for value in self.omissions}) != len(self.omissions):
            raise ValueError("omission fields must be unique")
        if self.omissions and self.capture_complete is not False:
            raise ValueError("known omissions require capture_complete=False")
        if self.memory_injection and (not self.parent_revision_ids or not self.parent_receipt_ids):
            raise ValueError("injected memory must reference its original revisions and receipts")
        if self.source_revision_id in self.parent_revision_ids:
            raise ValueError("a capture revision cannot depend on itself")


def annotate_capture(
    event: LifecycleEvent,
    profile: CaptureProfile,
    observation: HostCaptureObservation,
) -> LifecycleEvent:
    """Attach trusted metadata without altering the published lifecycle schema."""
    if not isinstance(event, LifecycleEvent) or not isinstance(profile, CaptureProfile):
        raise TypeError("capture requires a LifecycleEvent and CaptureProfile")
    if not isinstance(observation, HostCaptureObservation):
        raise TypeError("observation must come from a trusted host adapter")
    if CAPTURE_METADATA_KEY in event.payload:
        raise ValueError("reserved capture metadata cannot come from the event payload")
    if (
        event.event_type != observation.event_type
        or event.origin != observation.origin
        or event.occurred_at != observation.occurred_at
    ):
        raise ValueError("event role, kind or time differs from trusted host observation")
    if (
        event.event_type.value not in profile.event_types
        or event.origin.value not in profile.origins
    ):
        raise ValueError("event is outside the declared capture profile")
    if not set(observation.content_types) <= set(profile.content_types):
        raise ValueError("content types are outside the declared capture profile")
    if not {item.field for item in observation.omissions} <= set(profile.allowed_omissions):
        raise ValueError("omission is not declared by the capture profile")
    if event.content and "text" not in observation.content_types:
        raise ValueError("nonempty text must be declared in content_types")
    if "result" in event.payload and "tool_result" not in observation.content_types:
        raise ValueError("tool results must be declared in content_types")
    if "tool_result" in observation.content_types and "result" not in event.payload:
        if not any(item.field == "payload.result" for item in observation.omissions):
            raise ValueError("missing tool result requires an explicit omission")
    from ..serialization import to_jsonable

    metadata = {
        "schema_version": 1,
        "profile": to_jsonable(profile),
        "observation": to_jsonable(observation),
        "evidence_role": "memory_context"
        if observation.memory_injection or observation.parent_revision_ids
        else (
            "assistant_claim"
            if observation.origin is LifecycleOrigin.MODEL
            else (
                "tool_result"
                if observation.event_type
                in {
                    LifecycleEventType.TOOL_COMPLETED,
                    LifecycleEventType.TOOL_FAILED,
                }
                else "source"
            )
        ),
        "independent_confirmation": False
        if observation.memory_injection or observation.parent_revision_ids
        else None,
    }
    return replace(event, payload={**event.payload, CAPTURE_METADATA_KEY: metadata})


@dataclass(frozen=True, slots=True)
class CaptureCoverage:
    expected_count: int | None
    captured_count: int
    coverage: float | None
    missing_event_ids: tuple[str, ...]


def measure_capture_coverage(
    host_event_ids: tuple[str, ...] | None,
    captured_event_ids: tuple[str, ...],
) -> CaptureCoverage:
    """Only a trusted host inventory can supply the coverage denominator."""
    captured = _tuple(captured_event_ids, "captured_event_ids", 100_000)
    if host_event_ids is None:
        return CaptureCoverage(None, len(captured), None, ())
    expected = _tuple(host_event_ids, "host_event_ids", 100_000)
    if not set(captured) <= set(expected):
        raise ValueError("captured IDs are outside the host inventory")
    missing = tuple(sorted(set(expected) - set(captured)))
    return CaptureCoverage(
        len(expected), len(captured), len(captured) / len(expected) if expected else None, missing
    )
