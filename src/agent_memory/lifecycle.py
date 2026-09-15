from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from hashlib import sha256
import json
import math
from types import MappingProxyType
from typing import Any

from .domain import MemoryEvent, MemoryScope

LIFECYCLE_SCHEMA_VERSION = 1


class LifecycleEventType(StrEnum):
    TURN_STARTED = "turn.started"
    MESSAGE_RECEIVED = "message.received"
    TOOL_CALLED = "tool.called"
    TOOL_COMPLETED = "tool.completed"
    TOOL_FAILED = "tool.failed"
    DECISION_MADE = "decision.made"
    OUTCOME_RECEIVED = "outcome.received"
    EVALUATION_RECEIVED = "evaluation.received"
    REWARD_RECEIVED = "reward.received"
    TURN_COMPLETED = "turn.completed"


class LifecycleOrigin(StrEnum):
    HOST = "host"
    USER = "user"
    MODEL = "model"
    TOOL = "tool"


_HOST_ONLY_TYPES = frozenset(
    {
        LifecycleEventType.DECISION_MADE,
        LifecycleEventType.OUTCOME_RECEIVED,
        LifecycleEventType.EVALUATION_RECEIVED,
        LifecycleEventType.REWARD_RECEIVED,
    }
)


class LifecycleEventError(ValueError):
    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field


def _text(value: object, field_name: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise LifecycleEventError(f"{field_name} must be a string", field=field_name)
    return value


def _optional_text(value: object, field_name: str) -> str | None:
    return None if value is None else _text(value, field_name)


def _freeze_payload(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise LifecycleEventError("payload keys must be strings", field="payload")
        return MappingProxyType({key: _freeze_payload(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_payload(item) for item in value)
    raise LifecycleEventError("payload must contain only finite JSON values", field="payload")


def _thaw_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_payload(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_payload(item) for item in value]
    return value


def _timestamp(value: object) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise LifecycleEventError("occurred_at must be ISO 8601", field="occurred_at") from error
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise LifecycleEventError("occurred_at must include a timezone", field="occurred_at")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    """Host-scoped evidence envelope; never grants authority to its payload."""

    scope: MemoryScope
    event_id: str
    event_type: LifecycleEventType
    origin: LifecycleOrigin
    occurred_at: datetime
    run_id: str
    content: str = ""
    trace_id: str | None = None
    turn_id: str | None = None
    actor: str = "agent"
    payload: Mapping[str, Any] = field(default_factory=dict)
    schema_version: int = LIFECYCLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.scope, MemoryScope):
            raise LifecycleEventError("scope must come from a trusted host", field="scope")
        if type(self.schema_version) is not int or self.schema_version != LIFECYCLE_SCHEMA_VERSION:
            raise LifecycleEventError("unsupported lifecycle schema version", field="schema_version")
        _text(self.event_id, "event_id")
        _text(self.run_id, "run_id")
        _text(self.content, "content", allow_empty=True)
        _text(self.actor, "actor")
        _optional_text(self.trace_id, "trace_id")
        _optional_text(self.turn_id, "turn_id")
        try:
            event_type = LifecycleEventType(self.event_type)
            origin = LifecycleOrigin(self.origin)
        except (TypeError, ValueError) as error:
            raise LifecycleEventError("unknown lifecycle event type or origin") from error
        if event_type in _HOST_ONLY_TYPES and origin is not LifecycleOrigin.HOST:
            raise LifecycleEventError(
                "decision, outcome, evaluation and reward signals require host origin",
                field="origin",
            )
        if not isinstance(self.payload, Mapping):
            raise LifecycleEventError("payload must be an object", field="payload")
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "origin", origin)
        object.__setattr__(self, "occurred_at", _timestamp(self.occurred_at))
        object.__setattr__(self, "payload", _freeze_payload(self.payload))

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        trusted_scope: MemoryScope,
        allow_host_feedback: bool = False,
    ) -> LifecycleEvent:
        """Parse transport data without accepting a caller-supplied scope."""

        if not isinstance(data, Mapping) or any(not isinstance(key, str) for key in data):
            raise LifecycleEventError("lifecycle event must be an object")
        required = {"schema_version", "event_id", "event_type", "origin", "occurred_at", "run_id"}
        optional = {"content", "trace_id", "turn_id", "actor", "payload"}
        if set(data) - required - optional:
            raise LifecycleEventError("unknown lifecycle event fields")
        if required - set(data):
            raise LifecycleEventError("missing required lifecycle event fields")
        if data["event_type"] in _HOST_ONLY_TYPES and not allow_host_feedback:
            raise LifecycleEventError(
                "host feedback events require an authorized host parser",
                field="event_type",
            )
        return cls(
            scope=trusted_scope,
            schema_version=data["schema_version"],
            event_id=data["event_id"],
            event_type=data["event_type"],
            origin=data["origin"],
            occurred_at=data["occurred_at"],
            run_id=data["run_id"],
            content=data.get("content", ""),
            trace_id=data.get("trace_id"),
            turn_id=data.get("turn_id"),
            actor=data.get("actor", "agent"),
            payload=data.get("payload", {}),
        )

    def to_dict(self) -> dict[str, Any]:
        """Transport representation deliberately excludes Trusted Scope."""

        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "origin": self.origin.value,
            "occurred_at": self.occurred_at.isoformat(),
            "run_id": self.run_id,
            "content": self.content,
            "trace_id": self.trace_id,
            "turn_id": self.turn_id,
            "actor": self.actor,
            "payload": _thaw_payload(self.payload),
        }

    @property
    def content_hash(self) -> str:
        """Digest the canonical evidence, excluding scope and the transport event ID."""

        document = self.to_dict()
        document.pop("event_id")
        encoded = json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return sha256(encoded).hexdigest()

    def to_memory_event(self) -> MemoryEvent:
        """Append evidence only; validated feedback uses record_* separately."""

        if self.event_type is LifecycleEventType.MESSAGE_RECEIVED:
            event_type = {
                LifecycleOrigin.USER: "user.message",
                LifecycleOrigin.MODEL: "agent.model.completed",
            }.get(self.origin, "agent.message.received")
        else:
            event_type = f"agent.{self.event_type.value}"
        return MemoryEvent(
            scope=self.scope,
            event_type=event_type,
            content=self.content or f"Lifecycle {self.event_type.value}.",
            occurred_at=self.occurred_at,
            idempotency_key=f"lifecycle:v1:{self.event_id}",
            actor=self.actor,
            metadata={
                "lifecycle": {
                    "schema_version": self.schema_version,
                    "event_id": self.event_id,
                    "content_hash": self.content_hash,
                    "origin": self.origin.value,
                    "occurred_at": self.occurred_at.isoformat(),
                    "trace_id": self.trace_id,
                    "run_id": self.run_id,
                    "turn_id": self.turn_id,
                    "session_id": self.scope.session_id,
                    "payload": _thaw_payload(self.payload),
                }
            },
        )
