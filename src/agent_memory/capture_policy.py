from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from hashlib import sha256
import json
import re
from typing import Any, Protocol

from .domain import IngestResult, MemoryScope
from .lifecycle import LifecycleEvent, LifecycleEventType
from .ports import MemoryProvider


class CapturePolicyError(ValueError):
    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field


class CaptureRedactor(Protocol):
    def redact(self, text: str) -> str: ...


class CaptureArtifactStore(Protocol):
    """put is failure-atomic and idempotent for scope, event ID and digest.

    A failed put must remove temporary files and must not leave a committed
    artifact without a returned, scope-checked reference ID.
    """

    def reference_for(
        self,
        *,
        scope: MemoryScope,
        event_id: str,
        content_hash: str,
    ) -> str: ...

    async def put(
        self,
        data: bytes,
        *,
        scope: MemoryScope,
        event_id: str,
        content_hash: str,
    ) -> str: ...

    async def discard(self, reference_id: str, *, scope: MemoryScope) -> None: ...


_PRIVATE_KEY = re.compile(
    r"-----BEGIN (?:[A-Z0-9 ]* )?PRIVATE KEY-----.*?"
    r"-----END (?:[A-Z0-9 ]* )?PRIVATE KEY-----",
    re.DOTALL,
)
_BEARER = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/-]{12,}=?=?", re.IGNORECASE)
_TOKEN = re.compile(r"\b(?:sk-|ghp_|gho_|ghu_|ghs_|github_pat_)[A-Za-z0-9_-]{16,}\b")
_CREDENTIAL = re.compile(
    r"(?i)(\b(?:api[_-]?key|access[_-]?token|secret|password|authorization)\b"
    r"\s*[:=]\s*['\"]?)([^\s'\",;}{]{8,})"
)
_URL_PASSWORD = re.compile(r"(://[^\s/@:]+:)[^\s/@]+(@)")
_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_REFERENCE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z")
_SENSITIVE_FIELDS = frozenset(
    {
        "apikey",
        "accesstoken",
        "refreshtoken",
        "idtoken",
        "token",
        "secret",
        "secretkey",
        "clientsecret",
        "password",
        "passwd",
        "passphrase",
        "privatekey",
        "authorization",
        "cookie",
        "setcookie",
        "credential",
        "credentials",
        "connectionstring",
    }
)


def _sensitive_field(name: str) -> bool:
    return re.sub(r"[^a-z0-9]", "", name.lower()) in _SENSITIVE_FIELDS


class DefaultCaptureRedactor:
    def redact(self, text: str) -> str:
        text = _PRIVATE_KEY.sub("[REDACTED_PRIVATE_KEY]", text)
        text = _BEARER.sub("Bearer [REDACTED]", text)
        text = _TOKEN.sub("[REDACTED_TOKEN]", text)
        text = _CREDENTIAL.sub(r"\1[REDACTED]", text)
        text = _URL_PASSWORD.sub(r"\1[REDACTED]\2", text)
        return _EMAIL.sub("[REDACTED_EMAIL]", text)


@dataclass(frozen=True, slots=True)
class CaptureLimits:
    max_events: int = 64
    max_event_bytes: int = 65_536
    max_content_bytes: int = 16_384
    max_payload_bytes: int = 32_768
    max_payload_depth: int = 8
    max_payload_items: int = 1_000
    tool_result_reference_bytes: int = 8_192
    max_external_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        bounds = {
            "max_events": (1, 1_000),
            "max_event_bytes": (1_024, 1_048_576),
            "max_content_bytes": (1, 1_048_576),
            "max_payload_bytes": (1, 1_048_576),
            "max_payload_depth": (1, 32),
            "max_payload_items": (1, 100_000),
            "tool_result_reference_bytes": (1, 1_048_576),
            "max_external_bytes": (1, 16_777_216),
        }
        for name, (minimum, maximum) in bounds.items():
            value = getattr(self, name)
            if type(value) is not int or not minimum <= value <= maximum:
                raise CapturePolicyError(
                    f"{name} must be an integer between {minimum} and {maximum}",
                    field=name,
                )
        if self.max_content_bytes > self.max_event_bytes:
            raise CapturePolicyError("content cannot exceed event limit", field="max_content_bytes")
        if self.max_payload_bytes > self.max_event_bytes:
            raise CapturePolicyError("payload cannot exceed event limit", field="max_payload_bytes")
        if self.tool_result_reference_bytes > self.max_payload_bytes:
            raise CapturePolicyError(
                "tool result reference threshold cannot exceed payload limit",
                field="tool_result_reference_bytes",
            )


def _encoded(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise CapturePolicyError("capture payload must contain only JSON values") from error


@dataclass(frozen=True, slots=True)
class CapturePlan:
    event: LifecycleEvent
    artifact_data: bytes | None = None
    artifact_hash: str | None = None
    artifact_id: str | None = None


class CaptureSanitizer:
    """Bound and redact evidence before writing or exporting tool artifacts."""

    def __init__(
        self,
        *,
        limits: CaptureLimits | None = None,
        redactor: CaptureRedactor | None = None,
        artifact_store: CaptureArtifactStore | None = None,
    ) -> None:
        self.limits = limits or CaptureLimits()
        self._default_redactor = DefaultCaptureRedactor()
        self._custom_redactor = redactor
        self._artifact_store = artifact_store

    @property
    def artifact_store(self) -> CaptureArtifactStore | None:
        return self._artifact_store

    def _redact(self, value: Any) -> Any:
        if isinstance(value, str):
            safe = self._default_redactor.redact(value)
            if self._custom_redactor is not None:
                safe = self._custom_redactor.redact(safe)
                if not isinstance(safe, str):
                    raise CapturePolicyError("redactor must return a string", field="redactor")
                safe = self._default_redactor.redact(safe)
            return safe
        if isinstance(value, Mapping):
            return {
                key: (
                    "[REDACTED]"
                    if _sensitive_field(key) and item is not None
                    else self._redact(item)
                )
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [self._redact(item) for item in value]
        return value

    def _check_structure(self, value: Any, *, depth: int = 0, count: list[int]) -> None:
        if depth > self.limits.max_payload_depth:
            raise CapturePolicyError("payload nesting limit exceeded", field="payload")
        count[0] += 1
        if count[0] > self.limits.max_payload_items:
            raise CapturePolicyError("payload item limit exceeded", field="payload")
        if isinstance(value, Mapping):
            for item in value.values():
                self._check_structure(item, depth=depth + 1, count=count)
        elif isinstance(value, (list, tuple)):
            for item in value:
                self._check_structure(item, depth=depth + 1, count=count)

    def plan(self, event: LifecycleEvent) -> CapturePlan:
        if not isinstance(event, LifecycleEvent):
            raise CapturePolicyError("capture requires a LifecycleEvent", field="event")
        self._check_structure(event.payload, count=[0])
        content = self._redact(event.content)
        if len(content.encode("utf-8")) > self.limits.max_content_bytes:
            raise CapturePolicyError("event content limit exceeded", field="content")
        payload = self._redact(event.payload)
        artifact_data: bytes | None = None
        artifact_hash: str | None = None
        artifact_id: str | None = None
        is_tool_result = event.event_type in {
            LifecycleEventType.TOOL_COMPLETED,
            LifecycleEventType.TOOL_FAILED,
        }
        if is_tool_result and "result" in payload:
            result_bytes = _encoded(payload["result"])
            if len(result_bytes) > self.limits.tool_result_reference_bytes:
                if len(result_bytes) > self.limits.max_external_bytes:
                    raise CapturePolicyError("external artifact limit exceeded", field="payload.result")
                if self._artifact_store is None:
                    raise CapturePolicyError(
                        "large tool result requires a trusted artifact store",
                        field="payload.result",
                    )
                digest = sha256(result_bytes).hexdigest()
                reference_id = self._artifact_store.reference_for(
                    scope=event.scope,
                    event_id=event.event_id,
                    content_hash=digest,
                )
                if not isinstance(reference_id, str) or not _REFERENCE_ID.fullmatch(reference_id):
                    raise CapturePolicyError(
                        "artifact store must plan an opaque reference ID",
                        field="payload.result",
                    )
                projected_reference = {
                    "artifact_ref": {
                        "id": reference_id,
                        "sha256": digest,
                        "size_bytes": len(result_bytes),
                    }
                }
                projected_payload = {**payload, "result": projected_reference}
                self._check_structure(projected_payload, count=[0])
                payload["result"] = projected_reference
                artifact_data = result_bytes
                artifact_hash = digest
                artifact_id = reference_id
        if len(_encoded(payload)) > self.limits.max_payload_bytes:
            raise CapturePolicyError("event payload limit exceeded", field="payload")
        prepared = replace(event, content=content, payload=payload)
        if len(_encoded(prepared.to_dict())) > self.limits.max_event_bytes:
            raise CapturePolicyError("event size limit exceeded", field="event")
        return CapturePlan(prepared, artifact_data, artifact_hash, artifact_id)

    async def materialize(self, plan: CapturePlan) -> LifecycleEvent:
        if plan.artifact_data is None:
            return plan.event
        if self._artifact_store is None or plan.artifact_id is None or plan.artifact_hash is None:
            raise CapturePolicyError("capture plan has no artifact store", field="payload.result")
        actual: str | None = None
        try:
            actual = await self._artifact_store.put(
                plan.artifact_data,
                scope=plan.event.scope,
                event_id=plan.event.event_id,
                content_hash=plan.artifact_hash,
            )
            if actual != plan.artifact_id:
                raise CapturePolicyError(
                    "artifact store returned a different reference ID",
                    field="payload.result",
                )
        except Exception as error:
            try:
                await self._artifact_store.discard(plan.artifact_id, scope=plan.event.scope)
                if isinstance(actual, str) and actual != plan.artifact_id:
                    await self._artifact_store.discard(actual, scope=plan.event.scope)
            except Exception:
                error.add_note("artifact cleanup failed; the reference may remain")
            raise
        return plan.event

    async def prepare(self, event: LifecycleEvent) -> LifecycleEvent:
        return await self.materialize(self.plan(event))

    async def prepare_batch(self, events: Sequence[LifecycleEvent]) -> tuple[LifecycleEvent, ...]:
        if isinstance(events, (str, bytes)) or len(events) > self.limits.max_events:
            raise CapturePolicyError("batch event limit exceeded", field="events")
        plans: list[CapturePlan] = []
        seen: dict[tuple[str, str], str] = {}
        for event in events:
            plan = self.plan(event)
            safe = plan.event
            key = (safe.scope.partition_key(), safe.event_id)
            previous = seen.get(key)
            if previous is not None:
                if previous != safe.content_hash:
                    raise CapturePolicyError(
                        "event ID reused with different content",
                        field="event_id",
                    )
                continue
            seen[key] = safe.content_hash
            plans.append(plan)
        return tuple([await self.materialize(plan) for plan in plans])

    async def ingest(
        self,
        event: LifecycleEvent,
        provider: MemoryProvider,
    ) -> IngestResult:
        safe = await self.prepare(event)
        return await provider.ingest_event(safe.to_memory_event())
