from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Protocol, TypeVar, runtime_checkable

from .domain import (
    Claim,
    ClaimDraft,
    ClaimEvidenceUpdate,
    DecisionRecord,
    Episode,
    MemoryChannel,
    MemoryEvent,
    MemoryItem,
    MemoryProposal,
    MemoryQuery,
    MemoryScope,
    OutcomeEvent,
    Procedure,
    ProcedureInductionRejection,
    RetrievalTrace,
    RewardSignal,
)
from .plugins import PluginManifest, PluginManifestError, PluginResourceLimits
from .ports import MemoryProvider


class PluginHealthStatus(StrEnum):
    READY = "ready"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class PluginHealth:
    status: PluginHealthStatus
    message: str = ""
    retry_after_ms: int | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        try:
            status = PluginHealthStatus(self.status)
        except (TypeError, ValueError) as exc:
            allowed = ", ".join(item.value for item in PluginHealthStatus)
            raise PluginManifestError(f"plugin health status must be one of: {allowed}") from exc
        object.__setattr__(self, "status", status)
        if not isinstance(self.message, str):
            raise PluginManifestError("plugin health message must be a string")
        if self.retry_after_ms is not None and (
            type(self.retry_after_ms) is not int or self.retry_after_ms < 1
        ):
            raise PluginManifestError("plugin health retry_after_ms must be a positive integer")
        if not isinstance(self.details, Mapping):
            raise PluginManifestError("plugin health details must be an object")
        if any(not isinstance(key, str) for key in self.details):
            raise PluginManifestError("plugin health details must use string keys")
        object.__setattr__(self, "details", MappingProxyType(dict(self.details)))


class PluginLogger(Protocol):
    def debug(self, message: str, **fields: Any) -> None: ...

    def info(self, message: str, **fields: Any) -> None: ...

    def warning(self, message: str, **fields: Any) -> None: ...

    def error(self, message: str, **fields: Any) -> None: ...


class PluginClock(Protocol):
    def now(self) -> datetime: ...


class PluginCancellation(Protocol):
    @property
    def cancelled(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class NullPluginLogger:
    def debug(self, message: str, **fields: Any) -> None:
        return None

    def info(self, message: str, **fields: Any) -> None:
        return None

    def warning(self, message: str, **fields: Any) -> None:
        return None

    def error(self, message: str, **fields: Any) -> None:
        return None


@dataclass(frozen=True, slots=True)
class SystemPluginClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class NeverCancelled:
    @property
    def cancelled(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class PluginContext:
    """Minimum trusted context available to a plugin; it never exposes the Kernel."""

    scope: MemoryScope
    resource_limits: PluginResourceLimits
    config: Mapping[str, Any] = field(default_factory=dict)
    request_id: str | None = None
    deadline: datetime | None = None
    logger: PluginLogger = field(default_factory=NullPluginLogger)
    clock: PluginClock = field(default_factory=SystemPluginClock)
    cancellation: PluginCancellation = field(default_factory=NeverCancelled)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, MemoryScope):
            raise PluginManifestError("plugin context requires a trusted MemoryScope")
        if not isinstance(self.resource_limits, PluginResourceLimits):
            raise PluginManifestError("plugin context requires PluginResourceLimits")
        if not isinstance(self.config, Mapping):
            raise PluginManifestError("plugin context config must be an object")
        if any(not isinstance(key, str) for key in self.config):
            raise PluginManifestError("plugin context config must use string keys")
        object.__setattr__(self, "config", MappingProxyType(dict(self.config)))
        if self.request_id is not None and (
            not isinstance(self.request_id, str) or not self.request_id.strip()
        ):
            raise PluginManifestError("plugin context request_id must be a non-empty string")
        if self.deadline is not None and self.deadline.utcoffset() is None:
            raise PluginManifestError("plugin context deadline must include a timezone")

    @property
    def cancelled(self) -> bool:
        return self.cancellation.cancelled

    @property
    def expired(self) -> bool:
        return self.deadline is not None and self.clock.now() >= self.deadline


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    claims: tuple[ClaimDraft | MemoryProposal, ...] = ()
    episodes: tuple[Episode, ...] = ()
    procedures: tuple[Procedure, ...] = ()


@dataclass(frozen=True, slots=True)
class RetrievalCandidate:
    item: MemoryItem
    channel: MemoryChannel
    rank: int
    source_event_ids: tuple[str, ...]
    retriever: str
    metadata: Mapping[str, Any] = field(default_factory=dict)
    retrieval_method: str | None = None

    def __post_init__(self) -> None:
        if type(self.rank) is not int or self.rank < 1:
            raise PluginManifestError("retrieval candidate rank must be a positive integer")
        method = self.retrieval_method
        if method is not None and (
            not isinstance(method, str)
            or not 1 <= len(method) <= 64
            or not method.isascii()
            or not method[0].isalpha()
            or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for character in method)
        ):
            raise PluginManifestError("retrieval_method must be a lowercase ASCII identifier")
        if not self.source_event_ids or any(
            not isinstance(event_id, str) or not event_id.strip()
            for event_id in self.source_event_ids
        ):
            raise PluginManifestError(
                "retrieval candidate source_event_ids must contain evidence IDs"
            )
        if not isinstance(self.retriever, str) or not self.retriever.strip():
            raise PluginManifestError("retrieval candidate retriever must be non-empty")
        if not isinstance(self.metadata, Mapping):
            raise PluginManifestError("retrieval candidate metadata must be an object")
        if any(not isinstance(key, str) for key in self.metadata):
            raise PluginManifestError("retrieval candidate metadata must use string keys")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class ConsolidationRequest:
    scope: MemoryScope
    events: tuple[MemoryEvent, ...] = ()
    claims: tuple[Claim, ...] = ()
    episodes: tuple[Episode, ...] = ()
    rewards: tuple[RewardSignal, ...] = ()
    decisions: tuple[DecisionRecord, ...] = ()
    outcomes: tuple[OutcomeEvent, ...] = ()
    retrieval_traces: tuple[RetrievalTrace, ...] = ()
    deleted_event_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ConsolidationResult:
    claims: tuple[MemoryProposal, ...] = ()
    claim_evidence_updates: tuple[ClaimEvidenceUpdate, ...] = ()
    episodes: tuple[Episode, ...] = ()
    procedures: tuple[Procedure, ...] = ()
    procedure_rejections: tuple[ProcedureInductionRejection, ...] = ()


class PluginLifecycle(Protocol):
    """Shared lifecycle. plugin_manifest avoids conflicting with ProviderManifest."""

    def plugin_manifest(self) -> PluginManifest: ...

    async def initialize(self, context: PluginContext) -> None: ...

    async def health(self) -> PluginHealth: ...

    async def close(self) -> None: ...


CaptureInputT = TypeVar("CaptureInputT", contravariant=True)
EvaluationRequestT = TypeVar("EvaluationRequestT", contravariant=True)
EvaluationResultT = TypeVar("EvaluationResultT", covariant=True)


@runtime_checkable
class CaptureAdapterPlugin(PluginLifecycle, Protocol[CaptureInputT]):
    async def capture(
        self,
        event: CaptureInputT,
        context: PluginContext,
    ) -> tuple[MemoryEvent, ...]: ...


@runtime_checkable
class ExtractorPlugin(PluginLifecycle, Protocol):
    async def extract(
        self,
        event: MemoryEvent,
        context: PluginContext,
    ) -> ExtractionResult: ...


@runtime_checkable
class RetrieverPlugin(PluginLifecycle, Protocol):
    async def retrieve(
        self,
        query: MemoryQuery,
        context: PluginContext,
    ) -> tuple[RetrievalCandidate, ...]: ...


@runtime_checkable
class ConsolidatorPlugin(PluginLifecycle, Protocol):
    async def consolidate(
        self,
        request: ConsolidationRequest,
        context: PluginContext,
    ) -> ConsolidationResult: ...


@runtime_checkable
class StorageProviderPlugin(PluginLifecycle, Protocol):
    async def create_provider(self, context: PluginContext) -> MemoryProvider: ...


@runtime_checkable
class EvaluatorPlugin(
    PluginLifecycle,
    Protocol[EvaluationRequestT, EvaluationResultT],
):
    async def evaluate(
        self,
        request: EvaluationRequestT,
        context: PluginContext,
    ) -> EvaluationResultT: ...
