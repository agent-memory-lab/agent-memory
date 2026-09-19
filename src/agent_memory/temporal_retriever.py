"""Optional bitemporal retriever for current and historical memory candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from typing import Protocol, Sequence

from .domain import MemoryChannel, MemoryItem, MemoryQuery, MemoryScope
from .plugin_protocol import PluginContext, PluginHealth, PluginHealthStatus, RetrievalCandidate
from .plugins import (
    PluginError,
    PluginErrorCode,
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)


def _aware(value: datetime, field: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class TemporalWindow:
    """Ask what was valid at valid_at using evidence known by recorded_before."""

    valid_at: datetime
    recorded_before: datetime

    def __post_init__(self) -> None:
        _aware(self.valid_at, "valid_at")
        _aware(self.recorded_before, "recorded_before")


@dataclass(frozen=True, slots=True)
class ScopedTemporalMatch:
    scope: MemoryScope
    item: MemoryItem
    channel: MemoryChannel
    source_event_ids: tuple[str, ...]
    valid_from: datetime
    valid_to: datetime | None
    recorded_at: datetime
    score: float


class TemporalWindowResolver(Protocol):
    def resolve(self, query: MemoryQuery, context: PluginContext) -> TemporalWindow: ...


class TemporalIndex(Protocol):
    async def search(
        self,
        text: str,
        scope: MemoryScope,
        window: TemporalWindow,
        *,
        limit: int,
    ) -> Sequence[ScopedTemporalMatch]: ...


class CurrentTemporalWindow:
    """Default resolver for facts valid and known now."""

    def resolve(self, query: MemoryQuery, context: PluginContext) -> TemporalWindow:
        now = context.clock.now()
        return TemporalWindow(valid_at=now, recorded_before=now)


class TemporalRetrieverPlugin:
    def __init__(
        self,
        index: TemporalIndex,
        resolver: TemporalWindowResolver | None = None,
    ) -> None:
        self._index = index
        self._resolver = resolver or CurrentTemporalWindow()
        self._context: PluginContext | None = None
        self._manifest = PluginManifest(
            name="temporal-bitemporal",
            version="0.1.0",
            kind=PluginKind.RETRIEVER,
            capabilities=("temporal.search", "temporal.bitemporal"),
            requires={"core": ">=0.1,<1.0"},
            config_schema={"type": "object", "additionalProperties": False},
            resource_limits=PluginResourceLimits(
                timeout_ms=1_000,
                max_candidates=8,
                max_batch_size=100,
                max_concurrency=2,
            ),
            failure_mode=PluginFailureMode.FALLBACK,
        )

    def plugin_manifest(self) -> PluginManifest:
        return self._manifest

    async def initialize(self, context: PluginContext) -> None:
        if self._context is not None:
            raise PluginError(
                "temporal retriever is already initialized",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        self._context = context

    async def health(self) -> PluginHealth:
        return PluginHealth(
            PluginHealthStatus.READY
            if self._context is not None
            else PluginHealthStatus.UNAVAILABLE
        )

    async def close(self) -> None:
        self._context = None

    async def retrieve(
        self, query: MemoryQuery, context: PluginContext
    ) -> tuple[RetrievalCandidate, ...]:
        if context is not self._context or context.cancelled or context.expired:
            raise PluginError(
                "temporal retriever context is not active",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )
        if query.scope != context.scope:
            raise PluginError(
                "temporal query is outside the trusted plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        window = self._resolver.resolve(query, context)
        if not isinstance(window, TemporalWindow):
            raise PluginError(
                "temporal resolver returned an invalid window",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        limit = min(query.limit, context.resource_limits.max_candidates)
        matches = await self._index.search(query.text, query.scope, window, limit=limit)
        if not isinstance(matches, Sequence) or isinstance(matches, (str, bytes)):
            raise PluginError(
                "temporal index returned a non-sequence",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if len(matches) > limit:
            raise PluginError(
                "temporal index exceeded the candidate limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        accepted: dict[str, ScopedTemporalMatch] = {}
        for match in matches:
            self._validate_match(match, query.scope, window)
            previous = accepted.get(match.item.id)
            if previous is None or match.score > previous.score:
                accepted[match.item.id] = match
        ranked = sorted(accepted.values(), key=lambda value: (-value.score, value.item.id))
        return tuple(
            RetrievalCandidate(
                item=match.item,
                channel=match.channel,
                rank=rank,
                source_event_ids=match.source_event_ids,
                retriever="temporal-bitemporal",
                retrieval_method="temporal",
                metadata={
                    "temporal_score": float(match.score),
                    "valid_at": window.valid_at.isoformat(),
                    "recorded_before": window.recorded_before.isoformat(),
                    "valid_from": match.valid_from.isoformat(),
                    "valid_to": match.valid_to.isoformat() if match.valid_to else None,
                    "recorded_at": match.recorded_at.isoformat(),
                },
            )
            for rank, match in enumerate(ranked, start=1)
        )

    @staticmethod
    def _validate_match(
        match: object, scope: MemoryScope, window: TemporalWindow
    ) -> None:
        if not isinstance(match, ScopedTemporalMatch):
            raise PluginError(
                "temporal index returned an unlabelled match",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if match.scope != scope:
            raise PluginError(
                "temporal index returned another scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        if not isinstance(match.item, MemoryItem) or not isinstance(match.channel, MemoryChannel):
            raise PluginError(
                "temporal index returned an invalid memory item",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        for field, value in (
            ("valid_from", match.valid_from),
            ("recorded_at", match.recorded_at),
        ):
            try:
                _aware(value, field)
            except ValueError as error:
                raise PluginError(str(error), code=PluginErrorCode.INVALID_IMPLEMENTATION) from error
        if match.valid_to is not None:
            try:
                _aware(match.valid_to, "valid_to")
            except ValueError as error:
                raise PluginError(str(error), code=PluginErrorCode.INVALID_IMPLEMENTATION) from error
        visible = (
            match.valid_from <= window.valid_at
            and (match.valid_to is None or window.valid_at < match.valid_to)
            and match.recorded_at <= window.recorded_before
        )
        if not visible:
            raise PluginError(
                "temporal index returned a match outside the requested time window",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if (
            not match.source_event_ids
            or len(match.source_event_ids) > 32
            or any(not isinstance(value, str) or not value.strip() for value in match.source_event_ids)
        ):
            raise PluginError(
                "temporal match must cite bounded source evidence",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if isinstance(match.score, bool) or not isinstance(match.score, (int, float)) or not math.isfinite(float(match.score)):
            raise PluginError(
                "temporal match score must be finite",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
