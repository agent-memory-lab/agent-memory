"""Optional entity candidate retriever without a required graph database."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
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

_KIND = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")


@dataclass(frozen=True, slots=True)
class EntityReference:
    kind: str
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, str) or not _KIND.fullmatch(self.kind):
            raise ValueError("entity kind must be a lowercase identifier")
        if not isinstance(self.value, str) or not self.value.strip() or len(self.value) > 256:
            raise ValueError("entity value must contain 1 to 256 characters")


@dataclass(frozen=True, slots=True)
class ScopedEntityMatch:
    scope: MemoryScope
    item: MemoryItem
    channel: MemoryChannel
    source_event_ids: tuple[str, ...]
    entities: tuple[EntityReference, ...]
    score: float


class EntityResolver(Protocol):
    async def resolve(self, text: str, scope: MemoryScope) -> Sequence[EntityReference]: ...


class EntityIndex(Protocol):
    async def search(
        self,
        entities: Sequence[EntityReference],
        scope: MemoryScope,
        *,
        limit: int,
    ) -> Sequence[ScopedEntityMatch]: ...


class EntityRetrieverPlugin:
    def __init__(self, resolver: EntityResolver, index: EntityIndex) -> None:
        self._resolver = resolver
        self._index = index
        self._context: PluginContext | None = None
        self._manifest = PluginManifest(
            name="entity-candidate",
            version="0.1.0",
            kind=PluginKind.RETRIEVER,
            capabilities=("entity.resolve", "entity.search"),
            requires={"core": ">=0.1,<1.0"},
            config_schema={"type": "object", "additionalProperties": False},
            resource_limits=PluginResourceLimits(
                timeout_ms=1_000,
                max_candidates=8,
                max_batch_size=16,
                max_concurrency=2,
            ),
            failure_mode=PluginFailureMode.FALLBACK,
        )

    def plugin_manifest(self) -> PluginManifest:
        return self._manifest

    async def initialize(self, context: PluginContext) -> None:
        if self._context is not None:
            raise PluginError(
                "entity retriever is already initialized",
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
                "entity retriever context is not active",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )
        if query.scope != context.scope:
            raise PluginError(
                "entity query is outside the trusted plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        raw_entities = await self._resolver.resolve(query.text, query.scope)
        if not isinstance(raw_entities, Sequence) or isinstance(raw_entities, (str, bytes)):
            raise PluginError(
                "entity resolver returned a non-sequence",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if len(raw_entities) > context.resource_limits.max_batch_size:
            raise PluginError(
                "entity resolver exceeded the entity limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        entities: list[EntityReference] = []
        seen: set[tuple[str, str]] = set()
        for entity in raw_entities:
            if not isinstance(entity, EntityReference):
                raise PluginError(
                    "entity resolver returned an invalid entity",
                    code=PluginErrorCode.INVALID_IMPLEMENTATION,
                )
            key = (entity.kind, entity.value.casefold())
            if key not in seen:
                seen.add(key)
                entities.append(entity)
        if not entities:
            return ()
        limit = min(query.limit, context.resource_limits.max_candidates)
        matches = await self._index.search(entities, query.scope, limit=limit)
        if not isinstance(matches, Sequence) or isinstance(matches, (str, bytes)):
            raise PluginError(
                "entity index returned a non-sequence",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if len(matches) > limit:
            raise PluginError(
                "entity index exceeded the candidate limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        requested = {(value.kind, value.value.casefold()) for value in entities}
        accepted: dict[str, ScopedEntityMatch] = {}
        for match in matches:
            self._validate_match(match, query.scope, requested)
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
                retriever="entity-candidate",
                retrieval_method="entity",
                metadata={
                    "entity_score": float(match.score),
                    "entities": tuple(
                        {"kind": entity.kind, "value": entity.value}
                        for entity in match.entities
                    ),
                },
            )
            for rank, match in enumerate(ranked, start=1)
        )

    @staticmethod
    def _validate_match(
        match: object,
        scope: MemoryScope,
        requested: set[tuple[str, str]],
    ) -> None:
        if not isinstance(match, ScopedEntityMatch):
            raise PluginError(
                "entity index returned an unlabelled match",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if match.scope != scope:
            raise PluginError(
                "entity index returned another scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        if not isinstance(match.item, MemoryItem) or not isinstance(match.channel, MemoryChannel):
            raise PluginError(
                "entity index returned an invalid memory item",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if not match.entities or any(not isinstance(value, EntityReference) for value in match.entities):
            raise PluginError(
                "entity match must identify matched entities",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        matched = {(value.kind, value.value.casefold()) for value in match.entities}
        if not matched.issubset(requested):
            raise PluginError(
                "entity index expanded beyond resolved query entities",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if (
            not match.source_event_ids
            or len(match.source_event_ids) > 32
            or any(not isinstance(value, str) or not value.strip() for value in match.source_event_ids)
        ):
            raise PluginError(
                "entity match must cite bounded source evidence",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if isinstance(match.score, bool) or not isinstance(match.score, (int, float)) or not math.isfinite(float(match.score)):
            raise PluginError(
                "entity match score must be finite",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
