"""Optional semantic retriever contract with no bundled model or vector database."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import math
import re
from typing import Protocol, Sequence

from .domain import MemoryChannel, MemoryItem, MemoryQuery, MemoryScope
from .plugin_protocol import (
    PluginContext,
    PluginHealth,
    PluginHealthStatus,
    RetrievalCandidate,
)
from .plugins import (
    PluginError,
    PluginErrorCode,
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._/-]{0,127}\Z")


class VectorNormalization(StrEnum):
    NONE = "none"
    L2 = "l2"


class SemanticIndexState(StrEnum):
    READY = "ready"
    REBUILDING = "rebuilding"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class SemanticVectorSpec:
    """The compatibility identity shared by an embedder and its index."""

    provider: str
    model: str
    dimension: int
    normalization: VectorNormalization
    embedding_version: str

    def __post_init__(self) -> None:
        for field_name in ("provider", "model", "embedding_version"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
                raise ValueError(f"{field_name} must be a stable lowercase identifier")
        if type(self.dimension) is not int or not 1 <= self.dimension <= 65_536:
            raise ValueError("dimension must be between 1 and 65536")
        object.__setattr__(self, "normalization", VectorNormalization(self.normalization))


@dataclass(frozen=True, slots=True)
class SemanticIndexDescriptor:
    vector_spec: SemanticVectorSpec
    index_version: str
    state: SemanticIndexState

    def __post_init__(self) -> None:
        if not isinstance(self.vector_spec, SemanticVectorSpec):
            raise TypeError("vector_spec must be a SemanticVectorSpec")
        if not isinstance(self.index_version, str) or not _IDENTIFIER.fullmatch(
            self.index_version
        ):
            raise ValueError("index_version must be a stable lowercase identifier")
        object.__setattr__(self, "state", SemanticIndexState(self.state))


@dataclass(frozen=True, slots=True)
class ScopedSemanticMatch:
    """A vector-index result labelled from authoritative storage."""

    scope: MemoryScope
    item: MemoryItem
    channel: MemoryChannel
    source_event_ids: tuple[str, ...]
    score: float
    index_version: str


class SemanticEmbedder(Protocol):
    """External text-to-vector implementation; no model is bundled by Core."""

    def vector_spec(self) -> SemanticVectorSpec: ...

    async def embed_query(self, text: str) -> Sequence[float]: ...


class SemanticIndex(Protocol):
    """External vector store that enforces scope in its storage query."""

    async def describe(self) -> SemanticIndexDescriptor: ...

    async def search(
        self,
        vector: Sequence[float],
        scope: MemoryScope,
        *,
        limit: int,
    ) -> Sequence[ScopedSemanticMatch]: ...


def _validated_vector(
    values: Sequence[float], spec: SemanticVectorSpec
) -> tuple[float, ...]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise PluginError(
            "embedder returned a non-vector value",
            code=PluginErrorCode.INVALID_IMPLEMENTATION,
        )
    if len(values) != spec.dimension:
        raise PluginError(
            "embedder dimension does not match its declared vector specification",
            code=PluginErrorCode.INVALID_IMPLEMENTATION,
        )
    vector: list[float] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PluginError(
                "embedder returned a non-numeric vector component",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        component = float(value)
        if not math.isfinite(component):
            raise PluginError(
                "embedder returned a non-finite vector component",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        vector.append(component)
    if spec.normalization is VectorNormalization.L2:
        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0:
            raise PluginError(
                "embedder returned a zero vector for l2 normalization",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        vector = [value / norm for value in vector]
    return tuple(vector)


class SemanticRetrieverPlugin:
    """Plugin Protocol v1 adapter for an external embedder and vector index."""

    def __init__(self, embedder: SemanticEmbedder, index: SemanticIndex) -> None:
        self._embedder = embedder
        self._index = index
        self._context: PluginContext | None = None
        self._manifest = PluginManifest(
            name="semantic-vector",
            version="0.1.0",
            kind=PluginKind.RETRIEVER,
            capabilities=("semantic.search", "semantic.versioned_index"),
            requires={"core": ">=0.1,<1.0"},
            config_schema={"type": "object", "additionalProperties": False},
            resource_limits=PluginResourceLimits(
                timeout_ms=2_000,
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
                "semantic retriever is already initialized",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        descriptor = await self._index.describe()
        self._require_compatible_index(descriptor)
        self._context = context

    async def health(self) -> PluginHealth:
        if self._context is None:
            return PluginHealth(
                PluginHealthStatus.UNAVAILABLE, "semantic retriever is not active"
            )
        try:
            descriptor = await self._index.describe()
            self._require_compatible_index(descriptor)
        except Exception:
            return PluginHealth(
                PluginHealthStatus.UNAVAILABLE,
                "semantic index is unavailable or incompatible",
            )
        if descriptor.state is SemanticIndexState.REBUILDING:
            return PluginHealth(
                PluginHealthStatus.DEGRADED, "semantic index is rebuilding"
            )
        if descriptor.state is SemanticIndexState.UNAVAILABLE:
            return PluginHealth(
                PluginHealthStatus.UNAVAILABLE, "semantic index is unavailable"
            )
        return PluginHealth(PluginHealthStatus.READY)

    async def close(self) -> None:
        self._context = None

    async def retrieve(
        self, query: MemoryQuery, context: PluginContext
    ) -> tuple[RetrievalCandidate, ...]:
        if context is not self._context or context.cancelled or context.expired:
            raise PluginError(
                "semantic retriever context is not active",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )
        if query.scope != context.scope:
            raise PluginError(
                "semantic query is outside the trusted plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        descriptor = await self._index.describe()
        self._require_compatible_index(descriptor)
        if descriptor.state is not SemanticIndexState.READY:
            raise PluginError(
                f"semantic index is {descriptor.state.value}",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )
        spec = self._embedder.vector_spec()
        vector = _validated_vector(await self._embedder.embed_query(query.text), spec)
        limit = min(query.limit, context.resource_limits.max_candidates)
        matches = await self._index.search(vector, query.scope, limit=limit)
        if not isinstance(matches, Sequence) or isinstance(matches, (str, bytes)):
            raise PluginError(
                "semantic index returned a non-sequence",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if len(matches) > limit:
            raise PluginError(
                "semantic index exceeded the candidate limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )

        accepted: dict[str, ScopedSemanticMatch] = {}
        conflicts: set[str] = set()
        for match in matches:
            self._validate_match(match, query.scope, descriptor.index_version)
            previous = accepted.get(match.item.id)
            if previous is not None and (
                previous.item != match.item
                or previous.channel != match.channel
                or previous.source_event_ids != match.source_event_ids
            ):
                conflicts.add(match.item.id)
            elif previous is None or match.score > previous.score:
                accepted[match.item.id] = match
        for item_id in conflicts:
            accepted.pop(item_id, None)

        ranked = sorted(accepted.values(), key=lambda item: (-item.score, item.item.id))
        return tuple(
            RetrievalCandidate(
                item=match.item,
                channel=match.channel,
                rank=rank,
                source_event_ids=match.source_event_ids,
                retriever="semantic-vector",
                retrieval_method="semantic",
                metadata={
                    "semantic_score": match.score,
                    "embedding_provider": spec.provider,
                    "embedding_model": spec.model,
                    "embedding_version": spec.embedding_version,
                    "index_version": descriptor.index_version,
                    "normalization": spec.normalization.value,
                    "dimension": spec.dimension,
                },
            )
            for rank, match in enumerate(ranked, start=1)
        )

    def _require_compatible_index(self, descriptor: object) -> None:
        if not isinstance(descriptor, SemanticIndexDescriptor):
            raise PluginError(
                "semantic index returned an invalid descriptor",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        spec = self._embedder.vector_spec()
        if not isinstance(spec, SemanticVectorSpec) or descriptor.vector_spec != spec:
            raise PluginError(
                "semantic index is incompatible with the configured embedder",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )

    @staticmethod
    def _validate_match(
        match: object, scope: MemoryScope, index_version: str
    ) -> None:
        if not isinstance(match, ScopedSemanticMatch):
            raise PluginError(
                "semantic index returned an unlabelled match",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if match.scope != scope:
            raise PluginError(
                "semantic index returned a match from another scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        if not isinstance(match.item, MemoryItem) or not isinstance(
            match.channel, MemoryChannel
        ):
            raise PluginError(
                "semantic index returned an invalid memory item",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if (
            not match.source_event_ids
            or len(match.source_event_ids) > 32
            or any(not isinstance(value, str) or not value.strip() for value in match.source_event_ids)
        ):
            raise PluginError(
                "semantic match must cite bounded source evidence",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if isinstance(match.score, bool) or not isinstance(match.score, (int, float)):
            raise PluginError(
                "semantic match score must be numeric",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if not math.isfinite(float(match.score)):
            raise PluginError(
                "semantic match score must be finite",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if match.index_version != index_version:
            raise PluginError(
                "semantic match came from a different index version",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
