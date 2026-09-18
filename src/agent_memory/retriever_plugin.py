"""Plugin Protocol v1 adapter for scope-checked lexical retrieval."""

import asyncio
from typing import TYPE_CHECKING

from .domain import MemoryQuery
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
from .scoped_lexical_retrieval import ScopedEvidenceSource, scoped_lexical_candidates

if TYPE_CHECKING:
    from .plugin_loader import PluginLoader
    from .sqlite import SQLiteMemoryRepository


class ScopedLexicalRetrieverPlugin:
    """Optional retriever that consumes authoritative, scope-labelled evidence."""

    def __init__(self, source: ScopedEvidenceSource) -> None:
        self._source = source
        self._context: PluginContext | None = None
        self._manifest = PluginManifest(
            name="scoped-lexical",
            version="0.1.0",
            kind=PluginKind.RETRIEVER,
            capabilities=("lexical.search",),
            requires={"core": ">=0.1,<1.0"},
            config_schema={"type": "object", "additionalProperties": False},
            resource_limits=PluginResourceLimits(
                timeout_ms=1_000,
                max_candidates=8,
                max_batch_size=128,
                max_concurrency=4,
            ),
            failure_mode=PluginFailureMode.FAIL_CLOSED,
        )

    @classmethod
    def from_sqlite(
        cls, repository: "SQLiteMemoryRepository"
    ) -> "ScopedLexicalRetrieverPlugin":
        from .sqlite_evidence_source import SQLiteRecentEventEvidenceSource

        return cls(SQLiteRecentEventEvidenceSource(repository))

    def plugin_manifest(self) -> PluginManifest:
        return self._manifest

    async def initialize(self, context: PluginContext) -> None:
        if self._context is not None:
            raise PluginError(
                "retriever is already initialized",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        self._context = context

    async def health(self) -> PluginHealth:
        if self._context is None:
            return PluginHealth(PluginHealthStatus.UNAVAILABLE, "retriever is not active")
        return PluginHealth(PluginHealthStatus.READY)

    async def close(self) -> None:
        self._context = None

    async def retrieve(
        self, query: MemoryQuery, context: PluginContext
    ) -> tuple[RetrievalCandidate, ...]:
        if context is not self._context or context.cancelled or context.expired:
            raise PluginError(
                "retriever context is not active",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )
        if query.scope != context.scope:
            raise PluginError(
                "retrieval query is outside the trusted plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        limits = context.resource_limits
        result = await asyncio.to_thread(
            scoped_lexical_candidates,
            query.text,
            query.scope,
            self._source,
            limit=min(query.limit, limits.max_candidates, 128),
            max_items=min(limits.max_batch_size, 512),
        )
        if context is not self._context or context.cancelled or context.expired:
            raise PluginError(
                "retriever context expired before returning candidates",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )
        return tuple(result.candidates)


def register_sqlite_lexical_retriever(
    loader: "PluginLoader", repository: "SQLiteMemoryRepository"
) -> None:
    """Register an opt-in local retriever without changing the default recall path."""
    loader.register(
        name="scoped-lexical",
        kind=PluginKind.RETRIEVER,
        factory=lambda: ScopedLexicalRetrieverPlugin.from_sqlite(repository),
    )
