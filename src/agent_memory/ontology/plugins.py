"""Ontology consolidation and retrieval plugin lifecycle."""

from __future__ import annotations

from ..domain import (
    MemoryChannel,
    MemoryQuery,
)
from ..extensions.protocol import (
    ConsolidationRequest,
    ConsolidationResult,
    PluginContext,
    PluginHealth,
    PluginHealthStatus,
    RetrievalCandidate,
)
from ..extensions.registry import (
    PluginError,
    PluginErrorCode,
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)
from .model import (
    OntologyEvidenceVerifier,
    OntologySchema,
    OntologyStore,
    OntologyValidationError,
    project_claim_to_ontology,
)


class _OntologyPlugin:
    def __init__(
        self,
        store: OntologyStore,
        schema: OntologySchema,
        manifest: PluginManifest,
    ) -> None:
        self._store = store
        self._schema = schema
        self._manifest = manifest
        self._context: PluginContext | None = None

    def plugin_manifest(self) -> PluginManifest:
        return self._manifest

    async def initialize(self, context: PluginContext) -> None:
        if self._context is not None:
            raise PluginError(
                "ontology plugin is already initialized",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        await self._store.initialize()
        await self._store.register_schema(self._schema)
        self._context = context

    async def health(self) -> PluginHealth:
        if self._context is None:
            return PluginHealth(PluginHealthStatus.UNAVAILABLE, "ontology plugin is inactive")
        return PluginHealth(
            PluginHealthStatus.READY,
            details={
                "ontology_id": self._schema.ontology_id,
                "ontology_version": self._schema.version,
            },
        )

    async def close(self) -> None:
        self._context = None

    def _require_context(self, context: PluginContext) -> None:
        if context is not self._context or context.cancelled or context.expired:
            raise PluginError(
                "ontology plugin context is not active",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )


class OntologyProjectionConsolidatorPlugin(_OntologyPlugin):
    """Materialize accepted Claims into a rebuildable ontology index."""

    def __init__(
        self,
        store: OntologyStore,
        schema: OntologySchema,
        evidence_verifier: OntologyEvidenceVerifier,
        *,
        timeout_ms: int = 2_000,
    ) -> None:
        super().__init__(
            store,
            schema,
            PluginManifest(
                name="ontology-projection",
                version="0.1.0",
                kind=PluginKind.CONSOLIDATOR,
                capabilities=("ontology.project", "ontology.evidence.invalidate"),
                requires={"core": ">=0.1,<1.0"},
                config_schema={"type": "object", "additionalProperties": False},
                resource_limits=PluginResourceLimits(
                    timeout_ms=timeout_ms,
                    max_candidates=100,
                    max_batch_size=256,
                    max_concurrency=1,
                ),
                failure_mode=PluginFailureMode.FALLBACK,
            ),
        )
        self._evidence_verifier = evidence_verifier

    async def consolidate(
        self,
        request: ConsolidationRequest,
        context: PluginContext,
    ) -> ConsolidationResult:
        self._require_context(context)
        if request.scope != context.scope:
            raise PluginError(
                "ontology consolidation is outside the trusted scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        if len(request.claims) > context.resource_limits.max_batch_size:
            raise PluginError(
                "ontology projection exceeded the claim batch limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        deleted = set(request.deleted_event_ids)
        for claim in request.claims:
            if claim.scope != request.scope:
                raise PluginError(
                    "ontology claim is outside the trusted scope",
                    code=PluginErrorCode.INVALID_IMPLEMENTATION,
                    field="scope",
                )
            projection = project_claim_to_ontology(claim, self._schema)
            if projection is not None:
                if deleted.intersection(projection.assertion.source_event_ids):
                    continue
                verified = await self._evidence_verifier.verify(
                    claim.scope, projection.assertion.source_event_ids
                )
                if not verified:
                    raise OntologyValidationError(
                        "ontology projection evidence does not exist in the trusted scope"
                    )
                await self._store.upsert_projection(projection)
        if deleted:
            await self._store.invalidate_sources(
                request.scope,
                tuple(deleted),
                max_rows=context.resource_limits.max_batch_size,
            )
        return ConsolidationResult()


class OntologyRetrieverPlugin(_OntologyPlugin):
    """Return bounded, evidence-cited candidates from one ontology version."""

    def __init__(self, store: OntologyStore, schema: OntologySchema) -> None:
        super().__init__(
            store,
            schema,
            PluginManifest(
                name="ontology-retriever",
                version="0.1.0",
                kind=PluginKind.RETRIEVER,
                capabilities=("ontology.search", "ontology.versioned"),
                requires={"core": ">=0.1,<1.0"},
                config_schema={"type": "object", "additionalProperties": False},
                resource_limits=PluginResourceLimits(
                    timeout_ms=1_000,
                    max_candidates=8,
                    max_batch_size=512,
                    max_concurrency=2,
                ),
                failure_mode=PluginFailureMode.FALLBACK,
            ),
        )

    async def retrieve(
        self,
        query: MemoryQuery,
        context: PluginContext,
    ) -> tuple[RetrievalCandidate, ...]:
        self._require_context(context)
        if query.scope != context.scope:
            raise PluginError(
                "ontology query is outside the trusted scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        limit = min(query.limit, context.resource_limits.max_candidates)
        matches = await self._store.search(
            query.text,
            query.scope,
            ontology_id=self._schema.ontology_id,
            ontology_version=self._schema.version,
            at_time=context.clock.now(),
            limit=limit,
            max_scan=context.resource_limits.max_batch_size,
        )
        if len(matches) > limit:
            raise PluginError(
                "ontology store exceeded the candidate limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        return tuple(
            RetrievalCandidate(
                item=match.item,
                channel=MemoryChannel.SEMANTIC,
                rank=rank,
                source_event_ids=match.source_event_ids,
                retriever="ontology-retriever",
                retrieval_method="ontology",
                metadata={
                    "ontology_id": self._schema.ontology_id,
                    "ontology_version": self._schema.version,
                    "ontology_score": match.score,
                },
            )
            for rank, match in enumerate(matches, start=1)
        )
