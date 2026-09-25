"""Opt-in Plugin Protocol assembly for versioned Ontology Memory."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from .candidate_fusion import FusedCandidate
from .candidate_guard import GovernedCandidate
from .domain import MemoryKind, MemoryScope
from .governed_recall import GovernedRecallPipeline
from .ontology_memory import (
    OntologyEvidenceVerifier,
    OntologyProjectionConsolidatorPlugin,
    OntologyRetrieverPlugin,
    OntologySchema,
    OntologyStore,
)
from .plugin_loader import LoadedPlugin, PluginLoadRequest, PluginLoader
from .plugin_protocol import (
    ConsolidationRequest,
    ConsolidationResult,
    PluginContext,
)
from .plugins import PluginError, PluginErrorCode, PluginKind


class OntologyCandidateGovernance:
    """Re-authorize fused ontology candidates against current storage state."""

    def __init__(
        self,
        store: OntologyStore,
        schema: OntologySchema,
        *,
        clock: Callable[[], datetime],
        max_scan: int = 512,
    ) -> None:
        if type(max_scan) is not int or not 1 <= max_scan <= 4_096:
            raise ValueError("max_scan must be between 1 and 4096")
        self._store = store
        self._schema = schema
        self._clock = clock
        self._max_scan = max_scan

    async def resolve(
        self,
        scope: MemoryScope,
        candidates: Sequence[FusedCandidate],
    ) -> tuple[GovernedCandidate, ...]:
        if not isinstance(scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope")
        if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
            raise TypeError("candidates must be a bounded sequence")
        if len(candidates) > 256:
            raise ValueError("candidate input exceeds the governance limit")

        if any(not isinstance(candidate, FusedCandidate) for candidate in candidates):
            raise TypeError("candidate must be a FusedCandidate")
        selected = tuple(value for value in candidates if "ontology-retriever" in value.retrievers)
        if not selected:
            return ()
        assertions = await self._store.get_assertions(
            scope, tuple(value.item.id for value in selected),
            ontology_id=self._schema.ontology_id, ontology_version=self._schema.version,
            at_time=self._clock(),
        )
        by_id = {value.assertion_id: value for value in assertions}
        governed: list[GovernedCandidate] = []
        for candidate in candidates:
            if not isinstance(candidate, FusedCandidate):
                raise TypeError("candidate must be a FusedCandidate")
            if "ontology-retriever" not in candidate.retrievers:
                continue
            authoritative = by_id.get(candidate.item.id)
            if authoritative is None:
                continue
            if tuple(sorted(authoritative.source_event_ids)) != tuple(
                sorted(candidate.source_event_ids)
            ):
                continue
            if (candidate.item.kind is not MemoryKind.CLAIM
                or authoritative.text != candidate.item.text
                or authoritative.valid_from != candidate.item.occurred_at):
                continue
            expected_metadata = {
                "ontology_id": authoritative.ontology_id,
                "ontology_version": authoritative.ontology_version,
                "subject_entity_id": authoritative.subject_entity_id,
                "predicate_id": authoritative.predicate_id,
                "object_entity_id": authoritative.object_entity_id,
                "literal_value": authoritative.literal_value,
                "valid_to": authoritative.valid_to.isoformat() if authoritative.valid_to else None,
            }
            if any(candidate.item.metadata.get(key) != value for key, value in expected_metadata.items()):
                continue
            governed.append(GovernedCandidate(scope=scope, candidate=candidate,
                valid_from=authoritative.valid_from, valid_to=authoritative.valid_to))
        return tuple(governed)


@dataclass(frozen=True, slots=True)
class LoadedOntologyMemory:
    """Loaded plugin handles plus an opt-in pipeline for AgentMemory.recall()."""

    retriever: LoadedPlugin
    consolidator: LoadedPlugin
    recall_pipeline: GovernedRecallPipeline

    async def consolidate(self, request: ConsolidationRequest) -> ConsolidationResult:
        if request.scope != self.consolidator.context.scope:
            raise PluginError(
                "ontology consolidation is outside the loaded plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        limits = self.consolidator.context.resource_limits
        result = await asyncio.wait_for(
            self.consolidator.instance.consolidate(
                request,
                self.consolidator.context,
            ),
            timeout=limits.timeout_ms / 1_000,
        )
        if not isinstance(result, ConsolidationResult):
            raise PluginError(
                "ontology consolidator returned an invalid result",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        return result


def register_ontology_memory(
    loader: PluginLoader,
    store: OntologyStore,
    schema: OntologySchema,
    evidence_verifier: OntologyEvidenceVerifier,
) -> None:
    """Register both ontology plugins without enabling either one implicitly."""
    loader.register(
        name="ontology-retriever",
        kind=PluginKind.RETRIEVER,
        factory=lambda: OntologyRetrieverPlugin(store, schema),
    )
    loader.register(
        name="ontology-projection",
        kind=PluginKind.CONSOLIDATOR,
        factory=lambda: OntologyProjectionConsolidatorPlugin(
            store,
            schema,
            evidence_verifier,
        ),
    )


async def load_ontology_memory(
    loader: PluginLoader,
    context: PluginContext,
    store: OntologyStore,
    schema: OntologySchema,
    evidence_verifier: OntologyEvidenceVerifier,
) -> LoadedOntologyMemory:
    """Register and load an isolated ontology extension for one trusted scope."""
    register_ontology_memory(loader, store, schema, evidence_verifier)
    retriever, consolidator = await loader.load_many(
        (
            PluginLoadRequest(
                name="ontology-retriever",
                kind=PluginKind.RETRIEVER,
                context=context,
                required_capabilities=("ontology.search", "ontology.versioned"),
            ),
            PluginLoadRequest(
                name="ontology-projection",
                kind=PluginKind.CONSOLIDATOR,
                context=context,
                required_capabilities=(
                    "ontology.project",
                    "ontology.evidence.invalidate",
                ),
            ),
        )
    )
    governance = OntologyCandidateGovernance(
        store,
        schema,
        clock=context.clock.now,
        max_scan=context.resource_limits.max_batch_size,
    )
    return LoadedOntologyMemory(
        retriever=retriever,
        consolidator=consolidator,
        recall_pipeline=GovernedRecallPipeline(
            (retriever,),
            governance,
            policy_version="ontology-memory-v1",
        ),
    )
