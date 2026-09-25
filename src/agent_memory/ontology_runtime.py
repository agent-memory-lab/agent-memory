"""Load a scope's activated ontology with an explicitly owned plugin lifecycle."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Protocol

from .domain import MemoryScope
from .governed_recall import GovernedRecallPipeline
from .ontology_memory import OntologyEvidenceVerifier, OntologySchema, OntologyStore
from .ontology_plugin import LoadedOntologyMemory, load_ontology_memory
from .ontology_registry import OntologyActivation, OntologyRegistryConflict
from .ontology_schema import ontology_schema_digest
from .plugin_loader import PluginLoader
from .plugin_protocol import ConsolidationRequest, ConsolidationResult, PluginContext


class ActiveOntologyCatalog(Protocol):
    async def active(
        self, scope: MemoryScope, ontology_id: str
    ) -> OntologyActivation | None: ...

    async def get(
        self, scope: MemoryScope, ontology_id: str, version: str
    ) -> OntologySchema: ...


@dataclass(frozen=True, slots=True)
class ActiveOntologyMemory:
    """One pinned activation, valid for the lifetime of its context manager."""

    activation: OntologyActivation
    extension: LoadedOntologyMemory

    @property
    def recall_pipeline(self) -> GovernedRecallPipeline:
        return self.extension.recall_pipeline

    async def consolidate(self, request: ConsolidationRequest) -> ConsolidationResult:
        return await self.extension.consolidate(request)


@asynccontextmanager
async def open_active_ontology_memory(
    catalog: ActiveOntologyCatalog,
    ontology_id: str,
    context: PluginContext,
    store: OntologyStore,
    evidence_verifier: OntologyEvidenceVerifier,
) -> AsyncIterator[ActiveOntologyMemory]:
    """Resolve, load, and close the activated ontology for a trusted scope.

    The catalog must already be initialized. A private loader owns the two
    plugins, while the caller owns the shared catalog and index store. Missing
    activation or a changed generation fails before yielding a usable handle.

    Once yielded, this handle stays pinned even if another task activates a new
    version. Open a new context to adopt that version. Activation authorizers
    remain responsible for ensuring the target index is ready before switching.
    """
    if not isinstance(ontology_id, str) or not ontology_id.strip():
        raise ValueError("ontology_id must be a non-empty string")
    _require_live_context(context)
    activation = await catalog.active(context.scope, ontology_id)
    if activation is None:
        raise LookupError("no ontology version is active in this scope")
    schema = await catalog.get(context.scope, ontology_id, activation.version)
    if (
        activation.ontology_id != ontology_id
        or schema.ontology_id != ontology_id
        or schema.version != activation.version
        or ontology_schema_digest(schema) != activation.digest
    ):
        raise OntologyRegistryConflict("active schema document does not match its activation")
    loader = PluginLoader()
    primary_error: BaseException | None = None
    try:
        extension = await load_ontology_memory(loader, context, store, schema, evidence_verifier)
        current = await catalog.active(context.scope, ontology_id)
        if current != activation:
            raise OntologyRegistryConflict("ontology activation changed while plugins were loading")
        _require_live_context(context)
        yield ActiveOntologyMemory(activation, extension)
    except BaseException as error:
        primary_error = error
        raise
    finally:
        try:
            await loader.close()
        except BaseException as error:
            if primary_error is None:
                raise
            primary_error.add_note(f"ontology plugin cleanup also failed: {type(error).__name__}")


def _require_live_context(context: PluginContext) -> None:
    if context.cancelled or context.expired:
        raise RuntimeError("ontology plugin context is cancelled or expired")
