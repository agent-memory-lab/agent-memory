"""Keep local evidence under local authority without replacing external resolvers."""

from dataclasses import replace

from ..consolidation.admission_runtime import ATOM_EVENT_TYPES, ATOM_PREFIX
from ..domain import Claim
from .delivery_guard import delivery_evidence_key
from .packing import source_ids


def normalize_pipeline_bundle(bundle):
    """Resolve the governed packer's citation provenance before the final snapshot."""
    citations = {}
    for citation in bundle.citations:
        citations.setdefault(citation.memory_id, set()).update(citation.source_event_ids)

    def normalize(item):
        sources = source_ids(item)
        cited = citations.get(item.id, set())
        if sources and cited and set(sources) != cited:
            raise ValueError("pipeline evidence and citation provenance disagree")
        if isinstance(item, Claim):
            if cited and not sources:
                raise ValueError("pipeline current state requires authoritative source evidence")
            return item
        if not sources and cited:
            return replace(
                item, metadata={**item.metadata, "source_event_ids": tuple(sorted(cited))}
            )
        return item

    return replace(
        bundle,
        current_state=tuple(normalize(item) for item in bundle.current_state),
        relevant_memories=tuple(normalize(item) for item in bundle.relevant_memories),
        episodes=tuple(normalize(item) for item in bundle.episodes),
        procedures=tuple(normalize(item) for item in bundle.procedures),
    )


def pipeline_reference_ids(bundle):
    values = (
        *bundle.current_state,
        *bundle.relevant_memories,
        *bundle.episodes,
        *bundle.procedures,
    )
    return {identity for value in values for identity in (value.id, *source_ids(value))}


def external_candidates(bundle, owned_ids, live_source_ids, protected_sources):
    """External content stays delegated; every known local source must still be live.

    Ownership includes erased IDs and other scopes, so a missing or unauthorized
    local record cannot masquerade as evidence in an external store.
    """
    result = set()
    for value in (*bundle.relevant_memories, *bundle.episodes, *bundle.procedures):
        if isinstance(value, Claim) or value.id in owned_ids:
            continue
        sources = set(source_ids(value))
        if (
            not sources
            or sources.intersection(protected_sources)
            or value.id.startswith("atom-claim:")
            or str(value.metadata.get("key", "")).startswith(ATOM_PREFIX)
            or value.metadata.get("event_type") in ATOM_EVENT_TYPES
        ):
            continue
        if (sources & owned_ids) <= live_source_ids:
            key = delivery_evidence_key(value)
            if key is not None:
                result.add(key)
    return result
