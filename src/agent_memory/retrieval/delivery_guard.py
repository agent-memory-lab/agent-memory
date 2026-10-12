"""Pure final-delivery filtering over a repository-owned authoritative snapshot.

This boundary only removes stale evidence. It never retrieves replacements or
invokes a model. Adapters must supply one current governance snapshot, even for
queries whose valid/knowledge coordinates address historical facts.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from hashlib import sha256

from ..consolidation.admission_runtime import ATOM_EVENT_TYPES, ATOM_PREFIX
from ..domain import Claim, MemoryKind
from .analyzer import lexical_terms
from .candidate_identity import candidate_identity
from .packing import source_ids, token_estimate
from .temporal_history import temporal_candidates


def bundle_values(bundle):
    return (*bundle.current_state, *bundle.relevant_memories, *bundle.episodes, *bundle.procedures)


def diagnostic_ids(bundle, kinds):
    return {row["id"] for stage in bundle.retrieval_metadata.get("retrieval_stages", {}).values()
            for row in stage.get("items", ()) if row.get("kind") in kinds}


def requested_claim_ids(bundle):
    return {value.id for value in bundle_values(bundle)
            if isinstance(value, Claim) or value.kind == MemoryKind.CLAIM} | diagnostic_ids(
                bundle, (MemoryKind.CLAIM,),
            )


def selected_source_ids(bundle):
    return {source for value in bundle_values(bundle) for source in source_ids(value)}


def selected_artifact_ids(bundle):
    kinds = (MemoryKind.BLOCK, MemoryKind.EPISODE, MemoryKind.PROCEDURE)
    return {value.id for value in bundle_values(bundle) if not isinstance(value, Claim)
            and value.kind in kinds} | diagnostic_ids(bundle, kinds)


def _text_matches(item, text):
    metadata = item.metadata
    revision = metadata.get("source_revision")
    if revision is not None and revision != sha256(text.encode()).hexdigest():
        return False
    span = metadata.get("source_span")
    if span is None:
        return item.text == text
    if not isinstance(span, Mapping) or span.get("unit") != "characters":
        return False
    start, end = span.get("start"), span.get("end")
    return (type(start) is int and type(end) is int and 0 <= start < end <= len(text)
            and metadata.get("source_chars") == len(text) and text[start:end] == item.text)


def filter_delivery(bundle, *, query, valid_at, known_at, claims, atoms, atom_info,
                    protected_sources, events, artifacts, claim_source_ids_live,
                    external_candidate_ids=(), pipeline=False):
    """Keep only exact evidence whose live identity, scope and dependencies agree."""
    protected = set(protected_sources)
    event_ids = set(events)
    legacy = [claim for claim in claims if not claim.key.startswith(ATOM_PREFIX)
              and not claim.id.startswith("atom-claim:")
              and not protected.intersection(claim.provenance.source_event_ids)]
    canonical = {claim.id: claim for claim in (*legacy, *atoms)
                 if source_ids(claim) and set(source_ids(claim)) <= claim_source_ids_live}
    temporal = {item.id: item for item in temporal_candidates(
        tuple(canonical.values()), query.text, len(canonical),
    )}

    def accepts(value):
        if not isinstance(value, Claim) and candidate_identity(value) in external_candidate_ids:
            return True
        sources = set(source_ids(value))
        if not sources:
            return False
        if isinstance(value, Claim) or value.kind == MemoryKind.CLAIM:
            # Trusted promotion grants disclosure through the Claim's own
            # visible scope. Its source may belong to the originating session;
            # require live same-tenant evidence without exposing that raw event.
            if not sources <= claim_source_ids_live:
                return False
        elif not sources <= event_ids:
            return False
        if isinstance(value, Claim):
            authority = canonical.get(value.id)
            if authority is None:
                return False
            # A later observation can close a historical system interval while
            # its fact remains valid at the requested coordinates. Refresh only
            # projected temporal metadata; never substitute another assertion.
            return replace(value, valid_from=authority.valid_from, valid_to=authority.valid_to,
                           system_from=authority.system_from, system_to=authority.system_to,
                           status=authority.status) == authority
        if value.kind == MemoryKind.CLAIM:
            authority = temporal.get(value.id)
            if authority is None or not _text_matches(value, canonical[value.id].text):
                return False
            # Scope and admission are resolved by canonical projection. Do not
            # feed typed Atom claims through the legacy-source exclusion gate.
            return all(value.metadata.get(key) == authority.metadata[key]
                       for key in ("channel", "key", "version", "source_event_ids"))
        if (value.id in protected or protected.intersection(source_ids(value))
                or value.metadata.get("event_type") in ATOM_EVENT_TYPES):
            return False
        if value.kind == MemoryKind.EVENT:
            row = events.get(value.id)
            return (row is not None and candidate_identity(value)
                    in row["verified_candidate_ids"]
                    and value.metadata.get("event_type", row["event_type"] if pipeline else None)
                    == row["event_type"])
        row = artifacts.get(value.id)
        return (row is not None and value.kind == row["kind"]
                and value.metadata.get("version", 1) == row["version"]
                and source_ids(value) == tuple(dict.fromkeys(row["source_event_ids"]))
                and _text_matches(value, row["text"]))

    def refresh(value):
        if isinstance(value, Claim):
            return canonical[value.id]
        if candidate_identity(value) in external_candidate_ids:
            return value
        if pipeline and value.kind == MemoryKind.EVENT:
            return replace(value, metadata={
                **value.metadata, "event_type": events[value.id]["event_type"],
            })
        if value.kind == MemoryKind.CLAIM:
            return replace(value, metadata={**value.metadata, **temporal[value.id].metadata})
        return value

    current = tuple(refresh(value) for value in bundle.current_state if accepts(value))
    relevant = tuple(refresh(value) for value in bundle.relevant_memories if accepts(value))
    episodes = tuple(value for value in bundle.episodes if accepts(value))
    procedures = tuple(value for value in bundle.procedures if accepts(value))
    values = (*current, *relevant, *episodes, *procedures)
    ids = {value.id for value in values}
    candidate_ids = {value.id if isinstance(value, Claim) else candidate_identity(value)
                     for value in values}
    removed = len(bundle_values(bundle)) - len(values)
    external_count = sum(
        not isinstance(value, Claim) and candidate_identity(value) in external_candidate_ids
        for value in values
    )
    metadata = dict(bundle.retrieval_metadata)
    if pipeline:
        if "included_count" in metadata:
            metadata["included_count"] = len(relevant) + len(episodes) + len(procedures)
        if "character_count" in metadata:
            metadata["character_count"] = sum(len(value.text) for value in values)
        if "diversity_selected" in metadata:
            metadata["diversity_selected"] = len(relevant) + len(episodes) + len(procedures)
        if removed:
            metadata["degraded"] = True
    for field in ("citation_spans", "packing_selection_trace"):
        if field in metadata:
            metadata[field] = tuple(row for row in metadata[field]
                                    if row.get("candidate_id") in candidate_ids)
    if metadata.get("retrieval_stages"):
        authorized_by_kind = {
            MemoryKind.CLAIM: set(canonical),
            MemoryKind.EVENT: {identity for identity, row in events.items()
                               if identity not in protected
                               and row["event_type"] not in ATOM_EVENT_TYPES},
            **{kind: {identity for identity, row in artifacts.items()
                       if row["kind"] == kind
                       and not protected.intersection(row["source_event_ids"])}
               for kind in (MemoryKind.BLOCK, MemoryKind.EPISODE, MemoryKind.PROCEDURE)},
        }
        # Original counts remain useful for recall diagnostics. Identity-only
        # rows are governed even when their candidate never entered the bundle.
        metadata["retrieval_stages"] = {
            name: {**stage, "items": [row for row in stage["items"]
                   if row["id"] in authorized_by_kind.get(row["kind"], ())],
                   "identity_guard": "current_authorized_snapshot"}
            for name, stage in metadata["retrieval_stages"].items()
        }
    terms = set(lexical_terms(query.text))
    covered_terms = {term for value in values
                     for term in lexical_terms(value.text) if term in terms}
    metadata.update(atom_info)
    metadata.update({
        "selected_count": len(relevant) + len(episodes) + len(procedures),
        "state_count": len(current),
        "packing_query_terms_covered": len(covered_terms),
        "packing_source_count": len({source for value in values for source in source_ids(value)}),
        "atom_support": {key: value for key, value in atom_info["atom_support"].items()
                         if key in ids},
        "delivery_guard": {
            "policy": "authoritative_snapshot_v1",
            "status": "mixed_authorities" if external_count else "validated",
            "checked_count": len(bundle_values(bundle)), "removed_count": removed,
            "valid_at": valid_at.isoformat(), "known_at": known_at.isoformat(),
            "trace_stage": "pre_delivery" if query.trace_enabled else "disabled",
            **({"native_evidence_count": len(values) - external_count,
                "external_evidence_count": external_count,
                "external_content_authority": "configured_pipeline_resolver",
                "external_source_guard": "known_local_sources_live_and_visible",
                "external_versions_revalidated_by_native": False} if pipeline else {}),
        },
    })
    return replace(bundle, current_state=current, relevant_memories=relevant,
                   episodes=episodes, procedures=procedures,
                   citations=tuple(c for c in bundle.citations if c.memory_id in ids),
                   token_estimate=sum(token_estimate(value.text) for value in values),
                   retrieval_metadata=metadata)
