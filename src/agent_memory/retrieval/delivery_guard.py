"""Pure final-delivery filtering over a repository-owned authoritative snapshot.

This boundary only removes stale evidence. It never retrieves replacements or
invokes a model. Adapters must supply one current governance snapshot, even for
queries whose valid/knowledge coordinates address historical facts.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from hashlib import sha256
from json import dumps

from ..consolidation.admission_runtime import ATOM_EVENT_TYPES, ATOM_PREFIX
from ..domain import Claim, MemoryKind
from .analyzer import lexical_terms
from .candidate_identity import candidate_identity
from .packing import source_ids, token_estimate
from .temporal_history import temporal_candidates


def snapshot_delivery_bundle(bundle):
    """Detach caller-owned metadata before adapter I/O can yield to a mutation.

    Domain metadata accepts general Mapping objects, including read-only proxies;
    normalize those recursively rather than relying on deepcopy's pickle support.
    """
    def snapshot(value):
        if isinstance(value, Mapping):
            return {key: snapshot(item) for key, item in value.items()}
        if isinstance(value, tuple):
            return tuple(snapshot(item) for item in value)
        if isinstance(value, list):
            return [snapshot(item) for item in value]
        if is_dataclass(value) and not isinstance(value, type):
            return replace(value, **{field.name: snapshot(getattr(value, field.name))
                                     for field in fields(value) if field.init})
        return deepcopy(value)

    return snapshot(bundle)


def bundle_values(bundle):
    return (*bundle.current_state, *bundle.relevant_memories, *bundle.episodes, *bundle.procedures)


def delivery_evidence_key(item):
    """Bind authorization to exact evidence, separately from packing identities.

    Multiple items can share a public ID or an original chunk locator, including
    different narrowed spans. Neither identity proves the other item's text or
    provenance. Snapshot the fields used by final authorization so a valid sibling
    cannot authorize another item, or changed metadata between adapter awaits.
    """
    fields = ("event_type", "key", "channel", "version", "source_span",
              "source_revision", "source_chars")
    try:
        payload = dumps(
            [item.id, item.kind, item.text, source_ids(item),
             {key: item.metadata.get(key) for key in fields}],
            sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
        )
    except (TypeError, ValueError):
        return None
    return sha256(payload.encode()).hexdigest()


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
                    external_evidence_keys=(), pipeline=False):
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
        if not isinstance(value, Claim) and delivery_evidence_key(value) in external_evidence_keys:
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
            return (row is not None and sources == {value.id}
                    and delivery_evidence_key(value) in row["verified_evidence_keys"]
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
        if delivery_evidence_key(value) in external_evidence_keys:
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
        not isinstance(value, Claim) and delivery_evidence_key(value) in external_evidence_keys
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
    # IDs alone do not bind diagnostic provenance either: a removed sibling can
    # share the retained item's ID/chunk locator. Keep only exact span proofs and
    # discard selection rows whose candidate identity became ambiguous.
    if "citation_spans" in metadata:
        proofs = []
        for value in values:
            proof = {"memory_id": value.id,
                     "candidate_id": value.id if isinstance(value, Claim)
                     else candidate_identity(value), "source_event_ids": source_ids(value)}
            if not isinstance(value, Claim):
                proof.update({key: value.metadata[key] for key in (
                    "source_span", "source_revision", "source_chars",
                ) if key in value.metadata})
            proofs.append(proof)
        metadata["citation_spans"] = tuple(
            proof for row in metadata["citation_spans"] for proof in proofs
            if all((tuple(row.get(key, ())) == expected if key == "source_event_ids"
                    else row.get(key) == expected) for key, expected in proof.items())
            and all(row.get(key) == proof.get(key) for key in (
                "source_span", "source_revision", "source_chars",
            ))
        )
    if "packing_selection_trace" in metadata:
        original_keys = {}
        for value in bundle_values(bundle):
            identity = value.id if isinstance(value, Claim) else candidate_identity(value)
            original_keys.setdefault(identity, set()).add(
                value.id if isinstance(value, Claim) else delivery_evidence_key(value)
            )
        trace_fields = ("memory_id", "candidate_id", "kind", "input_rank", "relevance",
                        "selection_score", "query_coverage", "new_query_coverage",
                        "event_redundancy", "tokens")
        metadata["packing_selection_trace"] = tuple(
            {key: row[key] for key in trace_fields if key in row}
            for row in metadata["packing_selection_trace"]
            if row.get("candidate_id") in candidate_ids
            and len(original_keys.get(row.get("candidate_id"), ())) == 1
        )
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
                   citations=tuple(c for c in bundle.citations if any(
                       c.memory_id == value.id and tuple(c.source_event_ids) == source_ids(value)
                       for value in values)),
                   token_estimate=sum(token_estimate(value.text) for value in values),
                   retrieval_metadata=metadata)
