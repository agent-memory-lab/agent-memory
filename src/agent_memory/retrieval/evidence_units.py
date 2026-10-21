"""Source-bound context relations, never semantic fact equivalence or authority."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from json import dumps

from ..domain import MemoryItem, MemoryKind
from .candidate_identity import candidate_identity


@dataclass(frozen=True, slots=True)
class ContextRelations:
    parents: dict[str, str]
    context_ids: frozenset[str]
    rejected: int


def _sources(value):
    sources = value.metadata.get("source_event_ids")
    return tuple(sources) if isinstance(sources, (tuple, list)) else ()


def context_relations(ranked: Sequence[MemoryItem]) -> ContextRelations:
    """Accept only located, touching/overlapping windows of the same source.

    Context flags alone do not permit an independent score or cross-source link.
    Overlapping characters must agree. Store authorization still happens in the
    repository and final delivery guard; this only constrains allocation.
    """
    by_id = {candidate_identity(value): value for value in ranked}
    parents, context_ids = {}, set()
    for key, child in by_id.items():
        if child.metadata.get("context_only") is not True:
            continue
        context_ids.add(key)
        parent_key = child.metadata.get("context_anchor_candidate_id")
        parent = by_id.get(parent_key) if isinstance(parent_key, str) else None
        if not (
            child.kind == MemoryKind.EVENT and key.startswith("source-span:")
            and child.metadata.get("relevance_origin") == "inherited_context_anchor"
            and parent is not None and parent.kind == MemoryKind.EVENT
            and parent.metadata.get("context_only") is not True
            and parent_key.startswith("source-span:") and parent_key != key
            and parent.id == child.id
            and _sources(child) == (child.id,)
            and _sources(parent) == (parent.id,)
            and parent.metadata.get("source_revision") == child.metadata.get("source_revision")
            and parent.metadata.get("source_chars") == child.metadata.get("source_chars")
        ):
            continue
        left, right = parent.metadata["source_span"], child.metadata["source_span"]
        start, end = max(left["start"], right["start"]), min(left["end"], right["end"])
        if start > end:
            continue
        if parent.text[start-left["start"]:end-left["start"]] != child.text[start-right["start"]:end-right["start"]]:
            continue
        parents[key] = parent_key
    return ContextRelations(parents, frozenset(context_ids), len(context_ids)-len(parents))


def evidence_proof(value: MemoryItem) -> dict:
    """Bind a dependency to exact text and locator, not a shared public ID."""
    return {
        "candidate_id": candidate_identity(value), "memory_id": value.id,
        "text_sha256": sha256(value.text.encode()).hexdigest(),
        "source_event_ids": _sources(value),
        "source_revision": value.metadata.get("source_revision"),
        "source_span": dict(value.metadata["source_span"])
        if isinstance(value.metadata.get("source_span"), Mapping) else None,
    }


def dependency_group(values: Sequence[MemoryItem]) -> dict:
    members = tuple(evidence_proof(value) for value in values)
    return {"group_id": sha256(dumps(members, sort_keys=True).encode()).hexdigest(),
            "members": members, "policy": "atomic_source_context/1"}


def retain_complete_groups(values, groups):
    """Only remove evidence after authority filtering; never restore a member.

    Atomic groups are all-or-nothing at delivery too. A still-visible answer
    cannot survive loss of the question or a negation it was selected with.
    """
    proofs = {candidate_identity(v): evidence_proof(v) for v in values if isinstance(v, MemoryItem)}
    # Native tuples become arrays across a JSON boundary. Normalize only this
    # representation detail; exact text, locator and source bindings still match.
    groups = tuple({**group, "members": tuple({**proof,
        "source_event_ids": tuple(proof["source_event_ids"])} for proof in group.get("members", ()))}
        for group in groups)
    grouped_ids = {proof.get("candidate_id") for group in groups for proof in group.get("members", ())}
    dropped = {candidate_identity(value) for value in values if isinstance(value, MemoryItem)
               and value.metadata.get("context_only") is True
               and candidate_identity(value) not in grouped_ids}
    changed = True
    while changed:
        size = len(dropped)
        for group in groups:
            members = group.get("members", ())
            if not members or any(proof.get("candidate_id") in dropped
                                  or proofs.get(proof.get("candidate_id")) != proof for proof in members):
                dropped.update(proof.get("candidate_id") for proof in members)
        changed = len(dropped) != size
    kept = tuple(v for v in values if not isinstance(v, MemoryItem) or candidate_identity(v) not in dropped)
    complete = tuple(group for group in groups if group.get("members")
                     and all(proof.get("candidate_id") not in dropped for proof in group["members"]))
    return kept, complete
