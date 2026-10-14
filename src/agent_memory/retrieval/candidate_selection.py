"""Bounded representation selection and body-free native retrieval diagnostics."""

from collections import defaultdict
from dataclasses import replace
from math import isfinite

from ..domain import MemoryKind
from .candidate_identity import candidate_identity


def _lexical_key(item):
    if "candidate_route_rank" in item.metadata:
        return (item.metadata["candidate_route_rank"], candidate_identity(item))
    return (-float(item.metadata.get("lexical_score", item.score)), candidate_identity(item))


def rank_candidate_routes(literal, probe, *, probe_name, limit, identity):
    """Mix a minority intent lane into one representation, deduplicating IDs.

    Three literal slots alternate with one probe slot. Empty or duplicate slots
    are borrowed immediately; the global candidate limit is never increased.
    Rank positions, rather than BM25 values from different queries, are compared.
    Shared owners always keep the literal locator, even when the probe schedules
    them first. A metadata/intent hit must not replace a topical evidence window.
    Accept locator rows so deduplication/selection precede evidence hydration;
    return (row, rank, routes) without copying or fabricating evidence bodies.
    """
    if type(limit) is not int or limit < 1:
        raise ValueError("candidate limit must be a positive integer")
    lanes = (list(literal), list(probe))
    memberships = defaultdict(list)
    canonical = {}
    for name, lane in zip(("literal", probe_name), lanes, strict=True):
        for item in lane:
            item_id = identity(item)
            canonical.setdefault(item_id, item)
            memberships[item_id].append(name)
    positions = [0, 0]
    selected, seen = [], set()
    schedule = (0, 0, 0, 1)
    turn = 0
    while len(selected) < limit and any(positions[i] < len(lanes[i]) for i in (0, 1)):
        preferred = schedule[turn % len(schedule)]
        turn += 1
        for lane in (preferred, 1 - preferred):
            while positions[lane] < len(lanes[lane]):
                item = lanes[lane][positions[lane]]
                positions[lane] += 1
                item_id = identity(item)
                if item_id not in seen:
                    seen.add(item_id)
                    selected.append((canonical[item_id], len(selected) + 1,
                                     tuple(dict.fromkeys(memberships[item_id]))))
                    break
            else:
                continue
            break
    return tuple(selected)


def _event_source_rounds(items):
    """Allocate one real hit per source before borrowing slots for more spans.

    Public EVENT IDs identify sources; provenance metadata is not an identity
    authority. Context-only probes inherit the anchor's score, so their score
    must not put them before its actual matched window. Claims intentionally do
    not use this policy: several claims from one event may be independent facts.
    """
    sources = defaultdict(list)
    for item in items:
        sources[item.id].append(item)
    priority = {"matched": 0, "additional_match": 1, "source_tail": 2, "adjacent": 3}
    for group in sources.values():
        group.sort(key=lambda item: (
            priority.get(item.metadata.get("context_reason"), 0), *_lexical_key(item),
        ))
    groups = sorted(sources.values(), key=lambda group: _lexical_key(group[0]))
    return [
        group[position]
        for position in range(max((len(group) for group in groups), default=0))
        for group in groups
        if position < len(group)
    ]


def balanced_candidates(candidates, limit):
    """Rank within representations, then share the pre-reranking budget fairly.

    A branch with fewer matches lends its unused slots to the others.
    EVENT slots themselves rotate across sources before taking additional spans.
    This trades some depth in the strongest source for breadth across sources;
    unused slots are borrowed without increasing the global limit. This is
    candidate recall, not a quota on final evidence. BM25 scores from different
    corpora never compete as if they were calibrated.
    """
    if type(limit) is not int or limit < 1:
        raise ValueError("candidate limit must be a positive integer")
    groups = defaultdict(list)
    for item in candidates:
        groups[item.kind.value].append(item)
    ordered = []
    for branch, items in sorted(groups.items()):
        items = (
            _event_source_rounds(items) if branch == MemoryKind.EVENT.value
            else sorted(items, key=_lexical_key)
        )
        ordered.append([
            replace(item, score=1 / (60 + rank), metadata={
                **item.metadata, "retrieval_branch": "native." + branch,
                "lexical_rank": rank,
            }) for rank, item in enumerate(items, 1)
        ])
    selected = []
    for position in range(max((len(group) for group in ordered), default=0)):
        for group in ordered:
            if position < len(group):
                selected.append(group[position])
                if len(selected) == limit:
                    return tuple(selected)
    return tuple(selected)


def stage_summary(items, *, maximum=512):
    """Never serialize candidate evidence text into a diagnostic trace."""
    result = {"count": len(items), "truncated": len(items) > maximum, "items": [
        {"id": item.id, "kind": item.kind.value, "candidate_id": candidate_identity(item),
         "score": float(item.score) if isfinite(item.score) else None}
        for item in items[:maximum]
    ]}
    facets = {}
    for item in items:
        name = item.metadata.get("query_facet")
        if not name:
            continue
        summary = facets.setdefault(name, {
            "policy": item.metadata["query_facet_policy"],
            "probe_limit": item.metadata["query_facet_probe_limit"],
            "locator_count": item.metadata["query_facet_locator_count"],
            "required_group_count": item.metadata["query_facet_group_count"],
            "hydration_limit": item.metadata["query_facet_hydration_limit"],
            "admitted_count": 0,
        })
        summary["admitted_count"] += name in item.metadata.get("candidate_routes", ())
    if facets:
        result["query_facets"] = facets
    return result
