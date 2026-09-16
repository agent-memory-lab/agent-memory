"""Optional, bounded rank fusion for evidence-backed retriever plugins.

This layer does not query storage or decide scope visibility. The host must
authorize candidate sources under the query's Trusted Scope before fusion.
Neither this module nor its trace is part of the default retrieval hot path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .domain import MemoryChannel, MemoryItem
from .plugin_protocol import RetrievalCandidate


@dataclass(frozen=True, slots=True)
class FusedCandidate:
    item: MemoryItem
    score: float
    source_event_ids: tuple[str, ...]
    method_ranks: tuple[tuple[str, int], ...]
    retrievers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CandidateFusionTrace:
    input_count: int
    accepted_count: int
    duplicate_count: int
    rejected_count: int
    conflict_count: int
    selected_count: int
    method_counts: tuple[tuple[str, int], ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class CandidateFusionResult:
    candidates: tuple[FusedCandidate, ...]
    trace: CandidateFusionTrace


def fuse_candidates(
    candidates: Iterable[RetrievalCandidate],
    *,
    max_candidates: int = 256,
    max_results: int = 8,
    k: int = 60,
    max_evidence_per_candidate: int = 32,
) -> CandidateFusionResult:
    """Fuse comparable ranks, not incomparable lexical/embedding raw scores.

    A memory ID is counted once per retrieval method, independently of its
    memory channel. Conflicting versions of the same
    ID are excluded instead of silently choosing a source. Deterministic ID
    ties and a bounded input protect callers from unstable plugin ordering.
    """

    if type(max_candidates) is not int or not 1 <= max_candidates <= 4_096:
        raise ValueError("max_candidates must be between 1 and 4096")
    if type(max_results) is not int or not 1 <= max_results <= 128:
        raise ValueError("max_results must be between 1 and 128")
    if type(k) is not int or not 1 <= k <= 1_000:
        raise ValueError("k must be between 1 and 1000")
    if type(max_evidence_per_candidate) is not int or not 1 <= max_evidence_per_candidate <= 256:
        raise ValueError("max_evidence_per_candidate must be between 1 and 256")

    best: dict[tuple[str, str], RetrievalCandidate] = {}
    conflicted_ids: set[str] = set()
    input_count = rejected = duplicates = 0
    input_truncated = False
    for candidate in candidates:
        input_count += 1
        if input_count > max_candidates:
            input_truncated = True
            break
        if (
            not isinstance(candidate, RetrievalCandidate)
            or not isinstance(candidate.channel, MemoryChannel)
            or candidate.retrieval_method is None
            or not isinstance(candidate.item, MemoryItem)
            or not candidate.item.id
            or len(candidate.source_event_ids) > max_evidence_per_candidate
        ):
            rejected += 1
            continue
        key = (candidate.retrieval_method, candidate.item.id)
        previous = best.get(key)
        if previous is None:
            best[key] = candidate
        else:
            duplicates += 1
            if (
                candidate.item.kind != previous.item.kind
                or candidate.item.text != previous.item.text
                or candidate.channel != previous.channel
            ):
                conflicted_ids.add(candidate.item.id)
                continue
            if (candidate.rank, candidate.retriever) < (previous.rank, previous.retriever):
                best[key] = candidate

    grouped: dict[str, list[RetrievalCandidate]] = {}
    method_counts: dict[str, int] = {}
    for (method, item_id), candidate in sorted(best.items()):
        grouped.setdefault(item_id, []).append(candidate)
        method_counts[method] = method_counts.get(method, 0) + 1

    fused: list[FusedCandidate] = []
    conflicts = 0
    for item_id, entries in sorted(grouped.items()):
        canonical = min(entries, key=lambda entry: (entry.rank, entry.retrieval_method, entry.retriever))
        if item_id in conflicted_ids or any(
            entry.item.kind != canonical.item.kind
            or entry.item.text != canonical.item.text
            or entry.channel != canonical.channel
            for entry in entries
        ):
            conflicts += 1
            continue
        evidence = tuple(sorted({event_id for entry in entries for event_id in entry.source_event_ids}))
        fused.append(
            FusedCandidate(
                item=canonical.item,
                score=sum(1.0 / (k + entry.rank) for entry in entries),
                source_event_ids=evidence,
                method_ranks=tuple(sorted((entry.retrieval_method, entry.rank) for entry in entries)),
                retrievers=tuple(sorted({entry.retriever for entry in entries})),
            )
        )

    fused.sort(key=lambda entry: (-entry.score, -len(entry.method_ranks), entry.item.id))
    selected = tuple(fused[:max_results])
    return CandidateFusionResult(
        candidates=selected,
        trace=CandidateFusionTrace(
            input_count=input_count,
            accepted_count=len(best),
            duplicate_count=duplicates,
            rejected_count=rejected,
            conflict_count=conflicts,
            selected_count=len(selected),
            method_counts=tuple(sorted(method_counts.items())),
            truncated=input_truncated or len(fused) > max_results,
        ),
    )
