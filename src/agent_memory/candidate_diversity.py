"""Deterministic source and memory-kind diversity for guarded candidates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence

from .candidate_fusion import FusedCandidate
from .domain import MemoryKind


@dataclass(frozen=True, slots=True)
class DiversityBudget:
    max_items: int = 8
    max_per_kind: Mapping[MemoryKind, int] = field(
        default_factory=lambda: {
            MemoryKind.EVENT: 4,
            MemoryKind.CLAIM: 4,
            MemoryKind.BLOCK: 4,
            MemoryKind.EPISODE: 3,
            MemoryKind.PROCEDURE: 2,
            MemoryKind.LATENT_REFERENCE: 1,
        }
    )
    max_per_source_event: int = 2

    def __post_init__(self) -> None:
        if type(self.max_items) is not int or not 1 <= self.max_items <= 100:
            raise ValueError("max_items must be between 1 and 100")
        if type(self.max_per_source_event) is not int or not 1 <= self.max_per_source_event <= 100:
            raise ValueError("max_per_source_event must be between 1 and 100")
        normalized: dict[MemoryKind, int] = {}
        for kind, limit in self.max_per_kind.items():
            resolved = MemoryKind(kind)
            if type(limit) is not int or not 0 <= limit <= 100:
                raise ValueError("per-kind limits must be between 0 and 100")
            normalized[resolved] = limit
        object.__setattr__(self, "max_per_kind", normalized)


@dataclass(frozen=True, slots=True)
class DiversityTrace:
    input_count: int
    selected_count: int
    dropped_duplicate: int
    dropped_kind_quota: int
    dropped_source_quota: int
    dropped_total_quota: int


@dataclass(frozen=True, slots=True)
class DiversityResult:
    candidates: tuple[FusedCandidate, ...]
    trace: DiversityTrace


def select_diverse_candidates(
    candidates: Sequence[FusedCandidate],
    *,
    budget: DiversityBudget | None = None,
) -> DiversityResult:
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
        raise TypeError("candidates must be a bounded sequence")
    if len(candidates) > 256:
        raise ValueError("candidate input cannot exceed 256 items")
    policy = budget or DiversityBudget()
    ordered = sorted(candidates, key=lambda value: (-value.score, value.item.id))
    selected: list[FusedCandidate] = []
    seen_ids: set[str] = set()
    kind_counts: dict[MemoryKind, int] = {}
    source_counts: dict[str, int] = {}
    duplicate = kind_quota = source_quota = total_quota = 0

    for candidate in ordered:
        if not isinstance(candidate, FusedCandidate):
            raise TypeError("all candidates must be FusedCandidate values")
        if candidate.item.id in seen_ids:
            duplicate += 1
            continue
        if len(selected) >= policy.max_items:
            total_quota += 1
            continue
        kind = candidate.item.kind
        if kind_counts.get(kind, 0) >= policy.max_per_kind.get(kind, 0):
            kind_quota += 1
            continue
        if any(
            source_counts.get(source_id, 0) >= policy.max_per_source_event
            for source_id in candidate.source_event_ids
        ):
            source_quota += 1
            continue
        selected.append(candidate)
        seen_ids.add(candidate.item.id)
        kind_counts[kind] = kind_counts.get(kind, 0) + 1
        for source_id in set(candidate.source_event_ids):
            source_counts[source_id] = source_counts.get(source_id, 0) + 1

    return DiversityResult(
        tuple(selected),
        DiversityTrace(
            input_count=len(candidates),
            selected_count=len(selected),
            dropped_duplicate=duplicate,
            dropped_kind_quota=kind_quota,
            dropped_source_quota=source_quota,
            dropped_total_quota=total_quota,
        ),
    )
