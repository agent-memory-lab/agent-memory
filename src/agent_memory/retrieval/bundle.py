"""Hard-budget packing of guarded retrieval candidates into MemoryBundle."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Sequence

from .fusion import FusedCandidate
from .diversity import DiversityBudget, DiversityTrace
from ..domain import (
    Citation,
    Claim,
    MemoryBundle,
    MemoryCapabilities,
    MemoryItem,
    MemoryKind,
    MemoryScope,
)


@dataclass(frozen=True, slots=True)
class BundleBudget:
    max_items: int = 8
    max_characters: int = 8_000
    max_tokens: int = 1_200

    def __post_init__(self) -> None:
        if type(self.max_items) is not int or not 1 <= self.max_items <= 100:
            raise ValueError("max_items must be between 1 and 100")
        if type(self.max_characters) is not int or not 1 <= self.max_characters <= 1_000_000:
            raise ValueError("max_characters must be between 1 and 1000000")
        if type(self.max_tokens) is not int or not 64 <= self.max_tokens <= 1_000_000:
            raise ValueError("max_tokens must be between 64 and 1000000")


@dataclass(frozen=True, slots=True)
class BundlePackingTrace:
    considered_count: int
    included_count: int
    dropped_item_budget: int
    dropped_character_budget: int
    dropped_token_budget: int
    character_count: int
    token_estimate: int


@dataclass(frozen=True, slots=True)
class PackedMemoryBundle:
    bundle: MemoryBundle
    trace: BundlePackingTrace
    diversity_trace: DiversityTrace | None = None


def _token_estimate(text: str) -> int:
    if not text:
        return 0
    ascii_count = sum(ord(character) < 128 for character in text)
    non_ascii_count = len(text) - ascii_count
    return max(1, (ascii_count + 3) // 4 + non_ascii_count)


def pack_memory_bundle(
    scope: MemoryScope,
    candidates: Sequence[FusedCandidate],
    *,
    current_state: Sequence[Claim] = (),
    budget: BundleBudget | None = None,
    capabilities: MemoryCapabilities | None = None,
    request_id: str | None = None,
    policy_version: str = "candidate-policy-v1",
    diversity_budget: DiversityBudget | None = None,
) -> PackedMemoryBundle:
    """Pack candidates without truncating individual memory text or provenance."""
    if not isinstance(scope, MemoryScope):
        raise TypeError("scope must be a MemoryScope")
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
        raise TypeError("candidates must be a bounded sequence")
    if len(candidates) > 256:
        raise ValueError("candidate input cannot exceed 256 items")
    if not isinstance(current_state, Sequence) or isinstance(current_state, (str, bytes)):
        raise TypeError("current_state must be a bounded sequence")
    if len(current_state) > 64:
        raise ValueError("current_state cannot exceed 64 claims")
    claims = tuple(current_state)
    if any(not isinstance(claim, Claim) or claim.scope != scope for claim in claims):
        raise ValueError("all current-state claims must belong to the bundle scope")
    limits = budget or BundleBudget()

    state_chars = sum(len(claim.text) for claim in claims)
    state_tokens = sum(_token_estimate(claim.text) for claim in claims)
    if state_chars > limits.max_characters or state_tokens > limits.max_tokens:
        raise ValueError("current state alone exceeds the bundle budget")

    included: list[MemoryItem] = []
    citations: list[Citation] = []
    characters = state_chars
    tokens = state_tokens
    item_drops = character_drops = token_drops = 0
    seen_ids: set[str] = set()
    kind_counts: dict[MemoryKind, int] = {}
    source_counts: dict[str, int] = {}
    duplicate_drops = kind_drops = source_drops = 0
    for candidate in sorted(candidates, key=lambda value: (-value.score, value.item.id)):
        if not isinstance(candidate, FusedCandidate):
            raise TypeError("all candidates must be FusedCandidate values")
        if diversity_budget is not None and candidate.item.id in seen_ids:
            duplicate_drops += 1
            continue
        if len(included) >= min(limits.max_items, diversity_budget.max_items if diversity_budget else limits.max_items):
            item_drops += 1
            continue
        if diversity_budget is not None:
            if kind_counts.get(candidate.item.kind, 0) >= diversity_budget.max_per_kind.get(candidate.item.kind, 0):
                kind_drops += 1
                continue
            if candidate.item.kind is not MemoryKind.CLAIM and any(
                source_counts.get(source, 0) >= diversity_budget.max_per_source_event
                for source in candidate.source_event_ids
            ):
                source_drops += 1
                continue
        item_chars = len(candidate.item.text)
        item_tokens = _token_estimate(candidate.item.text)
        if characters + item_chars > limits.max_characters:
            character_drops += 1
            continue
        if tokens + item_tokens > limits.max_tokens:
            token_drops += 1
            continue
        metadata = dict(candidate.item.metadata)
        metadata.update(
            {
                "fusion_score": (
                    candidate.original_fusion_score
                    if candidate.original_fusion_score is not None else candidate.score
                ),
                "retrieval_methods": candidate.method_ranks,
                "retrievers": candidate.retrievers,
                "policy_version": policy_version,
            }
        )
        if candidate.original_fusion_score is not None:
            metadata.update({
                "selection_score": candidate.score,
                "selection_score_kind": "pair-rank-order/1",
                "pair_score": candidate.pair_score,
                "pair_probability": candidate.pair_probability,
                "pair_rank": candidate.pair_rank,
            })
        included.append(replace(candidate.item, score=candidate.score, metadata=metadata))
        citations.append(Citation(candidate.item.id, candidate.source_event_ids))
        # Only evidence actually packed spends diversity quotas. Oversized or
        # policy-rejected candidates cannot crowd out feasible lower ranks.
        seen_ids.add(candidate.item.id)
        kind_counts[candidate.item.kind] = kind_counts.get(candidate.item.kind, 0) + 1
        # A shared source may contain several distinct or conflicting claims.
        # It is not a truth-equivalence key, and claims must not prevent their
        # supporting original event from being included.
        if candidate.item.kind is not MemoryKind.CLAIM:
            for source in set(candidate.source_event_ids):
                source_counts[source] = source_counts.get(source, 0) + 1
        characters += item_chars
        tokens += item_tokens

    relevant = tuple(
        value
        for value in included
        if value.kind not in (MemoryKind.EPISODE, MemoryKind.PROCEDURE)
    )
    episodes = tuple(value for value in included if value.kind is MemoryKind.EPISODE)
    procedures = tuple(value for value in included if value.kind is MemoryKind.PROCEDURE)
    trace = BundlePackingTrace(
        considered_count=len(candidates),
        included_count=len(included),
        dropped_item_budget=item_drops,
        dropped_character_budget=character_drops,
        dropped_token_budget=token_drops,
        character_count=characters,
        token_estimate=tokens,
    )
    bundle = MemoryBundle(
        current_state=claims,
        relevant_memories=relevant,
        episodes=episodes,
        procedures=procedures,
        citations=tuple(citations),
        token_estimate=tokens,
        retrieval_metadata={
            "policy_version": policy_version,
            "considered_count": trace.considered_count,
            "included_count": trace.included_count,
            "dropped_item_budget": trace.dropped_item_budget,
            "dropped_character_budget": trace.dropped_character_budget,
            "dropped_token_budget": trace.dropped_token_budget,
            "character_count": trace.character_count,
        },
        capability_snapshot=capabilities or MemoryCapabilities(),
        request_id=request_id,
    )
    diversity_trace = (
        DiversityTrace(len(candidates), len(included), duplicate_drops,
                       kind_drops, source_drops, item_drops)
        if diversity_budget is not None else None
    )
    return PackedMemoryBundle(bundle=bundle, trace=trace, diversity_trace=diversity_trace)
