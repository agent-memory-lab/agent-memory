"""Acceptance tests for final candidate governance and bundle packing."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from agent_memory.bundle_packer import BundleBudget, pack_memory_bundle
from agent_memory.candidate_diversity import DiversityBudget, select_diverse_candidates
from agent_memory.candidate_fusion import FusedCandidate
from agent_memory.candidate_guard import (
    CandidateRejection,
    GovernedCandidate,
    guard_candidates,
)
from agent_memory.domain import MemoryItem, MemoryKind, MemoryScope
from agent_memory.scoped_lexical_retrieval import ScopeIsolationError


NOW = datetime(2026, 9, 19, tzinfo=timezone.utc)
SCOPE = MemoryScope("tenant-a", session_id="session-a")


def fused(
    memory_id: str,
    *,
    kind: MemoryKind = MemoryKind.EVENT,
    text: str = "migration evidence",
    score: float = 1.0,
    sources: tuple[str, ...] = ("event-1",),
) -> FusedCandidate:
    return FusedCandidate(
        item=MemoryItem(memory_id, kind, text, score, NOW),
        score=score,
        source_event_ids=sources,
        method_ranks=(("lexical", 1),),
        retrievers=("scoped-lexical",),
    )


def test_guard_filters_policy_failures_and_preserves_deterministic_order() -> None:
    records = (
        GovernedCandidate(SCOPE, fused("accepted-low", score=0.5)),
        GovernedCandidate(SCOPE, fused("accepted-high", score=0.9)),
        GovernedCandidate(SCOPE, fused("deleted"), deleted=True),
        GovernedCandidate(SCOPE, fused("untrusted"), trusted=False),
        GovernedCandidate(SCOPE, fused("expired"), valid_to=NOW),
        GovernedCandidate(SCOPE, fused("future"), valid_from=NOW + timedelta(days=1)),
        GovernedCandidate(SCOPE, fused("no-source", sources=())),
    )

    result = guard_candidates(SCOPE, records, now=NOW)

    assert [value.item.id for value in result.accepted] == ["accepted-high", "accepted-low"]
    assert {value.reason for value in result.rejected} == {
        CandidateRejection.DELETED,
        CandidateRejection.UNTRUSTED,
        CandidateRejection.EXPIRED,
        CandidateRejection.NOT_YET_VALID,
        CandidateRejection.MISSING_PROVENANCE,
    }


def test_guard_fails_closed_on_scope_drift_and_drops_conflicting_id() -> None:
    with pytest.raises(ScopeIsolationError, match="another scope"):
        guard_candidates(
            SCOPE,
            (GovernedCandidate(MemoryScope("tenant-b"), fused("foreign")),),
            now=NOW,
        )

    first = GovernedCandidate(SCOPE, fused("same", text="first"))
    second = GovernedCandidate(SCOPE, fused("same", text="second"))
    result = guard_candidates(SCOPE, (first, second), now=NOW)
    assert not result.accepted
    assert result.rejected[0].reason is CandidateRejection.CONFLICT


def test_diversity_enforces_kind_source_and_total_quotas() -> None:
    candidates = (
        fused("episode-1", kind=MemoryKind.EPISODE, score=1.0, sources=("shared",)),
        fused("episode-2", kind=MemoryKind.EPISODE, score=0.9, sources=("shared",)),
        fused("procedure-1", kind=MemoryKind.PROCEDURE, score=0.8, sources=("event-3",)),
        fused("event-1", score=0.7, sources=("event-4",)),
    )
    result = select_diverse_candidates(
        candidates,
        budget=DiversityBudget(
            max_items=2,
            max_per_kind={
                MemoryKind.EVENT: 1,
                MemoryKind.EPISODE: 1,
                MemoryKind.PROCEDURE: 1,
            },
            max_per_source_event=1,
        ),
    )

    assert [value.item.id for value in result.candidates] == ["episode-1", "procedure-1"]
    assert result.trace.dropped_kind_quota == 1
    assert result.trace.dropped_total_quota == 1


def test_bundle_packer_splits_channels_and_preserves_citations() -> None:
    packed = pack_memory_bundle(
        SCOPE,
        (
            fused("event", kind=MemoryKind.EVENT, score=0.9, sources=("source-event",)),
            fused("episode", kind=MemoryKind.EPISODE, score=0.8, sources=("source-episode",)),
            fused("procedure", kind=MemoryKind.PROCEDURE, score=0.7, sources=("source-procedure",)),
        ),
        budget=BundleBudget(max_items=3, max_characters=1000, max_tokens=256),
        request_id="request-1",
    )

    bundle = packed.bundle
    assert [value.id for value in bundle.relevant_memories] == ["event"]
    assert [value.id for value in bundle.episodes] == ["episode"]
    assert [value.id for value in bundle.procedures] == ["procedure"]
    assert {value.memory_id for value in bundle.citations} == {"event", "episode", "procedure"}
    assert bundle.request_id == "request-1"
    assert all(value.metadata["policy_version"] == "candidate-policy-v1" for value in (
        *bundle.relevant_memories, *bundle.episodes, *bundle.procedures
    ))


def test_bundle_packer_records_character_token_and_item_drops() -> None:
    character_limited = pack_memory_bundle(
        SCOPE,
        (
            fused("included", text="small", score=1.0),
            fused("character-drop", text="x" * 500, score=0.9),
        ),
        budget=BundleBudget(max_items=2, max_characters=100, max_tokens=256),
    )
    token_limited = pack_memory_bundle(
        SCOPE,
        (
            fused("included", text="small", score=1.0),
            fused("token-drop", text="界" * 70, score=0.9),
        ),
        budget=BundleBudget(max_items=2, max_characters=1000, max_tokens=64),
    )
    item_limited = pack_memory_bundle(
        SCOPE,
        (
            fused("included", text="small", score=1.0),
            fused("item-drop", text="tiny", score=0.9),
        ),
        budget=BundleBudget(max_items=1, max_characters=1000, max_tokens=256),
    )

    assert character_limited.trace.dropped_character_budget == 1
    assert token_limited.trace.dropped_token_budget == 1
    assert item_limited.trace.dropped_item_budget == 1


def test_guard_diversity_bundle_pipeline_is_bounded_and_traceable() -> None:
    guarded = guard_candidates(
        SCOPE,
        (
            GovernedCandidate(SCOPE, fused("event", score=1.0, sources=("source-1",))),
            GovernedCandidate(SCOPE, fused("episode", kind=MemoryKind.EPISODE, score=0.8, sources=("source-2",))),
            GovernedCandidate(SCOPE, fused("archived", score=0.7), archived=True),
        ),
        now=NOW,
    )
    diverse = select_diverse_candidates(
        guarded.accepted,
        budget=DiversityBudget(max_items=2),
    )
    packed = pack_memory_bundle(
        SCOPE,
        diverse.candidates,
        budget=BundleBudget(max_items=2, max_characters=1000, max_tokens=128),
    )

    assert guarded.input_count == 3
    assert len(guarded.rejected) == 1
    assert diverse.trace.selected_count == 2
    assert packed.trace.included_count == 2
    assert len(packed.bundle.citations) == 2
