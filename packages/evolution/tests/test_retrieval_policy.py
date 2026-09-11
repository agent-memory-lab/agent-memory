import pytest
from agent_memory_evolution import (
    DeterministicRetrievalPolicyEvaluator,
    RetrievalPolicyCandidate,
    RetrievalPolicyPointer,
    RetrievalReplaySample,
)

from agent_memory import MemoryChannel, MemoryScope


def candidate(scope: MemoryScope) -> RetrievalPolicyCandidate:
    return RetrievalPolicyCandidate(
        scope=scope,
        baseline_version="rrf-v1",
        version="rrf-weighted-v2",
        channel_weights={
            MemoryChannel.SEMANTIC: 0.5,
            MemoryChannel.EPISODIC: 0.3,
            MemoryChannel.PROCEDURAL: 0.2,
        },
        state_budget_ratio=0.4,
        token_budget=900,
        baseline_token_budget=1200,
    )


def samples(scope: MemoryScope, count: int, *, degraded: bool = False):
    return tuple(
        RetrievalReplaySample(
            scope=scope,
            feedback_version="feedback-v1",
            baseline_utility=0.75,
            candidate_utility=0.70 if degraded else 0.80,
            baseline_tokens=1000,
            candidate_tokens=850,
        )
        for _ in range(count)
    )


def test_policy_requires_enough_fixed_scope_samples_and_can_rollback() -> None:
    scope = MemoryScope("tenant", session_id="session")
    policy = candidate(scope)
    evaluator = DeterministicRetrievalPolicyEvaluator(minimum_samples=3)
    assert evaluator.evaluate(policy, samples(scope, 2)).passed is False
    assert evaluator.evaluate(policy, samples(scope, 3, degraded=True)).passed is False

    report = evaluator.evaluate(policy, samples(scope, 3))
    assert report.passed is True
    pointer = RetrievalPolicyPointer("rrf-v1")
    pointer.activate(policy, report)
    assert pointer.active_version == "rrf-weighted-v2"
    pointer.rollback()
    assert pointer.active_version == "rrf-v1"


def test_policy_cannot_expand_budget_or_accept_cross_scope_replay() -> None:
    scope = MemoryScope("tenant", session_id="session")
    with pytest.raises(ValueError, match="cannot expand"):
        RetrievalPolicyCandidate(
            scope=scope,
            baseline_version="v1",
            version="v2",
            channel_weights={MemoryChannel.SEMANTIC: 1.0},
            state_budget_ratio=0.5,
            token_budget=1300,
            baseline_token_budget=1200,
        )
    wrong_scope = MemoryScope("other", session_id="session")
    report = DeterministicRetrievalPolicyEvaluator(minimum_samples=1).evaluate(
        candidate(scope), samples(wrong_scope, 1)
    )
    assert report.passed is False
    assert "replay sample scope mismatch" in report.reasons
