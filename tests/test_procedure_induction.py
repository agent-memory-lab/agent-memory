"""T38 acceptance tests for conservative Procedure induction."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from agent_memory import (
    ArtifactStatus,
    ConsolidationRequest,
    DeterministicProcedureInducer,
    Episode,
    MemoryScope,
    OutcomeStatus,
    PluginContext,
    PluginResourceLimits,
    ProcedureInductionLimits,
    Provenance,
    RewardSignal,
    induce_procedure_candidates,
)


NOW = datetime(2026, 9, 20, tzinfo=timezone.utc)
SCOPE = MemoryScope("tenant", session_id="session")


def episode(
    index: int,
    status: OutcomeStatus,
    *,
    action: str = "Use bounded evidence retrieval",
    quality: float = 0.9,
) -> Episode:
    return Episode(
        id=f"episode-{index}",
        scope=SCOPE,
        observation=f"task-context-{index % 2}",
        action=action,
        outcome=f"{status.value}-outcome-{index}",
        lesson=f"{status.value}-lesson-{index}",
        quality=quality,
        outcome_status=status,
        outcome_ids=(f"outcome-{index}",),
        provenance=Provenance(source_event_ids=(f"event-{index}",)),
        occurred_at=NOW + timedelta(minutes=index),
    )


def reward(index: int, definition: str = "task-success") -> RewardSignal:
    return RewardSignal(
        id=f"reward-{index}",
        scope=SCOPE,
        outcome_id=f"outcome-{index}",
        value=1.0,
        formula_version="v1",
        reward_definition_id=definition,
        created_at=NOW + timedelta(minutes=index),
    )


def test_induces_candidate_with_boundaries_counterexamples_and_evidence_range():
    episodes = (
        episode(1, OutcomeStatus.SUCCEEDED),
        episode(2, OutcomeStatus.SUCCEEDED),
        episode(3, OutcomeStatus.SUCCEEDED),
        episode(4, OutcomeStatus.FAILED),
        episode(5, OutcomeStatus.TIMED_OUT),
    )
    plan = induce_procedure_candidates(
        ConsolidationRequest(SCOPE, episodes=episodes), now=NOW + timedelta(hours=1)
    )

    assert plan.rejections == ()
    procedure = plan.procedures[0]
    assert procedure.status is ArtifactStatus.CANDIDATE
    assert procedure.source_episode_ids == tuple(sorted(item.id for item in episodes))
    assert procedure.counterexample_episode_ids == ("episode-4", "episode-5")
    assert procedure.failure_patterns == ("failed-lesson-4", "timed_out-lesson-5")
    assert procedure.applicability_conditions == ("task-context-1", "task-context-0")
    assert procedure.extractor_version == "deterministic-procedure-inducer-v1"
    assert procedure.evidence_start_at == episodes[0].occurred_at
    assert procedure.evidence_end_at == episodes[-1].occurred_at
    assert procedure.provenance.source_event_ids == tuple(
        f"event-{index}" for index in range(1, 6)
    )


def test_insufficient_support_and_unknown_outcome_are_rejected():
    insufficient = induce_procedure_candidates(
        ConsolidationRequest(
            SCOPE,
            episodes=(
                episode(1, OutcomeStatus.SUCCEEDED),
                episode(2, OutcomeStatus.SUCCEEDED),
                episode(3, OutcomeStatus.FAILED),
            ),
        ),
        now=NOW,
    )
    assert insufficient.procedures == ()
    assert insufficient.rejections[0].reason == "insufficient_support"

    unknown = induce_procedure_candidates(
        ConsolidationRequest(
            SCOPE,
            episodes=(
                episode(1, OutcomeStatus.SUCCEEDED),
                episode(2, OutcomeStatus.SUCCEEDED),
                episode(3, OutcomeStatus.SUCCEEDED),
                episode(4, OutcomeStatus.UNKNOWN),
            ),
        ),
        now=NOW,
    )
    assert unknown.procedures == ()
    assert unknown.rejections[0].reason == "unknown_outcome"


def test_incompatible_or_incomplete_reward_definitions_reject_group():
    episodes = tuple(episode(index, OutcomeStatus.SUCCEEDED) for index in range(1, 4))
    incompatible = induce_procedure_candidates(
        ConsolidationRequest(
            SCOPE,
            episodes=episodes,
            rewards=(reward(1), reward(2), reward(3, "different-definition")),
        ),
        now=NOW + timedelta(hours=1),
    )
    assert incompatible.procedures == ()
    assert incompatible.rejections[0].reason == "incompatible_evaluation"

    incomplete = induce_procedure_candidates(
        ConsolidationRequest(SCOPE, episodes=episodes, rewards=(reward(1), reward(2))),
        now=NOW + timedelta(hours=1),
    )
    assert incomplete.rejections[0].reason == "incomplete_evaluation"


def test_compatible_rewards_are_recorded_without_promotion():
    episodes = tuple(episode(index, OutcomeStatus.SUCCEEDED) for index in range(1, 4))
    plan = induce_procedure_candidates(
        ConsolidationRequest(
            SCOPE,
            episodes=episodes,
            rewards=tuple(reward(index) for index in range(1, 4)),
        ),
        now=NOW + timedelta(hours=1),
    )
    procedure = plan.procedures[0]
    assert procedure.reward_definition_id == "task-success"
    assert procedure.reward_formula_version == "v1"
    assert procedure.status is ArtifactStatus.CANDIDATE


def test_deleted_evidence_cannot_support_induction():
    episodes = tuple(episode(index, OutcomeStatus.SUCCEEDED) for index in range(1, 4))
    plan = induce_procedure_candidates(
        ConsolidationRequest(
            SCOPE,
            episodes=episodes,
            deleted_event_ids=("event-3",),
        ),
        now=NOW,
    )
    assert plan.procedures == ()
    assert plan.rejections[0].reason == "insufficient_support"


def test_plugin_returns_candidates_only_and_respects_limits():
    async def scenario():
        episodes = tuple(episode(index, OutcomeStatus.SUCCEEDED) for index in range(1, 4))
        context = PluginContext(SCOPE, PluginResourceLimits(max_batch_size=4))
        plugin = DeterministicProcedureInducer(
            ProcedureInductionLimits(minimum_successful_episodes=3)
        )
        await plugin.initialize(context)
        result = await plugin.consolidate(
            ConsolidationRequest(SCOPE, episodes=episodes), context
        )
        assert len(result.procedures) == 1
        assert result.procedures[0].status is ArtifactStatus.CANDIDATE
        assert (await plugin.health()).status.value == "ready"
        await plugin.close()

    asyncio.run(scenario())
