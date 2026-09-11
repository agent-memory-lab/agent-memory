import asyncio

import pytest
from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    EvolutionEngine,
    FeedbackEpisodeBuilder,
    FeedbackTrajectory,
    NullProcedureDeployment,
    RuleBasedProcedureGenerator,
    SQLiteEvolutionRegistry,
)

from agent_memory import (
    DecisionRecord,
    EvaluationRecord,
    FeedbackStatus,
    MemoryScope,
    OutcomeEvent,
    OutcomeStatus,
    RewardSignal,
)


def trajectory(scope: MemoryScope, index: int, *, success: bool = True) -> FeedbackTrajectory:
    decision = DecisionRecord(
        id=f"decision-{index}",
        scope=scope,
        action="Use bounded evidence retrieval",
        memory_ids=(),
        context_hash="sha256:context",
    )
    outcome = OutcomeEvent(
        id=f"outcome-{index}",
        scope=scope,
        decision_id=decision.id,
        outcome="answer accepted" if success else "stale evidence selected",
        success=success,
        outcome_status=OutcomeStatus.SUCCEEDED if success else OutcomeStatus.FAILED,
        score=0.9 if success else 0.8,
    )
    evaluation = EvaluationRecord(
        id=f"evaluation-{index}",
        scope=scope,
        outcome_id=outcome.id,
        evaluator_id="trusted-host",
        evaluator_version="1",
        rubric_id="task-result",
        rubric_version="1",
        metrics={"quality": 0.9 if success else 0.8},
        evidence_digest=f"sha256:{index}",
    )
    reward = RewardSignal(
        id=f"reward-{index}",
        scope=scope,
        outcome_id=outcome.id,
        evaluation_id=evaluation.id,
        value=1.0 if success else -1.0,
        formula_version="1",
    )
    return FeedbackTrajectory(
        scope=scope,
        decision=decision,
        outcome=outcome,
        evaluation=evaluation,
        reward=reward,
        source_event_ids=(f"event-{index}",),
    )


def test_complete_feedback_builds_idempotent_candidate_with_counterexample(tmp_path) -> None:
    async def scenario() -> None:
        scope = MemoryScope("tenant", session_id="session")
        builder = FeedbackEpisodeBuilder()
        episodes = tuple(
            builder.build(trajectory(scope, index, success=index < 4))
            for index in range(1, 5)
        )
        assert builder.build(trajectory(scope, 1)).id == episodes[0].id
        assert episodes[-1].outcome.startswith("failed:")

        registry = SQLiteEvolutionRegistry(tmp_path / "evolution.db")
        engine = EvolutionEngine(
            registry, DeterministicPromotionPolicy(), NullProcedureDeployment()
        )
        await engine.initialize()
        generator = RuleBasedProcedureGenerator(minimum_support=3)
        first = await engine.generate_candidates(scope, episodes, generator)
        second = await engine.generate_candidates(scope, episodes, generator)

        assert len(first) == len(second) == 1
        assert first[0].id == second[0].id
        assert first[0].procedure.id == second[0].procedure.id
        assert first[0].procedure.failure_patterns == (
            "Counterexample (failed): stale evidence selected",
        )
        assert len(first[0].source_episode_ids) == 4

    asyncio.run(scenario())


def test_failure_does_not_count_as_successful_support() -> None:
    async def scenario() -> None:
        scope = MemoryScope("tenant", session_id="session")
        builder = FeedbackEpisodeBuilder()
        episodes = (
            builder.build(trajectory(scope, 1)),
            builder.build(trajectory(scope, 2)),
            builder.build(trajectory(scope, 3, success=False)),
        )
        generated = await RuleBasedProcedureGenerator(minimum_support=3).generate(
            scope, episodes
        )
        assert generated == ()

    asyncio.run(scenario())


def test_incomplete_or_untrusted_feedback_is_rejected() -> None:
    scope = MemoryScope("tenant", session_id="session")
    valid = trajectory(scope, 1)
    pending = DecisionRecord(
        id=valid.decision.id,
        scope=scope,
        action=valid.decision.action,
        memory_ids=(),
        feedback_status=FeedbackStatus.PENDING,
    )
    with pytest.raises(ValueError, match="only accepted"):
        FeedbackTrajectory(
            scope=scope,
            decision=pending,
            outcome=valid.outcome,
            evaluation=valid.evaluation,
            reward=valid.reward,
            source_event_ids=valid.source_event_ids,
        )

    with pytest.raises(ValueError, match="source-event evidence"):
        FeedbackTrajectory(
            scope=scope,
            decision=valid.decision,
            outcome=valid.outcome,
            evaluation=valid.evaluation,
            reward=valid.reward,
            source_event_ids=(),
        )
