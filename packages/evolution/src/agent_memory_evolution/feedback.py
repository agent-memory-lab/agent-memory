from __future__ import annotations

from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from agent_memory.domain import (
    DecisionRecord,
    Episode,
    EvaluationRecord,
    FeedbackStatus,
    MemoryScope,
    OutcomeEvent,
    Provenance,
    RewardSignal,
)


@dataclass(frozen=True, slots=True)
class FeedbackTrajectory:
    """A complete, trusted feedback chain supplied by the host."""

    scope: MemoryScope
    decision: DecisionRecord
    outcome: OutcomeEvent
    evaluation: EvaluationRecord
    reward: RewardSignal
    source_event_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        partition = self.scope.partition_key()
        records = (self.decision, self.outcome, self.evaluation, self.reward)
        if any(record.scope.partition_key() != partition for record in records):
            raise ValueError("feedback trajectory records must share one exact scope")
        if self.outcome.decision_id != self.decision.id:
            raise ValueError("outcome must reference the trajectory decision")
        if self.evaluation.outcome_id != self.outcome.id:
            raise ValueError("evaluation must reference the trajectory outcome")
        if self.reward.outcome_id != self.outcome.id:
            raise ValueError("reward must reference the trajectory outcome")
        if self.reward.evaluation_id != self.evaluation.id:
            raise ValueError("reward must reference the trajectory evaluation")
        if any(record.feedback_status != FeedbackStatus.ACCEPTED for record in records):
            raise ValueError("only accepted feedback can form an episode")
        if not self.source_event_ids or any(not item.strip() for item in self.source_event_ids):
            raise ValueError("feedback trajectory requires source-event evidence")


class FeedbackEpisodeBuilder:
    VERSION = "feedback-episode-v1"

    def build(self, trajectory: FeedbackTrajectory) -> Episode:
        outcome_status = trajectory.outcome.outcome_status
        if outcome_status is None:  # guarded by OutcomeEvent, retained for type narrowing
            raise ValueError("outcome status is required")
        source_event_ids = tuple(dict.fromkeys(trajectory.source_event_ids))
        identity = ":".join(
            (
                trajectory.scope.partition_key(),
                trajectory.decision.id,
                trajectory.outcome.id,
                trajectory.evaluation.id,
                trajectory.reward.id,
                self.VERSION,
            )
        )
        quality = trajectory.evaluation.metrics.get("quality")
        if quality is None:
            quality = trajectory.outcome.score
        if quality is None:
            quality = max(0.0, min(1.0, (trajectory.reward.value + 1.0) / 2.0))
        quality = max(0.0, min(1.0, float(quality)))
        outcome = f"{outcome_status.value}:{trajectory.outcome.outcome}"
        lesson = (
            f"Counterexample ({outcome_status.value}): {trajectory.outcome.outcome}"
            if trajectory.outcome.success is not True
            else f"Validated outcome: {trajectory.outcome.outcome}"
        )
        return Episode(
            id=str(uuid5(NAMESPACE_URL, identity)),
            scope=trajectory.scope,
            observation=(
                trajectory.decision.context_hash
                or trajectory.decision.run_id
                or "host context"
            ),
            action=trajectory.decision.action,
            outcome=outcome,
            lesson=lesson,
            quality=quality,
            outcome_status=outcome_status,
            run_id=trajectory.decision.run_id or trajectory.outcome.run_id,
            decision_ids=(trajectory.decision.id,),
            outcome_ids=(trajectory.outcome.id,),
            provenance=Provenance(
                source_event_ids=source_event_ids,
                extractor=type(self).__name__,
                provider="agent-memory-evolution",
                prompt_version=self.VERSION,
            ),
            occurred_at=trajectory.outcome.occurred_at,
        )
