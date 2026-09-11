import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from agent_memory import (
    AgentMemory,
    FeedbackStatus,
    MemoryScope,
    MemoryUsage,
    OutcomeStatus,
)


def _statuses(database) -> dict[str, str]:
    with sqlite3.connect(database) as connection:
        return dict(connection.execute("SELECT id, feedback_status FROM evolution_records"))


def test_local_feedback_round_trip_is_linked_and_accepted(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            decision = await memory.record_decision(
                "answer",
                memory_usage=MemoryUsage.NONE,
                run_id="run-1",
                idempotency_key="decision-1",
            )
            outcome = await memory.record_outcome(
                decision.id,
                "accepted",
                True,
                run_id="run-1",
                idempotency_key="outcome-1",
            )
            evaluation = await memory.record_evaluation(
                outcome.id,
                evaluator_id="host-evaluator",
                evaluator_version="1",
                rubric_id="answer-quality",
                rubric_version="1",
                metrics={"quality": 1.0},
                evidence_digest="sha256:evidence",
                idempotency_key="evaluation-1",
            )
            reward = await memory.record_reward(
                outcome.id,
                1.0,
                "formula-1",
                evaluation_id=evaluation.id,
                reward_definition_id="task-success",
                idempotency_key="reward-1",
            )

        assert _statuses(database) == {
            decision.id: FeedbackStatus.ACCEPTED,
            outcome.id: FeedbackStatus.ACCEPTED,
            evaluation.id: FeedbackStatus.ACCEPTED,
            reward.id: FeedbackStatus.ACCEPTED,
        }

    asyncio.run(scenario())


def test_out_of_order_feedback_activates_when_chain_arrives(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            reward = await memory.record_reward(
                "outcome-late",
                1.0,
                "formula-1",
                evaluation_id="evaluation-late",
                record_id="reward-early",
            )
            evaluation = await memory.record_evaluation(
                "outcome-late",
                evaluator_id="host",
                evaluator_version="1",
                rubric_id="quality",
                rubric_version="1",
                metrics={"quality": 1.0},
                evidence_digest="sha256:evidence",
                record_id="evaluation-late",
            )
            outcome = await memory.record_outcome(
                "decision-late",
                "accepted",
                True,
                idempotency_key="late-outcome",
                record_id="outcome-late",
            )
            assert set(_statuses(database).values()) == {FeedbackStatus.PENDING}
            receipt = await memory.feedback_status(reward.id, "reward")
            assert receipt is not None and receipt.status == FeedbackStatus.PENDING

            decision = await memory.record_decision(
                "answer",
                memory_usage=MemoryUsage.NONE,
                idempotency_key="late-decision",
                record_id="decision-late",
            )
            assert set(_statuses(database).values()) == {FeedbackStatus.ACCEPTED}
            receipt = await memory.feedback_status(reward.id, "reward")
            assert receipt is not None and receipt.status == FeedbackStatus.ACCEPTED
            assert decision.id == outcome.decision_id
            assert reward.evaluation_id == evaluation.id

    asyncio.run(scenario())


def test_pending_feedback_expires_and_capacity_is_bounded(tmp_path) -> None:
    async def scenario() -> None:
        async with AgentMemory.local(tmp_path / "memory.db") as memory:
            expired = await memory.record_outcome(
                "missing-expired",
                "unknown",
                None,
                outcome_status=OutcomeStatus.UNKNOWN,
                expires_at=datetime.now(UTC) - timedelta(seconds=1),
            )
            receipt = await memory.feedback_status(expired.id, "outcome")
            assert receipt is not None and receipt.status == FeedbackStatus.EXPIRED

            memory.provider.MAX_PENDING_FEEDBACK_PER_SCOPE = 2
            await memory.record_outcome("missing-1", "unknown", None)
            await memory.record_outcome("missing-2", "unknown", None)
            with pytest.raises(ValueError, match="capacity exceeded"):
                await memory.record_outcome("missing-3", "unknown", None)

    asyncio.run(scenario())


def test_outcome_status_does_not_treat_unknown_as_failure(tmp_path) -> None:
    async def scenario() -> None:
        async with AgentMemory.local(tmp_path / "memory.db") as memory:
            decision = await memory.record_decision(
                "wait", memory_usage=MemoryUsage.NONE
            )
            unknown = await memory.record_outcome(
                decision.id,
                "evaluation unavailable",
                None,
                outcome_status=OutcomeStatus.UNKNOWN,
            )
            cancelled = await memory.record_outcome(
                decision.id,
                "host cancelled",
                None,
                outcome_status=OutcomeStatus.CANCELLED,
            )
            assert unknown.success is None
            assert cancelled.success is None

    asyncio.run(scenario())


def test_host_evaluator_allowlist_rejects_untrusted_source(tmp_path) -> None:
    async def scenario() -> None:
        async with AgentMemory.local(
            tmp_path / "memory.db", trusted_evaluator_ids={"trusted-host"}
        ) as memory:
            decision = await memory.record_decision(
                "answer", memory_usage=MemoryUsage.NONE
            )
            outcome = await memory.record_outcome(decision.id, "accepted", True)
            with pytest.raises(ValueError, match="not authorized"):
                await memory.record_evaluation(
                    outcome.id,
                    evaluator_id="model-self-evaluation",
                    evaluator_version="1",
                    rubric_id="quality",
                    rubric_version="1",
                    metrics={"quality": 1.0},
                    evidence_digest="sha256:self",
                )
            accepted = await memory.record_evaluation(
                outcome.id,
                evaluator_id="trusted-host",
                evaluator_version="1",
                rubric_id="quality",
                rubric_version="1",
                metrics={"quality": 1.0},
                evidence_digest="sha256:host",
            )
            assert accepted.evaluator_id == "trusted-host"

    asyncio.run(scenario())


def test_feedback_rejects_cross_scope_parent(tmp_path) -> None:
    async def scenario() -> None:
        provider_owner = AgentMemory.local(
            tmp_path / "memory.db",
            scope=MemoryScope("tenant-a", session_id="one"),
        )
        await provider_owner.initialize()
        decision = await provider_owner.record_decision(
            "answer", memory_usage=MemoryUsage.NONE
        )

        intruder = AgentMemory(
            provider_owner.provider,
            MemoryScope("tenant-b", session_id="two"),
        )
        await intruder.initialize()
        with pytest.raises(ValueError, match="outside the authorized scope"):
            await intruder.record_outcome(decision.id, "stolen", True)

    asyncio.run(scenario())


def test_feedback_idempotency_reuses_id_and_rejects_conflict(tmp_path) -> None:
    async def scenario() -> None:
        async with AgentMemory.local(tmp_path / "memory.db") as memory:
            first = await memory.record_decision(
                "answer",
                memory_usage=MemoryUsage.NONE,
                idempotency_key="same-request",
            )
            duplicate = await memory.record_decision(
                "answer",
                memory_usage=MemoryUsage.NONE,
                idempotency_key="same-request",
            )
            assert duplicate.id == first.id

            with pytest.raises(ValueError, match="payload differs"):
                await memory.record_decision(
                    "different action",
                    memory_usage=MemoryUsage.NONE,
                    idempotency_key="same-request",
                )

    asyncio.run(scenario())


def test_feedback_correction_supersedes_without_overwrite(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            original = await memory.record_decision(
                "draft",
                memory_usage=MemoryUsage.NONE,
            )
            corrected = await memory.record_decision(
                "final",
                memory_usage=MemoryUsage.NONE,
                corrects_id=original.id,
            )

        statuses = _statuses(database)
        assert statuses[original.id] == FeedbackStatus.SUPERSEDED
        assert statuses[corrected.id] == FeedbackStatus.ACCEPTED

    asyncio.run(scenario())
