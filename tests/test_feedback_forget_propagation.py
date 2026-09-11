import asyncio
import json
import sqlite3

from agent_memory import (
    AgentMemory,
    ArtifactStatus,
    Episode,
    FeedbackStatus,
    MemoryUsage,
    Procedure,
    Provenance,
)


async def _feedback_chain(memory: AgentMemory, query: str):
    bundle = await memory.recall(query)
    used = tuple(citation.memory_id for citation in bundle.citations)
    decision = await memory.record_decision(
        "answer",
        memory_ids=used,
        memory_usage=MemoryUsage.CONFIRMED,
        bundle_id=bundle.bundle_id,
    )
    outcome = await memory.record_outcome(decision.id, "accepted", True)
    evaluation = await memory.record_evaluation(
        outcome.id,
        evaluator_id="host",
        evaluator_version="1",
        rubric_id="quality",
        rubric_version="1",
        metrics={"quality": 1.0},
        evidence_digest="sha256:evidence",
    )
    reward = await memory.record_reward(
        outcome.id,
        1.0,
        "formula-1",
        evaluation_id=evaluation.id,
        reward_definition_id="task-success",
    )
    return bundle, decision, outcome, evaluation, reward


def test_archiving_source_invalidates_entire_feedback_chain(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            event = await memory.remember("Use concise answers")
            chain = await _feedback_chain(memory, "concise")
            await memory.forget(memory_ids=(event.event_id,))

            record_types = ("retrieval", "decision", "outcome", "evaluation", "reward")
            for item, record_type in zip(chain, record_types, strict=True):
                record_id = item.bundle_id if record_type == "retrieval" else item.id
                receipt = await memory.feedback_status(record_id, record_type)
                assert receipt is not None
                assert receipt.status == FeedbackStatus.INVALIDATED

    asyncio.run(scenario())


def test_erasing_source_redacts_feedback_payload_but_keeps_safe_audit(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            event = await memory.remember("secret customer payload")
            chain = await _feedback_chain(memory, "secret")
            await memory.forget(memory_ids=(event.event_id,), erase=True)

        with sqlite3.connect(database) as connection:
            rows = connection.execute(
                "SELECT feedback_status, payload_json FROM evolution_records"
            ).fetchall()
        assert rows
        for status, raw_payload in rows:
            payload = json.loads(raw_payload)
            assert status == FeedbackStatus.INVALIDATED
            assert payload["redacted"] is True
            assert "secret customer payload" not in raw_payload
        assert chain

    asyncio.run(scenario())


def test_forget_removes_event_derived_episode_and_procedure(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            event = await memory.remember("tool trajectory evidence")
            provenance = Provenance(source_event_ids=(event.event_id,))
            episode = Episode(
                scope=memory.scope,
                observation="request",
                action="tool",
                outcome="worked",
                lesson="reuse tool",
                status=ArtifactStatus.CANDIDATE,
                provenance=provenance,
            )
            procedure = Procedure(
                scope=memory.scope,
                name="reuse",
                trigger="request",
                steps=("call tool",),
                success_conditions=("accepted",),
                status=ArtifactStatus.CANDIDATE,
                provenance=provenance,
            )
            await memory.provider.record_episode(episode)
            await memory.provider.publish_procedure(procedure)
            await memory.forget(memory_ids=(event.event_id,))

        with sqlite3.connect(database) as connection:
            states = dict(connection.execute("SELECT id, status FROM artifacts"))
        assert states[episode.id] == ArtifactStatus.ARCHIVED
        assert states[procedure.id] == ArtifactStatus.ARCHIVED

    asyncio.run(scenario())


def test_outcome_correction_invalidates_old_evaluation_and_reward(tmp_path) -> None:
    async def scenario() -> None:
        async with AgentMemory.local(tmp_path / "memory.db") as memory:
            decision = await memory.record_decision(
                "answer", memory_usage=MemoryUsage.NONE
            )
            original = await memory.record_outcome(decision.id, "wrong", False)
            evaluation = await memory.record_evaluation(
                original.id,
                evaluator_id="host",
                evaluator_version="1",
                rubric_id="quality",
                rubric_version="1",
                metrics={"quality": 0.0},
                evidence_digest="sha256:old",
            )
            reward = await memory.record_reward(
                original.id,
                0.0,
                "formula-1",
                evaluation_id=evaluation.id,
            )
            corrected = await memory.record_outcome(
                decision.id,
                "accepted",
                True,
                corrects_id=original.id,
            )

            expected = {
                (original.id, "outcome"): FeedbackStatus.SUPERSEDED,
                (evaluation.id, "evaluation"): FeedbackStatus.INVALIDATED,
                (reward.id, "reward"): FeedbackStatus.INVALIDATED,
                (corrected.id, "outcome"): FeedbackStatus.ACCEPTED,
            }
            for (record_id, record_type), status in expected.items():
                receipt = await memory.feedback_status(record_id, record_type)
                assert receipt is not None and receipt.status == status

    asyncio.run(scenario())
