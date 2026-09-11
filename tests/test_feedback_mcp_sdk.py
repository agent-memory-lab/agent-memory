import asyncio

import pytest
from agent_memory_sdk import EmbeddedMemoryClient

from agent_memory import AgentMemory, MCPRequestContext, MemoryScope


def test_embedded_sdk_feedback_round_trip_uses_trusted_scope(tmp_path) -> None:
    async def scenario() -> None:
        memory = AgentMemory.local(tmp_path / "memory.db")
        context = MCPRequestContext(memory.scope, actor="host")
        client = EmbeddedMemoryClient(memory.provider, context)
        await client.initialize()

        decision = await client.record_decision(
            "answer",
            memory_usage="none",
            idempotency_key="decision-sdk",
        )
        outcome = await client.record_outcome(
            decision["record_id"],
            "accepted",
            True,
            idempotency_key="outcome-sdk",
        )
        evaluation = await client.record_evaluation(
            outcome["record_id"],
            evaluator_id="host",
            evaluator_version="1",
            rubric_id="quality",
            rubric_version="1",
            metrics={"quality": 1.0},
            evidence_digest="sha256:evidence",
        )
        reward = await client.record_reward(
            outcome["record_id"],
            1.0,
            "formula-1",
            evaluation_id=evaluation["record_id"],
            reward_definition_id="task-success",
        )

        assert all(
            item["record_id"]
            for item in (decision, outcome, evaluation, reward)
        )
        receipt = await client.feedback_status(reward["record_id"], "reward")
        assert receipt["receipt"]["status"] == "accepted"

    asyncio.run(scenario())


def test_embedded_sdk_cannot_reference_feedback_in_another_scope(tmp_path) -> None:
    async def scenario() -> None:
        memory = AgentMemory.local(
            tmp_path / "memory.db",
            scope=MemoryScope("tenant-a", session_id="one"),
        )
        owner = EmbeddedMemoryClient(
            memory.provider,
            MCPRequestContext(MemoryScope("tenant-a", session_id="one")),
        )
        await owner.initialize()
        decision = await owner.record_decision("answer", memory_usage="none")

        intruder = EmbeddedMemoryClient(
            memory.provider,
            MCPRequestContext(MemoryScope("tenant-b", session_id="two")),
        )
        with pytest.raises(ValueError, match="outside the authorized scope"):
            await intruder.record_outcome(decision["record_id"], "stolen", True)

    asyncio.run(scenario())
