import asyncio

from agent_memory import AgentMemory, MemoryLimits


def test_zero_config_runtime_is_bounded(tmp_path) -> None:
    async def scenario() -> None:
        limits = MemoryLimits(max_recall_items=2, max_context_tokens=128, max_state_claims=2)
        async with AgentMemory.local(tmp_path / "memory.db", limits=limits) as memory:
            receipt = await memory.remember("Keep answers concise.", idempotency_key="one")
            assert receipt.duplicate is False
            bundle = await memory.recall("answer style", limit=100, token_budget=100_000)
            assert bundle.token_estimate <= 128
            count = len(bundle.relevant_memories) + len(bundle.episodes) + len(bundle.procedures)
            assert count <= 2

    asyncio.run(scenario())
