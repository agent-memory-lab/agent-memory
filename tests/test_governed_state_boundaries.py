"""Governed recall retains native scope inheritance and bounded state delivery."""

import asyncio
from dataclasses import replace

import pytest

from agent_memory import AgentMemory, MemoryScope
from agent_memory.retrieval.bundle import BundleBudget, pack_memory_bundle
from test_governed_recall_integration import (
    GovernedRecallPipeline, TrustedResolver, loaded_retriever,
)


@pytest.mark.parametrize("level", ["tenant", "user", "agent", "workspace", "session"])
def test_governed_recall_inherits_visible_state(tmp_path, level):
    async def run():
        scope = MemoryScope("tenant", user_id="user", agent_id="agent",
                            workspace_id="workspace", session_id="session")
        pipeline = GovernedRecallPipeline((loaded_retriever(scope),), TrustedResolver())
        async with AgentMemory.local(tmp_path / "memory.db", scope=scope,
                                     recall_pipeline=pipeline) as memory:
            await memory.remember("Prefer concise answers.", claims=[{
                "key": "style", "value": "concise", "text": "Prefer concise answers.",
                "scope_level": level,
            }])
            result = await memory.recall("style")
            assert len(result.current_state) == 1
            assert result.current_state[0].scope == scope.project(level)
    asyncio.run(run())


@pytest.mark.parametrize("field", ["tenant_id", "namespace", "user_id", "agent_id",
                                    "workspace_id", "session_id"])
def test_packer_rejects_foreign_state_even_when_oversized(tmp_path, field):
    async def run():
        scope = MemoryScope("tenant", user_id="user", agent_id="agent",
                            workspace_id="workspace", session_id="session")
        async with AgentMemory.local(tmp_path / "memory.db", scope=scope) as memory:
            await memory.remember("Concise.", claims=[{
                "key": "style", "value": "concise", "text": "Concise.",
            }])
            claim = (await memory.state())[0]
            foreign = replace(claim, scope=replace(scope, **{field: "other"}), text="x" * 9000)
            with pytest.raises(ValueError, match="scope"):
                pack_memory_bundle(scope, (), current_state=(foreign,), bound_current_state=True)
    asyncio.run(run())


@pytest.mark.parametrize("texts,budget", [
    (["preference " * 600], BundleBudget()),
    (["preference " * 600, "Concise."], BundleBudget()),
    (["a" * 160, "b" * 160, "c" * 20], BundleBudget(max_tokens=64)),
    (["a" * 80, "b" * 80, "c" * 20], BundleBudget(max_characters=100)),
])
def test_governed_state_budget_skips_whole_claims(tmp_path, texts, budget):
    async def run():
        scope = MemoryScope("tenant", session_id="session")
        pipeline = GovernedRecallPipeline((loaded_retriever(scope),), TrustedResolver(),
                                         bundle_budget=budget)
        async with AgentMemory.local(tmp_path / "memory.db", scope=scope,
                                     recall_pipeline=pipeline) as memory:
            for index, text in enumerate(texts):
                await memory.remember(text, claims=[{
                    "key": f"style-{index}", "value": text, "text": text,
                }])
            state = await memory.state()
            with pytest.raises(ValueError, match="current state alone"):
                pack_memory_bundle(scope, (), current_state=state, budget=budget)
            first = await memory.recall("style", token_budget=budget.max_tokens)
            second = await memory.recall("style", token_budget=budget.max_tokens)
            assert first.current_state == second.current_state
            assert len(first.current_state) < len(state)
            if len(texts) > 1:
                assert first.current_state
            assert all(claim in state for claim in first.current_state)
            assert first.token_estimate <= budget.max_tokens
            assert sum(len(value.text) for value in (*first.current_state,
                       *first.relevant_memories, *first.episodes, *first.procedures)) <= budget.max_characters
            assert first.retrieval_metadata["state_dropped_budget"] == len(state) - len(first.current_state)
            assert first.retrieval_metadata["degraded"] is True
            assert first.retrieval_metadata["coverage"] == "partial"
            assert first.retrieval_metadata["world_negative"] is False
    asyncio.run(run())
