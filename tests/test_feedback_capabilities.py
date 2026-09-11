import asyncio
from dataclasses import replace

import pytest

from agent_memory import (
    AgentLifecycleContext,
    AgentMemory,
    AgentMemoryAdapter,
    MCPMemoryTools,
    MCPRequestContext,
    MCPToolError,
)


class ProviderWithoutFeedback:
    def __init__(self, delegate) -> None:
        self._delegate = delegate

    def manifest(self):
        manifest = self._delegate.manifest()
        return replace(
            manifest,
            capabilities=replace(
                manifest.capabilities,
                decision_lineage=False,
                outcome_feedback=False,
            ),
        )

    def __getattr__(self, name):
        return getattr(self._delegate, name)


def test_feedback_tools_and_adapter_degrade_by_capability(tmp_path) -> None:
    async def scenario() -> None:
        memory = AgentMemory.local(tmp_path / "memory.db")
        await memory.initialize()
        provider = ProviderWithoutFeedback(memory.provider)
        tools = MCPMemoryTools(provider)
        names = {tool["name"] for tool in tools.list_tools()}
        assert not any(name.startswith("memory_record_") for name in names)
        assert "memory_feedback_status" not in names

        context = MCPRequestContext(memory.scope)
        with pytest.raises(MCPToolError, match="does not support feedback"):
            await tools.call_tool(
                "memory_record_decision",
                {"action": "answer", "memory_usage": "none"},
                context,
            )

        adapter = AgentMemoryAdapter(provider)
        bundle = await memory.recall("anything")
        with pytest.raises(NotImplementedError, match="does not support lifecycle feedback"):
                await adapter.record_decision(
                AgentLifecycleContext(scope=memory.scope, run_id="run", turn_id="turn"),
                bundle,
                "answer",
            )

    asyncio.run(scenario())
