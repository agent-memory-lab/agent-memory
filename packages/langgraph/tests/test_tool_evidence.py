import asyncio

import pytest

from agent_memory_langgraph import LangGraphMemoryAdapter

from agent_memory.composition import build_local_kernel


@pytest.mark.parametrize("enabled", [False, True])
def test_tool_evidence_requires_host_configuration_not_graph_state(tmp_path, enabled):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "graph.db")
        options = (
            {"tool_result_projector": lambda name, result: result["location"]}
            if enabled else {}
        )
        adapter = LangGraphMemoryAdapter(kernel, **options)
        await adapter.initialize()
        config = {"configurable": {"memory_tenant_id": "graph", "thread_id": "session-1"}}
        state = {
            "messages": [{"content": "Where is the zirconium certificate?"}],
            "memory_tool_event": {
                "tool_call_id": "call-1",
                "tool_name": "lookup",
                "result": {
                    "location": "The zirconium certificate is in the blue archive box.",
                    "private": "unapproved-result-marker",
                },
                "arguments": {"private": "unapproved-argument-marker"},
                "evidence_text": "graph-state-must-not-enable-projection",
                "tool_result_projector": "untrusted-field",
                "success": True,
            },
        }
        try:
            receipt = await adapter.after_tool(state, config)
            assert receipt["memory_receipt"]["claim_ids"] == []
            response = await adapter.before_model(state, config)
            items = response["memory"]["relevant_memories"]
            assert bool(items) == enabled
            if enabled:
                assert any("blue archive box" in item["text"] for item in items)
            for marker in (
                "unapproved-result-marker", "unapproved-argument-marker",
                "graph-state-must-not-enable-projection",
            ):
                assert marker not in str(response)
        finally:
            await kernel.close()

    asyncio.run(scenario())


def test_host_byte_budget_reaches_generic_adapter(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "disabled.db")
        adapter = LangGraphMemoryAdapter(
            kernel, tool_result_projector=lambda name, result: result,
            tool_result_max_bytes=0,
        )
        await adapter.initialize()
        config = {"configurable": {"memory_tenant_id": "graph", "thread_id": "session-1"}}
        state = {
            "messages": [{"content": "zirconium"}],
            "memory_tool_event": {
                "tool_call_id": "call-1", "tool_name": "lookup", "result": "zirconium"
            },
        }
        try:
            await adapter.after_tool(state, config)
            response = await adapter.before_model(state, config)
            assert response["memory"]["relevant_memories"] == []
        finally:
            await kernel.close()

    asyncio.run(scenario())
