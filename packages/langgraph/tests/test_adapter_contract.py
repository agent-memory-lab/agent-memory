import asyncio

from agent_memory.composition import build_local_kernel
from agent_memory_langgraph import LangGraphMemoryAdapter


def test_langgraph_hooks_round_trip(tmp_path) -> None:
    async def scenario() -> None:
        adapter = LangGraphMemoryAdapter(build_local_kernel(tmp_path / "memory.db"))
        await adapter.initialize()
        config = {"configurable": {
            "memory_tenant_id": "graph",
            "memory_user_id": "user-1",
            "thread_id": "thread-1",
        }}
        state = {"messages": [{"content": "How should answers look?"}]}
        assert "memory" in await adapter.before_model(state, config)
        receipt = await adapter.after_tool({
            **state,
            "memory_tool_event": {
                "tool_call_id": "call-1",
                "tool_name": "preferences",
                "result": "concise",
                "success": True,
            },
        }, config)
        assert receipt["memory_receipt"]["duplicate"] is False

    asyncio.run(scenario())

