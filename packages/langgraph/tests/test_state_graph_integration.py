"""Exercise public hooks through real LangGraph config injection, not direct calls."""

import asyncio
from collections.abc import Mapping
from typing import Any, TypedDict

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryScope
from agent_memory_langgraph import LangGraphKeys, LangGraphMemoryAdapter

# A bad config annotation must fail at graph construction, even if a future
# LangGraph release allows the resulting invocation to continue.
pytestmark = pytest.mark.filterwarnings("error::UserWarning")


class GraphState(TypedDict, total=False):
    messages: list[dict[str, str]]
    memory: dict[str, Any]
    memory_tool_event: dict[str, Any]
    memory_response: Any
    memory_receipt: dict[str, Any]
    memory_turn_id: str
    history: list[dict[str, str]]
    recall: dict[str, Any]
    tool: dict[str, Any]
    answer: Any
    receipt: dict[str, Any]
    turn: str
    # Deliberately retained by the graph so tests prove the adapter ignores them.
    memory_tenant_id: str
    memory_user_id: str
    thread_id: str


CUSTOM_KEYS = LangGraphKeys(
    messages="history", bundle="recall", tool_event="tool", response="answer",
    receipt="receipt", turn_id="turn",
)


class RecordingProvider:
    """Observe the adapter boundary while retaining real SQLite write/retrieval."""

    def __init__(self, kernel):
        self.kernel = kernel
        self.queries = []
        self.events = []
        self.receipts = []

    async def initialize(self):
        await self.kernel.initialize()

    async def retrieve(self, query):
        self.queries.append(query)
        return await self.kernel.retrieve(query)

    async def ingest_event(self, event):
        self.events.append(event)
        receipt = await self.kernel.ingest_event(event)
        self.receipts.append(receipt)
        return receipt


def compile_hooks(adapter, *hooks):
    graph = StateGraph(GraphState)
    previous = START
    for index, hook in enumerate(hooks):
        name = f"{hook}_{index}"
        # This exact bound-method registration is the advertised integration.
        graph.add_node(name, getattr(adapter, hook))
        graph.add_edge(previous, name)
        previous = name
    graph.add_edge(previous, END)
    return graph.compile()


def trusted_config(**overrides) -> RunnableConfig:
    return {"configurable": {
        "memory_tenant_id": "trusted-tenant",
        "memory_namespace": "research",
        "memory_user_id": "trusted-user",
        "memory_agent_id": "trusted-agent",
        "memory_workspace_id": "trusted-workspace",
        "thread_id": "trusted-thread",
        "memory_actor": "host-agent",
        **overrides,
    }}


def input_state(keys=LangGraphKeys()):
    return {
        keys.messages: [{"content": "Where is the zirconium certificate?"}],
        keys.tool_event: {
            "tool_call_id": "call-1", "tool_name": "lookup",
            "result": {"location": "The zirconium certificate is in the blue archive box."},
        },
        keys.response: {"content": "I found the zirconium certificate."},
        keys.turn_id: "turn-7",
        "memory_tenant_id": "untrusted",
        "memory_user_id": "untrusted",
        "thread_id": "untrusted",
    }


@pytest.mark.parametrize("keys", [LangGraphKeys(), CUSTOM_KEYS], ids=["default", "custom"])
def test_compiled_graph_runs_all_hooks_with_trusted_config_and_replay(tmp_path, keys):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "graph.db")
        provider = RecordingProvider(kernel)
        adapter = LangGraphMemoryAdapter(
            provider, keys=keys, token_budget=512, limit=3,
            tool_result_projector=lambda name, result: result["location"],
        )
        await adapter.initialize()
        try:
            app = compile_hooks(adapter, "after_tool", "before_model", "after_model")
            config = trusted_config(memory_session_id="explicit-session")
            state = input_state(keys)
            result = await app.ainvoke(state, config=config)
            assert "blue archive box" in str(result[keys.bundle]["relevant_memories"])
            assert result[keys.receipt]["duplicate"] is False
            assert [event.event_type for event in provider.events] == [
                "agent.tool.completed", "agent.model.completed",
            ]
            expected_scope = MemoryScope(
                tenant_id="trusted-tenant", namespace="research", user_id="trusted-user",
                agent_id="trusted-agent", workspace_id="trusted-workspace",
                session_id="explicit-session",
            )
            assert all(event.scope == expected_scope for event in provider.events)
            assert provider.queries[0].scope == expected_scope
            assert provider.queries[0].text == "Where is the zirconium certificate?"
            assert provider.queries[0].limit == 3
            assert provider.queries[0].token_budget == 512
            assert all(event.actor == "host-agent" for event in provider.events)
            assert [event.idempotency_key for event in provider.events] == [
                "trusted-thread:turn-7:after_tool:call-1",
                "trusted-thread:turn-7:after_model:default",
            ]
            assert all(not receipt.duplicate for receipt in provider.receipts)
            replay = await app.ainvoke(state, config=config)
            assert replay[keys.receipt]["duplicate"] is True
            assert all(receipt.duplicate for receipt in provider.receipts[2:])
        finally:
            await kernel.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("changed_key", [
    "memory_tenant_id", "memory_namespace", "memory_user_id", "memory_agent_id",
    "memory_workspace_id", "thread_id",
])
def test_compiled_graph_recall_cannot_cross_configured_scope(tmp_path, changed_key):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "isolation.db")
        adapter = LangGraphMemoryAdapter(
            kernel, tool_result_projector=lambda name, result: result["location"],
        )
        await adapter.initialize()
        try:
            capture = compile_hooks(adapter, "after_tool")
            recall = compile_hooks(adapter, "before_model")
            state = input_state()
            await capture.ainvoke(state, config=trusted_config())
            own = await recall.ainvoke(state, config=trusted_config())
            assert "blue archive box" in str(own["memory"]["relevant_memories"])
            other = await recall.ainvoke(state, config=trusted_config(**{changed_key: "other"}))
            assert other["memory"]["relevant_memories"] == []
        finally:
            await kernel.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("hook", ["before_model", "after_tool", "after_model"])
@pytest.mark.parametrize("config", [None, {"configurable": {"memory_tenant_id": " "}}])
def test_compiled_hooks_fail_closed_without_trusted_tenant(tmp_path, hook, config):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "missing-scope.db")
        provider = RecordingProvider(kernel)
        adapter = LangGraphMemoryAdapter(provider)
        await adapter.initialize()
        try:
            app = compile_hooks(adapter, hook)
            with pytest.raises(ValueError, match="configurable.memory_tenant_id is required"):
                await app.ainvoke(input_state(), config=config)
            assert provider.events == []
            assert provider.queries == []
        finally:
            await kernel.close()

    asyncio.run(scenario())


def test_compiled_graph_passes_config_to_host_scope_resolver(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "resolver.db")
        provider = RecordingProvider(kernel)
        received = []
        scope = MemoryScope("resolved-tenant", session_id="resolved-session")

        def resolve(config: Mapping[str, Any]) -> MemoryScope:
            received.append(config)
            assert config["configurable"]["host_principal"] == "authenticated-user"
            assert "host-tag" in config["tags"]
            assert config["metadata"]["host_request"] == "request-42"
            return scope

        adapter = LangGraphMemoryAdapter(provider, scope_resolver=resolve)
        await adapter.initialize()
        try:
            app = compile_hooks(adapter, "before_model", "after_tool", "after_model")
            await app.ainvoke(input_state(), config={
                "configurable": {"host_principal": "authenticated-user", "thread_id": "run"},
                "tags": ["host-tag"], "metadata": {"host_request": "request-42"},
            })
            assert len(received) == 3
            assert provider.queries[0].scope == scope
            assert all(event.scope == scope for event in provider.events)
        finally:
            await kernel.close()

    asyncio.run(scenario())


def test_readme_stategraph_example_runs(tmp_path, monkeypatch, capsys):
    from pathlib import Path

    readme = Path(__file__).resolve().parents[1] / "README.md"
    example = readme.read_text(encoding="utf-8").split("```python\n", 1)[1].split("```", 1)[0]
    monkeypatch.chdir(tmp_path)
    exec(compile(example, str(readme), "exec"), {"__name__": "__main__"})
    assert "relevant_memories" in capsys.readouterr().out
