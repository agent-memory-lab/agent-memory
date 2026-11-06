"""Run unchanged against the minimum and latest supported LangGraph 1.x versions."""

import asyncio
from datetime import UTC, datetime
from typing import Annotated, Any, TypedDict

import pytest

pytest.importorskip("langgraph")

from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages

from agent_memory_sdk import LangGraphCaptureAdapter


class State(TypedDict):
    messages: Annotated[list[Any], add_messages]


class RecordingCapture:
    def __init__(self):
        self.events = []

    async def try_capture(self, event):
        self.events.append(event)
        return {"event_id": event["event_id"], "status": "pending"}


def _graph(nested):
    builder = StateGraph(State)
    builder.add_node("reply", lambda _: {"messages": [AIMessage(id="reply", content="Answer")]})
    builder.add_node("tools", lambda _: {"messages": [
        ToolMessage(id="tool-a", tool_call_id="call-a", content="Result A"),
        ToolMessage(id="tool-b", tool_call_id="call-b", content="Result B"),
    ]})
    builder.add_edge(START, "reply")
    builder.add_edge("reply", "tools")
    builder.add_edge("tools", END)
    graph = builder.compile()
    if nested:
        parent = StateGraph(State)
        parent.add_node("child", graph)
        parent.add_edge(START, "child")
        parent.add_edge("child", END)
        graph = parent.compile()
    return graph


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("subgraphs", [False, True])
def test_real_graph_captures_messages_and_returns_original_chunks(nested, subgraphs):
    async def scenario():
        graph = _graph(nested)
        source_chunks = []

        class ObservedGraph:
            async def astream(self, inputs, **options):
                async for part in graph.astream(inputs, **options):
                    source_chunks.append(part)
                    yield part

        capture = RecordingCapture()
        adapter = LangGraphCaptureAdapter(
            capture, run_id="real-graph", started_at=datetime(2026, 1, 1, tzinfo=UTC)
        )
        returned = [part async for part in adapter.astream_updates(
            ObservedGraph(), {"messages": []}, subgraphs=subgraphs,
            config={"configurable": {"thread_id": "capture-test"}},
        )]
        assert source_chunks and len(returned) == len(source_chunks)
        assert all(left is right for left, right in zip(returned, source_chunks, strict=True))
        assert [event["event_type"] for event in capture.events] == [
            "turn.started", "message.received", "tool.completed", "tool.completed",
            "turn.completed",
        ]
        messages = capture.events[1:-1]
        assert messages[0]["content"] == "Answer"
        assert [event["payload"]["result"] for event in messages[1:]] == ["Result A", "Result B"]
        assert len({event["event_id"] for event in capture.events}) == 5
        if nested and subgraphs:
            assert all(event["payload"]["namespace"][0].startswith("child:") for event in messages)
            assert [event["payload"]["node"] for event in messages] == ["reply", "tools", "tools"]
        else:
            assert all(event["payload"]["namespace"] == [] for event in messages)
        # Parent output and replayed full histories cannot duplicate admissions.
        for part in returned:
            assert await adapter.observe(part) == ()
        assert len(capture.events) == 5

    asyncio.run(scenario())
