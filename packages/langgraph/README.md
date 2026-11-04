# Agent Memory LangGraph Adapter

LangGraph node callables over the stable `AgentMemoryAdapter`. Memory scope comes
from trusted run configuration, not graph state or model output.

Install `agent-memory-langgraph` alongside your application. The package supports
LangGraph 1.x and declares its public `langchain-core` dependency explicitly;
pip resolves compatible versions of both.

## A complete async graph

```python
import asyncio
from typing import Any, TypedDict

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from agent_memory.composition import build_local_kernel
from agent_memory_langgraph import LangGraphMemoryAdapter


class State(TypedDict, total=False):
    messages: list[dict[str, str]]
    memory: dict[str, Any]


async def main():
    provider = build_local_kernel("memory.db")
    memory = LangGraphMemoryAdapter(provider)
    await memory.initialize()
    try:
        graph = StateGraph(State)
        graph.add_node("load_memory", memory.before_model)
        graph.add_edge(START, "load_memory")
        graph.add_edge("load_memory", END)
        app = graph.compile()
        config: RunnableConfig = {
            "configurable": {
                "memory_tenant_id": "acme",
                "memory_user_id": "user-42",
                "memory_agent_id": "research-agent",
                "thread_id": "session-7",
            }
        }
        result = await app.ainvoke(
            {"messages": [{"content": "How should answers look?"}]},
            config=config,
        )
        print(result["memory"])
    finally:
        await provider.close()


if __name__ == "__main__":
    asyncio.run(main())
```

The async hooks can be registered directly as bound methods. Their `config`
parameters are annotated with `RunnableConfig`, which LangGraph uses to inject
run configuration. Keep that annotation if you add a wrapper; a generic
`Mapping` annotation is not an equivalent node signature.

## Capture hooks and state

In a larger graph, register the capture hooks after the corresponding tool or
model node, and add the input/output keys to your graph's state schema:

```python
graph.add_node("capture_tool", memory.after_tool)
graph.add_node("capture_model", memory.after_model)
```

- `before_model` reads the last `messages` item and writes the `memory` bundle.
- `after_tool` reads `memory_tool_event`: a mapping with `tool_call_id`,
  `tool_name`, and optional `arguments`, `result`, `success`, and `claims`.
- `after_model` reads `memory_response`: a string or a mapping with `content`
  and optional `claims`.
- Both capture hooks write `memory_receipt`. Set `memory_turn_id` to a stable
  per-turn ID for retries; otherwise it defaults to the message count.

Use `LangGraphKeys` to change these state keys for an existing graph. Invoke
compiled graphs asynchronously with `ainvoke` or `astream`.

The default scope resolver requires `configurable.memory_tenant_id`. Optional
host-controlled values are `memory_namespace` (default `default`),
`memory_user_id`, `memory_agent_id`, `memory_workspace_id`, and
`memory_session_id` (falls back to `thread_id`). Missing tenant configuration
fails closed; placing tenant or user fields in graph state does not authorize
access. A host-provided `scope_resolver` can map authenticated run configuration
to a `MemoryScope` instead.

Tool results remain metadata-only by default and their facts are not searchable.
To enable approved result recall, pass a host-controlled `tool_result_projector`
callback that returns sanitized evidence text, optionally with
`tool_result_max_bytes` (4,096 by default; `0` disables projection). Graph state
cannot enable this setting. See [tool-result evidence](../../docs/TOOL_RESULT_EVIDENCE.md)
for the privacy boundary, field-selection example and compatibility notes.

