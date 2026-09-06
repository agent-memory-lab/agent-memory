# Agent Memory LangGraph Adapter

LangGraph node callables over the stable `AgentMemoryAdapter`. Memory scope comes
from trusted run configuration, not graph state or model output.

```python
from agent_memory_langgraph import LangGraphMemoryAdapter

memory = LangGraphMemoryAdapter(provider)
graph.add_node("load_memory", memory.before_model)
graph.add_node("capture_tool", memory.after_tool)
graph.add_node("capture_model", memory.after_model)
```

```python
config = {
    "configurable": {
        "memory_tenant_id": "acme",
        "memory_user_id": "user-42",
        "memory_agent_id": "research-agent",
        "thread_id": "session-7",
    }
}
```

`before_model` writes `memory`; `after_tool` consumes `memory_tool_event`; and
`after_model` consumes `memory_response`. All keys can be changed for an existing graph.

