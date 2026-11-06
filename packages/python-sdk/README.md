# Agent Memory Python SDK

One async client surface for embedded, stdio, and Streamable HTTP deployments.
Every operation maps exactly to the public MCP tool contract.

## Installation

Python 3.13+ is required. From the repository root, install the core first, then
choose the SDK dependencies for your host:

```bash
python -m pip install -e .
python -m pip install -e packages/python-sdk                  # Embedded client only
python -m pip install -e 'packages/python-sdk[mcp]'           # stdio / HTTP client
python -m pip install -e 'packages/python-sdk[langgraph]'     # LangGraph capture bridge
# Combine extras for a remote LangGraph host:
python -m pip install -e 'packages/python-sdk[mcp,langgraph]'
```

The base SDK does not install MCP or LangGraph. Installing the separate
`agent-memory-mcp` server is not required for a remote client. The remote examples
below require the `mcp` extra; authentication and a running server are host concerns.

## Remote and embedded clients

```python
from agent_memory_sdk import MCPMemoryClient

async with MCPMemoryClient.from_http("https://memory.example.com/mcp") as memory:
    bundle = await memory.retrieve("What does this user prefer?")
```

For HTTP authentication, enter a configured `httpx2.AsyncClient` and pass it to
`from_http`. OAuth, mTLS, cookies, and gateway credentials remain transport concerns.

```python
from agent_memory_sdk import EmbeddedMemoryClient
from agent_memory import MCPRequestContext, MemoryScope, build_local_kernel

client = EmbeddedMemoryClient(
    build_local_kernel("./memory.db"),
    MCPRequestContext(MemoryScope(tenant_id="acme", session_id="session-1")),
)
await client.initialize()
```

With a host-configured `ObservationService` passed as `derived=service`, the same
embedded and MCP clients expose `page_capabilities()`, `page_read(page_id)`,
`page_context(page_id)` and `page_status(target_id)`. These operations are read-only.
`complete` certifies the fixed refresh job; `page_complete` also requires the current
guarded page to match that job, including a completed empty page. `page_ready`
requires usable nonempty content. A completed old target cannot certify a newer page.

The first L2 template is `language-scenario/1`, with deterministic full rebuild from
fixed current language Observations. The host registers `ScenarioDefinition` and
`PageDefinition` and uses the existing `FacetRefreshQueue` to request and execute a
refresh. See [the runnable page example](../../examples/derived_scenario_page.py).

## LangGraph update capture

`LangGraphCaptureAdapter` supports LangGraph 1.x (`>=1,<2`), including the legacy
node-update format in 1.0.0 and the v2 stream envelope in newer releases. CI runs
real `StateGraph` tests against 1.0.0 and the latest available 1.x, including subgraph
namespaces. This SDK capture bridge is separate from
[`agent-memory-langgraph`](../langgraph/README.md)'s `LangGraphMemoryAdapter`.

```python
from agent_memory_sdk import LangGraphCaptureAdapter

adapter = LangGraphCaptureAdapter(memory, run_id="stable-host-run-id")
async for chunk in adapter.astream_updates(graph, inputs, subgraphs=True):
    consume(chunk)  # Original graph output, not normalized to a different format.
```

The host supplies a compiled graph and a capture-enabled SDK client. The bridge
requests `stream_mode="updates"`, observes completed messages, and returns the graph's
original chunks unchanged. Older versions can return `(namespace, update)` tuples
with `subgraphs=True`; newer versions return v2 envelopes. Capture failures remain
best-effort and do not change graph output. For direct `observe()` calls, pass only
chunks from an **updates** stream, never arbitrary state, tokens, or other stream modes:
legacy node updates do not carry a mode discriminator. Persist both `run_id` and
`started_at` for stable replay identity. Capture does not infer outcomes, evaluations,
rewards, or permissions from graph state.
