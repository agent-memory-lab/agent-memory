# Agent Memory Python SDK

One async client surface for embedded, stdio, and Streamable HTTP deployments.
Every operation maps exactly to the public MCP tool contract.

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
