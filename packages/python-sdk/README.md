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

