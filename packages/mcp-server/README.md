# Agent Memory MCP Server

Official MCP Python SDK v2 transport package for `agent-memory`. It exposes the
stable memory contract over stdio or Streamable HTTP without placing MCP dependencies
in the kernel.

## Local stdio

Each process is bound to one trusted scope supplied by the host, never by model output.

```bash
agent-memory-mcp --transport stdio --database ./agent-memory.db \
  --tenant-id acme --user-id user-42 --agent-id research-agent \
  --session-id session-7
```

## Streamable HTTP

Production HTTP requires a trusted gateway. The gateway authenticates the caller,
removes incoming `x-agent-memory-*` identity headers, writes fresh identity headers,
and signs them with HMAC-SHA256.

```bash
export AGENT_MEMORY_GATEWAY_SECRET='at-least-32-random-bytes-long'
agent-memory-mcp --transport streamable-http --database ./agent-memory.db
```

Signed fields are `tenant-id`, `namespace`, `user-id`, `agent-id`, `workspace-id`,
`session-id`, `actor`, `can-erase`, and `timestamp`. Header names use the
`x-agent-memory-` prefix. `canonical_identity_payload()` defines the signing payload.
The timestamp is checked to limit replay. Never expose the server around the gateway.

For local development only, `--allow-insecure-single-tenant-http` binds HTTP to one
fixed CLI scope. It must not be used for multi-tenant deployment.

## Another provider

```python
from agent_memory_mcp import GatewayHeaderIdentityResolver, create_server
from agent_memory_postgres import build_postgres_kernel

provider = build_postgres_kernel("postgresql://...")
resolver = GatewayHeaderIdentityResolver(b"at-least-32-random-bytes-long")
server = create_server(provider, resolver)
server.run(
    transport="streamable-http",
    host="127.0.0.1",
    port=8000,
    stateless_http=True,
    json_response=True,
)
```

