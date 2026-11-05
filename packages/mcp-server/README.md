# Agent Memory MCP Server

Official MCP Python SDK v2 transport package for `agent-memory`. It exposes the
stable memory contract over stdio or Streamable HTTP without placing MCP dependencies
in the kernel.

## Tool contract and compatibility

The server publishes the same capability-filtered tool names, input schemas, and
parameter descriptions as the embedded `MCPMemoryTools` contract. Registration
checks that wrappers cover that contract; unknown tool arguments are rejected
rather than silently discarded. Block and feedback arguments are validated against
shared JSON types, enum values, and numeric bounds before any transport coercion.
Strings such as `"false"`, numeric strings, booleans used as numbers, fractional
integer fields, and JSON-encoded array/object strings are rejected without writes;
existing optional null values retain their SDK semantics.
Tenant, user, actor, and legal-erase permissions
come only from the trusted identity resolver.

- Base tools: `memory_ingest`, `memory_retrieve`, `memory_get_state`,
  `memory_propose`, `memory_forget`, and `memory_capabilities`.
- With `memory_blocks`: `memory_block_read`, `memory_block_write`,
  `memory_block_search`, and `memory_block_forget`.
- With both `decision_lineage` and `outcome_feedback`: `memory_record_decision`,
  `memory_record_outcome`, `memory_record_evaluation`, `memory_record_reward`, and
  `memory_feedback_status`. The provider's evaluator allowlist and scope checks
  still apply. Registering these tools does not enable learning or promotion.
- Diagnostics, deletion audit, ontology, capture, recovery, and derived tools
  remain opt-in through their host configuration.

When `bitemporal_claims` is enabled, `memory_retrieve` accepts `valid_at` (real-world
effective time) and `known_at` (system knowledge cutoff) as timezone-aware ISO-8601
strings. Either can be supplied independently; an omitted axis uses the query time.
Explicit temporal queries return historical Claims, without mixing in current
nonhistorical artifacts. Invalid timestamps fail through the shared core validation;
providers without this capability do not advertise the fields and explicitly reject
non-null temporal arguments. See [the temporal contract](../../docs/BITEMPORAL_MEMORY.md).

This repairs transport parity for existing optional capabilities; the protocol remains
`0.1` and the core schema remains v2. The MCP changes themselves require no database
migration or SDK method change. Clients relying on previously ignored, unrecognized tool arguments must
remove those arguments.

The integration contract is verified with the official MCP client and a real stdio
subprocess, including temporal results, all nine block/feedback operations, capability
filtering, validation, and identity/erase/evaluator authorization:

```bash
pytest -q packages/mcp-server/tests/test_tool_contract.py
```

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
export AGENT_MEMORY_GATEWAY_SECRET="$(python -c 'import secrets; print(secrets.token_hex(32))')"
agent-memory-mcp --transport streamable-http --database ./agent-memory.db
```

Generate this secret once, store it securely, and configure the trusted gateway with
exactly the same UTF-8 string (do not hex-decode it). Reuse that shared secret across
server restarts; do not generate separate values independently at the two ends.

Signed fields are `tenant-id`, `namespace`, `user-id`, `agent-id`, `workspace-id`,
`session-id`, `actor`, `can-erase`, and `timestamp`. Header names use the
`x-agent-memory-` prefix. `canonical_identity_payload()` defines the signing payload.
The timestamp is checked to limit replay. Never expose the server around the gateway.

For local development only, `--allow-insecure-single-tenant-http` binds HTTP to one
fixed CLI scope. It must not be used for multi-tenant deployment.

## Another provider

Install `packages/postgres` in addition to this package. Set
`AGENT_MEMORY_POSTGRES_DSN` to your database connection string and supply the same
`AGENT_MEMORY_GATEWAY_SECRET` configured for your trusted gateway above.

`create_server()` registers tools but does not own the supplied provider's lifecycle.
Initialize and close PostgreSQL on the **same event loop** that serves requests.
Do not initialize it with one `asyncio.run()` and then call synchronous `server.run()`,
which creates a separate loop. The `finally` also closes a partially initialized
provider if startup fails.

```python
import asyncio
import os

from agent_memory_mcp import GatewayHeaderIdentityResolver, create_server
from agent_memory_postgres import build_postgres_kernel


async def main():
    resolver = GatewayHeaderIdentityResolver(
        os.environ["AGENT_MEMORY_GATEWAY_SECRET"].encode("utf-8")
    )
    provider = build_postgres_kernel(os.environ["AGENT_MEMORY_POSTGRES_DSN"])
    try:
        await provider.initialize()
        server = create_server(provider, resolver)
        await server.run_streamable_http_async(
            host="127.0.0.1",
            port=8000,
            stateless_http=True,
            json_response=True,
        )
    finally:
        await provider.close()


if __name__ == "__main__":
    asyncio.run(main())
```

Hosts can pass `derived=observation_service` to `create_server()` to expose the
read-only `memory_derived` tool. Its page operations are `page_capabilities`,
`page_read`, `page_context` and `page_status`; page definitions and refresh writes
remain host APIs. The current page capability requires the backend contract
`page-full-rebuild/1`. Historical pages, page-to-page inputs and inferred personas
are disabled. `page_context` rechecks current input permissions before delivery.
