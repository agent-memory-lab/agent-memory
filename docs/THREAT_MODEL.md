# Threat Model

## Scope

This model covers the core package, SQLite provider, plugin discovery, MCP transport,
Python SDK, LangGraph adapter, PostgreSQL provider, and controlled evolution package.

## Assets

- User and tenant memory content.
- Current State accuracy and supersession history.
- Scope identifiers and authorization decisions.
- Procedure and policy activation state.
- Deletion intent and audit evidence.
- Provider credentials and gateway signing secrets.

## Trust boundaries

```text
Model output (untrusted)
        |
Agent host / authenticated gateway (trusted identity boundary)
        |
MemoryProvider contract (validation and scope enforcement)
        |
Storage and optional derivative indexes

Evaluation input (untrusted until verified)
        |
Evolution gates + human approval
        |
Active Procedure deployment
```

## Primary threats and controls

| Threat | Control |
|---|---|
| Cross-tenant retrieval | Scope partitioning before search and trusted identity-derived scope |
| Model changes its own scope | Scope is absent from MCP tool arguments |
| Forged HTTP identity | HMAC-signed gateway headers with timestamp validation |
| Prompt injection becomes policy | Events are untrusted; Procedures enter as candidates only |
| Duplicate or replayed writes | Scope-bound idempotency keys and immutable events |
| Stale state overwrites truth | Optimistic expected-version checks and supersession |
| Unauthorized legal erase | Separate `can_erase` authorization in request context |
| Unbounded context growth | Hard item, state, metadata, event, and token limits |
| Dependency expansion | Zero-dependency core and lazy optional entry points |
| Unsafe self-evolution | Offline, shadow, canary, approval, two-phase activation, rollback |
| Failed activation leaves active state | `activating` intermediate state returns to canary on failure |
| Failed rollback hides deployment | `rolling_back` returns to active when deactivation fails |

## Deployment requirements

- Prefer stdio for local desktop Agent hosts.
- Bind Streamable HTTP to a private interface behind an authenticating gateway.
- The gateway must remove caller-provided `x-agent-memory-*` headers before signing.
- Store the gateway HMAC key in a secret manager and rotate it operationally.
- Use TLS for all remote traffic and database connections.
- Use separate database roles and physical environments for tests and production.
- Keep learned policy, graph, latent, and online-training capabilities disabled unless the
  deployment has independent evaluation and rollback controls.

## Known MVP limitations

- Complete erasure of every external derivative index requires provider-specific tests.
- Streamable HTTP gateway integration is implemented but not part of the local MVP test.
- PostgreSQL behavior requires a real database integration environment.
- The SQLite evolution registry is for one controller, not a distributed control plane.
- No built-in secret rotation, KMS integration, legal hold, or signed artifact registry.

