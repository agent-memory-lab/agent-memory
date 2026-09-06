# Agent Memory

`agent-memory` is a standalone, framework-neutral memory plugin for AI agents. It is
designed to be published as its own GitHub repository and does not depend on an agent
framework, web framework, vector database, graph database, Redis, or external LLM.

## Phase 1 status

Implemented:

- Stable protocol and schema versions
- Immutable events, claims, state deltas, episodes, procedures, provenance, decisions,
  outcomes, and reward signals
- Atomic Unit of Work for event ingestion, idempotency, claim updates, and supersession
- Scope isolation across tenant, namespace, user, agent, workspace, and session
- State-first retrieval with semantic, episodic, and procedural channels
- SQLite provider with no external service dependency
- Archive and legal erase as separate operations
- Provider manifest and capability negotiation
- Extension ports for extraction, embedding, reranking, graph, and controlled evolution
- Evidence-backed `MemoryProposal` updates with optimistic version checks
- Transport-neutral MCP tool contracts with trusted request-derived scope
- Generic `before_model`, `after_tool`, `after_model`, and `session_end` lifecycle hooks
- Decision, outcome, and reward lineage for later offline evolution
- Official MCP v2 stdio and Streamable HTTP server package
- Embedded/stdio/HTTP Python client SDK with one operation surface
- LangGraph node adapter with trusted config-derived scope
- Controlled Procedure evolution with offline, shadow, canary, approval, and rollback gates
- Zero-config bounded runtime and lazy Python entry-point plugin discovery

Deliberately disabled in the trusted default kernel:

- Learned write policies
- Latent memory
- Policy/RL joint training
- Test-time learning
- Automatic promotion of generated procedures or policies

These features must remain optional providers and pass offline evaluation, shadow,
canary, promotion, and rollback gates before production activation.

## Quick start

```python
import asyncio

from agent_memory import MemoryEvent, MemoryQuery, MemoryScope, build_local_kernel


async def main() -> None:
    memory = build_local_kernel("./agent-memory.db")
    await memory.initialize()

    scope = MemoryScope(
        tenant_id="acme",
        user_id="user-42",
        agent_id="research-agent",
        workspace_id="project-a",
        session_id="session-7",
    )

    await memory.ingest_event(
        MemoryEvent(
            scope=scope,
            event_type="user.preference.updated",
            content="Use email for project notifications.",
            idempotency_key="message-1042",
            metadata={
                "claims": [
                    {
                        "key": "contact.preference",
                        "value": "email",
                        "text": "The user prefers email for project notifications.",
                        "scope": "user",
                        "confidence": 0.98,
                    }
                ]
            },
        )
    )

    bundle = await memory.retrieve(
        MemoryQuery(scope=scope, text="How should I notify the user?")
    )
    print(bundle.current_state)


asyncio.run(main())
```

## Stable plugin boundary

Agents depend on `MemoryProvider`, not on SQLite or any future infrastructure:

```text
Agent / MCP / SDK / Framework Adapter
                |
        Stable MemoryProvider
                |
            MemoryKernel
                |
 Storage / Extraction / Retrieval / Evolution providers
```

The local composition root is `build_local_kernel`. Production packages can provide a
different repository, extractor, embedder, reranker, graph provider, or evolution
provider without changing the domain kernel.

## MCP integration

`MCPMemoryTools` exposes the v3 tool surface without binding the kernel to an MCP SDK:

- `memory_ingest`
- `memory_retrieve`
- `memory_get_state`
- `memory_propose`
- `memory_forget`
- `memory_capabilities`

The host constructs `MCPRequestContext` from authenticated identity. Tenant and scope are
never accepted from model-generated tool arguments. Legal erase additionally requires
`can_erase=True` in the trusted context. `packages/mcp-server` registers the contract with
the official MCP Python SDK v2. It supports process-isolated stdio and stateless
Streamable HTTP. HTTP identity is accepted only through a signed trusted-gateway resolver
by default; unsigned request headers are never treated as identity.

`packages/python-sdk` provides the same operations through embedded, stdio, or
Streamable HTTP clients. `packages/langgraph` maps trusted LangGraph run configuration
and graph lifecycle nodes to `AgentMemoryAdapter`.

## Agent lifecycle integration

`AgentMemoryAdapter` maps framework lifecycle events to the stable provider contract:

```text
before_model -> retrieve MemoryBundle
after_tool   -> append tool outcome event and optional claims
after_model  -> append model event and optional claims
session_end  -> create candidate Episode with provenance
```

The adapter also records which memories and procedure versions contributed to a decision,
then links environment outcomes and versioned reward calculations to that decision.

## Production PostgreSQL provider

The optional package in `packages/postgres` implements the same repository and Unit of
Work contracts with PostgreSQL. It adds generated full-text indexes, a pgvector component,
and a leased consolidation queue. Installing or importing the core package does not install
`psycopg` and does not require PostgreSQL.

```python
from agent_memory_postgres import build_postgres_kernel

memory = build_postgres_kernel("postgresql://memory@localhost:5432/agent_memory")
await memory.initialize()
```

The production write path is:

```text
transaction: Event + Claim/StateDelta/Proposal
commit
idempotent consolidation job enqueue
worker: lease -> process -> complete | retry -> dead-letter
```

Vector indexing is derivative data. `PgVectorIndex` can be disabled, rebuilt, or replaced
without changing source events, current state, or the stable `MemoryProvider` contract.

## Memory-driven self-evolution boundary

Phase 1 records the evidence required for later evolution:

```text
DecisionRecord -> OutcomeEvent -> RewardSignal -> Episode/Procedure candidates
```

It does not train or promote policies. A later `EvolutionProvider` may generate immutable
candidate artifacts, but activation must be performed by a separate evaluator and
artifact registry using versioned shadow/canary/promotion/rollback records.

## Package layout

```text
src/agent_memory/domain.py       Versioned domain contracts
src/agent_memory/ports.py        Inbound and outbound plugin ports
src/agent_memory/kernel.py       Framework-neutral application kernel
src/agent_memory/providers.py    Deterministic trusted defaults
src/agent_memory/sqlite.py       Local SQLite provider and Unit of Work
src/agent_memory/composition.py  Explicit dependency wiring
packages/mcp-server              Official MCP v2 transport and identity boundary
packages/python-sdk              Embedded, stdio, and HTTP client SDK
packages/langgraph               LangGraph node lifecycle adapter
packages/postgres                PostgreSQL, pgvector, and background jobs
packages/evolution               Candidate evaluation, promotion, activation, and rollback
```

## Controlled self-evolution

`packages/evolution` implements the v3 L2 evolution control plane. High-quality Episodes
may generate immutable Procedure candidates, but candidates cannot enter retrieval as
active procedures until separate offline, shadow, and canary reports pass deterministic
quality and safety gates. Active promotion requires a human approval reference.

Activation uses a two-phase `canary -> activating -> active` transition. Failed deployment
returns to canary. Rollback archives the deployed Procedure through `MemoryProvider` and
records every transition. Learned policies, latent representations, and online training
remain disabled experimental capabilities.

## Lightweight zero-config use

The default path has no third-party dependency, external service, resident vector index,
or process-wide cache. Optional providers and integrations are discovered through Python
entry points and imported only when selected.

```python
from agent_memory import AgentMemory

async with AgentMemory.local() as memory:
    await memory.remember("The user prefers concise answers.")
    bundle = await memory.recall("How should I answer?")
```

`MemoryLimits` places hard bounds on event characters, metadata bytes, claims per event,
retrieved items, state claims, and context tokens. Defaults are intentionally small and
can be overridden per Agent without changing a Provider.
