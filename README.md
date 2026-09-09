# Agent Memory

[English](README.md) | [简体中文](README.zh-CN.md)

<p align="center">
  <img src="docs/assets/agent-memory-architecture.svg" alt="Agent Memory architecture" width="100%">
</p>

A lightweight, pluggable, evidence-backed memory layer for AI agents.

Agent Memory turns raw agent activity into bounded, traceable context for the next decision. It separates current facts, historical episodes, reusable procedures, and experimental self-evolution so that an agent can remember without turning its prompt, process, or storage into an unbounded black box.

> Status: **v0.1 MVP**. The local SQLite path, plugin contract, MCP adapter, Python SDK, LangGraph adapter, and controlled evolution primitives are implemented. The project is not yet recommended for production use.

## Why Agent Memory

Most agent memory implementations begin and end with vector search:

~~~text
conversation -> chunks -> embeddings -> top-k context
~~~

That is useful for semantic recall, but it does not answer several operational questions:

- What is the agent's current source of truth?
- Which old fact was superseded by a newer fact?
- Where did a memory come from?
- How much memory may enter the model context?
- Can a host application isolate users, agents, and sessions?
- Can a learned behavior be evaluated and rolled back before activation?

Agent Memory uses a state-first model instead:

~~~text
agent events
    |
    v
immutable evidence log
    |
    +--> claims ----------> current state
    +--> episodes --------> historical recall
    +--> procedures ------> reusable behavior
    |
    v
bounded retrieval bundle
    |
    v
next model decision
~~~

Every returned item keeps provenance. New truth supersedes old truth instead of silently overwriting it. Experimental procedures move through explicit evaluation gates instead of modifying live behavior directly.

## Design Principles

| Principle | Meaning |
| --- | --- |
| Pluggable by contract | Agents depend on **MemoryProvider**, not on SQLite, PostgreSQL, MCP, or a framework adapter. |
| Local by default | The MVP runs in-process with Python and SQLite; the core has no runtime dependencies. |
| Evidence before inference | Claims and summaries retain links to source events. |
| State before similarity | Current accepted state is retrieved before semantic or episodic material. |
| Bounded context | Item count, character count, token estimate, and per-channel limits are enforced. |
| Host-owned isolation | Scope is supplied by trusted host code, not accepted from model-generated arguments. |
| Safe evolution | Candidate behaviors are evaluated offline, then in shadow and canary stages, before activation. |
| Honest degradation | Optional channels may fail without corrupting the current-state path. |

## Five-Minute Start

### 1. Install the local MVP

~~~bash
git clone https://github.com/your-org/agent-memory.git
cd agent-memory
./setup.sh
~~~

For an editable core-only install:

~~~bash
python -m pip install -e .
~~~

### 2. Remember and recall

~~~python
import asyncio

from agent_memory import AgentMemory, MemoryScope


async def main() -> None:
    scope = MemoryScope(
        tenant_id="demo",
        namespace="support-agent",
        agent_id="assistant",
        user_id="user-42",
        session_id="session-1",
    )

    async with AgentMemory.local(
        ".agent-memory/demo.sqlite3",
        scope=scope,
    ) as memory:
        await memory.remember(
            "Please contact me by email.",
            event_type="user.message",
            claims=(
                {
                    "key": "contact.preference",
                    "value": "email",
                    "text": "The user prefers email.",
                    "scope": "user",
                    "confidence": 0.98,
                },
            ),
        )

        bundle = await memory.recall(
            "How should I contact this user?",
            limit=8,
            token_budget=1_200,
        )

        for claim in bundle.current_state:
            print(claim.key, claim.value, claim.provenance.source_event_ids)


asyncio.run(main())
~~~

**AgentMemory.local()** creates a ready-to-use runtime with conservative limits. No vector database, external service, model key, or background worker is required.

### Automatic trajectory extraction

Automatic extraction is optional and model-vendor neutral. Implement the small
`ClaimGenerator` protocol, then inject it without changing the agent or storage layer:

~~~python
from agent_memory import AgentMemory, build_trajectory_extractor

extractor = build_trajectory_extractor(
    my_claim_generator,
    provider="my-model-provider",
    model="my-model",
)
memory = AgentMemory.local("memory.sqlite3", scope=scope, extractor=extractor)
~~~

The generator receives trusted lifecycle events, including user messages and tool metadata.
Its structured output is schema-validated, confidence-gated, scope-checked, capped per event,
and bound to source evidence. Explicit host claims take priority. Generator failures degrade
to event-only ingestion and do not block the agent.

## Core Model

Agent Memory treats memory as several related but distinct artifacts.

| Artifact | Purpose | Mutation rule |
| --- | --- | --- |
| Event | Immutable evidence from the agent lifecycle | Append-only and idempotent |
| Claim | Structured statement derived from evidence | Versioned; may supersede an older claim |
| Current state | Latest accepted claims for a scope | Rebuilt from active claims |
| Episode | Compressed account of a completed interaction | Append-only with citations |
| Procedure | Reusable behavioral knowledge | Versioned and governed |
| Decision | Record of what an agent chose | Linked to context and policy version |
| Outcome | Observable result of a decision | Linked to the decision |
| Reward | Evaluation signal for an outcome | Stored separately from activation |

A memory bundle is a bounded projection over these artifacts. It is not a database dump and it is not an unqualified list of semantically similar text.

## Retrieval

The default retrieval pipeline is deterministic and inspectable:

~~~text
query + trusted scope
    |
    +--> current-state channel
    +--> semantic channel
    +--> episodic channel
    +--> procedural channel
    |
    v
reciprocal-rank fusion
    |
    v
deduplication + policy filtering
    |
    v
hard budget enforcement
    |
    v
MemoryBundle(items, citations, token_estimate)
~~~

The current-state channel has priority because recent accepted truth should not lose to an older but more similar passage. Reciprocal-rank fusion combines channel rankings without requiring their raw scores to share a scale.

The local MVP enforces:

- maximum returned items;
- maximum characters;
- estimated token budget;
- per-channel limits;
- tenant, application, agent, user, and session scope;
- deterministic ordering;
- citations back to source evidence.

## Plugin Architecture

<p align="center">
  <img src="docs/assets/plugin-integration-flow.svg" alt="Plugin integration flow" width="100%">
</p>

The public boundary is the **MemoryProvider** protocol. Storage engines, transports, framework integrations, and evolution engines remain replaceable.

~~~text
Agent / Host Application
          |
          v
 AgentMemory facade
          |
          v
   MemoryProvider
          |
          +--> local SQLite kernel
          +--> PostgreSQL provider
          +--> remote MCP provider
          +--> future third-party provider
~~~

Plugins are discovered lazily through Python entry points. Importing the core does not import optional databases, MCP runtimes, frameworks, or machine-learning libraries.

| Entry-point group | Responsibility |
| --- | --- |
| agent_memory.providers | Storage and retrieval providers |
| agent_memory.adapters | Agent-framework lifecycle adapters |
| agent_memory.embedders | Optional embedding implementations |
| agent_memory.rerankers | Optional reranking implementations |
| agent_memory.evolution | Candidate generation and evaluation engines |

Switching providers does not require changing agent logic:

~~~python
from agent_memory import AgentMemory, MemoryScope

scope = MemoryScope(
    tenant_id="acme",
    namespace="research",
    agent_id="analyst",
)

memory = AgentMemory.from_plugin(
    "postgres",
    scope=scope,
    dsn="postgresql://memory@localhost/agent_memory",
)
~~~

Third-party packages can implement the protocol and register their own entry point without modifying this repository.

## Packages

| Package | Role | Core dependency impact |
| --- | --- | --- |
| agent-memory | Domain model, local kernel, SQLite store, retrieval, policy, plugin registry | Zero runtime dependencies |
| agent-memory-postgres | PostgreSQL and pgvector-capable provider | Optional |
| agent-memory-mcp-server | MCP stdio/HTTP transport | Optional |
| agent-memory-python-sdk | Client-facing Python facade | Optional |
| agent-memory-langgraph | LangGraph lifecycle adapter | Optional |
| agent-memory-evolution | Governed procedure evolution | Optional |

This split keeps the default installation small while allowing deployments to add only the integration surface they need.

## MCP Integration

Install the MCP package:

~~~bash
python -m pip install -e packages/mcp-server
~~~

Start a local stdio server:

~~~bash
agent-memory-mcp --transport stdio --database .agent-memory/mcp.sqlite3
~~~

The MCP surface exposes six operations:

| Tool | Purpose |
| --- | --- |
| memory_remember | Ingest an event and optional structured claims |
| memory_recall | Return a bounded memory bundle |
| memory_get_state | Read current accepted state |
| memory_forget | Remove memory according to scope and policy |
| memory_feedback | Record outcome or evaluation feedback |
| memory_health | Report provider and protocol health |

For stdio integrations, the host supplies scope through trusted configuration. For remote HTTP deployments, use a signed gateway or authenticated reverse proxy and derive scope from verified identity. Do not let a model choose arbitrary tenant or user identifiers.

## Framework Integration

Framework adapters translate lifecycle hooks into the provider protocol; they do not own memory semantics.

| Lifecycle point | Memory action |
| --- | --- |
| Session start | Resolve trusted scope and open provider |
| After user/model/tool event | Append evidence |
| Before model call | Retrieve a bounded bundle |
| After outcome | Record outcome and feedback |
| Session end | Finalize episode and close provider |

The included LangGraph package demonstrates this boundary. The same model supports custom agents, command-line agents, hosted runtimes, and other orchestration frameworks.

## Controlled Self-Evolution

Self-evolution is not unrestricted prompt rewriting. Agent Memory models it as a governed release process for procedures:

~~~text
observations
    -> candidate
    -> offline evaluation
    -> shadow
    -> canary
    -> active
             |
             +-> rollback
~~~

Safety rules in the MVP:

- evolution is disabled by default;
- candidates never become active directly;
- evaluation evidence is persisted;
- policy versions are explicit;
- activation and rollback are auditable;
- deterministic rules remain the fallback;
- latent or learned policies cannot replace the source-of-truth state.

This creates a path from task trajectories to improved behavior without allowing experimental memory to silently control production decisions.

## Capability Status

| Capability | Status | Notes |
| --- | --- | --- |
| Immutable event ingestion | Implemented | Idempotent event keys |
| Structured claims and current state | Implemented | Provenance and supersession |
| Episodic memory | Implemented | Citation-preserving summaries |
| Procedural memory | Implemented | Versioned procedures |
| State-first hybrid retrieval | Implemented | Multi-channel RRF |
| Hard context budgets | Implemented | Items, characters, and token estimate |
| Scoped forgetting | Implemented | Policy-controlled local deletion |
| SQLite provider | Implemented | Default zero-config path |
| Python SDK | Implemented | Optional package |
| LangGraph adapter | Implemented | Optional package |
| MCP stdio transport | Implemented | Protocol smoke-tested |
| PostgreSQL provider | Implemented, not deployment-certified | Requires live environment validation |
| pgvector retrieval | Optional | Provider capability, not a core requirement |
| Controlled procedure evolution | MVP implemented | Disabled by default |
| Graph memory | Planned | Must remain optional |
| Learned latent memory | Research track | Must not become source of truth |
| Online autonomous policy training | Not implemented | Requires a separate safety and evaluation design |

## Resource Profile

The project is deliberately optimized for small local agents and plugin hosts.

A local development measurement for the SQLite MVP with 100 events produced:

| Measurement | Observed value |
| --- | ---: |
| Heap after core import | approximately 1.84 MiB |
| Final heap | approximately 1.95 MiB |
| Peak heap | approximately 2.17 MiB |
| SQLite database | 136 KiB |
| SQLite shared-memory file | 32 KiB |

These values are directional measurements from one development environment, not cross-platform guarantees. Embedding models, remote clients, framework runtimes, and database drivers are excluded from the core process unless their plugins are installed and used.

## Security Model

Memory is a privileged subsystem because it can influence future model behavior.

The design assumes:

- the host application authenticates the caller;
- the host derives the memory scope;
- stored content is untrusted data, not executable instruction;
- credentials are supplied at runtime and never committed;
- providers enforce tenant boundaries;
- deletion and retention policies are explicit;
- retrieved items remain attributable to evidence.

Before deploying remotely, read:

- [Security policy](SECURITY.md)
- [Threat model](docs/THREAT_MODEL.md)
- [MVP architecture](docs/MVP.md)

If you discover a vulnerability, follow the private reporting process in **SECURITY.md**. Do not open a public issue containing exploit details or secrets.

## Development

Create a development environment:

~~~bash
./setup.sh --all
~~~

Run the core test suite:

~~~bash
python -m pytest -q
~~~

Build a package:

~~~bash
python -m build
python -m twine check dist/*
~~~

The repository currently contains focused tests for ingestion, idempotency, supersession, scoped retrieval, context budgets, forgetting, plugin discovery, and evolution gates.

## Repository Layout

~~~text
agent-memory/
├── src/agent_memory/          # zero-dependency core
├── packages/
│   ├── postgres/              # PostgreSQL provider
│   ├── mcp-server/            # MCP transport
│   ├── python-sdk/            # Python client facade
│   ├── langgraph/             # framework adapter
│   └── evolution/             # governed self-evolution
├── tests/                     # core behavior tests
├── examples/                  # runnable examples
├── docs/                      # architecture, plan, and security
└── setup.sh                   # local setup entry point
~~~

## Current Boundaries

The MVP intentionally does not claim to provide:

- a hosted memory service;
- a production-certified multi-region PostgreSQL deployment;
- semantic quality guarantees from a bundled embedding model;
- a universal graph-memory engine;
- end-to-end deletion across external backups and replicas;
- safe online autonomous training without human-defined gates;
- compatibility certification for every agent framework.

These are deployment or research tracks, not hidden behavior in the lightweight core.

## Roadmap

### v0.1: Minimal reliable memory

- local SQLite runtime;
- event, claim, state, episode, and procedure model;
- bounded retrieval with citations;
- plugin protocol and lazy discovery;
- MCP, SDK, LangGraph, PostgreSQL, and evolution packages;
- packaging, security, and release documentation.

### v0.2: Operational hardening

- live PostgreSQL integration suite;
- signed HTTP gateway reference;
- retention and deletion audit reports;
- retrieval quality benchmarks;
- compatibility matrix for supported agent frameworks;
- package publication and reproducible release workflow.

### v0.3: Optional advanced memory

- graph-memory plugin;
- external embedding and reranking plugins;
- conflict-resolution policies;
- richer evaluation datasets;
- canary dashboards and automated rollback signals.

### Research

- trajectory compression;
- learned retrieval policies;
- latent memory representations;
- counterfactual procedure evaluation;
- memory-aware agent planning.

Advanced features must remain optional, measurable, reversible, and subordinate to evidence-backed state.

## Contributing

Contributions should preserve the core constraints: small default footprint, framework neutrality, strict scope isolation, traceable evidence, and optional heavy dependencies.

See [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Architecture changes should include the problem, contract impact, failure behavior, migration path, and resource cost.

## License

Licensed under the [Apache License 2.0](LICENSE).
