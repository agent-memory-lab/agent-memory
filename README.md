<h1 align="center">Agent Memory</h1>

<p align="center">
  <strong>Give your agents memory they can trace, update, and use within a budget.</strong>
</p>

<p align="center">
  A lightweight, pluggable memory layer for AI agents.<br>
  Start locally with Python and SQLite. Add models, storage, and framework integrations as you need them.
</p>

<p align="center">
  <a href="pyproject.toml"><img src="https://img.shields.io/badge/Python-3.13%2B-3776AB?logo=python&logoColor=white" alt="Python 3.13+"></a>
  <a href="pyproject.toml"><img src="https://img.shields.io/badge/Core_runtime_dependencies-0-14866D" alt="Zero third-party core runtime dependencies"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue" alt="Apache 2.0 license"></a>
  <a href="docs/design/v6.1.0/task.md"><img src="https://img.shields.io/badge/Status-Alpha-orange" alt="Alpha status"></a>
</p>

<p align="center">
  <a href="README.zh-CN.md">简体中文</a> ·
  <a href="#quickstart">Quickstart</a> ·
  <a href="#examples">Examples</a> ·
  <a href="#integrations">Integrations</a> ·
  <a href="#documentation">Documentation</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

Agents need to remember preferences, follow changing project constraints, and reuse what worked. Agent Memory turns those interactions into **current facts, cited history, and governed procedures**, then returns a bounded `MemoryBundle` for the next model call.

**Run the basic write-and-recall loop without an API key, vector database, or background service.** The core has zero third-party runtime dependencies; model-assisted extraction and other integrations are opt-in.

> **Alpha · software v0.1.0.** The local runtime and optional integration packages are available. The v6.1 architecture is being implemented in stages; it is a design version, not the package version. The full M0/M1 milestones and production acceptance remain open. See [capability status](#capability-status) for the supported scope.

## Why Agent Memory?

A useful memory layer should help an agent answer both **“What applies now?”** and **“What evidence supports it?”**

| What your agent needs | What Agent Memory provides |
| --- | --- |
| Keep up with changing preferences and decisions | Versioned claims and current state; explicit fact admission and correction APIs |
| Explain where a fact came from | Source event IDs and evidence links; richer quote/field checks through Atom admission |
| Distinguish effective time from knowledge time | Claim queries with independent `valid_at` and `known_at` |
| Keep context under control | Limits on items, characters, estimated tokens, and retrieval channels |
| Serve different users and workspaces | Trusted host scopes for tenants, users, agents, workspaces, and sessions |
| Change frameworks or storage | A `MemoryProvider` contract with SQLite, PostgreSQL, SDK, MCP, and LangGraph integrations |
| Learn from task outcomes | Linked feedback and Procedure candidates with evaluation, shadow, canary, and rollback stages |

**A concrete example:** a user moves from Hangzhou to Shanghai. The contribution API can record the move, preserve the earlier state, and track the evidence for ending the old state separately from the evidence for the new city. If the Shanghai evidence is erased, the later answer becomes **unknown** rather than reviving Hangzhou without support. [Run the example →](examples/contribution_memory.py)

## Quickstart

### Install

Requires **Python 3.13+**. Install from this repository into a virtual environment:

```bash
git clone https://github.com/agent-memory-lab/agent-memory.git
cd agent-memory
python3.13 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
python -m pip install -e .
```

For a development environment, use `./setup.sh`; use `./setup.sh --all` to include the optional packages. Select an interpreter with `PYTHON_BIN=/path/to/python3.13 ./setup.sh`.

### Remember → recall → inspect the source

Save this as `demo.py`, then run `python demo.py`:

```python
import asyncio
from agent_memory import AgentMemory, MemoryScope


async def main():
    # Your application supplies identity and scope.
    scope = MemoryScope(tenant_id="demo", user_id="alice", agent_id="assistant")
    async with AgentMemory.local("memory.sqlite3", scope=scope) as memory:
        await memory.remember(
            "I prefer concise answers.",
            event_type="user.message",
            actor="user",
            idempotency_key="alice-answer-style-1",
            claims=({
                "key": "answer.style",
                "value": "concise",
                "text": "Alice prefers concise answers.",
                "scope": "user",
                "confidence": 0.98,
            },),
        )
        bundle = await memory.recall("How should I answer Alice?", token_budget=600)
        for claim in bundle.current_state:
            print(f"{claim.key}: {claim.value}")
            print("source events:", len(claim.provenance.source_event_ids))


asyncio.run(main())
```

Output on a fresh database:

```text
answer.style: concise
source events: 1
```

This example supplies the structured claim from host code. It demonstrates persistence and recall; automatic extraction is a separate, optional path. `confidence` is an input score, not a guarantee of truth. The returned bundle contains memory context for your host to assemble into a model request; it does not call an LLM.

## Examples

Choose the behavior you want to try. These local examples run without a model key; the durable examples also require `python -m pip install -e packages/python-sdk`.

| Example | What you can inspect |
| --- | --- |
| [Basic memory](examples/quickstart.py) | Write a preference and retrieve current state |
| [Durable memory](examples/durable_memory.py) | Persist sources and processing requests, then publish L1 facts |
| [Contextual memory](examples/contextual_memory.py) | Query conditional facts with field and temporal support |
| [Contribution correction](examples/contribution_memory.py) | Separate state termination, correction, and source erasure |
| [Offline deletion sync](examples/durable_purge.py) | Clean a participating SDK outbox before delivering new events |
| [Plugin contracts](examples/plugin_contract.py) | Implement and validate a plugin lifecycle |

For lifecycle hooks, [the capture integration recipe](examples/capture_harness.py) shows how to wire a host provider, authenticated context, and queue. It requires the Python SDK and host setup.

Run a standalone example from the repository root, for instance:

```bash
python examples/contribution_memory.py
```

## How it works

```text
Host events → source evidence → claims / episodes / procedures
                                      ↓
                       current state + historical recall
                                      ↓
                       filtering + fusion + context budgets
                                      ↓
                                 MemoryBundle
                                      ↓
                              your next model call
```

- **Events** preserve source activity; writes support idempotency. Retention and erasure are explicit operations.
- **Claims** represent versioned statements. Atom admission adds typed predicates, source authority, pending/contested decisions, and evidence checks.
- **Episodes and Procedures** organize task history and reusable behavior. Procedure evolution is disabled by default and requires host-defined gates.
- **MemoryBundle** is the retrieval result with citations and budget accounting. Current accepted state has priority; optional candidate channels remain subject to scope and provenance checks.

<p align="center">
  <img src="docs/assets/agent-memory-architecture.svg" alt="Agent Memory architecture: evidence, state, retrieval, and optional integrations" width="100%">
</p>

Models can generate candidates through injectable protocols such as `ClaimGenerator`, `AtomGenerator`, and `AtomReviewer`. The host controls identity, policies, and admission. See [automatic Atom extraction](docs/ATOM_EXTRACTION.md), [Atom admission](docs/ATOM_ADMISSION.md), and [bitemporal memory](docs/BITEMPORAL_MEMORY.md) for usage and supported boundaries.

## Integrations

Start with the core and install the integration packages you need:

| Package | Purpose | Local installation |
| --- | --- | --- |
| `agent-memory` | Domain contracts, SQLite runtime, retrieval, plugin loading | `python -m pip install -e .` |
| [Python SDK](packages/python-sdk/README.md) | Embedded/remote facade and durable host outbox | `python -m pip install -e packages/python-sdk` |
| [MCP server](packages/mcp-server/README.md) | stdio and Streamable HTTP transport | `python -m pip install -e packages/mcp-server` |
| [LangGraph](packages/langgraph/README.md) | Lifecycle adapter | `python -m pip install -e packages/langgraph` |
| [PostgreSQL](packages/postgres/README.md) | PostgreSQL provider and optional vector support | `python -m pip install -e packages/postgres` |
| [Evolution](packages/evolution/README.md) | Evaluated Procedure candidates and promotion | `python -m pip install -e packages/evolution` |

For a local MCP host, install the MCP package above and start a scope-bound stdio server:

```bash
agent-memory-mcp --transport stdio --database memory.sqlite3 \
  --tenant-id demo --user-id alice --agent-id assistant --session-id session-1
```

For remote HTTP, use the [authenticated gateway contract](packages/mcp-server/README.md#streamable-http). Identity comes from trusted host configuration or verified authentication.

The public boundary is `MemoryProvider`. Optional packages use lazy discovery; importing the core does not load database drivers, framework runtimes, or ML libraries. Plugin Protocol v1 adds manifests, capability negotiation, lifecycle health, resource limits, and stable errors. See the [plugin example](examples/plugin_manifest.py) and [architecture guide](docs/ARCHITECTURE.md).

## Where it fits

- **Personal assistants:** keep user preferences and inspect the inputs that established them.
- **Coding and research agents:** carry project constraints and decisions into later calls with a context budget.
- **Support agents:** link interaction history, decisions, and outcomes within a host-defined user scope.
- **Agent infrastructure:** build storage or framework adapters against a shared contract, then evaluate memory policies with fixed replay.

The host owns task execution, caller identity, and the meaning of outcomes. Agent Memory supplies the evidence, state, and retrieval layer.

## Capability status

| Area | Current scope |
| --- | --- |
| Local memory | Implemented: SQLite, explicit claims, current state, citations, budgets, and scoped forgetting |
| Integrations | Python SDK, MCP, LangGraph, PostgreSQL, and Evolution packages; selected live PostgreSQL contracts validated |
| Fact admission and extraction | Implemented for documented predicates/grammars; automatic real-world truth verification is outside the reference adapters |
| Claim history | SQLite/PostgreSQL `valid_at` / `known_at`; supported corrections and current erasure guards |
| Durable processing | Validated slices: atomic receive/publish, producer recovery, application SIGKILL recovery, and opt-in SDK/MCP purge sync |
| Conditional and contribution semantics | Validated subsets: same-scope scalar facts/preferences and same-slot ordinary contribution operations; complex combinations remain restricted |
| Retrieval extensions | Bounded lexical/hybrid candidate plugins are opt-in; advanced orchestration and candidate-to-bundle work remain on the implementation plan |
| Observation and L2/L3 | The complete dependency-aware lifecycle, refresh, and historical safety design remains planned; existing summary/block primitives do not establish that contract |
| External model governance | Full v6.1 dispatch, permission, budget, and delivery contracts remain incomplete |

Validation is documented with scope and limitations in [contribution operations](docs/design/v6.1.0/stage-03.md), [recovery and deletion sync](docs/design/v6.1.0/stage-04.md), and the [resource baseline](docs/RESOURCE_BASELINE.md). These are engineering checks, not a claim of superior benchmark accuracy or production certification.

The alpha does not yet provide full production acceptance, cross-scope atomic correction, end-to-end erasure across external backups/caches, or autonomous online training. Participating outboxes support deletion sync; complete backup restoration with authoritative deletion-log replay remains open. Read the [security policy](SECURITY.md) and [threat model](docs/THREAT_MODEL.md) before remote deployment.

## Roadmap

Implementation follows [the v6.1 plan](docs/design/v6.1.0/plan.md) and [task ledger](docs/design/v6.1.0/task.md). Software, architecture, protocol, and database versions evolve independently.

| Milestone | Goal |
| --- | --- |
| M0 · Implementation baseline | Stable contracts, trusted domain gold, fault fixtures, and calibrated acceptance profiles |
| M1 · Reliable L1 | Host capture → durable processing → fact retrieval, correction, recovery, and erasure |
| M2 · Maintainable knowledge | Dependency-aware Observation and refreshable L2/L3 views |
| M3 · Evaluated retrieval | Evidence tracing, purpose-aware retrieval, frozen comparisons, and optional read-only Reflect |
| M4 · On-demand extensions | Scale, multimodal inputs, portable transfer, and governed evolution where needed |

**The immediate focus is completing reliable L1 delivery and recovery contracts, alongside real-domain quality calibration.** See [next steps](docs/design/v6.1.0/next-steps.md) for current priorities and remaining slices. Optional extensions have separate acceptance requirements.

## Documentation

| Start here | Read |
| --- | --- |
| Understand module boundaries | [Code architecture](docs/ARCHITECTURE.md) |
| Admit and extract facts | [Atom admission](docs/ATOM_ADMISSION.md) · [Automatic extraction](docs/ATOM_EXTRACTION.md) |
| Query historical facts | [Bitemporal memory](docs/BITEMPORAL_MEMORY.md) |
| Record task feedback | [Feedback contract](docs/FEEDBACK_CONTRACT.md) |
| Configure and recover a deployment | [Single-host deployment](docs/single-host-deployment.md) · [Recovery operations](docs/recovery-operations.md) |
| Inspect tests and resource measurements | [Evaluation methodology](docs/LOCAL_MEMORY_COMPARISON_EVAL.md) · [Resource baseline](docs/RESOURCE_BASELINE.md) |
| Follow development | [Design versions](docs/design/README.md) · [Plan](docs/design/v6.1.0/plan.md) · [Tasks](docs/design/v6.1.0/task.md) |

## Contributing

Contributions are welcome in adapters, evidence-focused examples, fault recovery, and evaluation datasets. Check the [task ledger](docs/design/v6.1.0/task.md) for dependencies and open work, then read [CONTRIBUTING.md](CONTRIBUTING.md).

```bash
./setup.sh --all
source .venv/bin/activate
python -m pytest -q
```

Integration and live PostgreSQL tests have additional setup; see [CI](.github/workflows/ci.yml). Preserve the project's core properties: a small default footprint, framework-neutral contracts, host-owned scope, and traceable evidence.

Found a reproducible bug or an integration gap? [Open an issue](https://github.com/agent-memory-lab/agent-memory/issues). Report security vulnerabilities through the private process in [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE).
