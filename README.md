<h1 align="center">Agent Memory</h1>

<p align="center">
  <strong>Remember what changed. Keep the evidence.</strong>
</p>

<p align="center">
  Traceable, correctable memory for AI agents.<br>
  Start with Python + SQLite. Add PostgreSQL, MCP, and LangGraph when you need them.
</p>

<p align="center">
  <a href="https://github.com/agent-memory-lab/agent-memory/actions/workflows/ci.yml"><img src="https://github.com/agent-memory-lab/agent-memory/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="pyproject.toml"><img src="https://img.shields.io/badge/Python-3.13%2B-3776AB?logo=python&amp;logoColor=white" alt="Python 3.13+"></a>
  <a href="pyproject.toml"><img src="https://img.shields.io/badge/Core_dependencies-0-1F5B45" alt="Zero third-party core runtime dependencies"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue" alt="Apache 2.0 license"></a>
  <a href="#capability-status-and-roadmap"><img src="https://img.shields.io/badge/Status-Alpha-D07839" alt="Alpha status"></a>
</p>

<p align="center">
  <a href="README.zh-CN.md">简体中文</a> ·
  <a href="#quickstart">Quickstart</a> ·
  <a href="#from-source-evidence-to-usable-knowledge">Architecture</a> ·
  <a href="#examples">Examples</a> ·
  <a href="#integrations">Integrations</a> ·
  <a href="#documentation">Docs</a>
</p>

Agent Memory gives your agent **current facts, evidence, and a history of what changed**. It records where a statement came from, when it applies, and whether it can be used in the current scope. Your host receives a bounded `MemoryBundle` for its next model call.

**The basic memory loop runs locally with zero third-party runtime dependencies—no model key, vector database, or background service required.** Extraction, integrations, and workers are opt-in.

> **Alpha · v0.1.0.** Try the local examples below. Supported slices and remaining acceptance work are documented in [capability status](#capability-status-and-roadmap).

## Memory changes. The evidence stays connected.

| A real memory problem | What your agent can do |
| --- | --- |
| **“I moved from Hangzhou to Shanghai.”** | Record the change and its effective time. Keep the earlier history. If Shanghai's evidence is erased, return **unknown** after the move while preserving the independent evidence that Hangzhou ended. [Run it →](examples/contribution_memory.py) |
| **“Use Chinese for Project A, except on holidays.”** | Preserve the project condition and the exception. Return a qualified language preference when applicable, or `context_unknown` when the trusted context is missing. [Run it →](examples/derived_contextual_observation.py) |
| **“Forget this source—even after restoring a backup.”** | Replay an authoritative deletion log over an isolated old database copy. Keep unrelated sources while preventing erased content from returning. [Run it →](examples/purge_restore.py) |

These examples use explicit host policies and deterministic adapters. They demonstrate inspectable behavior without a model API key. Source identity, time, and permission remain attached as memory is corrected, combined, retrieved, or erased.

## Quickstart

### Install

Requires **Python 3.13+**. Install from this repository into a virtual environment. On macOS or Linux:

```bash
git clone https://github.com/agent-memory-lab/agent-memory.git
cd agent-memory
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

<details>
<summary>Windows PowerShell</summary>

```powershell
git clone https://github.com/agent-memory-lab/agent-memory.git
cd agent-memory
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\python.exe demo.py
```

Create `demo.py` from the example below before running the last command. This uses the virtual environment's interpreter directly; activation is optional.

</details>

On macOS or Linux, `./setup.sh` creates a development environment; `./setup.sh --all` also installs the integration packages. Local inference is installed separately with `python -m pip install -e 'packages/local-models[inference]'`. Select an interpreter with `PYTHON_BIN=/path/to/python3.13 ./setup.sh`.

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
            print("source event:", claim.provenance.source_event_ids[0])


asyncio.run(main())
```

Output on a fresh database:

```text
answer.style: concise
source event: <event-id>
```

The event ID is generated at runtime. Rerunning the same input with the same idempotency key returns the same source event; changing that input requires a new key.

This example supplies the structured claim from host code. It demonstrates persistence and recall; automatic extraction is a separate, optional path. `confidence` is an input score, not a guarantee of truth. The returned bundle contains memory context for your host to assemble into a model request; it does not call an LLM.

## From source evidence to usable knowledge

<p align="center">
  <img src="docs/assets/agent-memory-overview.svg" alt="Agent Memory: trusted host events, L0 sources and verified L1 facts feed a bounded MemoryBundle, registered L2 scenarios and reviewed L3 Persona hypotheses. Evidence, time, current permissions and erasure govern every path." width="100%">
</p>

The [v7.2 architecture and data flow](docs/design/AGENT_MEMORY_DESIGN_V7.2.0.md) separate the source, the accepted interpretation, and the views built from it:

| Component | Responsibility | Available scope |
| --- | --- | --- |
| **L0 · Source evidence** | Preserve captured content, source identity, revisions, and processing requests | Durable receive, deduplication, explicit source revisions, and replay |
| **L1 · Atomic memory** | Decide what can be used, with evidence, conditions, and time | Typed admission; accepted, pending, or contested outcomes; supported corrections and dual-time queries |
| **Observation · Derived views** | Organize a facet across sources and rebuild when its inputs change | Same-scope language templates, bounded historical reads, current non-conditional parent views, and versioned host permissions |
| **L2 · Scenario** | Organize versioned scenario pages and blocks | Language pages plus automatically maintained registered project scenes and dual-time reconstruction within registered coverage |
| **L3 · Core / Persona** | Organize explicit long-term preferences and carefully evaluated patterns | Separate declarations and hypotheses; complete evidence, independent families, observation spans and reviewed counterexamples drive refresh, withdrawal and immutable history |

Observation is a derived building block that can support L2 or L3. L1 also feeds retrieval directly. A summary's position in this structure never increases the authority of its evidence.

- **Two clocks:** `valid_at` asks when a fact applies; `known_at` asks what the system knew at that point.
- **Reliable work:** sources, tasks, publication receipts, and index coverage have explicit transaction boundaries and recovery behavior.
- **Controlled delivery:** the host supplies identity and permissions; retrieval and derived reads enforce their supported scope and current erasure guards.
- **Bounded context:** `MemoryBundle` carries relevant memory and citations within item, character, and token estimates configured by the host.

See [Atom admission](docs/ATOM_ADMISSION.md), [bitemporal memory](docs/BITEMPORAL_MEMORY.md), and [code architecture](docs/ARCHITECTURE.md) for contracts and module boundaries.

## Examples

Run these from the repository root. **Core** means `python -m pip install -e .`; **+ SDK** adds `python -m pip install -e packages/python-sdk`. All examples below run locally without a model key.

| Try this | What to inspect | Install |
| --- | --- | --- |
| [Basic memory](examples/quickstart.py) | A saved preference, current state, and source citations | Core |
| [Contribution correction](examples/contribution_memory.py) | A move, independent termination evidence, and safe erasure | Core |
| [Conditional language](examples/derived_contextual_observation.py) | Project conditions, exceptions, and qualified Observation output | + SDK |
| [Host-controlled views](examples/derived_controls.py) | Versioned query definitions, expiring host permissions, and read-only delivery | + SDK |
| [Batched publication](examples/publication_batches.py) | Partial publication, closure, and complete index coverage | + SDK |
| [Backup deletion replay](examples/purge_restore.py) | Recover an old backup without restoring erased evidence | Core |

```bash
python examples/contribution_memory.py
```

```text
October 2: ['Hangzhou']
October 6: unknown
```

<details>
<summary>More examples: capture, recovery, indexes, and plugins</summary>

| Example | What to inspect | Install |
| --- | --- | --- |
| [Durable memory](examples/durable_memory.py) | Persist sources and processing requests, then publish L1 | + SDK |
| [Contextual facts](examples/contextual_memory.py) | Field evidence, conditions, and supported time ranges | Core |
| [Derived parent views](examples/derived_parent_views.py) | Fixed current parent versions, transitive access and revocation | + SDK |
| [Versioned L2 page](examples/derived_scenario_page.py) | Full rebuild, stable block identities and guarded current page readiness | + SDK |
| [Current Observation](examples/derived_observation.py) | Build and read a current language facet | + SDK |
| [Offline deletion sync](examples/durable_purge.py) | Clean a participating SDK outbox before new delivery | + SDK |
| [L1 readiness](examples/durable_readiness.py) | Wait for a fixed set of requests; cancel a deleted offline sequence | + SDK |
| [Reprocessing readiness](examples/reprocessing_readiness.py) | Track an explicit interpretation replacement | + SDK |
| [Local index readiness](examples/index_readiness.py) | Separate publication from candidate locator visibility | + SDK |
| [Index repair and rollover](examples/index_recovery.py) | Repair a gap and switch to a rebuilt index stream | + SDK |
| [Resource refresh](examples/resource_refresh.py) | Coalesce work while preserving fixed completion targets | Core |
| [Plugin contracts](examples/plugin_contract.py) | Implement and validate a plugin lifecycle | Core |

The [capture integration recipe](examples/capture_harness.py) shows how to connect lifecycle hooks, a host provider, authenticated context, and a queue. It requires the Python SDK and host setup.

</details>

## Integrations

Start with the core and install the integration packages you need:

| Package | Purpose | Local installation |
| --- | --- | --- |
| `agent-memory` | Domain contracts, SQLite runtime, retrieval, plugin loading | `python -m pip install -e .` |
| [Python SDK · `agent-memory-sdk`](packages/python-sdk/README.md) | Embedded/remote facade and durable host outbox | `python -m pip install -e packages/python-sdk` |
| [MCP server · `agent-memory-mcp`](packages/mcp-server/README.md) | stdio and Streamable HTTP transport | `python -m pip install -e packages/mcp-server` |
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

## Capability status and roadmap

The current documented delivery baseline is **[v7.2: raw project lifecycle and derived evolution](docs/design/v7.2.0/validation.md)**, building on [V7-B6](docs/design/v7.0.0/batch-b6.md) and the [v7.1 runtime](docs/design/v7.1.0/validation.md). Its targeted engineering validation is separate from real business quality and cost acceptance. The software package is **v0.1.0 / Alpha**; architecture, protocol, and package versions are tracked separately.

| Capability | Current implementation | Evidence |
| --- | --- | --- |
| Raw-source project lifecycle and business policies | Host-bound membership, storage rules and independent authoritative field verification drive automatic views; user self-reports stay pending | [v7.2](docs/design/AGENT_MEMORY_DESIGN_V7.2.0.md) · [Example](examples/project_memory_lifecycle.py) |
| Scene/persona evolution and project history | One shared scheduler; admitted-L1 dual-time rebuilding with frozen business state and current authorization/erasure | [v7.2 tasks](docs/design/v7.2.0/task.md) |
| Governed extraction, domain verification and host | Model proposals/review, finite cross-turn inputs, durable authoritative verification and native project qualification/refresh; real local model probes use authored inputs | [v7.1](docs/design/AGENT_MEMORY_DESIGN_V7.1.0.md) · [Host](examples/memory_host.py) · [Model host](examples/model_memory_host.py) |
| Exact local model budgets and logits | Optional local Transformer renderer/receipt checks and real Qwen3 yes/no logits; core remains dependency free; production promotion requires real acceptance | [Local adapters](packages/local-models/README.md) |
| Reliable L0 → L1 | Host outbox, atomic receive/publish, source revisions, explicit reprocessing, same-slot corrections, and dual-time fact reads | [Delivery chain](docs/design/v6.1.0/batch-03-05.md) · [Contribution lifecycle](docs/design/v6.1.0/stage-03.md) |
| Recovery and readiness | Fixed processing targets, bounded waiting, batch closure, local candidate indexing, explicit repair, and stream rollover | [Index recovery](docs/design/v6.1.0/stage-09.md) · [Batched publication](docs/design/v6.1.0/stage-11.md) |
| Erasure and backup replay | Participating outbox deletion sync and controlled offline replay using an independently held authoritative checkpoint | [Deletion sync](docs/design/v6.1.0/stage-04.md) · [Backup replay](docs/design/v6.1.0/stage-10.md) |
| Current Observation | Same-scope language facets, complete input dependencies, invalidation, full rebuild, and conditional language templates | [Lifecycle](docs/design/v6.1.0/stage-12.md) · [Conditions](docs/design/v6.1.0/stage-13.md) |
| Historical Observation | Frozen language snapshots/context, independent known/valid time, certified coverage and current permission/erasure checks | [History](docs/design/v6.1.0/stage-14b3.md) |
| Derived parent inputs | Fixed current language revisions, complete processing lineage, guarded delivery and transitive physical erasure | [Stage 14C](docs/design/v6.1.0/stage-14c.md) |
| Query and host permissions | Versioned current queries, expiring local authority, source-grant binding, and checks before final delivery | [Stage 14A](docs/design/v6.1.0/stage-14a.md) |
| Current language L2 pages | Typed Scenario/Page/Block versions, stable block identities, atomic full rebuild, fixed readiness targets, guarded delivery, and transitive erasure | [Stage 15](docs/design/v6.1.0/stage-15.md) |
| Trusted project questions | Host-reviewed owner, status, commitments and risks; deterministic full/delta answers and guarded proof reuse, shared durable refresh, exact routing and opt-in SDK/MCP reads | [B3](docs/design/v7.0.0/batch-b3.md) · [B4](docs/design/v7.0.0/batch-b4.md) · [Example](examples/project_questions.py) |
| Qualified current parents and project L2 pages | Compatible trusted contexts, preserved conditions/exceptions, stable blocks, host-only typed patches and immutable provenance; guarded whole-page delivery | [B3](docs/design/v7.0.0/batch-b3.md) · [Typed patches](docs/design/v7.0.0/typed-page-patches.md) |
| Governed model/cache contracts (opt-in) | Host-bound immutable provider/input/output identity, exact caching, multi-account reservations and unknown-cost recovery; real local Qwen 9B smoke over synthetic facts, domain/cost acceptance open | [B5](docs/design/v7.0.0/batch-b5.md) · [Local smoke](docs/design/v7.0.0/ollama-smoke.md) |
| Bounded host retention | Atomic reference-aware QuestionView GC with explicit holds; exact receipts, delta ancestry and model audits can remain pinned and exhaust capacity | [Retention/GC](docs/design/v7.0.0/retention-gc.md) |
| Offline A9 evaluation tooling | Actual SQLite runtime arms, isolated ablations, all-phase unknown debt, paired bootstrap and opt-in observer/tariff/Ollama binding; synthetic evidence only | [A9 runner](docs/design/v7.0.0/a9-experiment-runner.md) |
| Retrieval and feedback foundations | Scoped, bounded recall; optional lexical/hybrid candidates; outcome-linked Episode/Procedure and gated Evolution components | [Architecture](docs/ARCHITECTURE.md) · [Feedback](docs/FEEDBACK_CONTRACT.md) |

Observation remains limited to documented language templates. Published-point and certified-interval history preserve frozen policies/context and current access checks; gaps are rejected. Current `locale-parents/1` views bind fixed parent revisions and transitive processing permissions. Current `language-scenario/1` pages combine 1–4 non-conditional language Observation parents in the same exact scope, with compatible subject, purpose, and authority.

With explicit `qualified_current=True`, language templates preserve trusted conditions and exceptions. `QuestionService` supplies project questions, maintained pages, proof reuse and [host-only typed block patches](docs/design/v7.0.0/typed-page-patches.md); pass `questions=service` to the SDK/MCP adapter for the optional interface. [v7.2](docs/design/AGENT_MEMORY_DESIGN_V7.2.0.md) adds automatic raw-source project handoff, storage/independent-verification policies, registered scene/persona evolution and `history_rebuild=True` dual-time question/page rebuilding. Coverage begins at opt-in registration and supports admitted-L1 censuses; historical publication-manifest state, precoverage reconstruction, pages as parents, legacy facet/page delta, distributed composition and remote ACL synchronization retain explicit limits. L1 dual-time queries remain independent; `l1_decided` establishes processing completion while admission and qualification establish factual support. Real business quality and complete cost acceptance still require approved corpus, independent gold and tariffs.

Validation records include SQLite and real PostgreSQL contracts, cross-connection races, process-kill recovery, and backup replay. The [stage 12 full-suite report](docs/design/v6.1.0/stage-12-full-test.md) is an older baseline; [stage 13](docs/design/v6.1.0/stage-13.md) and stage 14 [A](docs/design/v6.1.0/stage-14a.md)/[B.3](docs/design/v6.1.0/stage-14b3.md)/[C](docs/design/v6.1.0/stage-14c.md) record targeted and affected regression runs. [Stage 15](docs/design/v6.1.0/stage-15.md) records a full repository/package run and build/install verification, with [independent evidence](docs/design/v6.1.0/validation-stage-15.json). Each report applies to its recorded code baseline; counts are not cumulative. Production acceptance, real-domain extraction quality, and full M0/M1/M2 milestone acceptance remain open.

**The full architecture target remains [v7.0.0](docs/design/AGENT_MEMORY_DESIGN_V7.0.0.md), with the current implementation contract in [v7.2](docs/design/AGENT_MEMORY_DESIGN_V7.2.0.md).** B0 provides strict protocol/domain and whole-cost contracts; [B1](docs/design/v7.0.0/batch-b1.md) provides indexed invalidation; the [B2 scheduler](docs/design/v7.0.0/batch-b2.md) supplies durable coalescing, fixed coverage targets, shared budgets and host execution. [B3](docs/design/v7.0.0/batch-b3.md) closes the finite current project read/page lifecycle on those foundations. Its [validation record](docs/design/v7.0.0/validation-b3.json) states the actual source fingerprints and per-run results. Earlier stage counts are separate evidence, not cumulative passes.

[ProjectAdmission](src/agent_memory/consolidation/project_admission.py) requires trusted source authorities, reviewed membership and field/time support. Missing completion stays unknown, conflicting owners stay contested, and no risk match means only a complete empty result within the known authorized scope. Both admitted-L1 and explicitly closed finite publication manifests are supported. Current permission/context/time is checked before bodies and at delivery; deletion and old-backup replay also scrub unpublished registrations and page routes.

Upgrade requires stopping old writers. The project candidate index is additive; opt-ins default off and rollback disables the new readers/workers while preserving the authoritative deletion journal. Project pages are bounded projections of current parents. After initial host publication, an enabled `RefreshHost` maintains published pages through the existing shared scheduler, parent dependencies, leases and resource budgets; stale pages still refuse reads until valid publication. No second queue or free-form editor is introduced. Governed model answers reserve cache slots before dispatch and report current ledger settlement. Long-lived model authorization audit requires explicit host durable archival and checkpoint/receipt acknowledgment; financial receipts and other finite retention limits remain protected. See [runtime repairs and compatibility](docs/design/v7.0.0/runtime-repairs.md).

[B4](docs/design/v7.0.0/batch-b4.md) adds complete-group deterministic delta, old-remove/new-add aggregates, continuous-log checks with real full fallback, and immutable generation versus current validation proofs. Original private inputs remain guarded even when public values match. Its [validation record](docs/design/v7.0.0/validation-b4.json) covers randomized full equivalence, real dual-provider lifecycle/erasure, and independent adversarial review. Runtime/registration/worker contracts advance to version 2; upgrade requires re-registration after draining old writers.

B2's [verified lifecycle evidence](docs/design/v7.0.0/validation-b2.json) includes fixed responsibilities, fair shared limits, clock guards and process-kill recovery. Its deterministic resource limits do not represent model-spend budgets or measured production savings.

Remaining acceptance follows the [v7 plan](docs/design/v7.0.0/plan.md) and [task ledger](docs/design/v7.0.0/task.md): licensed held-out domain quality and measured whole-cost comparison. Local Ollama configuration and nine synthetic runtime checks passed with seven actual Qwen 9B generations; this does not establish those external results. All 44 previous task statuses remain preserved. Synthetic tests and this local smoke do not establish licensed real-data quality, production savings or full M0/M1/M2 acceptance; those gates remain open.

Bounded read-only Reflect is implemented through two governed source calls, exact citations and final authorization; it does not publish memory or expose open tools. Remaining broader targets stay on the [v7.1 ledger](docs/design/v7.1.0/task.md). Remote deployments use the [security policy](SECURITY.md) and [threat model](docs/THREAT_MODEL.md); external caches, remote ACL systems, and provider-held copies need their own integration contracts.

Explicit model explanations are available through the host-bound [B5 runtime](docs/design/v7.0.0/batch-b5.md) and SDK `question_model_answer`. Ordinary structured answers remain model-free. The opt-in [local validation CLI](docs/design/v7.0.0/ollama-smoke.md) freezes installed model/configuration and exercises caching, authorization and erasure. Real-domain quality/cost acceptance remains open; local compute is never assumed free.

## Documentation

| You want to… | Start here |
| --- | --- |
| Understand the design | [v7 architecture](docs/design/AGENT_MEMORY_DESIGN_V7.0.0.md) · [Module boundaries](docs/ARCHITECTURE.md) |
| Admit or extract facts | [Atom admission](docs/ATOM_ADMISSION.md) · [Automatic extraction](docs/ATOM_EXTRACTION.md) |
| Read facts across time | [Bitemporal memory](docs/BITEMPORAL_MEMORY.md) |
| Build a controlled language view | [Observation lifecycle](docs/design/v6.1.0/stage-12.md) · [Conditional view](docs/design/v6.1.0/stage-13.md) · [Query and permissions](docs/design/v6.1.0/stage-14a.md) |
| Read historical views or compose current pages | [Bounded history](docs/design/v6.1.0/stage-14b3.md) · [Parent inputs](docs/design/v6.1.0/stage-14c.md) · [Current L2 pages](docs/design/v6.1.0/stage-15.md) |
| Connect feedback and evolution | [Feedback contract](docs/FEEDBACK_CONTRACT.md) · [Evolution package](packages/evolution/README.md) |
| Deploy and recover | [Single-host deployment](docs/single-host-deployment.md) · [Recovery operations](docs/recovery-operations.md) |
| Inspect evaluation evidence | [Evaluation methodology](docs/LOCAL_MEMORY_COMPARISON_EVAL.md) · [Resource baseline](docs/RESOURCE_BASELINE.md) |
| Follow or contribute to development | [v7 plan](docs/design/v7.0.0/plan.md) · [v7 tasks](docs/design/v7.0.0/task.md) · [Design versions](docs/design/README.md) |

## Contributing

Useful contributions include **a reproducible edge case, an integration adapter, or a well-annotated evaluation scenario**. Start with the [task ledger](docs/design/v7.0.0/task.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

```bash
./setup.sh --all
source .venv/bin/activate
python -m pytest -q
```

Integration and live PostgreSQL tests have additional setup; see [CI](.github/workflows/ci.yml). Preserve the small default footprint, host-owned scope, and traceable evidence.

[Open an issue](https://github.com/agent-memory-lab/agent-memory/issues) with a reproducible example, or report security vulnerabilities privately through [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE).

B6 fixed-source closeout verification: 3931 passed, zero skips/failures/errors across the complete repository/all package suite; all prior 3783 passing cases remain covered. Six distributions were built/clean-installed, 241 Python files and 19 SQL migrations matched, and full source/12 archives scanned with zero findings. Bounded reference-aware GC and frozen offline A9 tooling are included. See [the scoped report](docs/design/v7.0.0/batch-b6.md). The later [local Ollama smoke](docs/design/v7.0.0/ollama-smoke.md) has separate targeted evidence; licensed-domain quality and measured whole-cost acceptance remain open.

The [v7.1 runtime extension](docs/design/AGENT_MEMORY_DESIGN_V7.1.0.md) documents module boundaries, algorithms, data flow and additive rollout. [Validation](docs/design/v7.1.0/validation.md) separates contracts, actual local-model execution and missing real business gold/pricing. Code interfaces do not constitute production acceptance.
