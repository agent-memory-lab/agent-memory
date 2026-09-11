# Agent Memory PostgreSQL Provider

Production persistence provider for `agent-memory`. The package is optional: the core
kernel and SQLite provider do not import PostgreSQL or pgvector dependencies.

## Capabilities

- Atomic event, claim, state-delta, proposal, and evolution-record transactions
- PostgreSQL JSONB and generated full-text search documents
- Optimistic claim supersession
- Provider-scoped schema migrations
- Leased consolidation jobs with `FOR UPDATE SKIP LOCKED`
- Retry backoff and dead-letter handling
- Optional pgvector artifact index with configurable dimensions

## Usage

```python
from agent_memory_postgres import build_postgres_kernel

memory = build_postgres_kernel("postgresql://memory@localhost:5432/agent_memory")
await memory.initialize()
```

The database role must own its schema. pgvector is not enabled by the base migration;
call `PgVectorIndex.initialize()` only when the database has the `vector` extension or
the deployment role is authorized to install it.

## Durable semantic MemoryBlock search

`PgVectorBlockMemory` is an optional sidecar for semantic MemoryBlock retrieval. It stores
only vectors, block ids, and scope columns in pgvector. Block text, evidence, versions, and
authorization remain in the selected `MemoryProvider`; every vector hit is read again through
that provider before it is returned.

```python
from agent_memory_postgres import PgVectorBlockMemory, PgVectorIndex

semantic_blocks = PgVectorBlockMemory(
    provider=memory,
    index=PgVectorIndex(postgres_pool, dimensions=768),
    embedding_provider=embeddings,
    model="text-embedding-3-small",
)
await semantic_blocks.initialize()
await semantic_blocks.write_block(block)
relevant_blocks = await semantic_blocks.search(scope, "deployment preferences")
```

Use `reindex(blocks)` for an explicit, operator-controlled backfill. This sidecar is not loaded
by SQLite or by the core package, and it retains no vectors in the agent process.

## Controlled background consolidation

The PostgreSQL queue is intentionally separate from the request path. Start a worker explicitly;
the default `TrajectoryBlockConsolidator` creates an evidence-backed block only when an event
produced at least two claims that are still current. It does not call a model or promote a learned
procedure.

```python
from agent_memory_postgres import (
    ConsolidationWorker,
    PostgresConsolidationQueue,
    TrajectoryBlockConsolidator,
)

queue = memory.consolidation_scheduler
assert isinstance(queue, PostgresConsolidationQueue)
worker = ConsolidationWorker(
    queue,
    {"memory.consolidate": TrajectoryBlockConsolidator(memory).as_handler()},
    worker_id="memory-worker-1",
)
await worker.run_once()
```

Run `worker.run_forever(stop_event)` in a separate worker process for production. The queue
provides leases, exponential retry, and dead-letter state; request-time ingestion is unaffected.

For a standalone process, the package also installs a worker command:

```bash
export AGENT_MEMORY_POSTGRES_DSN='postgresql://memory@localhost:5432/agent_memory'
agent-memory-consolidate
```

Use `agent-memory-consolidate --once` for a single-job health check or a scheduled worker. The
command uses the same conservative block policy as the in-process example and does not require
an embedding model.

## Live contract tests

The live suite refuses ordinary database names. Create an isolated database whose name contains
`test`, then run:

```bash
export AGENT_MEMORY_TEST_POSTGRES_DSN='postgresql://memory@localhost:5432/agent_memory_test'
python -m pytest -q packages/postgres/tests/test_live_contract.py
```

The suite applies packaged migrations and verifies feedback idempotency, restart persistence,
pagination, and erase propagation. Never point this variable at a business database. A skipped
live test is not PostgreSQL certification.

## Release-ready health scan for capacity governance

Use `agent-memory-health` to run a release-oriented, read-only scan before deployment.
It reports:

- event/claim/artifact cardinality in visible scope
- consolidation queue pressure (`pending`, `running`, `completed`, `dead`)
- lease expiry and dead-job sample
- vector/index integrity (`orphan vectors`, `vectors for archived blocks`)

```bash
agent-memory-health --all-scopes \
  --max-active-claims 1200 \
  --max-active-blocks 800 \
  --max-orphan-vectors 0 \
  --max-dead-jobs 5 \
  --fail-on-violation
```

By default the command exits `2` when `--fail-on-violation` is set and any capacity rule
is exceeded. It prints JSON when `--json` is passed for CI integration.

## Sensitive information scan for release readiness

Use `agent-memory-sensitive-scan` to scan persisted events/claims/artifacts for likely secrets
before publishing.

```bash
export AGENT_MEMORY_POSTGRES_DSN='postgresql://memory@localhost:5432/agent_memory'
agent-memory-sensitive-scan --all-scopes \
  --max-critical 0 \
  --max-high 0 \
  --max-per-pattern 25 \
  --fail-on-violation
```

The scanner checks the main text and JSON fields in memory tables:

- `agent_memory_events.content` / `agent_memory_events.metadata_json`
- `agent_memory_claims.text` / `agent_memory_claims.value_json`
- `agent_memory_artifacts.text` / `agent_memory_artifacts.payload_json`

By default it checks active rows only (`--include-archived` is off). Set `--all-scopes` for a full
database-wide scan.

## One-command release preflight

Use `agent-memory-preflight` to run health scan, sensitive scan, and build packaging in one run.

```bash
agent-memory-preflight \
  --all-scopes \
  --build-path packages/postgres \
  --max-active-claims 1200 \
  --max-orphan-vectors 0 \
  --max-critical 0 \
  --max-high 0 \
  --max-per-pattern 25
```

`agent-memory-preflight` prints a human summary by default and can emit JSON using `--json`.

The preflight also scans source files plus generated wheel/sdist archives for embedded secrets.
Matches are always redacted. Use `--release-scan-path` more than once to scan additional paths,
or run the standalone scanner:

```bash
pip install -e "packages/postgres[release]"
agent-memory-release-scan . --fail-on-violation
```
It exits with code `2` on violations or build failures unless `--no-fail-on-violation` is set.
