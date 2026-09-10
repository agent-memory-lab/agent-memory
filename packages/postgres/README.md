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
