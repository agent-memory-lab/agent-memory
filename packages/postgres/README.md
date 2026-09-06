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

memory = build_postgres_kernel(
    "postgresql://memory@localhost:5432/agent_memory"
)
await memory.initialize()
```

The database role must own its schema. pgvector is not enabled by the base migration;
call `PgVectorIndex.initialize()` only when the database has the `vector` extension or
the deployment role is authorized to install it.
