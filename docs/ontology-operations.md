# Ontology operations: query, recover, integrate

These features are opt-in. Core installation still has no external dependencies.
The ontology representation is a typed, evidence-backed projection, not an OWL
reasoner. Source Claims remain authoritative.

## Exact authorization and graph queries

`get_assertions(scope, ids, ontology_id=..., ontology_version=..., at_time=...)`
reads at most 256 exact IDs. Only active, visible, currently valid assertions with
live subject/object entities are returned. Recall governance uses this operation
instead of rerunning lexical search. Ranking scores are not used as authority.

`neighbors()` supports outgoing, incoming and both directions plus predicate
filters. `traverse_ontology()` supports a target entity and returns discovered
shortest paths as assertion-ID sequences with evidence-bearing edges. Configure
`max_depth`, `max_nodes`, `max_edges` and `token_budget`. Its token estimate is a
character-based estimate, not a model tokenizer. Traversal stops or marks results
truncated when a budget is reached. It does not combine paths across scopes.

```python
from datetime import datetime, UTC
from agent_memory import traverse_ontology

graph = await traverse_ontology(
    store, trusted_scope, "person:alice",
    ontology_id="people", ontology_version="1.0.0",
    at_time=datetime.now(UTC), target_entity="person:bob",
    predicates=("knows",), max_depth=3, max_nodes=32,
    max_edges=64, token_budget=1200,
)
```

## Persistent rebuild and recovery

`LiveOntologyMemory` now keeps an atomic generation manifest under its work
directory. The source database has a persistent source identity; every target
index has an independent index UUID. Durable checkpoints are bound to that UUID.
After restart, matching source revision and Schema can reuse completed work or
continue a partial backfill. Missing or replaced indexes are rejected instead
of silently accepting their old checkpoints.

The workspace holds an OS file lock for the worker lifetime. A second worker for
the same source, scope and ontology is rejected. A crashed process releases that
lock automatically. Use a local filesystem with working advisory locks and keep
the work directory private to the host.

Successful generations replace running plugins after acceptance; old readers
finish before their plugin is closed. The latest two schema generations may be
retained for reuse/rollback when the source revision is unchanged. A changed
source revision retires and reclaims old generations after successful publication
so erased evidence is not indefinitely retained in an old rollback index.
This does not replace the host's backup and legal-erasure retention policy.

Existing checkpoint callers may omit `target_index_id` for legacy compatibility.
New standalone backfills should initialize the target store, obtain
`await store.index_identity()`, and pass it to both `SQLiteOntologyCheckpointSink`
and `backfill_ontology_memory`. Managed live jobs do this automatically.

## SDK and MCP

Construct a host-bound `OntologyAPI(store, catalog, ontology_id)` and pass it as
`ontology=` to `EmbeddedMemoryClient`, `MCPMemoryTools` or `create_server`.
Both embedded and remote `MCPMemoryClient` clients expose:

- `ontology_status()`
- `ontology_search(text, limit=8)`
- `ontology_assertions(ids)`
- `ontology_graph(start_entity, **budgets_and_filters)`
- `ontology_switch(version, expected_generation=..., reason=..., action=...)`

The switch tool is advertised only if `OntologyAPI(..., switch_policy=...)` is
configured with a trusted host authorizer. The agent cannot supply a scope or an
approver identity through tool arguments. Register schemas and prepare indexes
through host APIs before activating them. Fixed-store API instances must point
to an index that contains the selected version.

The MCP CLI can enable read-only ontology tools:

```sh
agent-memory-mcp --tenant-id demo --user-id alice \
  --ontology-id people --ontology-registry ontology-registry.db \
  --ontology-database ontology.db
```

## PostgreSQL

Install the existing optional `agent-memory-postgres` package. PostgreSQL ontology
storage does not need pgvector. Keep the DSN in a host environment variable:

```python
from agent_memory import OntologyStoreConfig

store = OntologyStoreConfig(
    backend="postgres", dsn_env="AGENT_MEMORY_ONTOLOGY_DSN",
    namespace="agent_memory_ontology",
).create_store()
await store.initialize()
```

The adapter implements schema registration, projections, evidence invalidation,
functional conflicts, host resolution, lexical search, exact authorization and
graph queries using native PostgreSQL tables. Transactions use namespace-scoped
advisory locks and statement/lock timeouts. This favors predictable correctness
over maximum parallel throughput; configure separate namespaces for independent
installations. Runtime DSNs are never serialized into SDK responses.

For MCP use `--ontology-backend postgres --ontology-dsn-env
AGENT_MEMORY_ONTOLOGY_DSN --ontology-namespace agent_memory_ontology`.

The managed core-Claim snapshot/rebuild coordinator remains SQLite-specific.
PostgreSQL hosts supply their own Claim snapshot through `OntologyClaimSnapshot`
and can reuse the generic backfill and plugin interfaces. The shared SQL
projection implementation is exercised against both real database engines.

## Verification

`tests/test_ontology_queries.py` runs the same contract against SQLite and, when
`AGENT_MEMORY_ONTOLOGY_TEST_DSN` is set, PostgreSQL. Other tests cover durable
resume, index replacement, worker exclusion, SDK validation and real in-process
MCP transport. PostgreSQL tests use unique test namespaces; point the DSN at a
disposable database. No database server is installed or launched by the tests.
