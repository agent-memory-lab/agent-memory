# Managed SQLite Ontology Memory

`LiveOntologyMemory` joins core Claim snapshots, automatic change detection,
index acceptance, and runtime schema replacement. It is an optional
`recall_pipeline` with no additional dependencies or background daemon.

## Host integration

Initialize the core database and registry first. Register and activate a schema
using your authenticated host policy. The live runtime requires that activation;
it independently builds and validates an index before serving it.

```python
from agent_memory import (
    AgentMemory, LiveOntologyMemory, PluginContext, PluginResourceLimits,
    SQLiteOntologySource,
)

# scope, registry and ontology_id are trusted, initialized host configuration.
context = PluginContext(scope, PluginResourceLimits(max_batch_size=32))
source = SQLiteOntologySource("memory.db")

async with LiveOntologyMemory(
    source, registry, ontology_id, context,
    work_directory="ontology-work", batch_size=32, max_batches=8,
) as ontology:
    # Each call performs at most eight projection batches. Larger jobs can be
    # advanced by the host scheduler before handling user requests.
    while not await ontology.refresh():
        pass
    async with AgentMemory.local(
        "memory.db", scope=scope, recall_pipeline=ontology,
    ) as memory:
        bundle = await memory.recall("user preferences")
```

For request-driven loading, handle `OntologySyncPending` and schedule another
`refresh()` call. Do not hide it by serving a previously cached ontology bundle.
Persistent writes, replacements, archive and erase operations from any process
using the tracked SQLite tables are detected at the next refresh or recall.

## Guarantees and operating limits

- Snapshots copy current Claims and live evidence in one SQLite read transaction.
  Rows are copied in batches and paged by Claim ID. Snapshots can be reopened with
  `SQLiteOntologySnapshot.open()` and used with durable backfill checkpoints.
- A transactional revision counter tracks changes to claims, events and their
  source links. It contains no conversation content. Unrelated scopes can cause
  conservative extra refreshes.
- Synchronization is lazy and scope-level: a changed revision rebuilds the bound
  scope in a fresh shadow index. It is not per-assertion delta indexing. Memory
  use is bounded by batches; disk use and total work scale with the scope size.
- The source adapter currently copies the exact trusted scope, not inherited
  parent scopes. Use separately configured jobs for those scopes when needed.
- Acceptance independently recomputes expected assertions, checks their content,
  evidence and entity identity, and rejects omissions, extras and conflicts.
- Source and activation revisions are checked around recall. A concurrent
  mutation causes the result to be discarded and a retry requested.
- Registry changes trigger preparation and replacement on the next call. The
  old instance is closed only when no read is using it. Incompatible schema or
  acceptance failures stop serving; old results are not used as fallback.
- Current-state Claims are supplied by the caller's provider and retain its own
  consistency semantics. This adapter governs the ontology retrieval channel.
- Generations and checkpoints persist across exit. A new runtime resumes matching
  work after checking source, Schema and index identities. An OS lock prevents
  concurrent workers from sharing the same job. Retired indexes are reclaimed
  according to the bounded generation retention policy.
- Acceptance is a snapshot integrity check, not a business quality benchmark.
  Hosts still decide approval, quality thresholds and when to activate versions.

The source tracker is installed only by `SQLiteOntologySource.initialize()`.
Existing core providers and default recall paths do not enable it implicitly.

See [ontology operations](ontology-operations.md) for graph queries, checkpoint
identity, recovery, SDK/MCP configuration and the optional PostgreSQL adapter.
