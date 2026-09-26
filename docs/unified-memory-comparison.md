# Unified capture, deletion and comparison

This is an optional composition layer, not a replacement for the core API.
It has not yet been tested. Earlier acceptance counts do not cover these changes.

## Raw interaction entry point

```python
from agent_memory import MemoryScope
from agent_memory.unified_memory import UnifiedMemory

scope = MemoryScope("tenant", user_id="user", session_id="session")
memory = UnifiedMemory.local("memory.db", scope, generator=host_generator)
await memory.initialize()
try:
    receipt = await memory.capture(
        event_id="message-1", role="user", content="I now prefer tea.", run_id="run-1"
    )
    bundle = await memory.recall("What drink does the user prefer?")
    await memory.forget_sources((receipt.provider_event_id,))
finally:
    await memory.close()
```

The host generator implements `generate_claims(MemoryEvent)` and receives only
the captured evidence, not benchmark answers. Without a generator, the entry
point stores events but does not claim automatic semantic extraction. Roles
are `user`, `assistant`, and `tool`; content may carry a textual tool transcript.
No arbitrary metadata claims are accepted. Capture uses the existing bounded
redaction policy. Generated claims must remain in the exact bound scope; the
generator must choose a scope level matching that scope. Invalid or low-confidence
model records can still be filtered by the existing extractor.

## Explicit deletion participants

Initialize the candidate store and open the managed ontology runtime, then pass:

```python
from agent_memory.unified_memory import ManagedOntologyDeletionTarget

targets = {
    "rule-candidates": candidate_store,
    "ontology": ManagedOntologyDeletionTarget(live_ontology),
}
memory = UnifiedMemory.local("memory.db", scope, generator=host_generator, targets=targets)
```

Only register an ontology runtime reading this same core database and exact
scope. Other enabled derived stores need their own idempotent `forget_sources`
participant. Hosts own initialization and closure of injected participants.

Use source **provider event IDs**, not transport event IDs or claim IDs.
Whole-scope deletion is `await memory.forget_sources(all_in_scope=True)`.
Archive retains content; erase removes matching candidate payloads. Any missing
root invalidates the whole candidate proof even if other roots remain.

The journal is durable and records IDs, scope hash, mode and participant names,
not source content. Source deletion runs first, followed by all participants.
Incomplete cleanup leaves a pending operation; capture and recall through this
facade are blocked until `await memory.resume_deletion()` succeeds. Restore the
same participant names and configuration after restart. Cleanup repeats on
retry; counts describe the current attempt. This is not an append-only audit log.

Important limits: use one host-owned facade per scope and coordinate all writers.
The journal does not fence independent concurrent writers across processes.
Direct core, raw index and candidate APIs bypass the facade. This is not a
distributed atomic deletion transaction, backup erasure, or automatic coverage
of unregistered caches, queues, artifact files or external systems. Managed
ontology refresh must finish before deletion succeeds. Static indexes require
a separate target. A source change may still occur after a point-in-time check.

## Real comparison adapters

`agent_memory.comparison_adapters` contains:

- `AgentMemoryComparisonAdapter`: calls the unified raw capture/recall path.
- `Mem0ComparisonAdapter`: calls the synchronous OSS `Memory.add/search/delete_all` API.
- `GraphitiComparisonAdapter`: calls `add_episode/search/remove_episode` APIs.
- `compare_raw_interactions`: records ingestion, initial search, appended updates,
  updated search, whole-run deletion and post-deletion search.

All arms receive the same timestamped role/content text. No gold facts are
supplied to the local arm. Metadata envelopes differ between APIs and must be
disclosed. Each external adapter creates a fresh evaluation user/group; the
host must also give Agent Memory a fresh scope. Never run with production data.
Clients, models, backend configuration and SDK versions are supplied and pinned
by the experiment runner, not installed by this module.

The current Mem0 adapter targets `search(top_k=..., filters=...)`; older SDKs
with a different signature are not silently substituted. Graphiti is not Zep
Cloud. Consult the actual pinned SDK before running:

- https://github.com/mem0ai/mem0/blob/main/mem0/memory/main.py
- https://github.com/getzep/graphiti/blob/main/graphiti_core/graphiti.py

This runner is an initial behavioral probe, not a scored scientific benchmark.
It has no automated answer judge, token/cost instrumentation, SDK version lock,
hard call deadline, repetitions or confidence intervals yet. Failure is recorded,
not replaced with fabricated results. Failed runs may require cleanup, particularly
if ingestion committed before a network error. Graphiti deletion tracks episode
receipts only within this adapter process; it does not prove full graph erasure.
Mem0 whole-user deletion is not a per-source deletion capability. API success
alone is not proof of deletion: inspect the recorded post-deletion queries.
Use only approved test data and authorized model budgets.
