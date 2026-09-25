# Optional ontology and training extensions

These extensions are explicitly assembled by the host. Default SQLite behavior
and core mandatory dependencies do not change. This development batch has not
yet been validated by tests; previous acceptance numbers do not cover it.

## Ancestor scope layers

`agent_memory.ontology_layers.LayeredOntologyMemory` takes a trusted child
context, child catalog, ontology ID and an explicit list of opened managed
runtimes. The runtimes must form a distinct ancestor chain within the child's
tenant and namespace, and must activate the same schema digest. Missing, stale,
or incompatible layers fail closed rather than silently returning partial data.

```python
from agent_memory.ontology_layers import LayeredOntologyMemory
from agent_memory.ontology_api import OntologyAPI

# parent_runtime and session_runtime are already opened by the host.
layers = LayeredOntologyMemory(
    session_context, session_catalog, ontology_id,
    [parent_runtime, session_runtime],
)
api = OntologyAPI(None, session_catalog, ontology_id, store_resolver=layers.borrow_store)
# Alternatively, pass layers as the host's ontology recall_pipeline.
```

Narrower valid facts override broader facts for functional properties. This also
applies when the narrower text does not match the search. A narrower unresolved
functional conflict blocks the broader value. Non-functional relations remain
additive. Source assertion IDs and scopes are retained; graph paths still never
join same-named entities across partitions. This composes ontology indexes, not
the provider's Current State Claims. Bounded per-layer retrieval is not an
exhaustive global ranking. Large override/conflict scans reject instead of
silently weakening isolation. Hosts own lifecycle opening/closing of all layers.

## PostgreSQL-native managed ontology

```python
from agent_memory_postgres import (
    PostgresOntologySource, PostgresOntologyRegistry, PostgresHostedOntologyMemory,
)

# Initialize the PostgreSQL core repository separately first.
catalog = PostgresOntologyRegistry(dsn)
await catalog.initialize()
# Register and activate a schema using the existing trusted host approval flow.
source = PostgresOntologySource(dsn)
async with PostgresHostedOntologyMemory(
    source, catalog, ontology_id, context, batch_size=32, max_batches=8,
) as memory:
    while not await memory.refresh():
        pass
    # Use memory as recall_pipeline, or memory.borrow_store for OntologyAPI.
```

The catalog, immutable source snapshots, evidence, checkpoint, index identity,
generation inventory and active state are all in PostgreSQL. No local SQLite
file is needed. A repeatable-read snapshot uses database-side INSERT SELECT;
projection reads bounded pages. Acceptance independently reconstructs expected
assertions and verifies content, evidence and entities before publication.

A session advisory lock admits one owner per control namespace/scope/ontology
job. Database ownership tokens fence checkpoints and publication after takeover.
The host must route a job's requests to its owning runtime; this is not a queue
or transparent distributed load balancer. Different scope jobs can run on
different hosts. Runtime cancellation waits for bounded refresh work to settle.
Source revision and activation freshness are checked around query execution.

At most the previous active generation and current building generation are
retained during normal operation. Publication removes older generations and
their snapshots. Rollback selects a host-approved previous schema and rebuilds
from current evidence; it does not restore forgotten evidence. PostgreSQL-hosted
refresh currently rebuilds the exact scope, not the local SQLite delta path.
Snapshot copy and final acceptance are not bounded by max_batches and may need
a larger configured database statement timeout for large scopes. The database
role needs schema/table/trigger creation and owned-generation cleanup rights.

## Bounded relation rules

```python
from agent_memory.ontology_rules import OntologyRuleEngine, RelationRule

engine = OntologyRuleEngine(store, schema, live_evidence_verifier, [
    RelationRule("two-hop-ancestor", "1", "parent", "parent", "ancestor"),
])
result = await engine.derive(scope, root_assertion_ids)
for candidate in result.candidates:
    if await engine.valid(candidate):
        pass  # Host may display the candidate with its proof or request approval.
```

Supported semantics are positive two-relation chains over entity-valued
properties, with bounded repeated application. This is not an OWL/RDFS reasoner,
negation engine or arbitrary rule interpreter. Rules are supplied by the host
and checked against declared domain/range classes. Proof roots, source event IDs,
rule versions, rule-set/schema digests and intersected validity intervals are
returned. Candidates are not written into active memory or auto-promoted.

Consumers must revalidate cached candidates before use. Revalidation refetches
the premises, checks current evidence and replays the rule set. Removed,
superseded, expired or changed premises invalidate the candidate without changing
raw evidence. There is no background invalidation daemon or persistent derived
fact store in this extension. A truncated result is incomplete, never a proof
that another inference is impossible.

## Training and orchestration handoff

`agent_memory_evolution.training.MemoryTrainingBridge` consumes existing
`FeedbackTrajectory` objects, which already link a decision, outcome, evaluation,
reward and source evidence in one exact scope. Required host callbacks authorize
operations, verify current feedback state and redact the exported JSON object.
Records and bytes are bounded; expired/revoked feedback and unavailable evidence
are rejected. Reward definitions and task success remain host-owned.

```python
from agent_memory_evolution.training import MemoryTrainingBridge

bridge = MemoryTrainingBridge(
    authorize=host_authorize, is_current=host_feedback_is_current,
    redact=host_redact, evidence=live_evidence_verifier,
    trainer=host_training_adapter,
)
batch = await bridge.export(scope, trajectories, "approved-dataset.jsonl")
job = await bridge.submit(scope, trajectories)
status = await bridge.status(job)
# await bridge.cancel(job)  # separately authorized by the host
```

The trainer implements async submit(payload, idempotency_key), status(job_id)
and cancel(job_id). Submit keys derive from authorized scope and redacted content;
the external adapter must persist idempotency and handle uncertain responses.
The host must persist returned job receipts if it needs restart recovery.
Exports use exclusive file creation and never overwrite existing datasets.
Host redaction remains responsible for PII, secrets and training consent.

This is feedback dataset exchange and controlled job orchestration, not bundled
PPO/GRPO/DPO training or autonomous Agent deployment. Existing AgentMemoryAdapter
and LangGraph lifecycle hooks remain the execution integrations. A training job
does not automatically alter prompts, tools, active procedures or model weights.
