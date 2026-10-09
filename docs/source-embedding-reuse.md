# Opt-in retained-source embedding reuse

`agent_memory.retrieval.source_embeddings` adds a finite, host-only persistent
embedding API. It is not installed in the default retrieval/reranking pipeline.
`EmbeddingReranker` keeps its existing request-only, no-retained-vectors contract.
No model, network client, downloads, model-quality claim, latency claim, or default
retention permission is added.

## Scope and authority

`SourceEmbeddingAuthority` accepts only the exact ID of a current retained raw
source revision. Atom text, derived objects, arbitrary candidate strings,
`MemoryItem.metadata` ACLs, and generic `MemoryQuery` fields cannot establish this
authority. A host selects candidate source IDs separately and consumes the returned
vectors explicitly. Query embeddings remain request-local in the existing reranker;
this API never stores query text or query vectors.

The host must provide an `ObservationService` with an independently maintained
current authority version floor, explicit source read grants, distinct embedding
processing grants, and a trusted `verify_coordinates(uow, coordinates)` callback.
That callback must authenticate the principal/purpose/audience and check the whole
current coordinate/query proof against authoritative state using the supplied
transaction. Returning a caller-provided Boolean or checking only a supplied epoch
is not a valid implementation. `ModelCoordinates` is a reusable exact-coordinate
value object; it is not a grant or a generation-model configuration.

Read and processing checks share the same internal implementation used by
`SourceModelAuthority`. Embedding processing grants have their own ledger kind,
so a grant to generate model answers cannot enable embedding calls. Explicitly
identify the actual processing destination/account/region/policy in
`EmbeddingConfiguration.recipient`. The injected port is trusted host code and
must not change this destination or the configured model.

## Host integration

```python
from agent_memory.retrieval.source_embeddings import (
    EmbeddingConfiguration,
    EmbeddingResponse,
    GovernedSourceEmbeddings,
    SourceEmbeddingAuthority,
)

configuration = EmbeddingConfiguration(
    provider="host-injected",
    recipient="the-hosts-verified-destination-account-region-policy",
    model="the-installed-embedding-model",
    model_revision=verified_immutable_model_digest,
    runtime_sha256=verified_runtime_digest,
    dimensions=verified_dimensions,
    normalization="l2",  # Or "none"; both are part of the exact key.
)
authority = SourceEmbeddingAuthority(
    observation_service,
    configuration=configuration,
    verify_coordinates=validate_authenticated_current_coordinates,
)
await authority.allow_processing(
    current_source_id,
    readers=(authenticated_actor,),
    purposes=(approved_purpose,),
    expires_at=processing_permission_expiry,
)

# The host supplies an async port with the SAME immutable configuration.
# port.embed(source_input) returns:
# EmbeddingResponse(configuration.fingerprint, source_input.key, vector_tuple)
# It must perform only the explicitly configured embedding operation.
embeddings = GovernedSourceEmbeddings(
    authority,
    injected_port,
    cache_seconds=300,
    max_cache_entries=128,
    max_cache_bytes=8 * 1024 * 1024,
    max_inflight=4,
)
result = await embeddings.embed_source(authenticated_coordinates, current_source_id)
# result: source_id, configuration_sha256, values (tuple), cache_hit
```

Pin provider, full recipient identity, model, immutable model revision, runtime
manifest digest, dimensions, normalization, input encoding revision, input byte
limit, and timeout. `l2` requires unit norm within 1e-6; the API does not silently
normalize a response. All vectors must have the exact dimension count, numeric
finite non-Boolean components, and a finite norm between 1e-150 and 1e150. Empty,
zero, unknown, malformed, mismatched-model, and mismatched-input results fail
closed. This validates the response contract, not semantic embedding quality.

The cache key includes the entire pinned configuration and exact coordinate
proof, source identity and content digest, authoritative document-head/revision
proof, scope, read/processing grants, current authority, and retention epoch.
Even equal text from another source, actor, model, or coordinate proof does not
reuse the entry. This intentionally conservative slice reuses across repeated
requests with the same complete proof; it does not attempt cross-query proof
substitution or semantic-similarity cache matching.

## Transactions and bounded lifecycle

- Before any source body read, read/processing grants and full coordinates are
  checked. Before a vector body read, dispatch, publication, and delivery, the
  complete original source/configuration/proof is revalidated in the existing
  scope-locked unit of work. Each waiter gets its own delivery revalidation.
- Cache entries use separate `source_embedding_header` and
  `source_embedding_body` records, never answer-cache response serialization.
  No raw source text is persisted in this cache. Published headers contain the
  original source-generation manifest, not a replacement proof inferred from
  equal text.
- Reusable opaque slots bound total header/body rows, including tombstones.
  Serialized-payload byte reservations are taken before dispatch. This is a
  logical payload budget, not an estimate of database pages or index size. Set
  consistent limits across hosts sharing a scope. A smaller runtime refuses a
  census larger than its configured slot limit.
- A durable reservation prevents duplicate concurrent execution for the same
  key across runtime instances and bounds in-flight calls. A timed-out or crashed
  reservation expires; its token can never publish over a replacement. Cancelling
  the executing caller cancels that execution and releases its reservation;
  another waiter may then retry. No background in-process task retains inputs.
- Permission expiry caps vector expiry. Operations scrub expired bodies before
  use. The host can call `sweep_expired()` for physical TTL cleanup while idle;
  there is no implicit background scheduler. Revoked/stale proofs are immediately
  unreadable, even before TTL cleanup. Capacity eviction also scrubs old bodies.
- Source erasure, whole-scope erasure, and authoritative old-backup purge replay
  scrub vectors, generation manifests, processing grants, and dependency edges
  on both SQLite and PostgreSQL. The same deletion planner is used by live erase
  and restore replay. A restored database must be replayed through the existing
  trusted purge-restore protocol before it is served.
- These raw-source records have no QuestionView/atom/derived parents. They are
  explicitly recognized by question retention/erasure handling; their source
  dependencies do not silently turn reusable slot IDs into question instances.

Erasure cannot retract a vector already returned to a caller, revoke an external
provider's copy, or promise physical overwrite of database free pages/backups.
The host/provider's existing retention obligations still apply. The API performs
no external calls unless the host explicitly supplies and invokes a port.

## Verification

`tests/test_source_embeddings.py` uses injected known vectors, the existing real
SQLite/live-PostgreSQL parameterized fixture, and an actual old-backup restore
replay. It covers exact reuse, configuration/coordinate changes, denied body
reads, read/processing/authority changes before dispatch and during execution,
post-publication delivery checks, source revision changes, unknown/malicious
responses, expiry, durable concurrency, cancellation, stale execution fencing,
physical payload bounds, erasure, and question-GC recognition. A missing
`AGENT_MEMORY_TEST_POSTGRES_DSN` skips the live PostgreSQL variants; SQLite passing
does not imply a live PostgreSQL pass. All values are synthetic contract evidence,
not a retrieval-quality or real-model-performance evaluation.

## Disabling reuse and rollback

For an operational rollback, stop invoking this optional API and keep the current
compatible storage runtime responsible for authorization, cleanup and erasure.
Drain or fence in-flight embedding work, then run the host-owned `sweep_expired()`
maintenance after entries expire. No scheduler is silently installed. Existing
read/processing obligations continue while any retained slot or grant remains.
The unchanged generic `EmbeddingReranker` is available as the request-only path.

This disables the optimization; it is not permission to downgrade the database
writer to code that does not understand `source_embedding_*` records. Such older
code cannot promise their erasure/restore handling. Do not use an older writer on
a database that has used this cache without a separately reviewed, verified
migration of the new records and current deletion authority. Restored backups
must use a compatible purge replayer before serving. This batch provides no
arbitrary binary-downgrade or credential/provider-retention cleanup shortcut.

`tests/test_source_embedding_runtime_isolation.py` additionally starts independent
repositories, services, authorities and injected ports in separate threads and
async event loops against the same database/scope. It verifies a simultaneous
cold-claim race dispatches exactly once, the in-flight limit is shared, and an
expired worker cannot publish over or scrub another runtime's replacement.
PostgreSQL variants assert distinct actual server backend PIDs from separate
connection pools. These tests do not share a repository's Python write lock or
an embedding-port instance; the backing database enforces their reservation
coordination. They are thread/event-loop isolation tests, not a process-crash test.
