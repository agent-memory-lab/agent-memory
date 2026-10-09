# Optional exact QuestionView evidence-pack reuse

`QuestionEvidencePacks` is a host-only, current-only projection of an already
published registered `QuestionService` view. It is disabled by default. It adds
no generic bundle cache, no new retained bodies, no source lookup, no model call,
no implicit refresh and no storage schema. Existing QuestionService remains the
owner of atomic publication, complete query dependencies, original-generation
lineage, expiration, erasure and old-backup purge replay.

This finite API must not be described as a new faster generic cache. Zero model
calls and reuse are already available from QuestionService. Any promotion test
must compare the existing optimized QuestionService read path, not a baseline
that needlessly regenerates the answer. No real incremental benefit is measured.

## Host setup and activation

Import `QuestionEvidencePacks` and `EvidencePackConfiguration` from
`agent_memory.retrieval.evidence_packs`. Construction takes the concrete trusted
QuestionService, a frozen configuration, and explicit enablement. Production
activation requires a configuration-bound `RetrievalFeatureApproval` for
`evidence-pack-reuse` plus a synchronous host verifier of the original controlled
real-evidence artifact and rollback authority. A constructed approval value is
not authority. The host must verify licensed workload, configuration, metrics,
measurement provenance and the actual existing optimized control. Enablement is
revalidated for every read and again after final storage awaits. Replacing the
projection configuration does not reuse its old approval. After the final host
approval callback, synchronous time, context and registration checks run again;
time spent verifying promotion cannot deliver a newly expired pack.

`contract_test_only=True` with explicit `enabled=True` is solely for contract
experiments and labels every result `contract-test-only`. It accepts no production
approval. Synthetic data, fake logits and test vectors do not establish efficacy.
Nothing in this API grants permission to send source data to a model or third
party. A model adapter still needs separate recipient/processing authorization.

## Read boundary

`read(question_id, actor=..., purpose=..., audience=...)` accepts an exact
registered identifier and a host-authenticated actor; the host must not populate
that actor from untrusted model arguments. Audience defaults to the same actor;
shared audiences are unsupported. Purpose must exactly match the registered
question. The QuestionService owns scope, context, principal rules and rights.
Aliases and exploratory queries must use their separate routing contracts.
Explicit `valid_at` or `known_at` is rejected; there is no historical cache fallback.

Every reuse validates current metadata and the original generation before loading
question content. It then uses the existing guarded read, checks the complete
original manifest, rereads the current header and rechecks authority/time before
delivery. The exact key includes scope, actor, purpose, audience, definition and
parameters, complete current header/query frontier, original generation manifest,
source IDs, current validity-window proof, output digest and projection config.
No cited-only key, coarse time bucket or semantic similarity substitution exists.

The pack is authorized and valid at the final guarded read within its transaction,
and is returned only after successful UoW completion. Transaction completion or
later transport may cross `valid_until`; consumers must honor the supplied
validity window. The adapter inherits QuestionService's delivery boundary and
does not promise wall-clock freshness at the later instant a caller receives it.

The immutable JSON result preserves original supporting evidence, provenance,
qualifiers, unknown/contested/incomplete states and `world_negative=false`.
Runtime refresh-status bookkeeping is excluded from the deterministic body.
The output byte budget includes both result and proof manifest; overflow is denied
rather than truncating support. A changed unselected candidate, new source after
an empty result, revoked original grant, erased input or time-boundary crossing
cannot be hidden by an old key. No failed proof silently falls back to old text.

## Scope of verification

Synthetic tests exercise real SQLite/PostgreSQL fixtures (live PostgreSQL requires
a test DSN), changed frontiers/rights/context, exact identity/time denial,
post-await expiry and promotion rollback, old-backup erasure replay, capacity and
caller mutation isolation. These establish contract behavior only. Actual local
model efficacy and cold/warm whole-workload performance remain unmeasured.
