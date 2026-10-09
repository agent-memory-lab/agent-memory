# T14 bounded QuestionView retention

This is a narrowly scoped implementation of frozen design §26.8 and
`migration-b0.md` step 5. The [final integrated verification](batch-b6.md) closes
T14 only for registered current capabilities, the explicit bounded retention policy
and coordinated stop/drain/forward-resume. Inherited tasks and real release-quality
gates remain open; the capacity pins and rollback limits below still apply.

## Host API and policy

`QuestionService.collect_garbage(QuestionRetentionPolicy(...))` is an explicit
trusted-host operation. It is absent from the SDK/MCP/model question operations,
never runs implicitly during a read/publication, and has no wall-clock TTL for
content or receipts. The caller supplies a named policy after resolving applicable
legal, policy and historical-retention obligations:

```python
from agent_memory.derived.question_gc import QuestionRetentionPolicy

result = await questions.collect_garbage(QuestionRetentionPolicy(
    policy_id="host:question-retention/1",
    retain_ids=(certificate_under_legal_hold,),
    retain_instances=(instance_with_external_history_obligation,),
    max_delete=256,
))
```

Holds use opaque object/instance IDs, apply transitively, and must be supplied on
every collection. They do not weaken source erasure. The collector cannot infer
external legal obligations, references in another application, or the host's
promises to retain earlier answers. Those belong in the host's explicit holds.
The policy is copied and validated before the first lock wait.

Default limits: 32,768 exact-scope ledger rows; 131,072 stored dependency edges and
computed reference links; 64 MiB of stored payload/edge bytes; 256 deletions per
transaction. Hard upper limits are respectively 65,536, 262,144, 256 MiB and 4,096.
Row/byte/edge census overflow returns `question_gc_census_limit`, with no deletion.
The byte guard uses each backend's encoded representation, so a near-limit census
can be conservatively rejected by one backend before the other.

## Collection and pins

Eligible kinds are question content/certificates, materialized page
content/certificates/block revisions, and completed question refresh
job/execution/publication records. All other records are roots. In particular:

- Current heads, original generation manifests, delta state, all page/block and
  validation references, query/proof/history records and host retention holds.
- Exact legacy and V7 finite coverage receipts, including publication-map keys,
  original targets and the complete immutable completion proof they reference.
- Pending/running/deferred/failed or unrecognized work, requested obligation IDs,
  active executions, scheduler reservations and current head work units.
- Completed tasks until both their lease and their `expires_at` acknowledgement
  window have ended. `complete(lease)` is still supported after `lease_until` and
  must not lose its proof during that window.
- Model cache bodies/headers, dispatch/delivery audit and in-flight model input
  parents. No model/financial audit or unresolved financial responsibility is
  collected by this operation.
- Minimal erased tombstones and all deletion/backup authority. Erase remains
  primary and cannot be reversed by a policy hold or GC.

The graph checks all nested string values and dictionary keys for exact opaque
identities and `derived:` references, plus stored typed dependency edges. This
conservatively pins incidental matching strings too. IDs are only unique within
kind; all records sharing an identity are coupled, and an unrecognized/external
edge owner pins its targets. Unknown ledger kinds, unsupported V7 schemas and
invalid candidate records defer the scope without partial deletion.

Root closure retains immutable processing ancestry, including private inputs of
removed page blocks. Deletion selects complete connected components among
unreachable candidates. If a component does not fit the current batch, it stays
intact; no retained/deferred record can point at a deleted candidate.

## Results and practical limits

The bounded result uses `question-gc-result/1`, reports deleted `(kind, id)` pairs,
retained counts, root-reason counts, remaining kind counts, full-capacity kinds,
and deferred candidate count. Stable reasons distinguish retained references,
batch budget, unsupported data and incomplete census. A `deferred` result can
include successful deletion of other complete components; only `deleted` records
were removed. No content bodies or manifests appear in the result.

Real unreceipted proof-only background cycles reclaim obsolete intermediate
certificate/completion groups. An independently regenerated full answer can
reclaim its obsolete generation when nothing still references it. Tests also
exercise recovery from the actual 4,096 content and certificate boundaries using
valid synthetic historical revisions, then resume real publication.

This does **not eliminate every capacity limit**. Existing finite receipts have no expiry or
acknowledged-release contract, so they and their proof stay pinned indefinitely.
Similarly, changing-value delta computation can legitimately extend original
processing ancestry forever even without receipts. Such workloads can still hit
capacity and receive backpressure. The collector does not rewrite manifests,
expire receipts, reinterpret exact units, release accounting reservations or
invent a lineage-reset operation to hide that limit. Model audit has a separate
[host-acknowledged archive contract](model-authorization-archive.md): authorization
rows and their reachable derived proofs are durably archived before selected
local audit roots are removed. This collector itself still never removes model
audit or financial receipts. Hosts without that archive setup can exhaust the
audit cap; instance retirement and receipt-expiry contracts remain separate work.
Existing fixed-size hotness counters and change windows are left unchanged.

## Transaction, upgrade and rollback

Both providers advertise the additive `question-reachability-gc/1` extension.
The census, retained-root verification, selected ledger deletes and owned-edge
cleanup run in one namespace-locked and scheduler-locked UoW. PostgreSQL uses its
existing real advisory locks; SQLite uses its existing `BEGIN IMMEDIATE` writer
transaction. The persisted clock high-water is sampled only after those locks.
Edge removal is conditional on no remaining record sharing that owner identity.
No scheduler reservation or due projection is removed.

No SQL table, stored schema, migration number, receipt format, deletion journal
or restore-checkpoint format changes. Old adapters remain usable for their prior
operations; this new host method rejects adapters without the extension. The
existing V7 reader/rollback compatibility gates still apply. Disable the host
collector to roll back its operation; already collected unreferenced data cannot
be recovered except from an allowed backup, which must still replay the
independently current deletion journal before opening. A backup cannot supersede
erasure. Tests exercise actual SQLite backup and PostgreSQL `pg_dump` restoration
and current-journal replay before/after GC, preserving minimal tombstones.

## Verification

Implementation tests: `tests/test_question_gc_v7.py`.
Independent safety tests: `tests/test_question_gc_review_v7.py`.

The suites cover actual background cycles; production-cap recovery; full
independent regeneration; indefinite receipt completion; immutable original
lineage; host holds; census/batch limits; unsupported schema/kind/backend;
transport rejection; clock rollback; active successor work and late completion
acknowledgement; same-ID/edge-owner collisions; atomic mid-delete rollback;
model-reference pins without inference calls; removed private page-block lineage;
real backup erasure replay; and a reference committed on a separate connection
while GC waits for the namespace writer lock. General changing-value delta-chain
backpressure is tested as a retained-reference result, not mislabeled as recovery.

Targeted both-provider regression command (use a disposable real PostgreSQL DSN):

```sh
python -m pytest \
  tests/test_question_gc_v7.py tests/test_question_gc_review_v7.py \
  tests/test_question_runtime_v7.py tests/test_question_erasure_v7.py \
  tests/test_question_proof_reuse_v7.py tests/test_question_pages_v7.py \
  tests/test_question_page_patches_v7.py tests/test_question_page_patch_review_v7.py \
  tests/test_refresh_demand.py tests/test_architecture.py
```
