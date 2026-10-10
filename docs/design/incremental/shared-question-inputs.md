# B1: transaction-local shared QuestionView inputs

This batch changes deterministic maintenance, not extraction quality, model routing,
or the meaning of a complete answer. The registered owner, status, commitments and
risks views already make **zero model calls**. PR #18 is outside this change.

## Delivered boundary

`QuestionService.snapshot_many(tasks)` and `publish_many(tasks, snapshots, prepared)`
accept one to eight **already-leased** ordinary question refresh tasks. Their per-job
lease, definition, input, output and head comparisons are the same ones used by the
single-task path. They commit or roll back as a batch. The caller still completes
or fails each lease through the existing queue. A final batch check rereads each
fenced durable job/execution and validates both rows' lease and task-expiry bounds
after the last await and host guard. Heartbeat renewal is supported even though it
does not replace the caller's task; caller-supplied task timestamps cannot extend
permission. No new queue, worker reservation, quota bypass, persistent cache, or
autonomous batching policy is added.

`read_many(question_ids, actor=...)` is an optional bounded read operation. Existing
single reads also reuse structural headers within their transaction. Batching is
host-facing; model transports do not accept arbitrary census inputs or proof tokens.
Sequential transactions do not share any retained body or authorization decision.

For example, after obtaining compatible leases through the existing queue:

```python
tasks = [lease.task for lease in leases]
snapshots = await service.snapshot_many(tasks)
prepared = [service.prepare(snapshot) for snapshot in snapshots]
results = await service.publish_many(tasks, snapshots, prepared)
for lease in leases:
    await service.queue.complete(lease)
```

An immutable common qualified input retains frozen typed facts and serializes mutable
grant/manifest maps to private JSON strings. Each view gets its own independent maps,
original per-job snapshot token, output, certificate and provenance. Capturing a
four-view batch and capturing its four jobs independently produce equal complete
snapshots at identical coordinates. A different transaction always builds again.

### Bindings and safety

Structural candidate-header enumeration is keyed by exact scope, project, domain
contract, current project/unbound/wildcard/fallback frontier and retention epoch.
It is independent of wall-clock time and is never an authorization decision.

Qualified-input sharing additionally binds registration, principal, reader audience,
purpose, authority and version floor, host context, admission/projection policy,
refresh policy, source basis, publication requests/manifests, both exact time
coordinates, and the complete freshly observed candidate/source/grant proof. The
per-job snapshot token is rebound into its existing snapshot identity, not dropped.
Different projects, source bases or refresh policies do not share qualification.

Every consumer still reads and checks current source/erasure metadata, grants,
authority, epoch, frontier, registration, context and clock. Original generation
lineage remains an independent guard; equal values cannot wash revoked evidence.
All existing before/after-body guards remain. At a batch's final boundary each exact
common current-and-original proof group is scanned **fresh**, after independently
rechecking every member's captured head, registration and subscription. Every
member then gets a synchronous time/context/registration check. This final grouping
is scoped to that one guarded operation, not a saved successful authorization.

New entrants, empty queries, unknown/unbound/wildcard candidates, moved-out routing
hits, publication closure, unsupported/legacy metadata, source removal, revocation,
clock rollback and expiry retain their existing conservative semantics. No field
filter is applied to the full candidate or processing-source census. In particular,
an unresolved risk can still make an owner answer incomplete.

## Semantic invalidation is not proof maintenance

New optional contracts are `question-semantic-readset/1` and
`question-candidate-field-effect/1`. Host registration pins each closed operator's
fields. Commitment rows include deadlines even for non-overdue questions; status
includes phase only when required; risks include their registered rule predicates.
A future unsupported operator obtains a wildcard readset rather than narrower
unproved dependencies.

A narrowed effect requires two consecutive fully reviewed versions, exact scope,
contract and stable membership, and field evidence spanning the complete assertion
interval. Missing support, pending/rejected/withdrawn candidates, initial/deleted
records, membership transitions, historical unbound routes, malformed or legacy
metadata and every safety write fall back to semantic invalidation.

A proved status-only rewrite can therefore leave owner `semantic_dirty` false.
It **still** sets the ordinary dirty flag and `proof_dirty`, advances the project
barrier, records refresh responsibility and invalidates the current certificate.
The complete-project coverage and processing provenance contracts are unchanged.
Owner/status delta signatures now read their declared semantic fields; all global
value, completeness, support and time checks still run. Existing separate value,
structure, support, generation, validation and safety digests are reused.

Diagnostics separate `semantic_dirty_count`, `proof_dirty_count` and
`predicate_disjoint_count`. `QuestionService.input_work` exposes transaction-work
counters: candidate scans/reuses, qualified builds/reuses, source-proof reads and
service authorization checks. These are host diagnostics, not claims that a cache
hit is permission or that every database operation is counted by one scalar.

## Frozen comparison and limits

The instrumented test was run against frozen baseline tree
`ddf0a97d0c7cf2934c31f2043d577ef02a726a09`, local commit
`04c7e1d002568925edfbf8e7335774c57756ca21`, verified tree-identical to remote main
`c5424dc`, and then against this implementation. Both use the same eight reviewed
inputs, four registered answer contracts, fixed semantic coordinates and SQLite.
The read phase uses an advancing host clock, not a frozen-clock cache illusion.

[Recorded per-phase evidence](validation-shared-inputs.json) includes setup/ingestion,
registration, queue request/claim, snapshot, preparation, publication, lease
completion and guarded reads. The ordinary kernel initialization/teardown is outside
both measurement windows. No paid model, download or remote service is involved.

| Measured work | Baseline | Shared |
| --- | ---: | ---: |
| Snapshot candidate census scans | 12 | 1 |
| Snapshot source body / candidate body reads, each | 32 | 8 |
| Snapshot semantic review validations | 32 | 8 |
| Publication source body / candidate body reads, each | 32 | 8 |
| Full measured workflow SQL SELECTs | 5,258 | 4,990 |
| Full measured workflow Python calls | 2,924,054 | 2,904,774 |
| Full measured workflow support-range evaluations | 72 | 32 |
| Advancing-clock read Python calls | 533,369 | 594,351 |

This demonstrates removal of repeated qualification/DB work. It does **not** establish
a general latency win: setup and preparation have additional contract work, and read
Python calls increase. Fresh safety scans remain a significant cost. CPU timings in
the JSON are a single profiled sample, not statistical performance evidence. No
improvement in real-world accuracy, extraction recall, or model cost is asserted.

Answer schema, domain fingerprint, statuses, rows, matched IDs, reasons, source
basis, exact citations and value digests match the baseline. Full certificate IDs
and support digests may legitimately differ because provenance includes the new
routing metadata and per-job proof token. This is not a relaxed answer contract.

Reproduce by running `tests/test_question_shared_benchmark_v7.py -k sqlite` with
`AGENT_MEMORY_SHARED_BENCHMARK_OUTPUT` pointing at a result JSON. Run once with
`PYTHONPATH` selecting the archived baseline's `src`, `packages/postgres/src` and
`tests`, and once selecting this checkout. Compare the `answers` values exactly
and retain all `stages`, including setup and read costs.

## Compatibility and verification

No SQL schema or public answer schema changes. Header schema `/3` accepts the
optional versioned field effect; old headers and registrations without these
contracts use conservative invalidation. Newly written effects take effect lazily;
no startup full-table rewrite is needed. The keyed-delta operator version advances
from `/1` to `/2`, so an old optimization baseline is rebuilt rather than trusted
under a changed readset. Existing coordinated-writer-upgrade requirements remain.

Focused tests exercise frozen answer parity, policy/project/source-basis isolation,
all compatibility-key coordinates, immutable sibling maps, transaction ownership,
new entrants and incomplete answers, predicate-disjoint invalidation, late grants,
source loss, epoch/frontier changes, expiry, host/context changes, captured-head
changes, atomic publication rollback, heartbeat renewal, late durable lease/task
expiry, mutated caller task timestamps, final execution fence/version/unit binding,
existing original-lineage security, erasure, project indexing and delta/oracle
equivalence. PostgreSQL behavior tests are present
but require the existing explicitly configured disposable test database; this local
run does not claim live PostgreSQL validation or a full repository-suite pass.
