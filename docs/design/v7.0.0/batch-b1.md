# V7-B1 — indexed invalidation and proof foundation

Status: admitted-L1 foundation implemented and independently verified.
Generic QuestionView execution remains a later B3 responsibility.

Baseline is B0 main `2c9974bac1e052efe4284738a2d1c7e51ca81de1`
([PR #4](https://github.com/agent-memory-lab/agent-memory/pull/4)).
B0's strict question contracts stay default-off. This batch improves the existing
admitted-L1 language/parent/page lifecycle; it does not expose project QuestionViews.

## Changes

- Reverse metadata subscriptions for candidate sources, canonical slots, query and
  authority controls, configured parents, and declared conservative scope fallback.
  Routing has separate `route:` namespaces and is never published as source evidence.
- An always-maintained scope barrier, existing slot barriers, per-subscription
  generation proof and a scope-locked index gate. Empty queries install their
  subscription before their census; publication still uses the existing atomic CAS.
- Normal invalidation uses indexed point/reverse lookups. Bounded registration,
  migration and physical erasure may enumerate metadata explicitly. Unknown source
  coverage and document changes use a metered, bounded fallback; unrelated precise
  slot writes do not invalidate every result.
- Source/record/retention and derived writes own mutable values before awaiting.
  Candidate headers derive from the canonical persisted payload, including tuple
  members normalized by JSON, rather than from a divergent caller-owned value.
- New UoW header point/census methods are required explicitly. Unsupported adapters
  fail closed. Scope/event/slot admission identities remain immutable.
- Live erase and authenticated offline restore replay scrub routing/subscription
  metadata, reconcile authoritative headers, fence old proofs, and require backfill.
  Cleanup must include every projected scope whose admissions were actually changed,
  while leaving unrelated scopes alone.

## Upgrade, old receipts, and rollback

This is a coordinated upgrade: drain/stop old writers before deploying the new
hooks. Application metadata cannot make an old binary observe a new contract.
The existing ledger and typed-edge tables are reused; no new SQL table migration
is required in this batch.

The first gated transaction rebuilds bounded subscription and candidate routing
from authoritative definitions/admissions. The cutover and its conservative
barrier commit together. Failure rolls back the index and proof together. A
candidate census exceeding 4,096 headers selects explicit scope fallback; it
cannot claim a partially built index is precise.

Old current heads become stale and old in-flight snapshots fail CAS. Completed
legacy exact receipts retain completion; a different later unit does not complete
an old exact target. Existing wire schemas are unchanged. Two reserved internal
proof keys are distinct from canonical admission slots and are excluded from
published query-provenance edges.

For existing interval history, cutover retires open coverage at its last proven
stored boundary and marks the unobserved interval uncertain. It never creates a
new historical endpoint using an unrelated process wall clock. Already sealed
historical evidence and current security checks remain intact.

Rollback requires stopping new work first and preserving authoritative deletion
barriers. Do not run old and new writers concurrently. Old heads carrying new
proof coordinates are not usable by an old exact comparison; rebuild through the
chosen supported runtime before serving. Do not discard current deletion journals
when restoring an older content backup.

## Scope of evidence

The new tests cover first-registration/write races, empty snapshot invalidation,
old/new slot and source membership, denied definition-enumeration hot paths,
precise unrelated-write behavior, metered fallback, atomic backfill, ownership
across real PostgreSQL lock waits, and live/offline-replay routing erasure.
Independent review also reopens an actual pre-B1 SQLite database under B1 to
check old heads, old finite receipts, old snapshots, and historical cutover.

T03/T04/T14 remain IN_PROGRESS. Q7-01–04/24/30 evidence is a foundation slice.
Dynamic cross-project entity membership belongs to B3's trusted domain bridge.
Received-source/publication-manifest completeness is still unsupported: an empty
admitted-L1 census cannot prove a source processing target has closed. Generic
historical question execution is also disabled. Existing locale-history tests
are inherited regression evidence, not proof of arbitrary project history.

## Final verification

- Final affected matrix: **1,093 passed, 5 skipped**, on SQLite and real PostgreSQL
  17.11/pgvector 0.8.0. The five skips are PostgreSQL-only asynchronous races under
  the SQLite parametrization; their real PostgreSQL variants passed.
- An earlier full repository/package run found one regression (2,909 passed,
  5 skipped): a writer's persisted-payload reread re-entered a public snapshot
  instrumentation hook. It was repaired with a dedicated exact SQL point-read;
  unchanged admission concurrency assertions and the final affected matrix pass.
- Independent review confirmed projected erasure/restore cleanup, persisted-JSON
  source routing, and a real pre-B1 SQLite database cutover. No outstanding blocker
  remains within this declared scope.
- GitHub's PostgreSQL CI now includes all derived tests, new B1 tests, and relevant
  purge/restore regressions, so these contracts do not silently run only on SQLite.

See [source hashes, exact commands and execution notes](validation-b1.json).
Test counts describe each run independently and must not be added together.

