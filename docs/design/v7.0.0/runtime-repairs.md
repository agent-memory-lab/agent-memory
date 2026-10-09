# V7 runtime repair scope

This repair starts from GitHub `main` at
`685742df8a9dd26a5e960f43c61ae85295e82c38`, including the separately recorded
[real local Ollama smoke](ollama-smoke.md). Its purpose is runtime correctness
and bounded lifecycle completion, not a new claim about memory quality or cost
savings. The frozen design and all inherited AM61 task statuses are unchanged.

## Cache admission and current cost

The exact answer cache reserves a durable publication slot before a money
reservation or provider dispatch. Both ready entries and unexpired reservations
count against the configured live capacity under the existing namespace lock.
The shared execution token fences publication and cleanup. Failed pre-dispatch
admission makes no provider call and creates no new financial obligation.
Abandoned reservations expire with their execution lease; this does not release
an already dispatched call's unknown financial debt.

Final delivery reads the current financial ledger under its lock, before taking
lifecycle locks and recording the final authorization. The public `cost_status`
values remain `unknown` and `measured`; a cached answer can therefore change from
unknown to measured after a separately verified settlement. This reports current
settlement, not a newly measured provider response or an estimated cost.

Existing cache headers remain readable through a bounded legacy call-ID lookup.
New headers carry the reservation key for direct lookup. No SQL migration is
required. Stop and drain governed model execution before downgrading: older
writers do not understand the new reserved-header state.

## Audit admission and bounded archival

A cold generation atomically reserves its first-delivery authorization slot along
with the dispatch record under the same 4,096-row audit cap. Known insufficient
headroom is rejected before reserving money; the dispatch transaction remains
the authoritative race-safe check. Cache hits cannot steal the held slot. Only
one successful same-call delivery consumes it. A failed/cancelled waiter cannot
release a slot another waiter still needs; unused slots have bounded expiry.
Consumed audit records and dispatched financial debt are never expired by that
cleanup. A concurrent loser can retain a released financial attempt record, and
additional deliveries still require their own audit capacity.

The 4,096 authorization boundary is retained. The explicit host API exports a
bounded batch of authorization records and reachable derived proof, then requires
durable archive storage and an independently pinned checkpoint/receipt before
the acknowledged local rows can be removed. A hash alone is not the archived
evidence. No external archive is contacted automatically.

See [the archive contract](model-authorization-archive.md) for age/recent-row
retention, finite export limits, exact acknowledgment, crash recovery and
erase/restore obligations. Financial calls, debt and receipts are separate and
are never deleted by this operation. Their existing finite limits, exact receipt
retention, source retention and original generation ancestry remain constraints;
this is not an unlimited-retention service.

## Background page maintenance

An enabled `RefreshHost(service.queue, ...)` maintains already-published finite
project pages using the existing shared scheduler. Initial registration and
publication remain trusted host actions. Page work uses its own processor route
inside that same queue, with parent dependencies, bounded demand, fairness,
aging, shared resource reservations, leases and atomic coverage publication.

Parent proof/content changes and time boundaries create maintenance work.
Unavailable cold parents can receive finite prerequisite demand without silently
changing their long-lived refresh policy. Invalid pages refuse reads until a new
valid publication; maintenance never bypasses permission or erasure checks.
Startup backfill is bounded by the existing finite registered-page limit.

See [page background maintenance](page-background-maintenance.md) for setup,
policy configuration, restart behavior and rollout/rollback restrictions. There
is no second queue, generic page composition, automatic raw-source extractor,
historical question API, free-form page editor or expansion to L3 in this repair.

## Verification and remaining acceptance

The frozen repaired source passed 4,073 repository and package tests across SQLite
and real PostgreSQL 17.11/pgvector 0.8.0 with zero failures, errors or skips.
Two actual pre-V7 upgrade/quiescent-rollback probes also passed. All six wheels
and sdists built; isolated noneditable install smoke, installed-source equivalence
and complete source/archive scans passed. Local third-party dependencies were
seeded from the existing environment; hosted CI separately checks clean dependency
installation. Final-head hosted CI must pass before merge.

Verification results are recorded against the final repaired source in
`validation-runtime-repairs.json`; earlier stage counts are not reused or added
together. Synthetic tests use controlled model ports. This repair does not make
paid model calls or repeat the real Ollama run.

T02/T12/T13/T15 remain IN_PROGRESS. Licensed-domain gold, actual extraction and
conditional/time quality, independently judged comparisons, measured whole
cost and production-benefit acceptance still need their own evidence.
