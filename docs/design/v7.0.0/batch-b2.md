# V7-B2 — durable coalescing, fair resource admission, and refresh host

Status: the opt-in current Observation/parent/page scheduler slice is implemented.
Question-runtime integration remains B3; monetary/model admission and measured
promotion remain B5. This is targeted batch validation, not the B6 full release gate.

Implementation starts at B1 local `c53fff1` and preserves B1's later `3c79eeb`
PGDG client setup. Initial implementation is `b1509a9`; reviewed runtime fixes are `06e005c`,
retained signed-restore regression tests are `9fbbaa1`, and the
late-heartbeat completion race repair is `30ecfe2`. PostgreSQL startup
serialization was added in `eef725b` after a real concurrent initializer deadlock. The accompanying
[validation record](validation-b2.json) pins the final source files and test evidence.
No deployment or remote configuration change is part of this batch.

## Delivered behavior

- Durable per-instance compatible demand coalesces an unsealed dirty batch. A
  request or claim seals exact obligation identities. Claimed inputs, policy, and
  fencing generation are fixed; new inputs retain distinct successor responsibility.
  Only the processor's checked atomic publication discharges claimed obligations.
- New coverage receipts are finite exact-unit sets. A compatible full successor
  may cover them; they never silently become an unbounded latest-state target.
  Legacy exact receipts retain their original unit and completion meaning. Neither
  kind of completion proves current freshness or caller permission.
- `on_change`, `on_demand`, `scheduled`, and `hybrid` policies persist debounce,
  first-dirty max-wait, semantic time boundaries, and optional schedule due times.
  Continuous writes cannot restart max-wait. A missed deadline stays visible after
  late successful publication, using the actual final checked publication time.
- Cold invalidation remains transactional and does no eager computation. Explicit
  readers and hot descendants create bounded shared demand; a temporarily activated
  cold parent keeps its cold policy after completion. Managed legacy exact requests
  join the same admission path rather than silently remaining unscheduled.
- One persisted limits configuration governs all participating foreground/background
  requests: pending/running caps at global, tenant, and instance levels. A mismatched
  worker fails closed. One running execution per instance, durable tenant turns,
  per-tenant ordering and priority aging prevent tested burst starvation.
  Deferred candidates may move directly into a reserved running slot in the same
  transaction. They do not need a transient pending slot which a busy tenant could
  continually refill. Both persisted pending and running hard caps remain intact.
- Authorized reuse counters are bounded and erased with their scope. Temperature
  recommendations require an observation window, minimum samples, residence and
  hysteresis; unknown counterfactual savings cannot justify promotion. Applying
  a policy is an explicit host action. No real-world cost reduction is claimed.
- The host polls the capability-filtered indexed due selector, so notification hints
  are optional. It claims only free execution slots, renews leases, bounds attempts,
  total active age and no-progress time, and drains active work on graceful stop.
  Lease renewal is not progress. A heartbeat queued behind atomic publication
  validates the exact immutable completion and does not cancel successful work;
  genuinely failed renewal before publication still cancels pending work.
  Repeated explicit recovery is distinguishable from
  an automatic unlimited retry, including dirty writes arriving after exhaustion.
- Durable UTC high-water survives process recreation and is included in signed
  restore checkpoints. Monotonic/wall-clock divergence degrades host health. Managed
  reads, snapshots and publications observe time before the body transaction;
  failed authorization cannot roll that initial observation back. Error paths persist
  later observed expiry after the failed body/lease transaction, including stale
  heartbeat and failure-handling attempts. Input authorization
  is rechecked before each source body, and final delivery/publication rechecks time.
  Expired source grants, leases, and semantic boundaries cannot be revived by the
  tested backward-clock retries. Unmanaged pages of managed parents inherit the guard.

## Supported adapter and limits

`RefreshProcessor` keeps template business logic out of scheduling. The delivered
`ObservationRefreshProcessor` wraps the existing complete current census and its
publication CAS. It does not manufacture continuous publication-frontier coverage,
project membership proofs, historical question execution, or arbitrary processor
compatibility. Unknown adapter keys are filtered before due selection and cannot
consume another processor's responsibility.

The limits are deterministic work/concurrency units, not dollars, model token
reservations, or dispatch settlement. The optional `allow_fallback` policy field
never grants permission to bypass admission. Bound definitions, requests, lineage,
input and revision capacity are inherited from the supported derived runtime;
capacity errors remain explicit. Automatic measured-benefit policy tuning, unbounded
workloads and production throughput/latency promises are outside this slice.

## Opt-in host use

The existing application must first initialize its upgraded repository and trusted
`ObservationService`. Configure only the definitions the host wants managed:

```python
from agent_memory.operations.refresh_demand import (
    ObservationRefreshProcessor, RefreshDemandQueue,
)
from agent_memory.operations.refresh_host import RefreshHost
from agent_memory.operations.refresh_policy import RefreshLimits, RefreshPolicy

queue = RefreshDemandQueue(
    (ObservationRefreshProcessor(service),),
    limits=RefreshLimits(global_running=8, tenant_running=2),
)
await queue.configure("language", RefreshPolicy(mode="on_demand"))
receipt = await queue.request("language", dedupe_key="request-1", actor="alice")
host = RefreshHost(queue, worker_id="host-1")
await host.run_once()
status = await queue.status(receipt["target_id"], actor="alice")
# Independently authorize/current-check the answer with service.read(...).
```

For a long-running host, await `host.run()` in the host's task and call `host.stop()`
to stop new claims while active work drains. Inspect `host.health`. A crash requires
no lost in-memory hint or completion callback; a compatible restarted host polls
persisted responsibility. Budget shortage and unsupported/expired configuration
are explicit deferred/dead outcomes, not permission to return an old answer.

## Migration, erase, restore, and rollback

Stop/drain old writers and workers before coordinated upgrade. Existing live legacy
leases prevent adoption until drained; cold-parent auto-adoption has the same guard.
Old binaries cannot be made safe by new metadata and must not run against opted-in
managed definitions.

SQLite initialization adds scheduler contract, due, fairness and reservation tables,
plus the durable clock column for pre-clock development backups. PostgreSQL uses
migration `017_refresh_schedule.sql`. Core, pgvector, and ontology startup writers
share a schema-scoped transaction advisory lock before any DDL; core migration
and backfill retain it until transaction completion. This prevents independently
starting processes from deadlocking on additive lock upgrades. Indexed due/lease projections are maintained
inside the same authoritative UoW as demand, publication, and deletion. A coordinated
new worker may not change the persisted global limit contract silently.

Erasure scrubs demands, executions, publications, policy/hotness and corresponding
SQL projections/reservations in every affected projected scope; unrelated scopes
remain intact. Coverage receipt tombstones contain no content. The wall-clock floor
and authoritative deletion barriers survive. Rollbacks of deletion and reservation
changes are tested as one transaction.

Before restoring an old backup, keep writers stopped, initialize the current schema,
and replay the independently pinned current signed deletion journal. Journal v2
adds the current scheduler clock floor; legacy v1 remains byte-compatible when no
floor exists, but cannot replace a required v2 floor. Forged/malformed checkpoint
fields fail closed. Actual SQLite copy and PG17 dump/restore tests prove the restored
scheduler does not revive erased content or lower the current clock floor.

To roll back runtime code, stop the new host and all writers first, preserve durable
responsibility and current authoritative erase/clock checkpoints, and drain or
explicitly audit/retire managed work before using an older binary. There is no
online downgrade/unmanage API in this batch. Do not delete scheduler metadata to
make an old worker claim managed work. A restored pre-upgrade backup is not current
until the current deletion and clock barriers are replayed.

## Acceptance evidence and remaining work

The validation JSON records exact commands, counts, hashes and skips. The suite
includes real SIGKILL before/after publication commit, both with and without later
successors, fresh-interpreter recovery, owner fencing, lost notifications, future
persisted due, backward-clock restart, and two independent processes racing the last
shared running slot. Each scenario runs against SQLite and real PostgreSQL 17.

| Responsibility | B2 evidence | Remaining boundary |
| --- | --- | --- |
| Q7-05–07 | `test_refresh_demand.py`, `test_refresh_scheduler_boundaries.py` | QuestionView integration in B3; full release combination in B6 |
| Q7-08 | `test_refresh_clock_delivery.py`, `test_refresh_scheduler_clock_storage.py`, process tests | Operational clock correction requires trusted host action; no fabricated time |
| Q7-09–10 | Cold demand, cold parent, bounded reuse/hysteresis tests | Measured production policy tuning and domain workload evidence |
| Q7-11 | Shared slot races, tenant rotation, sustained-input pending-limit counterexample, aging | Monetary/model limits and production SLO calibration in B5 |
| Q7-12 | Capability-filtered storage and processor mismatch tests | Future adapters must independently meet the processor contract |
| Q7-29 | `test_refresh_scheduler_process.py` actual process death/restart | Final combined B6 failure matrix |
| Q7-30 slice | Storage erasure, projected scope, actual backup and signed-clock replay | Full V7 object integration remains T14/B6 |

Independent review found and prompted fixes for durable clock re-entry, late-deadline
reporting, explicit recovery after exhausted dirty work, pending-cap starvation,
and a heartbeat/publication completion race reproduced with deterministic barriers.
Regression tests preserve each counterexample. Tests use synthetic deterministic
language fixtures; no real model, sensitive production data, or production load is
used. T05–T07 remain IN_PROGRESS in the overarching ledger until their cross-batch
QuestionView/resource-governance responsibilities are integrated and accepted.
