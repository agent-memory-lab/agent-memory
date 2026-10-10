# Verification discovery and host progress

This repair changes orchestration and operational bounds, not admission authority.
Unknown verification remains pending. Only the existing typed, authorized publisher
can qualify or refute a candidate; publication, task completion, source freshness,
erasure and lease fencing remain one transaction.

## Discovery and backpressure

The host reads at most 128 unqualified pending/contested candidates in its exact
scope per cycle, ordered by candidate ID. A durable keyset cursor survives host
restarts and wraps at the end of a sweep. A wrap can issue one additional empty
indexed query. Ancestor, other-scope, deleted, qualified, project-reviewed and
terminal admission records are excluded before the query limit. SQLite and
PostgreSQL use additive partial indexes over authoritative admission rows, so
ordinary admission updates and erasure update eligibility atomically without a
separate application-maintained projection or admission payload migration.

Unmatched or ambiguous tools are skipped for this sweep; a failing individual
candidate is revisited after the cursor wraps and cannot starve later candidates.
Scheduling still rechecks the candidate version, exact scope, live authority,
current source and erasure state in the queue's transaction. The cursor itself
is scrubbed by erasure and cannot restore a candidate ID after that record is
forgotten during discovery. Empty, live and erased cursors are known,
non-collectible roots in question garbage collection; discovery advancement and
source/scope erasure own their lifecycle. Merely running discovery must not
make the question collector defer on an unsupported record kind.

The configured verification capacity (1–4,096; default 128) bounds only pending,
retry and running tasks. When full, discovery stops before advancing past the
blocked candidate. Existing verification and refresh work still run, releasing
active capacity as tasks terminate. Previously admitted work can drain even if a
restarted host lowers its configured capacity. There is no age-based dropping or
conversion of pending assertions into accepted or rejected facts.

## Terminal history and retention

Completed, cancelled, dead and erased verification request IDs are terminal.
Completed `unknown` findings do not cause an automatic retry every sweep. A new
candidate version, tool fingerprint or explicitly supplied new request ID permits
a fresh attempt. Terminal receipts do not consume active capacity, and normal
scheduling, claiming and operational metrics never load terminal history.

Receipt retention has no automatic TTL or fixed history-count deletion limit.
Storage therefore grows with the number of distinct requests, independently of
the finite active-work bound. Hosts must provision storage for this durable
idempotency history; this repair adds no archive or arbitrary receipt-deletion
API. Source/scope erasure scrubs receipt bodies to erased markers, retaining the
request identity. Erasure visits the finite receipt history in 128-row keyset
pages inside the existing deletion transaction, rather than failing at the
unrelated 4,096-row generic ledger read cap. Thus peak receipt materialization is
bounded, but total erasure time/transaction duration can grow with history.

## Isolated stages and diagnostics

After shared repository initialization succeeds, extraction, discovery,
verification and refresh have separate exception boundaries. A stage failure
cannot prevent independent later stages from running. Cancellation still
propagates. `run_once` adds `stage_errors` and `verification_backpressured`; failed
extraction/refresh have null results and a failed verifier returns `failed`.
Only fixed error codes are exposed, never callback messages or source bodies.
A successful subsequent cycle clears previous stage errors.

`memory-host-metrics/2` replaces version 1. The `verification` counts now describe
active states only. `verification_backlog` reports configured capacity, active
count, oldest active age in seconds, capacity backpressure and the terminal
retention policy. Age starts at `created_at`; legacy tasks use `due_at` as an
explicit compatibility fallback. Backlog age is informational, never authority
or an expiry instruction. Metrics separately expose whether the last discovery
cycle encountered backpressure and the most recent stage errors.

## Final publication boundary and compatibility

The completion write precedes the final epoch/authorization and durable clock
checks. The host rechecks authorization after that clock write, since the clock
write itself can suspend. This final authorization callback is the last awaited
authority operation. Immediately afterward a synchronous check samples current
time, lease expiry and the registered tool fingerprint before transaction exit.
That guarded transaction is the publication linearization point; authority
changes after it do not retroactively invalidate a committed disposition.
Expiry, clock rollback or changed controls at the final guard roll back both
task completion and any fact publication, including `unknown` findings.

A separate post-commit clock checkpoint persists the maximum sampled time,
including time advanced by the final authorization callback. A newer durable
floor written by another worker already covers an older checkpoint and is not a
publication failure. Failed publication attempts to persist its sampled
high-water independently after rollback. The durable one-attempt fence below
prevents replay even when that independent checkpoint is unavailable.

If a post-commit checkpoint fails for another reason, the return value is
`committed_<disposition>_clock_checkpoint_pending`, explicitly distinguishing the
committed result from degraded clock durability. Backlog metrics expose only the
fixed `verification_clock_checkpoint_failed` code. The live queue keeps the
pending floor and blocks further scheduling, claiming and publication until a
retry stores it (or finds it already covered). This local retry state is not
durable: a crash after publication and before checkpoint recovery can lose that
last sampled floor. Hosts must recover the checkpoint with a trustworthy clock
before restarting/resuming from this degraded outcome. This repair does not
claim atomic fact-commit/final-clock-checkpoint durability across that crash
window. Committed terminal receipts still cannot be reopened as running leases.

### Durable one-attempt fence and explicit recovery

Before any publisher work, a separate transaction embeds a unique
`publication_attempt` token with state `pending` in the running verification
task. The fact transaction requires that exact private attempt token and the
original lease token. A lease can begin publication only once. A concurrent
caller, process restart or wall-clock rollback cannot reuse it. A failed marker
write never starts the publisher; if the marker committed but the process died
before publication, the durable pending marker remains.

After a failed publication, only successful clock checkpointing followed by a
separate exact-token update changes that attempt from `pending` to `fenced`.
A new claim may clear a fenced attempt and mint a new lease only after the old
lease deadline. `run_once` retains attempted leases/deadlines instead of making
an immediately claimable retry. A pending attempt is never reclaimed merely
because its lease expired, and a new request ID cannot bypass that unresolved
attempt for the same candidate version. On checkpoint or recovery-write failure,
publication raises `verification_commit_fenced_recovery_required`; it does not
claim that the missing clock observation was persisted. The marker remains
pending and the original fact/task transaction is not retried automatically.

The trusted-host method
`recover_publication(old_lease, trusted_clock_at=aware_datetime)` is an explicit
recovery assertion, never a model tool. The host must establish a trustworthy
clock floor that covers the interrupted attempt, rather than copy a restarted
worker's possibly rolled-back wall clock. The floor must be at least the stored
lease deadline and any locally remembered higher sample, and no later than the
current trusted clock. Recovery requires the exact old lease token, the same
pending/fenced attempt, current candidate/source/epoch/tool controls and current
authorization. In one locked transaction it observes the maximum of the attested
floor and current clock, marks the old attempt fenced, and performs a final
no-await time/tool check after authorization. It returns `recovered`; it never
publishes a fact or clears another lease's attempt. The next successful claim
mints a new token. A changed source or revoked authority cannot be overridden by
this API. Attestation is trusted host input, not something the runtime can infer
or independently prove.

Markers are embedded in existing task rows: they create no new per-attempt rows,
remain within active capacity, survive ordinary backup/restore, are retained by
question GC, and are scrubbed by source/scope erasure. Backlog metrics include
`publication_attempts_pending` (including currently executing attempts). Hosts
must investigate pending markers that remain after interruption; the safety
choice is blocked capacity rather than an unsafe retry. Completed supported,
refuted and unknown outcomes stay terminal, including after a failed post-commit
clock checkpoint. This per-attempt replay guarantee does not imply general
cross-process global clock monotonicity during an arbitrary checkpoint outage;
the explicit successful-publication crash limitation above remains.

Providers used by `MemoryHost`/`DomainVerificationQueue` need the optional
`verification_candidates` and `verification_active` unit-of-work methods.
SQLite initialization creates the indexes; PostgreSQL migration
`020_verification_discovery.sql` creates equivalent indexes. No existing
admission or task payload is rewritten; new tasks add optional `created_at`,
and publication adds the optional embedded attempt fence.
Drain old host/verification workers before rollout or rollback. Old workers still
count terminal receipts toward queue capacity and use the obsolete discovery
limit, so mixed binaries cannot provide the repaired progress guarantee.

## Focused regression coverage

`tests/test_memory_host_backlog.py` is parametrized for SQLite and real PostgreSQL.
It covers historical and ancestor rows, exact scope, indexed bounded pages,
restart/wrap, active capacity and age, 4,200 terminal receipts, unknown-result
idempotency, independent failed stages, and erasure/cursor races. The existing
host/native project tests preserve qualified-only question publication.
`tests/test_domain_verification.py` additionally covers lease expiry and current
host changes during final authorization/completion writes, for both supported
and unknown findings, and persisted final clock observations.
`tests/test_verification_attempt_fences.py` covers checkpoint failure/restart,
concurrent duplicate publication, actual backup of a crash-after-marker state,
marker-write rollback, preserved retry deadlines, explicit recovery CAS and
current authorization/source/tool/scope guards. Real PostgreSQL
execution requires the existing disposable test DSN; local runs without it do
not establish PostgreSQL runtime parity.
