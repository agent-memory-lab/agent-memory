# Bounded project-page background maintenance

This additive runtime repair closes the published L2 page validation loop. It does
not add a queue, model call, historical QuestionView mode, or generic raw-text
extraction. SQLite and PostgreSQL use the existing durable refresh ledger,
namespace locks, indexed discovery, admission limits, fenced leases and
`RefreshHost` runner.

## Host setup and policy

A trusted host still registers and initially publishes each finite page. Once
published, the page participates in the same queue as its QuestionView parents:

```python
from agent_memory.operations.refresh_host import RefreshHost
from agent_memory.operations.refresh_policy import RefreshPolicy
from agent_memory.operations.worker_tasks import WorkerLimits

await service.pages.register(
    "project-overview", ("project-a:owner", "project-a:status"), readers=(actor,)
)
# Parents must already be ready; publication never performs hidden parent work.
await service.pages.publish("project-overview", actor=actor)

# Optional host override. Existing configured policies survive restart.
await service.queue.configure(
    service.pages.key("project-overview"),
    RefreshPolicy(mode="on_change", max_age_seconds=3600, max_attempts=3),
    processor_key=service.page_processor.key,
)
runner = RefreshHost(
    service.queue, worker_id="project-maintainer",
    limits=WorkerLimits(max_concurrency=2),
)
await runner.run_once()  # One bounded batch, or await runner.run() in a host task.
# runner.stop() requests graceful stop; the host owns task/process lifetime.
```

No thread or daemon starts implicitly. Without a running host, stale reads remain
closed. Existing `pages.publish`, typed patches and `validate_pending` remain
trusted manual maintenance entry points; automatic maintenance does not require
calling `validate_pending`.

Default page policy is `on_change`; parents keep their registered policy. Page
parent subscriptions propagate source/permission changes through the existing
reverse dependency index. Parent publication additionally coalesces exact page
validation responsibility. A page's published `valid_until` is a scheduler time
boundary, including when its parents are `on_demand`. The child requests only
missing/stale parents through the common temporary-parent-demand path, without
permanently changing the parent's policy.

Each page contains at most four registered QuestionView parents. Published pages
share the existing scope limit of 128 scheduler/subscription definitions with
questions and other definitions; they do not receive a second independent budget.
One runner batch claims at most its configured execution concurrency. Global,
tenant and instance quotas, retry delay, priority aging, maximum age, no-progress
budget and failure count are those of `RefreshDemandQueue`. Repeated startup does
not reset an exhausted demand or extend a lease. Deliberate recovery uses a new
explicit finite request through the normal queue.

A bounded direct QuestionView answer targets its own finite receipt's demand, so
its one-step budget cannot be consumed by unrelated page or sibling-parent work.
It still uses the same running admission budget and publication fences.

## Publication and recovery

The page processor uses a distinct `question-page-refresh/1:<digest>` adapter key
inside `service.queue`. The ordinary QuestionView route remains the default for
existing `request`, `configure`, and `status` calls. Existing QuestionView workers
cannot accidentally claim page work with an unsupported payload.

Page work freezes a `question-page-refresh-unit/1` containing definition/epoch,
safety/time generations, exact parent header hashes and the previous page head.
It uses existing `job`, `refresh_demand`, `refresh_execution`,
`refresh_publication` and reservation records, with no second scheduling table.
Before rendering, the worker rechecks the lease, registration, parents, original
processing lineage and exact frozen input. The normal page transaction publishes
immutable blocks/content/certificate/head and discharges only its claimed
obligations atomically. Final authorization, wall-clock, lease and expiry checks
run after storage awaits. A concurrent parent change, manual patch, erase, expired
lease or replaced registration prevents stale publication.

Proof-only parent changes may preserve content and block bytes, but require a new
validated certificate. A source-sensitive answer change rebuilds the finite page.
A manual publication cannot discharge an outstanding scheduler obligation by
retroactively claiming coverage.

`RefreshHost` initialization performs one bounded registration census (at most
128 entries) to enroll previously published pages. Existing `validation_pending`
markers without scheduler demand are backfilled; current pages receive a dormant
time-boundary wake. Existing pending/running/dead responsibility is preserved.
Unpublished, erased, different-context or different-epoch pages are not enrolled.

The existing erase path scrubs page definitions, jobs, demands, executions and
publications through the registered page instance and parent lineage. Completed
page jobs/executions/publications are eligible for the existing conservative
reachability collector after lease/expiry fences, subject to retained receipts,
heads, original generations and explicit holds. No audit or receipt is silently
expired to make capacity.

## Compatibility and rollback

This is an additive typed-record change; no SQL schema migration or public
QuestionView/page wire schema changes. The existing page registration/content/
certificate schemas remain `/1`. Deploy trusted writers and workers together;
use the same persisted refresh limits configuration and original trusted context
when restarting a worker. Backfill is transactional and fail-closed at capacity.

For rollback, stop and drain the new host before replacing it. Preserve the new
ledger records and reservations rather than deleting backlog or rewriting leases.
Older readers continue their page proof checks and older hosts can use manual page
validation, but do not implement the new automatic maintenance route. A rollback
therefore suspends automatic page repair; it must not be represented as equivalent
background coverage. Resume the upgraded host to recover durable responsibility.

## Verification

`tests/test_question_page_background_v7.py` exercises real `RefreshHost` execution
on SQLite and a disposable PostgreSQL database: bounded batches, proof reuse,
cold-parent dependency demand, time-boundary refresh, shared concurrency budget,
restart/expired-lease rejection, erase, age exhaustion, old-page backfill, final
publication fence and conservative garbage collection. A separate interpreter
fixture is killed with SIGKILL immediately before/after the actual page publication
COMMIT; a fresh `RefreshHost` process recovers without partial or duplicate output.
All tests use deterministic synthetic sources and no model calls.
