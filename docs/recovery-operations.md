# Recovery operations: optional SDK/MCP and host-driven lifecycle

This extends the initial recovery/context API. No worker, timer, vector service
or LLM is enabled automatically. The initial `recovery-context.md` describes the
base data model; this document supersedes its old statements that SDK/MCP,
readiness and queue integration are unavailable.

## Embedded and MCP access

```python
from agent_memory.recovery_transport import RecoveryTransport
from agent_memory.mcp import MCPRequestContext
from agent_memory_sdk import EmbeddedMemoryClient

class HostPolicy:
    async def authorize(self, context, operation):
        # Replace this example with host-owned per-operation authorization.
        return context.actor == "trusted-host"

# memory is a recovery-enabled UnifiedMemory, owned exclusively by this host.
transport = RecoveryTransport(memory, HostPolicy())
client = EmbeddedMemoryClient(
    memory.provider,
    MCPRequestContext(memory.scope, actor="trusted-host", can_erase=True),
    recovery_tools=transport,
)
await client.initialize()
result = await client.load_recovery("task-1")
```

`MCPMemoryClient` exposes the same recovery methods. A server opts in with
`create_server(memory.provider, resolver, recovery_tools=transport)`. Initialize
the owned `memory` before serving and close it on shutdown. The added MCP tool
is `memory_recovery(operation, payload)`. All responses wrap their JSON payload
in `result`; receipt responses include a computed `stage`.

Operations: capture, receipt, save, load, compress, load_compression,
validate_compression, enqueue, process_one, retry, complete, expire, cleanup,
stats, forget and resume_deletion. Python helpers use descriptive method names
such as `capture_confirmed`, `save_recovery`, `process_next_capture` and
`forget_sources`. Pass dataclasses or equivalent JSON dictionaries through SDK
helpers. MCP requires JSON objects. No scope can be supplied by tool arguments.

Every operation requires the host authorizer and an exact authenticated scope
match. Erase and deletion resumption additionally require `can_erase`. Do not
grant task models blanket worker, cleanup or write authority. The example
policy is illustrative, not a production authorization policy.

When recovery transport is enabled, legacy mutating endpoints are rejected to
prevent bypassing receipt and deletion gates. Supported legacy reads use the
same gate and authorizer. A separately configured capture sink cannot be mixed
with this mode. Existing non-recovery clients/servers keep their old behavior.

## Readiness without guessing

The local recovery factory now installs `LocalRetrievalReadinessProbe` by
default. It verifies raw evidence, finds current exact-scope claims and calls
the actual provider retrieval path with claim text. It requires the claim ID
and evidence reference in the returned bundle before confirming facts.

Receipt fields distinguish `raw_readable`, `facts_retrievable`, `extracted` and
`retrievable`. No extracted claim means raw evidence may be readable while
facts are not retrievable. A successful probe is not a guarantee for arbitrary
queries or task outcomes. Custom probes remain injectable; old ready-only
probes do not automatically certify the new `facts_retrievable` field.

The probe performs at most eight source retrieval checks. Provider interfaces
may still materialize their current-state view; this is not a guarantee that
arbitrarily large current-state stores have constant memory use.

## Durable asynchronous capture

```python
receipt = await memory.enqueue_capture(
    event_id="queued-1", run_id="task-1", role="user", content="Read report A",
    occurred_at=stable_timestamp,
)
assert not receipt.persisted  # Admission is not source-ingestion completion.
processed = await memory.process_next_capture()  # Host schedules one job.
latest = await memory.capture_receipt("queued-1")
```

Jobs use the optional recovery store and the same sanitized capture sink, not
the older standalone `SQLiteCaptureQueue` database. No old queue is silently
migrated. This is a separate recovery-aware adapter with an explicit worker
entrypoint. Durable job status is queued/processing/done/failed; receipt status
adds attempts and a non-sensitive error code.

Processing jobs are eligible after restart. Failed jobs require an explicit
`retry_capture(event_id)` and are capped at five attempts. Each attempt has a
30-second ingestion timeout. Source ingestion is idempotent; a lost extraction
acknowledgement is not reconstructed by guessing. Successful jobs discard their
stored text, retaining completion metadata. Pending/failed jobs keep sanitized
text until source invalidation or run cleanup. No business tool is executed.

The exclusive ownership contract still applies: one UnifiedMemory object per
scope and no parallel independent processes driving that scope. There are no
distributed worker leases or cross-store atomic commits. A timed-out underlying
thread may finish a source write; the stable envelope and provider idempotency
are required on retry. A receipt error never grants trimming permission.

## Compression application preflight

```python
checked = await memory.validate_compression(summary_id, current_plan)
if checked.accepted:
    # Host still checks its context has not changed and approves replacement.
    replacement = checked.replacement
```

The current plan includes the complete source-segment text, run, recovery
version and budget, so its digest changes when any of those change. Preflight
re-reads the proposal, checks current state and live evidence, and recounts the
serialized replacement budget. It neither changes context nor emits a reusable
authorization token. Do not cache a successful preflight across mutations.

## Completion, expiry and bounded cleanup

`complete_recovery(run_id, expected_version=...)` closes a task after checking
its saved recovery version. `set_recovery_expiry(run_id, expires_at=...)` sets an
absolute timezone-aware deadline. Completion/expiry immediately hides recovery
receipts, states, summaries and jobs and rejects later writes for that run.
It does not delete the underlying source memory; use `forget_sources` for that.

`cleanup_recovery(limit=100)` physically removes at most 100 derivative rows
for closed/expired runs (maximum batch 1000). Repeated host-driven calls drain
the backlog. It preserves the closed-run registry so stale tasks cannot reopen
after cleanup. Runs must use new IDs once retired. `recovery_stats()` reports
record counts, retained payload bytes, active runs and registry capacity.

Both derivative record capacity and run registry capacity are bounded, each by
the store's configured max_records. Cleanup frees derivative capacity but does
not silently delete terminal run fences. A long-lived deployment eventually
needs an explicit retention/partition rotation policy for that registry; no
unbounded retention or unsafe automatic resurrection is promised.

Existing SQLite stores gain a run_id column and a separate lifecycle registry
on initialization. Old payloads with run_id are backfilled. Other RecoveryStore
implementations must add ensure_run/configure_run/cleanup/stats/next_job with
equivalent lifecycle and exact-scope semantics.

## Validation status

This increment has not been tested. It needs migration, SDK/MCP authorization,
queue restart/timeout/retry, retrieval probe, preflight-staleness, expiry/cleanup,
and cross-scope acceptance tests before release. Prior 469-test results apply
to the preceding implementation, not this increment.
