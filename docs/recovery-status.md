# Recovery status queries

These optional, read-only operations use the same UnifiedMemory deletion gate
and exact-scope authorization as recovery writes. No background worker, model
call or business tool is involved. Results contain metadata, not source text.

## Python and SDK

```python
page = await memory.list_recovery_runs(limit=20)
for run in page["items"]:
    print(run["run_id"], run["status"], run["retained_jobs"])
if page["next_cursor"] is not None:
    next_page = await memory.list_recovery_runs(after=page["next_cursor"], limit=20)

status = await memory.recovery_run_status("task-1")
job = await memory.capture_queue_status("queued-1")
```

EmbeddedMemoryClient and MCPMemoryClient have the same three methods, with
responses wrapped in `result`. MCP uses `memory_recovery` operations `runs`,
`run_status` and `queue_status`. Add these operation names to the trusted host
authorizer's read policy explicitly; existing authorization is not expanded.

## Run metadata

Run results contain run_id, active/completed/expired status, expiry timestamp,
current recovery version when active, retained record/payload counts, payload
bytes and counts of retained jobs by job status. Counts describe storage, not
executable work: expired runs may retain queued jobs until cleanup removes them.

Only registered lifecycle runs are listed. Legacy payloads without a lifecycle
registry entry become registered on a normal recovery write or explicit expiry
configuration; this read interface does not migrate them as a side effect.

Pages contain at most 100 items and use the last run ID as an exclusive cursor.
Each page is a consistent SQLite read snapshot; a sequence of pages is not one
transaction. New IDs inserted before the cursor appear on a fresh traversal.
The server always applies the authenticated scope, even if a cursor was copied
from another scope. A cursor is an ordering key, not an authorization token.

Completed/expired fences remain visible after cleanup, but their content is
not returned. Unknown runs return null. Counts may require reading the indexed
records for each selected run; bounded output does not imply constant query
cost for a run with many retained records.

## Queue metadata

Queue status contains event_id, run_id, durable job status, attempt count,
whether receipt status matches the job, a synchronized non-sensitive error
code, and retryable/resumable flags. The job is authoritative if a crash happened
between a job transition and its receipt acknowledgement. A resumable flag is
diagnostic; it never automatically processes a job. Retry remains host-driven.

Unknown, invalidated, expired or completed-run jobs return null. This method
does not probe semantic retrieval, expose the stored envelope, reset attempt
counters or reveal provider exception messages. Pending deletion blocks all
three endpoints until deletion is resumed.

Custom RecoveryStore adapters must implement list_runs and run_status with
equivalent exact-scope semantics. This change has not yet been tested.
