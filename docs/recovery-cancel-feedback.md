# Capture cancellation and host-reported compression feedback

## Cancellation

`await memory.cancel_capture(event_id)` cancels queued or failed recovery jobs.
The job envelope is removed and the receipt update is committed in the same
SQLite transaction. Repeated cancellation is idempotent. Direct capture and
enqueue reject the cancelled event identity; use a new ID for a new request.

Cancellation never rolls back source memory. A previously attempted failed job
may have committed its source before acknowledgement failed; the response sets
`source_may_be_persisted=True`. Completed jobs return `cancelled=False`.
Processing jobs require host reconciliation and are not force-cancelled. An
active worker holds the scope gate, so cancellation waits rather than aborting
an in-flight provider call. No exactly-once or remote cancellation guarantee is
implied. For source removal, call `forget_sources` separately.

Cancelled job metadata remains until run completion/expiry and bounded cleanup.
The scrub is logical deletion, not secure removal from SQLite free pages or
backups. External sanitizer artifact cleanup remains the host's responsibility.

## Compression feedback

```python
from agent_memory.compression_feedback import CompressionFeedback

await memory.record_compression_feedback(CompressionFeedback(
    feedback_id="evaluation-1",
    summary_id=proposal.summary_id,
    evaluator_id="host-task-evaluator-v1",
    outcome="succeeded",
    before_units=8000,
    after_units=2000,
    measurement_unit="tokens",
    counter_id="target-model-tokenizer-v1",
))
report = await memory.compression_feedback("evaluation-1")
```

The host provides the evaluator, outcome, measurement unit and counter identity.
The plugin records them as `host_reported`, binds the accepted summary's input
digest, recovery version and root evidence, and computes only the numeric
difference. Negative savings are allowed. No quality gain, causality, actual
context application or automatic promotion is inferred from these numbers.

Optional `outcome_event_ids` must be live captured evidence in the same scope
and run. The authorizer must restrict feedback writes to trusted evaluators;
an evaluator_id string itself grants no authority. Failure reasons are bounded
and checked for built-in sensitive patterns. Identical feedback IDs are
idempotent; changed payloads under the same identity are rejected.

Feedback can describe an earlier saved proposal after the recovery state has
advanced, provided its source evidence remains live. Submit feedback before
closing/expiring the run. Source deletion invalidates feedback with other
derivatives, and run cleanup removes it. No model or learning worker is started.

## SDK and MCP

Embedded and MCP clients expose `cancel_capture`, `record_compression_feedback`
and `compression_feedback`. MCP operations are `cancel`,
`record_compression_feedback` and `compression_feedback` through memory_recovery.
All use the same exact-scope authorization and pending-deletion gate.

## End-to-end tests

The new MCP tests start a separate Python process with the official stdio
server and connect through MCPMemoryClient. They cover capture, recovery,
compression, feedback, cancellation, process restart, scope/erase denial,
sanitized error transport and client timeout. The fixture is test-only and
does not replace production authentication. HTTP/TLS/gateway deployment is not
covered by these stdio tests. Results must be reported separately after running.
