# Optional recovery and context compression

This feature is opt-in, requires no new dependency and starts no worker. The
host remains responsible for tool execution, evaluation and context replacement.
The API is available from the dedicated modules, not additional SDK/MCP endpoints.

## Local use

```python
from datetime import UTC, datetime
from agent_memory import MemoryScope
from agent_memory.unified_memory import UnifiedMemory
from agent_memory.recovery import RecoveryState, ToolExecutionState
from agent_memory.context_compression import CompressionPlan, ContextSegment

scope = MemoryScope("example", session_id="session-1")
memory = UnifiedMemory.local(
    "memory.db", scope, recovery_path="recovery.db",
)
await memory.initialize()
try:
    receipt = await memory.capture_with_receipt(
        event_id="message-1", run_id="task-1", role="user",
        content="Compare the two reports. Do not publish the result.",
        occurred_at=datetime(2026, 9, 27, tzinfo=UTC),
    )
    state = RecoveryState(
        run_id="task-1", version=1, goal="Compare the two reports",
        constraints=("Do not publish the result",),
        pending_items=("Read the second report",),
        source_event_ids=(receipt.provider_event_id,),
    )
    await memory.save_recovery(state, expected_version=0)
    result = await memory.propose_compression(CompressionPlan(
        run_id="task-1", recovery_version=1, token_budget=4096,
        segments=(ContextSegment(receipt.provider_event_id,
            "Compare the two reports. Do not publish the result."),),
    ))
    if result.accepted:
        # The host must check that its current input still has this digest,
        # approve the proposal, and re-read load_compression(summary_id)
        # immediately before applying it. Do not replace context here blindly.
        proposal_id = result.summary_id
    # Restart with the same database paths and scope, then call:
    restored = await memory.load_recovery("task-1")
    # Source IDs are provider_event_id, NOT the transport event_id.
    await memory.forget_sources((receipt.provider_event_id,))
    assert await memory.load_recovery("task-1") is None
finally:
    await memory.close()
```

## Capture receipt meaning

Existing `capture()` still returns `CaptureSubmission`. When recovery is enabled,
`capture_with_receipt()` and `capture_receipt(event_id)` return `CaptureReceipt`.

| Field / stage | Meaning |
| --- | --- |
| received | Admission metadata persisted; source commit is not confirmed |
| persisted | Synchronous source ingestion returned and evidence existence was checked |
| extracted | The local generated extractor completed in that ingestion; zero claims is possible |
| retrievable | An injected `RetrievalReadinessProbe.ready(scope, source_ids)` currently confirms the host's retrieval path |

The booleans are distinct: raw evidence can be retrievable without automatic
extraction. No probe means `retrievable=False`, not an invented readiness claim.
The default configuration has no generator, so `extracted=False` is expected.
Custom provider ingestion does not imply extraction completion automatically.

Receipts are metadata, not permission to discard context. A host should require
persisted evidence, a saved recovery state, an accepted compression proposal,
and, when its workflow needs it, verified retrieval readiness before trimming.
No `safe_to_trim=True` is inferred from admission or extraction alone.

Use the same explicit `occurred_at`, run and content when retrying an event ID.
A crash between source commit and receipt completion is conservative: retry can
confirm persistence but does not fabricate a missing extraction acknowledgement.
Before source commit, errors leave a received record. There is no background
retry worker or automatic queue integration in this implementation.

## Recovery state

`RecoveryState` contains a goal, constraints, pending items, tool observations
and evidence IDs. It is exact-scope and bound to one run. Every source must have
a persisted capture receipt from that run and must still be live. Existing
events captured before enabling this feature need an explicit migration; they
are not silently accepted as acknowledged evidence.

Updates use `version=previous+1` with `expected_version=previous`. Stale writers
are rejected. A new state version makes summaries based on the previous version
unavailable. This is latest-state storage, not a full version-history archive.

Tool status is one of planned/running/succeeded/failed/unknown. Side effects
default to true. Running or unknown side-effecting calls require host
reconciliation after restart. The plugin never calls tools, retries actions or
claims exactly-once execution. Tool evidence must be included in state evidence.

Inputs are host-owned, not untrusted model instructions. Recovery text is
bounded and rejects built-in sensitive patterns rather than silently modifying
exact state. Hosts must apply their own domain-specific redaction policy too.

## Compression contract

The default extractive compressor selects complete original segments. A custom
async `ContextCompressor.compress(plan, summary_budget=...)` requires an injected
`CompressionValidator.validate(plan, state, summary)` returning exactly `True`.
The validator belongs to the host; a model's self-approval is not evidence of
fidelity. Context segments must be host-authorized sanitized source text.

The coordinator attaches the entire saved recovery state and source IDs itself.
The compressor cannot replace that structured state. It checks evidence and
state version before and after generation, checks the complete serialized output
budget, and persists only accepted proposals. Generation/validation errors,
timeouts, empty summaries, excessive output and stale state reject the proposal.
Rejected results have no replacement. Original context is never changed here.

The default counter counts UTF-8 bytes, not exact model tokens. Inject a
`TokenCounter.count(text)` for the target model. The host must also reserve budget
for system messages, tools, other context and message framing. Even the default
extractive compressor is lossy: omitted text is not a semantic-equivalence claim.

An accepted result still has reason `host_approval_required`. Keep the original
context until the host checks `input_digest`, retrieves a fresh proposal and
explicitly applies it. Returned Python objects cannot be revoked after delivery;
do not cache and apply old proposals after memory deletion or state updates.

## Deletion and operational limits

Recovery registers as the reserved `__recovery_v1` deletion target automatically.
`forget_sources` invalidates receipts, states and summaries referencing any
deleted source; `all_in_scope=True` covers the entire exact scope. Both archive
and erase remove derivative content in this optional store. Identity, revision
and source IDs remain as terminal tombstones. Use new event/run identities after
deletion; a stale writer cannot reactivate a tombstone. This is logical content
deletion, not secure erasure of SQLite pages, filesystem snapshots or backups.

An interrupted multi-store deletion remains in the existing durable journal and
blocks unified reads/writes until `resume_deletion()` succeeds. Restart with the
same recovery and deletion-target configuration. Verification exceptions fail
closed and propagate; missing evidence invalidates dependent artifacts.

One host-owned `UnifiedMemory` instance exclusively owns a scope. Its gate is
not a distributed transaction: do not bypass it via provider/store calls or use
multiple independent writers for the same scope. Direct external deletion is
checked at use time, not protected by a cross-database atomic fence.

Default storage limits: 10,000 records including tombstones, 64 KiB per payload,
256 evidence IDs, 64 constraints/pending items/tools, 128 context segments and
256 KiB aggregate compression input. Capacity exhaustion rejects writes rather
than evicting recovery evidence. No resident cache or automatic retention job.

For other persistence backends, inject `RecoveryMemory(scope, store, evidence,
retrieval_probe=...)` into `UnifiedMemory`. Implement `RecoveryStore` and
`EvidenceVerifier`; hosts own injected resource initialization/closure beyond
the recovery store's `initialize()` call.

## Validation status

This change has not yet been executed or tested. Acceptance should cover restart,
version conflicts, duplicate capture, failed extraction, missing readiness,
cross-scope/run references, generation timeout, budget overflow, host rejection,
source archive/erase, interrupted deletion, stale proposals and capacity limits.
