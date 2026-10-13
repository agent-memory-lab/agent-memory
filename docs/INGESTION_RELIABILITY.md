# Generic ingestion reliability and extraction diagnostics

These contracts apply to `MemoryKernel` / `UnifiedMemory` generic claims. Typed
Atom admission, independent authority verification and governed L1 publication
retain their existing policies. A generated confidence score describes extraction
support; it does not prove that the source statement is true.

## Extraction outcomes

`IngestResult.extraction`, `CaptureSubmission.extraction` and durable recovery
receipts expose an immutable `ExtractionDiagnostic` when the extractor supports
reports. `extract_with_report(event)` returns `ExtractionReport`; the existing
`extract(event)` interface still returns claim drafts.

| Status | Meaning |
| --- | --- |
| `skipped` | Extraction was not applicable or no explicit metadata claims were supplied. |
| `empty` | The extractor returned a valid empty candidate list. |
| `completed` | Validated candidates were accepted; individual rejected candidates remain visible. |
| `rejected` | Validation completed, but candidates failed confidence, scope, deduplication or policy checks. |
| `degraded` | Some safe candidates were retained while other output or a component failed. |
| `error` | Output validation or the extraction provider failed with no usable candidates. |

Reports include candidate/accepted counts, bounded rejection reasons and component
diagnostics. Unknown candidate counts remain `None`. Reasons contain no source
bodies or provider exception messages. A candidate-limit rejection carries a
`count` so a large discarded tail does not allocate one diagnostic per candidate.

Generated candidates require explicit numeric `confidence` in `[0, 1]`; invalid
types, missing required fields and unsupported fields are format errors. Structured
conditions, exceptions and source quotations must use the typed admission path;
the flat claim parser rejects them instead of silently dropping qualifiers. Trusted
SDK metadata keeps its documented default score for backwards compatibility.

With `fail_open=False`, both the generated extractor and the public composite
builder propagate failures. Direct capture returns `capture_extraction_failed`,
and does not commit a new source or its claims. The same capture identity can be
retried after correcting the generator. With `fail_open=True`, safe partial claims
and raw evidence can persist, accompanied by `degraded`/`error` diagnostics.

Capture `status="done"` describes the completed ingestion call, not extraction
quality. Check `.extraction` separately. Recovery only marks extraction successful
for `empty`, `completed` or `rejected`; an idempotent duplicate with no historical
diagnostic does not invent success. Existing receipt stage names remain compatible.

## Transactional scheduling handoff

When a scheduler is configured, SQLite and PostgreSQL save a consolidation intent
in the **same transaction** as the event and claims. Dispatch occurs after commit.
A scheduler error leaves the intent pending instead of returning a false write
failure and losing scheduling on the next idempotent retry.

`IngestResult.consolidation_status` and capture receipts distinguish `pending`,
`delivered` and `cancelled`. Delivered means the scheduler accepted the job; it
does **not** mean background consolidation completed. No configured scheduler
leaves this field `None`.

Initialization drains at most 64 pending intents. Duplicate ingestion also retries
its own intent. A long-running host should call the bounded drain periodically:

```python
await kernel.retry_consolidation(scope=scope, limit=64, lease_seconds=60)
```

Failed deliveries persist `next_attempt_at` with exponential backoff (1, 2, 4,
… seconds, capped at 300 seconds). A global or scoped drain skips intents that
are not due and continues after a failure, so one failing source or tenant cannot
block other ready intents. Each intent is attempted at most once per batch even
if the batch outlasts its backoff. Ready work is ordered by its next eligible time,
so retries also yield to older untried work when the batch limit is one.
Explicit event-ID retries, including duplicate
ingestion, can retry immediately; lease tokens still fence stale acknowledgements.
Attempts and bounded error codes survive restarts. Failed work stays pending for
repair and eventual retry; it is not silently discarded or marked delivered.

Delivery uses leases and token-checked acknowledgements. A crash after enqueue
and before acknowledgement can redeliver, so scheduler implementations must
deduplicate `event:{event.id}:consolidate`. Existing local and PostgreSQL queues
already use that identity. The outbox stores source IDs/revisions and result
metadata, without another copy of source text. Dispatch checks source liveness;
derived writers must still validate live evidence at publication. A remote queue
cannot share the source database transaction.

Forgetting a claim also cancels jobs already handed to the queue, including claims
stored at a broader scope than their source. Reverse source dependencies identify
the exact source scope for cancellation. Delivered intents retain only claim IDs;
an ID-only cancellation marker keeps duplicate ingestion from recreating the
handoff while the original source remains live. Pending cancellation requests
survive queue failures and are retried at initialization or through
`retry_consolidation`; its `cancellation_*` counters report that work separately.
Cancellation failures persist their own exponential backoff (1 second up to
300 seconds). Due cancellations are ordered by next eligible time, so a failing
source or tenant cannot starve older untried cancellations, even with a batch
limit of one. Failure recording is fenced by both generation and observed attempt;
concurrent reports cannot delay a newer cancellation or count one attempt twice.
Generation checks prevent an old cancellation acknowledgement from clearing a
new request caused by a late enqueue. Source erasure removes the intent with the
source; publication must still reject erased evidence even if remote queue
cleanup is unavailable.

Trajectory consolidation blocks now persist `source_claim_ids` in their existing
JSON payload. Required typed claim dependencies are validated in the artifact
write transaction (SQLite's `BEGIN IMMEDIATE`; PostgreSQL's namespace advisory
lock shared with deletion). If claim deletion wins the race, publication fails;
if publication wins, deletion withdraws the dependent block. Read validation also
rejects blocks whose referenced claim is outside the current bitemporal valid/known
intervals, even when the legacy claim head remains active. Future replacements
and temporary overrides use the same interval materialization as current state. Cancelling a queue job
alone is insufficient to stop a handler that already captured claim text.
Older block payloads without claim dependencies retain their existing event-only
contract; this change does not infer or backfill unknown historical claim lineage.
Regular updates preserve omitted claim dependencies inside the CAS transaction;
explicit empty arrays cannot clear an existing dependency set. The [block
contract](MEMORY_BLOCKS.md) documents Python, SDK, MCP and HTTP compatibility.

Custom repositories with a configured scheduler must implement the transactional
outbox extension in `ports.py`; unsupported combinations fail at initialization.
Repositories without a scheduler retain the existing repository contract.
PostgreSQL migration `021_consolidation_outbox.sql` adds the corresponding table;
`022_consolidation_outbox_retry.sql` adds retry scheduling.
`023_consolidation_cancellation_retry.sql` adds independent cancellation attempts
and retry scheduling without rewriting or duplicating source content. Existing pending rows
remain immediately eligible. SQLite upgrades the existing table additively.

## Long-source retrieval

SQLite native retrieval keeps a bounded set of matched and contextual parent windows
for each selected long event before semantic reranking: up to two lexical hits plus
tail/adjacent context, at most four windows per source and within the overall
candidate limit. Each parent spans at most four adjacent index chunks. After
reranking, the optional SQLite context expansion can add at most eight windows
from up to four ranked sources, subject to scope, source revision and lifecycle
checks. Different spans survive rank
fusion and evidence packing through an internal candidate identity. Public memory
IDs remain source IDs, preserving feedback and deletion behavior.

`bundle.retrieval_metadata["citation_spans"]` records span-specific candidate IDs,
source revisions, character spans and source event IDs. This supplements the
existing source citation interface. Packing keeps exact contiguous text and the
caller’s token budget; it never joins distant fragments into a fabricated quote.

This is bounded candidate expansion, not full-document semantic coverage. A fact
outside the selected spans can still be missed. Native PostgreSQL retrieval keeps
its existing behavior. See [candidate selection and bounds](design/retrieval/bounded-source-context.md).

## Benchmark contract

The LongMemEval generators share `examples/longmemeval_claim_extraction.py`, whose
schema requires explicit confidence. The full worker records body-free extraction
counts/statuses in per-question results and progress. Missing diagnostics remain
unknown. Legitimate zero-fact extraction is allowed; malformed output stops that
attempt under strict mode.

The existing stopped run and its frozen scripts are historical evidence. These
fixes do not alter its `4/7` partial score or establish a new score. Offline contract
and regression tests validate software behavior without restarting that run or
calling real models.
