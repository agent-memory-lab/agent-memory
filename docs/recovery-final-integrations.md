# Recovery integrations, history and evaluation

## Real optional tokenizer

`TiktokenModelCounter` lazily loads a named tiktoken encoding. The host installs
the optional package; importing agent-memory does not require it. Pass an
explicit encoding_name, model_id and template_version. This measures that
encoding, not an automatically inferred model API. Host rendering/framing and
tool schemas still determine the complete prompt. Arbitrary special-looking
input text is tokenized literally. Provision the vocabulary cache for offline
deployments. Tests use cl100k_base with real encoder output and known examples.

## Retirement cleanup retry

Retirement now commits a durable cleanup request with the generation fence.
It returns cleanup_complete=False if file removal fails. Retry through
`memory.retry_retired_cleanup(limit=8)`, also after restart. It removes only
managed inactive SQLite files and sidecars and rejects symlinks. Rotation
counts pending cleanup against its storage limit so failures cannot silently
accumulate unlimited partitions. Source deletion and physical disk sanitization
remain different operations; filesystem snapshots/backups need host policy.

## Historical recovery

Every successful state update atomically archives the previous snapshot.
`recovery_history(run_id, limit=20, before=...)` returns live-evidence-checked
older versions, newest first. New captures do not reconstruct versions that
were overwritten before this feature was installed. Histories count toward
capacity and participate in deletion, run cleanup and partition retirement.

`restore_recovery(run_id, version, expected_version=...)` creates a NEW current
version; it never decrements the version counter. Source evidence is rechecked,
stale writes fail, and side-effecting tool states become unknown so the host
must reconcile them. It never calls tools. MCP/SDK operations history and
restore require the host's existing authorizer; grant restore only to approved
operators, not arbitrary model requests.

## Feedback aggregation

`compression_feedback_report(limit=100, after=...)` returns bounded page
aggregates separated by units, counter, evaluator and strategy. Each source is
revalidated. Reports include sample/outcome counts and absolute measured-unit
differences; they do not infer quality gains or causal effects. next_cursor
indicates more pages. Complete reports require collecting pages while the host
holds a stable evaluation dataset. Mixed pages are not a database-wide snapshot.
Historical closed-run reporting requires a separate approved aggregate export;
this API does not bypass expiry or resurrect invalidated feedback.

## Reproducible comparison runner

`compare_memory_backends(cases, factories)` in memory_evaluation.py accepts
bounded immutable raw turns, update turns and queries. Each async backend
factory receives only a unique isolation ID and must implement add, search,
delete_all and close. Gold fragments stay in the runner and are never supplied
to factories or ingestion. Plug real Mem0/Graphiti/provider clients into this
small port using host-owned credentials and isolated test namespaces.

The report records identical raw-input digests, per-operation latency, initial
and updated fragment recall, deletion residuals and cleanup status. No raw text
or error messages are included. Billed usage is null unless separately measured
by the host; no estimated cost is presented as observed cost. Fragment matching
is a narrow reproducible metric, not semantic QA quality or a statistical
superiority claim. Timeouts can leave remote work running; isolated namespaces
must never be reused, and failed cleanup requires host intervention.

This project has no supplied real comparison services or dataset in this task.
Protocol tests are NOT a completed Mem0/Graphiti head-to-head experiment.

## HTTP MCP tests and fault acceptance

The new tests launch a localhost Streamable HTTP server and use real signed
gateway headers, real MCP client calls, signature rejection, timeout and
reconnection. They do not test production TLS, proxies or load balancing.
Fault tests cover retirement deletion failure/retry, SQLite transaction crash,
capture/cancellation ordering, storage write failure, history provenance and
scope-isolated pagination. Results are reported only after executing tests.
