# V7-B5 governed model runtime and cost contracts

2026-10-08. AM70-T12 and AM70-T13 remain **IN_PROGRESS**. This is an implemented,
opt-in runtime/security/accounting slice, not real-model quality or savings acceptance.
The v6.1 ledger is unchanged: 44 tasks, DONE 1 / IN_PROGRESS 33 / TODO 10.

## Implemented path

The host constructs `QuestionModelRuntime(questions, port, public_template=...,
account_keys=..., validate_output=...)` after configuring a trusted current authority,
registered QuestionViews, and a budget. Construction performs no network operation.
An attached runtime adds the explicit `memory_question` operation `model_answer`
and SDK `question_model_answer(question_id)`. Ordinary `question_answer` stays
structured and model-free. An unattached runtime does not advertise model operations.

1. B3/B4 `model_input_header` verifies the registered definition, audience, context,
   current complete query proof, validity interval, and immutable original generation
   proof without loading question bodies.
2. Every original-generation and current-validation source separately needs a current
   read grant and a recipient-specific processing grant. The recipient binds provider,
   account, endpoint, region and processing-policy revision. No body is loaded before
   all grants pass. Rejected/unused/private inputs remain in the full lineage.
3. The exact serialized request and input manifest bind the structured question,
   inherited generation manifest, source revisions, actor/project/purpose, task intent,
   current proof, proven time interval, public template, model/runtime/tokenizer,
   options, language, output schema, output/input limits and adapter contract.
4. Each execution atomically reserves every applicable monetary account. Dispatch
   intent and current authorization commit together before provider I/O. I/O runs
   outside SQL transactions. Only a currently fenced flight leader can dispatch.
5. An exact cache and durable cross-instance singleflight avoid duplicate compatible
   work. There is no vector-similarity hit, model failover, tools, hidden conversation
   state, or streaming. Cancellation of one waiter does not cancel the shared task.
6. Publication checks the full current proof again. Each waiter separately authorizes
   the final serialized response envelope. Current permission/time failures retain
   their observed clock floor, preventing a later wall-clock rollback from reviving
   an expired grant or answer.
7. Source/scope and direct question-content erasure scrub cache bodies, headers,
   flights, authorization metadata, processing grants, and reverse associations.
   Expired-cache maintenance also scrubs bodies. Finance keeps only minimal hashes,
   counters, random call IDs and invoice evidence, never source text or model output.

A new certificate does not rewrite old generation inputs. Even safe B4 proof reuse
changes the exact model key. Original source permission remains independently required.
No generated answer is promoted to L1 or treated as a semantic review.

## Ollama contract and remaining setup

`OllamaPort` uses the native `/api/chat` API. It sends the exact sealed bytes, sets
`stream=false`, `truncate=false`, and `shift=false`, rejects incomplete/tool outputs,
limits the complete HTTP response including undisplayed thinking, and sanitizes errors.
HTTP error responses are closed. Redirects and ambient proxies are disabled.
It compares installed `/api/tags` digest and the frozen `/api/show` + `/api/version`
manifest before and after generation. Every configuration field is part of the key.

References verified 2026-10-08:
- [Chat request and returned usage](https://docs.ollama.com/api/chat)
- [Installed model digest](https://docs.ollama.com/api/tags)
- [Model details](https://docs.ollama.com/api-reference/show-model-details)
- [Version endpoint](https://docs.ollama.com/api-reference/get-version)
- [Native ChatRequest overflow flags](https://github.com/ollama/ollama/blob/main/api/types.go)

Ollama versions may differ. The host must freeze the exact installed server/model,
inspect its public template/system state, and provide `overflow_guard_sha256` for
an independently recorded overflow-rejection check on that build. A version string
or ignored JSON field alone is not proof that truncation is disabled. This deployment
check is still pending for the requested Qwen3.5 9B installation. Its tag, endpoint,
host, installation/download state, model digest, runtime manifest and verified
server behavior have **not** been inferred or fabricated.

The adapter enforces exact UTF-8 request bytes and configured output reservation;
it does not claim an exact tokenizer ceiling. Pre/postflight detects ordinary model
replacement, not malicious A→B→A switching. The host must exclusively control and
pin the server. Strict immutable-server execution is explicitly false. Unknown
local compute cost is unknown, not a zero-dollar invoice. No actual inference,
paid API request, model pull/download or user-endpoint probe was performed here.

## Money, crash and restore contract

Migration `019_model_budget.sql` and the SQLite additive schema use one durable
ledger and short lock-ordered transactions. Shared/account/tenant limits constrain
one call without multiplying its cost. Integer microunits avoid rounding.

`reserved → dispatch_intent → reconciliation_pending → settled` is the conservative
state machine. A receipt lost after dispatch remains debt. Only a definitely
never-dispatched reservation may release. Retry means a new reservation/call ID;
lease expiry, cancellation, failed output validation and failed publication never
convert unknown cost to zero. Invoice/provider request replay is idempotent, and a
second attempt cannot charge the same provider request. A hard budget requires a
positive authoritative upper bound and evidence. The unpriced Ollama runtime does
not advertise strict monetary budgeting.

Restore is an **offline host procedure**, matching PurgeRestore's deployment contract:
1. Keep the restored database closed to model dispatch. Quiesce the previous
   writer at a recorded transfer cutoff; never run independent writable clones
   against the same monetary accounts. In-flight unknown obligations stay reserved.
2. Replay the independently pinned current deletion journal.
3. Export the current authoritative monetary checkpoint with `ModelBudget.export()`;
   obtain/pin its checkpoint independently of the old content backup.
4. Call `ModelBudget.replay(snapshot, expected_checkpoint=external_pin)` in the
   restored database. It merges later calls, rebuilds receipt uniqueness and account
   debt, and never reduces a newer local settled or unresolved obligation.
5. Verify both receipts, current host authority/model configuration, and only then
   reopen service. A content backup alone cannot prove current invoices or debt.

The ledger and scope audit collections are bounded (4096 records per kind; at most
16 accounts per call, 256 source dependencies and 128 live cache entries by default).
Capacity errors fail closed. Expired rows are scrubbed to minimal tombstones; no
claim of unlimited financial retention/rotation or automatic invoice lookup is made.

## Full cost accounting

Production code does not import evaluation. `evaluation/model_cost.py` projects
actual durable calls once per call ID into B0's `QuestionCostLedger`/`CostEntry`
contracts. Missing token or fee receipts stay unknown, including failed calls.
`ModelExperimentAccounting` records dedicated-process CPU/wall observations for
cold start, registration, prewarm, write, dependency, background, foreground,
failure, retry and drain. Overlapping phase scopes are rejected. Unmeasured GPU,
I/O, storage and network resources are explicitly listed; no unpriced resource is
silently called free. Missing phases remain missing, rather than being zero-filled.

The synthetic integration workload includes prewarming, a failed dispatched call,
an independently reserved retry, two guarded cache hits and inspection of all
remaining invoice debt. B0 reports both all-request and effective-answer denominators.
Its total/per-request/per-effective-answer monetary cost remains unknown and the
real acceptance gate fails. This proves accounting mechanics, not useful model
answers or a measured savings percentage.

## Verification and status

See [machine-readable verification](validation-b5.json) for exact commands/results.
Coverage includes SQLite + real PostgreSQL 17, HTTP recording-server wire equality,
separate grant checks before body access, cross-instance singleflight, original
private generation lineage after B4 noop, current revocation/erasure, actual SIGKILL,
real SQLite backup/pg_dump restore, minimal money replay and dual-denominator costs.
The stronger process test uses a recording synthetic HTTP server: it accepts the
exact `/api/chat` body, withholds the response, and the governor process is killed.
The replacement attempt retains old debt; source erasure instead prevents retry.
This is actual network/process evidence with a synthetic server, not real inference.

Q7-25/26/27/28/31 and relevant R03/R04/R12/N07 have implemented finite-slice protocol
evidence. Actual licensed data/model quality, physical provider input observation,
local compute pricing, frozen acceptance thresholds and paired confidence intervals
remain pending. No old task, whole milestone, or B5 real acceptance is marked done.

## Independent publication review

The final finite-slice review found and repaired six boundary defects before any
B5 commit/ref/PR publication: mutable transport reconfiguration, shared serializer
aliases, provider permission expiring before body access, receipt request identity
rebinding, incomplete certificate-erasure propagation, and late identity assignment
after settlement. A per-call immutable transport configuration now binds every
HTTP operation; the returned envelope is owned from the exact authorized JSON.
Raw and QuestionView body loads get provider guards at their actual load boundary,
and current/historical certificate erasure propagates through affected instances.
Once known, a provider request identity cannot change, disappear on settlement, or
be attached for the first time after settlement. Unproven matching leaves debt held.

The independent test-only commit `c5d2e6c` passed all 41 SQLite/PostgreSQL cases.
The repaired source `caf15f0` passed the final 173-case dual-backend model/security/
transport/erasure run with resource and unraisable warnings treated as errors.
The B5 CI-only change `986dc03` explicitly includes all seven B5 suites in the
hosted `postgres-live` job and retains the four B4 suites. These are code/protocol
checks with synthetic model transports; real-model acceptance remains pending.

## Final dependency integration (2026-10-08)

B5 includes the B4 cached-body guard correction and the stronger restart-clock timing regression.
The exact combined source `3773b10` passed **239 tests**, with **zero failures, errors or skips**,
on SQLite and actual PostgreSQL 17. This run includes all B5 governed model, budget, buffered
Ollama protocol, real process-kill/recovery and independent security suites, plus B4 adversarial
cached-body guards and question snapshot/transport checks. Resource and unraisable warnings are
errors. Per-run commands and hashes are in `validation-b5.json`; counts are not cumulative.

The synthetic local recording HTTP server verifies bytes and lifecycle boundaries. It is not
the requested installed Qwen3.5 9B server and does not establish real quality or monetary savings.
The actual Ollama endpoint, model/runtime manifest and licensed frozen gold/thresholds remain
required for the real acceptance gate. No model was downloaded, paid API called or service deployed.
