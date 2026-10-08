# A9 workload runner and paired statistics

This is **evaluation tooling and synthetic runtime integration evidence**, not
licensed-domain or real-model acceptance. AM70-T13/T15 and Q7-32 remain open.
No production feature is enabled, and no cost saving is claimed.

## Run locally

From an installed checkout with Python 3.13:

```sh
PYTHONPATH=src python -m agent_memory.evaluation.question_fixture \
  --output /tmp/a9-report.json --seed 917 --resamples 200
PYTHONPATH=src python -m agent_memory.evaluation.question_fixture \
  --scenario lifecycle --output /tmp/a9-lifecycle.json --resamples 200
python -m pytest tests/test_question_experiment_v7.py
```

The CLI first saves `/tmp/a9-report.plan.json`, containing the complete frozen
plan and its SHA-256, **before creating any runtime**. The default ablation scenario
runs seven isolated SQLite databases; the lifecycle scenario runs the four primary
arms. Each writes one JSON report. The only inference transport is a
local deterministic stub. The endpoint in its configuration is never contacted;
there is no download, real model call, external publication or bill.

Use a dedicated sequential process. The integration fixture temporarily binds
the existing runtime writers and scheduler to a single virtual clock and restores
their clocks on close. Its frozen one-millisecond post-write ticks accommodate
the database's strictly increasing admission-record timestamps. Its initial
clock is synthetic and must not be interpreted as measured production time.
Runtime operation IDs and measured CPU/wall times naturally vary across runs;
the input/event order, decisions, controls and resampling algorithm are repeatable.

## What actually executes

Every arm uses the existing `ProjectAdmission`, reviewed field evidence,
`QuestionService`, durable `RefreshDemandQueue.request/claim`, snapshot, guarded
publish and queue completion, plus `QuestionModelRuntime` and
`GovernedModelAnswers` for actual input/dispatch/cache/delivery authorization.
This is not a dictionary-cache simulation or a callback-only benchmark.

The four primary arms are:

- `on_demand_full`: no prewarming, on-demand policy, full computation on each
  request, no model-cache lookup
- `coalesced_full`: prewarmed maintained views, on-change policy, full computation
  only when the runtime refreshes, no model-cache lookup
- `delta_proof`: the same maintained policy with the production delta/proof path
- `exact_cache`: the same delta/proof policy with the production exact model cache

The first comparison is deliberately labeled **bundled maintenance policy**.
It does not attribute the combined change to coalescing alone. Three additional
arms and precommitted contrasts isolate one control each, holding prewarming,
other scheduling controls, persistence, model and data fixed:

- `eager_full` versus `coalesced_full`: run the real queue after every write versus
  coalescing between scheduled background opportunities
- `routed_full` versus `coalesced_full`: the production `QuestionRouter` resolves
  a registered alias versus a direct registered question ID
- `coalesced_full` versus `cold_full`: static host-owned on-change versus on-demand
  refresh policy

Plan validation rejects an allegedly isolated contrast that changes any other
declared control. Adaptive learned hotness thresholds, calibration and automatic
promotion are **unmeasured**, not implied by the static hot/cold comparison.

Two narrow **evaluation-only controls** create the reference arms without a
production API or dependency change:

1. Full arms replace baseline loading with a metadata-only CAS head read, before
   any old content/delta body can be read, and perform normal full materialization.
   Demand-full additionally uses a single-use evaluation unit coordinate so the
   actual queue must refresh on each demand. The source census, safety proof,
   lease fences, recomputation, publication and final delivery guards remain.
2. No-cache arms disable cache lookup only. Normal dispatch authorization,
   reservation, completion and delivery still execute. They still pay for cache
   writes; this experiment isolates lookup reuse rather than the separate effect
   of eliminating cache construction/storage. Cache construction is not free.

These controls are installed only on newly created fixture objects. The fixture
does not alter production code, default policies or public transport contracts.
No runtime module imports these evaluation modules. Existing DB/protocol schemas
are unchanged; report contracts are `question-workload-experiment/1` and
`question-paired-bootstrap/1`.

## Frozen inputs and extension contract

`ExperimentPlan` binds the initial snapshot, ordered events/requests, opaque
project/session grouping, independent gold annotations, code/backend/hardware,
semantic and security rules, hard resource limits, model configuration, finite
drain policy, arm controls, contrasts, judge identity, acceptance template and
statistical protocol. The local fixture additionally verifies installed Python
and SQL source bytes against the frozen package fingerprint before opening SQLite.

`run_experiment(plan, factory, judge, expected_plan_sha256=...)` is reusable:

- A factory receives an immutable **gold-free** `ExecutionInputs` and
  `ExperimentArm`. It must create a distinct isolated adapter. Neither the factory
  nor any execution step receives gold, the acceptance thresholds or judge.
- The adapter constructor only constructs. `execute(cold_start)` initializes;
  registration and prewarm precede event injection. Writes are followed by a
  dependency observation. Each request is retained in its original position.
  Absent phases are explicitly inspected rather than silently priced at zero.
- `execute(drain)` stops injection and performs the precommitted finite drain.
  `finish()` returns actual runtime model-ledger rows, fixed triggering-request
  bindings and remaining finite `PendingResponsibility` records. `close()` is
  measured in drain too. An unavailable final ledger or failed cleanup leaves
  explicit unknown debt and blocks the result.
- The judge carries `configuration_sha256` matching the frozen plan and returns
  `RequestResult` from the frozen gold and actual output. Request ID, group,
  answerability and diagnostic status cannot change. Missing gold/judge becomes
  an unscored system-error record and blocker, never self-scored output.
- Model calls must bind the declared role/configuration; invoices and measured
  token receipts use the existing `runtime_model_costs` contract. A declaration
  is a trusted host assertion, not independent proof of a real provider call.
- Only previously-null baseline/candidate **result identities** may be bound
  after execution. A contradictory prebound identity is rejected. Thresholds,
  judge, model and statistical choices are never filled in after seeing results.

Future licensed-domain/provider adapters need their own permitted inputs,
provenance and held-out split, actual endpoint configuration, independent judge,
resource measurements/tariffs, invoice reconciliation and approved thresholds.
The bundled fixture is intentionally synthetic-only and rejects attempts to
relabel its data or transport as real. Its declarative initial snapshot supports
host-owned project membership and registrations for all four production templates:
owner, status, commitments (including overdue-only), and risks. Source events may
bind subject, predicate, value, membership, valid-from/to and occurrence timestamps,
plus trusted condition/exception expressions and frozen context attributes.
Those qualifiers are persisted in AtomDraft field evidence and in the host semantic
review; the local renderer includes them in its structured output.
The workload supports source add/withdraw, grant revocation/restoration, real
repository erasure, membership moves with explicit optional host re-review, and
pure clock advances. A membership change never silently preserves qualification.
Erasure never causes the drain to resurrect registrations or manufacture new work.

`lifecycle_plan()` is a compact synthetic smoke matrix across the four primary
arms. It checks all four templates, owner expiry and commitment-overdue transition
without a new source write, late evidence, movement from one project's commitment
list to another, processing revocation/restoration, and physical source erasure.
An expected denial is returned as a structured observation only for a narrow set
of known runtime security codes. It is a reasonable unknown/refusal only if frozen
unanswerable gold names that exact permitted code; an unrelated exception remains
a system error and an unexpected denial fails quality. Required and forbidden
source annotations are checked against actual citations. Qualifier fidelity is
scored separately against explicit gold conditions/exceptions in the actual model
output, including the absence of qualifiers; missing gold is unmeasured/false.
Equal business values alone never establish qualifier preservation. Diagnostic
requests remain in the all-request denominator and cannot be effective answers. The matrix is 13
requests: 10 effective factual answers (including certified known-empty results)
and 3 explicitly annotated unknowns/denials. The renderer excludes unmatched rows
and distinguishes known-empty from unknown; empty answers require complete current
candidate-census evidence.
The status case carries a region condition and a holiday exception; a separate
adversarial scorer test drops the exception while retaining the correct value.
This is bounded integration coverage, not the full adversarial corpus, qualified
real-world data, or a completed real-model acceptance experiment.

## Later real Ollama invocation (not run here)

The existing production `OllamaPort` is already wired by
`run_ollama_experiment`; swapping the local stub does not require another model or
question-runtime adapter. This programmatic path is opt-in only; the CLI above
remains offline. After the host has permission to transmit the named dataset to
the exact endpoint, the call shape is:

```python
report = await run_ollama_experiment(
    frozen_plan,
    independent_judge,
    host_resource_observer_factory,
    expected_plan_sha256=recorded_pre_run_fingerprint,
    allow_real_model=True,
    settle_calls=authoritative_invoice_reconciler,  # optional; absent bills stay unknown
)
```

Before any runtime execution this path requires licensed dataset provenance,
a real-generation declaration and complete `ModelConfiguration` (explicit endpoint,
installed revision, runtime manifest, template, tokenizer/overflow assumptions and
limits), a matching `public_template` in shared configuration, `host_review_reference`,
a host-authenticated `source_authority` registry in the input manifest, independent
frozen gold/judge and a frozen resource observer/tariff. Every added real source must
supply its actual `text`, `reviewed_span` (`start`, `end`, `quote`),
`review_reference` and `occurred_at`; synthetic source text or review provenance is
never synthesized for that path. Synthetic transport-failure injection must be
removed from the real plan. Model/tag/runtime checks remain the production port's
responsibility, with its documented immutable-server and tokenizer limitations.

This binding measures generation over **host-reviewed L1 annotations**. It does
not run semantic extraction/review; `semantic_model.execution` must explicitly be
`not_used`. A licensed raw-event extraction experiment still needs its existing
host extraction pipeline and reviewed artifact preparation. Dataset license,
source authority/review truth, held-out split and calibration approval remain
host responsibilities, not consequences of a JSON label.

Platform-specific read-only resource telemetry and authoritative billing lookup
remain host integration points. They cannot be inferred from an endpoint or an
Ollama token count. The optional `settle_calls(ledger)` hook runs during measured
drain, may be synchronous/asynchronous, and must carry a `configuration_sha256`
matching the plan's `billing_reconciler_sha256`. It uses existing ledger settlement
for actual receipts. Missing/unsettled invoices remain unknown even after successful
local resource pricing; zero provider cost requires explicit evidence. The host
must also provide approved quality/cost thresholds and statistical sufficiency.
No real endpoint, dataset, receipt, telemetry or rate was used in this delivery.

## Accounting and trace interpretation

The runner measures actual process CPU and elapsed wall time for cold start,
registration, prewarm, writes, dependencies, background, foreground, failure,
retry, drain and cleanup. It keeps each unique model attempt once, including a
failed dispatched attempt. Retries have separate attempts. Pending work and
unreconciled invoices remain visible. Setup and requestless background work use
the existing fixed equal-share allocation across all requests; attribution is
never selected after seeing answer quality.

By default CPU is unpriced; GPU, I/O, storage and network are explicitly
**unmeasured**. The local CLI supplies no observer, tariff, model tokens or provider
bill. Thus total actual cost, cost per request and cost per effective answer are
all `null`, even though known billed subtotals may be zero. No real telemetry or
rates are claimed by the fixtures.

For a configured host, `ExperimentPlan.resource_pricing` accepts an immutable
`ResourcePricingProtocol` before execution. Its fingerprint binds observer
configuration, currency, schema/units, explicit `ResourceTariff` rates and
provenance, exact rounding policy and the restricted charge scope
`host_local_resources_excluding_provider_bills/1`. A caller supplies
`observer_factory(execution_inputs, arm)` to `run_experiment`; each arm needs a
separate observer whose configuration hash matches the protocol.

The read-only `HostResourceObserver` interface is:

- `begin(phase, request_ids)` obtains a host observation token before the phase
- `end(token, PhaseResourceRequest)` supplies evidence-backed per-phase deltas;
  the immutable request binds operation ID, phase, raw CPU/wall observation hash,
  protocol/configuration hash and currency
- `finalize()` validates collector closure after all measured work, including
  ledger snapshot and resource cleanup; only successful explicit completion
  permits resource pricing

The runner supports synchronous or asynchronous observer hooks. Begin/end/finalize
failures are retained without leaking exception bodies. Failed operations are
observed too. Measurement collection and pricing are evaluation instrumentation,
not an additional production model charge.

The versioned resource schema requires CPU milliseconds, GPU milliseconds, I/O
bytes, storage byte-seconds and network bytes. CPU/wall come from the measured
phase; an observer cannot overwrite them. All other dimensions need explicit
nonnegative observations with evidence. An explicit measured zero is valid;
an absent dimension is never filled with zero. Every charged dimension needs a
matching unit and frozen rate. Wall time is reporting-only unless its own explicit,
non-overlapping tariff basis is supplied. Remote-provider resources already billed
by the model provider are excluded; provider bills remain separate ledger entries.

`reconcile_resources` validates operation/observation/configuration/currency bindings
and preserves all model entries and pending obligations. Quantities and rates use
exact decimal/rational arithmetic; each operation's total is rounded upward once
to integer microunits under the frozen rule. Missing resources, missing tariffs,
nonfinalized observations or failed collector finalization retain unknown cost.
Invalid bindings fail closed. `resource_reports` exposes measurements, missing
resources/rates, reasons and pricing for every operation. Raw `observations` remain
unaltered CPU/wall records; their unmeasured-resource list describes the native
instrumentation, while the separate resource report records any host completion.

This interface checks supplied evidence and arithmetic, not the truth of a host
counter, tariff contract or provider bill. Real host telemetry and rates remain
required external inputs. Unit tests exercise explicit synthetic measurements and
rates only; they do not reclassify the local runtime workload as measured whole-cost
or production-benefit evidence.

Reports include all request outcomes, raw local output, model attempts, per-phase
resource observations, debt, allocations and both denominators. Failed attempts
remain in all requests but never in effective factual answers. Reasonable unknowns
and diagnostic requests are also distinct from effective factual answers.

`runtime_operations` comes from actual queue and answer calls:

- successful full/delta/proof modes, evaluated-group counts and census source count
- actual request/claim/completion and empty-claim observations
- guarded question reuse and exact-model-cache hits, rather than unguarded body hits
- claimed obligation count and lag from the queue's recorded obligation timestamps
- publication certificate identity and whether that specific certificate was ever
  delivered to a subsequent foreground request; prewarm and an older same-content
  certificate do not manufacture a foreground hit

The fixture includes a positive two-second lag window, an actual dispatched
transport failure followed by a new retry, multiple writes before refresh, repeated
reads, same-value grant recertification and final unused drain work. Source IDs,
names and text in this local demo are synthetic. Real adapters must use opaque IDs,
minimize stored bodies, apply retention/access controls and treat local output
artifacts as private; writing an artifact is not authorization to share it.

## Statistical plan

`BootstrapProtocol` freezes seed, resample count, confidence, minimum request/group
counts, allocation and failure rules. The demo's numbers are explicit software-test
parameters, **not calibrated domain thresholds or sufficiency guidance**.

`paired_bootstrap` rejects mismatched workload, currency, data, judge, models and
request/group/answerability manifests. It samples whole sorted project/session
groups with replacement, using the same group draw for both arms. Repeated model
outputs stay inside the original group. It then pools counts and allocated cost,
computing candidate-minus-baseline differences for:

1. effective answers / answerable non-diagnostic requests
2. total cost / all requests
3. total cost / effective answers

Percentile quantiles use linear interpolation. The artifact retains observed
differences, confidence intervals, sample counts, protocol/report hashes and a
hash of the actual group-index draw sequence. If any bootstrap replicate has an
undefined denominator, it is counted and the corresponding interval is undefined;
it is never dropped. Unknown cost leaves quality intervals inspectable but cost
intervals undefined. Insufficient counts or any undefined required interval prevent
creation of `PairedComparisonEvidence` for the existing acceptance gate.

The bootstrap is deterministic for frozen records/protocol and is not a proof of
IID projects, generalization, unbiased annotation, valid licensing or calibrated
sample size. Those remain domain-owner responsibilities. Statistical resampling and
judging are offline evaluation overhead, outside runtime workload cost, and are
never presented as production work.

The primary ablation integration test verifies the expected 8 requests / 7 effective
answers per arm, distinct actual runtime behavior, source/config/input equality,
nonzero lag, unused drain work, and a blocked quality/cost gate. Unit tests cover
hand-computable intervals, grouped denominators, reproducibility, sample shortage,
undefined draws, mutated profiles/protocols, gold isolation, missing annotations,
unknown cost, pending debt and cleanup failures. These are contract results only.
