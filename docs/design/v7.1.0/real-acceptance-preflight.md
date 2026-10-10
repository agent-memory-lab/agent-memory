# Real acceptance: integration preflight and remaining inputs

This is an extension of `evaluation.real_acceptance` around the existing A9
`question_experiment.run_experiment`, not a second runtime, evaluator, database,
model transport or statistical framework. It does not complete M5, promote source
omission audits/reranking, or change any of the historical 44 AM61 task states.
No model was invoked, installed or downloaded for this integration preflight.

## Evidence boundaries

The archived [2026-10-09 Ollama smoke](../v7.0.0/ollama-smoke.md) records nine
successful check groups and seven actual Qwen3.5:9B generations over authored
synthetic facts. Its model digest, runtime manifest, prompts and source revision
are archived with that run. This is genuine local inference and runtime-guard
coverage. Its token/duration receipts do not measure host GPU/I/O/storage/network
costs; its monetary ledger remains unreconciled.

The separate [v7.1 extraction-host smoke](model-host-synthetic.json) records real
generation/review of an authored language preference. Neither historical smoke
is independent held-out business data, calibrated extraction quality, temporal
relation accuracy, or a full baseline-versus-features business-cost result.
Historical runs are not reruns against the integrated code.

`run_ollama_experiment` already binds the production Ollama port to A9. However,
its input consists of host-reviewed L1 annotations and its semantic model is
explicitly `not_used`. Changing its endpoint/model tag cannot make it an
end-to-end raw-source extraction/omission experiment. The new whole-workflow
protocol rejects that declaration; the existing annotation-only path remains
available for its original, narrower scope.

## Reusing the existing experiment

1. Prepare a licensed raw-source `ExperimentPlan` with the existing ordered
   writes, background work, requests, failures/retries and finite drain contract.
   Freeze source versions, valid/known-time coordinates, code/backend/hardware,
   budgets, model identities, independent judge, gold, statistical grouping and
   approved thresholds before execution. The semantic model must be declared
   `real`; actual role-bound ledger receipts must prove its execution before the
   existing acceptance gate can pass.
2. Call `workflow_feature_ablations(plan, baseline_arm="exact_cache")`. It retains
   all original A9 strategies/contrasts and the optimized exact-cache baseline.
   Three added arms each vary only one explicit boolean control:
   `shared_proof`, `source_omission_audit`, or `relation_views`. A fourth added arm
   enables all three and is labeled a bundled comparison. The helper does not
   introduce production switches, disable permission checks, or run an adapter.
   Every feature arm uses the same baseline strategy. Existing unrelated controls
   must stay identical in each feature comparison.
3. Wrap the result in `RealWorkflowAcceptancePlan`, binding the byte hashes of
   corpus, independent gold and pricing artifacts. The experiment's judge hash,
   license and calibration references must also match the input manifest.
   Its `coverage` maps all eight named slices to frozen, nondiagnostic request
   IDs: shared proof; omission recall; omission false acceptance; relation
   derivation; temporal retraction; source retraction; authorization retraction;
   and erasure. Useful-fact omission cases must be answerable, and negative
   omission cases unanswerable. Exact triple/span/qualifier labels remain in the
   independent gold, not in the adapter input.
4. Invoke the wrapper with the existing A9 adapter, judge and observer interfaces:

```python
report = await run_real_workflow_acceptance(
    verified_inputs,
    frozen_workflow_protocol,
    trusted_raw_source_adapter_factory,
    independent_judge,
    authorize_inputs=trusted_host_authorization,
    expected_protocol_sha256=recorded_before_execution,
    observer_factory=host_resource_observer_factory,
)
```

`authorize_inputs(inputs, protocol)` authenticates the input preparation against
the hashed corpus/gold, rights and endpoint scope, real source authority, judge
independence, actual feature implementations and pricing provenance. It runs
before execution and before delivery; artifacts and the frozen protocol are
rechecked both before and after each authorization await. This protection is
specific to `run_real_workflow_acceptance`; the original retrieval wrapper is
unchanged. An affirmative callback is a trusted host decision, not a license or
quality proof manufactured by the library. The wrapper does not lock the
filesystem or prevent every time-of-check/time-of-use race. Adapters must consume
immutable verified snapshots or use current governed reads at each dispatch,
including normal source-revision, authorization and erasure checks.

Factories receive the existing immutable gold-free `ExecutionInputs`. They must
only construct distinct adapters; initialization and all substantive work belong
inside measured phases. Each adapter explicitly attests its applied controls as
`acceptance_controls_sha256 = digest(json.loads(arm.controls_json))`. An adapter
that ignores these controls must not attest them. This hash binds a host assertion;
it cannot prove what arbitrary host code does. The annotation-only fixture lacks
this attestation and is not silently upgraded to a raw-source adapter.

Shared-proof ablation should compare isolated single-view transactions with the
existing bounded batch APIs, holding source census, all safety checks and output
semantics fixed. Omission ablation must include zero-candidate generations,
positive omitted facts and unsupported/adversarial negatives; an auditor finding
alone is not an accepted fact. Relation ablation must inspect actual candidate
lineage and complete/unknown states before and after clock-only expiry, premise
withdrawal, source/grant changes and erasure. Inferred relation candidates must
never be judged as source-asserted facts merely because their values match.

## Metrics and honest interpretation

A9 reports contain plan/gold and raw runtime output. Treat real reports as private
artifacts; preflight permission does not authorize publishing them in a PR.

The report preserves A9's whole workload, all requests, model attempts, retries,
failures, CPU/wall observations, resource reconciliation, pending debt, fixed cost
allocation and paired group bootstrap. Cold start, registration, prewarm, writes,
dependencies, background, foreground, failure, retry, drain, accounting and cleanup
remain charged. An absent measurement, tariff, invoice or unresolved operation is
unknown, never zero. Both total cost / all requests and total cost / effective
answers retain their original denominators.

`real_acceptance.coverage` exposes per-arm, per-slice request/group counts,
effective answers, each judged outcome, and unsafe/stale counts. These are
explicitly descriptive. In particular, system errors are retained separately
from correct refusals; a failed negative case is not evidence of zero false
acceptance. The slices may overlap without duplicating whole-workload costs.

These request counts do not silently become atom-level extraction recall,
precision, false-acceptance rate, calibrated slice confidence intervals or
feature-specific promotion. The existing extraction evaluator can report
admitted-triple true/false positives and false negatives against independent gold;
the trusted raw-source adapter/judge must preserve those actual outputs for
business review. Each useful-fact request may be mapped to one independently
annotated fact, but that unit/denominator must be frozen explicitly. Aggregate
A9 `cost_utility_ready` remains only that existing gate's cost/utility scope.
`production_benefit_claim` remains false and
`feature_quality_promotion_assessed` is false. Do not use these slice counts alone
to mint a `SourceAuditApproval` or retrieval promotion receipt.

## Exact missing-input checklist for an actual business gate

The integration session has no supplied reachable model endpoint, approved real
corpus/gold or completed host accounting binding. The following remain required:

1. **Execution target and permission:** the exact reachable local Ollama endpoint,
   already-installed Qwen3.5:9B digest and runtime/configuration, permission for
   those named data to reach it, resource limits and an approved call/time budget.
   No endpoint, credential, GPU availability, model installation or free billing
   is inferred. Ollama byte bounds must not be represented as exact tokenizer
   enforcement.
2. **Authorized raw corpus and provenance:** the approved corpus/artifact location,
   license and scope/retention references; versioned raw sources, independent
   project/session groups, source authorities, valid/known-time history and the
   eight coverage slices. Existing synthetic examples cannot replace it.
3. **Independent held-out gold and business verification:** annotated useful
   atoms/spans/qualifiers, positive omissions and unsupported negatives, relation
   premise/lineage and temporal retractions, refusal/unknown/empty decisions, plus
   authenticated business verification sources/interfaces and their permissions.
   Generation, candidate review and source audit output are not their own gold.
4. **Precommitted judge and acceptance choices:** the independent judge/rubric
   artifact and hash, calibration reference, task-specific quality/cost limits,
   extraction/false-acceptance denominators, repetition count, minimum samples and
   independent groups, paired statistical protocol and frozen baseline/features.
   Slice-level business promotion requires its own approved evidence assessment;
   the wrapper supplies no default favorable threshold or fabricated approval.
5. **Actual host workflow binding:** a trusted gold-free A9 adapter connecting
   existing governed raw-source generation/review, domain verification, omission
   stage, project admission, shared QuestionViews and relation plans; isolated
   state per arm, current grants, legitimate feature activation/rollback authority
   and control attestation. This is host integration still to be supplied and
   verified, not an implemented real-business adapter in this preflight.
6. **Complete cost evidence:** the real host observer and frozen tariffs for CPU,
   GPU, I/O, storage and network, currency/units/rounding, actual measured model
   token/attempt receipts and authoritative billing reconciliation, including
   explicit evidence for any claimed zero local-provider price. Finite unfinished
   work and unresolved bills must be drained/reconciled or stay blockers.

Once these inputs exist, freeze the final integrated code and protocol, run the
real paired workload and inspect both quality and complete-cost evidence. Until
then M5 remains `CODE_DONE / AWAITING_INPUT`; no historical quality task is closed.
