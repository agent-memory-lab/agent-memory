# Generic benchmark denominators and uncertainty

`run_memory_benchmark` is a deterministic scoring contract. These metrics are not
public QA accuracy, evidence of an unseen holdout advantage, or proof of a bypass
in the public LongMemEval pipeline. This repair addresses the generic harness's
completed-case reporting, without changing the public QA runner or historical
results.

## Primary: all planned cases

For each arm, `planned_cases` equals `sample_count`, the number of cases in the
input dataset. `planned_mean_score` is the sum of completed scores divided by that
count. `planned_success_rate` is successful completed cases divided by that same
count. Per-dimension metrics apply the identical rule within each dimension.

A technical failure, explicit `BenchmarkSkip`, or unsupported-dimension skip
receives zero credit in these **operational, completion-adjusted** metrics. It
remains separately recorded as a failure or skip, with no invented observation or
`case_results` row. Zero credit does not assert that an unobserved answer was
incorrect or that a skip is a technical failure. Report coverage and both status
counts alongside scores. When all cases fail or skip, primary scores are zero,
not evidence of measured model quality.

Primary paired comparisons use `planned_cases`, `planned_mean_score_delta`, and
`planned_confidence_low/high`. Both arms are paired on every original dataset
case, including asymmetric failures/skips and cases where neither arm completed.
The delta is candidate operational score minus baseline operational score. An arm
cannot improve its own primary score or advantage by failing/skipping a case it
would otherwise complete. Baseline failures can increase the candidate's relative
operational advantage; that is an availability difference, not proof of superior
retrieval or reasoning. Both arms' coverage must remain visible.

Skips never shrink a denominator after outcomes are observed. If some dimensions
are intentionally outside a study's scope, freeze that dataset subset **before**
execution and apply it to every arm. Do not compare different arm-specific
subsets as a shared planned population. This harness does not authenticate
pre-registration or enforce the independence of an externally supplied dataset.

## Conditional diagnostics and compatibility

Existing Python constructor positions and legacy JSON fields remain available.
These legacy fields retain their original conditional meaning:

- Arm/dimension `mean_score` aliases `completed_mean_score`.
- Other legacy `mean_*` fields (precision, recall, evidence coverage and tokens)
  also describe completed observations only, not planned-case quality or full
  lifecycle cost.
- Arm `success_confidence_low/high` alias
  `completed_success_confidence_low/high`; these are completed-case Wilson
  intervals, without cluster adjustment.
- Comparison `comparable_cases`, `mean_score_delta`, and `confidence_low/high`
  alias `jointly_completed_cases`, `jointly_completed_mean_score_delta`, and
  `jointly_completed_confidence_low/high`. They use only jointly completed cases
  and a case-independent normal approximation. `incomparable_reason` still
  describes this legacy conditional comparison, not the primary planned metric.
- `completed_success_rate` is explicit for arm and dimension diagnostics.
  Historical empty-completion means/rates/arm intervals remain zero; the
  `completed_cases` count distinguishes absent evidence from measured zero.

These conditional diagnostics can have survivor bias and must not be presented
as primary advantage. JSON adds `metric_policy` and `legacy_metric_policy`, and
Markdown leads with planned metrics before separately labelled diagnostics.
Consumers should migrate rankings and decision rules to `planned_*` fields;
retaining legacy semantics avoids silently changing existing integrations.
New planned fields default to unavailable on manually constructed legacy metric
objects, while reports produced by the runner always calculate them. No schema
fields are removed. Consumers that reject unknown JSON keys must update their
schema. These contracts do not authorize feature promotion by themselves.

## Correlated cases and limits

`MemoryBenchmarkCase.cluster_id` is optional evaluator-side metadata, for example
a conversation/source identity shared by related questions. It must be a nonempty
string of at most 128 characters. It is **not passed to retrieval arms**. Completed,
failed and skipped outcome records preserve it so denominators remain auditable.
Do not encode a cluster in an arm's query or treat neutral generated request IDs
as cluster evidence. The evaluator must identify the correct independence unit;
the harness cannot establish independence from identifiers alone.

Primary uncertainty is reported with a method, cluster count and limitation:

- All cases have cluster IDs: use a cluster-robust normal approximation for the
  case-weighted mean or success rate. For N cases, G clusters and case mean m,
  the variance estimate is `G/(G-1) * sum_g((sum_i_in_g(x_i-m))^2) / N^2`.
  Cluster totals are centered on the **case-weighted** mean, so unequal cluster
  sizes do not silently change the estimand to an unweighted mean of clusters.
  Failures and skips contribute their zero-credit values to these totals.
- Some IDs are missing: primary intervals are unavailable. Do not drop missing
  cases or fabricate independent clusters for them.
- No IDs are supplied: planned success uses a Wilson interval, and paired score
  deltas use a normal approximation. Methods explicitly say case independence
  is assumed and warn that within-source dependence is not accounted for.
- Fewer than two known clusters, or fewer than two unclustered paired cases:
  no normal interval is reported. Zero empirical variance (including differences
  within four ULPs of the group/overall mean's floating-point resolution) yields an
  unavailable normal interval rather than a misleading point interval.
  A Wilson success interval with no successes remains nondegenerate.

Normal intervals use 1.96 standard errors and are clipped to the metric's support
([0, 1] for success rates, [-1, 1] for score deltas). They are descriptive
approximations, especially unreliable with few clusters; they are not corrected
for multiple comparisons, an exact small-sample inference, or a confirmatory
significance test. The legacy conditional intervals remain unadjusted and may be
degenerate. No interval solves selection bias, contaminated holdouts, unknown
correlation, scoring validity, or changes in the evaluation population.

## Regression checks

`tests/test_benchmark_denominators.py` covers the failure-induced survivor-bias
counterexample; explicit skips; all-missing and asymmetric outcomes; dimension
metrics; JSON/Markdown labels; cluster metadata isolation and validation; unequal
clusters; repeated observations within clusters; partial/one-cluster metadata;
and suppression of misleading zero-variance primary normal intervals.
