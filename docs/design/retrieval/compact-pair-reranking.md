# Optional compact pair reranking and controlled promotion

## Status and scope

This is a dependency-free, host-only contract and adapter. It is **off by default**.
There is no bundled model, model download, transport, endpoint assumption, runtime
installation, or claim of accuracy, latency, or cost improvement. The local
Ollama Qwen3.5:9B endpoint was unavailable for this batch. Real efficacy remains
blocked on an authorized corpus, installed local scorer and reader, frozen judge,
and measured controlled runs. Contract fixtures are not promotion evidence.
Historical V7 acceptance records are unchanged.

The candidate compact scorer is Qwen3-Reranker-0.6B, if the host has independently
installed and approved it. An Ollama chat response containing a number is not its
scoring contract. `FinalTokenPairScorer` receives a host-injected async callable
that returns the actual final-answer-token `yes` and `no` logits for every pair.
The host owns exact official prompt rendering, tokenizer, final-position lookup,
token identities, truncation behavior, quantization, and local execution. This
adapter does not pretend to implement or verify an uninstalled runtime.

## Contracts and safety

Import new APIs from their owning modules:

- `retrieval.pair_reranker`: `PairModelSpec`, `PairCandidate`, `PairScore`,
  `FinalTokenLogits`, `FinalTokenPairScorer`, `CompactPairReranker`
- `retrieval.feature_gate`: `RetrievalFeatureApproval`,
  `validate_feature_enablement`
- `evaluation.controlled_retrieval`: frozen plan, four-arm runner, and assessor

The model, tokenizer, prompt, runtime and quantization must each be pinned by a
SHA-256 identity, with exact distinct yes/no token IDs. The spec says
`host-local`; it cannot authorize a remote model destination. A trusted host
must implement and verify this property of its injected callable.

The rank-only port gets a tuple of at most 64 immutable pairs, not mutable memory
objects. Each pair contains only exact memory identity, unchanged text and
unchanged source IDs. The wrapper defaults to 32 pairs, 131,072 input characters,
and a 10-second scoring timeout. The entire retrieved wave remains bounded by
existing governance limits. No hidden refill, rewrite or truncation occurs.
Unscored tail candidates retain original order after the scored head.

The scorer returns exactly one finite numeric score for each exact input ID.
Duplicate, missing, extra, malformed, boolean, NaN and infinite results are
rejected. The real-score adapter orders by `yes_logit - no_logit`, rejecting
nonfinite differences. It computes the sigmoid stably for telemetry only:
probabilities can saturate and are **not calibrated confidence**. No numeric
blending with lexical, vector or reciprocal-rank-fusion scores is performed.

Before dispatch, the explicit host processing-authorizer receives scope, exact
candidate/source IDs, payload digest and configuration digest. A second check
after scoring prevents ordinary model errors from bypassing authorization.
Permission errors, changed model/input identity, revocation and unavailable
authority fail closed. An ordinary scoring exception or timeout produces an
explicit degraded original-RRF fallback with only the exception type in traces.
Error bodies and source text are not echoed into diagnostics.

`GovernedRecallPipeline(..., pair_reranker=...)` first authoritatively guards all
candidates, scores only accepted local material, then **resolves and guards the
same candidate versions again after all scoring/authorization awaits**. The
resolver must revalidate current source revisions, authorization and retention
against authoritative storage. Missing, changed, revoked, expired or deleted
material fails closed before packing. The final synchronous promotion verifier runs before candidate equality and
current-clock/expiry checks, so a callback that crosses an expiry cannot deliver
expired material. There is no further await or host callback between this last
candidate validation and synchronous packing. Current-state claims remain
owned by the existing kernel/provider path; this adapter does not score claims
supplied separately as current state or provide transactional snapshot isolation.

Packing sorts on a deterministic rank-derived selection score. Original RRF,
raw pair score, pair probability and pair rank are separate fields; packed
metadata retains the original `fusion_score` and labels the selection score
`pair-rank-order/1`. Memory bodies and citations stay unchanged.

## Activation, qualification and rollback

Construction alone does not enable anything. Set `enabled=True` only with a
`RetrievalFeatureApproval` bound to the exact configuration, report and protocol
digests, a rollback identity, and a synchronous trusted `verify_approval` callback
that returns exactly `True`. The verifier is checked on every rank invocation,
after every scoring/processing-authorization await, and after the pipeline's last
governance await. Revocation or an unavailable verifier fails closed, including
a scoring-error path. Primitive approval/configuration fingerprints and the
original scorer/verifier identities are pinned; replacing or mutating them
requires a new ranker instance. The callback must authenticate the report artifact,
real measurement provenance and approved scope. A receipt is a value object, not
a cryptographic authority token; constructing one is not self-authorization.

For isolated tests only, `enabled=True, contract_test_only=True` bypasses efficacy
qualification and explicitly labels every ranking trace `contract-test-only`.
It cannot carry production approval. Local source processing permission and
post-await governance still apply. Contract-mode output cannot qualify a real
promotion report. No defaults or SDK/MCP model-facing enablement changed.

Rollback is to omit the ranker or construct it with `enabled=False`; the original
bounded retrieval/fusion/guard/packing path remains. There is no new persistent
ranking state or migration. Existing positional `FusedCandidate` construction
is compatible; four optional score-metadata fields were appended. Existing
`GovernedRecallTrace` gains an optional pair trace. The legacy benchmark keeps
required no-memory/lexical-only/hybrid names by default and now allows at most
16 total arms; a caller must explicitly supply another required set. Failed
calls now contribute latency and unreported calls/costs/resources stay unknown.

## Controlled experiment and host responsibilities

Freeze `ControlledRetrievalPlan.fingerprint` **before** execution, then pass that
exact fingerprint to runner and assessor. The version-2 plan freezes the case/group and
answerability manifest, corpus, model, judge, required-support/qualifier annotation
manifest digest, baseline/rank/reuse configurations,
all regression limits, each arm's primary benefit, sample minima, bootstrap seed,
resamples and confidence. Stable digests do not by themselves establish license,
permission, domain representativeness or independent judge quality.

Exactly these arms are required:

1. `baseline-b3`: B3 plus existing optimized QuestionService read/reuse
2. `b3+rank`: the same baseline plus compact pair reranking
3. `b3+cache`: the same baseline plus registered evidence-pack reuse
4. `b3+both`: both optional changes

The baseline is explicitly `b3-existing-question-service-read-and-reuse/1`.
Forcing regeneration or disabling already-available QuestionView reuse is an
invalid control. Existing baseline reads can already make zero model calls, so
reuse may show no benefit and must then remain unpromoted. This harness does not
create a generic text/result cache; the registered QuestionView owner retains
publication, source generation, proof and erasure semantics.

Each arm implements `prepare(regime)`, `run(case, regime)` and `finish(regime)`.
The host must isolate arms, verify/reset cold state, establish the frozen warm
state, keep corpus and model/judge identities fixed, and return all measured
preparation/warmup/index/embedding/refresh/erase/drain costs. Arm order is seeded
and counterbalanced across cold/warm regimes. Independent repeated runs require
precommitted case/group identities; repeated draws are not independent projects.

Observations cover candidate recall, final-context document-ID recall, independently
judged supporting-span/required-evidence coverage, qualifier fidelity, answer score
and refusal on unanswerable cases, forbidden candidates/context,
explicit unsafe/stale delivery counts,
actual elapsed success/failure latency, model/embedding/reranking/generation call
counts, source rows hydrated, money, tokens, CPU, peak RSS and retained bytes.
Document-ID recall is diagnostic; retaining one word with a correct document ID
cannot establish complete supporting evidence. On every applicable answerable
case, the independent pinned judge must compare the actual delivered context
against the frozen required supporting spans and qualifiers, return coverage and
fidelity scores in [0, 1], and identify its saved judgment artifact by SHA-256.
The host verifier authenticates that artifact and its binding to the delivered
context, frozen annotations, and independent judge. Missing annotations, receipt
or either judgment remains unknown and blocks promotion; no judgment is inferred
from document IDs. Both metrics have explicit precommitted non-regression limits
and paired group confidence intervals in each regime.

All cases, including refusals, also require independently measured unsafe and
stale delivery counts. These include unauthorized/revoked authority, forbidden
scope, outdated source revisions, expired material, or unsafe evidence delivery
even when its document ID is otherwise allowed. Absence of a forbidden-ID match
or an exception is not a measurement of safety. Each count must be a bounded
integer and exactly zero to qualify; nonzero, unknown, and failed observations
block promotion. These absolute safety gates cannot be relaxed by statistical
regression thresholds.

Preparation and final maintenance wall time and additive costs are allocated
equally to the triggering case workload. RSS/storage are workload maxima, not sums or average per-query peaks; their paired
intervals compare workload maxima on each resampled group draw. Both
foreground and whole-workload mean/p95 latency comparisons are recorded. Unknown
costs remain `None`; zero must be an actual measurement. Per-arm lifecycle costs
must include failed work, not merely successful output receipts.

The assessor resamples paired project/session groups, preserving within-group
case dependence, and produces candidate-minus-baseline intervals separately for
cold and warm. Latency p95 uses paired group draws and differences of each arm's
p95, not the mean of per-case differences. Undefined bootstrap replicates are
retained as blockers. Missing/failed metrics, forbidden evidence, insufficient
independent groups, changed controls or contract-only provenance block all
promotion. Every required metric must meet its precommitted regression bound,
and the arm's primary benefit must be strictly established in **both** regimes.
The host verifier still authenticates any resulting receipt before activation.

Run contract validation locally, without a model:

```bash
python -m pytest tests/test_compact_pair_reranker.py \
  tests/test_controlled_retrieval_promotion.py tests/test_retrieval_evaluation.py
```

Version-2 plan/report schemas add these evidence and safety requirements.
Version-1 controlled artifacts cannot qualify activation; they need a new frozen
protocol and measurements rather than invented zero/one values.

No real experiment result is committed by these tests. Quality, p95 speedup,
whole-cost savings, PostgreSQL runtime claims and production approval remain
unestablished until their corresponding real controlled evidence is available.
