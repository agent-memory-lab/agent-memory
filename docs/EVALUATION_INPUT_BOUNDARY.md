# Evaluation arm input boundary

The deterministic policy/retrieval harnesses are scoring contracts, not evidence
of real retrieval or answer quality. Their scripted test arms deliberately return
fixed observations to exercise scores, failures, uncertainty and promotion gates.
They must remain labelled as contract fixtures; controlled `contract-only` results
cannot authorize feature promotion.

## Python protocol compatibility change

`run_memory_benchmark`, `run_retrieval_benchmark` and
`run_controlled_retrieval` now pass **`RetrievalEvaluationInput`** to arm
`retrieve(request)` / `run(request, regime)` methods. Import it from
`agent_memory.evaluation.retrieval` (also exported by `agent_memory`). It is a
frozen, slotted value containing only:

- `query_text`: the task query;
- `scope`: the authorized memory scope;
- `case_id`: a runner-generated neutral ordinal such as `case-000000`, stable
  across arms and cold/warm regimes within the run.

This deliberately breaks arms that consumed `RetrievalEvalCase`,
`MemoryBenchmarkCase` or `ControlledRetrievalCase` annotations. Migrate retrieval
implementations to query and scope, not gold IDs or answerability. Original case
IDs can encode labels, so they are not forwarded; reports retain original IDs.
Do not use ordinal IDs as persistent identities across dataset revisions.

Relevant/forbidden memory IDs, required evidence, expected ordering, success
thresholds, dimensions, answerability and group labels remain scorer-side.
`CallableMemoryBenchmarkArm.supported_dimensions` is still supported: the runner
applies the dimension filter before invoking the callable; the callable no longer
receives a dimension. Direct `retrieve` calls take the new input and do not perform
that scorer-side filter. Dataset/report schemas and scoring formulas are unchanged.
Controlled answer/evidence scores still require the host's independent pinned
judge; the harness does not manufacture or authenticate those judgments.

This boundary prevents accidental gold access through the request, including the
old `request.relevant_memory_ids` oracle. It is **not** a security sandbox against
hostile Python introspection or an arm separately given the dataset. Hosts must
keep scoring annotations out of arm construction, corpus indexing and prompts,
and supply legitimate queries/scopes without embedded answer labels.

The public LongMemEval pipeline already separates tested-system input from gold;
this repair concerns the generic contract harnesses, not a finding of cheating in
that pipeline. No model-quality claim or new paid-model experiment is implied.
