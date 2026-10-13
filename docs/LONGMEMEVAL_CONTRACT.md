# Legacy LongMemEval experiment validity

The optional `examples/longmemeval_ollama_comparison.py` runner uses the **legacy
scored-claim** extractor. Its previous key/value/text-only output schema omitted
the explicit numeric confidence required by `GeneratedTrajectoryClaimExtractor`.
A schema-conforming model could therefore produce useful proposals while the
extractor accepted none. Such results cannot establish extraction/retrieval quality.

## Contract repair

The schema and prompt now require the model to supply a numeric score in [0, 1].
No score is invented or defaulted to 1. The existing 0.8 threshold remains.
Malformed responses fail visibly; legitimate empty output and low-score output
remain valid. Model confidence is not verified truth. This change is restricted
to the old comparison example: new Atom proposals still forbid model confidence
and retain their independent evidence, review and admission rules.

Offline fixtures drive schema-shaped scripted responses through the real local
capture, extractor, SQLite storage and retrieval path. They require a nonzero
stored fact, exact expected value/text and source provenance. Other tests cover
missing/invalid/low confidence, empty output, UTF-8 segmentation and stale runs.
These are deterministic engineering contracts, **not a quality benchmark**.

## Frozen inputs and diagnostics

A run manifest fixes the input file hash, selected ordered question IDs, model
configuration, generator schema/prompt, claim threshold, chunk size, retrieval
limit, runner/client-adapter/core source hashes and installed client versions. Reusing an output directory with
changed settings or historical unversioned JSONL is rejected. Rows bind the run
identity and the exact timestamped input stream hash. Namespaces include the run
identity. The same chunks reach all arms, but backend extraction policies,
Graphiti history context and budgets remain different: this is not a controlled
causal comparison of extraction algorithms.

Each Agent Memory row separates generated proposals, legacy-validated drafts,
current stored facts and retrieved items. Current fact count is not a cumulative
admission count: corrections can replace previous facts. A zero count is visible,
not automatically labeled a failure because no-fact input is legitimate. Failures
record arm/question, counters and exception type without provider error strings.
Answer/judge usage and Agent Memory generation usage retain provider-reported
counts; unavailable counts are null. External backend calls, embedding work,
local hardware cost and tariffs are not fully accounted for. No whole-cost or
cost-saving claim is supported by this runner.

The manifest records configured model tags, not immutable model-weight digests
(which can change even when installed package versions do not). Freeze weights externally and use fresh output
and work directories for each model/runtime change; never compare or resume a
run across such changes. The A9 and controlled-retrieval frameworks remain the
place for independently frozen groups, matched budgets and promotion decisions.

## Reproduction and next evidence

Run `python -m pytest -q tests/test_longmemeval_contract.py
 tests/test_trajectory_extraction.py tests/test_comparison_adapters.py` from an
installed development checkout (put the command on one line). No Ollama,
Graphiti, Mem0, paid API, downloaded weights or third-party core runtime is
required for these tests.

A live comparison requires separately installed optional clients and an explicit
local Ollama endpoint. Keep dataset licensing, independent gold and held-out
splits outside extraction inputs. Actual model-quality/cost results remain open;
no production default, novel-method claim, release or deployment follows from
this repair. Future method experiments should isolate one mechanism, preserve
an unchanged baseline and report failure categories as well as aggregate scores.

## Research protocol: derive methods from failures

Before proposing a new mechanism, independently annotate a held-out trace into
these stage failures (multiple labels may apply):

- Required evidence exists in raw sources but was not extracted.
- An extracted/admitted fact exists but was not retrieved.
- Retrieved support was dropped, or its condition/negation/time qualifier was
  lost during packing.
- Stale or superseded evidence was used.
- Delivered support was sufficient but the answer ignored or distorted it.

These labels require independent supporting-span/qualifier judgments and stage
artifacts. A zero counter alone does not identify the causal failure. This
legacy runner does not yet preserve all intermediate candidate/packing traces;
missing stage evidence must remain **unknown**, not count as a clean result.
Use the existing A9/controlled-retrieval protocol for deeper stage traces, rather
than introducing a parallel benchmark or leaking gold into runtime extraction.

Freeze train/development/held-out groups by user or source family before tuning,
retain the same accessible evidence and independent gold, and match total
candidate/context/call budgets. Preserve the existing baseline, compare one
mechanism at a time, report per-group paired outcomes, quality uncertainty,
unsafe/stale deliveries, latency and measured whole cost with unknown debt.

One unproven research hypothesis is persistent records of still-unmet evidence
requirements across queries and source revisions. It should only be pursued if
real failures show reusable missing-evidence patterns. Compare it against the
existing pipeline, equal-cost wider retrieval, a fresh per-query gap check,
saving only corrected facts and an ordinary cache. Source revisions, scope
changes and erasure must invalidate any such records. Persistence alone is not
a novelty claim; prior art and causal ablations must establish any contribution.
No such mechanism or measured quality improvement is implemented by this repair.
