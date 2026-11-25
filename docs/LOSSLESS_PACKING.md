# Opt-in source-preserving evidence packing

This change addresses a retrieval boundary: evidence present in ranked candidates
can disappear or be truncated before the reader receives it. It does not improve
extraction, establish claim correctness, or guarantee answer accuracy.

## Activation and compatibility

Pass `lossless_packing_budget=LosslessBudget(count, tokenizer)` to
`build_local_kernel`, `build_postgres_kernel`, `AgentMemory.local`, or
`AgentMemory.from_plugin` with either native provider. Import `LosslessBudget` from
`agent_memory.retrieval.lossless_packing`. The option requires `packing_version="v2"`
and no passage reranker. Omit the option to retain existing behavior.

Unknown factory options raise an error. Native factories share validation through
`NativeRetrievalOptions`. Read `kernel.retrieval_options` to inspect the selected settings.
A custom recall pipeline owns its candidate selection and packing strategy. The
kernel still applies its configured complete-payload counter after final delivery
authorization, for both native and custom pipeline results.

`count(state, items)` must synchronously and deterministically count the complete
reader request rendered by the host, including prompt, dates, provenance, and
serialization overhead. The tokenizer name is descriptive metadata, not proof
that the callback matches a provider tokenizer. Synthetic character counters in
tests are not model tokenizers. Changing the request after retrieval requires
another count at the sending boundary; this module does not own model transport.

## Behavior

* Keep retained records whole, preserving upstream ranking. Oversized records are
  omitted with a reason rather than rewritten or silently clipped.
* Process ranked hits before unmatched state. A matching Claim uses its full
  authoritative state at that rank when it fits; otherwise its already-retrieved
  representation remains eligible. Retain only one representation of each Claim.
* Complete source events can replace derived renderings from the same source;
  current-state claims remain distinct because validity/status matter.
* Compare with the existing v2 selection. If any previously retained event text
  loses same-source, same-revision, aligned-span containment, explicitly fall back
  to v2. The fallback can contain excerpts and must also fit the supplied budget.
  Complete legacy events need no index locator: their actual text hash, length,
  and absolute excerpt offsets must prove the same containment.
* Fail closed for invalid counters, excess prompt overhead, and over-budget
  fallback or final selection. Counter exceptions propagate.
* After the authoritative delivery guard, synchronously recount the actual final
  selection without restoring removed evidence or introducing another await.

Diagnostics expose omission positions/reasons, fallback, candidate source proofs,
and the final-delivery recount. They are not completeness or truth guarantees.
The packer does not read benchmark answers, labels, or question identifiers.

## Scope and limitations

This is an opt-in retrieval feature, not an enabled-by-default production rollout.
It cannot recover evidence absent from candidates, guarantee all relevant sources
fit, validate derived facts, or make model output obey citations. Repository
source authorization remains the existing delivery guard's responsibility.
No new model adapter, billing ledger, private benchmark artifact, or paid request
is included. Four-time storage changes, reviewer-reason migration, aggregation
primitives, and experimental guarded-reader transport are deliberately separate.
Tests cover synthetic evidence and real local SQLite integration; they do not
establish improved LongMemEval accuracy or superiority over other frameworks.
