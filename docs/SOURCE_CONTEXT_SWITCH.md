# Optional post-ranking source context

The native kernel now accepts `enable_source_context: bool = True`. This setting
is available through `MemoryKernel`, `build_local_kernel`, `AgentMemory.local`,
and `AgentMemory.from_plugin("sqlite", ...)`. It is separate from packing
versions. The default still performs the existing post-ranking adjacent-source
reads; `False` opts into ranked-only input. Pass an actual boolean, not a string
or integer. Invalid values fail before opening the SQLite database.

```python
from agent_memory import AgentMemory, MemoryScope

async with AgentMemory.local(
    "memory.db", scope=MemoryScope("example", session_id="session"),
    enable_source_context=False,
) as memory:
    bundle = await memory.recall("What changed in the archive plan?")
```

The switch only suppresses additional `expand_source_context` reads after
reranking. It does not change recall, embeddings, reranking, candidate narrowing,
packing policy, source locators, admission, tenant permissions, erasure, or the
final delivery guard. It does not remove context already present in ranked
candidates. Direct `pack_native_evidence` calls still pack exactly the candidates
supplied by the caller. Configured external recall pipelines are unaffected.

## Compatibility and audit

- Existing calls and v2/v3/v4/v5 packing contracts remain unchanged. No database
  migration, new packing version, dependency, model, or network request is added.
- The existing `source_context_expansion.status` metadata gains the additive
  value `disabled_config`; its anchor/candidate counts are zero. Historical
  queries retain `disabled_historical` and non-semantic requests retain
  `disabled_channel`. Existing default-enabled metadata is unchanged.
- Traced retrieval reports an empty `source_context` stage and identical
  `reranked`/`packing_input` stages when disabled. Existing authoritative delivery
  checks and persisted pre-delivery traces still run. The new setting is visible
  in returned retrieval metadata; no new durable trace field is added.
- Reopen the native kernel with the option omitted or `True` to roll back. The
  option does not mutate stored records or alter the saved defaults of another
  kernel. `UnifiedMemory.local` is not changed by this patch.

## Evidence and limits

This is an optional operational control, not a promoted default or proof of
better answers. A previous frozen diagnostic on 470 already-exposed LongMemEval
questions found that disabling context under v2 raised annotated-source recall
from 70.8901% to 71.6773% (10 improvements, 1 regression) and single-fragment full
body retention from 68.4894% to 69.3901% (8 improvements, no regressions). Those
results informed this implementation and are not independent evaluation.

Adjacent reads can recover a qualifying statement, negative clause, speaker or
time context that ranked windows omit. Disabling them can lose that information.
This patch deliberately leaves exact excerpt construction unchanged and does
not introduce internal-turn spans, detached headers or text synthesis. It does
not guarantee semantic completeness or arbitrary negation preservation. Existing
prefix excerpts can still omit later qualifications; preserving all such context
is a separate unsolved contract. Source text headers are literal labels, not
verified authorship or event time.

The adjacent developer report records the frozen real-path replay, regressions,
source/hash/budget checks and software-test limits. Retrieval diagnostics are not
QA accuracy, answer sufficiency, hallucination measurement, or unseen holdout
performance. Do not promote this option based only on the exposed benchmark.
