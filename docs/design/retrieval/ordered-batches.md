# Retrieval correctness and efficiency batches

## Frozen control and acceptance boundary

The control is main `629b3c4af8f42f2ee834b9acc118b28b3d8a8623`, tree
`20d73af25bbfa207e52ef5fbe458f6b07ed434fa`. Comparisons must keep corpus,
questions, embedding specification, reader model, prompts and judgments fixed.
Each batch is reviewed and merged separately after exact-head CI; post-merge
main CI is checked before the next publication. Historical V7 task statuses and
unaccepted real-quality/whole-cost gates are not changed by these fixes.

Synthetic fixtures establish regressions, bounds and governance behavior. They
cannot establish deployment accuracy, answer quality, speedup or cost benefit.
Real-model acceptance still needs the actual local Ollama Qwen3.5:9B endpoint,
pinned runtime/hardware, authorized domain corpus and held-out labels, frozen
judgments/thresholds/statistical plan, and repeated end-to-end cold/warm runs.
No model download, paid call or service deployment is part of these batches.

Required comparison dimensions: candidate recall, final-context evidence
coverage, answer correctness and abstention, forbidden evidence, source rows
hydrated, embedding/reranking/generation calls, p95 latency, and ingest/index/
embedding/refresh/erase maintenance cost. Unknown measurements remain unknown.
Warm hits cannot hide cold preparation or maintenance costs. A top-k result or
an early exit is partial evidence, never proof of all risks or of absence.

## Batch 1: constraints and bounded candidate selection

- Preserve every `MemoryQuery` field when applying a per-plugin candidate cap.
  Built-ins exclude unrequested channels; current-only plugins reject explicit time axes.
  The lexical adapter filters its bounded pool before ranking. Existing semantic,
  entity and temporal index ports cannot accept channels upstream; their finite
  top-k pools are filtered before fusion and can still underfill. No exhaustive
  channel-specific search is implied by those adapters.
  The bitemporal plugin resolves each unspecified axis to the same clock value.
- Preserve the existing public historical route through native provider state.
  Direct governed-pipeline historical calls fail explicitly because its current
  governance resolver cannot certify historical snapshots. Current access,
  expiration and erasure checks are not evaluated using a historical clock.
- The default governed pipeline requests bounded headroom in one wave, capped
  at 256 candidates across plugins, 100 per plugin and 100 fused candidates.
  Resource limits remain authoritative; explicitly injected orchestrators can
  impose lower caps. There is no retry loop or completeness claim.
- Policy checks precede final item limits. Source/kind quotas are spent only on
  evidence that fits the token/character budget, allowing bounded backfill after
  oversized or forbidden higher-ranked candidates. Individual evidence text and
  citations are not rewritten or truncated by this packer.
- The shared versioned ASCII/Han analyzer makes Han unigrams and bigrams only
  within contiguous Han runs. Punctuation and Latin characters break adjacency.
  PostgreSQL native `simple` FTS/GIN stays intact; this is not a claim of identical
  SQLite/PostgreSQL free-text ranking. Shared temporal scoring uses the analyzer.

Pipeline and query policy identifiers remain separate metadata. Request/run IDs
are preserved, and ordinary retrieval explicitly reports partial coverage and
`world_negative=false`. These flags do not replace registered-question census
certificates, proof freshness or negative dependencies.

## Remaining ordered work

2. Incremental SQLite lexical indexing and exact long-document chunk lineage;
   opt-in, scope/version/erase-governed candidate embedding reuse. Preserve the
   default embedding reranker's no-retention behavior.
3. Cheap adaptive bounded retrieval and evidence-coverage-aware packing; reuse
   certified standing-question routes and preserve conflict/completeness states.
4. Optional compact pair-scoring reranker and governed evidence-pack cache;
   controlled real quality/whole-cost evidence is required before promotion.
   Synthetic scores do not qualify as a successful efficacy experiment.

Detailed token compatibility: [lexical analysis](../../LEXICAL_ANALYSIS.md).

## Reproducing the synthetic comparison

Run each immutable checkout in a fresh process with
`python tests/operational/retrieval/run_frozen_comparison.py --source-root CHECKOUT --out EXTERNAL_DIRECTORY --label SOURCE_REVISION --repeats 9`.
The fixture and tracked-source hashes are recorded in the resulting manifest.
`compare_frozen_reports.py --help` describes comparison of two full result files.
[The compact control report](baseline-summary.json) records mechanism failures,
logical SQL result materialization and uncontrolled-host latency, not physical
rows scanned, answer quality, production performance or financial cost.
[The comparison plan](controlled-comparison-plan.json) leaves unmeasured gates open.

## Batch 2: retrieval index and source embedding reuse

SQLite now ranks versioned lexical chunk locators before bounded hydration;
source text remains exact and original IDs remain subject to native admission.
See [migration/lifecycle contracts](../../SQLITE_LEXICAL_INDEX.md). The source-bound
embedding cache is a separate host-only opt-in with authoritative retained-source
revisions, processing grants, pinned model specification and erasure/restore
integration. Generic MemoryQuery reranking and query embeddings retain their prior
no-retention behavior; no model is bundled or called by default.

[The synthetic comparison](batch2-synthetic-comparison.json) shows improved old/long
evidence retrieval and lower logical result materialization, along with explicit
maintenance cost. On its one uncontrolled-host 534-event ingest sample, indexed
ingest took about 88 ms versus 44 ms, and allocated SQLite pages were about 5.0 MB
versus 0.79 MB. These measurements demonstrate a write/storage tradeoff, not a
production cost win; broad corpus sweeps and real cold/warm whole-workload evidence
remain unmeasured. The indexed path is not substituted for any complete census or
negative-proof query, and existing V7 real-quality gates remain open.
