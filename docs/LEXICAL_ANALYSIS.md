# Shared lexical analyzer: `ascii-han/1`

`agent_memory.retrieval.analyzer.lexical_terms` is the single source of terms for
SQLite event/artifact/block overlap, optional in-memory BM25, and the temporal
claim scorer shared by SQLite and PostgreSQL. BM25 preserves term frequency;
overlap scoring uses unique terms. BM25 candidate metadata includes
`lexical_analyzer_version` for inspection.

The version freezes these rules:

- Unicode casefold followed by ASCII `[a-z0-9]+` runs, including one-character
  words and digits. Hyphens and underscores separate words.
- U+3400–U+9FFF runs emit each character and every adjacent two-character term.
  Repetitions are retained. Punctuation, whitespace, Latin, numbers, emoji, and
  any character outside that range end a run. `上，海` never emits `上海`.
- No NFKC normalization, stemming, stop-word filtering, dictionary segmentation,
  transliteration, or support for CJK extension characters outside that range.
  This is lexical matching, not a semantic model or model-token estimator.

## Compatibility

The optional BM25 term semantics are unchanged. SQLite now follows those rules:
it no longer invents Han bigrams across separators, retains single ASCII letters
and digits, splits hyphenated/underscored identifiers, and casefolds before
matching. Temporal claim ranking now uses the same token overlap rather than
whitespace-delimited substring matching. Claim values are serialized with their
actual Unicode characters for analysis, so Chinese values remain searchable.
Existing score weights, evidence IDs, visibility rules, time selection and
nonlexical fallback policies are unchanged. Relevance ordering can change.

This batch adds no persisted lexical index, storage migration, or domain schema
change. The analyzer version identifies token semantics, independently of the
package version. Any future persisted index must bind this version to both its
documents and queries, refuse incompatible use, and rebuild on a term-semantics
change. Changing the alphabet, normalization or boundary rules requires a new
analyzer version; updating only a version label is insufficient.

## PostgreSQL boundary

Ordinary PostgreSQL event/artifact/block search and current-claim lexical ranking
still use `search_document @@ plainto_tsquery('simple', ...)`, `ts_rank`, and the
existing GIN indexes. PostgreSQL's native `simple` analyzer is not `ascii-han/1`:
for example, native FTS does not produce the `上` unigram from `上海`. Historical
claim scoring uses the shared analyzer after the backend selects visible temporal
claims, but native current-search scores and candidate eligibility are not promised
to match SQLite or BM25. No fallback full-table Python scan replaces native FTS.
Full indexed lexical parity requires a separate versioned index migration.

Tests freeze term output, Chinese boundaries, mixed-script/identifier handling,
SQLite ranking and scope/evidence behavior, shared temporal scoring, and retention
of PostgreSQL's parameterized GIN-compatible SQL. The optional live PostgreSQL
test characterizes the native Chinese boundary; it requires a disposable test
DSN and does not substitute for live PostgreSQL verification when skipped.
