# SQLite lexical candidate index

This implementation adds a SQLite-only retrieval locator index. PostgreSQL keeps
its existing GIN/HNSW implementation. It does not replace the admission ledger,
complete-census graph/question indexes, final governance, or source evidence.
It is an implementation and regression-test milestone, not retrieval-quality or
real-model acceptance evidence.

## Compatibility and migration

`SQLiteMemoryRepository.initialize()` installs additive `lexical_*` tables and
SQL triggers, then atomically backfills existing event, artifact and claim-version
sources. The state binds `sqlite-lexical/2`, the shared `ascii-han/1` analyzer, and
the chunk geometry. A version change rebuilds locators from authoritative sources;
ordinary initialization only resumes pending changed IDs. No event or claim
schema, PostgreSQL index, wire protocol, or governed locator schema changes.

Repository writes consume the dirty-ID queue before their source transaction
commits. SQL triggers invalidate outdated locators immediately, including writes
from older/direct SQL clients. A read after a committed legacy update repairs only
those IDs in a transaction before starting candidate retrieval. Backfill/repair
uses batches of 128 source identities. That bounds each fetch, not total work:
initialization or a version rebuild still processes the whole corpus and can hold
the writer lock for its full transaction, with CPU, database and WAL growth.
Schedule large upgrades in a maintenance window with capacity headroom. Errors
roll back source publication and its index changes together. Concurrent reads use
one SQLite snapshot.

The tradeoff favors read-heavy retrieval: indexed lookups avoid repeatedly loading
and tokenizing the corpus, while every source publication pays analysis and
posting-update costs before commit. Write-heavy deployments should measure that
cost on their document sizes, structured values and retained history. SQL triggers
also remain active if an older writer is rolled back into service: it invalidates
changed locators and queues dirty IDs, but cannot maintain this index itself.
Current-host initialization or read repair must consume that backlog before using
the index again. Do not drop its triggers while writers are active.

Archive and erase invalidate event/artifact locators. Claim-version eligibility
also requires a live claim and surviving source evidence. Physical deletion of
claim history cascades to its lexical locators. Purge-journal replay uses these
same transaction/trigger paths, so restoring an old content backup does not
restore evidence deleted by the pinned journal. The index is derivative and does
not weaken tombstone/replay fences.

## Candidate and evidence contract

Queries use the indexed `(partition_key, term)` posting key, rank matching chunk
locators in SQL, select the best chunk per original source, and apply a candidate
bound before evidence hydration. Event lookup retrieves only the selected source
substring, not a full long event. Nonempty lexical queries do not use unrelated
no-hit sources as filler. Up to 1,024 unique query terms are considered, in analyzer
order; this covers the optional adapter's 512-character query bound. Native
repository candidate pools are capped at 2,400 per source family; the optional
lexical adapter keeps its existing 512-item cap. Candidate bounds are not source
retention windows: every indexed live source remains eligible regardless of age.

Chunk text is a contiguous, verbatim source substring, at most 1,024 Python
characters, with 128-character overlap. This geometry is fixed, versioned
implementation behavior, not a public tuning knob. Internal database-encoding
byte offsets (UTF-8/UTF-16) allow bounded SQL
substring reads to preserve embedded NULs and Unicode while public offsets stay
Python character coordinates. There is no 2,048-character document
exclusion, generated summary, or latest-512-event corpus cut-off. The overlap
retains ordinary terms/phrases across chunk boundaries; the analyzer is still a
lexical heuristic, not arbitrary-length phrase search or Chinese segmentation.
Claims also index their key/value fields for candidate matching. Their metadata
term frequencies contribute only to the first text span of each authoritative
claim revision; later spans index only their own text. A metadata-only match can
therefore select the first span even when its text does not contain the matching
key/value term. The quoted text and span still describe only original claim text,
never a fabricated key/value quote. Direct storage permits empty Claim text, so
those revisions retain one zero-length `[0, 0)` span for metadata lookup; the
higher-level ClaimDraft API continues to reject empty text. The new index version
forces an atomic rebuild of the older metadata-per-chunk layout.

Original event/artifact/claim IDs and `source_event_ids` remain unchanged. Metadata:

- `lexical_chunk_id`: opaque deterministic chunk locator
- `source_revision`: SHA-256 of full source text encoded as UTF-8
- `source_span`: zero-based `start`, exclusive `end`, and `unit: characters`
- `source_chars`: original source-text length
- `lexical_analyzer`: analyzer compatibility identity
- `excerpt`: true when the selected span is shorter than the source
- Claims additionally retain `source_revision_id` for the temporal revision
- Artifact metadata retains its existing `version`

These offsets identify the retrieved chunk. Any later presentation transform must
retain exact evidence or separately describe its narrower span; it must not claim
rewritten text is that source substring.

`SQLiteRecentEventEvidenceSource` retains its import/class name for compatibility.
Its new query-aware `search()` is preferred by the scoped lexical adapter;
legacy `load()` preserves newest-first source ordering as a bounded browse
interface, without a relevance promise. It returns the first exact chunk for long
sources. Query-aware `search()` has no source-age exclusion.
Custom sources that only implement `load()` continue to work unchanged.

## Storage growth and rollback

For one claim revision, let `T` be the sum of distinct analyzed text terms over
its overlapping spans, and `M` the distinct analyzed key/value terms. The number
of posting rows is at most `T + M`, rather than `T + number_of_spans * M`.
Metadata frequencies may merge into an already-present first-span term. A term
that independently occurs in later source text still gets a normal text posting
there. The locator index does not store an additional full raw evidence body:
source text is hydrated from canonical tables. Its analyzed terms and lineage are
nevertheless sensitive derived data and need the same access and deletion care.
This bound is per authoritative revision; retained temporal revisions, canonical
source text, long term strings, SQLite B-trees and transaction/WAL overhead still
consume storage. It is not a byte-size or disk-quota guarantee.

There is no hard disk quota, automatic eviction, retention change, or total-source
size cap in this feature. Retrieval candidate limits constrain hydration, not
index bytes or write/migration work. Hosts should measure representative database
and WAL growth, reserve migration disk headroom, and manage their own storage and
backup policy. Removing rows makes SQLite pages reusable but does not guarantee
immediate filesystem shrinkage or forensic removal of older database/backups.

Schema/analyzer/layout rebuild and the compatibility-state update commit in one
transaction. A failed rebuild leaves the previous index/state intact and can be
retried; a failed ordinary index publication rolls back the accompanying source
write. Before an application upgrade or rollback, take a consistent SQLite backup
and coordinate writers. Keep canonical sources and deletion history authoritative;
do not edit the compatibility marker to disguise an incompatible layout. Restored
content backups still require the pinned purge-journal replay before serving
reads. Reinitializing a different index version rebuilds derivative locators and
may require substantial temporary storage; reverting application code is not a
storage-reclamation procedure.

## Authorization boundaries

The optional source remains exact-scope. Native lookup retains inherited-scope
visibility and authoritative source scope checks. Temporal eligibility and
surviving evidence are checked before claim hydration. Direct artifact evidence
validity is checked in SQL. Ranked locator pages undergo typed/transitive
dependency-header validation before evidence hydration, so rejected stale rows do
not consume the accepted candidate budget. Validation is capped at 2,400 artifact
locators; unresolved additional candidates raise an explicit capacity error rather
than silently claiming an exhausted result. Dependency graph headers may extend
beyond that candidate count, but do not hydrate evidence bodies. Existing final native admission and governed policy checks
remain mandatory. Index membership is never evidence admission or permission to
deliver a source.

Question/project/graph complete-census paths remain independent. This candidate
index must not be substituted into those correctness-critical census APIs.

## Regression evidence

`tests/test_sqlite_lexical_index.py` covers old sources beyond 600 newer events,
long-document tail spans, Chinese terms, indexed SQL query plans, bounded
hydration for events/artifacts/claims, legacy migration/analyzer rebuild,
write/update/delete, archive/erase rollback, failed publication, restart repair,
backup/purge replay, scope inheritance versus exact scope, channels and native
admission exclusion. Existing temporal, artifact-erasure, admission, retention,
and graph/question regression suites remain required.
