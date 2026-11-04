# Bounded state reads

The optional `BoundedStateProvider.get_state_bounded(scope, *, limit)` port returns
exactly the prefix of `get_state(scope)`, for the same database state and read time.
`AgentMemory.state()` and its configured recall pipeline use this port when present.
Existing providers remain compatible through a full-read-and-slice fallback. The
required provider interface, wire schema, and database schema are unchanged.

SQLite and PostgreSQL perform legacy-claim authorization, valid/system-time
selection, source-liveness checks, and protected-Atom-source exclusion before SQL
`LIMIT`. They preserve the existing importance/confidence/ID order. The protected
source closure and legacy selection share one database snapshot. Source IDs are
aggregated in their original provenance order instead of issuing one source query
per claim. Full historical reads also use this batched source validation.

Atom conflict, replacement, correction, expiry, qualification, and withdrawal
projection still sees the complete visible candidate set before output truncation.
A conflict later in record order cannot be hidden by the output limit. Current
admission state reuses current record payloads only when their publication time is
already known at the requested time. Future or historical publication boundaries
use the existing version-history path. Public audit-history reads are unchanged.
The final recall guard hydrates legacy claims only for IDs actually referenced by
the response or its diagnostics, while retaining complete Atom conflict checks.

## Measured regression

A file-backed SQLite database containing 96 ordinary claims, with
`MemoryLimits(max_state_claims=1)`, produced these traces:

| Measurement | Before | After |
| --- | ---: | ---: |
| Returned claims | 1 | 1 |
| SQL statements (after connection setup) | 114 | 14 |
| Individual source SELECTs | 96 | 0 |
| Hydrated legacy claim payloads | 96 | 1 |

The 192-claim regression has the same 14 statements and one hydrated claim. A
configured pipeline with 192 claims hydrates one initial state claim plus one final
source-authority recheck. Current admission reads with 97 versions of one record
reuse its current payload and do not SELECT `admission_versions`.

## Remaining limits

This is **not a constant-work or constant-memory guarantee for the whole request**:

- The database may scan and sort all eligible legacy rows to find the ordered
  prefix. The limit bounds returned/hydrated legacy payloads, not query-planner work.
- Atom projection retains the existing 1,024-record ceiling and fails closed above
  it. That ceiling is independent of `max_state_claims`; complete conflict groups
  cannot be truncated safely.
- Protected-source closure, tombstone identities, and dependent-source metadata
  are still complete scans. Historical admission reads can still load all versions
  of records whose current publication is newer than the requested knowledge time.
- Native recall ranking, explicit unlimited provider reads, and third-party
  providers without the optional port can still materialize their full state.

Tests in `tests/test_bounded_state_reads.py` cover 96/192 claims, filtering before
limits across all scope dimensions, a conflicting Atom beyond the first 96 record
IDs, current/history parity, invalid limits, and old-provider compatibility. The
shared backend tests run against PostgreSQL only when a verified disposable
`AGENT_MEMORY_TEST_POSTGRES_DSN` is supplied; SQLite passes do not establish live
PostgreSQL parity.
