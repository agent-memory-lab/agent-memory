# Unified comparison integration (2026-10-13)

This integration reconciles `codex/unified-local-comparison` with main without
replacing either history. The integration branch starts at main
`babe7615392101d495bf02967457f6022fc636e4` and merges the original branch head
`58fe3c6959a3df8e7629832c05faeae4fd1fd27e` with a normal two-parent merge.
The eight original commits remain reachable:

- `65dd3f136a895e55b4b8209bc17c2f12f22bef8d`: extraction and transactional consolidation handoffs
- `d558439cf24b26bc6d3be682c81c9aca8c6102b4`: bounded contextual retrieval and packing
- `f5e9458e7c26bc2912eeaa6d97f11829b07d9acd`: local benchmark ledger and retrieval replay
- `69a1aebb378f68c4ebabb2099237523d1b7f41f6`: temporal block provenance
- `c09337be148d9efa48cf2ffa0390619a0f576505`: outbox retry fairness
- `b93fbd7f8ec74c15a75584abb91f3d9e883a9464`: final evidence reauthorization
- `9c9b4e3d5e0fe10fbdb5f5e6a1c0ef46a2d6353c`: incomplete model-call accounting
- `58fe3c6959a3df8e7629832c05faeae4fd1fd27e`: prior reliability validation record

## Reconciliation and review

The two textual conflicts were the changelog and the legacy LongMemEval runner.
The latter combined overlapping extraction implementations. Its shared generator
must retain strict scored-claim validation, generated/validated counts, provider
usage receipts, and body-free extraction diagnostics. Frozen manifests must bind
the actual prompt/options and shared helper implementation, not only the entry
point. The report-based core extraction path must update legacy counters too.

Request-local candidate identities are for deduplication and ordering, not
security proofs. Final-delivery authorization must bind the exact evidence and
provenance, preventing a valid candidate from authorizing a forged same-ID
sibling. This applies to both native event bodies/spans and externally resolved
items that reference local sources. SQLite and PostgreSQL share this filtering
rule while acquiring their own authoritative snapshots.

Failed cancellation deliveries now use durable, bounded backoff in both databases,
with generation/attempt fencing so stale failures cannot postpone newer work.
PostgreSQL migration `023_consolidation_cancellation_retry.sql` is additive;
existing cancellation markers remain immediately eligible.

A failed pilot resource cleanup stops later trials and prevents a successful
completion status. Resource ownership begins during partial factory setup, before
local-memory construction. Shared-client close failures still preserve final
accounting and a best-effort report. Continuing after uncertain process/database
cleanup would invalidate isolation and resource accounting.

`extraction_result.py` is an immutable shared domain contract. The architecture
boundary check covers its dependencies as well as callers' dependencies; it does
not allow storage, provider or runtime implementation imports into domain code.

## Evidence boundaries

The existing `experiments/public_longmemeval` pipeline and public dataset pin are
preserved. Gold answers remain evaluator-only. The local comparison worker uses
POSIX locking and is supported on Linux/macOS; this does not narrow the supported
platforms of core packages or the separate public LongMemEval runner.

Synthetic contracts, mocked model calls, package builds and live database tests
are software evidence, not measured model quality. No model API calls, benchmark
score regeneration, release/tag creation, or deployment are part of this
integration. Prior branch-only counts in the October 12 record are historical;
the integration is accepted only against its own final commit and CI results.
