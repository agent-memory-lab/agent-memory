# Changelog

All notable changes are documented here. The project follows Semantic Versioning after
the first stable release.

## 0.1.0 - Unreleased

### Added

- Explicitly host-enabled V7-B3 current project QuestionViews for owner, status,
  commitments and risks, reviewed field/time evidence, complete indexed census,
  immutable full provenance, closed publication manifests and shared bounded refresh.
- Optional question SDK/MCP routing and guarded reads; qualified current language
  parents/pages and finite host-published project pages with stable full-rebuild blocks.
- Project candidate indexes on SQLite/PostgreSQL and physical erasure/old-backup
  replay covering unpublished registrations, original processing inputs and routes.
  Migration, opt-ins and rollback: [B3](docs/design/v7.0.0/batch-b3.md).

- Versioned v7 architecture design with algorithm/data-flow diagrams, implementation plan,
  inherited-task mapping and explicit planned-versus-implemented boundaries; no v7 runtime capability is enabled by this documentation update.

- Host-bound query context, bounded three-valued conditions and explicit applicability precedence.
- Versioned complete-field temporal AND/OR support with source-family and business-revision checks.
- Contextual projections without unconditional Claim leakage; atomic auxiliary-evidence erasure
  preserves surviving OR branches and removes broken AND proofs on both storage providers.

- Immutable source revisions with shared SDK/MCP delivery and acknowledgment recovery.
- Explicit whole-source additive/replacement processing, independent old-contribution review,
  interpretation head CAS, preserved evidence identities and bitemporal history.

- Opt-in durable local extraction with immutable sources, resumable stages, fenced leases,
  and transactional publication on SQLite and PostgreSQL.
- Shared Embedded/MCP producer sessions, contiguous acknowledgments, status queries,
  and a persistent SDK outbox for lost-confirmation recovery.
- Preserved fact conditions/exceptions/negation, required field evidence with bounded
  AND/OR source spans, and host-authorized bitemporal state termination.
- Versioned v6.1 design, execution ledger, evidence evaluation and explicit remaining scope.

- Opt-in automatic Atom extraction with separate source-faithfulness and retention reviews,
  host-controlled admission, persisted diagnostics and first-result idempotent replay.
- Bounded Chinese/English reference rules, injectable generation/review protocols,
  and authored evaluation measuring useful recall as well as false acceptance.

- Explicit typed Atom admission with source authority, pending verification,
  conflict review, temporal evidence and bitemporal state projection.
- SQLite/PostgreSQL admission snapshots, compare-and-swap reviews and deletion fences.
- Framework-neutral memory domain and `MemoryProvider` contract.
- SQLite provider with state-first retrieval, citations, supersession, and forgetting.
- Zero-config bounded `AgentMemory.local()` facade.
- Lazy Python entry-point discovery for providers and integrations.
- Optional PostgreSQL/pgvector provider and consolidation queue.
- MCP v2 stdio and Streamable HTTP package with trusted identity resolvers.
- Embedded, stdio, and HTTP Python SDK.
- LangGraph lifecycle adapter.
- Controlled Procedure candidate evaluation, promotion, activation, and rollback.

### Fixed

- Erasing evidence now conservatively invalidates dependent free-text blocks, episodes,
  and procedures, including archived artifacts and child scopes, instead of retaining
  text after stripping provenance. Direct reads and search apply matching validity guards.
- pgvector deletion requires the authorized scope and follows authoritative primary-store
  deletion, including retry cleanup of absent blocks after a sidecar failure. Conflicting
  vector identities cannot be reassigned across scopes.
- MCP transports preserve historical query timestamps and register all nine supported block
  and feedback operations with canonical capability-filtered tool schemas. Unknown arguments
  now fail validation instead of being silently ignored.
- LangGraph capture processes complete multi-message updates, deduplicates replayed
  histories, and retains stable event identities when capture must be retried.

Compatibility: initialization applies an additive typed local-memory identity tombstone table for
erasure fencing; the public protocol and core schema versions are unchanged. Direct
`PgVectorIndex.delete` callers must pass `scope=...`; the high-level block API is unchanged.
Erasing one source of a free-text derived artifact removes the complete artifact because
arbitrary prose cannot be safely redacted by removing a provenance reference.

### Security

- Governed Atom sources and derived audit events cannot bypass admission through recall.
- Generated trajectory claims require an explicit valid confidence value;
  missing model scores no longer default to certainty.
- Scope is derived from trusted host context rather than model arguments.
- Remote identity headers require HMAC verification by default.
- Active Procedure promotion cannot bypass offline, shadow, canary, and approval gates.
