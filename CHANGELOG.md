# Changelog

All notable changes are documented here. The project follows Semantic Versioning after
the first stable release.

## 0.1.0 - Unreleased

### Added

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

### Security

- Governed Atom sources and derived audit events cannot bypass admission through recall.
- Generated trajectory claims require an explicit valid confidence value;
  missing model scores no longer default to certainty.
- Scope is derived from trusted host context rather than model arguments.
- Remote identity headers require HMAC verification by default.
- Active Procedure promotion cannot bypass offline, shadow, canary, and approval gates.
