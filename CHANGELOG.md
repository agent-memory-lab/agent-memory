# Changelog

All notable changes are documented here. The project follows Semantic Versioning after
the first stable release.

## 0.1.0 - Unreleased

### Added

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
