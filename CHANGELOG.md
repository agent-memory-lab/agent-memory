# Changelog

All notable changes are documented here. The project follows Semantic Versioning after
the first stable release.

## 0.1.0 - Unreleased

### Fixed

- Pack native recall by feasible evidence coverage without dropping later claims
  after an oversized state entry. Preserve exact indexed chunks and annotate
  legacy excerpts with contiguous source spans; keep distinct same-source claims.
  Add an opt-in bounded host planner that reuses exact registered questions and
  preserves abstention, historical axes and partial-retrieval boundaries. See
  [adaptive retrieval and packing](docs/design/retrieval/adaptive-and-packing.md).

- Preserve retrieval query constraints and reject unsupported historical plugin
  paths instead of silently answering as current. Keep bounded candidate headroom
  through policy checks and feasible diversity packing, and share a versioned
  contiguous-Han analyzer. PostgreSQL native FTS remains indexed. See
  [retrieval batch scope and measurement gates](docs/design/retrieval/ordered-batches.md).

- Reserve governed model cache capacity atomically before paid dispatch; fence abandoned
  reservations and return the current ledger settlement status on cached delivery.
  Hold first-delivery audit capacity atomically with dispatch so the last available
  audit slot cannot cause a predictable billed-but-undelivered generation.
- Maintain already-published project pages through the existing bounded refresh host,
  dependencies, leases and shared budgets, with restart and erasure safeguards.
- Add host-acknowledged bounded model authorization audit archival, preserving full
  exported derived proof and independently pinned archive receipts without deleting
  money debt or silently expiring audit evidence. See [scope and compatibility](docs/design/v7.0.0/runtime-repairs.md).

### Added

- Incremental SQLite lexical locators with bounded pre-hydration ranking, exact
  long-source spans and legacy-write repair; PostgreSQL keeps native indexes.
  Add explicit host-only current retained-source embedding reuse with separate
  authorization/retention lifecycle. The generic embedding reranker still keeps
  no vectors. See [index scope](docs/SQLITE_LEXICAL_INDEX.md) and
  [source embedding reuse](docs/source-embedding-reuse.md).

- Opt-in local Ollama synthetic runtime smoke with a saved pre-dispatch plan,
  pinned installation/configuration, actual question/cache/authorization/erasure
  checks, token receipts and unknown-cost ledger retention. Real local Qwen 9B
  execution is recorded; licensed-domain quality and whole-cost acceptance stay open.
  CLI and scope: [local Ollama smoke](docs/design/v7.0.0/ollama-smoke.md).

- Host-only bounded QuestionView garbage collection with atomic SQLite/PostgreSQL
  reference checks, explicit retention holds, immutable lineage and finite-receipt pins.
  Background proof-only and independent full generations can reclaim unused objects;
  receipts, delta ancestry and model audit can still exhaust capacity.
  Scope and rollback: [T14 retention](docs/design/v7.0.0/retention-gc.md).

- Offline A9 multi-arm workload tooling with frozen gold-free execution inputs,
  actual SQLite question/refresh/model-cache integration, isolated control ablations,
  full-phase unknown-cost/debt accounting and reproducible paired cluster bootstrap.
  Synthetic-only evidence, limitations and CLI: [A9 runner](docs/design/v7.0.0/a9-experiment-runner.md).

- Host-only deterministic QuestionView page append/insert/replace/remove with page/certificate
  and affected-block CAS, exact retained revisions, multi-generation processing lineage,
  bounded whole-page delivery, and live/backup erasure. SDK/MCP retain read-only page access.
  Scope, conflicts and compatibility: [Q7-23 typed patches](docs/design/v7.0.0/typed-page-patches.md).

- V7-B4 complete-group project deltas, old-remove/new-add aggregates and verified full fallback;
  immutable original generation versus current certificates, safe noops and bounded fair page validation.
  Original source permissions/expiry, snapshot ownership and erasure remain enforced. Version-2
  runtime/registration/worker upgrade and evidence: [B4](docs/design/v7.0.0/batch-b4.md).

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
