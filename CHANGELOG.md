# Changelog

All notable changes are documented here. The project follows Semantic Versioning after
the first stable release.

## Unreleased

### Fixed

- Deduplicate covered source spans inside evidence groups before charging slots
  and tokens. In opt-in v4/v5, reserve minimum complete evidence under budget pressure, price
  parent/context units jointly, and preserve leading semantic evidence when
  competing reservations cannot fit. Rebind dependency proofs after context
  growth and skip empty units. V2 retains its previous default policy; v4/v5 have
  explicit strategy revisions and no schema changes. See the
  [offline repair validation](docs/evaluation/PACKING_REPAIR_2026-10-28.md).

## 0.1.0 - 2026-10-26 (Alpha)

First GitHub pre-release. The software version is 0.1.0; the architecture baseline
is v7.3. This release does not claim complete real-model benchmark or cost acceptance.

### Added

- Add opt-in v7.3 evidence research, recoverable context, shared policy receipts,
  measured refresh decisions, retractable multi-step Skills and frozen paired QA
  reports. Source recovery requires a native recovery snapshot; evolution source
  invalidation now requires an explicit scope. Defaults and real quality acceptance
  remain separate; see [v7.3 contracts and limits](docs/design/AGENT_MEMORY_DESIGN_V7.3.0.md).

- Add repeated local Agent Memory/Mem0 OSS/Graphiti comparisons with shared
  answering, rotated arm order, immutable source/model manifests and transport
  token accounting. Isolate Mem0 history as well as vector storage. This is an
  authored diagnostic, not a LongMemEval leaderboard or business acceptance.
  See [the controlled local protocol](docs/UNIFIED_LOCAL_BENCHMARK.md).
- Add a durable full 500-question LongMemEval-S worker for all three local
  backends, with exclusive process ownership, frozen package/script/model
  hashes, bounded retries, interruption recovery and explicit unknown usage
  after incomplete log writes. Local Qwen judging is separate from official
  scores. See [the full-run protocol](docs/FULL_LONGMEMEVAL_LOCAL_RUN.md).

- Add host-bound automatic raw-source project extraction, explicit storage and
  independent-verification policies, typed qualifier verification and complete
  refutation provenance. Registered scenes/personas share the existing refresh
  scheduler; persona revisions preserve historical beliefs and current security.
- Rebuild registered admitted-L1 questions/pages at arbitrary dual-time
  coordinates inside proven coverage, with frozen business definitions and live
  authorization/erasure. Add gold-free A9 raw-host execution and explicitly
  grouped model configuration accounting. Real business acceptance remains open.
  See [v7.2 architecture and limits](docs/design/AGENT_MEMORY_DESIGN_V7.2.0.md).

### Fixed

- Filter admission-protected semantic sources before result and context limits,
  hydrate only positive vector matches, and honor disabled semantic channels
  before embedding or census dispatch. Stop research planning once no acquisition
  round remains. Reject paired refresh measurements that reuse the same model
  attempt across both arms. APIs, storage schemas and default recall are unchanged;
  see [targeted optimization checks](docs/design/v7.3.0/validation.md#7-查询与成本优化2026-10-25).

- Report benchmark technical failures over recorded attempts, keep official
  rubric credit separate from false abstention, validate judge/summary consistency
  and retain conservative unknown/pending costs. Add immutable evidence replay
  with gold opened only after frozen answers and independent V3 audit outputs;
  old ledgers and results are never reset. Paid replay explicitly requires POSIX
  private credential-file permissions; offline replay stays portable. See [acceptance record](docs/design/qa-reliability-2026-10-20/validation.md).

- Add an opt-in audited v4 answer reader with exact citation positions, bounded
  deterministic arithmetic/date calculations, declared temporal precision and
  a separately accounted support check. Keep approximate source-month assumptions
  explicit and preserve semantic refusals. Protocol 4.2 changes no stored schemas,
  default readers or authoritative facts. See [limits](docs/BENCHMARK_QA_RELIABILITY_V4_2.md).

- Bind reader v3 quotes to exact source-labeled evidence blocks, retaining host
  dates, speakers and spans without a second text crop. Separate malformed
  responses from abstention and freeze packing/framing ablations before opening
  labels. Existing callable defaults stay v2; new benchmark CLI runs declare v3.
  See [reader compatibility](docs/BENCHMARK_RELIABILITY_V3.md).

- Keep evidence dependency groups intact during budgeted packing and final
  authorized delivery. Add opt-in v3/v4/v5 packing, bounded coverage reservations
  and an explicitly configured passage reranker; v2 stays the default and stored
  schemas are unchanged. See [packing limits](docs/BENCHMARK_COVERAGE_PASSAGES_V5.md).

- Validate benchmark provider formats and retain redacted transport failures; deliver
  native evidence without recropping and preserve frozen runs through isolated
  evidence audits. Add opt-in bounded query facets without changing default
  retrieval or stored schemas. See [v2 contracts](docs/BENCHMARK_RELIABILITY_V2.md).

- Repair the legacy LongMemEval generator's explicit scored-claim schema so
  useful proposals no longer all disappear at validation. Add real-core positive
  contract fixtures, extraction diagnostics, partial provider usage receipts and
  frozen input/configuration manifests that refuse stale result reuse. These
  deterministic checks are not model-quality evidence; new Atom admission and
  production defaults are unchanged. See [experiment validity](docs/LONGMEMEVAL_CONTRACT.md).

- Persist generic consolidation handoffs transactionally in SQLite/PostgreSQL,
  retry queue failures after commit, and expose delivery status. Distinguish empty,
  rejected and failed extraction, propagate strict mode through the public builder,
  and preserve bounded multiple source spans through native SQLite reranking and
  packing. Repair the LongMemEval confidence contract and record extraction
  diagnostics without rewriting stopped runs. See
  [ingestion reliability and compatibility](docs/INGESTION_RELIABILITY.md).

- Serialize SQLite schema creation and forward migrations in one database writer
  transaction, preserving the lock across scripts and rolling back partial
  initialization on failure. Nested scheduler/index initialization respects its
  caller's transaction. Stored schemas and ordinary write semantics are unchanged.
  See [initialization guarantees](docs/SQLITE_INITIALIZATION.md).

- Keep core imports and SQLite operations available without Unix `resource`;
  unavailable peak RSS raises explicitly instead of fabricating a measurement.
  Add a unified seven-distribution build/install/archive gate on Linux, macOS,
  and Windows, with local-model inference dependencies behind an explicit extra.
  See [platform scope and validation](docs/PACKAGE_VALIDATION.md).

- Recheck compact-reranker promotion approval across invocation and delivery
  boundaries, fail closed on revocation or configuration changes, and enforce
  final expiry after host callbacks. Controlled retrieval evidence now requires
  independent supporting-span/qualifier judgments and measured zero unsafe/stale
  deliveries; unknown observations cannot qualify promotion.

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

- Integrate the governed v7.1 runtime with shared question proofs, source omission
  holds and maintained relation views. Fence composed model/audit publication,
  durable completion and historical delivery after final awaited work. Bounded
  indexed verification discovery and active-only backpressure keep terminal
  history from blocking progress; per-stage errors do not suppress independent
  work. See [integration boundaries](docs/design/v7.1.0/runtime-integration.md).

- Opt-in governed Atom model generation/review, bounded cross-turn provenance, durable
  authority-backed domain verification and native project/conditional publishers.
  Add an assembled capture/extraction/verification/question-refresh host with atomic
  grants, restart/stop behavior and body-free metrics.
- Exact final-request token budgets checked after current source authorization and
  before financial reservation; optional local Qwen3 Transformer rendering/generation
  and actual final-token reranking in a separate dependency-bearing package.
- Published-point question/page history with current permissions, explicit same-repository
  cross-scope current composition, host-reviewed L3 support/counterexamples and Atom
  version guards, and bounded read-only Reflect. Real acceptance input binding delegates
  to the existing four-arm evaluator; actual business gold/pricing are still missing.
  Scope, algorithms and evidence: [v7.1](docs/design/AGENT_MEMORY_DESIGN_V7.1.0.md).

- Transaction-local shared qualified inputs for bounded batches of already-leased
  project QuestionViews, preserving fresh source, permission, erasure, time and
  original-lineage checks. Add versioned semantic readsets/field effects and
  separate semantic-versus-proof invalidation diagnostics; complete-project
  coverage and certificate invalidation remain unchanged. Batch commit revalidates
  durable job/execution leases, including heartbeat renewal and final expiry, rather
  than trusting caller task timestamps. Frozen SQLite evidence
  reports fewer repeated qualification reads and full workflow SQL SELECTs, while
  explicitly retaining read-phase CPU overhead and zero-model-call baseline.
  See [scope and evidence](docs/design/incremental/shared-question-inputs.md).

- Default-off bounded source-level omission audits for exact host-selected
  questions, predicates and source spans, including zero-candidate generation.
  Recovered candidates remain pending explicit host verification; conflict and
  unresolved coverage never prove recall or world-negative facts. Reuse and
  durable publication recheck source/version/authorization/erasure/time fences.
  Same-source audited-slot holds survive model withdrawal and regenerated
  candidates without blocking unrelated subjects, predicates or independent sources.
  Explicit contract-test mode and live configuration/report-bound host promotion
  approval prevent unmeasured production enablement. No new table or dependency
  on PR #18; real local Qwen model-gain remains
  unmeasured. See [source omission audits](docs/source-omission-audits.md).

- Host-registered bounded dependency/date joins in maintained project risk views,
  reusing typed ontology relation rules and full-census QuestionView guards.
  Preserve inferred candidate lineage, validity intersections, exact/unknown
  aggregates, new-premise invalidation and clock-boundary refresh; bounded
  truncation remains incomplete. Includes coordinated relation-predicate index
  migration and dual-provider adversarial contracts. See
  [maintained relation views](docs/design/incremental/relation-views.md).

- Incremental SQLite lexical locators with bounded pre-hydration ranking, exact
  long-source spans and legacy-write repair; PostgreSQL keeps native indexes.
  Add explicit host-only current retained-source embedding reuse with separate
  authorization/retention lifecycle. The generic embedding reranker still keeps
  no vectors. See [index scope](docs/SQLITE_LEXICAL_INDEX.md) and
  [source embedding reuse](docs/source-embedding-reuse.md).

- Default-off host-local compact pair-reranker contract and actual final-token
  yes/no-logit adapter, with exact rank-only identities, processing authorization,
  fresh post-score governance and separate RRF/neural/selection metadata. Add a
  frozen four-arm cold/warm controlled promotion harness with full maintenance
  costs, unknown/failure accounting and paired group uncertainty. Contract tests
  do not qualify real efficacy or enable deployment. See
  [compact pair reranking](docs/design/retrieval/compact-pair-reranking.md).

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
