# Batch 3: bounded host planning and source-faithful evidence packing

## What is wired

`MemoryKernel.retrieve()` now uses `retrieval.packing.pack_native_evidence` after
its existing admission gate and reranker. It considers feasible state and
retrieved evidence together, without reserving half the budget for state or
stopping at the first oversized claim. A deterministic greedy score combines
query-term overlap, additional uncovered query terms, distinct sources,
supporting original events and rank order. State outputs remain bounded at 64;
retrieved outputs remain bounded by `MemoryQuery.limit`, and all outputs share
the token estimate budget. This is a lexical packing heuristic, not a semantic
coverage or answer-correctness guarantee. Token costs, query-term sets and source
signatures are cached during initial preparation; the greedy rounds rescan
cached sets rather than re-tokenizing the evidence. The full authoritative state
read and initial text analysis still happen, and set scoring can scan the full
pool up to `64 + limit` times. Indexed repository search does not make all kernel
retrieval work constant or corpus-size independent. There is no silent initial
state/candidate pre-truncation that could hide competing claims.

Exact memory IDs are included once. Only identical source-linked raw event
representations are deduplicated; claims are not equated because they share a
source. Different conflicting claims, passages and supporting originals remain
eligible. Governed bundle packing likewise exempts claims from raw-source
quotas, while preserving item, kind, character and token limits. Such limits can
still omit conflicting evidence: ordinary results always report `coverage:
partial` and `world_negative: false`, including empty results.

The native kernel keeps an indexed EVENT chunk unchanged when it fits. If it
does not fit, it can select a precomputed narrower contiguous variant. The B2
locator is validated first: Python-character bounds and source length must be
integers (not booleans), the interval length must equal the received chunk text,
the chunk is at most 1024 characters, and its ID must match the SHA-256 of the
B2 JSON tuple `["events", original_item_id, source_revision, start, end]`.
Malformed or unverifiable located EVENT evidence is skipped. The source store
remains authoritative for a partial chunk's full-source revision; the packer
does not claim to have rehashed unavailable source text. When the complete source
is present, its hash is also checked.

A narrowed item retains its original memory ID, full-source `source_revision`
and `source_chars`. Its `source_span` is the exact delivered interval in the
full original source. The old locator moves to `retrieved_lexical_chunk_id`, the
original interval to `retrieved_source_span`, and `retrieved_chunk_text_sha256`
binds the received chunk text. The old `lexical_chunk_id` is removed so it cannot
misidentify the new delivered interval. Offsets are zero-based and end-exclusive;
no separated passages are joined. At most eleven geometric-width variants are
prepared once from a bounded indexed chunk. Selection picks a feasible variant
using cached costs and query-term sets as earlier items consume the budget.
This is deterministic source selection, with no generated compression.

Claims and other memory kinds are never shortened by the packer. If a full
state claim cannot fit but an already indexed representation under the same ID
can, that existing representation remains eligible without further trimming.
Legacy complete EVENT sources receive one contiguous initial-budget excerpt,
with the full UTF-8 source SHA-256, original character count and exact offsets.
Legacy excerpts without a verified indexed locator are not narrowed again.
A prepared legacy excerpt that no longer fits later is skipped; smaller feasible
entries remain eligible. Bounds and geometric variants can still underfill a
bundle or omit relevant passages, so coverage is always partial.

`retrieval.adaptive.AdaptiveRetriever` is a separate, explicit host facade.
It is not automatically installed by `AgentMemory`, the SDK, or MCP. Hosts pass
loaded plugins, an authoritative governance resolver, and optionally the
existing `QuestionRouter` and snapshot-aware provider. No new router model,
registration system, graph traversal, certificate or authorization authority is
introduced.

## Host API and constraints

- `answer(query, actor=..., dedupe_key=..., parameters=...)` first uses the
  supplied exact registered `QuestionRouter`. It returns a distinct
  `RegisteredQuestionResult` holding the unchanged service response, or
  `QuestionAbstention`. Registration is not proof that an answer is currently
  certified: the host must inspect the existing service's availability,
  completeness, contested/unknown status, citations and proof fields.
- Unregistered/unauthorized queries, ambiguous aliases and parameter conflicts
  never silently fall through to top-k. Registered-service failures also
  propagate rather than bypassing the service. Independent retrieval entities
  or temporal axes are not translated into question parameters; the registered
  path abstains for those unsupported combinations.
- With no router, `answer` returns `PartialRecallResult`. The host may also
  explicitly call `retrieve_partial` for exploratory evidence even when it has
  a router. Neither path answers "all risks" or establishes absence.
- `plan(query, needs)` exposes the exact selected names, candidate capacity,
  deterministic reasons and zero routing-model calls. The first configured
  lexical and dense capability matches are primary. This is trusted host
  configuration, not plugin discovery or capability verification.
- Supplemental entity lookup requires an explicit `RetrievalNeeds.entities`
  tuple, matching the exact scope/value/case/order binding of a loaded
  `ExplicitEntityRetrieverPlugin`. Its native entity adapter preserves its
  existing entity de-duplication and match-validation semantics. The tuple is
  never extracted from prose. Entity lookup adds candidates; it is not a global
  entity filter over lexical and dense paths by default. For a hard constraint,
  set `RetrievalNeeds(..., hard_entity_filter=True)`: a nonempty exact binding is
  required, and only the bound native entity plugin runs. Every result must match
  at least one requested entity under that adapter's existing validation rules;
  there is no unbound lexical/dense fallback on failure or empty results. Host
  `current_state` is excluded in this mode because it has no verified entity-match
  labels; matching claim candidates can still be returned by the entity index. This
  union-of-requested-entities semantics does not imply all-entity conjunction.
  Combining the hard entity mode with a separate temporal need is rejected
  because no intersection-capable source is provided.
- Current temporal lookup requires an explicit `RetrievalNeeds.temporal=True`.
  Explicit `valid_at` or `known_at` instead go unchanged to the host's
  snapshot-aware provider; a missing provider fails explicitly. The facade
  does not apply current governance to historical evidence or reinterpret an
  axis. Combining historical retrieval with entity bindings is unsupported and
  rejected, rather than silently weakening either constraint.
- Planning uses a 16–256 host cap (128 default), with per-query capacity
  `min(host_cap, max(16, min(limit * 8, token_budget // 8)))`. The existing
  governed one-wave headroom, plugin resource limits, channel constraints,
  fail-closed policy and authorization gates still apply. At most four paths
  are selected. A fallback-mode retriever failure leaves results from the same
  wave; it does not trigger another wave or relax constraints. Failed required
  plugins fail the request.

The facade does not load state automatically. A host supplies authorized
`current_state`, or an empty sequence. Current governed packing still requires
that the supplied state fit its budget; native packing is a different path.
Both current and historical provider implementations remain responsible for
their existing admission/access/erasure rules. No new cached evidence retention
or model capability is enabled by this change.

## Compatibility, verification and limits

No persisted schema or domain/protocol version changes. Native packing order
and the strategy metadata change to `feasible_evidence_coverage_v1`; query policy
IDs remain caller-supplied audit fields. Citation IDs and indexed source spans
retain their original meaning. The new host facade has concrete-module imports
only, with no new flat compatibility export.

Tests cover oversized-state backfill, term/source packing, duplicate versus
conflicting same-source evidence, original-event support, indexed chunk
identity, verified indexed EVENT narrowing with original/delivered locators,
real indexed-kernel Han-tail budgets, randomized Unicode/NUL/combining offsets,
Unicode-safe legacy offsets, bounded primary selection/failure,
explicit entity/time inputs, and real registered-risk service routing with
ambiguous/unregistered/unauthorized/parameter-conflicted abstention. The
registered-service test uses the existing SQLite/PostgreSQL fixture; PostgreSQL
requires the explicitly configured disposable CI database. Synthetic fixtures
establish these contracts only. No deployment-quality, latency, financial-cost
or whole-cost benefit is claimed; the frozen comparison and real-quality gates
in [ordered batches](ordered-batches.md) still apply.

## Frozen mechanism regression evidence

The recorded [synthetic comparison](batch3-synthetic-comparison.json) keeps the
same 9-query fixture and 9 repetitions. Both recorded Batch 2 and final Batch 3
native kernels retrieve all 7 annotated positives, with zero forbidden hits or
execution errors. An intermediate long-document omission was caught and repaired
with validated narrower original spans before publication. Randomized Unicode,
NUL and offset/budget tests also protect exact substring lineage.

These are regression checks, not real answer-quality or efficiency acceptance.
The long-tail case retains more contiguous surrounding context and grows from 35
to 187 estimated delivered tokens. Neither larger coverage scores nor stable
memory IDs establish preserved qualifiers or model answer correctness. The later
controlled promotion gate must measure those independently, and the unchanged
real-data quality and whole-cost gates remain open.
