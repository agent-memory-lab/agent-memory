# B4 — deterministic delta and immutable proof reuse

## Scope and delivered behavior

AM70-T10/T11 implement the registered, bounded project-question runtime. No model is called.
`full_project_question` remains the independent B3 semantic oracle. The runtime now supports:

- Keyed owner/status projection and complete commitment/risk groups, including rule groups.
  Only changed groups are evaluated. A metadata/security census still runs; this is not a claim
  of incremental source retrieval or reduced admission work.
- Explicit old-contribution removal/new-contribution addition; withdrawals, membership moves,
  qualification revisions, valid/known-time visibility, condition changes and deadline crossings.
- Exact keyed count, distinct/reference counts, integer-unit sum/min/max. Unknown and missing
  contributions are counted separately, never coerced to zero. The project runtime uses the
  matched-row aggregate; arbitrary user-authored aggregate/query programs are not exposed.
- Transactional, bounded commit windows tied to query/barrier generations. Identical duplicates
  and ordering differences normalize; missing/conflicting sequences, rollover, damaged state,
  incompatible operator/schema/definition/context, and unsafe original inputs cause a real full
  evaluation. Appending a new event cannot re-seal corrupted old history as valid continuity.
- Complete immutable generation inputs, separate current validation manifests/certificates,
  and independent value/structure/support/generation/validation/safety digests. Volatile snapshot
  coordinates and validation time belong to the certificate/head/response, while citations,
  qualifiers, conflicting/unknown states and factual intervals remain semantic content.
- A true safe noop keeps the original content ID and generation manifest, appends a new
  certificate, updates current coverage/safety, and records the completed job as `noop`.
  Delta that consumes cached groups inherits their original generation inputs, including
  immediate prior content/certificate references and flattened original source safety metadata.
- Original plus current source grants, retention/source revisions and authority are checked
  before body access and again at delivery/publication. The earliest inherited grant/authority
  expiry limits the new certificate too. Equal public evidence never relabels old private inputs.
  If current authorized inputs permit it, an independent full successor can be generated without
  loading the now-forbidden cached body.
- Source-sensitive project pages preserve immutable original parent content/certificate lineage,
  separate current validation manifests and response proof overlays. Equal business values alone
  cannot suppress changed citations/explanations. Parent publication durably marks pages
  `validation_pending`; they remain unavailable until bounded validation commits. Least-attempted
  ordering prevents a permanently blocked page from starving other pages.

The admitted downstream graph is the existing finite QuestionView → project-page layer (at most
four parents per page and 128 page registrations). Arbitrary dependency programs, multi-level new
question graphs, free-form LLM patches and arbitrary value-only consumers remain unsupported.
Existing qualified Observation/page behavior is unchanged and retains its separate guards.

## Safety fixes found during independent review

The review added concrete counterexamples for caller-selected question/context in a returned
snapshot, inherited private-source expiry, and bounded page-validation starvation. The first
issue was fixed in the B3-owned commits `85976f3`, `aa0f5fe`, `a6472fe`: a full owned snapshot
hash is persisted on the fenced job before return and checked atomically at publication; input
controls are rechecked at source and candidate body boundaries. B4 includes those fixes.

B4-specific regressions additionally cover original-source revocation before body access,
source-sensitive same-value rerendering, immutable generation through noop and delta, corrupted
baseline/log full fallback after service restart, and actual backup/forget replay of new state.

## Storage, upgrade, retention and rollback

New objects use the existing derived-entry ledger: `question_delta_state`,
`question_change_log`, and `question_page_validation`. No new SQL table is needed.
State and validation records participate in same-scope erasure and backup replay. Commit-window
history is conservatively scrubbed in an affected scope; surviving states subsequently full
rebuild when continuity is absent. Logs retain at most 128 entries per route, with no source
names or bodies. Content/certificate/page capacities remain explicit, bounded publication gates.

Registration/runtime/processor contracts advance to version 2; the public question-answer wire
remains version 1. Drain old writers/workers, upgrade both backend adapters and host registrations,
then re-register old project questions with the expected registration generation. Version-1
registrations are not silently interpreted as version-2 content. Legacy subscription metadata
can still be routed for invalidation/erasure. Missing optimization state always starts with full.
Rollback must disable/drain version-2 registrations/work before starting an older worker; do not
rewrite newer proofs into old contracts. Keep current authoritative erasure replay when restoring.

## Validation evidence

Final results: 230 affected tests passed (105 SQLite, 105 PostgreSQL 17, 20 provider-independent);
16 additional SDK/MCP transport tests passed (8 per provider); the separate protocol/oracle suite
passed 225 tests. These counts describe their individual runs, not a cumulative whole-repository pass.
Final commands, source SHA and results are recorded in `validation-b4.json` (the commands use the
existing local Python environment and the shared-lock real PostgreSQL 17 test server).

- Five deterministic randomized/replay sequences, 110 steps each, compare complete business,
  membership, evidence/qualification, processing references, time transitions and scope metadata
  to the B3 full oracle. An instrumentation test fails if the delta path calls the full oracle.
- Unit tests include old-remove/new-add integer aggregates, duplicate extrema, distinct reference
  counts, missing/unknown values, unsupported units/types, and full fallback/continuity variants.
- Real SQLite and PostgreSQL tests cover registration, grants, refresh queue publication, noop,
  current guarded reads, source-sensitive pages, backup replay, input ownership and atomic CAS.
- Independent review tests run on both real providers in the final affected suite.

These are synthetic correctness and security fixtures, not licensed production outcome data.
No production cost/quality saving, real-model result, deployment or paid API result is claimed.
The real Ollama/provider evaluation is a separate B5/B6 gate and was not run by B4.

## Canonical B4-only commits

`52313c2` → `8b9d421` → `51230e2` → `cf16ba0` → `8e2011d`, followed by this evidence/ledger commit.
Do not cherry-pick `4c29dde`; it only preserves the copied B3 baseline. B2/B3 prerequisite commits
are integrated separately and are not B4 implementation credit.

## Cached-body boundary correction (2026-10-08)

Additional testing while draft PR CI ran found that expiry/context/host changes during an
awaited cached-content read could allow the next cached body to load before another guard.
Final delivery already failed closed, but the later processing read was outside its valid input
boundary. `2ed0037` applies before-and-after guards to each cached question content/certificate/
delta-state and page content/certificate/block read, and guards the baseline after metadata awaits.
Twelve new scenarios cover snapshot, question read, page read and page publication, each with
grant expiry, context changes and host-registration changes during the first cached body await.

The corrected source passed 184 affected tests: 92 SQLite and 92 PostgreSQL 17, with zero failures,
errors or skips. This is a new independent run, not a sum with earlier counts. Ruff and diff checks
also passed. Exact source fingerprints and the JUnit hash are in `validation-b4.json`.

CI-only commit `e83d39c` explicitly adds all four B4 suites to the live PostgreSQL job; its YAML
and exact test collection were checked. These follow-ons extend the canonical B4 sequence after
`9cff5ee`; no production/model result or deployment is implied.
