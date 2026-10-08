# V7-B3 — trusted project questions and qualification-preserving pages

Status: finite current full runtime implemented, explicitly host-enabled. This
record covers B3 on the B2 shared-scheduler foundation. It does not enable models,
delta, generic historical questions, or general-purpose scenario generation.
Software remains v0.1.0 / Alpha; the frozen v7 design is unchanged.

## Supported lifecycle

- `ProjectAdmission` registers a closed project contract, source authorities,
  reviewed membership bindings, qualification policy and purpose. Source staging
  atomically preserves field evidence and creates pending candidates. An explicit
  host review with field/time/condition support is required before a candidate
  can contribute. Merely quoting a source never grants semantic authority.
- The complete indexed census includes rejected, unbound, pending and moved
  candidates. Old/new project selectors and source/qualification changes advance
  transactional barriers. Membership changes cannot silently remove a relevant
  unknown or counterexample. Oversized or incomplete indexes fail closed.
- `QuestionService` binds a trusted current context and registers finite owner,
  status, commitment and risk definitions. It materializes the existing full
  oracle with immutable generation inputs, content, certificates and guarded head.
  The fenced job commits a digest of the complete owned snapshot, so callers
  cannot change the registered question or context and legitimize it by recomputing
  deterministic output IDs. Current source and candidate permissions/time/host
  controls are rechecked immediately before each body input.
  Missing completion is unknown, conflicting owners remain contested, and an
  empty risk result is only a complete no-match in the known authorized scope.
- Both admitted-L1 and explicit finite publication-manifest source bases are
  supported. A source receipt alone is insufficient: the exact registered source
  processing requests must be closed and account for the candidate census.
- Question reads verify metadata/current ACL/context/time before body access,
  then verify the complete response and recheck at delivery. New semantic inputs,
  source revisions, pending reviews, membership movement, grant changes and
  elapsed time all prevent delivery of the old current answer.
- On-demand direct work shares the B2 durable queue, finite target, singleflight,
  lease, hard running quota, failure/retry and publication path. A successful
  direct answer leaves no unconditional duplicate background job. Finite coverage
  completion is distinct from current availability.

## Routing and supported SDK/MCP surface

Pass `questions=service` explicitly to `EmbeddedMemoryClient`, `MCPMemoryTools`,
or `agent_memory_mcp.create_server`. Omission exposes no `memory_question` tool.
The authenticated transport context supplies the scope and actor; model payloads
cannot supply authority, project bindings, grants, definitions or trusted context.

| SDK method | `memory_question` operation | Contract |
| --- | --- | --- |
| `question_capabilities()` | `capabilities` | Actual opt-in backend/template/schema boundary |
| `question_read(id)` | `read` | Current guarded answer; historical coordinates rejected |
| `question_route(query, parameters=...)` | `route` | Exact registered ID first, then exact normalized aliases |
| `question_answer(query, dedupe_key=..., max_steps=...)` | `answer` | Guarded reuse, or 0–8 shared queue steps; explicit pending/deferred |
| `question_request(id, dedupe_key=...)` | `request` | Fixed finite coverage target, not authority to register a question |
| `question_status(target)` | `status` | Finite completion, never an implicit current-ready assertion |
| `question_page_read(page_id)` | `page_read` | Read-only, complete current project L2 page |

Ambiguous aliases, unsupported open questions and conflicting parameters abstain;
there is no guessed project or hidden model fallback. The deterministic routes,
full renderer and page projection make zero model calls. Payloads preserve
qualifiers, unknowns, evidence, complete processing references and response byte
limits. Transport delivery reacquires current permission after an earlier result.

## Two explicit page combinations

### Qualified language parents and L2 pages (stage 16)

`ObservationService(..., qualified_current=True)` permits
`locale-qualified-parents/1` and `language-qualified-scenario/1` with compatible
trusted `FacetContext`. Existing nonconditional templates remain unchanged.
Scope, subject, purpose, authority, route token, context attributes, timezone and
qualification policy must agree; the child's lifetime must fit the parent's.
Unknown context never becomes an unconditional fact. Every parent header and
generation manifest binds the actual qualification route.

The existing facet queue atomically publishes page/block/head/completion. Complete
nested observations preserve conditions, exceptions, conflicts, time and evidence.
Metadata is checked before bodies and again after the final awaited body/write;
route/authority/lease/job expiry causes denial or transaction rollback. The durable
clock high-water also prevents a restarted process from reviving expired outputs.

### Project QuestionView-to-L2 full projection

`questions.pages.register(page_id, question_ids, readers=...)` is a host API for
1–4 distinct templates from one project and the same trusted runtime. Refresh the
parents through `question_answer` or the shared worker, then call
`questions.pages.publish(page_id, actor=...)`. This host-triggered bounded full
projection consumes only ready parents; it does not secretly compute missing
parents or advertise automatic page maintenance/a separate page scheduler.

The transaction persists immutable page content, stable block identities, a
certificate and a head. Every block contains the whole parent answer; its original
generation manifest binds actual parent content AND certificate revisions, plus
the complete internal processing metadata. Public page manifests expose immutable
input references and authenticated parent proof digests, never private moved-out
candidate routing headers. Full rebuild preserves stable block IDs while
producing new revision identities. No block's old provenance is overwritten.
Read-time guards reject the whole page after any parent source/member change,
even before rebuilding. Actual materialized blocks, references, status and
manifests count toward the complete response budget; truncation is not success.

Run the end-to-end synthetic host example with core + SDK installed:

```sh
python examples/project_questions.py
```

It stages and reviews evidence, registers/refreshes all four questions, publishes
a four-block page, reads it through the SDK and demonstrates denial after source
permission revocation. `test_question_pages_v7.py` also exercises the same path
through the actual MCP server and invalidation after a new conflicting source.

## Migration, erasure and rollback

Stop/drain old writers before enabling B3. PostgreSQL migration
`018_project_candidate_index.sql` adds project route indexes; SQLite installs the
equivalent table/index. First access rebuilds an exact-scope bounded index under
the existing writer lock and conservative barrier. Existing legacy candidates
without a project contract remain wildcard dependencies, never silently ignored.

Generic derived storage now holds `question_registration`, `question_content`,
`question_certificate`, `question_head`, and five `question_page_*` record kinds.
Qualified language generations use `derived-input-manifest/3` and explicit current
route proofs. Backend contract gates reject unsupported adapters; no new schema
is inferred from missing fields. Host state/authority restore pins remain required. Old in-memory snapshots must be recaptured by the new writer; publication
requires the fenced job's committed snapshot digest. PostgreSQL core, optional vector
and ontology-trigger startup share a schema-scoped transaction advisory lock before
DDL; it remains held through migration and legacy-header/history backfill.

Erasure follows every original processing input, including uncited inputs and
older content generations. It scrubs aliases, context/project labels, registration,
content, certificates, page blocks/heads and bounded route selectors, retaining
only opaque tombstones. Never-published subscriptions/pages participate too.
The shared scheduler's schema-agnostic scope hook clears its sensitive statistics,
receipts and in-flight responsibility. Unknown scheduler schemas cannot block
deletion. Authoritative deletion replay over a real old backup applies the same
cleanup and fences restored readers before admitting traffic.

Rollback means disable these opt-ins and stop the new workers; preserve the
authoritative deletion journal and restore gate. Do not serve B3 data with an old
binary or run old and new writers together. No attempt is made to down-convert
question certificates or qualified generations into weaker legacy proofs.

## Verification and remaining limits

See `validation-b3.json` for exact commands, counts, source fingerprints and the
initial regression/fix record. Counts are per-run, never cumulative. The B3 suite
covers both SQLite and real PostgreSQL, registration/routing, finite source closure,
all four full templates, complete/unknown/contested outcomes, shared-budget direct
work, input ownership/CAS, grants, source revisions, indexed membership/backfill,
qualified parent/page process crashes, exceptions during project-page atomic
publication, and actual backup/restore deletion replay.

Independent review found and fixed late host-membership changes during page reads,
failed-publication rollback losing the observed expiry floor, and public manifests
revealing moved-out project/source routing metadata. A full regression also exposed
a late-heartbeat/completed-publication race and concurrent PostgreSQL startup DDL
deadlock; both now have deterministic counterexamples and dedicated fixes.

The inputs are synthetic deterministic fixtures, not licensed real conversations
or an automatic semantic extractor evaluation. T02's real gold/quality obligations
and T13/T15 production benefit gates remain open. Generic historical project
questions/pages, cross-scope composition, inferred persona, model routing,
generated explanations, exact model caches, delta and partial block editing are
unsupported in B3. Stable block identities do not claim partial-edit support.
No production cost or latency savings are asserted.
