# Maintained dependency/date relation views

## Implemented boundary

This batch adds one finite host-registered relational plan to the existing project
`risks` QuestionView. `DependencyRiskPlan` joins a project dependency edge with
that project's launch date and the deliverable's commitment date. A commitment
after launch produces an **inferred rule candidate**, never a source-asserted
fact or an accepted Claim. Example: A depends on B; B is committed for October 12;
A launches October 10. The candidate identifies B and its two-day lag.

The host may register typed `RelationRule` chains over the two fixed edge types:
`project.depends_on` and `deliverable.depends_on`. The existing
`OntologyRuleEngine` validates rule domains/ranges and supplies its bounded pure
chain kernel. This is not a new graph store, query language, general relational
IR, automatic relationship extractor, OWL reasoner, or cross-project federation.
Existing bounded ontology BFS APIs remain unchanged. Runtime plans consume the
owned, host-reviewed project census, rather than independently querying an
ontology store with different security or transaction coordinates.

```python
from dataclasses import replace
from agent_memory.derived.relation_questions import DependencyRiskPlan
from agent_memory.ontology.rules import RelationRule

plan = DependencyRiskPlan(
    id="dependency-date-risk",
    version="1",
    relation_rules=(RelationRule(
        "project-dependency-chain", "1",
        "project.depends_on", "deliverable.depends_on", "project.depends_on",
    ),),
)
contract = replace(existing_project_contract, relation_plans=(plan,))
```

Construct `ProjectAdmission` with that contract, source authorities that cover the
new predicates, and authenticated memberships for the project and deliverables.
Stage and semantically qualify source-backed assertions through its existing
API. Register/read `QuestionService`'s `risks` template normally. A relation edge's
target must be a host-registered deliverable member of the same project. A date
on a deliverable belonging only to a different project cannot satisfy this join.
No transport accepts a plan, membership, source authority, or qualification from
a model. A plan is disabled unless it is explicitly included in the host contract.

## Semantics and lineage

- The only added literal predicates are `project.launch_date` and
  `deliverable.commitment_date`. They require absolute timezone-aware instants;
  comparisons and equivalent representations normalize to UTC. All arithmetic
  uses exact integer microseconds, not floats or ambiguous local dates.
- Premises are evaluated at one current valid-time/known-time coordinate. Each
  conclusion carries the intersection of its premise assertion validity
  intervals. Disjoint historical assertions never join. The certificate can
  expire earlier due to source-support, authority, grant, or context boundaries.
- Rows expose the common `id/status/matches/fields/origin/rule` interface and
  `field(name)`, plus `conclusions`, `aggregates`, `reasons`, and explicit
  `qualification="rule_candidate"`. Each conclusion carries premise IDs and
  digest, source IDs, rule versions, interval, and lag. All survive full, delta,
  cached-state, and response serialization. Actual source assertions remain
  visible as evidence; their existence does not make the inferred conclusion an
  asserted source fact.
- Unknown applicability, missing dates, unsupported premises, and conflicting
  dates remain unknown/contested. An unknown edge cannot disappear into a
  certified empty relation set. A truncated frontier is `incomplete` even when
  some positive candidates are available. There is no anti-join or world-negative
  operator. Empty means no matches in the complete authorized **known project
  census**, never no risks in reality.
- Aggregates deduplicate by deliverable endpoint, not by number of paths. The
  selected chain proof is deterministic and is not an enumeration of every
  possible proof. Counts and lag aggregates reuse `KeyedAggregate`, including
  distinct values, reference counts, sums/extrema, and independent unknown
  contributions. `total_dependencies` and `late_dependencies` are null unless
  exactness is established; partial known contributions are labeled `exact=false`
  and expose `unknown_frontier`. Missing dates never become zero lag.

## Maintenance, authorization, and hard bounds

The maintenance unit is a **complete registered plan group**, not one changed
edge. A changed edge/date or condition reevaluates that bounded plan. Existing
project full/delta proof machinery still scans and authorizes the complete
candidate frontier. Owner/status values are not changed by relation assertions,
while their complete-census safety proofs still refresh. This does not claim
sublinear graph maintenance or an avoided permission/census scan.

The B1 semantic readset records the exact union of the four declared relation
predicates, observed-risk fields and registered equality-rule predicates. A bounded
`question-relation-reads/1` declaration pins each typed plan, fingerprint and impact
conditions. All conditions/exceptions on the declared premise fields remain inputs;
unknown qualifier attributes are never treated as irrelevant. Complete registered
context checks remain mandatory. Missing, malformed or unsupported plan metadata
falls back to conservative semantic invalidation. Reviewed relation-field updates
may be semantically disjoint from owner/status while still dirtying every affected
complete-census proof. New/pending/unbound premises remain conservative entrants.

The existing transaction-local `snapshot_many`/`publish_many`/`read_many` APIs can
share a qualified census between owner/status and relation risk views. Each
consumer retains fresh authority and source checks and its own immutable result,
certificate and inferred-rule lineage; no cross-transaction relation cache is added.

The same project index subscriptions cover newly arriving, unbound, rejected,
withdrawn, moved, and future-valid premises. Query generation barriers and source
reverse dependencies invalidate stale views even if the new premise was not in
a prior supporting path. Every original processing source is in the generation
manifest. The existing CAS, retention epoch, source revision, audience, purpose,
grant/authority, and final delivery guards apply before loading cached bodies and
again before publication/return. Erasure scrubs the entire affected QuestionView,
including its conclusions and aggregates; no separate relation cache exists.

Time maintenance reuses `ProjectCensus.next_transition_at`, the full/delta result
boundary, and `question_materialize`'s minimum with context and generation
boundaries. The scheduler persists the existing `next_transition_at` field.
There is no second timer or rounded-time cache identity. Future relation/date
premises enter and expired edges leave at half-open boundaries without a write.

Bounds are explicit:

| Resource | Maximum |
| --- | ---: |
| Host relation plans per contract | 8 |
| Typed chain rules per plan | 8 |
| Chain rounds per plan | 8 (default 4) |
| Root assertions passed to chain kernel | 128 |
| Inferred chain candidates per plan | 128 (default 64) |
| Distinct deliverable joins per plan | 64 |
| Integrated project census | Existing 64 candidates / 128 inputs |
| Output | Existing registration byte limit, fail closed |

The pure protocol accepts at most 1,024 qualified facts; root overflow there is
explicitly truncated before chain evaluation. Runtime admission is already more
restrictive. With at most 256 retained root/derived relation keys, the deliberately
simple kernel has a conservative upper bound of 8 rounds × 8 rules × 256² pair
checks per plan (8 plans maximum); most inputs are much smaller. Limits are not
proof of complete transitive closure. Exhausting the last round after adding
facts conservatively marks truncation. Dates and aggregates introduce no
unbounded callback or model call. No runtime dependency was added.

## Compatibility and rollout

The host contract's optional `relation_plans` field is append-only. Empty plans
preserve the prior contract fingerprint, predicate specs, and query fingerprint.
Enabled plans pin their full definition/version into the existing contract and
QuestionDefinition identities. A change requires re-registration/review under
the changed host contract. Existing project row consumers may keep using the
common interface, but consumers rendering inferred rows should preserve the new
qualification/lineage fields rather than flatten them into asserted facts.

`derived-project-index/2` is a coordinated writer upgrade: drain old writers
before enabling it. A valid `/1` gate becomes `needs_backfill`; the existing
transactional, bounded rebuild rereads retained candidate headers and installs
routes for previously unrecognized relation predicates. It bumps the fallback
barrier so old proofs cannot bypass the expanded census. Interrupted migration
rolls back; unknown/corrupt schema and overflow fail closed. No SQL table change
or new backend is required. Older writers must not resume against the upgraded
scope; rollback requires an appropriate quiescent restore, not mixed writers.

## Verification

`tests/test_relation_questions_v7.py` is parameterized through the existing real
SQLite/live-PostgreSQL fixture. It exercises direct and typed-chain joins,
endpoint deduplication, intervals and clock-only transitions, date replacement,
edge withdrawal and newly arriving premises, source/ACL/purpose/expiry/deletion
fences, bounded truncation and unknown/contested inputs, index migration, and
unchanged owner/status semantics with refreshed census proofs, exact typed readset
binding, conservative new entrants, and B1 shared-batch publication/read guards. A seeded 100-step
protocol replay compares delta output (including candidate qualifications,
conclusions and aggregates) with full recomputation after adds/removes/date and
time changes. Related project, ontology, erasure, and proof tests remain required.

Local PostgreSQL absence is a skip, not a backend pass. Exact-head hosted
`postgres-live` CI must run these same contracts before integration is accepted.
Synthetic contract coverage does not establish extraction accuracy, business
rule suitability, domain recall, or an all-in cost benefit.
