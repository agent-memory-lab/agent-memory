"""Finite host-selected dependency/date joins over the owned project census.

This module is a pure plan, not another admission authority or graph store. Roots
are qualified L1 project members already fenced by QuestionService. The existing
ontology typed-chain kernel supplies bounded relation candidates; dates are joined
only at the same current valid/known coordinate. No absence anti-join exists.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from ..conditions import Condition, applicability
from ..ontology.model import OntologyAssertion, OntologyClass, OntologyProperty, OntologySchema
from ..ontology.rules import OntologyRuleEngine, RelationRule
from ..serialization import to_jsonable
from .model import DerivedError, digest, identity
from .question_model import AnswerStatus

RELATION_PREDICATES = (
    "project.depends_on", "deliverable.depends_on",
    "project.launch_date", "deliverable.commitment_date",
)
EDGE_PREDICATES = RELATION_PREDICATES[:2]
DATE_PREDICATES = RELATION_PREDICATES[2:]
SCHEMA = OntologySchema(
    "project-dependencies", "1",
    (OntologyClass("project", "Project"), OntologyClass("deliverable", "Deliverable")),
    (OntologyProperty("project.depends_on", "Depends on", "project", "deliverable"),
     OntologyProperty("deliverable.depends_on", "Depends on", "deliverable", "deliverable")),
    created_at=datetime(2026, 1, 1, tzinfo=UTC),
)


@dataclass(frozen=True, slots=True)
class DependencyRiskPlan:
    """The only relational plan in this version: dependency + due > launch.

    Optional RelationRules extend the two registered edge types by typed chains.
    Bounds never authorize a complete result when a frontier is truncated.
    Aggregates have exact integer units and separate unknown contributions.
    """

    id: str
    version: str
    label: str = "dependency commitment after project launch"
    relation_rules: tuple[RelationRule, ...] = ()
    impact_conditions: tuple[Condition, ...] = ()
    max_rounds: int = 4
    max_candidates: int = 64
    max_dependencies: int = 64
    schema: str = "dependency-risk-plan/1"

    def __post_init__(self):
        for value in (self.id, self.version, self.label):
            identity(value)
        if self.schema != "dependency-risk-plan/1":
            raise DerivedError("unsupported_relation_plan")
        for name, maximum in (("max_rounds", 8), ("max_candidates", 128),
                              ("max_dependencies", 64)):
            if type(getattr(self, name)) is not int or not 1 <= getattr(self, name) <= maximum:
                raise DerivedError("relation_plan_capacity")
        if (not isinstance(self.relation_rules, (tuple, list))
                or len(self.relation_rules) > 8
                or any(type(r) is not RelationRule for r in self.relation_rules)):
            raise DerivedError("relation_plan_rules_required")
        rules = tuple(sorted(self.relation_rules, key=lambda r: r.rule_id))
        if rules:
            # Share the existing ontology domain/range and unique-rule checks.
            OntologyRuleEngine(None, SCHEMA, None, rules,
                               max_rounds=self.max_rounds, max_candidates=self.max_candidates)
        object.__setattr__(self, "relation_rules", rules)
        if (not isinstance(self.impact_conditions, (tuple, list))
                or len(self.impact_conditions) > 16
                or any(type(c) is not Condition for c in self.impact_conditions)):
            raise DerivedError("relation_plan_conditions_required")
        object.__setattr__(self, "impact_conditions", tuple(self.impact_conditions))

    @property
    def fingerprint(self):
        return digest(to_jsonable(self))

    @property
    def predicates(self):
        return RELATION_PREDICATES


@dataclass(frozen=True, slots=True)
class DependencyConclusion:
    """An inferred rule qualification with explicit source-assertion premises."""

    id: str
    project_id: str
    deliverable_id: str
    status: AnswerStatus
    matches: bool | None
    premise_ids: tuple[str, ...]
    premise_digest: str
    source_event_ids: tuple[str, ...]
    rule_versions: tuple[str, ...]
    valid_from: datetime | None
    valid_to: datetime | None
    lag_microseconds: int | None
    qualification: str = "rule_candidate"
    origin: str = "inferred"


@dataclass(frozen=True, slots=True)
class DependencyRiskResult:
    id: str
    status: AnswerStatus
    matches: bool | None
    fields: tuple
    origin: str
    rule: DependencyRiskPlan
    conclusions: tuple[DependencyConclusion, ...]
    aggregates: dict
    reasons: tuple[str, ...]
    qualification: str = "rule_candidate"

    def field(self, name):
        return next(item for item in self.fields if item.name == name)


def evaluate_dependency_plan(plan, snapshot, items=None):
    """One complete plan group. Unknown premises cannot disappear into an empty set."""
    from .project_questions import _deadline, _field, _row_status
    from .question_delta import KeyedAggregate

    context = snapshot.context
    items = tuple(i for i in (snapshot.facts if items is None else items)
                  if i.fact.predicate in plan.predicates
                  and i.possibly_active(context.valid_at, context.known_at))
    by_id = {item.fact.id: item for item in items}
    fields = tuple(
        _field(predicate, [i for i in items if i.fact.predicate == predicate],
               context, multiple=True)
        for predicate in plan.predicates
    )
    condition = applicability(plan.impact_conditions, (), context)
    reasons, uncertain = set(), False
    roots, entities, truncated = [], {}, False
    for item in items:
        f = item.fact
        if f.predicate not in EDGE_PREDICATES:
            continue
        state = applicability(f.conditions, f.exceptions, context)
        if item.missing_support or state is None or f.origin != "observed":
            uncertain = True
            continue
        if state is False:
            continue
        # Map arbitrary host entity labels to ontology-safe opaque identifiers.
        subject, target = "entity:" + digest(f.entity_id), "entity:" + digest(f.value)
        entities[subject], entities[target] = f.entity_id, f.value
        sources = tuple(sorted({s.source_event_id for s in item.source_references}))
        if len(sources) > 128:
            truncated = True
            continue
        roots.append(OntologyAssertion(
            assertion_id=f.id, scope=snapshot.scope, ontology_id=SCHEMA.ontology_id,
            ontology_version=SCHEMA.version, subject_entity_id=subject,
            predicate_id=f.predicate, object_entity_id=target,
            source_event_ids=sources,
            text="host-qualified dependency premise", confidence=1.0,
            valid_from=f.valid_from, valid_to=f.valid_to, created_at=f.known_from,
        ))
    # Protocol snapshots allow more inputs than the current integrated 64-candidate
    # census. The pure plan has the same explicit bounded/incomplete behavior.
    truncated |= len(roots) > 128
    roots = sorted(roots, key=lambda r: r.assertion_id)[:128]
    dependencies = {}
    for root in roots:
        if root.predicate_id == "project.depends_on":
            dependencies.setdefault(entities[root.object_entity_id], ((root.assertion_id,), ()))
    if roots and plan.relation_rules:
        engine = OntologyRuleEngine(None, SCHEMA, None, plan.relation_rules,
                                    max_rounds=plan.max_rounds,
                                    max_candidates=plan.max_candidates)
        inferred = engine.derive_checked_roots(snapshot.scope, roots, at_time=context.valid_at)
        truncated |= inferred.truncated
        for candidate in inferred.candidates:
            if (candidate.predicate == "project.depends_on"
                    and entities.get(candidate.subject) == snapshot.project_id):
                dependencies.setdefault(entities[candidate.object],
                                        (candidate.premise_ids, candidate.rule_versions))
    truncated |= len(dependencies) > plan.max_dependencies
    dependencies = dict(sorted(dependencies.items())[:plan.max_dependencies])
    launch = _field("project.launch_date", [i for i in items
                    if i.fact.entity_id == snapshot.project_id
                    and i.fact.predicate == "project.launch_date"], context)
    conclusions, counts, lags = [], KeyedAggregate("late-dependency"), KeyedAggregate("microsecond")
    for target, (edge_ids, chain_versions) in dependencies.items():
        deadline = _field("deliverable.commitment_date", [i for i in items
                          if i.fact.entity_id == target
                          and i.fact.predicate == "deliverable.commitment_date"], context)
        status, matches, lag = _row_status((launch, deadline)), None, None
        if status == AnswerStatus.RESOLVED:
            delta = _deadline(deadline.value) - _deadline(launch.value)
            lag = ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
        if condition is False:
            status, matches = AnswerStatus.RESOLVED, False
        elif condition is None:
            status = AnswerStatus.UNKNOWN
        elif status == AnswerStatus.RESOLVED:
            matches = lag > 0
        premise_ids = tuple(sorted(set(edge_ids) | {i.fact.id for field in (launch, deadline)
                                                   for i in field.candidates}))
        premises = tuple(by_id[key] for key in premise_ids)
        start = max((p.fact.valid_from for p in premises), default=None)
        end = min((p.fact.valid_to for p in premises if p.fact.valid_to), default=None)
        # Every premise was selected at the same valid coordinate. Preserve the
        # interval intersection explicitly; never combine disjoint historical rows.
        if end is not None and start is not None and start >= end:
            status, matches, lag = AnswerStatus.UNKNOWN, None, None
        key = "dependency:" + digest([plan.id, snapshot.project_id, target])
        conclusions.append(DependencyConclusion(
            key, snapshot.project_id, target, status, matches, premise_ids,
            digest(to_jsonable(premises)),
            tuple(sorted({s.source_event_id for p in premises for s in p.source_references})),
            tuple(sorted((*chain_versions, plan.id + "@" + plan.version))),
            start, end, lag,
        ))
        counts.replace(key, "unknown" if matches is None else int(matches), unit=counts.unit)
        lags.replace(key, "unknown" if lag is None else lag, unit=lags.unit)
    if uncertain:
        reasons.add("unknown_relation_premise")
    if condition is None:
        reasons.add("unknown_rule_applicability")
    if truncated:
        reasons.add("relation_frontier_truncated")
    if truncated:
        status = AnswerStatus.INCOMPLETE
    elif any(c.status == AnswerStatus.CONTESTED for c in conclusions):
        status = AnswerStatus.CONTESTED
    elif uncertain or condition is None or any(c.matches is None for c in conclusions):
        status = AnswerStatus.UNKNOWN
    else:
        status = AnswerStatus.RESOLVED
    matches = (any(c.matches is True for c in conclusions)
               if status == AnswerStatus.RESOLVED else None)
    # Counts are only exact in the authorized complete known census. Lower known
    # counts remain visible with their uncertainty; no missing/unknown becomes 0.
    exact = (status == AnswerStatus.RESOLVED and not incomplete_frontier(snapshot)
             and all(c.lag_microseconds is not None for c in conclusions))
    aggregates = dict(
        exact=exact, scope_basis="complete_known_project_census",
        total_dependencies=len(conclusions) if exact else None,
        late_dependencies=sum(c.matches is True for c in conclusions) if exact else None,
        unknown_frontier=uncertain or truncated or bool(incomplete_frontier(snapshot)),
        dependencies=counts.result(), lag=lags.result(),
    )
    return DependencyRiskResult("rule:" + plan.id, status, matches, fields, "inferred",
                                plan, tuple(conclusions), aggregates, tuple(sorted(reasons)))


def incomplete_frontier(snapshot):
    """Pure complete-census conditions that affect an aggregate's exactness."""
    reasons = set(snapshot.coverage.incomplete_reasons)
    if any({"subject_id", "predicate"} & set(item.missing_support)
           for item in snapshot.facts
           if item.possibly_active(snapshot.context.valid_at, snapshot.context.known_at)):
        reasons.add("candidate_membership_unproved")
    return tuple(sorted(reasons))
