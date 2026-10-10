"""Host-owned field dependencies, separate from whole-census proof maintenance.

The optional v1 contracts extend existing headers/subscriptions without changing
their wire schemas. Older or unproved metadata is a conservative semantic hit.
These contracts never authorize omitting census, source, ACL, or oracle checks:
every project write still invalidates the complete answer/certificate proof.
"""

from datetime import datetime

from ..conditions import Condition, instant
from ..domain import MemoryScope
from ..evidence_support import evaluate_support
from ..serialization import to_jsonable
from .model import DerivedError, digest, identity
from .relation_questions import DependencyRiskPlan, RELATION_PREDICATES

READSET_SCHEMA = "question-semantic-readset/1"
EFFECT_SCHEMA = "question-candidate-field-effect/1"
INVALIDATION_SCHEMA = "question-invalidation/1"
OPERATOR = "project-closed-fields/1"
PREDICATES = frozenset(
    {
        "project.owner", "project.status", "project.phase",
        "commitment.promisor", "commitment.action", "commitment.state", "commitment.deadline",
        "risk.label", "risk.state", *RELATION_PREDICATES,
    }
)


def predicates(contract, question):
    """Fields present in public rows, including optional displayed deadlines."""
    if question == "owner":
        return ("project.owner",)
    if question == "status":
        return ("project.status", "project.phase") if contract.require_phase else ("project.status",)
    if question == "commitments":
        return (
            "commitment.promisor", "commitment.action", "commitment.state", "commitment.deadline",
        )
    if question == "risks":
        return tuple(sorted({
            "risk.label", "risk.state", *(r.predicate for r in contract.risk_rules),
            *(predicate for plan in contract.relation_plans for predicate in plan.predicates),
        }))
    raise DerivedError("unsupported_project_question")


def _seal(value):
    return {**value, "sha256": digest(value)}


def _sealed(value, schema):
    try:
        return (
            type(value) is dict
            and value.get("schema") == schema
            and value.get("sha256") == digest({k: v for k, v in value.items() if k != "sha256"})
        )
    except (TypeError, ValueError):
        return False


def _hash(value):
    return (type(value) is str and len(value) == 64
            and all(c in "0123456789abcdef" for c in value))


def readset(contract, scope, project_id, question, *, overdue_only=False):
    """Construct only during authenticated closed-template host registration."""
    from .project_questions import ProjectDomainContract

    if type(contract) is not ProjectDomainContract or type(scope) is not MemoryScope:
        raise DerivedError("project_registered_contract_and_scope_required")
    identity(project_id)
    if type(overdue_only) is not bool or (overdue_only and question != "commitments"):
        raise DerivedError("unsupported_project_question")
    value = dict(
        schema=READSET_SCHEMA,
        operator=OPERATOR,
        scope_sha256=digest(to_jsonable(scope)),
        contract_fingerprint=contract.fingerprint,
        project_id=project_id,
        question=question,
        overdue_only=overdue_only,
        predicates=sorted(predicates(contract, question)),
        proof_scope="complete_project_census",
    )
    if question == "risks" and contract.relation_plans:
        value["relation_plans"] = [_plan_reads(plan) for plan in contract.relation_plans]
    return _seal(value)


def _plan_reads(plan):
    return dict(
        schema="question-relation-reads/1",
        plan=to_jsonable(plan),
        fingerprint=plan.fingerprint,
        predicates=list(plan.predicates),
        # All qualifiers on these fields remain semantic inputs, including
        # qualifiers whose context attributes were unknown at registration.
        qualifier_scope="all_declared_predicate_conditions_and_exceptions",
        context_scope="complete_registered_context",
    )


def _bound_plan_reads(value):
    """Only the implemented finite operator may turn a wildcard into exact reads."""
    from ..ontology.rules import RelationRule

    plans = value.get("relation_plans")
    if plans is None:
        return not set(value["predicates"]).intersection(RELATION_PREDICATES)
    if value["question"] != "risks" or type(plans) is not list or not 1 <= len(plans) <= 8:
        return False
    ids, required = set(), {"risk.label", "risk.state"}
    try:
        for declared in plans:
            raw = declared["plan"]
            if (type(raw) is not dict or type(raw.get("relation_rules")) is not list
                    or len(raw["relation_rules"]) > 8
                    or type(raw.get("impact_conditions")) is not list
                    or len(raw["impact_conditions"]) > 16):
                return False
            plan = DependencyRiskPlan(**{
                **raw,
                "relation_rules": tuple(RelationRule(**r) for r in raw["relation_rules"]),
                "impact_conditions": tuple(Condition(**c) for c in raw["impact_conditions"]),
            })
            if declared != _plan_reads(plan) or plan.id in ids:
                return False
            ids.add(plan.id)
            required.update(plan.predicates)
    except (KeyError, TypeError, ValueError, AttributeError):
        return False
    return required <= set(value["predicates"])


def bound_readset(definition, scope):
    """An absent legacy contract or an unknown version cannot narrow writes."""
    spec = definition.get("spec", {})
    value = spec.get("semantic_readset")
    parameters = spec.get("instance", {}).get("parameters", {})
    if (
        spec.get("schema") not in {"question-instance-registration/1", "question-instance-registration/2"}
        or not _sealed(value, READSET_SCHEMA)
        or value.get("operator") != OPERATOR
        or value.get("scope_sha256") != digest(to_jsonable(scope))
        or value.get("contract_fingerprint") != spec.get("contract_fingerprint")
        or value.get("project_id") != spec.get("project_id")
        or value.get("question") != spec.get("question")
        or value.get("question") not in {"owner", "status", "commitments", "risks"}
        or type(value.get("overdue_only")) is not bool
        or value.get("overdue_only") is not parameters.get("overdue_only")
        or value.get("proof_scope") != "complete_project_census"
        or type(value.get("predicates")) is not list
        or not value["predicates"]
        or any(type(p) is not str or p not in PREDICATES for p in value["predicates"])
        or value["predicates"] != sorted(set(value["predicates"]))
    ):
        return None
    if not _bound_plan_reads(value):
        return None
    return value


def candidate_effect(payload, record_id, version, *, scope, project):
    """Derive a bounded effect only from a current, fully supported host review.

    Coverage must span the entire assertion interval, not only the write instant.
    A temporally unsupported candidate can change global incomplete status even
    when its predicate is disjoint. Source truth/permission is still validated by
    the complete project snapshot and never established by this routing metadata.
    """
    try:
        return _candidate_effect(payload, record_id, version, scope, project)
    except (KeyError, TypeError, ValueError, AttributeError, IndexError):
        return None


def _candidate_effect(payload, record_id, version, scope, project):
    if type(scope) is not MemoryScope or type(version) is not int or version < 1:
        return None
    draft = payload["draft"]
    binding = payload["project_candidate"]
    membership, review = binding["membership"], binding["review"]
    qualification = payload["qualification"]
    if (
        not project or project.get("unreviewed") or project["was_unbound"]
        or project["contract_fingerprint"] is None
        or binding["schema"] != "project-candidate/1"
        or binding["contract_fingerprint"] != project["contract_fingerprint"]
        or not membership or project["project_ids"] != [membership["project_id"]]
        or project["current_project_id"] != membership["project_id"]
        or binding["membership_history"] != [membership]
        or membership["entity_id"] != draft["subject_id"]
        or (draft["predicate"].startswith("project.")
            and draft["subject_id"] != membership["project_id"])
        or draft["predicate"] not in PREDICATES
        or payload.get("deleted") or payload["action"] != "PENDING_VERIFICATION"
        or payload.get("claim_id") is not None
        or draft["kind"] != "fact" or draft["modality"] != "asserted"
        or draft["change_kind"] != "replace" or draft.get("negated", False)
        or not review or review["schema"] != "project-review/1"
        or review["disposition"] != "qualified"
        or type(review["version"]) is not int or not 1 <= review["version"] <= version
        or review["principal"] != binding["principal"]
        or review["target_sha256"] != digest(draft)
        or review["membership_sha256"] != digest(membership)
        or review["qualification_sha256"] != digest(qualification)
        or qualification["schema"] != "contextual-qualification/1"
        or qualification["principal"] != review["principal"]
        or qualification["target_sha256"] != digest(draft)
        or qualification["policy"]["revision"] != review["policy_revision"]
        or qualification["policy_sha256"] != digest(qualification["policy"])
        or len(qualification["conditions"]) != len(draft.get("conditions", ()))
        or len(qualification["exceptions"]) != len(draft.get("exceptions", ()))
    ):
        return None
    for key in ("binding_id", "registry_revision", "project_id", "entity_id"):
        identity(membership[key])
    for key in ("id", "reviewer_version", "policy_revision", "principal"):
        identity(review[key])
    required = {"subject_id", "predicate", "value", "valid_from"}
    required.update(k for k in ("valid_to", "conditions", "exceptions") if draft.get(k))
    supported = qualification["field_support"]
    if not required <= {f["field"] for f in supported}:
        return None
    links = qualification["links"]
    if not links or len({link["id"] for link in links}) != len(links):
        return None
    if any(link["target_sha256"] != digest(draft) for link in links):
        return None
    start = instant(datetime.fromisoformat(draft["valid_from"]))
    end = instant(datetime.fromisoformat(draft["valid_to"])) if draft.get("valid_to") else None
    if end is not None and end <= start:
        return None
    ranges, _ = evaluate_support(qualification, {link["span"]["source_event_id"] for link in links})
    if not any(
        r.kind == "interval" and r.start <= start
        and (r.end is None or (end is not None and r.end >= end))
        for r in ranges
    ):
        return None
    return _seal(dict(
        schema=EFFECT_SCHEMA,
        readset_schema=READSET_SCHEMA,
        operator=OPERATOR,
        scope_sha256=digest(to_jsonable(scope)),
        candidate_id=record_id,
        candidate_version=version,
        contract_fingerprint=binding["contract_fingerprint"],
        project_id=membership["project_id"],
        membership_sha256=digest(membership),
        project_sha256=digest(project),
        predicate=draft["predicate"],
        qualification_sha256=digest(qualification),
        review_sha256=digest(review),
        completeness="reviewed_full_assertion_interval",
    ))


def _bound_effect(header, read):
    from .project_index import checked_project
    from .subscriptions import HEADER_SCHEMA

    if type(header) is not dict or header.get("schema") != HEADER_SCHEMA:
        return None
    effect = header.get("field_effect")
    if not _sealed(effect, EFFECT_SCHEMA):
        return None
    try:
        project = checked_project(header.get("project"))
    except DerivedError:
        return None
    if (
        not project or project.get("unreviewed") or project["was_unbound"]
        or project["project_ids"] != [read["project_id"]]
        or project["current_project_id"] != read["project_id"]
        or project["contract_fingerprint"] != read["contract_fingerprint"]
        or effect.get("readset_schema") != READSET_SCHEMA
        or effect.get("operator") != read["operator"]
        or effect.get("scope_sha256") != read["scope_sha256"]
        or effect.get("contract_fingerprint") != read["contract_fingerprint"]
        or effect.get("project_id") != read["project_id"]
        or effect.get("project_sha256") != digest(project)
        or effect.get("candidate_id") != header.get("id")
        or type(effect.get("candidate_version")) is not int
        or effect["candidate_version"] < 1
        or effect["candidate_version"] != header.get("version")
        or effect["candidate_version"] != header.get("generation")
        or effect.get("predicate") not in PREDICATES
        or effect.get("completeness") != "reviewed_full_assertion_interval"
        or any(not _hash(effect.get(key)) for key in (
            "membership_sha256", "qualification_sha256", "review_sha256",
        ))
    ):
        return None
    return effect


def classify(definition, scope, *, candidate_change=None, safety=False):
    """Return semantic classification only; callers must always dirty proof."""
    result = dict(schema=INVALIDATION_SCHEMA, semantic_dirty=True, proof_dirty=True,
                  classification="conservative", reason="unproved_effect")
    if safety:
        return {**result, "reason": "safety"}
    read = bound_readset(definition, scope)
    if read is None:
        return {**result, "reason": "unproved_readset"}
    if not candidate_change or len(candidate_change) != 2:
        return result
    before, after = (_bound_effect(h, read) for h in candidate_change)
    if not before or not after:
        return result
    if (
        before["candidate_id"] != after["candidate_id"]
        or before["membership_sha256"] != after["membership_sha256"]
        or after["candidate_version"] != before["candidate_version"] + 1
    ):
        return {**result, "reason": "membership_or_version_transition"}
    if {before["predicate"], after["predicate"]} & set(read["predicates"]):
        return {**result, "classification": "predicate_overlap", "reason": "declared_field"}
    return {**result, "semantic_dirty": False, "classification": "predicate_disjoint",
            "reason": "qualified_disjoint_fields"}


def record_invalidation(definition, scope, *, candidate_change=None, safety=False):
    """Keep observable semantic work distinct from mandatory proof maintenance."""
    decision = classify(definition, scope, candidate_change=candidate_change, safety=safety)
    definition["semantic_dirty"] = (
        definition.get("semantic_dirty", definition.get("dirty", True))
        or decision["semantic_dirty"]
    )
    definition["proof_dirty"] = True
    for field, increment in (
        ("semantic_dirty_count", int(decision["semantic_dirty"])),
        ("proof_dirty_count", 1),
        ("predicate_disjoint_count", int(not decision["semantic_dirty"])),
    ):
        definition[field] = definition.get(field, 0) + increment
    definition["last_invalidation"] = decision
    return decision
