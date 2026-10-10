"""Closed field effects never weaken full-census proof or incomplete semantics."""

import asyncio
from dataclasses import replace

import pytest
import test_project_admission_v7 as admission
import test_project_questions_v7 as p
from test_question_delta_v7 import oracle, work
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.conditions import ProjectionPolicy
from agent_memory.consolidation.admission import draft_to_payload
from agent_memory.derived.model import DerivedError, digest
from agent_memory.derived.project_questions import ProjectRiskRule
from agent_memory.derived.question_delta import evaluate
from agent_memory.derived.question_dependencies import (
    EFFECT_SCHEMA,
    READSET_SCHEMA,
    bound_readset,
    classify,
    readset,
    record_invalidation,
)
from agent_memory.derived.subscriptions import header_data
from agent_memory.domain import MemoryScope
from agent_memory.evidence_support import EvidenceLink, FieldSupport, SupportRange
from agent_memory.fact_qualification import SourceSpan
from agent_memory.serialization import to_jsonable

contract = p.contract
store = admission.store


def definition(contract=admission.CONTRACT, scope=p.SCOPE, question="owner", **options):
    value = readset(contract, scope, "project-a", question, **options)
    return dict(
        dirty=False,
        spec=dict(
            schema="question-instance-registration/2", question=question,
            contract_fingerprint=contract.fingerprint, project_id="project-a",
            instance={"parameters": {"overdue_only": options.get("overdue_only", False)}},
            semantic_readset=value,
        ),
    )


def reviewed_payload(predicate="project.status", value="active"):
    """Build the same review envelope as the host, without claiming source truth."""
    event, draft = admission.inputs(p.SCOPE, predicate=predicate, value=value)
    target = digest(draft_to_payload(draft))
    fields = tuple(field.field for field in draft.field_evidence)
    evidence = EvidenceLink(
        "evidence", fields, target, SourceSpan(event.id, 0, len(event.content), event.content),
        admission.AUTHORITY, SupportRange(draft.valid_from),
    )
    policy = ProjectionPolicy(admission.CONTRACT.qualification_revision, "project_questions")
    qualification = dict(
        schema="contextual-qualification/1", principal=ACTOR, target_sha256=target,
        conditions=[], exceptions=[], policy=to_jsonable(policy), policy_sha256=policy.fingerprint,
        links=[to_jsonable(evidence)],
        field_support=to_jsonable(tuple(FieldSupport(field, (("evidence",),)) for field in fields)),
    )
    membership = to_jsonable(admission.MEMBERSHIPS[0])
    review = dict(
        schema="project-review/1", id="review", version=3, reviewer_version="host/1",
        principal=ACTOR, policy_revision=admission.CONTRACT.qualification_revision,
        disposition="qualified", target_sha256=target, membership_sha256=digest(membership),
        qualification_sha256=digest(qualification),
    )
    return dict(
        draft=draft_to_payload(draft), action="PENDING_VERIFICATION", claim_id=None,
        qualification=qualification,
        project_candidate=dict(
            schema="project-candidate/1", contract_fingerprint=admission.CONTRACT.fingerprint,
            principal=ACTOR, membership=membership, membership_history=[membership],
            was_unbound=False, review=review,
        ),
    )


def header(payload=None, version=3, *, scope=p.SCOPE):
    return header_data("candidate", "source", "atom:" + "a" * 64,
                       payload or reviewed_payload(), version, scope=scope)


def reseal(value):
    value["sha256"] = digest({key: item for key, item in value.items() if key != "sha256"})


def test_readsets_cover_public_optional_fields_and_risk_rule_predicates(contract):
    assert readset(contract, p.SCOPE, "P", "owner")["predicates"] == ["project.owner"]
    assert readset(contract, p.SCOPE, "P", "status")["predicates"] == ["project.status"]
    phased = replace(contract, require_phase=True)
    assert readset(phased, p.SCOPE, "P", "status")["predicates"] == [
        "project.phase", "project.status",
    ]
    for overdue in (False, True):
        assert "commitment.deadline" in readset(
            contract, p.SCOPE, "P", "commitments", overdue_only=overdue
        )["predicates"]
    ruled = replace(contract, risk_rules=(
        ProjectRiskRule("phase", "1", "project.phase", "build", "Building"),
        ProjectRiskRule("status", "1", "project.status", "blocked", "Blocked"),
    ))
    assert readset(ruled, p.SCOPE, "P", "risks")["predicates"] == [
        "project.phase", "project.status", "risk.label", "risk.state",
    ]
    assert readset(ruled, p.SCOPE, "P", "risks")["schema"] == READSET_SCHEMA


def test_disjoint_is_semantic_only_and_preserves_existing_dirty():
    before, after = header(), header(version=4)
    assert before["field_effect"]["schema"] == EFFECT_SCHEMA
    value = definition()
    assert bound_readset(value, p.SCOPE) == value["spec"]["semantic_readset"]
    result = record_invalidation(value, p.SCOPE, candidate_change=(before, after))
    assert result["classification"] == "predicate_disjoint"
    assert value["semantic_dirty"] is False and value["proof_dirty"] is True
    assert (value["semantic_dirty_count"], value["proof_dirty_count"],
            value["predicate_disjoint_count"]) == (0, 1, 1)
    value["semantic_dirty"] = True
    record_invalidation(value, p.SCOPE, candidate_change=(before, after))
    assert value["semantic_dirty"] is True
    overlapping = record_invalidation(definition(question="status"), p.SCOPE,
                                      candidate_change=(before, after))
    assert overlapping["classification"] == "predicate_overlap"
    assert overlapping["semantic_dirty"] and overlapping["proof_dirty"]


@pytest.mark.parametrize("damage", [
    "legacy_readset", "readset_version", "readset_digest", "scope", "contract", "project",
    "legacy_effect", "effect_version", "effect_digest", "effect_scope", "membership",
    "missing_membership", "missing_qualification", "new_entrant", "deleted", "version_gap",
    "wildcard", "unbound", "safety",
])
def test_every_unproved_binding_or_new_entrant_stays_broad(damage):
    value, before, after = definition(), header(), header(version=4)
    scope, safety = p.SCOPE, False
    if damage == "legacy_readset":
        del value["spec"]["semantic_readset"]
    elif damage == "readset_version":
        value["spec"]["semantic_readset"]["schema"] = "question-semantic-readset/999"
        reseal(value["spec"]["semantic_readset"])
    elif damage == "readset_digest":
        value["spec"]["semantic_readset"]["predicates"] = ["risk.label"]
    elif damage == "scope":
        scope = MemoryScope("another-tenant")
    elif damage in {"contract", "project"}:
        field = "contract_fingerprint" if damage == "contract" else "project_id"
        value["spec"][field] = "different"
    elif damage == "legacy_effect":
        del before["field_effect"]
    elif damage in {"effect_version", "effect_digest", "effect_scope", "membership"}:
        field = {"effect_version": "schema", "effect_digest": "predicate",
                 "effect_scope": "scope_sha256", "membership": "membership_sha256"}[damage]
        after["field_effect"][field] = "different"
        if damage != "effect_digest":
            reseal(after["field_effect"])
    elif damage in {"missing_membership", "missing_qualification"}:
        field = "membership_sha256" if damage == "missing_membership" else "qualification_sha256"
        for candidate in (before, after):
            candidate["field_effect"].pop(field)
            reseal(candidate["field_effect"])
    elif damage == "new_entrant":
        before = None
    elif damage == "deleted":
        after = None
    elif damage == "version_gap":
        after = header(version=5)
    elif damage == "wildcard":
        before["project"] = dict(
            schema="project-candidate/1", contract_fingerprint=None, current_project_id=None,
            project_ids=[], was_unbound=True, unreviewed=True,
        )
    elif damage == "unbound":
        before["project"]["was_unbound"] = True
    else:
        safety = True
    result = classify(value, scope, candidate_change=(before, after), safety=safety)
    assert result["classification"] == "conservative"
    assert result["semantic_dirty"] and result["proof_dirty"]


@pytest.mark.parametrize("damage", [
    "no_review", "review_target", "membership", "missing_subject", "missing_predicate",
    "missing_value", "partial_time", "unknown_time", "disposition", "historical_membership",
])
def test_unqualified_or_incomplete_candidate_never_gets_narrow_effect(damage):
    payload = reviewed_payload()
    binding, qualification = payload["project_candidate"], payload["qualification"]
    if damage == "no_review":
        binding["review"] = None
    elif damage == "review_target":
        binding["review"]["target_sha256"] = digest("wrong")
    elif damage == "membership":
        binding["membership"]["entity_id"] = "wrong"
    elif damage.startswith("missing_"):
        name = {"missing_subject": "subject_id", "missing_predicate": "predicate",
                "missing_value": "value"}[damage]
        qualification["field_support"] = [f for f in qualification["field_support"]
                                           if f["field"] != name]
        binding["review"]["qualification_sha256"] = digest(qualification)
    elif damage in {"partial_time", "unknown_time"}:
        qualification["links"][0]["support"] = (
            to_jsonable(SupportRange(p.at(0), p.at(10))) if damage == "partial_time" else None
        )
        binding["review"]["qualification_sha256"] = digest(qualification)
    elif damage == "disposition":
        binding["review"]["disposition"] = "withdrawn"
    else:
        binding["membership_history"].append(to_jsonable(admission.MEMBERSHIPS[1]))
    assert header(payload)["field_effect"] is None


@pytest.mark.parametrize("question,phase,evaluated", [
    ("owner", False, 0), ("status", False, 0), ("status", True, 1),
])
def test_project_group_reads_only_declared_fields_without_changing_oracle(
    contract, question, phase, evaluated
):
    contract = replace(contract, require_phase=phase)
    initial = [p.qualified("project.owner", "alice"), p.qualified("project.status", "active"),
               p.qualified("project.phase", "design")]
    _, old, _ = evaluate(contract, work(contract, initial, question))
    changed = initial[:2] + [p.qualified("project.phase", "build")]
    data = work(contract, changed, question, old=old, sequence=1)
    result, _, trace = evaluate(contract, data)
    assert result == oracle(contract, data)
    assert trace["groups_evaluated"] == evaluated


def test_unread_predicate_missing_membership_support_still_changes_global_incomplete(contract):
    facts = [p.qualified("project.owner", "alice"), p.qualified("project.status", "active")]
    _, old, _ = evaluate(contract, work(contract, facts, "owner"))
    changed = [facts[0], p.qualified("project.status", "active", support=("value", "valid_from"))]
    data = work(contract, changed, "owner", old=old, sequence=1)
    result, _, trace = evaluate(contract, data)
    assert result == oracle(contract, data)
    assert trace["groups_evaluated"] == 0
    assert result["status"] == "incomplete"


def test_disjoint_group_pruning_never_bypasses_global_value_validation(contract):
    initial = [p.qualified("project.owner", "alice")]
    _, old, _ = evaluate(contract, work(contract, initial, "owner"))
    changed = initial + [p.qualified("project.status", "not-a-registered-state")]
    data = work(contract, changed, "owner", old=old, sequence=1)
    with pytest.raises(DerivedError, match="unregistered_project_state"):
        evaluate(contract, data)


def test_optional_deadline_and_risk_rule_inputs_recompute_visible_rows(contract):
    for question, original, updated, configured in (
        ("commitments", p.commitment(deadline=p.at(15).isoformat()),
         p.commitment(deadline=p.at(30).isoformat()), contract),
        ("risks", [p.qualified("project.status", "active")],
         [p.qualified("project.status", "blocked")], replace(contract, risk_rules=(
             ProjectRiskRule("blocked", "1", "project.status", "blocked", "Blocked"),
         ))),
    ):
        _, old, _ = evaluate(configured, work(configured, original, question))
        data = work(configured, updated, question, old=old, sequence=1)
        result, _, trace = evaluate(configured, data)
        assert result == oracle(configured, data)
        assert trace["groups_evaluated"] == 1


def test_real_write_keeps_proof_barriers_and_new_pending_entrant_incomplete(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            owner = await admission.stage(svc.admission, scope, identity="owner")
            status = await admission.stage(svc.admission, scope, identity="status",
                                           predicate="project.status", value="active")
            await admission.qualify(svc.admission, *owner)
            await admission.qualify(svc.admission, *status)
            registered = {}
            for question in ("owner", "status"):
                registered[question] = await register(svc, question)
                await fresh(svc, clock, question, dedupe="initial:" + question)
            async with engine.repository.unit_of_work() as uow:
                before = {
                    q: await uow.derived_get(scope, "definition", r["instance_id"])
                    for q, r in registered.items()
                }
                old_header = await uow.derived_header(scope, status[2])
                assert old_header["field_effect"] is not None
                row = await uow.get_admission_record(scope, status[2])
                await svc.admission._save(uow, row)
                for q, r in registered.items():
                    current = await uow.derived_get(scope, "definition", r["instance_id"])
                    assert current["dirty"] and current["proof_dirty"]
                    assert current["proof_dirty_count"] == before[q].get("proof_dirty_count", 0) + 1
                    assert current["semantic_dirty"] is (q == "status")
                    assert current["semantic_dirty_count"] == (
                        before[q].get("semantic_dirty_count", 0) + int(q == "status")
                    )
            result = await fresh(svc, clock, dedupe="proof-maintenance")
            assert result["answer_status"] == "resolved"
            assert result["compute_trace"]["predicate_disjoint_count"] == 1
            async with engine.repository.unit_of_work() as uow:
                current = await uow.derived_get(scope, "definition", registered["owner"]["instance_id"])
                assert not current["semantic_dirty"] and not current["proof_dirty"]
            # A previously unseen field can change completeness before review.
            await admission.stage(svc.admission, scope, identity="new-risk", subject="promise-1",
                                  membership="promise-a", predicate="risk.label", value="Delay")
            async with engine.repository.unit_of_work() as uow:
                current = await uow.derived_get(scope, "definition", registered["owner"]["instance_id"])
                assert current["semantic_dirty"] and current["proof_dirty"]
                assert current["last_invalidation"]["classification"] == "conservative"
            assert (await fresh(svc, clock, dedupe="pending-new-entrant"))["answer_status"] == "incomplete"

    asyncio.run(run())
