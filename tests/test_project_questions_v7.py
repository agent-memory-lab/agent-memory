"""Synthetic protocol fixtures; not licensed domain gold or integrated admission tests."""

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from itertools import permutations

import pytest

from agent_memory.conditions import Condition, ContextAttribute, QueryContext
from agent_memory.derived.model import DerivedError, digest
from agent_memory.derived.project_questions import (
    PROJECT_CONTRACT_SCHEMA,
    PROJECT_FULL_ALGORITHM,
    ProjectDomainContract,
    ProjectFact,
    ProjectInputSnapshot,
    ProjectQualification,
    ProjectRiskRule,
    ProjectSnapshotCoverage,
    QualifiedProjectFact,
    full_project_question,
    project_query_fingerprint,
)
from agent_memory.derived.question_model import AnswerStatus
from agent_memory.domain import MemoryScope, PredicateSpec, SourceAuthority
from agent_memory.fact_qualification import FieldEvidence, SourceSpan

BASE = datetime(2026, 10, 1, tzinfo=UTC)
SCOPE = MemoryScope("tenant", user_id="user", workspace_id="workspace")


def at(hours):
    return BASE + timedelta(hours=hours)


@pytest.fixture
def contract():
    return ProjectDomainContract(
        id="project-domain",
        version="1",
        qualification_revision="host-reviewed/1",
        owner_role="accountable",
        project_statuses=("planned", "active", "blocked", "done"),
        project_phases=("design", "build"),
    )


def qualified(
    predicate,
    value,
    *,
    entity="P",
    fact_id=None,
    valid_from=None,
    known_from=None,
    valid_to=None,
    known_to=None,
    conditions=(),
    exceptions=(),
    support=None,
    origin="observed",
    policy="host-reviewed/1",
    project="P",
):
    fact = ProjectFact(
        fact_id or f"{entity}:{predicate}:{value}",
        project,
        entity,
        predicate,
        value,
        valid_from or at(0),
        known_from or at(0),
        valid_to,
        known_to,
        conditions,
        exceptions,
        origin,
    )
    fields = {"subject_id", "predicate", "value", "valid_from"}
    if valid_to is not None:
        fields.add("valid_to")
    if conditions:
        fields.add("conditions")
    if exceptions:
        fields.add("exceptions")
    if support is not None:
        fields = set(support)
    span = SourceSpan("source:" + fact.id, 0, len(value), value)
    authority = SourceAuthority("business-system", "tool_observation", (entity,), (predicate,))
    review = ProjectQualification(
        "review:" + fact.id,
        policy,
        fact.fingerprint,
        authority,
        tuple(FieldEvidence(field, ((span,),)) for field in sorted(fields)),
    )
    return QualifiedProjectFact(fact, review)


def snapshot(
    contract,
    facts=(),
    *,
    valid_at=None,
    known_at=None,
    complete=True,
    truncated=(),
    pending=(),
    basis="admitted_l1",
    closed=None,
    attributes=(),
):
    facts = tuple(facts)
    return ProjectInputSnapshot(
        "snapshot",
        SCOPE,
        "P",
        contract.fingerprint,
        QueryContext(
            "host",
            SCOPE,
            "P",
            "agent_context",
            valid_at or at(10),
            known_at or at(10),
            tuple(attributes),
            "UTC",
            "snapshot",
        ),
        ProjectSnapshotCoverage(
            project_query_fingerprint(contract, SCOPE, "P"),
            basis,
            complete,
            len(facts),
            truncated,
            pending,
            closed,
        ),
        facts,
    )


def commitment(*, state="open", deadline="2026-10-01T10:00:00+00:00", entity="C"):
    fields = [
        qualified("commitment.promisor", "alice", entity=entity),
        qualified("commitment.action", "Ship the spec", entity=entity),
    ]
    if state is not None:
        fields.append(qualified("commitment.state", state, entity=entity))
    if deadline is not None:
        fields.append(qualified("commitment.deadline", deadline, entity=entity))
    return fields


def test_registered_predicate_specs_are_bounded_explicit_contracts(contract):
    assert len(contract.predicate_specs) == 9
    assert all(isinstance(spec, PredicateSpec) for spec in contract.predicate_specs)
    assert all(not spec.allow_self_report for spec in contract.predicate_specs)
    assert all(
        set(spec.required_evidence_fields) == {"subject_id", "predicate", "value", "valid_from"}
        for spec in contract.predicate_specs
    )
    assert contract.schema == PROJECT_CONTRACT_SCHEMA
    assert contract.fingerprint != replace(contract, owner_role="delivery").fingerprint
    assert (
        contract.fingerprint
        != replace(contract, calendar_version="absolute-deadline/1", version="2").fingerprint
    )


@pytest.mark.parametrize("question", ["owner", "status"])
def test_no_record_is_unknown_never_ownerless_or_invented_status(contract, question):
    result = full_project_question(contract, snapshot(contract), question)
    assert result.status == AnswerStatus.UNKNOWN
    assert result.matched_ids == ()
    assert result.rows[0].fields[0].known_values == ()


def test_exclusive_owners_are_contested_and_all_sources_survive(contract):
    alice = qualified("project.owner", "alice")
    bob = qualified("project.owner", "bob")
    result = full_project_question(contract, snapshot(contract, [alice, bob]), "owner")
    field = result.rows[0].field("project.owner")
    assert result.status == field.status == AnswerStatus.CONTESTED
    assert field.value is None
    assert field.known_values == ("alice", "bob")
    assert set(field.candidates) == {alice, bob}
    assert set(result.processing_references) == set(alice.source_references + bob.source_references)


def test_registered_multiple_owner_role_has_set_semantics(contract):
    contract = replace(contract, owner_cardinality="multiple")
    facts = [qualified("project.owner", "alice"), qualified("project.owner", "bob")]
    result = full_project_question(contract, snapshot(contract, facts), "owner")
    assert result.status == AnswerStatus.RESOLVED
    assert result.rows[0].field("project.owner").known_values == ("alice", "bob")


def test_owner_ending_and_delegation_are_explicit_half_open_intervals(contract):
    facts = [
        qualified("project.owner", "alice", valid_to=at(10)),
        qualified("project.owner", "bob", valid_from=at(10)),
    ]
    earlier = full_project_question(contract, snapshot(contract, facts, valid_at=at(9)), "owner")
    current = full_project_question(contract, snapshot(contract, facts, valid_at=at(10)), "owner")
    assert earlier.rows[0].fields[0].value == "alice"
    assert earlier.next_transition_at == at(10)
    assert current.rows[0].fields[0].value == "bob"


def test_status_retains_partial_known_fields_without_summarizing_missing_phase(contract):
    contract = replace(contract, require_phase=True)
    result = full_project_question(
        contract, snapshot(contract, [qualified("project.status", "active")]), "status"
    )
    assert result.status == AnswerStatus.UNKNOWN
    assert result.rows[0].field("project.status").value == "active"
    assert result.rows[0].field("project.phase").status == AnswerStatus.UNKNOWN


def test_inferred_project_state_is_not_promoted_to_authoritative_status(contract):
    fact = qualified("project.status", "done", origin="inferred")
    result = full_project_question(contract, snapshot(contract, [fact]), "status")
    assert result.status == AnswerStatus.UNKNOWN
    assert result.rows[0].fields[0].candidates == (fact,)
    assert result.rows[0].fields[0].reasons == ("inference_not_authoritative_state",)


def test_missing_value_evidence_preserves_fact_but_cannot_resolve_it(contract):
    fact = qualified("project.owner", "alice", support=("subject_id", "predicate", "valid_from"))
    result = full_project_question(contract, snapshot(contract, [fact]), "owner")
    assert result.status == AnswerStatus.UNKNOWN
    assert result.rows[0].fields[0].candidates == (fact,)
    assert "unsupported_field:value" in result.rows[0].fields[0].reasons


def test_conditions_and_exceptions_remain_attached_to_evidence(contract):
    conditions = (Condition("eq", "region", "EU"),)
    exceptions = (Condition("eq", "holiday", True),)
    fact = qualified("project.owner", "alice", conditions=conditions, exceptions=exceptions)
    attrs = (ContextAttribute("region", "EU", "host"), ContextAttribute("holiday", False, "host"))
    result = full_project_question(contract, snapshot(contract, [fact], attributes=attrs), "owner")
    assert result.status == AnswerStatus.RESOLVED
    candidate = result.rows[0].fields[0].candidates[0]
    assert candidate.fact.conditions == conditions
    assert candidate.fact.exceptions == exceptions
    assert candidate.source_references == fact.source_references
    assert result.next_transition_at == at(24)
    unknown = full_project_question(contract, snapshot(contract, [fact]), "owner")
    assert unknown.status == AnswerStatus.UNKNOWN
    assert "unknown_applicability" in unknown.rows[0].fields[0].reasons
    exempt = full_project_question(
        contract,
        snapshot(
            contract,
            [fact],
            attributes=(
                ContextAttribute("region", "EU", "host"),
                ContextAttribute("holiday", True, "host"),
            ),
        ),
        "owner",
    )
    assert exempt.status == AnswerStatus.UNKNOWN
    assert exempt.processing_references == fact.source_references


def test_missing_qualifier_support_cannot_be_dropped_to_resolve(contract):
    fact = qualified(
        "project.owner",
        "alice",
        conditions=(Condition("eq", "holiday", False),),
        support=("subject_id", "predicate", "value", "valid_from"),
    )
    result = full_project_question(
        contract,
        snapshot(contract, [fact], attributes=(ContextAttribute("holiday", False, "host"),)),
        "owner",
    )
    assert result.status == AnswerStatus.UNKNOWN
    assert "unsupported_field:conditions" in result.rows[0].fields[0].reasons


@pytest.mark.parametrize("overdue_only", [False, True])
def test_no_completion_observation_does_not_prove_uncompleted(contract, overdue_only):
    result = full_project_question(
        contract,
        snapshot(contract, commitment(state=None)),
        "commitments",
        overdue_only=overdue_only,
    )
    assert result.status == AnswerStatus.UNKNOWN
    assert result.matched_ids == ()
    assert result.rows[0].field("commitment.action").value == "Ship the spec"
    assert result.rows[0].field("commitment.state").status == AnswerStatus.UNKNOWN
    assert result.rows[0].matches is None


def test_explicit_open_state_is_sufficient_for_uncompleted_not_absence_of_done(contract):
    result = full_project_question(
        contract, snapshot(contract, commitment(deadline=None)), "commitments"
    )
    assert result.status == AnswerStatus.RESOLVED
    assert result.matched_ids == ("C",)
    assert result.rows[0].field("commitment.deadline").status == AnswerStatus.UNKNOWN


def test_missing_deadline_is_unknown_for_overdue_membership(contract):
    result = full_project_question(
        contract, snapshot(contract, commitment(deadline=None)), "commitments", overdue_only=True
    )
    assert result.status == AnswerStatus.UNKNOWN
    assert result.rows[0].matches is None


def test_deadline_boundary_changes_membership_without_a_write(contract):
    facts = commitment()
    before = full_project_question(
        contract, snapshot(contract, facts, valid_at=at(9)), "commitments", overdue_only=True
    )
    at_due = full_project_question(
        contract, snapshot(contract, facts, valid_at=at(10)), "commitments", overdue_only=True
    )
    assert before.status == AnswerStatus.EMPTY
    assert before.next_transition_at == at(10)
    assert at_due.status == AnswerStatus.RESOLVED
    assert at_due.matched_ids == ("C",)


def test_equivalent_absolute_deadlines_are_one_value(contract):
    facts = commitment()
    facts.append(qualified("commitment.deadline", "2026-10-01T12:00:00+02:00", entity="C"))
    result = full_project_question(
        contract, snapshot(contract, facts), "commitments", overdue_only=True
    )
    assert result.status == AnswerStatus.RESOLVED
    assert result.rows[0].field("commitment.deadline").known_values == (
        "2026-10-01T10:00:00+00:00",
    )


def test_late_completion_changes_new_knowledge_not_historical_knowledge(contract):
    fields = [item for item in commitment() if item.fact.predicate != "commitment.state"]
    fields += [
        qualified("commitment.state", "open", entity="C", known_to=at(12)),
        qualified(
            "commitment.state", "completed", entity="C", valid_from=at(9.5), known_from=at(12)
        ),
    ]
    old = full_project_question(
        contract,
        snapshot(contract, fields, valid_at=at(11), known_at=at(11)),
        "commitments",
        overdue_only=True,
    )
    new = full_project_question(
        contract,
        snapshot(contract, fields, valid_at=at(11), known_at=at(12)),
        "commitments",
        overdue_only=True,
    )
    replay = full_project_question(
        contract,
        snapshot(contract, fields, valid_at=at(11), known_at=at(11)),
        "commitments",
        overdue_only=True,
    )
    assert old.status == AnswerStatus.RESOLVED
    assert old.matched_ids == ("C",)
    assert new.status == AnswerStatus.EMPTY
    assert old == replay
    assert new.valid_at == old.valid_at == at(11)
    assert new.known_at == at(12)
    assert old.known_at == at(11)


@pytest.mark.parametrize("state", ["completed", "cancelled"])
def test_explicit_terminal_commitment_can_be_excluded(contract, state):
    result = full_project_question(
        contract, snapshot(contract, commitment(state=state)), "commitments"
    )
    assert result.status == AnswerStatus.EMPTY
    assert result.rows[0].matches is False


def test_contested_completion_is_not_removed_from_unknowns(contract):
    facts = commitment()
    facts.append(qualified("commitment.state", "completed", entity="C"))
    result = full_project_question(contract, snapshot(contract, facts), "commitments")
    assert result.status == AnswerStatus.CONTESTED
    assert result.rows[0].matches is None
    assert result.rows[0].field("commitment.state").known_values == ("completed", "open")


@pytest.mark.parametrize("question", ["risks", "commitments"])
def test_no_matches_only_means_complete_known_scope_empty(contract, question):
    result = full_project_question(contract, snapshot(contract), question)
    assert result.status == AnswerStatus.EMPTY
    assert result.reasons == ("no_matches_in_complete_known_scope",)
    assert result.world_negative is False
    assert result.protocol_only is True
    assert result.payload()["scope_basis"] == "declared_known_snapshot_scope"
    assert result.payload()["algorithm"] == PROJECT_FULL_ALGORITHM


@pytest.mark.parametrize(
    "kwargs,reason",
    [
        ({"complete": False}, "candidate_census_incomplete"),
        ({"truncated": ("top_k",)}, "top_k"),
        ({"truncated": ("budget", "failed_channel")}, "failed_channel"),
        ({"pending": ("candidate-1",)}, "unqualified_candidates"),
        ({"basis": "publication_manifest", "closed": False}, "publication_manifest_open"),
    ],
)
def test_incomplete_coverage_never_proves_no_risk(contract, kwargs, reason):
    result = full_project_question(contract, snapshot(contract, **kwargs), "risks")
    assert result.status == AnswerStatus.INCOMPLETE
    assert reason in result.reasons
    assert "no_matches_in_complete_known_scope" not in result.reasons


def test_truncated_nonempty_results_keep_partial_known_rows(contract):
    facts = [qualified("project.owner", "alice")]
    result = full_project_question(
        contract, snapshot(contract, facts, truncated=("top_k",)), "owner"
    )
    assert result.status == AnswerStatus.INCOMPLETE
    assert result.rows[0].field("project.owner").value == "alice"


def test_closed_publication_target_may_prove_known_scope_empty(contract):
    result = full_project_question(
        contract, snapshot(contract, basis="publication_manifest", closed=True), "risks"
    )
    assert result.status == AnswerStatus.EMPTY
    assert not result.world_negative


def test_missing_risk_state_is_unknown_not_no_risk(contract):
    result = full_project_question(
        contract,
        snapshot(contract, [qualified("risk.label", "Vendor outage", entity="R")]),
        "risks",
    )
    assert result.status == AnswerStatus.UNKNOWN
    assert result.rows[0].field("risk.label").value == "Vendor outage"


def test_observed_and_rule_inferred_risks_stay_distinct(contract):
    rule = ProjectRiskRule("blocked-delivery", "1", "project.status", "blocked", "Delivery risk")
    contract = replace(contract, risk_rules=(rule,))
    facts = [
        qualified("project.status", "blocked"),
        qualified("risk.label", "Vendor outage", entity="R"),
        qualified("risk.state", "open", entity="R"),
    ]
    result = full_project_question(contract, snapshot(contract, facts), "risks")
    assert result.status == AnswerStatus.RESOLVED
    assert result.matched_ids == ("R", "rule:blocked-delivery")
    observed, inferred = result.rows
    assert observed.origin == "observed" and observed.rule is None
    assert inferred.origin == "inferred" and inferred.rule == rule
    assert inferred.fields[0].candidates[0].source_references


def test_risk_rule_with_missing_input_or_condition_is_unknown(contract):
    rule = ProjectRiskRule(
        "blocked-delivery",
        "1",
        "project.status",
        "blocked",
        "Delivery risk",
        (Condition("eq", "region", "EU"),),
    )
    contract = replace(contract, risk_rules=(rule,))
    result = full_project_question(
        contract, snapshot(contract, [qualified("project.status", "blocked")]), "risks"
    )
    assert result.status == AnswerStatus.UNKNOWN
    assert result.rows[0].rule.impact_conditions == rule.impact_conditions


def test_full_oracle_is_input_order_independent_and_does_not_mutate_inputs(contract):
    facts = [
        qualified("project.owner", "alice"),
        qualified("project.owner", "bob"),
        qualified("project.status", "active"),
    ]
    expected = full_project_question(contract, snapshot(contract, facts), "owner")
    for order in permutations(facts):
        candidate = snapshot(contract, order)
        assert full_project_question(contract, candidate, "owner") == expected
        assert full_project_question(contract, candidate, "owner").payload() == expected.payload()
    with pytest.raises(FrozenInstanceError):
        facts[0].fact.value = "changed"
    caller_list = list(facts)
    captured = snapshot(contract, caller_list)
    caller_list.clear()
    assert len(captured.facts) == 3


def test_raw_fact_dictionary_or_claim_is_never_implicitly_qualified(contract):
    raw = qualified("project.owner", "alice").fact
    for item in (raw, {"predicate": "project.owner", "value": "alice"}, object()):
        with pytest.raises(DerivedError, match="project_typed_input_required"):
            snapshot(contract, [item])
    with pytest.raises(DerivedError, match="project_registered_contract_and_snapshot_required"):
        full_project_question(contract, {"facts": []}, "owner")


def test_review_cannot_be_rebound_to_another_value_or_authority():
    fact = qualified("project.owner", "alice")
    with pytest.raises(DerivedError, match="target_mismatch"):
        QualifiedProjectFact(replace(fact.fact, value="bob"), fact.qualification)
    with pytest.raises(DerivedError, match="authority_mismatch"):
        QualifiedProjectFact(
            fact.fact,
            replace(
                fact.qualification,
                authority=SourceAuthority(
                    "system", "tool_observation", ("other",), ("project.owner",)
                ),
            ),
        )
    with pytest.raises(DerivedError, match="authoritative_source_required"):
        QualifiedProjectFact(
            fact.fact,
            replace(
                fact.qualification,
                authority=SourceAuthority("model", "self_report", ("P",), ("project.owner",)),
            ),
        )


@pytest.mark.parametrize(
    "change",
    [
        {"qualification_revision": "other"},
        {"version": "2"},
        {"owner_role": "delivery"},
    ],
)
def test_semantic_contract_change_requires_new_snapshot(contract, change):
    with pytest.raises(DerivedError, match="fingerprint_mismatch"):
        full_project_question(replace(contract, **change), snapshot(contract), "owner")


@pytest.mark.parametrize(
    "predicate,value",
    [
        ("project.status", "made-up-state"),
        ("commitment.state", "probably-done"),
        ("risk.state", "none-anywhere"),
        ("arbitrary.predicate", "yes"),
        ("commitment.deadline", "2026-10-01"),
    ],
)
def test_unregistered_semantics_and_naive_deadlines_are_rejected(contract, predicate, value):
    with pytest.raises(DerivedError):
        full_project_question(contract, snapshot(contract, [qualified(predicate, value)]), "owner")


def test_bounded_registration_rejects_ambiguous_or_unsupported_policy(contract):
    with pytest.raises(DerivedError):
        replace(contract, commitment_open_states=("completed",))
    with pytest.raises(DerivedError):
        replace(contract, owner_cardinality="pick_latest")
    with pytest.raises(DerivedError):
        replace(contract, calendar_version="guess-business-days/1")
    with pytest.raises(DerivedError):
        replace(contract, require_phase=True, project_phases=())
    with pytest.raises(DerivedError):
        ProjectRiskRule("rule", "1", "free-form-model", "yes", "Risk")
    with pytest.raises(DerivedError):
        replace(contract, project_statuses=tuple(f"state-{i}" for i in range(33)))


def test_snapshot_rejects_cross_project_scope_and_inexact_census(contract):
    snap = snapshot(contract)
    with pytest.raises(DerivedError, match="context_mismatch"):
        replace(snap, scope=MemoryScope("other", user_id="user"))
    with pytest.raises(DerivedError, match="census_count_mismatch"):
        replace(snap, coverage=replace(snap.coverage, expected_fact_count=1))
    with pytest.raises(DerivedError, match="membership_mismatch"):
        snapshot(contract, [qualified("project.owner", "alice", project="other")])
    with pytest.raises(DerivedError, match="publication_closure_required"):
        snapshot(contract, basis="publication_manifest")


def test_duplicate_fact_revisions_and_unsupported_question_flags_are_rejected(contract):
    fact = qualified("project.owner", "alice")
    with pytest.raises(DerivedError, match="duplicate_fact_revision"):
        snapshot(contract, [fact, fact])
    with pytest.raises(DerivedError, match="unsupported_project_question"):
        full_project_question(contract, snapshot(contract), "owner", overdue_only=True)
    with pytest.raises(DerivedError, match="unsupported_project_question"):
        full_project_question(contract, snapshot(contract), "free-form-model")


def test_invalid_temporal_coordinates_are_not_filled_by_wall_clock():
    fact = qualified("project.owner", "alice").fact
    with pytest.raises(ValueError):
        replace(fact, valid_from=BASE.replace(tzinfo=None))
    with pytest.raises(ValueError):
        replace(fact, known_from=None)
    with pytest.raises(DerivedError):
        replace(fact, valid_to=fact.valid_from)
    with pytest.raises(DerivedError):
        replace(fact, known_to=fact.known_from)


@pytest.mark.parametrize("qualifier", ["conditions", "exceptions"])
def test_unproved_qualifier_cannot_hide_a_competing_owner(contract, qualifier):
    # The forged qualifier would exclude Bob, but its meaning has not been
    # qualified. It must remain unknown alongside the supported owner Alice.
    kwargs = {qualifier: (Condition("eq", "holiday", qualifier == "exceptions"),)}
    bob = qualified(
        "project.owner", "bob", **kwargs, support=("subject_id", "predicate", "value", "valid_from")
    )
    facts = [qualified("project.owner", "alice"), bob]
    result = full_project_question(
        contract,
        snapshot(contract, facts, attributes=(ContextAttribute("holiday", True, "host"),)),
        "owner",
    )
    assert result.status == AnswerStatus.UNKNOWN
    field = result.rows[0].field("project.owner")
    assert field.value is None
    assert field.known_values == ("alice",)
    assert bob in field.candidates
    assert "unsupported_field:" + qualifier in field.reasons


@pytest.mark.parametrize(
    "boundary,value,missing",
    [
        ("valid_from", at(20), "valid_from"),
        ("valid_to", at(5), "valid_to"),
    ],
)
def test_unproved_valid_boundary_cannot_hide_a_competing_owner(contract, boundary, value, missing):
    support = {"subject_id", "predicate", "value", "valid_from", "valid_to"} - {missing}
    bob = qualified("project.owner", "bob", **{boundary: value}, support=tuple(support))
    result = full_project_question(
        contract, snapshot(contract, [qualified("project.owner", "alice"), bob]), "owner"
    )
    assert result.status == AnswerStatus.UNKNOWN
    assert bob in result.rows[0].fields[0].candidates
    assert "unsupported_field:" + missing in result.rows[0].fields[0].reasons


@pytest.mark.parametrize(
    "question,predicate",
    [("status", "project.status"), ("commitments", "commitment.state"), ("risks", "risk.state")],
)
def test_explicit_unknown_state_is_never_resolved(contract, question, predicate):
    if question == "status":
        contract = replace(contract, project_statuses=(*contract.project_statuses, "unknown"))
    fact = qualified(predicate, "unknown")
    result = full_project_question(contract, snapshot(contract, [fact]), question)
    assert result.status == AnswerStatus.UNKNOWN
    cell = result.rows[0].field(predicate)
    assert cell.value is None
    assert "explicit_unknown_state" in cell.reasons


def test_legacy_mutable_scope_coordinates_cannot_enter_immutable_snapshot(contract):
    mutable_user = ["user"]
    scope = MemoryScope("tenant", user_id=mutable_user)
    snap = snapshot(contract)
    context = replace(snap.context, scope=scope)
    with pytest.raises(DerivedError, match="project_exact_scope_required"):
        replace(snap, scope=scope, context=context)
    mutable_user.append("another-user")
    assert snap.scope == SCOPE


def test_unrelated_query_coverage_cannot_prove_empty_project_results(contract):
    snap = snapshot(contract)
    snap = replace(
        snap, coverage=replace(snap.coverage, query_fingerprint=digest("unrelated-query"))
    )
    with pytest.raises(DerivedError, match="project_query_fingerprint_mismatch"):
        full_project_question(contract, snap, "risks")


@pytest.mark.parametrize("missing", ["subject_id", "predicate"])
def test_unproved_census_membership_cannot_route_a_candidate_out_of_owner_query(contract, missing):
    alice = qualified("project.owner", "alice")
    apparent_status = qualified(
        "project.status",
        "active",
        support=tuple({"subject_id", "predicate", "value", "valid_from"} - {missing}),
    )
    result = full_project_question(contract, snapshot(contract, [alice, apparent_status]), "owner")
    assert result.status == AnswerStatus.INCOMPLETE
    assert "candidate_membership_unproved" in result.reasons
    assert result.rows[0].field("project.owner").value == "alice"
    assert set(apparent_status.source_references) <= set(result.processing_references)
