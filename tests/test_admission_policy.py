"""Admission accepts explicit authorized states and preserves uncertain inputs."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from agent_memory.consolidation.admission import (
    AdmissionPolicy,
    authority_from_payload,
    authority_to_payload,
    candidate_id,
    draft_from_payload,
    draft_to_payload,
    slot_key,
)
from agent_memory.domain import (
    AtomDraft,
    EvidenceSupport,
    MemoryEvent,
    MemoryScope,
    PredicateSpec,
    ScopeLevel,
    SourceAuthority,
)

NOW = datetime(2026, 10, 6, tzinfo=UTC)
SCOPE = MemoryScope("tenant", user_id="alice", session_id="conversation")
EVENT = MemoryEvent(SCOPE, "message", "我住杭州。也有人说我住上海。", id="event", occurred_at=NOW)
DRAFT = AtomDraft("alice", "home_city", "杭州", "Alice住杭州", "我住杭州")
AUTHORITY = SourceAuthority("user:alice", subjects=("alice",), predicates=("home_city",))
POLICY = AdmissionPolicy([PredicateSpec("home_city")])


def test_explicit_self_report_is_accepted_without_a_confidence_score():
    action, reasons = POLICY.evaluate(EVENT, DRAFT, AUTHORITY)
    assert action == "ACCEPT"
    assert reasons
    tool_only = AdmissionPolicy([PredicateSpec("home_city", allow_self_report=False)])
    assert tool_only.evaluate(EVENT, DRAFT, AUTHORITY)[0] == "PENDING_VERIFICATION"
    tool = replace(AUTHORITY, kind="tool_observation")
    assert tool_only.evaluate(EVENT, DRAFT, tool)[0] == "ACCEPT"


def test_event_actor_and_claimed_trust_do_not_supply_authority():
    event = replace(EVENT, actor="host", metadata={"trust_label": "authoritative", "confidence": 1})
    unauthorized = SourceAuthority("untrusted", kind="unknown")
    action, reasons = POLICY.evaluate(event, DRAFT, unauthorized)
    assert action == "PENDING_VERIFICATION"
    assert {"subject_not_authorized", "predicate_not_authorized", "unsupported_source_kind"} <= set(
        reasons
    )
    assert POLICY.evaluate(event, DRAFT, replace(AUTHORITY, kind="inference"))[0] != "ACCEPT"


@pytest.mark.parametrize("quote", ["", "  ", "我住北京"])
def test_missing_or_unlocated_quotes_remain_pending(quote):
    assert POLICY.evaluate(EVENT, replace(DRAFT, source_quote=quote), AUTHORITY)[0] == (
        "PENDING_VERIFICATION"
    )


@pytest.mark.parametrize("modality", ["planned", "hypothetical", "quoted", "inferred"])
def test_plans_and_other_nonasserted_content_do_not_become_current_state(modality):
    assert POLICY.evaluate(EVENT, replace(DRAFT, modality=modality), AUTHORITY)[0] == "L0_ONLY"


def test_unknown_predicate_type_and_event_kind_cannot_publish_a_state():
    draft = replace(DRAFT, predicate="unknown")
    authority = replace(AUTHORITY, predicates=("unknown",))
    assert POLICY.evaluate(EVENT, draft, authority) == (
        "PENDING_VERIFICATION",
        ("predicate_not_registered",),
    )
    unknown_type = AdmissionPolicy([PredicateSpec("home_city", "custom")])
    assert unknown_type.evaluate(EVENT, DRAFT, AUTHORITY) == (
        "PENDING_VERIFICATION",
        ("unsupported_value_type",),
    )
    assert POLICY.evaluate(EVENT, replace(DRAFT, kind="event"), AUTHORITY)[0] == "L0_ONLY"


@pytest.mark.parametrize(
    ("value_type", "value", "accepted"),
    [
        ("number", True, False),
        ("integer", 1.5, False),
        ("boolean", 1, False),
        ("number", 1.5, True),
        ("integer", 1, True),
        ("boolean", True, True),
        ("string", None, False),
    ],
)
def test_value_types_are_strict_without_python_boolean_number_coercion(value_type, value, accepted):
    policy = AdmissionPolicy([PredicateSpec("home_city", value_type)])
    result = policy.evaluate(EVENT, replace(DRAFT, value=value), AUTHORITY)
    assert (result[0] == "ACCEPT") is accepted


def test_same_slot_competing_claims_have_distinct_candidate_identities():
    other_value = replace(DRAFT, value="上海", text="Alice住上海", source_quote="我住上海")
    other_subject = replace(DRAFT, subject_id="bob")
    assert slot_key(SCOPE, DRAFT) == slot_key(SCOPE, other_value)
    assert slot_key(SCOPE, DRAFT) != slot_key(SCOPE, other_subject)
    assert len({candidate_id(EVENT, draft) for draft in (DRAFT, other_value, other_subject)}) == 3
    assert candidate_id(EVENT, replace(DRAFT, modality="planned")) != candidate_id(EVENT, DRAFT)
    assert candidate_id(EVENT, replace(DRAFT, valid_from=NOW)) != candidate_id(EVENT, DRAFT)
    assert candidate_id(replace(EVENT, id="event-2"), DRAFT) != candidate_id(EVENT, DRAFT)


def test_projected_slot_reuses_user_state_without_crossing_tenant_or_subject():
    draft = replace(DRAFT, scope_level=ScopeLevel.USER)
    later_scope = replace(SCOPE, session_id="later")
    assert slot_key(SCOPE, draft) == slot_key(later_scope, draft)
    assert slot_key(SCOPE, draft) != slot_key(replace(SCOPE, tenant_id="other"), draft)
    assert slot_key(SCOPE, DRAFT) != slot_key(later_scope, DRAFT)


def test_saved_candidates_and_authority_roundtrip_without_changing_identity():
    local_time = NOW.astimezone(timezone(timedelta(hours=8)))
    draft = replace(DRAFT, valid_from=local_time, valid_to=local_time + timedelta(days=1))
    payload = json.loads(json.dumps(draft_to_payload(draft)))
    loaded = draft_from_payload(payload)
    assert loaded == draft
    assert candidate_id(EVENT, loaded) == candidate_id(EVENT, replace(draft, valid_from=NOW))
    saved_authority = json.loads(json.dumps(authority_to_payload(AUTHORITY)))
    assert authority_from_payload(saved_authority) == AUTHORITY


def test_config_identity_is_serializable_and_independent_of_registration_order():
    specs = [PredicateSpec("b", "boolean"), PredicateSpec("a", allow_self_report=False)]
    policy = AdmissionPolicy(specs)
    assert (
        json.loads(json.dumps(policy.config_payload()))
        == AdmissionPolicy(list(reversed(specs))).config_payload()
    )
    assert policy.config_payload()["version"] == "atom-admission-v1"
    with pytest.raises(ValueError, match="duplicate predicate"):
        AdmissionPolicy([PredicateSpec("a"), PredicateSpec("a")])


@pytest.mark.parametrize(
    "changes",
    [
        {"valid_from": NOW.replace(tzinfo=None)},
        {"valid_from": NOW, "valid_to": NOW},
        {"scope_level": "unrecognized"},
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": {"nested": "unregistered type"}},
        {"value": "x" * 16_385},
        {"subject_id": ""},
        {"predicate": "p" * 129},
        {"change_kind": "unknown"},
        {"change_kind": "correct"},
        {"corrects_id": "old"},
        {"change_kind": "temporary_override"},
    ],
)
def test_invalid_or_unbounded_drafts_cannot_reach_publication(changes):
    with pytest.raises(ValueError):
        replace(DRAFT, **changes)


def test_implicit_observation_start_cannot_follow_the_proposed_end():
    draft = replace(DRAFT, valid_to=NOW - timedelta(seconds=1))
    assert POLICY.evaluate(EVENT, draft, AUTHORITY) == (
        "PENDING_VERIFICATION",
        ("invalid_effective_interval",),
    )
    no_session = replace(EVENT, scope=MemoryScope("tenant"))
    assert "scope_not_available" in POLICY.evaluate(no_session, DRAFT, AUTHORITY)[1]


def test_evidence_point_never_implicitly_supports_an_interval():
    point = EvidenceSupport("event", "tool_observation", support_at=NOW, recorded_at=NOW)
    assert point.support_from is None and point.support_to is None
    with pytest.raises(ValueError, match="point evidence"):
        replace(point, support_from=NOW)
    with pytest.raises(ValueError, match="point evidence"):
        replace(point, support_at=None)
    interval = EvidenceSupport(
        "event",
        "document",
        support_kind="interval",
        support_from=NOW,
        support_to=NOW + timedelta(days=1),
        recorded_at=NOW,
    )
    assert interval.support_at is None
    with pytest.raises(ValueError, match="support_to"):
        replace(interval, support_to=NOW)
    with pytest.raises(ValueError, match="timezone"):
        replace(point, recorded_at=NOW.replace(tzinfo=None))
