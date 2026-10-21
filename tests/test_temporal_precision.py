import json

import pytest

from examples.memory_evidence_reader_v4 import build_candidate, validate_candidate
from tests.test_memory_evidence_reader_v4 import draft


def case(
    date="2024-02-29T12:00:00+08:00",
    text="I just finished the course.",
    kind="event",
    low="2024-02-01",
    high="2024-02-29",
    roles=("user",),
):
    records = [
        {
            "kind": kind,
            "text": "user: " + text,
            "source_details": [{"occurred_at": date, "roles": list(roles)}],
        },
        {"kind": "event", "text": "user: I submitted the report on 2024-05-09."},
    ]
    value = draft(
        answer="About three months.",
        supporting_quotes=[{"span_id": "E1:S1"}, {"span_id": "E2:S1"}],
        computation={
            "kind": "elapsed_months",
            "unit": "months",
            "operands": [
                {
                    "label": "course",
                    "value": low,
                    "end_value": high,
                    "supports": [0],
                    "basis": "source_month_proxy",
                },
                {
                    "label": "report",
                    "value": "2024-05-09",
                    "end_value": "2024-05-09",
                    "supports": [1],
                },
            ],
        },
    )
    return value, records


def test_leap_month_proxy_and_policy_disable_are_explicit():
    value, records = case()
    result = validate_candidate(json.dumps(value), records)
    assert result["status"] == "pending_verification"
    assert result["answer"].startswith("Approximately 3 calendar months")
    disabled = validate_candidate(json.dumps(value), records, allow_source_month_proxy=False)
    assert disabled["errors"] == ["source_month_proxy_disabled"]
    prompt = build_candidate(
        question="Month difference?",
        question_date=None,
        records=records,
        allow_source_month_proxy=False,
    )
    policy = json.loads(prompt.split("TEMPORAL_POLICY_JSON=", 1)[1].split("\nQUESTION_JSON=", 1)[0])
    assert policy["allow_source_month_proxy"] is False


def test_anchor_uses_source_local_month_not_the_utc_month():
    value, records = case(date="2024-03-01T00:30:00+14:00", low="2024-03-01", high="2024-03-31")
    result = validate_candidate(json.dumps(value), records)
    assert result["temporal_interpretation"]["assumptions"][0]["source_month"] == "2024-03"


@pytest.mark.parametrize(
    "changes",
    [
        {"date": "2024-02-29T12:00:00"},
        {"date": "unknown"},
        {"roles": ("assistant",)},
        {"kind": "claim"},
        {"text": "She just completed a course."},
        {"text": "I just completed a course in 2021."},
        {"low": "2024-01-01", "high": "2024-01-31"},
        {"low": "2024-02-29", "high": "2024-02-29"},
    ],
)
def test_proxy_cannot_be_asserted_with_wrong_role_dates_or_missing_bounds(changes):
    value, records = case(**changes)
    assert validate_candidate(json.dumps(value), records)["errors"] == [
        "source_month_proxy_not_grounded"
    ]


def test_declared_proxy_is_not_legal_for_day_precision():
    value, records = case()
    value["computation"]["kind"] = "elapsed_days"
    value["computation"]["unit"] = "days"
    assert validate_candidate(json.dumps(value), records)["errors"] == [
        "source_month_proxy_not_grounded"
    ]


@pytest.mark.parametrize(
    "utterance",
    ["I've just finished the course.", "I’ve just finished the course.", "我才刚刚完成课程。"],
)
def test_common_recent_report_forms_share_the_same_precision_policy(utterance):
    value, records = case(text=utterance)
    assert validate_candidate(json.dumps(value), records)["status"] == "pending_verification"


def test_related_context_quotes_need_not_each_repeat_the_temporal_phrase():
    value, records = case()
    records[0]["text"] += " This was the evening course."
    value["supporting_quotes"] = [
        {"evidence_id": "E1", "text": "I just finished the course."},
        {"evidence_id": "E1", "text": "This was the evening course."},
        {"span_id": "E2:S1"},
    ]
    value["computation"]["operands"][0]["supports"] = [0, 1]
    value["computation"]["operands"][1]["supports"] = [2]
    assert validate_candidate(json.dumps(value), records)["status"] == "pending_verification"


@pytest.mark.parametrize("gap", ["", " might have "])
def test_precision_interpretation_survives_adjacent_citation_boundaries_without_joining_gaps(gap):
    value, records = case(text="I just fini" + gap + "shed the course.")
    value["supporting_quotes"] = [
        {"evidence_id": "E1", "text": "I just fini"},
        {"evidence_id": "E1", "text": "shed the course."},
        {"span_id": "E2:S1"},
    ]
    value["computation"]["operands"][0]["supports"] = [0, 1]
    value["computation"]["operands"][1]["supports"] = [2]
    result = validate_candidate(json.dumps(value), records)
    assert result["status"] == ("pending_verification" if not gap else "invalid")
