import asyncio
import json

import pytest

from examples.memory_evidence_reader_v4 import (
    build_candidate,
    read_answer,
    span_evidence,
    validate_candidate,
    verification_prompt,
)

RECORDS = [{"text": "user: I attended the fair at Birch College."}]


def draft(**changes):
    return {
        "schema_version": "evidence-reader/4",
        "mode": "fact",
        "answer": "Birch College.",
        "supporting_quotes": [
            {"evidence_id": "E1", "text": "I attended the fair at Birch College."}
        ],
        "missing_information": [],
        "computation": {"kind": "none", "operands": [], "unit": ""},
        **changes,
    }


def test_exact_quote_is_not_itself_semantic_verification():
    result = validate_candidate(json.dumps(draft()), RECORDS)
    assert result["status"] == "pending_verification"
    assert result["traceability_verified"] is True
    assert result["entailment_verified"] is False


def test_long_quoted_paragraph_is_not_silently_trimmed():
    result = validate_candidate(
        json.dumps(draft(supporting_quotes=[{"evidence_id": "E1", "text": "x" * 257}])),
        [{"text": "x" * 300}],
    )
    assert result["status"] == "invalid" and result["errors"] == ["quote_byte_budget_exceeded"]


@pytest.mark.parametrize(
    "change",
    [
        {"answerable": True},
        {"mode": "fact", "missing_information": ["date"]},
        {"supporting_quotes": [{"evidence_id": "E2", "text": "Birch College"}]},
        {
            "computation": {
                "kind": "count",
                "operands": [{"label": "a", "value": "a", "end_value": "a", "supports": [9]}],
                "unit": "",
            }
        },
    ],
)
def test_bad_contracts_cannot_be_rendered_as_normal_answers(change):
    assert validate_candidate(json.dumps(draft(**change)), RECORDS)["status"] == "invalid"


def test_verification_is_bound_to_question_evidence_plan_and_computed_answer():
    prompt = verification_prompt(
        question="Where did I present a poster?",
        question_date=None,
        records=RECORDS,
        candidate=validate_candidate(json.dumps(draft()), RECORDS),
    )
    assert "attending is not presenting" in prompt
    assert "assistant suggestions" in prompt
    assert "Where did I present a poster?" in prompt


def test_host_coverage_cannot_be_asserted_by_the_model():
    candidate = draft(
        computation={
            "kind": "count",
            "operands": [
                {"label": "A", "value": "entity-a", "end_value": "entity-a", "supports": [0]}
            ],
            "unit": "",
        }
    )
    result = validate_candidate(json.dumps(candidate), RECORDS)
    assert result["answer"].startswith("At least 1")
    assert result["coverage_complete"] is False


def test_recommendations_are_separate_from_personal_factual_claims():
    prompt = build_candidate(question="Suggest an outing", question_date=None, records=RECORDS)
    assert "recommendation" in prompt and "not a remembered user action" in prompt


class Client:
    def __init__(self, decision="unsupported", raw=None):
        self.calls = []
        self.decision = decision
        self.raw = raw or json.dumps(draft())

    async def chat(self, prompt, phase, *, format, limit):
        self.calls.append(phase)
        return (
            self.raw
            if phase.endswith(":draft")
            else json.dumps(
                {"decision": self.decision, "reason": "The requested relationship is absent."}
            )
        )


def test_semantic_rejection_returns_abstention_and_retains_original_draft():
    client = Client()
    result = asyncio.run(
        read_answer(
            client,
            question="Where did I present a poster?",
            question_date=None,
            records=RECORDS,
            phase="answer:test",
        )
    )
    assert result["status"] == "unsupported" and result["answer"].startswith("I don't know")
    assert result["candidate"]["raw_response"] == client.raw
    assert len(client.calls) == 2 and result["entailment_verified"] is False
    assert result["answer"] == "I don't know."
    assert "requested relationship" in result["diagnostic_reason"]


def test_span_references_preserve_all_unicode_content_and_resolve_exact_offsets():
    text = ("用户：我刚刚完成了一个项目。🙂\nuser: A repeated sentence. " * 30) + "tail"
    records = [{"text": text}]
    encoded, catalog = span_evidence(records)
    blocks = json.loads(encoded)
    assert "".join(s["text"] for s in blocks[0]["spans"]) == text
    for sid in catalog:
        result = validate_candidate(
            json.dumps(draft(supporting_quotes=[{"span_id": sid}])), records
        )
        assert result["status"] == "pending_verification"
        bound = result["supporting_quotes"][0]
        start = bound["character_start"]
        assert text[start : start + len(bound["text"])] == bound["text"]


def test_unknown_span_is_never_approximated_or_looked_up_in_other_sources():
    result = validate_candidate(
        json.dumps(draft(supporting_quotes=[{"span_id": "E9:S9"}])), RECORDS
    )
    assert result["errors"] == ["unknown_quote_span"]


def test_identical_text_at_distinct_addressed_positions_is_not_a_duplicate_citation():
    records = [{"text": "user: The workshop ended.\n" * 80}]
    _, catalog = span_evidence(records)
    seen = {}
    for sid, span in catalog.items():
        if span["text"] in seen:
            pair = [seen[span["text"]], sid]
            break
        seen[span["text"]] = sid
    else:
        pytest.fail("fixture must contain repeated addressable spans")
    result = validate_candidate(
        json.dumps(draft(supporting_quotes=[{"span_id": sid} for sid in pair])), records
    )
    assert result["status"] == "pending_verification"
    assert len({q["character_start"] for q in result["supporting_quotes"]}) == 2
    prompt = verification_prompt(
        question="What happened?", question_date=None, records=records, candidate=result
    )
    audit = json.loads(prompt.split("AUDIT_JSON=", 1)[1].split("\nEVIDENCE_JSON=", 1)[0])
    assert len({q["character_start"] for q in audit["candidate"]["supporting_quotes"]}) == 2


@pytest.mark.parametrize("utterance", ["I just completed my training.", "我刚刚完成课程。"])
def test_source_month_proxy_is_visible_to_user_and_support_auditor(utterance):
    records = [
        {
            "kind": "event",
            "text": "user: " + utterance,
            "source_details": [{"occurred_at": "2025-03-18T12:00:00+08:00"}],
        },
        {"kind": "event", "text": "user: I submitted the report on 2025-08-20."},
    ]
    value = draft(
        answer="About five months, using the report month as a proxy.",
        supporting_quotes=[{"span_id": "E1:S1"}, {"span_id": "E2:S1"}],
        computation={
            "kind": "elapsed_months",
            "unit": "months",
            "operands": [
                {
                    "label": "training",
                    "value": "2025-03-01",
                    "end_value": "2025-03-31",
                    "supports": [0],
                },
                {
                    "label": "report",
                    "value": "2025-08-20",
                    "end_value": "2025-08-20",
                    "supports": [1],
                },
            ],
        },
    )
    result = validate_candidate(json.dumps(value), records)
    assert result["status"] == "pending_verification"
    assert result["computation"]["precision"] == "source_month_proxy"
    assert result["answer"].startswith("Approximately 5 calendar months")
    assert "report month" in result["answer"]
    audit = verification_prompt(
        question="How many months between the events?",
        question_date=None,
        records=records,
        candidate=result,
    )
    payload = json.loads(audit.split("AUDIT_JSON=", 1)[1].split("\nEVIDENCE_JSON=", 1)[0])
    assert payload["temporal_interpretation"]["assumptions"][0]["source_month"] == "2025-03"
    assert payload["answer_to_deliver"] == result["answer"]


def test_model_cannot_claim_a_source_month_proxy_without_a_bound_anchor():
    value = draft(
        computation={
            "kind": "elapsed_months",
            "unit": "months",
            "operands": [
                {
                    "label": "a",
                    "value": "2025-03-01",
                    "end_value": "2025-03-31",
                    "supports": [0],
                    "basis": "source_month_proxy",
                },
                {"label": "b", "value": "2025-08-20", "end_value": "2025-08-20", "supports": [0]},
            ],
        }
    )
    result = validate_candidate(json.dumps(value), RECORDS)
    assert result["status"] == "invalid" and result["errors"] == ["source_month_proxy_not_grounded"]


def test_relative_offsets_require_shared_host_source_date_and_render_approximation():
    records = [
        {
            "text": "user: I started A about a month ago.",
            "source_details": [{"occurred_at": "2025-06-10T01:00:00+00:00"}],
        },
        {
            "text": "user: I started B about two months ago.",
            "source_details": [{"occurred_at": "2025-06-10T19:00:00+00:00"}],
        },
    ]
    value = draft(
        answer="A came first.",
        supporting_quotes=[{"span_id": "E1:S1"}, {"span_id": "E2:S1"}],
        computation={
            "kind": "relative_time_order",
            "unit": "months",
            "operands": [
                {"label": "A", "value": "-1", "end_value": "-1", "supports": [0]},
                {"label": "B", "value": "-2", "end_value": "-2", "supports": [1]},
            ],
        },
    )
    result = validate_candidate(json.dumps(value), records)
    assert result["answer"] == "Approximately, B happened first."
    records[1]["source_details"][0]["occurred_at"] = "2025-07-10T19:00:00+00:00"
    assert validate_candidate(json.dumps(value), records)["errors"] == [
        "shared_source_date_anchor_required"
    ]
    records[0]["source_details"][0]["occurred_at"] = "2025-06-10T23:00:00-02:00"
    records[1]["source_details"][0]["occurred_at"] = "2025-06-11T02:00:00+00:00"
    assert validate_candidate(json.dumps(value), records)["errors"] == [
        "shared_source_date_anchor_required"
    ]


def test_invalid_draft_has_no_paid_verification_or_automatic_retry():
    client = Client(raw="invalid")
    result = asyncio.run(
        read_answer(
            client, question="Where?", question_date=None, records=RECORDS, phase="answer:test"
        )
    )
    assert result["status"] == "invalid" and client.calls == ["answer:test:draft"]


def test_malformed_verifier_output_is_a_technical_failure_not_abstention():
    client = Client(decision="maybe")
    result = asyncio.run(
        read_answer(
            client, question="Where?", question_date=None, records=RECORDS, phase="answer:test"
        )
    )
    assert result["status"] == "invalid" and result["errors"] == ["invalid_verifier_response"]


def test_model_checked_answer_does_not_claim_authoritative_fact_verification():
    result = asyncio.run(
        read_answer(
            Client(decision="supported"),
            question="Where was the fair?",
            question_date=None,
            records=RECORDS,
            phase="answer:test",
        )
    )
    assert result["status"] == "supported" and result["support_check"] == "model_checked"
    assert result["entailment_verified"] is False


@pytest.mark.parametrize(
    "raw", ['{"x":NaN}', '{"mode":"fact","mode":"abstain"}', "[]", "x" * 32769, None, "\ud800"]
)
def test_unparseable_or_ambiguous_completions_cannot_produce_answers(raw):
    result = validate_candidate(raw, RECORDS)
    assert result["status"] == "invalid" and result["answer"] is None


@pytest.mark.parametrize(
    "changes",
    [
        {"mode": "unsupported"},
        {"answer": "x" * 1025},
        {"missing_information": "a date"},
        {"missing_information": [""]},
        {"supporting_quotes": {}},
        {"supporting_quotes": []},
        {"supporting_quotes": [{"evidence_id": "E1", "text": "Birch College."}] * 2},
        {"supporting_quotes": [{"text": "Birch College."}]},
        {"mode": "abstain", "answer": "", "supporting_quotes": [], "missing_information": []},
        {
            "mode": "recommendation",
            "computation": {
                "kind": "count",
                "operands": [{"label": "a", "value": "a", "end_value": "a", "supports": [0]}],
                "unit": "",
            },
        },
    ],
)
def test_malformed_support_and_mode_contracts_are_technical_errors(changes):
    result = validate_candidate(json.dumps(draft(**changes)), RECORDS)
    assert result["status"] == "invalid"


def test_verification_audits_only_the_computed_answer_and_resolved_citations():
    records = [{"text": "user: I paid 2 and then 3."}]
    value = draft(
        answer="A wrong draft total.",
        supporting_quotes=[{"span_id": "E1:S1"}],
        computation={
            "kind": "sum",
            "unit": "CNY",
            "operands": [
                {"label": "first", "value": "2", "end_value": "2", "supports": [0]},
                {"label": "second", "value": "3", "end_value": "3", "supports": [0]},
            ],
        },
    )
    candidate = validate_candidate(json.dumps(value), records)
    prompt = verification_prompt(
        question="Total?", question_date=None, records=records, candidate=candidate
    )
    assert "A wrong draft total." not in prompt and "5 CNY." in prompt
    assert "I paid 2 and then 3." in prompt
