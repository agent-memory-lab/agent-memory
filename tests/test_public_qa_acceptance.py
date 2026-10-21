import pytest

from agent_memory.evaluation.public_qa_acceptance import summarize_public_qa
from agent_memory.evaluation.question_cost import (
    CostEntry,
    CostPhase,
    CostSnapshot,
    PendingResponsibility,
)


def outcome(case_id="a", correct=True, **updates):
    return {
        "case_id": case_id,
        "status": "completed",
        "correct": correct,
        "answered": True,
        "answerable": True,
        "exposure": "development",
        **updates,
    }


def costs(*entries, phases=tuple(CostPhase)):
    return CostSnapshot("CNY", entries, (), phases)


def test_partial_scores_never_become_full_benchmark_or_release_acceptance():
    report = summarize_public_qa(
        ("a", "b", "c"), [outcome()], costs(), local_resources_measured=False
    )
    assert report["accuracy_scored"] == 1
    assert report["full_benchmark_accuracy"] is None
    assert report["not_started"] == 2 and report["accepted"] is False


def test_failures_and_missing_judges_remain_in_planned_denominator():
    report = summarize_public_qa(
        ("a", "b"), [outcome(), outcome("b", None, status="invalid")], costs()
    )
    assert report["correct"] == 1 and report["technical_failures"] == 1
    assert report["resolved_fraction"] == 0.5 and report["full_benchmark_accuracy"] is None


def test_unknown_model_usage_cost_and_pending_work_block_full_cost_acceptance():
    entry = CostEntry(
        "failed", CostPhase.FAILURE, model_call=True, provider="test", reservation_microunits=40000
    )
    report = summarize_public_qa(("a",), [outcome()], costs(entry), local_resources_measured=True)
    assert report["cost"]["unknown_usage_calls"] == 1
    assert report["cost"]["accounted_upper_microunits"] == 40000
    assert "unresolved_model_usage" in report["acceptance_gaps"]


def test_actual_tariff_estimates_are_not_invoices_and_costs_include_failures():
    entries = [
        CostEntry(
            str(i),
            phase,
            estimated_microunits=100,
            tokens=10,
            token_basis="measured",
            model_call=True,
            provider="test",
        )
        for i, phase in enumerate((CostPhase.WRITE, CostPhase.FOREGROUND, CostPhase.FAILURE))
    ]
    report = summarize_public_qa(
        ("a",), [outcome()], costs(*entries), local_resources_measured=True
    )
    assert report["cost"]["accounted_upper_microunits"] == 300
    assert report["cost"]["invoiced_microunits"] is None
    assert report["cost"]["per_scored_case_upper_microunits"] == 300


def test_exposed_data_and_unmeasured_resources_are_explicit_gaps():
    report = summarize_public_qa(("a",), [outcome()], costs(), local_resources_measured=False)
    assert {"no_independent_holdout", "local_resource_cost_unmeasured"} <= set(
        report["acceptance_gaps"]
    )


def test_rubric_credit_for_answer_text_inside_a_refusal_does_not_pass_reliability():
    report = summarize_public_qa(
        ("a",), [outcome(correct=True, answered=False, answerable=True)], costs()
    )
    assert report["accuracy_scored"] == 1
    assert report["effective_accuracy_scored"] == 0
    assert report["answerability_disagreements"] == 1
    assert "quality_threshold_not_met" in report["acceptance_gaps"]


def test_pending_responsibilities_are_included_in_the_cost_bound():
    snapshot = CostSnapshot(
        "CNY", (), (PendingResponsibility("refresh", ("a",), 25),), tuple(CostPhase)
    )
    report = summarize_public_qa(("a",), [outcome()], snapshot)
    assert report["cost"]["accounted_upper_microunits"] == 25
    assert "pending_responsibilities" in report["acceptance_gaps"]
    unknown = CostSnapshot(
        "CNY", (), (PendingResponsibility("refresh", ("a",), None),), tuple(CostPhase)
    )
    report = summarize_public_qa(("a",), [outcome()], unknown)
    assert report["cost"]["accounted_upper_microunits"] is None
    assert "unbounded_cost" in report["acceptance_gaps"]


def test_technical_failure_rate_uses_started_cases_not_unstarted_plan_rows():
    ids = tuple("case-" + str(i) for i in range(500))
    report = summarize_public_qa(ids, [outcome(ids[0], None, status="invalid")], costs())
    assert report["technical_failure_rate_started"] == 1
    assert "technical_failure_threshold_exceeded" in report["acceptance_gaps"]
    assert report["full_benchmark_accuracy"] is None


def test_rubric_yes_does_not_hide_an_incorrect_abstention_in_diagnostics():
    report = summarize_public_qa(
        ("a",), [outcome(answered=False, answerable=True, correct=True)], costs()
    )
    assert report["incorrect_abstentions"] == 1


def test_unknown_usage_cannot_release_a_larger_reservation_via_a_smaller_estimate():
    entry = CostEntry(
        "unknown",
        CostPhase.FAILURE,
        model_call=True,
        provider="test",
        estimated_microunits=10,
        reservation_microunits=100,
    )
    report = summarize_public_qa(("a",), [outcome()], costs(entry))
    assert report["cost"]["accounted_upper_microunits"] == 100


def test_phase_totals_include_pending_responsibilities():
    snapshot = CostSnapshot(
        "CNY",
        (),
        (PendingResponsibility("refresh", ("a",), 25, CostPhase.BACKGROUND),),
        tuple(CostPhase),
    )
    report = summarize_public_qa(("a",), [outcome()], snapshot)
    assert report["cost"]["by_phase_upper_microunits"] == {"background": 25}


def test_cost_attribution_cannot_silently_reference_another_workload():
    entry = CostEntry(
        "foreign", CostPhase.FOREGROUND, request_ids=("another-scope",), estimated_microunits=1
    )
    with pytest.raises(ValueError, match="cost_request_outside_workload"):
        summarize_public_qa(("a",), [outcome()], costs(entry))


@pytest.mark.parametrize(
    "rows",
    [
        [outcome(), outcome()],
        [outcome("unknown")],
        [outcome(correct=1)],
        [outcome(status="invalid", correct=True)],
    ],
)
def test_ambiguous_or_forged_evaluation_records_are_rejected(rows):
    with pytest.raises(ValueError):
        summarize_public_qa(("a",), rows, costs())
