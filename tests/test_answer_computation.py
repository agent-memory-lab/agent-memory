"""Generic arithmetic invariants; no benchmark names, answers, or IDs."""

from decimal import Inexact, localcontext

import pytest

from agent_memory.retrieval.answer_computation import compute_answer


def test_decimal_results_are_independent_of_the_callers_decimal_context():
    operation = calculation("sum", operand("a", "12.3"), operand("b", "0.04"), unit="hours")
    with localcontext() as context:
        context.prec = 2
        context.traps[Inexact] = True
        assert compute_answer(operation)["answer"] == "12.34 hours."
        assert context.prec == 2 and context.traps[Inexact]


def test_sum_retains_all_supported_decimal_places_at_the_operand_limit():
    operation = calculation(
        "sum", *[operand(str(i), "999999999999999.999999999999") for i in range(32)]
    )
    assert compute_answer(operation)["answer"] == "31999999999999999.999999999968."


def test_uncertain_calendar_months_keep_their_precision_qualifier():
    operation = calculation(
        "elapsed_months",
        operand("a", "2025-03-01", "2025-03-31"),
        operand("b", "2025-08-20"),
        unit="months",
    )
    result = compute_answer(operation)
    assert result["answer"].startswith("Approximately 5 calendar months")
    assert result["precision"] == "date_interval"


@pytest.mark.parametrize("kind,unit", [("elapsed_days", "months"), ("elapsed_months", "days")])
def test_temporal_operation_cannot_ignore_a_conflicting_unit(kind, unit):
    with pytest.raises(ValueError, match="unit"):
        compute_answer(
            calculation(kind, operand("a", "2025-01-01"), operand("b", "2025-02-01"), unit=unit)
        )


def operand(label, value, end_value=None):
    return {"label": label, "value": value, "end_value": end_value or value, "supports": [0]}


def calculation(kind, *values, unit=""):
    return {"kind": kind, "operands": list(values), "unit": unit}


def test_temporal_order_uses_dates_instead_of_draft_or_retrieval_order():
    result = compute_answer(
        calculation(
            "time_order",
            operand("陶艺课", "2025-04-01", "2025-04-30"),
            operand("Cycling", "2025-03-01", "2025-03-31"),
        )
    )
    assert result["answer"] == "Cycling happened first."


def test_overlapping_time_intervals_do_not_invent_an_order():
    result = compute_answer(
        calculation(
            "time_order", operand("A", "2025-03-01", "2025-04-01"), operand("B", "2025-03-15")
        )
    )
    assert result["status"] == "insufficient"


def test_duration_is_computed_with_explicit_elapsed_day_convention():
    result = compute_answer(
        calculation("elapsed_days", operand("start", "2024-02-28"), operand("end", "2024-03-01"))
    )
    assert result["answer"] == "2 elapsed days (3 calendar dates including both endpoints)."


def test_calendar_month_difference_does_not_require_day_precision():
    result = compute_answer(
        calculation(
            "elapsed_months",
            operand("start", "2025-10-01", "2025-10-31"),
            operand("end", "2026-02-01", "2026-02-28"),
        )
    )
    assert result["answer"].startswith("Approximately 4 calendar months")
    assert result["precision"] == "date_interval"


def test_uncertain_duration_remains_a_range():
    result = compute_answer(
        calculation(
            "elapsed_days",
            operand("start", "2025-05-01", "2025-05-03"),
            operand("end", "2025-05-06"),
        )
    )
    assert result["answer"].startswith("3–5 elapsed days")


def test_open_world_count_is_a_lower_bound_and_deduplicates_entities():
    request = calculation(
        "count",
        operand("Ada", "person-1"),
        operand("Ada again", "person-1"),
        operand("Ben", "person-2"),
    )
    assert (
        compute_answer(request)["answer"]
        == "At least 2 distinct items are evidenced; the total is not established."
    )
    assert compute_answer(request, complete=True)["answer"] == "2 distinct items."


def test_decimal_subtraction_does_not_use_floating_point_or_external_values():
    result = compute_answer(
        calculation("difference", operand("quoted", "0.3"), operand("paid", "0.1"), unit="CNY")
    )
    assert result["answer"] == "0.2 CNY."


def test_numeric_bounds_and_calendar_ambiguity_are_preserved():
    assert (
        compute_answer(calculation("difference", operand("a", "10", "20"), operand("b", "2", "4")))[
            "answer"
        ]
        == "6–18."
    )
    assert (
        compute_answer(calculation("sum", operand("a", "1.2"), operand("b", "3.4"), unit="hours"))[
            "answer"
        ]
        == "4.6 hours."
    )
    assert (
        compute_answer(
            calculation(
                "elapsed_months",
                operand("a", "2025-01-01", "2025-02-28"),
                operand("b", "2025-04-01", "2025-05-31"),
            )
        )["answer"]
        == "Approximately 2–4 calendar months (calendar-month separation from date ranges)."
    )


def test_relative_order_needs_a_host_anchor_and_does_not_guess_overlapping_offsets():
    value = calculation(
        "relative_time_order", operand("A", "-2", "-1"), operand("B", "-1", "0"), unit="months"
    )
    with pytest.raises(ValueError, match="anchor"):
        compute_answer(value)
    assert compute_answer(value, shared_time_anchor=True)["status"] == "insufficient"
    result = compute_answer(
        calculation("relative_time_order", operand("A", "-3"), operand("B", "-2"), unit="months"),
        shared_time_anchor=True,
    )
    assert result["answer"] == "Approximately, A happened first."


@pytest.mark.parametrize(
    "operation",
    [
        {"kind": "none", "operands": [], "unit": "unexpected"},
        {"kind": "sum", "operands": [], "unit": ""},
        {
            "kind": "sum",
            "operands": [{"label": "x", "value": "1", "end_value": "1", "supports": [True]}],
            "unit": "",
        },
        calculation("time_order", operand("a", "20250101"), operand("b", "2025-02-01")),
        calculation(
            "elapsed_days", operand("a", "2025-02-01", "2025-01-01"), operand("b", "2025-03-01")
        ),
        calculation("count", operand("a", "a", "b")),
        calculation("sum", operand("a", "not a number")),
    ],
)
def test_bad_operation_shapes_and_unsupported_precision_are_rejected(operation):
    with pytest.raises(ValueError):
        compute_answer(operation)


@pytest.mark.parametrize(
    "operation",
    [
        calculation("elapsed_days", operand("a", "2024-03-01"), operand("b", "2024-02-28")),
        calculation("time_order", operand("a", "bad-date"), operand("b", "2024-01-01")),
        calculation("difference", operand("a", "NaN"), operand("b", "1")),
        calculation("difference", operand("a", "1e10000"), operand("b", "1")),
        calculation("sum", operand("a", "1", "0")),
        {"kind": "eval", "operands": [], "unit": ""},
    ],
)
def test_invalid_or_unbounded_operations_fail_closed(operation):
    with pytest.raises(ValueError):
        compute_answer(operation)
