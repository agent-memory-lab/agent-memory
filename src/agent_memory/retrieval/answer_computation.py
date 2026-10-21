"""Bounded arithmetic over source-bound operands supplied by a reader.

This proves arithmetic, not the interpretation of a question or a quotation.
The caller must independently check operand/event/unit interpretation. No eval,
natural-language date guessing, I/O, or external facts enter this component.
"""

from datetime import date
from decimal import Context, Decimal, InvalidOperation, localcontext

KINDS = (
    "none",
    "time_order",
    "relative_time_order",
    "elapsed_days",
    "elapsed_months",
    "count",
    "difference",
    "sum",
)


def _decimal(value):
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError("invalid_numeric_operand") from None
    if (
        not number.is_finite()
        or number.copy_abs() > Decimal("1e15")
        or number.as_tuple().exponent < -12
    ):
        raise ValueError("unbounded_numeric_operand")
    return number


def _number(value):
    return format(value.normalize(), "f") if value else "0"


def compute_answer(
    operation, *, complete=False, shared_time_anchor=False, source_month_proxy_validated=False
):
    # Caller settings (including Inexact traps) cannot change bounded arithmetic.
    # 64 digits cover 32 operands of <=16 integer + 12 fractional digits.
    with localcontext(Context(prec=64)):
        return _compute_answer(
            operation,
            complete=complete,
            shared_time_anchor=shared_time_anchor,
            source_month_proxy_validated=source_month_proxy_validated,
        )


def _compute_answer(operation, *, complete, shared_time_anchor, source_month_proxy_validated):
    """Return a deterministic result; incomplete counts are explicit lower bounds.

    ``complete`` is a trusted host assertion about the exact query snapshot,
    never a field accepted from generated JSON. Top-k delivery is not complete.
    Dates are explicit inclusive uncertainty bounds; duration uses elapsed days
    and additionally states the inclusive-calendar convention.
    """
    if (
        type(complete) is not bool
        or type(operation) is not dict
        or set(operation) != {"kind", "operands", "unit"}
    ):
        raise ValueError("invalid_computation")
    kind, operands, unit = operation["kind"], operation["operands"], operation["unit"]
    if kind not in KINDS or type(operands) is not list or len(operands) > 32:
        raise ValueError("invalid_computation")
    if type(unit) is not str or len(unit.encode()) > 64:
        raise ValueError("invalid_unit")
    if (kind == "elapsed_days" and unit not in ("", "days")) or (
        kind == "elapsed_months" and unit not in ("", "months")
    ):
        raise ValueError("conflicting_temporal_unit")
    for operand in operands:
        required = {"label", "value", "end_value", "supports"}
        if (
            type(operand) is not dict
            or not required <= set(operand)
            or set(operand) - required - {"basis"}
        ):
            raise ValueError("invalid_operand")
        basis = operand.get("basis", "explicit")
        if basis not in ("explicit", "source_month_proxy"):
            raise ValueError("invalid_operand_basis")
        if basis == "source_month_proxy" and (
            kind != "elapsed_months" or source_month_proxy_validated is not True
        ):
            raise ValueError("source_month_proxy_not_grounded")
        if any(
            type(operand[k]) is not str or not 1 <= len(operand[k].encode()) <= 256
            for k in ("label", "value", "end_value")
        ):
            raise ValueError("invalid_operand")
        supports = operand["supports"]
        if (
            type(supports) is not list
            or not 1 <= len(supports) <= 12
            or any(type(index) is not int or not 0 <= index < 12 for index in supports)
            or len(supports) != len(set(supports))
        ):
            raise ValueError("invalid_operand_supports")
    if kind == "none":
        if operands or unit:
            raise ValueError("unused_computation_fields")
        return {"status": "not_required", "answer": None}
    if not operands or (
        kind
        in ("time_order", "relative_time_order", "elapsed_days", "elapsed_months", "difference")
        and len(operands) != 2
    ):
        raise ValueError("invalid_operand_count")
    if kind == "relative_time_order":
        if shared_time_anchor is not True or unit not in ("days", "weeks", "months", "years"):
            raise ValueError("shared_source_date_anchor_required")
        bounds = [(_decimal(o["value"]), _decimal(o["end_value"])) for o in operands]
        if any(lo > hi for lo, hi in bounds):
            raise ValueError("reversed_numeric_interval")
        (a, b), (c, d) = bounds
        index = 0 if b < c else 1 if d < a else None
        if index is None:
            return {
                "status": "insufficient",
                "answer": None,
                "missing": "Relative event offsets overlap.",
            }
        return {
            "status": "computed",
            "answer": "Approximately, " + operands[index]["label"] + " happened first.",
            "precision": "relative_source_report_not_exact_calendar_dates",
        }
    if kind == "count":
        if any(o["value"] != o["end_value"] for o in operands):
            raise ValueError("count_requires_entity_keys")
        count = len({o["value"] for o in operands})
        return {
            "status": "computed",
            "answer": (
                f"{count} distinct items."
                if complete
                else f"At least {count} distinct items are evidenced; the total is not established."
            ),
            "lower_bound": count,
            "coverage_complete": complete,
        }
    if kind in ("difference", "sum"):
        bounds = [(_decimal(o["value"]), _decimal(o["end_value"])) for o in operands]
        if any(lo > hi for lo, hi in bounds):
            raise ValueError("reversed_numeric_interval")
        low = sum(lo for lo, _ in bounds) if kind == "sum" else bounds[0][0] - bounds[1][1]
        high = sum(hi for _, hi in bounds) if kind == "sum" else bounds[0][1] - bounds[1][0]
        amount = _number(low) if low == high else f"{_number(low)}–{_number(high)}"
        return {"status": "computed", "answer": amount + (" " + unit if unit else "") + "."}
    bounds = []
    for operand in operands:
        try:
            # Require canonical date-only values, not local/naive timestamps.
            low, high = (date.fromisoformat(operand[k]) for k in ("value", "end_value"))
        except (ValueError, TypeError):
            raise ValueError("invalid_date_operand") from None
        if (
            any(d.isoformat() != operand[k] for d, k in ((low, "value"), (high, "end_value")))
            or low > high
        ):
            raise ValueError("invalid_date_interval")
        bounds.append((low, high))
    (a, b), (c, d) = bounds
    if kind == "time_order":
        if b < c:
            return {"status": "computed", "answer": operands[0]["label"] + " happened first."}
        if d < a:
            return {"status": "computed", "answer": operands[1]["label"] + " happened first."}
        return {
            "status": "insufficient",
            "answer": None,
            "missing": "The event date ranges overlap; an order is not established.",
        }
    if b > c:
        raise ValueError("reversed_or_overlapping_duration")
    if kind == "elapsed_months":

        def months(x):
            return x.year * 12 + x.month

        low, high = months(c) - months(b), months(d) - months(a)
        value = str(low) if low == high else f"{low}–{high}"
        if any(o.get("basis") == "source_month_proxy" for o in operands):
            return {
                "status": "computed",
                "answer": f"Approximately {value} calendar months, "
                "using the source report month as an approximate event month.",
                "precision": "source_month_proxy",
            }
        if any(lo != hi for lo, hi in bounds):
            return {
                "status": "computed",
                "answer": f"Approximately {value} calendar months "
                "(calendar-month separation from date ranges).",
                "precision": "date_interval",
            }
        return {
            "status": "computed",
            "answer": value + " calendar months.",
            "precision": "exact_dates",
        }
    low, high = (c - b).days, (d - a).days
    value = str(low) if low == high else f"{low}–{high}"
    inclusive = str(low + 1) if low == high else f"{low + 1}–{high + 1}"
    return {
        "status": "computed",
        "answer": f"{value} elapsed days ({inclusive} calendar dates including both endpoints).",
    }
