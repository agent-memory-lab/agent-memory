"""Public QA projection over the existing full-cost ledger contract.

This is an engineering gate, not calibrated business acceptance. Partial public
scores, exposed regression data, unknown provider usage and unpriced local work
cannot acquire a full-quality/full-cost acceptance label.
"""

from .question_cost import CostPhase, CostSnapshot, TokenBasis


def summarize_public_qa(
    planned_case_ids,
    outcomes,
    costs,
    *,
    local_resources_measured=False,
    minimum_accuracy=0.8,
    maximum_technical_failure_rate=0.02,
):
    ids, rows = tuple(planned_case_ids), tuple(outcomes)
    if not ids or any(type(cid) is not str or not cid for cid in ids) or len(ids) != len(set(ids)):
        raise ValueError("unique_nonempty_planned_cases_required")
    if type(costs) is not CostSnapshot or type(local_resources_measured) is not bool:
        raise ValueError("typed_cost_snapshot_and_resource_status_required")
    for value in (minimum_accuracy, maximum_technical_failure_rate):
        if type(value) not in (int, float) or not 0 <= value <= 1:
            raise ValueError("bounded_threshold_required")
    planned = set(ids)
    if any(not set(item.request_ids) <= planned for item in (*costs.entries, *costs.pending)):
        raise ValueError("cost_request_outside_workload")
    seen = set()
    for row in rows:
        if type(row) is not dict or set(row) != {
            "case_id",
            "status",
            "correct",
            "answered",
            "answerable",
            "exposure",
        }:
            raise ValueError("invalid_qa_outcome")
        cid, correct = row["case_id"], row["correct"]
        if (
            cid not in ids
            or cid in seen
            or any(type(row[k]) is not bool for k in ("answered", "answerable"))
        ):
            raise ValueError("duplicate_or_unknown_case")
        seen.add(cid)
        if (correct is not None and type(correct) is not bool) or row["status"] not in {
            "completed",
            "invalid",
            "provider_rejected",
            "budget_stopped",
            "unjudged",
        }:
            raise ValueError("invalid_qa_status_or_judgment")
        if (row["status"] == "completed") != (correct is not None):
            raise ValueError("completed_requires_independent_judgment")
        if row["exposure"] not in {"development", "unknown", "holdout"}:
            raise ValueError("invalid_exposure_status")
    scored = [r for r in rows if r["correct"] is not None]
    correct = sum(r["correct"] for r in scored)
    technical = sum(r["status"] in ("invalid", "provider_rejected") for r in rows)
    technical_rate = technical / len(rows) if rows else None
    full = len(scored) == len(ids)
    accuracy = correct / len(scored) if scored else None
    effective_correct = sum(r["correct"] and r["answered"] == r["answerable"] for r in scored)
    effective_accuracy = effective_correct / len(scored) if scored else None
    gaps = []
    if not full:
        gaps.append("full_dataset_not_scored")
    if effective_accuracy is None or effective_accuracy < minimum_accuracy:
        gaps.append("quality_threshold_not_met")
    if technical_rate is not None and technical_rate > maximum_technical_failure_rate:
        gaps.append("technical_failure_threshold_exceeded")
    if not rows or any(r["exposure"] != "holdout" for r in rows):
        gaps.append("no_independent_holdout")
    total = invoiced = unknown = 0
    any_unknown_cost = False
    by_phase = {}
    for entry in costs.entries:
        value = entry.actual_microunits
        if value is not None:
            invoiced += entry.provider_billed_microunits or 0
        elif entry.model_call and entry.token_basis == TokenBasis.UNKNOWN:
            available = [
                v
                for v in (entry.reservation_microunits, entry.estimated_microunits)
                if v is not None
            ]
            value = max(available) if entry.reservation_microunits is not None else None
        elif entry.estimated_microunits is not None:
            value = entry.estimated_microunits
        else:
            value = entry.reservation_microunits
        if value is None:
            any_unknown_cost = True
        else:
            total += value
            by_phase[str(entry.phase)] = by_phase.get(str(entry.phase), 0) + value
        unknown += bool(entry.model_call and entry.token_basis == TokenBasis.UNKNOWN)
    if unknown:
        gaps.append("unresolved_model_usage")
    if any_unknown_cost:
        gaps.append("unbounded_cost")
    if costs.pending:
        gaps.append("pending_responsibilities")
        for responsibility in costs.pending:
            if responsibility.maximum_microunits is None:
                any_unknown_cost = True
            else:
                total += responsibility.maximum_microunits
                phase = str(responsibility.phase)
                by_phase[phase] = by_phase.get(phase, 0) + responsibility.maximum_microunits
        if any_unknown_cost and "unbounded_cost" not in gaps:
            gaps.append("unbounded_cost")
    if set(costs.accounted_phases) != set(CostPhase):
        gaps.append("cost_phases_incomplete")
    if not local_resources_measured:
        gaps.append("local_resource_cost_unmeasured")
    return {
        "schema": "public-qa-acceptance/2",
        "planned": len(ids),
        "started": len(rows),
        "scored": len(scored),
        "correct": correct,
        "accuracy_scored": accuracy,
        "effective_correct": effective_correct,
        "effective_accuracy_scored": effective_accuracy,
        "answerability_disagreements": sum(r["answered"] != r["answerable"] for r in scored),
        "full_benchmark_accuracy": accuracy if full else None,
        "not_started": len(ids) - len(rows),
        "unjudged": len(rows) - len(scored),
        "technical_failures": technical,
        "technical_failure_rate_started": technical_rate,
        "resolved_fraction": len(scored) / len(ids),
        "abstentions_scored": sum(not r["answered"] for r in scored),
        "incorrect_answers": sum(
            r["answered"] and (not r["correct"] or not r["answerable"]) for r in scored
        ),
        "incorrect_abstentions": sum(not r["answered"] and r["answerable"] for r in scored),
        "accepted": not gaps,
        "acceptance_gaps": gaps,
        "cost": {
            "currency": costs.currency,
            "ledger_sha256": costs.fingerprint,
            "accounted_upper_microunits": None if any_unknown_cost else total,
            "invoiced_microunits": invoiced
            if all(e.actual_microunits is not None for e in costs.entries) and costs.entries
            else None,
            "by_phase_upper_microunits": by_phase,
            "unknown_usage_calls": unknown,
            "model_calls": sum(e.model_call for e in costs.entries),
            "per_scored_case_upper_microunits": total / len(scored)
            if scored and not any_unknown_cost
            else None,
            "basis": "provider bills where available, otherwise tariff estimates or conservative "
            "reserves; local resources separate",
        },
        "thresholds": {
            "minimum_accuracy": minimum_accuracy,
            "maximum_technical_failure_rate": maximum_technical_failure_rate,
            "calibration": "engineering_defaults_not_business_calibrated",
        },
    }
