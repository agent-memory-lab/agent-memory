"""Synthetic fixtures test V7 accounting/gate mechanics, not production savings."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from agent_memory.evaluation.question_cost import (
    AnswerOutcome,
    CostEntry,
    CostPhase,
    CostSnapshot,
    ModelEvidence,
    PairedComparisonEvidence,
    PendingResponsibility,
    QuestionAcceptanceProfile,
    QuestionCostLedger,
    QuestionCostRun,
    QuestionQualityThresholds,
    RequestResult,
    TokenBasis,
    WorkloadManifest,
    evaluate_question_acceptance,
    request_allocations,
    summarize_costs,
)

H = "a" * 64
OTHER = "b" * 64
ALL_PHASES = tuple(CostPhase)


def request(identity="r1", **kwargs):
    return replace(
        RequestResult(
            identity, identity, True, AnswerOutcome.CORRECT_ANSWER, True, True, True, True, 10
        ),
        **kwargs,
    )


def entry(identity="call1", phase=CostPhase.FOREGROUND, cost=100, **kwargs):
    return replace(
        CostEntry(
            identity,
            phase,
            model_call=True,
            provider="fixture-provider",
            provider_billed_microunits=cost,
            billing_reference="fixture-receipt" if cost is not None else None,
            tokens=12,
            token_basis=TokenBasis.MEASURED,
            model_role="generation_model",
            model_configuration_sha256=H,
        ),
        **kwargs,
    )


def workload(ids=("r1",)):
    return WorkloadManifest(ids, H, H, H, H, H, H, H, H)


def run(cost=100, requests=None, **kwargs):
    requests = (request(),) if requests is None else requests
    # "licensed_real" here exercises validation only; no real data/model is run.
    return replace(
        QuestionCostRun(
            "fixture-run",
            "on_demand_full",
            H,
            H,
            "licensed_real",
            "fixture-license",
            H,
            ModelEvidence(H, "not_used"),
            ModelEvidence(H, "real"),
            workload(tuple(r.request_id for r in requests)),
            requests,
            CostSnapshot("USD", (entry(cost=cost),), (), ALL_PHASES),
        ),
        **kwargs,
    )


def profile(baseline, candidate, **kwargs):
    thresholds = QuestionQualityThresholds(0.5, 0.5, 0.5, 0.1, 0.1, 100, 1_000, 2_000)
    return replace(
        QuestionAcceptanceProfile(
            "fixture-profile",
            "1",
            1,
            1,
            0.95,
            H,
            "fixture-license",
            candidate.workload.fingerprint,
            candidate.configuration_sha256,
            baseline.fingerprint,
            H,
            baseline.semantic_model,
            baseline.generation_model,
            thresholds,
            "fixture-calibration",
            H,
        ),
        **kwargs,
    )


def comparison(baseline, candidate, **kwargs):
    return replace(
        PairedComparisonEvidence(
            candidate.fingerprint,
            baseline.fingerprint,
            H,
            "fixture-confidence-interval",
            1,
            0.95,
            0,
            0,
            -110,
            -90,
        ),
        **kwargs,
    )


def gate(candidate=None, baseline=None, **kwargs):
    baseline = run(cost=200) if baseline is None else baseline
    candidate = run(cost=100) if candidate is None else candidate
    frozen = profile(baseline, candidate)
    return evaluate_question_acceptance(
        frozen,
        candidate,
        baseline,
        expected_profile_sha256=frozen.fingerprint,
        comparison=comparison(baseline, candidate),
        **kwargs,
    )


def test_fully_bound_fixture_exercises_positive_gate_without_claiming_real_results():
    result = gate()
    assert result.ready
    assert result.reasons == ()
    assert result.candidate.cost_per_request_microunits == 100
    assert result.candidate.cost_per_effective_answer_microunits == 100


def test_all_phases_including_failed_retries_and_drain_are_charged_once():
    ledger = QuestionCostLedger("USD")
    for phase in CostPhase:
        assert ledger.record(entry(phase.value, phase))
        assert not ledger.record(entry(phase.value, phase))
    snapshot = ledger.snapshot(accounted_phases=ALL_PHASES)
    summary = summarize_costs(run(costs=snapshot))
    assert summary.model_calls == 10
    assert summary.total_actual_microunits == 1_000
    assert summary.cost_per_effective_answer_microunits == 1_000
    assert summary.measured_tokens == 120


def test_conflicting_operation_replay_is_rejected():
    ledger = QuestionCostLedger("USD")
    ledger.record(entry())
    with pytest.raises(ValueError, match="conflicting operation"):
        ledger.record(entry(cost=101))


def test_dispatch_without_receipt_preserves_debt_and_retry_has_own_cost():
    ledger = QuestionCostLedger("USD")
    ledger.record(
        entry(
            cost=None,
            reservation_microunits=500,
            estimated_microunits=150,
            tokens=None,
            token_basis=TokenBasis.UNKNOWN,
        )
    )
    ledger.record(entry("retry", CostPhase.RETRY, cost=180))
    before = ledger.snapshot(accounted_phases=ALL_PHASES)
    summary = summarize_costs(run(costs=before))
    assert summary.provider_billed_microunits == 180
    assert summary.estimated_microunits == 150
    assert summary.outstanding_upper_bound_microunits == 500
    assert summary.total_actual_microunits is None
    assert summary.cost_per_request_microunits is None
    assert summary.model_calls == 2
    assert summary.unknown_token_calls == 1
    assert ledger.settle(
        "call1",
        provider_request_id="provider-call1",
        billed_microunits=200,
        billing_reference="bill-1",
    )
    assert not ledger.settle(
        "call1",
        provider_request_id="provider-call1",
        billed_microunits=200,
        billing_reference="bill-1",
    )
    after = summarize_costs(run(costs=ledger.snapshot(accounted_phases=ALL_PHASES)))
    assert after.total_actual_microunits == 380
    assert after.outstanding_upper_bound_microunits == 0
    # Previous immutable observation remains unresolved after reconciliation.
    assert summarize_costs(run(costs=before)).total_actual_microunits is None
    with pytest.raises(ValueError, match="conflicting settlement"):
        ledger.settle(
            "call1",
            provider_request_id="provider-call1",
            billed_microunits=201,
            billing_reference="bill-1",
        )


def test_provider_receipt_cannot_be_double_counted_across_attempts():
    ledger = QuestionCostLedger("USD")
    ledger.record(entry(provider_request_id="same"))
    with pytest.raises(ValueError, match="multiple attempts"):
        ledger.record(entry("other", provider_request_id="same"))
    ledger.record(entry("other", cost=None))
    with pytest.raises(ValueError, match="multiple attempts"):
        ledger.settle(
            "other", provider_request_id="same", billed_microunits=100, billing_reference="bill-1"
        )
    assert len(ledger.snapshot(accounted_phases=ALL_PHASES).entries) == 2


def test_pending_finite_work_is_not_free_and_drain_is_in_global_total():
    pending = PendingResponsibility("background-debt", ("r1",), 500)
    costs = CostSnapshot("USD", (entry(),), (pending,), ALL_PHASES)
    summary = summarize_costs(run(costs=costs))
    assert summary.pending_responsibilities == 1
    assert summary.outstanding_upper_bound_microunits == 500
    assert summary.total_actual_microunits is None
    assert "candidate_unfinished_responsibilities" in gate(run(costs=costs)).reasons
    drained = replace(costs, pending=(), entries=(*costs.entries, entry("drain", CostPhase.DRAIN)))
    assert summarize_costs(run(costs=drained)).total_actual_microunits == 200


@pytest.mark.parametrize("debt", [None, 500])
def test_unknown_cost_has_no_zero_total_even_with_a_finite_bound(debt):
    unresolved = entry(cost=None, reservation_microunits=debt)
    summary = summarize_costs(run(costs=CostSnapshot("USD", (unresolved,), (), ALL_PHASES)))
    assert summary.total_actual_microunits is None
    assert summary.outstanding_upper_bound_microunits == debt
    assert summary.unresolved_cost_entries == 1


def test_unaccounted_phase_is_unknown_not_an_empty_free_phase():
    costs = CostSnapshot("USD", (), (), ())
    summary = summarize_costs(run(costs=costs))
    assert summary.total_actual_microunits is None
    assert summary.outstanding_upper_bound_microunits is None
    assert request_allocations(run(costs=costs))["r1"]["total_microunits"] is None
    assert set(summary.unaccounted_phases) == set(CostPhase)
    assert "candidate_unaccounted_phases" in gate(run(costs=costs)).reasons


def test_resource_usage_cannot_implicitly_become_money_or_provider_bills():
    resource = CostEntry("cpu", CostPhase.BACKGROUND, cpu_ms=25, io_bytes=400)
    summary = summarize_costs(run(costs=CostSnapshot("USD", (resource,), (), ALL_PHASES)))
    assert summary.total_actual_microunits is None
    priced = replace(resource, priced_resource_microunits=10, tariff_reference="frozen-tariff")
    summary = summarize_costs(run(costs=CostSnapshot("USD", (priced,), (), ALL_PHASES)))
    assert summary.priced_resource_microunits == 10
    assert summary.provider_billed_microunits == 0
    assert summary.total_actual_microunits == 10
    with pytest.raises(ValueError, match="tariff"):
        replace(resource, priced_resource_microunits=10)
    with pytest.raises(ValueError, match="model bill"):
        replace(entry(cost=None), priced_resource_microunits=10, tariff_reference="tariff")


def test_zero_bill_requires_explicit_receipt_and_is_not_an_unknown_estimate():
    summary = summarize_costs(run(cost=0))
    assert summary.total_actual_microunits == 0
    with pytest.raises(ValueError, match="billing reference"):
        entry(cost=0, billing_reference=None)
    with pytest.raises(ValueError, match="zero reservation"):
        entry(cost=None, reservation_microunits=0)
    with pytest.raises(ValueError, match="zero debt"):
        PendingResponsibility("pending", (), 0)


def test_all_requests_and_effective_answers_are_distinct_denominators():
    requests = (
        request("r1"),
        request("r2", outcome=AnswerOutcome.INCORRECT_REFUSAL),
        request("r3", outcome=AnswerOutcome.REASONABLE_UNKNOWN, answerable=False),
        request("diagnostic", diagnostic_only=True),
    )
    summary = summarize_costs(run(cost=400, requests=requests))
    assert summary.all_requests == 4
    assert summary.effective_answers == 1
    assert summary.cost_per_request_microunits == 100
    assert summary.cost_per_effective_answer_microunits == 400


def test_all_refusal_and_empty_workloads_do_not_manufacture_cheap_success():
    rejected = run(requests=(request(outcome=AnswerOutcome.INCORRECT_REFUSAL),))
    assert summarize_costs(rejected).cost_per_effective_answer_microunits is None
    assert "candidate_no_effective_answers" in gate(rejected).reasons
    empty = run(requests=())
    summary = summarize_costs(empty)
    assert summary.total_actual_microunits == 100
    assert summary.cost_per_request_microunits is None
    assert not gate(empty, empty).ready
    assert request_allocations(empty) == {}


@pytest.mark.parametrize("field", ["safe", "fresh", "evidence_complete", "qualifiers_preserved"])
def test_known_ineligible_answer_does_not_enter_effective_denominator(field):
    candidate = run(requests=(request(**{field: False}),))
    assert summarize_costs(candidate).effective_answers == 0
    assert not gate(candidate).ready


def test_shared_async_and_setup_costs_are_attributed_once_using_frozen_rule():
    requests = (request("r1"), request("r2"), request("r3"))
    costs = CostSnapshot(
        "USD",
        (
            entry("shared", CostPhase.BACKGROUND, 300, request_ids=("r1", "r2")),
            entry("setup", CostPhase.COLD_START, 300),
            entry("own", CostPhase.FOREGROUND, 100, request_ids=("r3",)),
        ),
        (),
        ALL_PHASES,
    )
    result = run(requests=requests, costs=costs)
    rows = request_allocations(result)
    assert [row["total_microunits"] for row in rows.values()] == [250, 250, 200]
    assert sum(row["known_microunits"] for row in rows.values()) == 700
    assert summarize_costs(result).total_actual_microunits == 700
    pending = PendingResponsibility("not-free", ("r1",), None)
    rows = request_allocations(replace(result, costs=replace(costs, pending=(pending,))))
    assert rows["r1"]["total_microunits"] is None
    assert rows["r2"]["total_microunits"] == 250


@pytest.mark.parametrize(
    "field",
    [
        "event_stream_sha256",
        "initial_snapshot_sha256",
        "distribution_sha256",
        "semantics_sha256",
        "security_sha256",
        "budget_sha256",
        "hardware_sha256",
        "drain_policy_sha256",
    ],
)
def test_each_frozen_workload_dimension_must_match(field):
    candidate = run(workload=replace(workload(), **{field: OTHER}))
    assert "workload_incomparable" in gate(candidate).reasons


def test_missing_request_or_foreign_async_attribution_cannot_shrink_denominator():
    with pytest.raises(ValueError, match="every workload request"):
        run(workload=workload(("r1", "omitted")))
    with pytest.raises(ValueError, match="outside the workload"):
        run(costs=CostSnapshot("USD", (entry(request_ids=("foreign",)),), (), ALL_PHASES))


def test_post_hoc_threshold_or_judge_changes_cannot_reuse_frozen_profile():
    baseline, candidate = run(cost=200), run()
    frozen = profile(baseline, candidate)
    changed = replace(frozen, thresholds=replace(frozen.thresholds, maximum_error_rate=1))
    result = evaluate_question_acceptance(
        changed,
        candidate,
        baseline,
        expected_profile_sha256=frozen.fingerprint,
        comparison=comparison(baseline, candidate),
    )
    assert "profile_changed_after_freeze" in result.reasons
    assert "candidate_judge_mismatch" in gate(replace(candidate, judge_sha256=OTHER)).reasons


@pytest.mark.parametrize(
    "field",
    [
        "dataset_sha256",
        "dataset_license_reference",
        "workload_sha256",
        "candidate_configuration_sha256",
        "baseline_report_sha256",
        "judge_sha256",
        "semantic_model",
        "generation_model",
        "thresholds",
        "calibration_reference",
        "statistics_protocol_sha256",
        "minimum_requests",
        "minimum_groups",
        "confidence_level",
    ],
)
def test_every_calibration_gap_blocks_acceptance(field):
    baseline, candidate = run(cost=200), run()
    frozen = profile(baseline, candidate, **{field: None})
    result = evaluate_question_acceptance(
        frozen,
        candidate,
        baseline,
        expected_profile_sha256=frozen.fingerprint,
        comparison=comparison(baseline, candidate),
    )
    assert not result.ready
    assert f"missing_{field}" in result.reasons


def test_stub_and_synthetic_results_do_not_become_production_evidence():
    candidate = run(dataset_kind="synthetic", generation_model=ModelEvidence(H, "stub"))
    result = gate(candidate)
    assert "candidate_synthetic_data_only" in result.reasons
    assert "candidate_generation_model_unverified" in result.reasons
    assert not result.ready


@pytest.mark.parametrize("token_count", [0, 20])
def test_estimated_tokens_even_zero_are_not_provider_measurements(token_count):
    costs = CostSnapshot(
        "USD", (entry(tokens=token_count, token_basis=TokenBasis.ESTIMATED),), (), ALL_PHASES
    )
    summary = summarize_costs(run(costs=costs))
    assert summary.estimated_tokens == token_count
    assert summary.estimated_token_calls == 1
    assert "candidate_unmeasured_model_tokens" in gate(run(costs=costs)).reasons


def test_safe_high_score_cannot_replace_required_confidence_evidence():
    baseline, candidate = run(cost=200), run()
    frozen = profile(baseline, candidate)
    result = evaluate_question_acceptance(
        frozen, candidate, baseline, expected_profile_sha256=frozen.fingerprint
    )
    assert "missing_paired_confidence_intervals" in result.reasons


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"cost_difference_upper_microunits": 0}, "cost_benefit_not_established"),
        ({"effective_rate_difference_lower": -0.2}, "quality_nonregression_not_established"),
        ({"group_count": 2}, "comparison_group_count_mismatch"),
        ({"candidate_report_sha256": OTHER}, "comparison_report_mismatch"),
        ({"statistics_protocol_sha256": OTHER}, "statistics_protocol_mismatch"),
        ({"confidence_level": 0.90}, "comparison_confidence_level_mismatch"),
    ],
)
def test_confidence_artifact_is_bound_to_reports_groups_and_protocol(change, reason):
    baseline, candidate = run(cost=200), run()
    frozen = profile(baseline, candidate)
    result = evaluate_question_acceptance(
        frozen,
        candidate,
        baseline,
        expected_profile_sha256=frozen.fingerprint,
        comparison=comparison(baseline, candidate, **change),
    )
    assert reason in result.reasons


def test_baseline_debt_cannot_be_hidden_to_manufacture_savings():
    baseline = run(cost=None)
    result = gate(baseline=baseline)
    assert "baseline_unresolved_cost" in result.reasons
    assert not result.ready


def test_more_expensive_result_cannot_pass_with_contradictory_ci():
    result = gate(candidate=run(cost=300))
    assert "no_observed_cost_benefit" in result.reasons


@pytest.mark.parametrize("bad", [True, -1, float("nan"), float("inf"), 1.5])
def test_money_counters_reject_noninteger_or_invalid_values(bad):
    with pytest.raises(ValueError):
        entry(cost=bad)


def test_freeze_owns_sequences_instead_of_retaining_mutable_input_lists():
    entries, phases = [entry()], list(ALL_PHASES)
    snapshot = CostSnapshot("USD", entries, [], phases)
    old_hash = snapshot.fingerprint
    entries.clear()
    phases.clear()
    assert snapshot.fingerprint == old_hash
    assert len(snapshot.entries) == 1


def test_checked_in_b0_profile_is_explicitly_uncalibrated_and_disabled():
    path = Path(__file__).parents[1] / "docs/design/v7.0.0/acceptance-profile.json"
    document = json.loads(path.read_text())
    profile_data = document["profile"]
    frozen = QuestionAcceptanceProfile(**profile_data)
    assert frozen.thresholds is None
    assert set(frozen.gaps) == set(document["calibration_gaps"])
    assert document["default_enablement"] is False
    assert document["production_benefit_claim"] is False
    assert set(document["accounting_contract"]["phases"]) == set(CostPhase)
    assert document["accounting_contract"]["all_request_denominator"]
    assert document["accounting_contract"]["effective_answer_denominator"]
    result = evaluate_question_acceptance(
        frozen, run(), run(cost=200), expected_profile_sha256=frozen.fingerprint
    )
    assert not result.ready
    assert "missing_thresholds" in result.reasons


def test_both_not_used_model_declarations_cannot_hide_any_recorded_model_calls():
    with pytest.raises(ValueError, match="not_used model declarations"):
        run(
            semantic_model=ModelEvidence(H, "not_used"),
            generation_model=ModelEvidence(H, "not_used"),
        )
    unbound = replace(entry(), model_role=None, model_configuration_sha256=None)
    with pytest.raises(ValueError, match="not_used model declarations"):
        run(
            semantic_model=ModelEvidence(H, "not_used"),
            generation_model=ModelEvidence(H, "not_used"),
            costs=CostSnapshot("USD", (unbound,), (), ALL_PHASES),
        )


def test_not_used_role_cannot_be_hidden_behind_a_different_declared_executed_role():
    with pytest.raises(ValueError, match="role-bound call"):
        run(semantic_model=ModelEvidence(H, "real"), generation_model=ModelEvidence(H, "not_used"))


def test_each_model_call_binds_to_the_frozen_configuration_for_its_role():
    with pytest.raises(ValueError, match="configuration differs"):
        run(generation_model=ModelEvidence(OTHER, "real"))
    with pytest.raises(ValueError, match="supplied together"):
        entry(model_role=None)
    with pytest.raises(ValueError, match="actual semantic or generation call"):
        entry(model_role="unrecognized_model")


def test_unbound_legacy_calls_can_be_reported_but_never_pass_acceptance():
    unbound = replace(entry(), model_role=None, model_configuration_sha256=None)
    candidate = run(costs=CostSnapshot("USD", (unbound,), (), ALL_PHASES))
    result = gate(candidate)
    assert "candidate_unbound_model_calls" in result.reasons
    assert "candidate_generation_model_execution_unproven" in result.reasons
    assert not result.ready


def test_declared_real_role_without_a_matching_call_is_unproven():
    candidate = run(semantic_model=ModelEvidence(H, "real"))
    result = gate(candidate)
    assert "candidate_semantic_model_execution_unproven" in result.reasons
    assert not result.ready


def test_genuinely_deterministic_arms_require_no_model_calls_and_can_be_compared():
    def deterministic(amount):
        resource = CostEntry(
            "cpu",
            CostPhase.FOREGROUND,
            priced_resource_microunits=amount,
            tariff_reference="fixture-tariff",
        )
        return run(
            generation_model=ModelEvidence(H, "not_used"),
            costs=CostSnapshot("USD", (resource,), (), ALL_PHASES),
        )

    assert gate(deterministic(100), deterministic(200)).ready


def test_both_executed_roles_need_distinct_role_bound_calls():
    semantic = entry("semantic-call", CostPhase.WRITE, 20, model_role="semantic_model")
    candidate = run(
        semantic_model=ModelEvidence(H, "real"),
        costs=CostSnapshot("USD", (entry(), semantic), (), ALL_PHASES),
    )
    baseline = run(
        cost=200,
        semantic_model=ModelEvidence(H, "real"),
        costs=CostSnapshot("USD", (entry(cost=200), semantic), (), ALL_PHASES),
    )
    assert gate(candidate, baseline).ready


def test_settlement_can_reconcile_previously_unknown_tokens_with_the_bill():
    ledger = QuestionCostLedger("USD")
    ledger.record(entry(cost=None, tokens=None, token_basis=TokenBasis.UNKNOWN))
    assert ledger.settle(
        "call1",
        provider_request_id="request-1",
        billed_microunits=100,
        billing_reference="bill-1",
        measured_tokens=37,
    )
    candidate = run(costs=ledger.snapshot(accounted_phases=ALL_PHASES))
    assert summarize_costs(candidate).unknown_token_calls == 0
    assert summarize_costs(candidate).measured_tokens == 37
    assert gate(candidate).ready
    assert not ledger.settle(
        "call1",
        provider_request_id="request-1",
        billed_microunits=100,
        billing_reference="bill-1",
        measured_tokens=37,
    )


@pytest.mark.parametrize("basis,tokens", [(TokenBasis.UNKNOWN, None), (TokenBasis.ESTIMATED, 80)])
def test_late_usage_receipt_enriches_same_bill_without_changing_cost_or_double_counting(
    basis, tokens
):
    ledger = QuestionCostLedger("USD")
    ledger.record(entry(cost=None, tokens=tokens, token_basis=basis))
    ledger.settle(
        "call1", provider_request_id="request-1", billed_microunits=100, billing_reference="bill-1"
    )
    before = ledger.snapshot(accounted_phases=ALL_PHASES)
    assert not gate(run(costs=before)).ready
    assert ledger.settle(
        "call1",
        provider_request_id="request-1",
        billed_microunits=100,
        billing_reference="bill-1",
        measured_tokens=40,
    )
    after = ledger.snapshot(accounted_phases=ALL_PHASES)
    assert gate(run(costs=after)).ready
    assert summarize_costs(run(costs=after)).provider_billed_microunits == 100
    assert summarize_costs(run(costs=after)).model_calls == 1
    assert before.entries[0].token_basis == basis
    with pytest.raises(ValueError, match="conflicting measured usage"):
        ledger.settle(
            "call1",
            provider_request_id="request-1",
            billed_microunits=100,
            billing_reference="bill-1",
            measured_tokens=41,
        )
    assert ledger.snapshot(accounted_phases=ALL_PHASES) == after
