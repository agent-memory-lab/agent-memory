"""Adversarial offline evaluator tests; no provider calls or model quality claims."""

import copy
import json
from dataclasses import dataclass

import pytest

from experiments.evidence_obligations.evaluate import BOOTSTRAP_PROTOCOL, evaluate

TEXT = "The request window is 14 calendar days. Eligibility is limited to unused seats only."
ANSWER = "14 calendar days; unused seats only."
ARMS = ("persistent_obligations", "repaired_atoms", "ordinary_cache")


def fixture(groups=1, arms=ARMS, *, repeats=1, answerable=True):
    results, gold = [], {}
    for group in range(groups):
        group_id, source_id = f"g{group}", f"source{group}"
        query_ids = [f"q{group}-{index}" for index in range(repeats)]
        for query in query_ids:
            gold[query] = dict(
                query_id=query,
                required_values=["14 calendar days"] if answerable else [],
                required_qualifiers=["unused seats only"] if answerable else [],
                answerable=answerable,
                supporting_source_ids=[source_id] if answerable else [],
                supporting_source_revisions=[[source_id, 2]] if answerable else [],
                category="qualifier" if answerable else "unanswerable",
            )
        for arm in arms:
            source = dict(source_id=source_id, revision=2, text=TEXT)
            quote = dict(source_id=source_id, revision=2, quote=TEXT)
            rows = []
            for query in query_ids:
                rows.append(
                    dict(
                        query_id=query,
                        group_id=group_id,
                        arm=arm,
                        raw_output=dict(
                            answer=ANSWER if answerable else "Insufficient evidence.",
                            abstain=not answerable,
                            quotes=[dict(source_id=source_id, quote=TEXT)] if answerable else [],
                        ),
                        delivered_answer=ANSWER if answerable else "Insufficient evidence.",
                        abstain=not answerable,
                        citations=[copy.deepcopy(quote)] if answerable else [],
                        delivery_blocked=False,
                        invalid_citations=0,
                        packed_evidence=[copy.deepcopy(source)],
                        live_sources=[copy.deepcopy(source)],
                        raw_evidence=[copy.deepcopy(source)],
                        retrieved_atoms=[],
                        revoked_source_ids=[],
                    )
                )
            receipts = [
                dict(
                    phase="extract",
                    receipt_id=f"{group_id}-extract",
                    input_tokens=100,
                    output_tokens=20,
                    shared_extraction_reused=arm != arms[0],
                ),
                dict(
                    phase="answer",
                    receipt_id=f"{group_id}-{arm}-answer",
                    input_tokens=50,
                    output_tokens=10,
                ),
            ]
            results.append(
                dict(
                    episode_id=f"episode{group}",
                    group_id=group_id,
                    arm=arm,
                    status="complete",
                    expected_query_ids=query_ids,
                    rows=rows,
                    receipts=receipts,
                    total_tokens=180,
                    total_calls=2,
                    counts=dict(raw_source_reads=1),
                    whole_seconds=3.0,
                )
            )
    return results, gold


def one_score(results, gold):
    return evaluate(results, gold)["rows"][0]


def change_answer(row, answer):
    row["raw_output"]["answer"] = row["delivered_answer"] = answer


def change_quote(row, text):
    row["raw_output"]["quotes"][0]["quote"] = text
    row["citations"][0]["quote"] = text


def test_strict_answer_qualifier_and_current_quote_support_are_separate():
    results, gold = fixture(arms=(ARMS[0],))
    score = one_score(results, gold)
    assert score["answer_correctness"] and score["qualifier_fidelity"]
    assert score["citation_coverage"] and score["citation_support"]
    assert score["effective_correct_answer"]
    change_answer(results[0]["rows"][0], "14 calendar days")
    score = one_score(results, gold)
    assert score["answer_correctness"] and not score["qualifier_fidelity"]
    assert score["citation_support"] and not score["effective_correct_answer"]
    assert score["diagnostic"] == "required_evidence_present_but_answer_or_citation_failed"


def test_complete_answer_does_not_compensate_for_unsupported_qualifier_quote():
    results, gold = fixture(arms=(ARMS[0],))
    change_quote(results[0]["rows"][0], "The request window is 14 calendar days.")
    score = one_score(results, gold)
    assert score["answer_correctness"] and score["qualifier_fidelity"]
    assert score["citation_coverage"] and not score["citation_support"]
    assert score["unsupported_qualifiers"] == ["unused seats only"]
    assert not score["effective_correct_answer"]


def test_correct_source_id_with_forged_quote_fails_exact_support():
    results, gold = fixture(arms=(ARMS[0],))
    change_quote(results[0]["rows"][0], ANSWER)
    score = one_score(results, gold)
    assert not score["citation_support"]
    assert "quote_not_in_packed_evidence" in score["citation_checks"][0]["issues"]


def test_unpacked_current_quote_is_not_admissible_support():
    results, gold = fixture(arms=(ARMS[0],))
    results[0]["rows"][0]["packed_evidence"] = []
    score = one_score(results, gold)
    assert not score["citation_support"]
    assert score["stages"]["raw"]["state"] == "present"
    assert score["stages"]["retrieved"]["state"] == "present"
    assert score["stages"]["packed"]["state"] == "absent"
    assert score["diagnostic"] == "required_evidence_lost_before_packed_stage"


def test_revision_mismatch_is_rejected_even_with_correct_answer_and_exact_quote():
    results, gold = fixture(arms=(ARMS[0],))
    row = results[0]["rows"][0]
    row["citations"][0]["revision"] = row["packed_evidence"][0]["revision"] = 1
    score = one_score(results, gold)
    assert score["answer_correctness"] and not score["citation_support"]
    assert score["stale_revision_leakage"] and not score["revoked_source_leakage"]


def test_gold_revision_check_does_not_depend_on_runtime_current_snapshot():
    results, gold = fixture(arms=(ARMS[0],))
    row = results[0]["rows"][0]
    row.pop("live_sources")
    row["citations"][0]["revision"] = row["packed_evidence"][0]["revision"] = 1
    score = one_score(results, gold)
    assert score["stale_revision_leakage"] and not score["effective_correct_answer"]


def test_revoked_delivery_is_separate_from_stale_revision_leakage():
    results, gold = fixture(arms=(ARMS[0],))
    row = results[0]["rows"][0]
    row["revoked_source_ids"] = ["source0"]
    row["live_sources"] = []
    score = one_score(results, gold)
    assert score["revoked_source_leakage"] and score["unauthorized_source_leakage"]
    assert not score["stale_revision_leakage"] and not score["effective_correct_answer"]


def test_true_abstention_is_separate_and_blocked_output_is_a_failure():
    results, gold = fixture(arms=(ARMS[0],), answerable=False)
    score = one_score(results, gold)
    assert score["correct_abstention"] and not score["effective_correct_answer"]
    row = results[0]["rows"][0]
    row["delivery_blocked"] = True
    row["delivered_answer"] = ""
    score = one_score(results, gold)
    assert not score["correct_abstention"] and score["failures"]


def test_missing_expected_answer_is_retained_in_denominator_and_fails():
    results, gold = fixture(arms=(ARMS[0],), repeats=2)
    results[0]["rows"].pop()
    report = evaluate(results, gold)
    summary = report["arms"][ARMS[0]]
    assert summary["requests"] == 2 and summary["effective_correct_answers"] == 1
    assert summary["effective_rate"] == 0.5 and summary["failed_queries"] == 1
    assert "missing_answer_row" in report["rows"][1]["failures"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("raw_output", None),
        ("delivered_answer", None),
        ("abstain", "false"),
        ("citations", None),
        ("delivery_blocked", None),
    ],
)
def test_invalid_answer_fields_never_pass(field, value):
    results, gold = fixture(arms=(ARMS[0],))
    results[0]["rows"][0][field] = value
    score = one_score(results, gold)
    assert not score["answer_schema_valid"] and not score["effective_correct_answer"]


def test_fabricated_delivered_citations_not_in_raw_output_fail():
    results, gold = fixture(arms=(ARMS[0],))
    results[0]["rows"][0]["raw_output"]["quotes"] = []
    assert not one_score(results, gold)["effective_correct_answer"]


def test_normalization_preserves_word_and_number_boundaries_and_accepts_whitespace():
    results, gold = fixture(arms=(ARMS[0],))
    row = results[0]["rows"][0]
    change_answer(row, "１４ CALENDAR\n  DAYS; UNUSED SEATS ONLY.")
    assert one_score(results, gold)["effective_correct_answer"]
    change_answer(row, "114 calendar days; unused seats only.")
    assert not one_score(results, gold)["answer_correctness"]


def test_unobserved_stages_are_unknown_and_causal_claims_are_not_invented():
    results, gold = fixture(arms=(ARMS[0],))
    row = results[0]["rows"][0]
    for name in ("raw_evidence", "live_sources", "packed_evidence", "retrieved_atoms"):
        row.pop(name)
    row["raw_source_ids"] = ["source0"]
    score = one_score(results, gold)
    assert all(
        score["stages"][stage]["state"] == "unknown" for stage in ("raw", "retrieved", "packed")
    )
    assert score["diagnostic"] == "unknown_stage_attribution"
    assert score["causal_attribution"].startswith("unknown")


def test_raw_present_but_retrieval_absent_is_observable_without_claiming_cause():
    results, gold = fixture(arms=(ARMS[0],))
    row = results[0]["rows"][0]
    row["raw_evidence"] = row["packed_evidence"] = []
    score = one_score(results, gold)
    assert score["diagnostic"] == "required_evidence_lost_before_retrieved_stage"
    assert score["causal_attribution"].startswith("unknown")


def test_costs_include_initial_extraction_and_use_separate_token_rates():
    results, gold = fixture()
    report = evaluate(results, gold)
    summary = report["arms"][ARMS[0]]
    assert summary["input_tokens"] == 150 and summary["output_tokens"] == 30
    assert summary["total_tokens"] == 180 and summary["total_calls"] == 2
    assert summary["tariff_estimated_cny"] == pytest.approx(0.000201)
    assert summary["whole_seconds"] == 3
    provider = report["observed_provider_usage"]
    assert provider["accounting_complete"] and provider["provider_calls"] == 4
    assert provider["total_tokens"] == 300
    assert provider["shared_receipt_references"] == 2
    assert "not provider bill" in provider["classification"]


@pytest.mark.parametrize(
    "mutation", ["missing_phase", "token_mismatch", "invalid_usage", "missing_receipts"]
)
def test_incomplete_accounting_never_yields_verified_cost_savings(mutation):
    results, gold = fixture(groups=2)
    result = results[0]
    if mutation == "missing_phase":
        result["receipts"][0].pop("phase")
    elif mutation == "token_mismatch":
        result["total_tokens"] = 60
    elif mutation == "invalid_usage":
        result["receipts"][0]["input_tokens"] = None
    else:
        result["receipts"] = []
    report = evaluate(results, gold)
    assert not report["arms"][ARMS[0]]["accounting_complete"]
    comparison = report["comparisons"]["persistent_obligations_minus_repaired_atoms"]
    assert comparison["metrics"]["total_tokens"]["difference"] is None
    assert comparison["metrics"]["total_tokens"]["interval"] is None


def test_conflicting_provider_receipt_identity_blocks_deduplicated_total():
    results, gold = fixture()
    results[1]["receipts"][0]["output_tokens"] += 1
    provider = evaluate(results, gold)["observed_provider_usage"]
    assert not provider["accounting_complete"] and provider["total_tokens"] is None
    assert "conflicting_duplicate_provider_receipt" in provider["issues"]


def test_protocol_is_existing_a9_and_resamples_groups_not_repeated_queries():
    results, gold = fixture(groups=2, repeats=7)
    first = evaluate(results, gold)
    assert first == evaluate(results, gold)
    comparison = first["comparisons"]["persistent_obligations_minus_repaired_atoms"]
    assert first["bootstrap_protocol_sha256"] == BOOTSTRAP_PROTOCOL.fingerprint
    assert BOOTSTRAP_PROTOCOL.seed == 17 and BOOTSTRAP_PROTOCOL.resamples == 2000
    assert comparison["group_count"] == 2 and comparison["request_count"] == 14
    assert comparison["resampling_unit"] == "whole_source_group"
    assert comparison["metrics"]["effective_rate"]["interval"] == [0, 0]
    assert comparison["metrics"]["total_tokens"]["interval"] == [0, 0]
    assert len(comparison["per_group"]) == 2 and not comparison["production_promotion"]
    single_results, single_gold = fixture(groups=2, repeats=1)
    single = evaluate(single_results, single_gold)["comparisons"][
        "persistent_obligations_minus_repaired_atoms"
    ]
    assert comparison["draws_sha256"] == single["draws_sha256"]
    json.dumps(first, allow_nan=False)


def test_single_cluster_has_no_interval_even_with_many_repeated_queries():
    results, gold = fixture(repeats=50)
    comparison = evaluate(results, gold)["comparisons"][
        "persistent_obligations_minus_ordinary_cache"
    ]
    assert comparison["group_count"] == 1 and comparison["resamples"] == 0
    assert all(value["interval"] is None for value in comparison["metrics"].values())
    assert "descriptive and unstable" in comparison["small_n_warning"]


def test_zero_effective_bootstrap_draws_are_retained_and_block_ratio_interval():
    results, gold = fixture(groups=2)
    for result in results:
        if result["group_id"] == "g1":
            change_answer(result["rows"][0], "No supported value.")
    comparison = evaluate(results, gold)["comparisons"][
        "persistent_obligations_minus_repaired_atoms"
    ]
    ratio = comparison["metrics"]["tariff_estimated_cny_per_effective_answer"]
    assert ratio["difference"] == 0 and ratio["interval"] is None
    assert 0 < ratio["undefined_resamples"] < 2000
    assert comparison["metrics"]["effective_rate"]["interval"] == [0, 0]


def test_paired_total_difference_direction_is_persistent_minus_comparator():
    results, gold = fixture(groups=2)
    for result in results:
        if result["arm"] == ARMS[0]:
            result["receipts"][1]["input_tokens"] -= 10
            result["total_tokens"] -= 10
            result["whole_seconds"] -= 1
    comparison = evaluate(results, gold)["comparisons"][
        "persistent_obligations_minus_ordinary_cache"
    ]
    assert comparison["metrics"]["total_tokens"]["difference"] == -20
    assert comparison["metrics"]["total_tokens"]["interval"] == [-20, -20]
    assert comparison["metrics"]["whole_seconds"]["difference"] == -2
    assert comparison["per_group"][0]["differences"]["total_tokens"] == -10


@pytest.mark.parametrize(
    "mutation", ["failed", "missing_cell", "duplicate_cell", "duplicate_query", "unknown_query"]
)
def test_incomplete_or_unidentifiable_cells_cannot_produce_report(mutation):
    results, gold = fixture(groups=2)
    if mutation == "failed":
        results[0]["status"] = "failed_or_budget_censored"
    elif mutation == "missing_cell":
        results.pop()
    elif mutation == "duplicate_cell":
        results.append(copy.deepcopy(results[0]))
    elif mutation == "duplicate_query":
        results[0]["rows"].append(copy.deepcopy(results[0]["rows"][0]))
    else:
        results[0]["rows"][0]["query_id"] = "not-declared"
    with pytest.raises(ValueError):
        evaluate(results, gold)


def test_missing_gold_revisions_cannot_be_assumed_current():
    results, gold = fixture()
    gold["q0-0"]["supporting_source_revisions"] = []
    with pytest.raises(ValueError, match="support/revision mismatch"):
        evaluate(results, gold)


def test_gold_dataclass_and_dictionary_have_identical_results():
    @dataclass(frozen=True)
    class Gold:
        query_id: str
        required_values: tuple
        required_qualifiers: tuple
        answerable: bool
        supporting_source_ids: tuple
        category: str
        supporting_source_revisions: tuple

    results, gold = fixture()
    objects = {key: Gold(**value) for key, value in gold.items()}
    assert evaluate(results, gold) == evaluate(results, objects)
