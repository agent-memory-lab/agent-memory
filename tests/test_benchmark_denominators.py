"""Planned-case operational metrics, not claims of public QA benchmark accuracy."""

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json
import math

import pytest

from agent_memory import (
    BenchmarkSkip,
    CallableMemoryBenchmarkArm,
    MemoryBenchmarkCase,
    MemoryBenchmarkConfig,
    MemoryBenchmarkDataset,
    MemoryBenchmarkDimension,
    MemoryBenchmarkObservation,
    NoMemoryBenchmarkArm,
    run_memory_benchmark,
)
from agent_memory.domain import MemoryScope


def _run(candidate, baseline=None, *, clusters=None, relevant_count=4):
    baseline = baseline if baseline is not None else [0.0] * len(candidate)
    clusters = clusters if clusters is not None else [None] * len(candidate)
    relevant = tuple(f"memory-{index}" for index in range(relevant_count))
    cases = tuple(
        MemoryBenchmarkCase(
            f"original-{index}", MemoryScope("tenant"), f"query {index}",
            MemoryBenchmarkDimension.FACTUAL, relevant,
            cluster_id=cluster,
        )
        for index, cluster in enumerate(clusters)
    )

    def retrieve(values):
        def run(request):
            assert not hasattr(request, "cluster_id")
            value = values[int(request.query_text.split()[-1])]
            if isinstance(value, Exception):
                raise value
            return MemoryBenchmarkObservation(relevant[:round(value * relevant_count)], (), 4)
        return run

    return asyncio.run(run_memory_benchmark(
        MemoryBenchmarkDataset("denominators", "1", "internal", cases),
        (
            NoMemoryBenchmarkArm(),
            CallableMemoryBenchmarkArm("fixed-policy", "1", retrieve(baseline)),
            CallableMemoryBenchmarkArm("candidate-policy", "1", retrieve(candidate)),
        ),
        MemoryBenchmarkConfig(datetime(2026, 11, 3, tzinfo=timezone.utc), "1", {"core": "1"}),
    ))


def _candidate(report):
    return next(arm for arm in report.arms if arm.arm == "candidate-policy")


def _comparison(report):
    return next(pair for pair in report.comparisons if pair.baseline_arm == "fixed-policy")


@pytest.mark.parametrize(
    "missing", [RuntimeError("technical failure"), BenchmarkSkip("unsupported")]
)
def test_dropping_hard_cases_cannot_inflate_planned_advantage(missing):
    complete = _run([0.75, 0.25, 0.25, 0.25], [0.5] * 4)
    incomplete = _run([0.75, missing, missing, missing], [0.5] * 4)
    candidate = _candidate(incomplete)
    pair = _comparison(incomplete)

    # The historical conditional score/delta increase: this is exactly the survivor-bias trap.
    assert candidate.mean_score == candidate.completed_mean_score == 0.75
    assert candidate.mean_score > _candidate(complete).mean_score
    assert pair.mean_score_delta == pair.jointly_completed_mean_score_delta == 0.25
    assert pair.mean_score_delta > _comparison(complete).mean_score_delta
    assert candidate.planned_mean_score == 0.75 / 4
    assert candidate.planned_mean_score <= _candidate(complete).planned_mean_score
    assert pair.planned_mean_score_delta == (0.75 - 4 * 0.5) / 4
    assert pair.planned_mean_score_delta <= _comparison(complete).planned_mean_score_delta
    assert pair.planned_mean_score_delta < 0
    assert candidate.planned_cases == candidate.sample_count == pair.planned_cases == 4
    assert candidate.completed_cases == pair.jointly_completed_cases == 1
    assert candidate.planned_success_rate == 0.25
    assert candidate.completed_success_rate == 1.0
    assert candidate.planned_success_confidence_high < candidate.completed_success_confidence_high
    dimension = candidate.dimensions[0]
    assert dimension.planned_cases == 4
    assert dimension.planned_mean_score == 0.75 / 4
    assert dimension.completed_mean_score == 0.75
    assert dimension.planned_success_rate == 0.25
    assert dimension.completed_success_rate == 1.0
    assert candidate.failed_cases + candidate.skipped_cases == 3
    assert candidate.failed_cases == (0 if isinstance(missing, BenchmarkSkip) else 3)
    # No fabricated observations are mixed into actual completed evidence.
    assert len([row for row in incomplete.case_results if row.arm == "candidate-policy"]) == 1


@pytest.mark.parametrize("missing", [RuntimeError("offline"), BenchmarkSkip("disabled")])
def test_all_missing_outcomes_keep_full_planned_denominator(missing):
    report = _run([missing, missing], [0.5, 1.0])
    candidate = _candidate(report)
    comparison = _comparison(report)
    assert candidate.planned_success_rate == candidate.planned_mean_score == 0
    assert candidate.planned_success_confidence_low == 0
    assert candidate.planned_success_confidence_high > 0
    assert comparison.comparable_cases == comparison.jointly_completed_cases == 0
    assert comparison.mean_score_delta is None
    assert comparison.incomparable_reason == "no jointly completed cases"
    assert comparison.planned_cases == 2
    assert comparison.planned_mean_score_delta == -0.75


def test_both_arms_missing_and_asymmetric_failures_are_paired_by_planned_case():
    report = _run([1.0, RuntimeError(), RuntimeError()], [RuntimeError(), 1.0, RuntimeError()])
    comparison = _comparison(report)
    assert comparison.jointly_completed_cases == 0
    assert comparison.planned_cases == 3
    assert comparison.planned_mean_score_delta == 0
    assert comparison.planned_confidence_low < 0 < comparison.planned_confidence_high
    assert _candidate(report).planned_mean_score == 1 / 3


def test_no_completion_in_either_arm_does_not_claim_proven_equivalence():
    report = _run([RuntimeError(), RuntimeError()], [RuntimeError(), RuntimeError()])
    comparison = _comparison(report)
    assert comparison.planned_mean_score_delta == 0
    assert comparison.planned_confidence_low is comparison.planned_confidence_high is None
    assert "Zero empirical variance" in comparison.planned_uncertainty_reason


def test_unequal_clusters_preserve_case_weighted_estimand_and_cluster_total_variance():
    clusters = ["source-a"] * 3 + ["source-b"] + ["source-c"] * 2
    report = _run([1.0] * 3 + [0.0] * 3, clusters=clusters)
    comparison = _comparison(report)
    # The unweighted cluster mean would be 1/3, which is not this metric's estimand.
    assert comparison.planned_mean_score_delta == 0.5
    assert comparison.planned_cluster_count == 3
    assert comparison.planned_confidence_method == "cluster_robust_normal_approximation"
    variance = (3 / 2) * (1.5**2 + (-0.5)**2 + (-1.0)**2) / 6**2
    assert comparison.planned_confidence_low == pytest.approx(0.5 - 1.96 * math.sqrt(variance))
    assert comparison.planned_confidence_high == 1.0
    candidate = _candidate(report)
    assert candidate.planned_success_confidence_low == 0
    assert candidate.planned_success_confidence_high == 1
    assert candidate.planned_cluster_count == 3
    assert "few clusters" in candidate.planned_uncertainty_reason
    assert [
        result.cluster_id for result in report.case_results if result.arm == "candidate-policy"
    ] == clusters


def test_repeated_cases_do_not_fake_independent_cluster_precision():
    small = _run([1.0, 0.0], clusters=["a", "b"])
    repeated = _run([1.0] * 10 + [0.0] * 10, clusters=["a"] * 10 + ["b"] * 10)
    assert _comparison(small).planned_confidence_low == pytest.approx(
        _comparison(repeated).planned_confidence_low
    )
    assert _comparison(small).planned_confidence_high == pytest.approx(
        _comparison(repeated).planned_confidence_high
    )


@pytest.mark.parametrize("outcome", [1.0, RuntimeError(), BenchmarkSkip("disabled")])
def test_zero_cluster_variance_does_not_claim_certain_success_or_equal_quality(outcome):
    report = _run([outcome, outcome], clusters=["a", "b"])
    candidate = _candidate(report)
    comparison = _comparison(report)
    assert candidate.planned_success_confidence_low is None
    assert candidate.planned_success_confidence_high is None
    assert comparison.planned_confidence_low is comparison.planned_confidence_high is None
    assert "Zero empirical cluster variance" in candidate.planned_uncertainty_reason


@pytest.mark.parametrize("values, count", [([1 / 3, 2 / 3], 3), ([0.1, 0.2, 0.3], 10)])
def test_equal_fractional_cluster_means_do_not_create_roundoff_intervals(values, count):
    report = _run(
        values * 2, clusters=["a"] * len(values) + ["b"] * len(values), relevant_count=count
    )
    comparison = _comparison(report)
    assert comparison.planned_confidence_low is comparison.planned_confidence_high is None
    assert "Zero empirical cluster variance" in comparison.planned_uncertainty_reason


@pytest.mark.parametrize("clusters, method, count", [
    (["source", "source"], "cluster_robust_normal_approximation", 1),
    (["source", None], "unavailable_incomplete_cluster_metadata", None),
])
def test_insufficient_cluster_information_does_not_fabricate_intervals(clusters, method, count):
    report = _run([1.0, RuntimeError()], clusters=clusters)
    comparison = _comparison(report)
    candidate = _candidate(report)
    assert comparison.planned_mean_score_delta == candidate.planned_mean_score == 0.5
    assert comparison.planned_confidence_low is comparison.planned_confidence_high is None
    assert candidate.planned_success_confidence_low is None
    assert candidate.planned_success_confidence_high is None
    assert candidate.planned_confidence_method == comparison.planned_confidence_method == method
    assert candidate.planned_cluster_count == comparison.planned_cluster_count == count
    assert report.failures[0].cluster_id == clusters[1]


def test_missing_cluster_metadata_is_disclosed_and_reports_lead_with_planned_metrics():
    report = _run([1.0, BenchmarkSkip("unsupported")])
    comparison = _comparison(report)
    candidate = _candidate(report)
    assert candidate.planned_confidence_method == "wilson_case_independence_assumed"
    assert comparison.planned_confidence_method == "normal_case_independence_assumed"
    assert "within-source dependence" in comparison.planned_uncertainty_reason
    data = json.loads(report.to_json())
    assert data["metric_policy"] == "planned_cases_zero_credit_for_failures_and_skips"
    assert "completed-only diagnostics" in data["legacy_metric_policy"]
    assert data["arms"][0]["planned_mean_score"] == 0.5
    assert data["arms"][0]["completed_mean_score"] == data["arms"][0]["mean_score"] == 1
    markdown = report.to_markdown()
    assert markdown.index("Planned mean score") < markdown.index("Completed-only diagnostics")
    assert "survivor bias" in markdown
    assert "within-source dependence" in markdown
    assert "Skipped: 1" in markdown


def test_cluster_identifier_validation_and_absent_single_case_interval():
    case = MemoryBenchmarkCase("case", MemoryScope("tenant"), "query", "factual", ("id",))
    for invalid in (True, 4, "", " ", "a" * 129):
        with pytest.raises(ValueError, match="cluster_id"):
            replace(case, cluster_id=invalid)
    report = _run([1.0])
    assert _comparison(report).planned_mean_score_delta == 1
    assert _comparison(report).planned_confidence_low is None
    assert "Fewer than 2 planned cases" in _comparison(report).planned_uncertainty_reason
