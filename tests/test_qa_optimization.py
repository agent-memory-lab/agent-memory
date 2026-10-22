from dataclasses import replace

import pytest

from agent_memory.evaluation.qa_optimization import (
    QACase,
    QAExecutionManifest,
    QAObservation,
    QAOptimizationPlan,
    QAOptimizationRun,
    compare_qa_optimization,
)
from agent_memory.evaluation.question_cost import CostEntry, CostPhase, CostSnapshot
from agent_memory.evaluation.question_statistics import BootstrapProtocol

SHA = "a" * 64


def plan(**changes):
    value = QAOptimizationPlan(
        tuple(
            QACase(str(i), f"session-{i // 2}", "time" if i % 2 else "fact", True)
            for i in range(12)
        ),
        SHA,
        SHA,
        SHA,
        SHA,
        BootstrapProtocol(7, 200, 0.95, 5, 10),
        minimum_accuracy=0.8,
        maximum_technical_failure_rate=0.02,
        maximum_group_accuracy_drop=0,
        maximum_cost_ratio=1,
        maximum_latency_p95_ratio=1.2,
    )
    return replace(value, **changes)


def run(frozen, name="baseline", **changes):
    rows = tuple(
        QAObservation(
            c.case_id, "completed", True, True, True, "holdout", 10, governance_passed=True
        )
        for c in frozen.cases
    )
    entry = CostEntry(
        "read",
        CostPhase.FOREGROUND,
        priced_resource_microunits=100,
        tariff_reference="fixture-only",
    )
    value = QAOptimizationRun(
        name,
        frozen.fingerprint,
        SHA,
        "offline",
        rows,
        CostSnapshot("CNY", (entry,), (), tuple(CostPhase)),
        True,
        QAExecutionManifest(
            frozen.dataset_sha256,
            frozen.answer_model_sha256,
            frozen.judge_sha256,
            frozen.workload_sha256,
        ),
    )
    return replace(value, **changes)


def compare(frozen, left, right):
    return compare_qa_optimization(frozen, left, right, expected_plan_sha256=frozen.fingerprint)


def test_perfect_offline_results_cannot_be_promoted_to_real_acceptance():
    frozen = plan()
    report = compare(frozen, run(frozen), run(frozen, "candidate"))
    assert report["candidate"]["full_benchmark_accuracy"] == 1
    assert report["paired"]["confidence_interval"] == (0, 0)
    assert not report["accepted"]
    assert {"no_licensed_real_dataset", "candidate:offline_execution"} <= set(
        report["acceptance_gaps"]
    )


def test_missing_cases_stay_in_paired_denominator_and_block_full_accuracy():
    frozen = plan()
    candidate = run(frozen, "candidate")
    candidate = replace(candidate, observations=candidate.observations[:-1])
    report = compare(frozen, run(frozen), candidate)
    assert report["candidate"]["full_benchmark_accuracy"] is None
    assert report["paired"]["effective_per_planned_difference"] == -1 / 12
    assert "candidate:full_dataset_not_scored" in report["acceptance_gaps"]


def test_regression_in_a_task_kind_cannot_hide_behind_other_improvements():
    frozen = plan(minimum_accuracy=0.1)
    before, after = run(frozen), run(frozen, "candidate")
    before = replace(
        before,
        observations=tuple(
            replace(r, correct=False, failure_stage="retrieval") if int(r.case_id) % 2 == 0 else r
            for r in before.observations
        ),
    )
    after = replace(
        after,
        observations=tuple(
            replace(r, correct=False, failure_stage="reasoning") if r.case_id == "1" else r
            for r in after.observations
        ),
    )
    report = compare(frozen, before, after)
    assert report["paired"]["effective_per_planned_difference"] > 0
    assert "task_kind_regression:time" in report["acceptance_gaps"]
    assert report["candidate"]["failure_stages"] == {"reasoning": 1}


def test_shared_histories_are_resampled_as_groups_deterministically():
    frozen = plan()
    before, after = run(frozen), run(frozen, "candidate")
    report = compare(frozen, before, after)
    assert report == compare(frozen, before, after)
    assert report["paired"]["groups"] == 6
    assert report["paired"]["planned"] == 12


def test_unjudged_technical_failure_stays_visible_in_latency_and_quality():
    frozen = plan()
    before, after = run(frozen), run(frozen, "candidate")
    failed = replace(
        after.observations[0],
        status="invalid",
        correct=None,
        failure_stage="technical",
        latency_ms=1000,
    )
    after = replace(after, observations=(failed, *after.observations[1:]))
    report = compare(frozen, before, after)
    assert report["candidate"]["technical_failures"] == 1
    assert report["candidate"]["latency_p95_ms"] == 1000
    assert "latency_ratio_threshold_not_met" in report["acceptance_gaps"]


def test_cost_estimates_cannot_certify_settled_full_cost():
    frozen = plan()
    before, after = run(frozen), run(frozen, "candidate")
    costs = CostSnapshot(
        "CNY",
        (CostEntry("unknown", CostPhase.FAILURE, estimated_microunits=1),),
        (),
        tuple(CostPhase),
    )
    report = compare(frozen, before, replace(after, costs=costs))
    assert "candidate:unsettled_cost" in report["acceptance_gaps"]


def test_governance_unknown_or_failed_blocks_an_otherwise_correct_answer():
    frozen = plan()
    before, after = run(frozen), run(frozen, "candidate")
    for value in (None, False):
        rows = (replace(after.observations[0], governance_passed=value), *after.observations[1:])
        report = compare(frozen, before, replace(after, observations=rows))
        assert "candidate:governance_failure" in report["acceptance_gaps"]
        assert report["paired"]["effective_per_planned_difference"] == -1 / 12


def test_development_cases_never_become_holdout_by_renaming_the_run():
    frozen = plan(dataset_kind="licensed_real", dataset_license_reference="fixture-license")
    before, after = run(frozen, execution="real"), run(frozen, "candidate", execution="real")
    after = replace(
        after, observations=tuple(replace(r, exposure="development") for r in after.observations)
    )
    assert "candidate:no_independent_holdout" in compare(frozen, before, after)["acceptance_gaps"]


def test_comparison_rejects_posthoc_plan_changes_and_foreign_workloads():
    frozen = plan()
    before, after = run(frozen), run(frozen, "candidate")
    with pytest.raises(ValueError, match="freeze"):
        compare_qa_optimization(
            replace(frozen, maximum_cost_ratio=2),
            before,
            after,
            expected_plan_sha256=frozen.fingerprint,
        )
    with pytest.raises(ValueError, match="manifest"):
        compare(
            frozen,
            before,
            replace(
                after,
                observations=(
                    replace(after.observations[0], answerable=False),
                    *after.observations[1:],
                ),
            ),
        )
    with pytest.raises(ValueError, match="currencies"):
        compare(frozen, before, replace(after, costs=replace(after.costs, currency="USD")))


def test_zero_cost_baseline_does_not_hide_positive_candidate_cost():
    frozen = plan()
    before, after = run(frozen), run(frozen, "candidate")
    before = replace(before, costs=CostSnapshot("CNY", (), (), tuple(CostPhase)))
    report = compare(frozen, before, after)
    assert report["paired"]["cost_upper_ratio"] is None
    assert "cost_ratio_threshold_not_met" in report["acceptance_gaps"]


def test_run_manifest_is_required_for_acceptance_even_with_matching_plan_hash():
    frozen = plan()
    before, after = run(frozen), run(frozen, "candidate", execution_manifest=None)
    report = compare(frozen, before, after)
    assert "candidate:execution_manifest_missing" in report["acceptance_gaps"]
    assert not report["accepted"]


def test_complete_host_attested_run_passes_only_all_frozen_contract_gates():
    # Contract fixture: declaring real here does not turn this test into a real experiment.
    frozen = plan(dataset_kind="licensed_real", dataset_license_reference="fixture-license")
    before, after = run(frozen, execution="real"), run(frozen, "candidate", execution="real")
    report = compare(frozen, before, after)
    assert report["accepted"] and report["acceptance_gaps"] == []
    after = replace(after, execution_manifest=None)
    report = compare(frozen, before, after)
    assert not report["accepted"]
    assert report["acceptance_gaps"] == ["candidate:execution_manifest_missing"]


@pytest.mark.parametrize(
    "field", ["dataset_sha256", "answer_model_sha256", "judge_sha256", "workload_sha256"]
)
def test_changed_runtime_binding_cannot_reuse_frozen_plan_hash(field):
    frozen = plan()
    before, after = run(frozen), run(frozen, "candidate")
    after = replace(
        after, execution_manifest=replace(after.execution_manifest, **{field: "b" * 64})
    )
    with pytest.raises(ValueError, match="execution manifest"):
        compare(frozen, before, after)


@pytest.mark.parametrize(
    "changes", [{"maximum_cost_ratio": float("nan")}, {"minimum_accuracy": True}, {"cases": ()}]
)
def test_invalid_frozen_plan_is_rejected(changes):
    with pytest.raises(ValueError):
        plan(**changes)
