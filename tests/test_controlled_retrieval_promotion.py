"""Synthetic contract tests only: these never assert real retrieval quality."""

import asyncio
from dataclasses import replace

import pytest

from agent_memory.domain import MemoryScope
from agent_memory.evaluation.controlled_retrieval import (
    ARMS,
    BASELINE_SEMANTICS,
    COSTS,
    METRICS,
    QUALITY,
    ControlledPreparation,
    ControlledRetrievalCase,
    ControlledRetrievalObservation,
    ControlledRetrievalPlan,
    ControlledWorkCost,
    PromotionBenefit,
    assess_controlled_retrieval,
    run_controlled_retrieval,
)
from agent_memory.evaluation.retrieval import (
    NoMemoryEvaluationArm,
    RetrievalDatasetSnapshot,
    RetrievalEvalCase,
    RetrievalObservation,
    run_retrieval_benchmark,
)

SCOPE = MemoryScope("test")
ZERO = ControlledWorkCost(**{name: 0 for name in COSTS})


def plan():
    cases = tuple(
        ControlledRetrievalCase(
            RetrievalEvalCase(
                f"case-{group}-{answerable}",
                SCOPE,
                "query",
                ("good",) if answerable else (),
                forbidden_memory_ids=("forbidden",),
            ),
            f"project-{group}",
            answerable,
        )
        for group in range(3)
        for answerable in (False, True)
    )
    return ControlledRetrievalPlan(
        cases,
        *(["a" * 64] * 6),
        tuple((metric, 1_000_000 if metric not in QUALITY else 0) for metric in METRICS),
        tuple(PromotionBenefit(arm, "answer_score", 0.1) for arm in ARMS[1:]),
        minimum_groups=3,
        minimum_cases=6,
        resamples=100,
        evidence_annotations_sha256="b" * 64,
    )


class ContractArm:
    measurement_kind = "contract-only"

    def __init__(
        self,
        name,
        protocol,
        *,
        fail=False,
        unknown=False,
        forbidden=False,
        maintenance=ZERO,
        disable_existing_reuse=False,
    ):
        self.name = name
        self.configuration_sha256 = protocol.arm_configuration(name)
        self.model_sha256, self.judge_sha256 = protocol.model_sha256, protocol.judge_sha256
        self.protocol = protocol
        self.fail, self.unknown, self.forbidden = fail, unknown, forbidden
        self.maintenance = maintenance
        self.disable_existing_reuse = disable_existing_reuse
        self.lifecycle = []

    async def prepare(self, regime):
        self.lifecycle.append(("prepare", regime))
        return ControlledPreparation(
            self.protocol.corpus_sha256, True, not self.disable_existing_reuse, self.maintenance
        )

    async def run(self, case, regime):
        self.lifecycle.append(("run", regime, case.case.case_id))
        if self.fail:
            raise RuntimeError("private fault text")
        ids = ("forbidden",) if self.forbidden else ("good",)
        return ControlledRetrievalObservation(
            ids,
            ids,
            0.5 if self.name == ARMS[0] else 1.0,
            not case.answerable,
            ControlledWorkCost() if self.unknown else ZERO,
            supporting_span_coverage=1.0,
            qualifier_fidelity=1.0,
            evidence_judgment_sha256="c" * 64,
            unsafe_deliveries=0,
            stale_deliveries=0,
        )

    async def finish(self, regime):
        self.lifecycle.append(("finish", regime))
        return self.maintenance


def run(protocol=None, **kwargs):
    protocol = protocol or plan()
    arms = tuple(ContractArm(name, protocol, **kwargs) for name in ARMS)
    report = asyncio.run(
        run_controlled_retrieval(protocol, arms, expected_protocol_sha256=protocol.fingerprint)
    )
    return protocol, arms, report


def assess(protocol, report):
    return assess_controlled_retrieval(
        protocol,
        report,
        expected_protocol_sha256=protocol.fingerprint,
        rollback_id="disable-features",
    )


def test_exact_four_arms_and_cold_warm_manifest_with_paired_uncertainty_cannot_promote_mocks():
    protocol, arms, report = run()
    assert len(report.rows) == 4 * 2 * 6
    assert not report.blockers
    for arm in arms:
        assert ("prepare", "cold") in arm.lifecycle and ("prepare", "warm") in arm.lifecycle
        assert ("finish", "cold") in arm.lifecycle and ("finish", "warm") in arm.lifecycle
    decisions = assess(protocol, report)
    assert len(decisions) == 3
    for decision in decisions:
        assert not decision.eligible and decision.approval is None
        assert "contract_only_or_unverified_measurements" in decision.blockers
        answer = [row for row in decision.intervals if row[1] == "answer_score"]
        assert answer == [
            ("cold", "answer_score", 0.5, 0.5, 0.5),
            ("warm", "answer_score", 0.5, 0.5, 0.5),
        ]


def test_frozen_controls_manifest_and_baseline_semantics_cannot_change():
    protocol = plan()
    arms = tuple(ContractArm(name, protocol) for name in ARMS)
    with pytest.raises(ValueError, match="freeze"):
        asyncio.run(run_controlled_retrieval(protocol, arms, expected_protocol_sha256="b" * 64))
    with pytest.raises(ValueError, match="exactly"):
        asyncio.run(
            run_controlled_retrieval(
                protocol, arms[:3], expected_protocol_sha256=protocol.fingerprint
            )
        )
    arms[1].model_sha256 = "b" * 64
    with pytest.raises(ValueError, match="controls"):
        asyncio.run(
            run_controlled_retrieval(protocol, arms, expected_protocol_sha256=protocol.fingerprint)
        )
    with pytest.raises(ValueError, match="optimized"):
        replace(protocol, baseline_semantics="regenerate-every-request")
    _, _, report = run(disable_existing_reuse=True)
    assert report.blockers and all(row.error_type for row in report.rows)
    assert all(not value.eligible for value in assess(protocol, report))


def test_all_maintenance_calls_costs_and_resources_are_included():
    maintenance = ControlledWorkCost(6, 60, 12, 1000, 2000, 24, 12, 2, 2, 2, 30)
    protocol, _, report = run(maintenance=maintenance)
    row = dict(report.rows[0].values)
    assert row["model_calls"] == 2 and row["cost_microunits"] == 20
    assert row["cpu_ms"] == 4 and row["peak_rss_bytes"] == 1000
    assert row["retained_bytes"] == 2000 and row["input_tokens"] == 8
    assert row["whole_latency_ms"] >= row["latency_ms"]
    assert BASELINE_SEMANTICS == protocol.baseline_semantics


@pytest.mark.parametrize(
    "kwargs, reason",
    [
        ({"unknown": True}, "unknown:cost_microunits"),
        ({"fail": True}, "failed_observation"),
        ({"forbidden": True}, "forbidden_or_unknown"),
    ],
)
def test_unknown_failed_or_forbidden_results_are_preserved_and_block_promotion(kwargs, reason):
    protocol, _, report = run(**kwargs)
    assert all(dict(row.values)["latency_ms"] >= 0 for row in report.rows)
    decisions = assess(protocol, report)
    assert all(any(reason in blocker for blocker in value.blockers) for value in decisions)
    assert all(value.approval is None for value in decisions)
    assert "private fault text" not in str(report)


def test_missing_duplicate_and_group_changed_rows_are_rejected():
    protocol, _, report = run()
    for rows in (report.rows[:-1], report.rows + report.rows[:1]):
        with pytest.raises(ValueError, match="paired rows"):
            assess(protocol, replace(report, rows=rows))
    bad = replace(report.rows[0], group_id="changed")
    with pytest.raises(ValueError, match="group binding"):
        assess(protocol, replace(report, rows=(bad, *report.rows[1:])))


def test_legacy_benchmark_allows_bounded_extra_arms_without_removing_baselines_and_counts_failures(
    monkeypatch,
):
    class Arm:
        def __init__(self, name):
            self.name = name

        async def retrieve(self, case):
            if self.name == "extra":
                raise RuntimeError("fault")
            return RetrievalObservation(
                ("good",),
                (),
                10,
                model_calls=1,
                cost_microunits=2,
                candidates_considered=3,
                cpu_ms=4,
                peak_rss_bytes=5,
            )

    clock = iter(range(100))
    monkeypatch.setattr("agent_memory.evaluation.retrieval.monotonic", lambda: next(clock))
    snapshot = RetrievalDatasetSnapshot("test", "1", (plan().cases[1].case,))
    arms = (NoMemoryEvaluationArm(), Arm("lexical-only"), Arm("hybrid"), Arm("extra"))
    report = asyncio.run(run_retrieval_benchmark(snapshot, arms))
    metrics = {value.arm: value for value in report.metrics}
    assert metrics["extra"].latency_count == 1 and metrics["extra"].latency_p95_ms == 1000
    assert (
        metrics["extra"].unknown_cost_cases == 1 and metrics["extra"].unknown_model_call_cases == 1
    )
    assert metrics["hybrid"].known_model_calls == 1 and metrics["hybrid"].known_cost_microunits == 2
    assert metrics["hybrid"].unknown_resource_cases == 0
    with pytest.raises(ValueError, match="baselines"):
        asyncio.run(run_retrieval_benchmark(snapshot, (Arm("extra"),)))
    with pytest.raises(ValueError, match="unique"):
        asyncio.run(run_retrieval_benchmark(snapshot, (*arms, Arm("extra"))))


def test_malformed_arm_return_is_a_timed_failed_observation_not_runner_abort():
    protocol = plan()

    class Malformed(ContractArm):
        async def run(self, case, regime):
            return {"wrong": "schema"}

    arms = tuple(Malformed(name, protocol) for name in ARMS)
    report = asyncio.run(
        run_controlled_retrieval(protocol, arms, expected_protocol_sha256=protocol.fingerprint)
    )
    assert len(report.rows) == 48
    assert all(row.error_type == "TypeError" for row in report.rows)
    assert all(dict(row.values)["cost_microunits"] is None for row in report.rows)
    assert all(value.approval is None for value in assess(protocol, report))


def test_peak_resource_gate_uses_workload_maxima_not_mean_query_peaks():
    protocol = plan()

    class Peaks(ContractArm):
        async def run(self, case, regime):
            observed = await super().run(case, regime)
            peak = 100 if self.name == ARMS[0] else (200 if case.answerable else 0)
            return replace(observed, costs=replace(ZERO, peak_rss_bytes=peak, retained_bytes=peak))

    arms = tuple(Peaks(name, protocol) for name in ARMS)
    report = asyncio.run(
        run_controlled_retrieval(protocol, arms, expected_protocol_sha256=protocol.fingerprint)
    )
    decisions = assess(protocol, report)
    for decision in decisions:
        peaks = [
            row for row in decision.intervals if row[1] in ("peak_rss_bytes", "retained_bytes")
        ]
        assert len(peaks) == 4
        assert all(row[2:] == (100, 100, 100) for row in peaks)


def test_report_retains_raw_lifecycle_and_rejects_altered_whole_cost_allocations():
    protocol, _, report = run(maintenance=replace(ZERO, model_calls=6, cost_microunits=60))
    assert len(report.lifecycle) == 8
    assert all(value.preparation.costs.model_calls == 6 for value in report.lifecycle)
    row = report.rows[0]
    values = dict(row.values)
    values["cost_microunits"] = 0
    with pytest.raises(ValueError, match="allocation"):
        assess(
            protocol,
            replace(report, rows=(replace(row, values=tuple(values.items())), *report.rows[1:])),
        )


def run_variant(variant):
    protocol = plan()

    class Variant(ContractArm):
        async def run(self, case, regime):
            observed = await super().run(case, regime)
            return variant(self.name, case, observed)

    arms = tuple(Variant(name, protocol) for name in ARMS)
    report = asyncio.run(
        run_controlled_retrieval(protocol, arms, expected_protocol_sha256=protocol.fingerprint)
    )
    return protocol, report


def test_document_ids_do_not_mask_lost_supporting_spans_or_qualifiers():
    def one_word_context(name, case, observed):
        if name != ARMS[0]:
            return replace(observed, supporting_span_coverage=0.1, qualifier_fidelity=0.0)
        return observed

    protocol, report = run_variant(one_word_context)
    assert all(
        dict(row.values)["final_context_recall"] == 1
        for row in report.rows
        if "True" in row.case_id
    )
    for decision in assess(protocol, report):
        assert "cold:regression:supporting_span_coverage" in decision.blockers
        assert "warm:regression:qualifier_fidelity" in decision.blockers
        assert decision.approval is None


@pytest.mark.parametrize(
    "field", ["supporting_span_coverage", "qualifier_fidelity", "evidence_judgment_sha256"]
)
def test_unknown_independent_context_judgments_block_promotion(field):
    protocol, report = run_variant(lambda _, case, observed: replace(observed, **{field: None}))
    for decision in assess(protocol, report):
        assert any(
            "unknown:" + ("evidence_judgment" if field == "evidence_judgment_sha256" else field)
            in reason
            for reason in decision.blockers
        )
        assert decision.approval is None


@pytest.mark.parametrize("field", ["unsafe_deliveries", "stale_deliveries"])
@pytest.mark.parametrize("value", [None, 1])
def test_stale_or_unsafe_delivery_blocks_even_when_all_returned_ids_are_allowed(field, value):
    protocol, report = run_variant(lambda _, case, observed: replace(observed, **{field: value}))
    assert all(row.forbidden_hits == 0 for row in report.rows)
    for decision in assess(protocol, report):
        assert any(field + "_or_unknown" in reason for reason in decision.blockers)
        assert decision.approval is None


@pytest.mark.parametrize(
    "field, value",
    [
        ("supporting_span_coverage", True),
        ("qualifier_fidelity", float("nan")),
        ("supporting_span_coverage", 1.1),
        ("unsafe_deliveries", True),
        ("stale_deliveries", -1),
        ("unsafe_deliveries", 257),
    ],
)
def test_new_evidence_and_safety_observations_are_strictly_bounded(field, value):
    with pytest.raises(ValueError):
        ControlledRetrievalObservation(("good",), ("good",), 1.0, False, ZERO, **{field: value})


def test_required_span_annotation_manifest_and_new_limits_must_be_frozen():
    protocol = plan()
    unannotated = replace(protocol, evidence_annotations_sha256=None)
    _, _, report = run(unannotated)
    assert all(
        "missing_frozen_evidence_annotations" in decision.blockers
        for decision in assess(unannotated, report)
    )
    with pytest.raises(ValueError, match="every metric"):
        replace(
            protocol,
            regression_limits=tuple(
                (name, value)
                for name, value in protocol.regression_limits
                if name != "qualifier_fidelity"
            ),
        )
    with pytest.raises(ValueError, match="baseline semantics"):
        replace(protocol, schema="controlled-retrieval-plan/1")
