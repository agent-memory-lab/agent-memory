"""T41 benchmark harness acceptance tests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

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
    load_benchmark_dataset,
    run_memory_benchmark,
)
from agent_memory.domain import MemoryScope


NOW = datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)
SCOPE = MemoryScope("tenant", session_id="benchmark")


def _dataset() -> MemoryBenchmarkDataset:
    cases = tuple(
        MemoryBenchmarkCase(
            case_id=f"case-{dimension.value}",
            scope=SCOPE,
            query_text=f"query for {dimension.value}",
            dimension=dimension,
            relevant_memory_ids=(f"{dimension.value}-1", f"{dimension.value}-2"),
            required_source_event_ids=(f"source-{dimension.value}",),
            forbidden_memory_ids=(f"forbidden-{dimension.value}",),
            expected_order=(f"{dimension.value}-1", f"{dimension.value}-2"),
        )
        for dimension in MemoryBenchmarkDimension
    )
    return MemoryBenchmarkDataset(
        dataset_id="memory-eval",
        version="1.0.0",
        license="CC-BY-4.0",
        cases=cases,
    )


def _observation(case: MemoryBenchmarkCase, *, complete: bool):
    ids = case.relevant_memory_ids if complete else case.relevant_memory_ids[:1]
    return MemoryBenchmarkObservation(
        memory_ids=ids,
        source_event_ids=case.required_source_event_ids if complete else (),
        token_estimate=24 if complete else 12,
    )


def test_three_arm_benchmark_scores_all_memory_dimensions_with_confidence():
    async def scenario():
        async def fixed(case):
            return _observation(case, complete=False)

        async def candidate(case):
            return _observation(case, complete=True)

        report = await run_memory_benchmark(
            _dataset(),
            (
                CallableMemoryBenchmarkArm("candidate-policy", "2.0.0", candidate),
                NoMemoryBenchmarkArm(),
                CallableMemoryBenchmarkArm("fixed-policy", "1.0.0", fixed),
            ),
            MemoryBenchmarkConfig(
                fixed_clock=NOW,
                harness_version="1.0.0",
                plugin_versions={"core": "0.3.0", "retriever": "1.4.0"},
            ),
        )

        assert report.case_count == 6
        assert tuple(metric.arm for metric in report.arms) == (
            "candidate-policy",
            "fixed-policy",
            "no-memory",
        )
        candidate_metrics = report.arms[0]
        assert candidate_metrics.completed_cases == 6
        assert candidate_metrics.successful_cases == 6
        assert candidate_metrics.success_confidence_low > 0
        assert {item.dimension for item in candidate_metrics.dimensions} == set(
            MemoryBenchmarkDimension
        )
        assert candidate_metrics.mean_score > report.arms[1].mean_score
        assert report.arms[2].mean_score == 0
        assert report.comparisons[0].comparable_cases == 6
        assert report.comparisons[0].mean_score_delta > 0

    asyncio.run(scenario())


def test_failures_skips_and_incomparable_reasons_are_never_hidden():
    async def scenario():
        dataset = MemoryBenchmarkDataset(
            dataset_id="small",
            version="1",
            license="internal",
            cases=_dataset().cases[:2],
        )

        async def fixed(case):
            if case.case_id.endswith("temporal"):
                raise RuntimeError("backend unavailable")
            return _observation(case, complete=False)

        async def candidate(case):
            if case.case_id.endswith("temporal"):
                raise BenchmarkSkip("prospective channel disabled")
            return _observation(case, complete=True)

        report = await run_memory_benchmark(
            dataset,
            (
                NoMemoryBenchmarkArm(),
                CallableMemoryBenchmarkArm("fixed-policy", "1", fixed),
                CallableMemoryBenchmarkArm("candidate-policy", "2", candidate),
            ),
            MemoryBenchmarkConfig(NOW, "1", {"core": "0.3.0"}),
        )

        assert len(report.failures) == 1
        assert report.failures[0].error_type == "RuntimeError"
        assert len(report.skips) == 1
        assert report.skips[0].reason == "prospective channel disabled"
        candidate_vs_fixed = next(
            item for item in report.comparisons if item.baseline_arm == "fixed-policy"
        )
        assert candidate_vs_fixed.comparable_cases == 1
        assert candidate_vs_fixed.incomparable_reason == "fewer than 2 comparable cases"
        assert candidate_vs_fixed.confidence_low is None

    asyncio.run(scenario())


def test_machine_and_human_reports_include_versions_counts_and_exceptions():
    async def scenario():
        async def fixed(case):
            return _observation(case, complete=False)

        async def candidate(case):
            return _observation(case, complete=True)

        report = await run_memory_benchmark(
            _dataset(),
            (
                NoMemoryBenchmarkArm(),
                CallableMemoryBenchmarkArm("fixed-policy", "1", fixed),
                CallableMemoryBenchmarkArm("candidate-policy", "2", candidate),
            ),
            MemoryBenchmarkConfig(NOW, "1.0.0", {"core": "0.3.0"}),
        )
        machine = report.to_json()
        human = report.to_markdown()

        assert '"dataset_id":"memory-eval"' in machine
        assert '"sample_count":6' in machine
        assert '"success_confidence_low"' in machine
        assert "# Agent Memory Benchmark" in human
        assert "candidate-policy" in human
        assert "Failures: 0" in human
        assert "Skipped: 0" in human
        assert "Plugin versions" in human

    asyncio.run(scenario())


def test_external_dataset_adapter_contract_and_dataset_validation():
    class Adapter:
        async def load(self):
            return _dataset()

    loaded = asyncio.run(load_benchmark_dataset(Adapter()))
    assert loaded.dataset_id == "memory-eval"

    case = _dataset().cases[0]
    with pytest.raises(ValueError, match="disjoint"):
        MemoryBenchmarkCase(
            "invalid",
            SCOPE,
            "query",
            MemoryBenchmarkDimension.FACTUAL,
            ("same",),
            forbidden_memory_ids=("same",),
        )
    with pytest.raises(ValueError, match="unique"):
        MemoryBenchmarkDataset("duplicate", "1", "internal", (case, case))

    class InvalidAdapter:
        async def load(self):
            return {"bundled": "data"}

    with pytest.raises(TypeError, match="MemoryBenchmarkDataset"):
        asyncio.run(load_benchmark_dataset(InvalidAdapter()))
