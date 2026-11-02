"""Acceptance tests for deterministic retrieval policy contracts, not retrieval quality."""

import asyncio

import pytest

from agent_memory.domain import MemoryScope
from agent_memory.retrieval_evaluation import (
    NoMemoryEvaluationArm,
    RetrievalDatasetSnapshot,
    RetrievalEvalCase,
    RetrievalObservation,
    run_retrieval_benchmark,
)


SCOPE = MemoryScope("tenant-a", session_id="session-a")


class FixedArm:
    def __init__(self, name, observations, *, fail_on=()):
        self._name = name
        self.observations = observations
        self.fail_on = set(fail_on)

    @property
    def name(self):
        return self._name

    async def retrieve(self, case):
        if case.query_text in self.fail_on:
            raise RuntimeError("offline")
        return self.observations[case.query_text]


def snapshot():
    return RetrievalDatasetSnapshot(
        "snapshot-1",
        "v1",
        (
            RetrievalEvalCase(
                "case-b",
                SCOPE,
                "beta",
                ("memory-b",),
                ("event-b",),
                ("forbidden-b",),
            ),
            RetrievalEvalCase(
                "case-a",
                SCOPE,
                "alpha",
                ("memory-a",),
                ("event-a",),
                ("forbidden-a",),
            ),
        ),
    )


def test_three_arm_report_tracks_quality_cost_failures_and_forbidden_hits():
    lexical = FixedArm(
        "lexical-only",
        {
            "alpha": RetrievalObservation(("memory-a",), ("event-a",), 10),
            "beta": RetrievalObservation(("forbidden-b",), (), 20),
        },
    )
    hybrid = FixedArm(
        "hybrid",
        {
            "alpha": RetrievalObservation(("memory-a",), ("event-a",), 12),
            "beta": RetrievalObservation(("memory-b",), ("event-b",), 12),
        },
        fail_on=("beta",),
    )

    report = asyncio.run(
        run_retrieval_benchmark(snapshot(), (hybrid, NoMemoryEvaluationArm(), lexical))
    )
    metrics = {value.arm: value for value in report.metrics}

    assert [value.arm for value in report.metrics] == ["hybrid", "lexical-only", "no-memory"]
    assert metrics["lexical-only"].precision == 0.5
    assert metrics["lexical-only"].recall == 0.5
    assert metrics["lexical-only"].evidence_coverage == 0.5
    assert metrics["lexical-only"].forbidden_hits == 1
    assert metrics["lexical-only"].mean_tokens == 15
    assert metrics["hybrid"].failed_cases == 1
    assert metrics["hybrid"].failure_rate == 0.5
    assert metrics["hybrid"].relevant_count == 2
    assert metrics["hybrid"].recall == 0.5
    assert report.failures[0].case_id == "case-b"
    assert report.failures[0].error_type == "RuntimeError"
    assert metrics["no-memory"].recall == 0


def test_benchmark_requires_exactly_the_three_comparison_arms():
    with pytest.raises(ValueError, match="exactly"):
        asyncio.run(
            run_retrieval_benchmark(
                snapshot(),
                (NoMemoryEvaluationArm(),),
            )
        )


def test_snapshot_rejects_relevant_forbidden_overlap_and_duplicate_cases():
    with pytest.raises(ValueError, match="disjoint"):
        RetrievalEvalCase("overlap", SCOPE, "query", ("same",), (), ("same",))

    case = RetrievalEvalCase("duplicate", SCOPE, "query", ("memory",))
    with pytest.raises(ValueError, match="unique"):
        RetrievalDatasetSnapshot("snapshot", "v1", (case, case))


def test_arm_input_has_only_query_scope_and_neutral_id_and_gold_echo_fails():
    from dataclasses import FrozenInstanceError, fields, replace

    from agent_memory import RetrievalEvaluationInput

    seen = []

    class InspectingArm:
        name = "inspect"

        async def retrieve(self, request):
            assert type(request) is RetrievalEvaluationInput
            assert {field.name for field in fields(request)} == {"case_id", "scope", "query_text"}
            assert not hasattr(request, "__dict__")
            with pytest.raises(FrozenInstanceError):
                request.query_text = "mutated"
            seen.append(request)
            return RetrievalObservation(("memory-a",), ("event-a",), 10)

    annotated = RetrievalEvalCase(
        "gold-answer-private", SCOPE, "alpha", ("memory-a",), ("event-a",)
    )
    first = RetrievalDatasetSnapshot("isolation", "1", (annotated,))
    second = replace(first, cases=(replace(annotated, relevant_memory_ids=("other",)),))
    reports = [
        asyncio.run(run_retrieval_benchmark(data, (InspectingArm(),), required_arms=("inspect",)))
        for data in (first, second)
    ]
    assert seen[0] == seen[1] == RetrievalEvaluationInput("case-000000", SCOPE, "alpha")
    assert reports[0].metrics[0].recall == 1
    assert reports[1].metrics[0].recall == 0
    assert reports[0].metrics[0].evidence_coverage == 1

    class GoldEchoArm:
        name = "oracle"

        async def retrieve(self, request):
            return RetrievalObservation(
                request.relevant_memory_ids, request.required_source_event_ids, 0
            )

    report = asyncio.run(
        run_retrieval_benchmark(first, (GoldEchoArm(),), required_arms=("oracle",))
    )
    assert report.metrics[0].recall == 0
    assert report.failures[0].error_type == "AttributeError"
    assert report.failures[0].case_id == "gold-answer-private"
