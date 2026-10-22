"""Frozen synthetic/offline comparison of one recall wave and bounded refills.

Run: PYTHONPATH=src python examples/evidence_research_comparison.py
This exercises the actual SQLite research loop with a deterministic fixture reader.
Wall time covers answer() only. CPU/I/O/storage and total costs remain unmeasured;
there are no provider/model calls. Development results cannot establish acceptance.
"""

import asyncio
import inspect
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter_ns

from agent_memory.domain import MemoryEvent, MemoryQuery, MemoryScope
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
from agent_memory.retrieval.evidence_research import (
    EvidenceGap,
    EvidenceRequirement,
    EvidenceUse,
    QueryContract,
    ReaderAssessment,
    ResearchLimits,
)
from agent_memory.retrieval.evidence_research_adapters import sqlite_research
from agent_memory.retrieval.evidence_research_contracts import digest


@dataclass(frozen=True)
class FixtureCase:
    id: str
    query: str
    refill: str
    sources: tuple[str, str]
    expected: str


FIXTURES = (
    FixtureCase(
        "violet",
        "deployment",
        "violet specialist",
        (
            "Deployment briefing references violet.",
            "Violet specialist is Mei.",
        ),
        "Mei.",
    ),
    FixtureCase(
        "amber",
        "rollout",
        "amber specialist",
        (
            "Rollout briefing references amber.",
            "Amber specialist is Luis.",
        ),
        "Luis.",
    ),
)
SOURCE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


async def fixture_reader(contract, records, allowance):
    # Reader receives no gold/case ID or old result. This is deliberately a
    # narrow synthetic parser; its behavior is not a model-quality estimate.
    for record in records:
        if " specialist is " in record.text:
            answer = record.text.split(" specialist is ", 1)[1].strip()
            return ReaderAssessment(answer, (EvidenceUse("owner", (record.key,)),))
    return ReaderAssessment(gaps=(EvidenceGap("owner", "native"),))


def fixture_judge(result, expected):
    return result.status == "answered" and result.answer == expected


async def run_arm(root, name, rounds, plan, manifest):
    observations, diagnostics = [], []
    limits = ResearchLimits(max_rounds=rounds)
    async with sqlite_research(root / f"{name}.db", fixture_reader) as loop:
        for case in FIXTURES:
            scope = MemoryScope("offline-comparison", session_id=case.id)
            for number, content in enumerate(case.sources):
                await loop.backend.kernel.ingest_event(
                    MemoryEvent(
                        scope,
                        "message",
                        content,
                        id=f"{case.id}:{number}",
                        occurred_at=SOURCE_TIME,
                        ingested_at=SOURCE_TIME,
                    )
                )
            contract = QueryContract(
                MemoryQuery(scope, case.query, token_budget=2048, include_current_state=False),
                (EvidenceRequirement("owner", case.refill),),
                limits=limits,
            )
            started = perf_counter_ns()
            result = await loop.answer(contract)
            elapsed_ms = (perf_counter_ns() - started) / 1_000_000
            correct = fixture_judge(result, case.expected)
            # A narrow, observed delivery check; semantic truth is judged above
            # by this fixture's exact expected string, not by model self-report.
            delivery_checked = (
                result.status == "answered"
                and any(e.stage == "delivery" and e.outcome == "validated" for e in result.events)
            ) or (result.status == "partial" and result.answer is None and result.evidence == ())
            observations.append(
                QAObservation(
                    case.id,
                    "completed",
                    correct,
                    result.status == "answered",
                    True,
                    "development",
                    elapsed_ms,
                    None if correct else "retrieval",
                    governance_passed=delivery_checked,
                )
            )
            diagnostics.append(
                {
                    "case_id": case.id,
                    "status": result.status,
                    "answer": result.answer,
                    "reason": result.reason,
                    "calls": result.calls,
                    "latency_ms": elapsed_ms,
                }
            )
    # Unknown local resource cost is intentionally unpriced, with no fabricated
    # tariff, finite estimate, billed zero or whole-workload measurement claim.
    costs = CostSnapshot(
        "CNY",
        (
            CostEntry(
                f"{name}:unmeasured-local-work",
                CostPhase.FOREGROUND,
                request_ids=tuple(c.id for c in FIXTURES),
            ),
        ),
        (),
        (CostPhase.FOREGROUND,),
    )
    run = QAOptimizationRun(
        name,
        plan.fingerprint,
        digest(asdict(limits)),
        "offline",
        tuple(observations),
        costs,
        local_resources_measured=False,
        execution_manifest=manifest,
    )
    return run, diagnostics


async def main():
    manifest = QAExecutionManifest(
        digest({"cases": tuple(asdict(c) for c in FIXTURES), "source_time": SOURCE_TIME}),
        digest(inspect.getsource(fixture_reader)),
        digest(inspect.getsource(fixture_judge)),
        digest(tuple((c.id, c.query, c.refill) for c in FIXTURES)),
    )
    plan = QAOptimizationPlan(
        tuple(QACase(c.id, c.id, "fact", True) for c in FIXTURES),
        manifest.dataset_sha256,
        manifest.answer_model_sha256,
        manifest.judge_sha256,
        manifest.workload_sha256,
        BootstrapProtocol(7, 200, 0.95, 2, 2),
        minimum_accuracy=0.8,
        maximum_technical_failure_rate=0.02,
        maximum_group_accuracy_drop=0,
        maximum_cost_ratio=1,
        maximum_latency_p95_ratio=1.2,
        dataset_kind="synthetic",
    )
    frozen_plan_sha256 = plan.fingerprint  # Freeze before either arm executes.
    with TemporaryDirectory(prefix="research-comparison-") as directory:
        root = Path(directory)
        baseline, before = await run_arm(root, "baseline", 1, plan, manifest)
        candidate, after = await run_arm(root, "candidate", 3, plan, manifest)
    report = compare_qa_optimization(
        plan,
        baseline,
        candidate,
        expected_plan_sha256=frozen_plan_sha256,
    )
    assert report["accepted"] is False
    report.update(
        fixture=True,
        execution="offline",
        exposure="development",
        paid_calls=0,
        timing_scope="answer() wall time; setup excluded; two synthetic cases only",
        local_resources="unknown",
        case_results={"baseline": before, "candidate": after},
        interpretation=(
            "Synthetic mechanism demonstration; no generalization or cost improvement claim."
        ),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
