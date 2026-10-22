"""Frozen, paired QA optimization reports over the existing cost ledger.

Annotations and measurement receipts are supplied by the evaluation host, never
by the answer model. This module checks their consistency, not their authenticity.
It does not execute models or turn offline regression results into acceptance.
"""

from __future__ import annotations

import math
import random
from collections import Counter
from dataclasses import dataclass

from .acceptance import _hash
from .evidence import _digest, _text
from .public_qa_acceptance import summarize_public_qa
from .question_cost import CostSnapshot
from .question_statistics import BootstrapProtocol, _interval


@dataclass(frozen=True, slots=True)
class QACase:
    case_id: str
    group_id: str
    task_kind: str
    answerable: bool

    def __post_init__(self):
        for name in ("case_id", "group_id", "task_kind"):
            _text(getattr(self, name), name)
        if type(self.answerable) is not bool:
            raise ValueError("answerability must be independently annotated")


@dataclass(frozen=True, slots=True)
class QAOptimizationPlan:
    cases: tuple[QACase, ...]
    dataset_sha256: str
    answer_model_sha256: str
    judge_sha256: str
    workload_sha256: str
    bootstrap: BootstrapProtocol
    minimum_accuracy: float
    maximum_technical_failure_rate: float
    maximum_group_accuracy_drop: float
    maximum_cost_ratio: float
    maximum_latency_p95_ratio: float
    minimum_accuracy_gain: float = 0.0
    dataset_kind: str = "synthetic"
    dataset_license_reference: str | None = None
    schema: str = "qa-optimization-plan/1"

    def __post_init__(self):
        if self.schema != "qa-optimization-plan/1":
            raise ValueError("unsupported optimization plan")
        if (
            type(self.cases) is not tuple
            or not 1 <= len(self.cases) <= 100_000
            or any(type(c) is not QACase for c in self.cases)
            or len({c.case_id for c in self.cases}) != len(self.cases)
        ):
            raise ValueError("unique nonempty frozen cases required")
        for name in ("dataset_sha256", "answer_model_sha256", "judge_sha256", "workload_sha256"):
            _hash(getattr(self, name), name)
        if type(self.bootstrap) is not BootstrapProtocol:
            raise ValueError("typed bootstrap protocol required")
        if self.dataset_kind not in {"synthetic", "licensed_real"}:
            raise ValueError("invalid dataset kind")
        if self.dataset_license_reference is not None:
            _text(self.dataset_license_reference, "dataset license reference", 2048)
        for name in (
            "minimum_accuracy",
            "maximum_technical_failure_rate",
            "maximum_group_accuracy_drop",
            "minimum_accuracy_gain",
        ):
            value = getattr(self, name)
            if type(value) not in (int, float) or not 0 <= value <= 1:
                raise ValueError("bounded quality threshold required")
        for name in ("maximum_cost_ratio", "maximum_latency_p95_ratio"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("positive finite ratio required")

    @property
    def fingerprint(self):
        return _digest(self)


@dataclass(frozen=True, slots=True)
class QAObservation:
    case_id: str
    status: str
    correct: bool | None
    answered: bool
    answerable: bool
    exposure: str
    latency_ms: float
    failure_stage: str | None = None
    governance_passed: bool | None = None

    def __post_init__(self):
        _text(self.case_id, "case_id")
        if self.governance_passed is not None and type(self.governance_passed) is not bool:
            raise ValueError("trusted governance annotation required")
        if (
            type(self.latency_ms) not in (int, float)
            or not math.isfinite(self.latency_ms)
            or self.latency_ms < 0
        ):
            raise ValueError("finite nonnegative latency required")
        if self.failure_stage not in {
            None,
            "extraction",
            "retrieval",
            "packing",
            "reasoning",
            "technical",
            "unknown",
        }:
            raise ValueError("invalid failure stage")
        # Reuse the public QA contract rather than accepting a different status vocabulary.
        summarize_public_qa((self.case_id,), (self.public_row(),), CostSnapshot("CNY", (), (), ()))
        if self.effective and self.failure_stage is not None:
            raise ValueError("successful observations cannot claim a failure stage")

    def public_row(self):
        return {
            name: getattr(self, name)
            for name in ("case_id", "status", "correct", "answered", "answerable", "exposure")
        }

    @property
    def effective(self):
        return (
            self.correct is True
            and self.answered == self.answerable
            and self.governance_passed is True
        )


@dataclass(frozen=True, slots=True)
class QAExecutionManifest:
    """Actual host-run bindings, checked separately from the planned fingerprint.

    The host creates this from its execution configuration and dataset receipts;
    it is an attestation, not proof that a provider or evaluator behaved correctly.
    """

    dataset_sha256: str
    answer_model_sha256: str
    judge_sha256: str
    workload_sha256: str

    def __post_init__(self):
        for name in ("dataset_sha256", "answer_model_sha256", "judge_sha256", "workload_sha256"):
            _hash(getattr(self, name), name)


@dataclass(frozen=True, slots=True)
class QAOptimizationRun:
    run_id: str
    plan_sha256: str
    configuration_sha256: str
    execution: str
    observations: tuple[QAObservation, ...]
    costs: CostSnapshot
    local_resources_measured: bool = False
    execution_manifest: QAExecutionManifest | None = None

    def __post_init__(self):
        _text(self.run_id, "run_id")
        _hash(self.plan_sha256, "plan_sha256")
        _hash(self.configuration_sha256, "configuration_sha256")
        if self.execution not in {"offline", "real"}:
            raise ValueError("execution must distinguish offline and real runs")
        if (
            type(self.observations) is not tuple
            or any(type(r) is not QAObservation for r in self.observations)
            or len({r.case_id for r in self.observations}) != len(self.observations)
        ):
            raise ValueError("unique typed observations required")
        if type(self.costs) is not CostSnapshot or type(self.local_resources_measured) is not bool:
            raise ValueError("typed cost snapshot and resource status required")
        if (
            self.execution_manifest is not None
            and type(self.execution_manifest) is not QAExecutionManifest
        ):
            raise ValueError("typed execution manifest required")

    @property
    def fingerprint(self):
        return _digest(self)


def _p95(rows):
    values = sorted(row.latency_ms for row in rows)
    return values[math.ceil(len(values) * 0.95) - 1] if values else None


def _ratio(candidate, baseline):
    if candidate is None or baseline is None:
        return None
    if baseline == 0:
        return 1.0 if candidate == 0 else None
    return candidate / baseline


def compare_qa_optimization(plan, baseline, candidate, *, expected_plan_sha256):
    """Compare all planned cases; missing/failed rows cannot improve the denominator.

    Confidence intervals resample whole frozen groups (e.g. shared histories).
    The external expected fingerprint must be committed before either run.
    A favorable offline report still has accepted=False.
    """
    if type(plan) is not QAOptimizationPlan or plan.fingerprint != expected_plan_sha256:
        raise ValueError("optimization plan changed after freeze")
    ids = tuple(c.case_id for c in plan.cases)
    cases = {c.case_id: c for c in plan.cases}
    reports, reasons, maps = {}, [], []
    if plan.dataset_kind != "licensed_real" or not plan.dataset_license_reference:
        reasons.append("no_licensed_real_dataset")
    if type(baseline) is not QAOptimizationRun or type(candidate) is not QAOptimizationRun:
        raise ValueError("typed paired runs required")
    if baseline.run_id == candidate.run_id:
        raise ValueError("paired arms require distinct run identities")
    if baseline.costs.currency != candidate.costs.currency:
        raise ValueError("incomparable currencies")
    for label, run in (("baseline", baseline), ("candidate", candidate)):
        if type(run) is not QAOptimizationRun or run.plan_sha256 != plan.fingerprint:
            raise ValueError("run is not bound to the frozen plan")
        if run.execution_manifest is None:
            reasons.append(f"{label}:execution_manifest_missing")
        elif any(
            getattr(run.execution_manifest, name) != getattr(plan, name)
            for name in ("dataset_sha256", "answer_model_sha256", "judge_sha256", "workload_sha256")
        ):
            raise ValueError("execution manifest differs from frozen plan")
        if any(
            r.case_id not in cases or r.answerable != cases[r.case_id].answerable
            for r in run.observations
        ):
            raise ValueError("observation differs from frozen case manifest")
        report = summarize_public_qa(
            ids,
            tuple(r.public_row() for r in run.observations),
            run.costs,
            local_resources_measured=run.local_resources_measured,
            minimum_accuracy=plan.minimum_accuracy if label == "candidate" else 0,
            maximum_technical_failure_rate=(
                plan.maximum_technical_failure_rate if label == "candidate" else 1
            ),
        )
        reports[label] = report
        reasons.extend(f"{label}:{gap}" for gap in report["acceptance_gaps"])
        if run.execution != "real":
            reasons.append(f"{label}:offline_execution")
        if any(r.governance_passed is not True for r in run.observations):
            reasons.append(f"{label}:governance_failure")
        if any(e.actual_microunits is None for e in run.costs.entries):
            reasons.append(f"{label}:unsettled_cost")
        maps.append({r.case_id: r for r in run.observations})
        report["failure_stages"] = dict(
            Counter(r.failure_stage or "unknown" for r in run.observations if not r.effective)
        )
        report["latency_p95_ms"] = _p95(run.observations)
        report["report_sha256"] = run.fingerprint
    groups = sorted({c.group_id for c in plan.cases})
    if len(groups) < plan.bootstrap.minimum_groups:
        reasons.append("insufficient_groups")
    if len(ids) < plan.bootstrap.minimum_requests:
        reasons.append("insufficient_requests")
    # Work with group totals so repeated questions from one history are not independent samples.
    totals = {group: [0, 0, 0] for group in groups}
    kinds = {}
    for case in plan.cases:
        outcomes = [int(case.case_id in rows and rows[case.case_id].effective) for rows in maps]
        group = totals[case.group_id]
        kind = kinds.setdefault(case.task_kind, [0, 0, 0])
        for row in (group, kind):
            row[0] += 1
            row[1] += outcomes[0]
            row[2] += outcomes[1]
    by_kind = {}
    for kind, (count, before, after) in sorted(kinds.items()):
        by_kind[kind] = {
            "planned": count,
            "baseline_effective_per_planned": before / count,
            "candidate_effective_per_planned": after / count,
            "difference": (after - before) / count,
        }
        if (before - after) / count > plan.maximum_group_accuracy_drop:
            reasons.append(f"task_kind_regression:{kind}")
    rng = random.Random(plan.bootstrap.seed)
    draws = []
    for _ in range(plan.bootstrap.resamples):
        sample = [totals[groups[rng.randrange(len(groups))]] for _ in groups]
        draws.append(sum(row[2] - row[1] for row in sample) / sum(row[0] for row in sample))
    interval = _interval(draws, plan.bootstrap.confidence_level)
    if interval[0] < plan.minimum_accuracy_gain:
        reasons.append("quality_gain_not_established")
    cost_ratio = _ratio(
        reports["candidate"]["cost"]["accounted_upper_microunits"],
        reports["baseline"]["cost"]["accounted_upper_microunits"],
    )
    latency_ratio = _ratio(
        reports["candidate"]["latency_p95_ms"], reports["baseline"]["latency_p95_ms"]
    )
    if cost_ratio is None or cost_ratio > plan.maximum_cost_ratio:
        reasons.append("cost_ratio_threshold_not_met")
    if latency_ratio is None or latency_ratio > plan.maximum_latency_p95_ratio:
        reasons.append("latency_ratio_threshold_not_met")
    return {
        "schema": "qa-optimization-comparison/1",
        "plan_sha256": plan.fingerprint,
        "accepted": not reasons,
        "acceptance_gaps": reasons,
        **reports,
        "task_kinds": by_kind,
        "paired": {
            "groups": len(groups),
            "planned": len(ids),
            "effective_per_planned_difference": sum(row[2] - row[1] for row in totals.values())
            / len(ids),
            "confidence_interval": interval,
            "confidence_level": plan.bootstrap.confidence_level,
            "cost_upper_ratio": cost_ratio,
            "latency_p95_ratio": latency_ratio,
        },
        "boundary": "Trusted host annotations. Offline/development/partial runs are diagnostic. "
        "A plan hash binds settings but does not prove annotation independence.",
    }
