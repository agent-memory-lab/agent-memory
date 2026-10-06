"""Frozen quality gates, additive to the existing operational release checks."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

from .evidence import SCORER_VERSION, EvidenceEvaluationReport, _digest, _finite, _strings, _text
from .release import ReleaseCheckKind, ReleaseCheckOutcome, ReleaseCheckStatus


def _hash(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class QualityThresholds:
    minimum_evidence_recall: float
    minimum_support_rate: float
    minimum_correct_answer_rate: float
    minimum_answer_coverage: float
    maximum_failure_rate: float
    maximum_recall_drop: float
    maximum_correct_answer_drop: float
    maximum_mean_tokens: float
    maximum_total_cost: float
    maximum_latency_p95_ms: float

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = _finite(getattr(self, name), name)
            if name not in {"maximum_mean_tokens", "maximum_total_cost", "maximum_latency_p95_ms"}:
                if value > 1:
                    raise ValueError(f"{name} must be between 0 and 1")
            object.__setattr__(self, name, value)


@dataclass(frozen=True, slots=True)
class AcceptanceProfile:
    profile_id: str
    version: str
    capabilities: tuple[str, ...]
    dataset_sha256: str
    candidate_configuration_sha256: str
    baseline_report_sha256: str
    minimum_cases: int
    minimum_answerable_cases: int
    thresholds: QualityThresholds | None
    calibration_reference: str | None
    scorer_version: str = SCORER_VERSION

    def __post_init__(self) -> None:
        for name in ("profile_id", "version", "scorer_version"):
            _text(getattr(self, name), name)
        capabilities = _strings(self.capabilities, "capabilities")
        if not capabilities:
            raise ValueError("a profile must name its capability slice")
        object.__setattr__(self, "capabilities", capabilities)
        for name in ("dataset_sha256", "candidate_configuration_sha256", "baseline_report_sha256"):
            _hash(getattr(self, name), name)
        for name in ("minimum_cases", "minimum_answerable_cases"):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 10_000:
                raise ValueError(f"{name} must be between 1 and 10000")
        if self.minimum_answerable_cases > self.minimum_cases:
            raise ValueError("minimum answerable cases cannot exceed minimum cases")
        if self.thresholds is not None and not isinstance(self.thresholds, QualityThresholds):
            raise TypeError("thresholds must be QualityThresholds or None")
        if self.calibration_reference is not None:
            _text(self.calibration_reference, "calibration_reference", 2048)

    @property
    def fingerprint(self) -> str:
        return _digest(self)


def evidence_metrics(report: EvidenceEvaluationReport) -> dict[str, float | int]:
    cases = [case for case in report.results if not case.diagnostic_only]
    answerable = [case for case in cases if case.answerable]
    count = len(cases)
    a_count = len(answerable)
    latencies = sorted(case.latency_ms for case in report.results)
    return {
        "case_count": count,
        "answerable_count": a_count,
        "evidence_recall": sum(case.evidence_recall or 0.0 for case in answerable) / a_count
        if a_count
        else 0.0,
        "support_rate": sum(case.support_complete for case in answerable) / a_count
        if a_count
        else 0.0,
        "correct_answer_rate": sum(case.outcome == "correct_answer" for case in answerable)
        / a_count
        if a_count
        else 0.0,
        "answer_coverage": sum(
            case.outcome in {"correct_answer", "incorrect_answer", "unjudged_answer"}
            for case in answerable
        )
        / a_count
        if a_count
        else 0.0,
        "failure_rate": sum(case.outcome == "system_error" for case in cases) / count
        if count
        else 1.0,
        "forbidden_hits": sum(len(case.forbidden_hits) for case in report.results),
        "unknown_evidence_hits": sum(len(case.unknown_evidence_ids) for case in report.results),
        "unjudged_answers": sum(case.outcome == "unjudged_answer" for case in cases),
        "unanswerable_answers": sum(
            case.outcome == "incorrect_answer" and not case.answerable for case in cases
        ),
        "diagnostic_errors": sum(
            case.diagnostic_only and case.outcome == "system_error" for case in report.results
        ),
        # Diagnostics stay out of answer accuracy, but not out of cost or leakage.
        "mean_tokens": sum(case.tokens for case in report.results) / len(report.results)
        if report.results
        else 0.0,
        "total_cost": sum(case.cost for case in report.results),
        "latency_p95_ms": latencies[max(0, math.ceil(len(latencies) * 0.95) - 1)]
        if latencies
        else 0.0,
    }


@dataclass(frozen=True, slots=True)
class QualityGateResult:
    profile_sha256: str
    candidate_report_sha256: str
    ready: bool
    reasons: tuple[str, ...]
    capabilities: tuple[str, ...]

    def release_outcome(self, *, check_id: str = "evidence-quality") -> ReleaseCheckOutcome:
        """A REPLAY check only; it cannot certify all operational release kinds."""
        return ReleaseCheckOutcome(
            check_id=check_id,
            kind=ReleaseCheckKind.REPLAY,
            status=ReleaseCheckStatus.PASS if self.ready else ReleaseCheckStatus.FAIL,
            summary="Frozen evidence quality gate passed"
            if self.ready
            else "; ".join(self.reasons),
            failures=0 if self.ready else len(self.reasons),
            subjects=self.capabilities,
            artifact_uri=f"sha256:{self.candidate_report_sha256}",
        )


def evaluate_quality_gate(
    profile: AcceptanceProfile,
    candidate: EvidenceEvaluationReport,
    baseline: EvidenceEvaluationReport,
    *,
    expected_profile_sha256: str,
) -> QualityGateResult:
    """The host supplies a previously frozen profile digest, outside the run."""
    if not isinstance(profile, AcceptanceProfile):
        raise TypeError("profile must be an AcceptanceProfile")
    if any(not isinstance(report, EvidenceEvaluationReport) for report in (candidate, baseline)):
        raise TypeError("reports must be EvidenceEvaluationReport instances")
    _hash(expected_profile_sha256, "expected_profile_sha256")
    reasons: list[str] = []
    if profile.fingerprint != expected_profile_sha256:
        reasons.append("profile_changed_after_freeze")
    if profile.thresholds is None or profile.calibration_reference is None:
        reasons.append("profile_uncalibrated")
    if profile.scorer_version != SCORER_VERSION:
        reasons.append("unsupported_scorer_version")
    for label, report in (("candidate", candidate), ("baseline", baseline)):
        if report.dataset_sha256 != profile.dataset_sha256:
            reasons.append(f"{label}_dataset_mismatch")
        if report.scorer_version != profile.scorer_version:
            reasons.append(f"{label}_scorer_mismatch")
        ids = [case.case_id for case in report.results]
        if len(ids) != len(set(ids)) or not ids:
            reasons.append(f"{label}_invalid_case_manifest")
    if {(r.case_id, r.answerable, r.diagnostic_only) for r in candidate.results} != {
        (r.case_id, r.answerable, r.diagnostic_only) for r in baseline.results
    }:
        reasons.append("case_manifest_mismatch")
    if baseline.fingerprint != profile.baseline_report_sha256:
        reasons.append("baseline_report_mismatch")
    if candidate.run_configuration_sha256 != profile.candidate_configuration_sha256:
        reasons.append("candidate_configuration_mismatch")
    metrics = evidence_metrics(candidate)
    baseline_metrics = evidence_metrics(baseline)
    if metrics["case_count"] < profile.minimum_cases:
        reasons.append("insufficient_cases")
    if metrics["answerable_count"] < profile.minimum_answerable_cases:
        reasons.append("insufficient_answerable_cases")
    if metrics["answer_coverage"] == 0:
        reasons.append("no_answer_coverage")
    for name in (
        "forbidden_hits",
        "unknown_evidence_hits",
        "unjudged_answers",
        "unanswerable_answers",
        "diagnostic_errors",
    ):
        if metrics[name]:
            reasons.append(name)
    thresholds = profile.thresholds
    if thresholds is not None:
        for name in ("evidence_recall", "support_rate", "correct_answer_rate", "answer_coverage"):
            if metrics[name] < getattr(thresholds, f"minimum_{name}"):
                reasons.append(f"{name}_below_floor")
        for name in ("failure_rate", "mean_tokens", "total_cost", "latency_p95_ms"):
            if metrics[name] > getattr(thresholds, f"maximum_{name}"):
                reasons.append(f"{name}_over_limit")
        for name, limit in (
            ("evidence_recall", thresholds.maximum_recall_drop),
            ("correct_answer_rate", thresholds.maximum_correct_answer_drop),
        ):
            if baseline_metrics[name] - metrics[name] > limit + 1e-12:
                reasons.append(f"{name}_regressed")
    return QualityGateResult(
        profile.fingerprint,
        candidate.fingerprint,
        not reasons,
        tuple(reasons),
        profile.capabilities,
    )


def acceptance_profile_from_dict(data: Mapping) -> AcceptanceProfile:
    if not isinstance(data, Mapping):
        raise ValueError("acceptance profile must be an object")
    values = dict(data)
    if values.get("thresholds") is not None:
        if not isinstance(values["thresholds"], Mapping):
            raise ValueError("thresholds must be an object or null")
        values["thresholds"] = QualityThresholds(**values["thresholds"])
    return AcceptanceProfile(**values)
