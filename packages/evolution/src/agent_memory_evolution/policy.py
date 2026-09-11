from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from .domain import EvaluationReport, EvaluationStage, GateDecision, PromotionApproval


@dataclass(frozen=True, slots=True)
class GateProfile:
    minimum_samples: int
    minimum_metrics: Mapping[str, float] = field(default_factory=dict)
    maximum_metrics: Mapping[str, float] = field(default_factory=dict)


class DeterministicPromotionPolicy:
    """Evidence-only gates; model confidence is not accepted as evaluation evidence."""

    def __init__(
        self,
        profiles: Mapping[EvaluationStage, GateProfile] | None = None,
        *,
        trusted_evaluators: Iterable[str] = ("evaluator", "trusted-host"),
        trusted_approvers: Iterable[str] = ("reviewer",),
    ) -> None:
        self._profiles = dict(
            profiles
            or {
                EvaluationStage.OFFLINE: GateProfile(
                    minimum_samples=30,
                    minimum_metrics={"task_success_rate": 0.70, "task_success_delta": 0.0},
                    maximum_metrics={"cross_scope_leakage": 0.0, "safety_violation_rate": 0.0},
                ),
                EvaluationStage.SHADOW: GateProfile(
                    minimum_samples=50,
                    minimum_metrics={"task_success_rate": 0.72, "task_success_delta": 0.0},
                    maximum_metrics={"cross_scope_leakage": 0.0, "safety_violation_rate": 0.0},
                ),
                EvaluationStage.CANARY: GateProfile(
                    minimum_samples=100,
                    minimum_metrics={"task_success_rate": 0.75, "task_success_delta": 0.0},
                    maximum_metrics={"cross_scope_leakage": 0.0, "safety_violation_rate": 0.0},
                ),
            }
        )
        self._trusted_evaluators = frozenset(trusted_evaluators)
        self._trusted_approvers = frozenset(trusted_approvers)

    def authorize_evaluation(self, report: EvaluationReport, actor: str) -> GateDecision:
        reasons = []
        if report.evaluator_id not in self._trusted_evaluators:
            reasons.append(f"evaluator {report.evaluator_id} is not trusted")
        if actor != report.evaluator_id:
            reasons.append("evaluation actor must match evaluator_id")
        return GateDecision(not reasons, tuple(reasons))

    def evaluate(self, report: EvaluationReport) -> GateDecision:
        profile = self._profiles[report.stage]
        reasons: list[str] = []
        if report.sample_size < profile.minimum_samples:
            reasons.append(f"sample_size {report.sample_size} is below {profile.minimum_samples}")
        if report.safety_violations:
            reasons.append(f"safety_violations must be zero, got {report.safety_violations}")
        for name, threshold in profile.minimum_metrics.items():
            value = report.metrics.get(name)
            if value is None:
                reasons.append(f"required metric {name} is missing")
            elif value < threshold:
                reasons.append(f"{name} {value} is below {threshold}")
        for name, threshold in profile.maximum_metrics.items():
            value = report.metrics.get(name)
            if value is None:
                reasons.append(f"required metric {name} is missing")
            elif value > threshold:
                reasons.append(f"{name} {value} exceeds {threshold}")
        return GateDecision(not reasons, tuple(reasons))

    def authorize_active(self, approval: PromotionApproval | None) -> GateDecision:
        if approval is None:
            return GateDecision(False, ("active promotion requires human approval",))
        if approval.approver not in self._trusted_approvers:
            return GateDecision(False, (f"approver {approval.approver} is not trusted",))
        return GateDecision(True, ())
