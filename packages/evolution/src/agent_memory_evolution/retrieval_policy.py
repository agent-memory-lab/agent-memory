from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from uuid import NAMESPACE_URL, uuid5

from agent_memory.domain import MemoryChannel, MemoryScope

from ._numbers import require_count, require_finite


@dataclass(frozen=True, slots=True)
class RetrievalPolicyCandidate:
    scope: MemoryScope
    baseline_version: str
    version: str
    channel_weights: Mapping[MemoryChannel, float]
    state_budget_ratio: float
    token_budget: int
    baseline_token_budget: int
    id: str = field(init=False)
    def __post_init__(self) -> None:
        self.validate_numbers()
        if not self.baseline_version.strip() or not self.version.strip():
            raise ValueError("policy versions are required")
        if not self.channel_weights or any(value < 0 for value in self.channel_weights.values()):
            raise ValueError("channel weights must be present and non-negative")
        if sum(self.channel_weights.values()) <= 0:
            raise ValueError("at least one channel weight must be positive")
        if not 0.0 <= self.state_budget_ratio <= 1.0:
            raise ValueError("state_budget_ratio must be between zero and one")
        if self.token_budget < 64 or self.token_budget > self.baseline_token_budget:
            raise ValueError("candidate cannot expand the baseline token budget")
        identity = ":".join(
            (
                self.scope.partition_key(),
                self.baseline_version,
                self.version,
                str(sorted((key.value, value) for key, value in self.channel_weights.items())),
                str(self.state_budget_ratio),
                str(self.token_budget),
            )
        )
        object.__setattr__(self, "id", str(uuid5(NAMESPACE_URL, identity)))

    def validate_numbers(self) -> None:
        for value in self.channel_weights.values():
            require_finite(value, "channel weight")
        require_finite(sum(self.channel_weights.values()), "total channel weight")
        require_finite(self.state_budget_ratio, "state_budget_ratio")
        require_count(self.token_budget, "token_budget")
        require_count(self.baseline_token_budget, "baseline_token_budget")


@dataclass(frozen=True, slots=True)
class RetrievalReplaySample:
    scope: MemoryScope
    feedback_version: str
    baseline_utility: float
    candidate_utility: float
    baseline_tokens: int
    candidate_tokens: int
    cross_scope_leakage: bool = False
    def __post_init__(self) -> None:
        self.validate_numbers()
    def validate_numbers(self) -> None:
        require_finite(self.baseline_utility, "baseline_utility")
        require_finite(self.candidate_utility, "candidate_utility")
        require_count(self.baseline_tokens, "baseline_tokens")
        require_count(self.candidate_tokens, "candidate_tokens")


@dataclass(frozen=True, slots=True)
class RetrievalPolicyReport:
    candidate_id: str
    sample_size: int
    feedback_version: str
    baseline_utility: float
    candidate_utility: float
    baseline_mean_tokens: float
    candidate_mean_tokens: float
    passed: bool
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        self.validate_numbers()
    def validate_numbers(self) -> None:
        require_count(self.sample_size, "sample_size")
        for name in (
            "baseline_utility", "candidate_utility", "baseline_mean_tokens", "candidate_mean_tokens"
        ):
            require_finite(getattr(self, name), name)
        if self.baseline_mean_tokens < 0 or self.candidate_mean_tokens < 0:
            raise ValueError("mean tokens cannot be negative")


class DeterministicRetrievalPolicyEvaluator:
    def __init__(self, *, minimum_samples: int = 30) -> None:
        require_count(minimum_samples, "minimum_samples", minimum=1)
        self._minimum_samples = minimum_samples

    def evaluate(
        self,
        candidate: RetrievalPolicyCandidate,
        samples: Sequence[RetrievalReplaySample],
    ) -> RetrievalPolicyReport:
        candidate.validate_numbers()
        for sample in samples:
            sample.validate_numbers()
        reasons: list[str] = []
        partition = candidate.scope.partition_key()
        versions = {sample.feedback_version for sample in samples}
        if len(samples) < self._minimum_samples:
            reasons.append("insufficient replay samples; keep baseline")
        if any(sample.scope.partition_key() != partition for sample in samples):
            reasons.append("replay sample scope mismatch")
        if len(versions) != 1:
            reasons.append("replay samples require one feedback version")
        if any(sample.cross_scope_leakage for sample in samples):
            reasons.append("cross-scope leakage detected")
        baseline_utility = self._mean(sample.baseline_utility for sample in samples)
        candidate_utility = self._mean(sample.candidate_utility for sample in samples)
        baseline_tokens = self._mean(float(sample.baseline_tokens) for sample in samples)
        candidate_tokens = self._mean(float(sample.candidate_tokens) for sample in samples)
        if samples and candidate_utility < baseline_utility:
            reasons.append("candidate utility is below baseline")
        if any(sample.candidate_tokens > candidate.token_budget for sample in samples):
            reasons.append("candidate exceeded its token budget")
        return RetrievalPolicyReport(
            candidate_id=candidate.id,
            sample_size=len(samples),
            feedback_version=next(iter(versions)) if len(versions) == 1 else "mixed",
            baseline_utility=baseline_utility,
            candidate_utility=candidate_utility,
            baseline_mean_tokens=baseline_tokens,
            candidate_mean_tokens=candidate_tokens,
            passed=not reasons,
            reasons=tuple(reasons),
        )

    @staticmethod
    def _mean(values) -> float:
        items = tuple(values)
        return sum(items) / len(items) if items else 0.0


class RetrievalPolicyPointer:
    """Small host-owned pointer with explicit rollback to a known baseline."""

    def __init__(self, baseline_version: str) -> None:
        self._baseline_version = baseline_version
        self._active_version = baseline_version

    @property
    def active_version(self) -> str:
        return self._active_version

    def activate(
        self, candidate: RetrievalPolicyCandidate, report: RetrievalPolicyReport
    ) -> None:
        candidate.validate_numbers()
        report.validate_numbers()
        if candidate.baseline_version != self._active_version:
            raise ValueError("candidate baseline is not currently active")
        if report.candidate_id != candidate.id or not report.passed:
            raise ValueError("a passing report for this candidate is required")
        self._active_version = candidate.version

    def rollback(self) -> None:
        self._active_version = self._baseline_version
