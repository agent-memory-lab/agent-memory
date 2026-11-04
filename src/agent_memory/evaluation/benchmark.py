"""Deterministic memory-policy benchmark contracts and reports."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
import inspect
import json
import math
from statistics import fmean, stdev
from types import MappingProxyType
from typing import Protocol

from ..domain import MemoryScope
from ..serialization import to_jsonable
from .retrieval import RetrievalEvaluationInput


_REQUIRED_ARMS = frozenset({"no-memory", "fixed-policy", "candidate-policy"})


class MemoryBenchmarkDimension(StrEnum):
    FACTUAL = "factual"
    TEMPORAL = "temporal"
    CONFLICT = "conflict"
    EPISODE = "episode"
    PROCEDURE = "procedure"
    PROSPECTIVE = "prospective"


class BenchmarkSkip(RuntimeError):
    """A reportable skip; retained with zero credit in planned-case metrics."""


def _identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValueError(f"{field_name} must contain 1 to 128 characters")
    return value


def _unique_ids(values: Sequence[str], field_name: str, maximum: int = 512) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise TypeError(f"{field_name} must be a sequence")
    if len(values) > maximum:
        raise ValueError(f"{field_name} exceeds its capacity")
    normalized = tuple(_identifier(value, field_name) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} must be unique")
    return normalized


def _versions(value: Mapping[str, str]) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError("plugin_versions must be a non-empty mapping")
    return MappingProxyType(
        {
            _identifier(name, "plugin name"): _identifier(version, "plugin version")
            for name, version in sorted(value.items())
        }
    )


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkCase:
    case_id: str
    scope: MemoryScope
    query_text: str
    dimension: MemoryBenchmarkDimension
    relevant_memory_ids: tuple[str, ...]
    required_source_event_ids: tuple[str, ...] = ()
    forbidden_memory_ids: tuple[str, ...] = ()
    expected_order: tuple[str, ...] = ()
    success_threshold: float = 0.75
    cluster_id: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.case_id, "case_id")
        if self.cluster_id is not None:
            _identifier(self.cluster_id, "cluster_id")
        if not isinstance(self.scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope")
        if not isinstance(self.query_text, str) or not self.query_text.strip():
            raise ValueError("query_text must not be empty")
        if len(self.query_text) > 4_096:
            raise ValueError("query_text cannot exceed 4096 characters")
        object.__setattr__(self, "dimension", MemoryBenchmarkDimension(self.dimension))
        relevant = _unique_ids(self.relevant_memory_ids, "relevant_memory_ids")
        required = _unique_ids(
            self.required_source_event_ids, "required_source_event_ids", 1_024
        )
        forbidden = _unique_ids(self.forbidden_memory_ids, "forbidden_memory_ids")
        order = _unique_ids(self.expected_order, "expected_order") or relevant
        if not relevant:
            raise ValueError("relevant_memory_ids must not be empty")
        if set(relevant) & set(forbidden):
            raise ValueError("relevant and forbidden memory IDs must be disjoint")
        if not set(order).issubset(relevant):
            raise ValueError("expected_order must be a subset of relevant_memory_ids")
        if not isinstance(self.success_threshold, (int, float)) or not (
            0 < float(self.success_threshold) <= 1
        ):
            raise ValueError("success_threshold must be between 0 and 1")
        object.__setattr__(self, "relevant_memory_ids", relevant)
        object.__setattr__(self, "required_source_event_ids", required)
        object.__setattr__(self, "forbidden_memory_ids", forbidden)
        object.__setattr__(self, "expected_order", order)
        object.__setattr__(self, "success_threshold", float(self.success_threshold))


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkDataset:
    dataset_id: str
    version: str
    license: str
    cases: tuple[MemoryBenchmarkCase, ...]

    def __post_init__(self) -> None:
        _identifier(self.dataset_id, "dataset_id")
        _identifier(self.version, "dataset version")
        _identifier(self.license, "dataset license")
        cases = tuple(self.cases)
        if not cases or len(cases) > 100_000:
            raise ValueError("dataset must contain between 1 and 100000 cases")
        if any(not isinstance(case, MemoryBenchmarkCase) for case in cases):
            raise TypeError("dataset cases must be MemoryBenchmarkCase values")
        if len({case.case_id for case in cases}) != len(cases):
            raise ValueError("dataset case IDs must be unique")
        object.__setattr__(self, "cases", cases)


class MemoryBenchmarkDatasetAdapter(Protocol):
    async def load(self) -> MemoryBenchmarkDataset: ...


async def load_benchmark_dataset(
    adapter: MemoryBenchmarkDatasetAdapter,
) -> MemoryBenchmarkDataset:
    loader = getattr(adapter, "load", None)
    if not callable(loader):
        raise TypeError("dataset adapter must implement load")
    dataset = loader()
    if inspect.isawaitable(dataset):
        dataset = await dataset
    if not isinstance(dataset, MemoryBenchmarkDataset):
        raise TypeError("dataset adapter must return MemoryBenchmarkDataset")
    return dataset


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkObservation:
    memory_ids: tuple[str, ...]
    source_event_ids: tuple[str, ...]
    token_estimate: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "memory_ids", _unique_ids(self.memory_ids, "memory_ids", 2_048)
        )
        object.__setattr__(
            self,
            "source_event_ids",
            _unique_ids(self.source_event_ids, "source_event_ids", 4_096),
        )
        if type(self.token_estimate) is not int or not 0 <= self.token_estimate <= 100_000_000:
            raise ValueError("token_estimate must be between 0 and 100000000")


class MemoryBenchmarkArm(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def version(self) -> str: ...

    async def retrieve(self, case: RetrievalEvaluationInput) -> MemoryBenchmarkObservation: ...


class NoMemoryBenchmarkArm:
    @property
    def name(self) -> str:
        return "no-memory"

    @property
    def version(self) -> str:
        return "1.0.0"

    async def retrieve(self, case: RetrievalEvaluationInput) -> MemoryBenchmarkObservation:
        return MemoryBenchmarkObservation((), (), 0)


class CallableMemoryBenchmarkArm:
    def __init__(
        self,
        name: str,
        version: str,
        retriever: Callable[
            [RetrievalEvaluationInput],
            MemoryBenchmarkObservation | Awaitable[MemoryBenchmarkObservation],
        ],
        *,
        supported_dimensions: Sequence[MemoryBenchmarkDimension] = tuple(
            MemoryBenchmarkDimension
        ),
    ) -> None:
        if name not in _REQUIRED_ARMS - {"no-memory"}:
            raise ValueError("callable arm must be fixed-policy or candidate-policy")
        self._name = name
        self._version = _identifier(version, "arm version")
        if not callable(retriever):
            raise TypeError("retriever must be callable")
        self._retriever = retriever
        dimensions = tuple(MemoryBenchmarkDimension(item) for item in supported_dimensions)
        if not dimensions or len(dimensions) != len(set(dimensions)):
            raise ValueError("supported_dimensions must be non-empty and unique")
        self._supported_dimensions = frozenset(dimensions)

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return self._version

    @property
    def supported_dimensions(self) -> frozenset[MemoryBenchmarkDimension]:
        return self._supported_dimensions

    async def retrieve(self, case: RetrievalEvaluationInput) -> MemoryBenchmarkObservation:
        observation = self._retriever(case)
        if inspect.isawaitable(observation):
            observation = await observation
        if not isinstance(observation, MemoryBenchmarkObservation):
            raise TypeError("benchmark arm returned an invalid observation")
        return observation


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkConfig:
    fixed_clock: datetime
    harness_version: str
    plugin_versions: Mapping[str, str]

    def __post_init__(self) -> None:
        if self.fixed_clock.tzinfo is None:
            raise ValueError("fixed_clock must be timezone-aware")
        _identifier(self.harness_version, "harness_version")
        object.__setattr__(self, "plugin_versions", _versions(self.plugin_versions))


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkCaseResult:
    arm: str
    case_id: str
    dimension: MemoryBenchmarkDimension
    score: float
    successful: bool
    precision: float
    recall: float
    evidence_coverage: float
    forbidden_hits: int
    token_estimate: int
    cluster_id: str | None = None


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkFailure:
    arm: str
    case_id: str
    error_type: str
    cluster_id: str | None = None


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkSkip:
    arm: str
    case_id: str
    reason: str
    cluster_id: str | None = None


@dataclass(frozen=True, slots=True)
class MemoryDimensionMetrics:
    dimension: MemoryBenchmarkDimension
    sample_count: int
    completed_cases: int
    successful_cases: int
    mean_score: float
    planned_mean_score: float | None = None
    planned_success_rate: float | None = None
    planned_cases: int = field(init=False)
    completed_mean_score: float = field(init=False)
    completed_success_rate: float = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "planned_cases", self.sample_count)
        object.__setattr__(self, "completed_mean_score", self.mean_score)
        object.__setattr__(self, "completed_success_rate", (
            self.successful_cases / self.completed_cases if self.completed_cases else 0.0
        ))


@dataclass(frozen=True, slots=True)
class MemoryArmMetrics:
    arm: str
    version: str
    sample_count: int
    completed_cases: int
    successful_cases: int
    failed_cases: int
    skipped_cases: int
    mean_score: float
    mean_precision: float
    mean_recall: float
    mean_evidence_coverage: float
    forbidden_hits: int
    mean_tokens: float
    success_confidence_low: float
    success_confidence_high: float
    dimensions: tuple[MemoryDimensionMetrics, ...]
    planned_mean_score: float | None = None
    planned_success_rate: float | None = None
    planned_success_confidence_low: float | None = None
    planned_success_confidence_high: float | None = None
    planned_cluster_count: int | None = None
    planned_confidence_method: str | None = None
    planned_uncertainty_reason: str | None = None
    planned_cases: int = field(init=False)
    completed_mean_score: float = field(init=False)
    completed_success_rate: float = field(init=False)
    completed_success_confidence_low: float = field(init=False)
    completed_success_confidence_high: float = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "planned_cases", self.sample_count)
        object.__setattr__(self, "completed_mean_score", self.mean_score)
        object.__setattr__(self, "completed_success_rate", (
            self.successful_cases / self.completed_cases if self.completed_cases else 0.0
        ))
        object.__setattr__(self, "completed_success_confidence_low", self.success_confidence_low)
        object.__setattr__(self, "completed_success_confidence_high", self.success_confidence_high)


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkComparison:
    baseline_arm: str
    candidate_arm: str
    comparable_cases: int
    mean_score_delta: float | None
    confidence_low: float | None
    confidence_high: float | None
    incomparable_reason: str | None = None
    planned_cases: int = 0
    planned_mean_score_delta: float | None = None
    planned_confidence_low: float | None = None
    planned_confidence_high: float | None = None
    planned_cluster_count: int | None = None
    planned_confidence_method: str | None = None
    planned_uncertainty_reason: str | None = None
    jointly_completed_cases: int = field(init=False)
    jointly_completed_mean_score_delta: float | None = field(init=False)
    jointly_completed_confidence_low: float | None = field(init=False)
    jointly_completed_confidence_high: float | None = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "jointly_completed_cases", self.comparable_cases)
        object.__setattr__(self, "jointly_completed_mean_score_delta", self.mean_score_delta)
        object.__setattr__(self, "jointly_completed_confidence_low", self.confidence_low)
        object.__setattr__(self, "jointly_completed_confidence_high", self.confidence_high)


@dataclass(frozen=True, slots=True)
class MemoryBenchmarkReport:
    dataset_id: str
    dataset_version: str
    dataset_license: str
    harness_version: str
    fixed_clock: datetime
    plugin_versions: Mapping[str, str]
    case_count: int
    arms: tuple[MemoryArmMetrics, ...]
    comparisons: tuple[MemoryBenchmarkComparison, ...]
    case_results: tuple[MemoryBenchmarkCaseResult, ...]
    failures: tuple[MemoryBenchmarkFailure, ...]
    skips: tuple[MemoryBenchmarkSkip, ...]
    metric_policy: str = "planned_cases_zero_credit_for_failures_and_skips"
    legacy_metric_policy: str = (
        "mean_* and success_confidence_* are completed-only diagnostics; comparison "
        "mean_score_delta/confidence_* use jointly completed cases. Not primary metrics."
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "plugin_versions", _versions(self.plugin_versions))

    def to_json(self) -> str:
        return json.dumps(
            to_jsonable(self),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def to_markdown(self) -> str:
        versions = ", ".join(
            f"{name}={version}" for name, version in self.plugin_versions.items()
        )
        lines = [
            "# Agent Memory Benchmark",
            "",
            f"Dataset: {self.dataset_id}@{self.dataset_version}",
            f"License: {self.dataset_license}",
            f"Harness: {self.harness_version}",
            f"Plugin versions: {versions}",
            f"Cases: {self.case_count}",
            f"Failures: {len(self.failures)}",
            f"Skipped: {len(self.skips)}",
            "",
            "Primary metrics use every planned case. Failures and skips receive zero credit; "
            "they remain separately reported, not measured zero-score observations.",
            "",
            "| Arm | Completed/planned | Successes | Planned mean score | "
            "Planned success rate | Approx. 95% success interval |",
            "| --- | ---: | ---: | ---: | ---: | --- |",
        ]
        for arm in self.arms:
            interval = _format_interval(
                arm.planned_success_confidence_low, arm.planned_success_confidence_high
            )
            lines.append(
                f"| {arm.arm}@{arm.version} | {arm.completed_cases}/{arm.sample_count} | "
                f"{arm.successful_cases} | {_format_number(arm.planned_mean_score)} | "
                f"{_format_number(arm.planned_success_rate)} | "
                f"{interval} |"
            )
        lines.extend([
            "", "## Planned paired comparisons", "",
            "| Baseline → candidate | Planned cases | Mean score delta | Approx. 95% interval |",
            "| --- | ---: | ---: | --- |",
        ])
        for comparison in self.comparisons:
            interval = _format_interval(
                comparison.planned_confidence_low, comparison.planned_confidence_high
            )
            lines.append(
                f"| {comparison.baseline_arm} → {comparison.candidate_arm} | "
                f"{comparison.planned_cases} | "
                f"{_format_number(comparison.planned_mean_score_delta)} | "
                f"{interval} |"
            )
        lines.extend(["", "## Uncertainty", ""])
        for arm in self.arms:
            lines.append(
                f"- {arm.arm}: {arm.planned_confidence_method}; "
                f"clusters={arm.planned_cluster_count}. {arm.planned_uncertainty_reason or ''}"
            )
        for comparison in self.comparisons:
            lines.append(
                f"- {comparison.baseline_arm} → {comparison.candidate_arm}: "
                f"{comparison.planned_confidence_method}; "
                f"clusters={comparison.planned_cluster_count}. "
                f"{comparison.planned_uncertainty_reason or ''}"
            )
        lines.extend([
            "", "## Completed-only diagnostics (conditional, not primary)", "",
            "These exclude failures/skips and may have survivor bias. Legacy mean_*, "
            "success_confidence_* and comparison mean_score_delta/confidence_* retain "
            "these conditional meanings. Conditional intervals assume independent cases "
            "and are not cluster-adjusted.",
            "",
            "| Arm | Completed mean score | Completed success rate |",
            "| --- | ---: | ---: |",
        ])
        for arm in self.arms:
            lines.append(
                f"| {arm.arm} | {arm.completed_mean_score:.4f} | "
                f"{arm.completed_success_rate:.4f} |"
            )
        for comparison in self.comparisons:
            lines.append(
                f"- {comparison.baseline_arm} → {comparison.candidate_arm}: "
                f"jointly completed={comparison.jointly_completed_cases}, "
                f"conditional mean score delta="
                f"{_format_number(comparison.jointly_completed_mean_score_delta)}."
            )
        if self.failures:
            lines.extend(["", "## Failures"])
            lines.extend(
                f"- {item.arm}/{item.case_id}: {item.error_type}" for item in self.failures
            )
        if self.skips:
            lines.extend(["", "## Skipped"])
            lines.extend(f"- {item.arm}/{item.case_id}: {item.reason}" for item in self.skips)
        return "\n".join(lines) + "\n"


async def run_memory_benchmark(
    dataset: MemoryBenchmarkDataset,
    arms: Sequence[MemoryBenchmarkArm],
    config: MemoryBenchmarkConfig,
) -> MemoryBenchmarkReport:
    if not isinstance(dataset, MemoryBenchmarkDataset):
        raise TypeError("dataset must be a MemoryBenchmarkDataset")
    if not isinstance(config, MemoryBenchmarkConfig):
        raise TypeError("config must be a MemoryBenchmarkConfig")
    by_name: dict[str, MemoryBenchmarkArm] = {}
    for arm in arms:
        name = getattr(arm, "name", None)
        version = getattr(arm, "version", None)
        if name not in _REQUIRED_ARMS:
            raise ValueError("arms must be no-memory, fixed-policy, and candidate-policy")
        _identifier(version, "arm version")
        if name in by_name:
            raise ValueError("benchmark arm names must be unique")
        if not callable(getattr(arm, "retrieve", None)):
            raise TypeError("benchmark arm must implement retrieve")
        by_name[name] = arm
    if set(by_name) != _REQUIRED_ARMS:
        raise ValueError("arms must be exactly no-memory, fixed-policy, and candidate-policy")

    case_results: list[MemoryBenchmarkCaseResult] = []
    failures: list[MemoryBenchmarkFailure] = []
    skips: list[MemoryBenchmarkSkip] = []
    ordered_cases = sorted(dataset.cases, key=lambda item: item.case_id)
    for arm_name in sorted(by_name):
        arm = by_name[arm_name]
        for index, case in enumerate(ordered_cases):
            try:
                # Dimension filtering belongs to the scorer, not the arm input.
                supported = getattr(arm, "supported_dimensions", tuple(MemoryBenchmarkDimension))
                if case.dimension not in supported:
                    raise BenchmarkSkip(f"{case.dimension.value} dimension is unsupported")
                observation = await arm.retrieve(
                    RetrievalEvaluationInput(f"case-{index:06d}", case.scope, case.query_text)
                )
                if not isinstance(observation, MemoryBenchmarkObservation):
                    raise TypeError("benchmark arm returned an invalid observation")
            except BenchmarkSkip as error:
                skips.append(
                    MemoryBenchmarkSkip(arm_name, case.case_id, str(error), case.cluster_id)
                )
                continue
            except Exception as error:
                failures.append(
                    MemoryBenchmarkFailure(
                        arm_name, case.case_id, type(error).__name__, case.cluster_id
                    )
                )
                continue
            case_results.append(_score_case(arm_name, case, observation))

    arm_metrics = tuple(
        _arm_metrics(
            name,
            str(by_name[name].version),
            ordered_cases,
            case_results,
            failures,
            skips,
        )
        for name in sorted(by_name)
    )
    comparisons = tuple(
        _comparison(baseline, "candidate-policy", ordered_cases, case_results)
        for baseline in ("fixed-policy", "no-memory")
    )
    return MemoryBenchmarkReport(
        dataset_id=dataset.dataset_id,
        dataset_version=dataset.version,
        dataset_license=dataset.license,
        harness_version=config.harness_version,
        fixed_clock=config.fixed_clock,
        plugin_versions=config.plugin_versions,
        case_count=len(ordered_cases),
        arms=arm_metrics,
        comparisons=comparisons,
        case_results=tuple(
            sorted(case_results, key=lambda item: (item.arm, item.case_id))
        ),
        failures=tuple(sorted(failures, key=lambda item: (item.arm, item.case_id))),
        skips=tuple(sorted(skips, key=lambda item: (item.arm, item.case_id))),
    )


def _score_case(
    arm_name: str,
    case: MemoryBenchmarkCase,
    observation: MemoryBenchmarkObservation,
) -> MemoryBenchmarkCaseResult:
    expected = set(case.relevant_memory_ids)
    returned = set(observation.memory_ids)
    hits = len(expected & returned)
    precision = hits / len(returned) if returned else 0.0
    recall = hits / len(expected)
    required = set(case.required_source_event_ids)
    evidence = (
        len(required & set(observation.source_event_ids)) / len(required)
        if required
        else 1.0
    )
    forbidden_hits = len(returned & set(case.forbidden_memory_ids))
    if case.dimension is MemoryBenchmarkDimension.TEMPORAL:
        base_score = (recall + _order_score(case.expected_order, observation.memory_ids)) / 2
    elif case.dimension is MemoryBenchmarkDimension.CONFLICT:
        base_score = recall if forbidden_hits == 0 else 0.0
    else:
        base_score = recall
    score = (base_score + evidence) / 2 if required else base_score
    successful = score >= case.success_threshold and forbidden_hits == 0
    return MemoryBenchmarkCaseResult(
        arm=arm_name,
        case_id=case.case_id,
        dimension=case.dimension,
        score=score,
        successful=successful,
        precision=precision,
        recall=recall,
        evidence_coverage=evidence,
        forbidden_hits=forbidden_hits,
        token_estimate=observation.token_estimate,
        cluster_id=case.cluster_id,
    )


def _order_score(expected: Sequence[str], returned: Sequence[str]) -> float:
    pairs = [
        (left, right)
        for index, left in enumerate(expected)
        for right in expected[index + 1 :]
    ]
    if not pairs:
        return 1.0 if expected and expected[0] in returned else 0.0
    positions = {value: index for index, value in enumerate(returned)}
    correct = sum(
        left in positions and right in positions and positions[left] < positions[right]
        for left, right in pairs
    )
    return correct / len(pairs)


def _arm_metrics(
    arm: str,
    version: str,
    cases: Sequence[MemoryBenchmarkCase],
    results: Sequence[MemoryBenchmarkCaseResult],
    failures: Sequence[MemoryBenchmarkFailure],
    skips: Sequence[MemoryBenchmarkSkip],
) -> MemoryArmMetrics:
    selected = [item for item in results if item.arm == arm]
    successful = sum(item.successful for item in selected)
    low, high = _wilson_interval(successful, len(selected))
    by_case = {item.case_id: item for item in selected}
    planned_successes = [
        float(by_case[case.case_id].successful) if case.case_id in by_case else 0.0
        for case in cases
    ]
    planned_low, planned_high, clusters, method, reason = _planned_interval(
        cases, planned_successes, success_rate=True
    )
    dimensions: list[MemoryDimensionMetrics] = []
    for dimension in MemoryBenchmarkDimension:
        dimension_cases = [case for case in cases if case.dimension is dimension]
        if not dimension_cases:
            continue
        dimension_results = [item for item in selected if item.dimension is dimension]
        dimensions.append(
            MemoryDimensionMetrics(
                dimension=dimension,
                sample_count=len(dimension_cases),
                completed_cases=len(dimension_results),
                successful_cases=sum(item.successful for item in dimension_results),
                mean_score=_mean(item.score for item in dimension_results),
                planned_mean_score=(
                    math.fsum(item.score for item in dimension_results) / len(dimension_cases)
                ),
                planned_success_rate=(
                    sum(item.successful for item in dimension_results) / len(dimension_cases)
                ),
            )
        )
    return MemoryArmMetrics(
        arm=arm,
        version=version,
        sample_count=len(cases),
        completed_cases=len(selected),
        successful_cases=successful,
        failed_cases=sum(item.arm == arm for item in failures),
        skipped_cases=sum(item.arm == arm for item in skips),
        mean_score=_mean(item.score for item in selected),
        mean_precision=_mean(item.precision for item in selected),
        mean_recall=_mean(item.recall for item in selected),
        mean_evidence_coverage=_mean(item.evidence_coverage for item in selected),
        forbidden_hits=sum(item.forbidden_hits for item in selected),
        mean_tokens=_mean(float(item.token_estimate) for item in selected),
        success_confidence_low=low,
        success_confidence_high=high,
        dimensions=tuple(dimensions),
        planned_mean_score=math.fsum(item.score for item in selected) / len(cases),
        planned_success_rate=successful / len(cases),
        planned_success_confidence_low=planned_low,
        planned_success_confidence_high=planned_high,
        planned_cluster_count=clusters,
        planned_confidence_method=method,
        planned_uncertainty_reason=reason,
    )


def _comparison(
    baseline_arm: str,
    candidate_arm: str,
    cases: Sequence[MemoryBenchmarkCase],
    results: Sequence[MemoryBenchmarkCaseResult],
) -> MemoryBenchmarkComparison:
    by_key = {(item.arm, item.case_id): item for item in results}
    case_ids = sorted(
        case_id
        for arm, case_id in by_key
        if arm == candidate_arm and (baseline_arm, case_id) in by_key
    )
    deltas = [
        by_key[(candidate_arm, case_id)].score - by_key[(baseline_arm, case_id)].score
        for case_id in case_ids
    ]
    mean_delta = fmean(deltas) if deltas else None
    low = high = None
    reason = None
    if len(deltas) < 2:
        reason = "fewer than 2 comparable cases" if deltas else "no jointly completed cases"
    else:
        margin = 1.96 * stdev(deltas) / math.sqrt(len(deltas))
        low, high = mean_delta - margin, mean_delta + margin
    planned_deltas = []
    for case in cases:
        candidate = by_key.get((candidate_arm, case.case_id))
        baseline = by_key.get((baseline_arm, case.case_id))
        planned_deltas.append(
            (candidate.score if candidate else 0.0) - (baseline.score if baseline else 0.0)
        )
    planned_low, planned_high, clusters, method, planned_reason = _planned_interval(
        cases, planned_deltas, success_rate=False
    )
    return MemoryBenchmarkComparison(
        baseline_arm=baseline_arm,
        candidate_arm=candidate_arm,
        comparable_cases=len(deltas),
        mean_score_delta=mean_delta,
        confidence_low=low,
        confidence_high=high,
        incomparable_reason=reason,
        planned_cases=len(cases),
        planned_mean_score_delta=fmean(planned_deltas),
        planned_confidence_low=planned_low,
        planned_confidence_high=planned_high,
        planned_cluster_count=clusters,
        planned_confidence_method=method,
        planned_uncertainty_reason=planned_reason,
    )


def _planned_interval(
    cases: Sequence[MemoryBenchmarkCase],
    values: Sequence[float],
    *,
    success_rate: bool,
) -> tuple[float | None, float | None, int | None, str, str]:
    """Case-weighted intervals; cluster IDs are supplied by the evaluator, never guessed."""
    known_clusters = [case.cluster_id for case in cases if case.cluster_id is not None]
    if known_clusters and len(known_clusters) != len(cases):
        return (
            None, None, None, "unavailable_incomplete_cluster_metadata",
            "Some planned cases lack cluster IDs; no partial-cluster interval is reported.",
        )
    mean = fmean(values)
    if known_clusters:
        grouped: dict[str, list[float]] = {}
        for cluster, value in zip(known_clusters, values, strict=True):
            grouped.setdefault(cluster, []).append(value)
        count = len(grouped)
        method = "cluster_robust_normal_approximation"
        if count < 2:
            return (
                None, None, count, method,
                "Fewer than 2 independent clusters; interval unavailable.",
            )
        residuals = []
        for group in grouped.values():
            group_mean = fmean(group)
            difference = group_mean - mean
            # fmean uses a stable sum; suppress residuals at floating-point
            # resolution, not at the arbitrary precision used to display reports.
            roundoff = 4 * max(math.ulp(mean), math.ulp(group_mean))
            residuals.append(0.0 if abs(difference) <= roundoff else len(group) * difference)
        # Cluster sandwich variance for the case-weighted mean, with G/(G-1) correction.
        variance = count / (count - 1) * math.fsum(value * value for value in residuals)
        if variance == 0:
            return (
                None, None, count, method,
                "Zero empirical cluster variance (within floating-point resolution); "
                "a degenerate interval would overstate certainty.",
            )
        margin = 1.96 * math.sqrt(variance) / len(values)
        lower_bound = 0.0 if success_rate else -1.0
        return (
            max(lower_bound, mean - margin), min(1.0, mean + margin), count, method,
            "Approximation assumes independent clusters; few clusters or zero empirical "
            "variance can understate uncertainty. Not a confirmatory significance test.",
        )
    limitation = "Cluster IDs unavailable; within-source dependence is not accounted for."
    if success_rate:
        low, high = _wilson_interval(int(sum(values)), len(values))
        return low, high, None, "wilson_case_independence_assumed", limitation
    method = "normal_case_independence_assumed"
    if len(values) < 2:
        return None, None, None, method, f"Fewer than 2 planned cases. {limitation}"
    deviation = stdev(values)
    if deviation == 0:
        return (
            None, None, None, method,
            "Zero empirical variance; a degenerate interval would overstate certainty. "
            f"{limitation}",
        )
    margin = 1.96 * deviation / math.sqrt(len(values))
    return max(-1.0, mean - margin), min(1.0, mean + margin), None, method, limitation


def _format_number(value: float | None) -> str:
    return "unavailable" if value is None else f"{value:.4f}"


def _format_interval(low: float | None, high: float | None) -> str:
    if low is None or high is None:
        return "unavailable"
    return f"[{low:.4f}, {high:.4f}]"


def _wilson_interval(successes: int, total: int) -> tuple[float, float]:
    if total == 0:
        return 0.0, 0.0
    z = 1.96
    proportion = successes / total
    denominator = 1 + z * z / total
    center = (proportion + z * z / (2 * total)) / denominator
    margin = (
        z
        * math.sqrt(
            proportion * (1 - proportion) / total + z * z / (4 * total * total)
        )
        / denominator
    )
    return max(0.0, center - margin), min(1.0, center + margin)


def _mean(values) -> float:
    collected = tuple(values)
    return fmean(collected) if collected else 0.0
