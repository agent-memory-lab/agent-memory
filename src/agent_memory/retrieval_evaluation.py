"""Deterministic, dependency-free evaluation contracts for retrieval policies."""

from __future__ import annotations

from dataclasses import dataclass
import math
from statistics import mean
from time import monotonic
from typing import Mapping, Protocol, Sequence

from .domain import MemoryScope

_REQUIRED_ARMS = frozenset({"no-memory", "lexical-only", "hybrid"})


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValueError(f"{field} must contain 1 to 128 characters")
    return value


def _ids(values: Sequence[str], field: str, *, maximum: int) -> tuple[str, ...]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise TypeError(f"{field} must be a sequence")
    if len(values) > maximum:
        raise ValueError(f"{field} exceeds its capacity")
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        resolved = _identifier(value, field)
        if resolved not in seen:
            seen.add(resolved)
            normalized.append(resolved)
    return tuple(normalized)


@dataclass(frozen=True, slots=True)
class RetrievalEvalCase:
    case_id: str
    scope: MemoryScope
    query_text: str
    relevant_memory_ids: tuple[str, ...]
    required_source_event_ids: tuple[str, ...] = ()
    forbidden_memory_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.case_id, "case_id")
        if not isinstance(self.scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope")
        if not isinstance(self.query_text, str) or not self.query_text.strip():
            raise ValueError("query_text must be non-empty")
        if len(self.query_text) > 2_048:
            raise ValueError("query_text cannot exceed 2048 characters")
        relevant = _ids(self.relevant_memory_ids, "relevant_memory_ids", maximum=128)
        required = _ids(
            self.required_source_event_ids,
            "required_source_event_ids",
            maximum=256,
        )
        forbidden = _ids(self.forbidden_memory_ids, "forbidden_memory_ids", maximum=128)
        if set(relevant) & set(forbidden):
            raise ValueError("relevant and forbidden memory IDs must be disjoint")
        object.__setattr__(self, "relevant_memory_ids", relevant)
        object.__setattr__(self, "required_source_event_ids", required)
        object.__setattr__(self, "forbidden_memory_ids", forbidden)


@dataclass(frozen=True, slots=True)
class RetrievalDatasetSnapshot:
    snapshot_id: str
    version: str
    cases: tuple[RetrievalEvalCase, ...]

    def __post_init__(self) -> None:
        _identifier(self.snapshot_id, "snapshot_id")
        _identifier(self.version, "version")
        if not self.cases or len(self.cases) > 10_000:
            raise ValueError("snapshot must contain between 1 and 10000 cases")
        if any(not isinstance(value, RetrievalEvalCase) for value in self.cases):
            raise TypeError("snapshot contains an invalid case")
        case_ids = [value.case_id for value in self.cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("snapshot case IDs must be unique")


@dataclass(frozen=True, slots=True)
class RetrievalObservation:
    memory_ids: tuple[str, ...]
    source_event_ids: tuple[str, ...]
    token_estimate: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "memory_ids",
            _ids(self.memory_ids, "memory_ids", maximum=256),
        )
        object.__setattr__(
            self,
            "source_event_ids",
            _ids(self.source_event_ids, "source_event_ids", maximum=1024),
        )
        if type(self.token_estimate) is not int or not 0 <= self.token_estimate <= 10_000_000:
            raise ValueError("token_estimate must be between 0 and 10000000")


class RetrievalEvaluationArm(Protocol):
    @property
    def name(self) -> str: ...

    async def retrieve(self, case: RetrievalEvalCase) -> RetrievalObservation: ...


class NoMemoryEvaluationArm:
    @property
    def name(self) -> str:
        return "no-memory"

    async def retrieve(self, case: RetrievalEvalCase) -> RetrievalObservation:
        return RetrievalObservation((), (), 0)


@dataclass(frozen=True, slots=True)
class RetrievalArmMetrics:
    arm: str
    case_count: int
    successful_cases: int
    failed_cases: int
    retrieved_count: int
    relevant_count: int
    relevant_hits: int
    required_evidence_count: int
    evidence_hits: int
    forbidden_hits: int
    precision: float
    recall: float
    evidence_coverage: float
    failure_rate: float
    mean_tokens: float
    latency_p50_ms: float
    latency_p95_ms: float


@dataclass(frozen=True, slots=True)
class RetrievalBenchmarkFailure:
    arm: str
    case_id: str
    error_type: str


@dataclass(frozen=True, slots=True)
class RetrievalBenchmarkReport:
    snapshot_id: str
    snapshot_version: str
    metrics: tuple[RetrievalArmMetrics, ...]
    failures: tuple[RetrievalBenchmarkFailure, ...]


@dataclass(slots=True)
class _Accumulator:
    successful: int = 0
    retrieved: int = 0
    relevant: int = 0
    relevant_hits: int = 0
    required_evidence: int = 0
    evidence_hits: int = 0
    forbidden_hits: int = 0
    tokens: int = 0
    latencies: list[float] | None = None

    def __post_init__(self) -> None:
        self.latencies = []


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _percentile(values: Sequence[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


async def run_retrieval_benchmark(
    snapshot: RetrievalDatasetSnapshot,
    arms: Sequence[RetrievalEvaluationArm],
) -> RetrievalBenchmarkReport:
    """Evaluate exactly the three V4 baseline arms in stable case order."""
    if not isinstance(snapshot, RetrievalDatasetSnapshot):
        raise TypeError("snapshot must be a RetrievalDatasetSnapshot")
    if not isinstance(arms, Sequence) or isinstance(arms, (str, bytes)):
        raise TypeError("arms must be a sequence")
    by_name: dict[str, RetrievalEvaluationArm] = {}
    for arm in arms:
        name = _identifier(getattr(arm, "name", None), "arm.name")
        if name in by_name:
            raise ValueError("evaluation arm names must be unique")
        if not callable(getattr(arm, "retrieve", None)):
            raise TypeError("evaluation arm must implement retrieve")
        by_name[name] = arm
    if set(by_name) != _REQUIRED_ARMS:
        raise ValueError("arms must be exactly: no-memory, lexical-only, hybrid")

    accumulators = {name: _Accumulator() for name in sorted(by_name)}
    failures: list[RetrievalBenchmarkFailure] = []
    ordered_cases = sorted(snapshot.cases, key=lambda value: value.case_id)
    for name in sorted(by_name):
        arm = by_name[name]
        accumulator = accumulators[name]
        for case in ordered_cases:
            relevant = set(case.relevant_memory_ids)
            required_evidence = set(case.required_source_event_ids)
            accumulator.relevant += len(relevant)
            accumulator.required_evidence += len(required_evidence)
            started = monotonic()
            try:
                observation = await arm.retrieve(case)
                if not isinstance(observation, RetrievalObservation):
                    raise TypeError("arm returned an invalid observation")
            except Exception as error:
                failures.append(
                    RetrievalBenchmarkFailure(name, case.case_id, type(error).__name__)
                )
                continue
            elapsed = (monotonic() - started) * 1_000
            accumulator.successful += 1
            accumulator.latencies.append(elapsed)
            accumulator.tokens += observation.token_estimate
            returned = set(observation.memory_ids)
            accumulator.retrieved += len(returned)
            accumulator.relevant_hits += len(returned & relevant)
            accumulator.evidence_hits += len(
                set(observation.source_event_ids) & required_evidence
            )
            accumulator.forbidden_hits += len(returned & set(case.forbidden_memory_ids))

    metrics: list[RetrievalArmMetrics] = []
    case_count = len(ordered_cases)
    for name in sorted(accumulators):
        value = accumulators[name]
        failed = case_count - value.successful
        latencies = value.latencies or []
        metrics.append(
            RetrievalArmMetrics(
                arm=name,
                case_count=case_count,
                successful_cases=value.successful,
                failed_cases=failed,
                retrieved_count=value.retrieved,
                relevant_count=value.relevant,
                relevant_hits=value.relevant_hits,
                required_evidence_count=value.required_evidence,
                evidence_hits=value.evidence_hits,
                forbidden_hits=value.forbidden_hits,
                precision=_ratio(value.relevant_hits, value.retrieved),
                recall=_ratio(value.relevant_hits, value.relevant),
                evidence_coverage=_ratio(value.evidence_hits, value.required_evidence),
                failure_rate=_ratio(failed, case_count),
                mean_tokens=mean([value.tokens / value.successful]) if value.successful else 0.0,
                latency_p50_ms=_percentile(latencies, 0.50),
                latency_p95_ms=_percentile(latencies, 0.95),
            )
        )
    failures.sort(key=lambda value: (value.arm, value.case_id, value.error_type))
    return RetrievalBenchmarkReport(
        snapshot_id=snapshot.snapshot_id,
        snapshot_version=snapshot.version,
        metrics=tuple(metrics),
        failures=tuple(failures),
    )
