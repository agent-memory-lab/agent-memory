"""Phase-aware, profile-specific resource measurement and release gates."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import inspect
import json
import math
import os
import platform
import resource
import sys
from time import perf_counter_ns
from typing import Protocol

from .serialization import to_jsonable


class ResourceProfile(StrEnum):
    MINIMAL = "minimal"
    SMART = "smart"
    SCALE = "scale"


class ResourcePhase(StrEnum):
    CONSTRUCTION = "construction"
    RETRIEVAL = "retrieval"
    GENERATION = "generation"


class ResourceGateStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    INCOMPARABLE = "incomparable"


def _identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValueError(f"{field_name} must contain 1 to 128 characters")
    return value


@dataclass(frozen=True, slots=True)
class ResourceHardware:
    operating_system: str
    machine: str
    cpu_count: int
    label: str

    def __post_init__(self) -> None:
        _identifier(self.operating_system, "operating_system")
        _identifier(self.machine, "machine")
        _identifier(self.label, "hardware label")
        if type(self.cpu_count) is not int or not 1 <= self.cpu_count <= 65_536:
            raise ValueError("cpu_count must be between 1 and 65536")


def detect_resource_hardware() -> ResourceHardware:
    return ResourceHardware(
        operating_system=platform.system() or "unknown",
        machine=platform.machine() or "unknown",
        cpu_count=os.cpu_count() or 1,
        label=os.environ.get("RUNNER_NAME") or platform.node() or "local",
    )


@dataclass(frozen=True, slots=True)
class ResourceProfileConfig:
    profile: ResourceProfile
    dataset_size: int
    concurrency: int
    repetitions: int = 5
    latency_tolerance_ratio: float = 0.20
    rss_tolerance_ratio: float = 0.20
    storage_tolerance_ratio: float = 0.20

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile", ResourceProfile(self.profile))
        for name, value, maximum in (
            ("dataset_size", self.dataset_size, 10_000_000),
            ("concurrency", self.concurrency, 10_000),
            ("repetitions", self.repetitions, 10_000),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{name} must be between 1 and {maximum}")
        for name, value in (
            ("latency_tolerance_ratio", self.latency_tolerance_ratio),
            ("rss_tolerance_ratio", self.rss_tolerance_ratio),
            ("storage_tolerance_ratio", self.storage_tolerance_ratio),
        ):
            if not isinstance(value, (int, float)) or not 0 <= float(value) <= 10:
                raise ValueError(f"{name} must be between 0 and 10")
            object.__setattr__(self, name, float(value))


@dataclass(frozen=True, slots=True)
class ResourceEvaluationPlan:
    plan_id: str
    version: str
    fixed_clock: datetime
    python_version: str
    hardware: ResourceHardware
    profiles: tuple[ResourceProfileConfig, ...]

    def __post_init__(self) -> None:
        _identifier(self.plan_id, "plan_id")
        _identifier(self.version, "plan version")
        if self.fixed_clock.tzinfo is None:
            raise ValueError("fixed_clock must be timezone-aware")
        if self.python_version != "3.13":
            raise ValueError("resource evaluation requires Python 3.13")
        if not isinstance(self.hardware, ResourceHardware):
            raise TypeError("hardware must be ResourceHardware")
        profiles = tuple(self.profiles)
        by_profile = {item.profile: item for item in profiles}
        if set(by_profile) != set(ResourceProfile) or len(profiles) != len(ResourceProfile):
            raise ValueError("profiles must contain exactly minimal, smart, and scale")
        object.__setattr__(
            self,
            "profiles",
            tuple(by_profile[profile] for profile in ResourceProfile),
        )


@dataclass(frozen=True, slots=True)
class ResourceUsage:
    token_count: int = 0
    model_calls: int = 0

    def __post_init__(self) -> None:
        if type(self.token_count) is not int or self.token_count < 0:
            raise ValueError("token_count must be a non-negative integer")
        if type(self.model_calls) is not int or self.model_calls < 0:
            raise ValueError("model_calls must be a non-negative integer")


ResourceAction = Callable[[], ResourceUsage | None | Awaitable[ResourceUsage | None]]
ResourceCounter = Callable[[], int | Awaitable[int]]


@dataclass(frozen=True, slots=True)
class ResourceScenario:
    cold_start: ResourceAction
    construction: ResourceAction
    retrieval: ResourceAction
    generation: ResourceAction
    queue_depth: ResourceCounter
    storage_bytes: ResourceCounter

    def __post_init__(self) -> None:
        for name in (
            "cold_start",
            "construction",
            "retrieval",
            "generation",
            "queue_depth",
            "storage_bytes",
        ):
            if not callable(getattr(self, name)):
                raise TypeError(f"{name} must be callable")


class ResourceMeasurementClock(Protocol):
    def now_ns(self) -> int: ...


class ResourceProbe(Protocol):
    def rss_bytes(self) -> int: ...


class SystemMeasurementClock:
    def now_ns(self) -> int:
        return perf_counter_ns()


class ProcessResourceProbe:
    """Dependency-free peak RSS probe normalized to bytes."""

    def rss_bytes(self) -> int:
        peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return peak if sys.platform == "darwin" else peak * 1_024


@dataclass(frozen=True, slots=True)
class ResourcePhaseMetrics:
    phase: ResourcePhase
    sample_count: int
    latency_p50_ms: float
    latency_p95_ms: float
    latency_max_ms: float
    rss_start_bytes: int
    rss_peak_bytes: int
    rss_delta_bytes: int
    token_count: int
    model_calls: int
    queue_growth: int
    storage_growth_bytes: int


@dataclass(frozen=True, slots=True)
class ResourceProfileReport:
    profile: ResourceProfile
    config: ResourceProfileConfig
    cold_start_latency_ms: float
    cold_start_rss_delta_bytes: int
    phases: tuple[ResourcePhaseMetrics, ...]


@dataclass(frozen=True, slots=True)
class ResourceEvaluationReport:
    plan_id: str
    plan_version: str
    fixed_clock: datetime
    python_version: str
    hardware: ResourceHardware
    profiles: tuple[ResourceProfileReport, ...]
    unmeasured_profiles: tuple[ResourceProfile, ...]

    def to_json(self) -> str:
        return json.dumps(
            to_jsonable(self),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def to_markdown(self) -> str:
        lines = [
            "# Agent Memory Resource Evaluation",
            "",
            f"Plan: {self.plan_id}@{self.plan_version}",
            f"Python: {self.python_version}",
            (
                "Hardware: "
                f"{self.hardware.label} ({self.hardware.operating_system}, "
                f"{self.hardware.machine}, {self.hardware.cpu_count} CPU)"
            ),
            "",
        ]
        measured = {item.profile: item for item in self.profiles}
        for profile in ResourceProfile:
            report = measured.get(profile)
            if report is None:
                lines.append(f"- {profile.value}: UNMEASURED")
                continue
            lines.append(
                f"- {profile.value}: measured, dataset={report.config.dataset_size}, "
                f"concurrency={report.config.concurrency}, "
                f"cold_start={report.cold_start_latency_ms:.3f} ms"
            )
            for phase in report.phases:
                lines.append(
                    f"  - {phase.phase.value}: p50={phase.latency_p50_ms:.3f} ms, "
                    f"p95={phase.latency_p95_ms:.3f} ms, "
                    f"RSS peak={phase.rss_peak_bytes} bytes, tokens={phase.token_count}, "
                    f"model_calls={phase.model_calls}, queue_growth={phase.queue_growth}, "
                    f"storage_growth={phase.storage_growth_bytes} bytes"
                )
        return "\n".join(lines) + "\n"


@dataclass(frozen=True, slots=True)
class ResourceProfileGate:
    profile: ResourceProfile
    status: ResourceGateStatus
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ResourceComparisonReport:
    baseline_plan_id: str
    candidate_plan_id: str
    profiles: tuple[ResourceProfileGate, ...]


async def run_resource_evaluation(
    plan: ResourceEvaluationPlan,
    scenarios: Mapping[ResourceProfile, ResourceScenario],
    *,
    clock: ResourceMeasurementClock | None = None,
    probe: ResourceProbe | None = None,
) -> ResourceEvaluationReport:
    if not isinstance(plan, ResourceEvaluationPlan):
        raise TypeError("plan must be a ResourceEvaluationPlan")
    if not isinstance(scenarios, Mapping):
        raise TypeError("scenarios must be a mapping")
    normalized: dict[ResourceProfile, ResourceScenario] = {}
    for raw_profile, scenario in scenarios.items():
        profile = ResourceProfile(raw_profile)
        if not isinstance(scenario, ResourceScenario):
            raise TypeError("scenario values must be ResourceScenario instances")
        normalized[profile] = scenario
    measurement_clock = clock or SystemMeasurementClock()
    resource_probe = probe or ProcessResourceProbe()
    config_by_profile = {item.profile: item for item in plan.profiles}
    reports: list[ResourceProfileReport] = []
    for profile in ResourceProfile:
        scenario = normalized.get(profile)
        if scenario is None:
            continue
        reports.append(
            await _measure_profile(
                config_by_profile[profile],
                scenario,
                measurement_clock,
                resource_probe,
            )
        )
    return ResourceEvaluationReport(
        plan_id=plan.plan_id,
        plan_version=plan.version,
        fixed_clock=plan.fixed_clock,
        python_version=plan.python_version,
        hardware=plan.hardware,
        profiles=tuple(reports),
        unmeasured_profiles=tuple(
            profile for profile in ResourceProfile if profile not in normalized
        ),
    )


async def _measure_profile(
    config: ResourceProfileConfig,
    scenario: ResourceScenario,
    clock: ResourceMeasurementClock,
    probe: ResourceProbe,
) -> ResourceProfileReport:
    cold_rss_start = _rss(probe)
    cold_started = clock.now_ns()
    await _invoke_action(scenario.cold_start)
    cold_elapsed = _elapsed_ms(cold_started, clock.now_ns())
    cold_rss_end = _rss(probe)
    phase_actions = {
        ResourcePhase.CONSTRUCTION: scenario.construction,
        ResourcePhase.RETRIEVAL: scenario.retrieval,
        ResourcePhase.GENERATION: scenario.generation,
    }
    phases = tuple(
        [
            await _measure_phase(
                phase,
                phase_actions[phase],
                scenario,
                config.repetitions,
                clock,
                probe,
            )
            for phase in ResourcePhase
        ]
    )
    return ResourceProfileReport(
        profile=config.profile,
        config=config,
        cold_start_latency_ms=cold_elapsed,
        cold_start_rss_delta_bytes=max(0, cold_rss_end - cold_rss_start),
        phases=phases,
    )


async def _measure_phase(
    phase: ResourcePhase,
    action: ResourceAction,
    scenario: ResourceScenario,
    repetitions: int,
    clock: ResourceMeasurementClock,
    probe: ResourceProbe,
) -> ResourcePhaseMetrics:
    queue_start = await _counter(scenario.queue_depth, "queue_depth")
    storage_start = await _counter(scenario.storage_bytes, "storage_bytes")
    rss_start = _rss(probe)
    rss_peak = rss_start
    latencies: list[float] = []
    tokens = 0
    model_calls = 0
    for _ in range(repetitions):
        started = clock.now_ns()
        usage = await _invoke_action(action)
        latencies.append(_elapsed_ms(started, clock.now_ns()))
        tokens += usage.token_count
        model_calls += usage.model_calls
        rss_peak = max(rss_peak, _rss(probe))
    queue_end = await _counter(scenario.queue_depth, "queue_depth")
    storage_end = await _counter(scenario.storage_bytes, "storage_bytes")
    return ResourcePhaseMetrics(
        phase=phase,
        sample_count=repetitions,
        latency_p50_ms=_percentile(latencies, 0.50),
        latency_p95_ms=_percentile(latencies, 0.95),
        latency_max_ms=max(latencies),
        rss_start_bytes=rss_start,
        rss_peak_bytes=rss_peak,
        rss_delta_bytes=max(0, rss_peak - rss_start),
        token_count=tokens,
        model_calls=model_calls,
        queue_growth=queue_end - queue_start,
        storage_growth_bytes=storage_end - storage_start,
    )


async def _invoke_action(action: ResourceAction) -> ResourceUsage:
    result = action()
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return ResourceUsage()
    if not isinstance(result, ResourceUsage):
        raise TypeError("resource action must return ResourceUsage or None")
    return result


async def _counter(counter: ResourceCounter, name: str) -> int:
    value = counter()
    if inspect.isawaitable(value):
        value = await value
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must return a non-negative integer")
    return value


def _rss(probe: ResourceProbe) -> int:
    value = probe.rss_bytes()
    if type(value) is not int or value < 0:
        raise ValueError("resource probe must return non-negative RSS bytes")
    return value


def _elapsed_ms(start: int, end: int) -> float:
    if type(start) is not int or type(end) is not int or end < start:
        raise ValueError("measurement clock must be monotonic integer nanoseconds")
    return (end - start) / 1_000_000


def _percentile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def compare_resource_reports(
    baseline: ResourceEvaluationReport,
    candidate: ResourceEvaluationReport,
) -> ResourceComparisonReport:
    baseline_profiles = {item.profile: item for item in baseline.profiles}
    candidate_profiles = {item.profile: item for item in candidate.profiles}
    global_reasons: list[str] = []
    if baseline.python_version != candidate.python_version:
        global_reasons.append("Python version differs")
    if baseline.hardware != candidate.hardware:
        global_reasons.append("hardware differs")
    if baseline.plan_version != candidate.plan_version:
        global_reasons.append("plan version differs")
    gates: list[ResourceProfileGate] = []
    for profile in ResourceProfile:
        if global_reasons:
            gates.append(
                ResourceProfileGate(
                    profile,
                    ResourceGateStatus.INCOMPARABLE,
                    tuple(global_reasons),
                )
            )
            continue
        before = baseline_profiles.get(profile)
        after = candidate_profiles.get(profile)
        if before is None or after is None:
            gates.append(
                ResourceProfileGate(
                    profile,
                    ResourceGateStatus.INCOMPARABLE,
                    ("profile was not measured in both reports",),
                )
            )
            continue
        if before.config != after.config:
            gates.append(
                ResourceProfileGate(
                    profile,
                    ResourceGateStatus.INCOMPARABLE,
                    ("profile configuration differs",),
                )
            )
            continue
        reasons = _regression_reasons(before, after)
        gates.append(
            ResourceProfileGate(
                profile,
                ResourceGateStatus.FAIL if reasons else ResourceGateStatus.PASS,
                tuple(reasons),
            )
        )
    return ResourceComparisonReport(
        baseline_plan_id=baseline.plan_id,
        candidate_plan_id=candidate.plan_id,
        profiles=tuple(gates),
    )


def _regression_reasons(
    baseline: ResourceProfileReport,
    candidate: ResourceProfileReport,
) -> list[str]:
    config = baseline.config
    reasons: list[str] = []
    if _exceeds(
        candidate.cold_start_latency_ms,
        baseline.cold_start_latency_ms,
        config.latency_tolerance_ratio,
    ):
        reasons.append("cold_start latency exceeds tolerance")
    if _exceeds(
        candidate.cold_start_rss_delta_bytes,
        baseline.cold_start_rss_delta_bytes,
        config.rss_tolerance_ratio,
    ):
        reasons.append("cold_start RSS exceeds tolerance")
    before_by_phase = {item.phase: item for item in baseline.phases}
    after_by_phase = {item.phase: item for item in candidate.phases}
    for phase in ResourcePhase:
        before = before_by_phase[phase]
        after = after_by_phase[phase]
        if _exceeds(
            after.latency_p95_ms,
            before.latency_p95_ms,
            config.latency_tolerance_ratio,
        ):
            reasons.append(f"{phase.value} latency_p95_ms exceeds tolerance")
        if _exceeds(
            after.rss_peak_bytes,
            before.rss_peak_bytes,
            config.rss_tolerance_ratio,
        ):
            reasons.append(f"{phase.value} rss_peak_bytes exceeds tolerance")
        if _exceeds(
            after.storage_growth_bytes,
            before.storage_growth_bytes,
            config.storage_tolerance_ratio,
        ):
            reasons.append(f"{phase.value} storage_growth_bytes exceeds tolerance")
    return reasons


def _exceeds(candidate: float | int, baseline: float | int, tolerance: float) -> bool:
    if baseline < 0 or candidate < 0:
        return candidate > baseline
    if baseline == 0:
        return candidate > 0
    return candidate > baseline * (1 + tolerance)
