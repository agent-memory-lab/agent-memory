"""T42 phase-aware resource evaluation acceptance tests."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_memory import (
    ProcessResourceProbe,
    ResourceEvaluationPlan,
    ResourceGateStatus,
    ResourceHardware,
    ResourcePhase,
    ResourceProfile,
    ResourceProfileConfig,
    ResourceScenario,
    ResourceUsage,
    compare_resource_reports,
    run_resource_evaluation,
)

NOW = datetime(2026, 9, 21, 10, 0, tzinfo=UTC)
HARDWARE = ResourceHardware("test-os", "arm64", 8, "ci-runner")


class StepClock:
    def __init__(self, step_ns: int = 1_000_000):
        self.value = 0
        self.step_ns = step_ns

    def now_ns(self) -> int:
        self.value += self.step_ns
        return self.value


class StepProbe:
    def __init__(self, start: int = 10_000, step: int = 100):
        self.value = start - step
        self.step = step

    def rss_bytes(self) -> int:
        self.value += self.step
        return self.value


def _plan(*, tolerance: float = 0.10) -> ResourceEvaluationPlan:
    return ResourceEvaluationPlan(
        plan_id="release-resource-gate",
        version="1.0.0",
        fixed_clock=NOW,
        python_version="3.13",
        hardware=HARDWARE,
        profiles=tuple(
            ResourceProfileConfig(
                profile,
                dataset_size=index * 100,
                concurrency=index,
                repetitions=3,
                latency_tolerance_ratio=tolerance,
                rss_tolerance_ratio=tolerance,
                storage_tolerance_ratio=tolerance,
            )
            for index, profile in enumerate(ResourceProfile, start=1)
        ),
    )


def _scenario() -> ResourceScenario:
    counters = {"queue": 0, "storage": 1_000}

    async def cold_start():
        counters["storage"] += 5

    async def construction():
        counters["queue"] += 1
        counters["storage"] += 10
        return ResourceUsage(token_count=20, model_calls=1)

    async def retrieval():
        counters["queue"] -= 1
        counters["storage"] += 2
        return ResourceUsage(token_count=8, model_calls=0)

    async def generation():
        counters["storage"] += 1
        return ResourceUsage(token_count=40, model_calls=2)

    return ResourceScenario(
        cold_start=cold_start,
        construction=construction,
        retrieval=retrieval,
        generation=generation,
        queue_depth=lambda: counters["queue"],
        storage_bytes=lambda: counters["storage"],
    )


def test_profiles_measure_each_phase_and_all_required_resources():
    async def scenario():
        report = await run_resource_evaluation(
            _plan(),
            {profile: _scenario() for profile in ResourceProfile},
            clock=StepClock(),
            probe=StepProbe(),
        )

        assert report.unmeasured_profiles == ()
        assert tuple(item.profile for item in report.profiles) == tuple(ResourceProfile)
        minimal = report.profiles[0]
        assert tuple(item.phase for item in minimal.phases) == tuple(ResourcePhase)
        construction = minimal.phases[0]
        assert construction.sample_count == 3
        assert construction.latency_p50_ms == construction.latency_p95_ms == 1.0
        assert construction.token_count == 60
        assert construction.model_calls == 3
        assert construction.queue_growth == 3
        assert construction.storage_growth_bytes == 30
        assert construction.rss_peak_bytes >= construction.rss_start_bytes
        assert minimal.cold_start_latency_ms == 1.0

    asyncio.run(scenario())


def test_unmeasured_profiles_are_explicit_and_never_receive_metrics():
    async def scenario():
        report = await run_resource_evaluation(
            _plan(),
            {ResourceProfile.MINIMAL: _scenario()},
            clock=StepClock(),
            probe=StepProbe(),
        )
        assert report.unmeasured_profiles == (
            ResourceProfile.SMART,
            ResourceProfile.SCALE,
        )
        assert tuple(item.profile for item in report.profiles) == (ResourceProfile.MINIMAL,)
        assert '"unmeasured_profiles":["smart","scale"]' in report.to_json()
        human = report.to_markdown()
        assert "smart: UNMEASURED" in human
        assert "scale: UNMEASURED" in human

    asyncio.run(scenario())


def test_repeated_reports_apply_tolerances_and_reject_incomparable_runs():
    async def scenario():
        baseline = await run_resource_evaluation(
            _plan(),
            {profile: _scenario() for profile in ResourceProfile},
            clock=StepClock(),
            probe=StepProbe(),
        )
        candidate = await run_resource_evaluation(
            _plan(),
            {profile: _scenario() for profile in ResourceProfile},
            clock=StepClock(),
            probe=StepProbe(),
        )
        passed = compare_resource_reports(baseline, candidate)
        assert all(item.status is ResourceGateStatus.PASS for item in passed.profiles)

        minimal = candidate.profiles[0]
        slower_phase = replace(minimal.phases[0], latency_p95_ms=2.0)
        regressed = replace(
            candidate,
            profiles=(
                replace(minimal, phases=(slower_phase, *minimal.phases[1:])),
                *candidate.profiles[1:],
            ),
        )
        failed = compare_resource_reports(baseline, regressed)
        assert failed.profiles[0].status is ResourceGateStatus.FAIL
        assert "construction latency_p95_ms" in failed.profiles[0].reasons[0]

        other_hardware = replace(
            candidate,
            hardware=ResourceHardware("other-os", "x86_64", 4, "other"),
        )
        incomparable = compare_resource_reports(baseline, other_hardware)
        assert all(
            item.status is ResourceGateStatus.INCOMPARABLE
            for item in incomparable.profiles
        )
        assert "hardware differs" in incomparable.profiles[0].reasons

    asyncio.run(scenario())


def test_plan_requires_python_313_fixed_hardware_and_complete_profile_matrix():
    with pytest.raises(ValueError, match="Python 3.13"):
        replace(_plan(), python_version="3.12")
    with pytest.raises(ValueError, match="exactly minimal, smart, and scale"):
        replace(_plan(), profiles=(_plan().profiles[0],))
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(_plan(), fixed_clock=datetime(2026, 9, 21))


def test_unavailable_resource_does_not_block_root_import_or_sqlite_write_recall():
    root = Path(__file__).resolve().parents[1]
    script = """
import asyncio
import runpy
import sys
sys.modules['resource'] = None
import agent_memory
smoke = runpy.run_path(sys.argv[1])
asyncio.run(smoke['sqlite_roundtrip']())
try:
    agent_memory.ProcessResourceProbe().rss_bytes()
except NotImplementedError as exc:
    assert 'ResourceProbe' in str(exc)
else:
    raise AssertionError('unavailable RSS must not become a fabricated measurement')
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(root / "tests/operational/v7_installed_smoke.py")],
        env={**os.environ, "PYTHONPATH": str(root / "src")},
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_unavailable_rss_stops_evaluation_before_actions_but_allows_explicit_probe(monkeypatch):
    monkeypatch.setitem(sys.modules, "resource", None)
    actions = []
    scenario = ResourceScenario(
        cold_start=lambda: actions.append("cold"), construction=lambda: None,
        retrieval=lambda: None, generation=lambda: None,
        queue_depth=lambda: 0, storage_bytes=lambda: 0,
    )
    with pytest.raises(NotImplementedError, match="Peak RSS measurement"):
        asyncio.run(run_resource_evaluation(_plan(), {ResourceProfile.MINIMAL: scenario}))
    assert actions == []
    report = asyncio.run(run_resource_evaluation(
        _plan(), {ResourceProfile.MINIMAL: scenario}, clock=StepClock(), probe=StepProbe(),
    ))
    assert report.profiles[0].phases[0].rss_start_bytes > 0
    assert actions == ["cold"]


@pytest.mark.parametrize(("platform", "expected"), [("linux", 7 * 1024), ("darwin", 7)])
def test_peak_rss_keeps_platform_units(monkeypatch, platform, expected):
    monkeypatch.setitem(sys.modules, "resource", SimpleNamespace(
        RUSAGE_SELF=0, getrusage=lambda who: SimpleNamespace(ru_maxrss=7),
    ))
    monkeypatch.setattr(sys, "platform", platform)
    assert ProcessResourceProbe().rss_bytes() == expected


def _measured_report():
    return asyncio.run(run_resource_evaluation(
        _plan(), {profile: _scenario() for profile in ResourceProfile},
        clock=StepClock(), probe=StepProbe(),
    ))


INVALID_MEASUREMENTS = (float("nan"), float("inf"), float("-inf"), True, False, "1", -1, 10**400)


@pytest.mark.parametrize("value", INVALID_MEASUREMENTS)
@pytest.mark.parametrize("name", ("latency_p50_ms", "latency_p95_ms", "latency_max_ms"))
def test_phase_latencies_require_finite_nonnegative_numbers(name, value):
    phase = _measured_report().profiles[0].phases[0]
    with pytest.raises(ValueError, match=f"{name} must be finite and non-negative"):
        replace(phase, **{name: value})


@pytest.mark.parametrize("value", INVALID_MEASUREMENTS)
def test_cold_start_latency_requires_finite_nonnegative_number(value):
    profile = _measured_report().profiles[0]
    with pytest.raises(ValueError, match="cold_start_latency_ms must be finite and non-negative"):
        replace(profile, cold_start_latency_ms=value)


@pytest.mark.parametrize("value", INVALID_MEASUREMENTS)
@pytest.mark.parametrize("name", (
    "latency_tolerance_ratio", "rss_tolerance_ratio", "storage_tolerance_ratio",
))
def test_resource_tolerances_reject_invalid_numeric_evidence(name, value):
    with pytest.raises(ValueError, match=name):
        replace(_plan().profiles[0], **{name: value})


@pytest.mark.parametrize("value", (float("nan"), float("inf"), float("-inf"), True))
@pytest.mark.parametrize("source", ("baseline", "candidate"))
@pytest.mark.parametrize("name", (
    "cold_start_latency_ms", "latency_p50_ms", "latency_p95_ms", "latency_max_ms",
    "latency_tolerance_ratio", "rss_peak_bytes", "storage_growth_bytes",
))
def test_resource_gate_revalidates_unchecked_evidence(source, name, value):
    baseline, candidate = _measured_report(), _measured_report()
    report = baseline if source == "baseline" else candidate
    profile = report.profiles[0]
    if name == "cold_start_latency_ms":
        target = profile
    elif name == "latency_tolerance_ratio":
        target = profile.config
    else:
        target = profile.phases[0]
    # Model persisted/unchecked evidence that bypassed constructor validation.
    object.__setattr__(target, name, value)
    result = compare_resource_reports(baseline, candidate)
    assert result.profiles[0].status is ResourceGateStatus.FAIL
    assert source in result.profiles[0].reasons[0]
    assert name in result.profiles[0].reasons[0]
    assert all(gate.status is ResourceGateStatus.PASS for gate in result.profiles[1:])


@pytest.mark.parametrize("source", ("baseline", "candidate"))
@pytest.mark.parametrize("malformed", ("duplicate-profile", "duplicate-phase", "missing-phase"))
def test_resource_gate_rejects_ambiguous_or_incomplete_measurements(source, malformed):
    baseline, candidate = _measured_report(), _measured_report()
    report = baseline if source == "baseline" else candidate
    profile = report.profiles[0]
    if malformed == "duplicate-profile":
        report = replace(report, profiles=(*report.profiles, profile))
    else:
        phases = profile.phases[:-1]
        if malformed == "duplicate-phase":
            phases = (*phases, phases[0])
        report = replace(report, profiles=(replace(profile, phases=phases), *report.profiles[1:]))
    baseline, candidate = (report, candidate) if source == "baseline" else (baseline, report)
    result = compare_resource_reports(baseline, candidate)
    assert result.profiles[0].status is ResourceGateStatus.FAIL
    assert all(gate.status is ResourceGateStatus.PASS for gate in result.profiles[1:])


def test_zero_latency_and_negative_resource_growth_remain_valid():
    report = _measured_report()
    minimal = report.profiles[0]
    phase = replace(
        minimal.phases[0], latency_p50_ms=0, latency_p95_ms=0.0, latency_max_ms=0,
        queue_growth=-1, storage_growth_bytes=-10,
    )
    valid = replace(report, profiles=(
        replace(minimal, cold_start_latency_ms=0, phases=(phase, *minimal.phases[1:])),
        *report.profiles[1:],
    ))
    assert all(
        gate.status is ResourceGateStatus.PASS
        for gate in compare_resource_reports(valid, valid).profiles
    )
    regressed = replace(valid, profiles=(
        replace(valid.profiles[0], phases=(
            replace(phase, storage_growth_bytes=-5), *minimal.phases[1:],
        )),
        *valid.profiles[1:],
    ))
    assert compare_resource_reports(valid, regressed).profiles[0].status is ResourceGateStatus.FAIL
