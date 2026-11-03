"""Invalid evaluator numerics must never become passing evidence."""

import asyncio
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest

from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    DeterministicRetrievalPolicyEvaluator,
    EvaluationReport,
    EvaluationStage,
    EvolutionEngine,
    EvolutionState,
    NullProcedureDeployment,
    RetrievalPolicyCandidate,
    RetrievalPolicyPointer,
    RetrievalReplaySample,
    SQLiteEvolutionRegistry,
)
from agent_memory_evolution.policy import GateProfile

from agent_memory import ArtifactStatus, MemoryChannel, MemoryScope, Procedure, Provenance


INVALID = (float("nan"), float("inf"), -float("inf"), True, "0.9", None)
SCOPE = MemoryScope("tenant", session_id="session")


def report(**kwargs):
    values = dict(
        candidate_id="candidate", candidate_version=1,
        scope_partition_key=SCOPE.partition_key(), stage=EvaluationStage.OFFLINE,
        dataset_id="fixed", dataset_version="1", evaluator_id="evaluator",
        evaluator_version="1", rubric_id="task", rubric_version="1", sample_size=30,
        metrics=dict(task_success_rate=.9, task_success_delta=.1,
                     cross_scope_leakage=0., safety_violation_rate=0.),
        evidence_digest="sha256:evidence",
    )
    values.update(kwargs)
    return EvaluationReport(**values)


def candidate():
    return RetrievalPolicyCandidate(SCOPE, "base", "new", {MemoryChannel.SEMANTIC: 1}, .5, 128, 128)


def sample():
    return RetrievalReplaySample(SCOPE, "v1", 1., 1., 100, 100)


@pytest.mark.parametrize("value", INVALID)
@pytest.mark.parametrize(
    "name",
    [
        "task_success_rate",
        "task_success_delta",
        "cross_scope_leakage",
        "safety_violation_rate",
        "extra",
    ],
)
def test_report_rejects_invalid_metrics_and_gate_rechecks_mutable_mapping(name, value):
    valid = report()
    metrics = dict(valid.metrics, **{name: value})
    with pytest.raises(ValueError, match="finite"):
        replace(valid, metrics=metrics)
    valid.metrics[name] = value
    assert not DeterministicPromotionPolicy().evaluate(valid).passed


@pytest.mark.parametrize("value", (*INVALID, 1.5, -1))
@pytest.mark.parametrize("name", ["sample_size", "candidate_version", "safety_violations"])
def test_report_requires_integer_counts(name, value):
    with pytest.raises(ValueError, match="integer"):
        report(**{name: value})


@pytest.mark.parametrize("value", INVALID)
def test_thresholds_cannot_disable_gate_with_invalid_number(value):
    with pytest.raises(ValueError):
        GateProfile(30, {"task_success_rate": value})
    profile = GateProfile(30, {"task_success_rate": .7})
    profile.minimum_metrics["task_success_rate"] = value
    policy = DeterministicPromotionPolicy({EvaluationStage.OFFLINE: profile})
    assert not policy.evaluate(report()).passed


@pytest.mark.parametrize("value", INVALID)
@pytest.mark.parametrize(
    "name",
    [
        "baseline_utility",
        "candidate_utility",
        "baseline_tokens",
        "candidate_tokens",
    ],
)
def test_replay_rejects_invalid_numbers_and_defensive_evaluator(name, value):
    with pytest.raises(ValueError):
        replace(sample(), **{name: value})
    legacy = sample()
    object.__setattr__(legacy, name, value)
    with pytest.raises(ValueError):
        DeterministicRetrievalPolicyEvaluator(minimum_samples=1).evaluate(candidate(), [legacy])


@pytest.mark.parametrize("value", INVALID)
@pytest.mark.parametrize(
    "name",
    [
        "state_budget_ratio",
        "token_budget",
        "baseline_token_budget",
        "channel_weights",
    ],
)
def test_candidate_rejects_invalid_numeric_configuration(name, value):
    if name == "channel_weights":
        value = {MemoryChannel.SEMANTIC: value}
    with pytest.raises(ValueError):
        replace(candidate(), **{name: value})


@pytest.mark.parametrize("value", INVALID)
@pytest.mark.parametrize(
    "name",
    [
        "baseline_utility",
        "candidate_utility",
        "baseline_mean_tokens",
        "candidate_mean_tokens",
        "sample_size",
    ],
)
def test_pointer_rejects_legacy_invalid_passing_report(name, value):
    policy = candidate()
    result = DeterministicRetrievalPolicyEvaluator(minimum_samples=1).evaluate(policy, [sample()])
    with pytest.raises(ValueError):
        replace(result, **{name: value})
    object.__setattr__(result, name, value)
    pointer = RetrievalPolicyPointer("base")
    with pytest.raises(ValueError):
        pointer.activate(policy, result)
    assert pointer.active_version == "base"


def test_overflowed_aggregate_does_not_pass():
    huge = replace(sample(), candidate_utility=1e308)
    with pytest.raises(ValueError, match="finite"):
        DeterministicRetrievalPolicyEvaluator(minimum_samples=1).evaluate(candidate(), [huge, huge])


@pytest.mark.parametrize("encoded", ["NaN", "Infinity", "-Infinity", "true", '"0.9"', "1e999"])
def test_legacy_invalid_persisted_report_cannot_promote(tmp_path, encoded):
    async def scenario():
        path = tmp_path / "evolution.db"
        registry = SQLiteEvolutionRegistry(path)
        engine = EvolutionEngine(
            registry, DeterministicPromotionPolicy(), NullProcedureDeployment()
        )
        await engine.initialize()
        proc = Procedure(scope=SCOPE, name="candidate", trigger="request", steps=("act",),
                         success_conditions=("success",), status=ArtifactStatus.CANDIDATE,
                         provenance=Provenance(source_event_ids=("event",)))
        item = await engine.register_procedure(proc, ("episode",))
        valid = report(candidate_id=item.id)
        await engine.submit_evaluation(valid, actor="evaluator")
        invalid = replace(valid, passed=True)
        invalid.metrics["task_success_rate"] = float("nan")
        with pytest.raises(ValueError):
            await registry.append_evaluation(invalid)
        with closing(sqlite3.connect(path)) as connection:
            metrics_json = (
                '{"task_success_rate":' + encoded
                + ',"task_success_delta":0.1,"cross_scope_leakage":0,"safety_violation_rate":0}'
            )
            connection.execute(
                "UPDATE evolution_evaluations SET metrics_json = ?", (metrics_json,)
            )
            connection.commit()
        with pytest.raises(ValueError):
            await engine.promote(item.id, actor="reviewer")
        assert (await registry.get(item.id)).state == EvolutionState.EVALUATED
    asyncio.run(scenario())


def test_finite_metrics_remain_accepted():
    assert DeterministicPromotionPolicy().evaluate(report()).passed
    policy = candidate()
    result = DeterministicRetrievalPolicyEvaluator().evaluate(policy, [sample()] * 30)
    pointer = RetrievalPolicyPointer("base")
    pointer.activate(policy, result)
    assert pointer.active_version == "new"
