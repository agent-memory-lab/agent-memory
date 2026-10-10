"""Offline contracts only: fake authorization/labels never establish business quality."""

import asyncio
import json
from dataclasses import replace
from hashlib import sha256

import pytest
from test_real_acceptance_v71 import fixture

from agent_memory.evaluation.question_cost import AnswerOutcome, CostPhase, RequestResult
from agent_memory.evaluation.question_experiment import ExperimentContrast, WorkItem
from agent_memory.evaluation.question_fixture import fixture_plan
from agent_memory.evaluation.real_acceptance import (
    WORKFLOW_FEATURES,
    WORKFLOW_SLICES,
    RealWorkflowAcceptancePlan,
    load_inputs,
    run_real_workflow_acceptance,
    workflow_feature_ablations,
)
from agent_memory.retrieval.model_contracts import canonical, digest


def workflow_fixture(tmp_path):
    path, _ = fixture(tmp_path)
    inputs = load_inputs(path, expected_manifest_sha256=sha256(path.read_bytes()).hexdigest())
    original = fixture_plan(include_ablations=False, resamples=10)
    # Synthetic contract declarations deliberately have no real provider receipts.
    plan = replace(
        original,
        dataset_kind="licensed_real",
        license_reference=inputs.corpus_license_reference,
        semantic_model=replace(original.semantic_model, execution="real"),
        generation_model=replace(original.generation_model, execution="real"),
        judge_sha256=dict(inputs.artifacts)["judge"],
        acceptance=replace(original.acceptance, calibration_reference=inputs.calibration_reference),
        items=(
            WorkItem("raw-source", CostPhase.WRITE, '{"test_only":true}'),
            *(
                WorkItem(
                    name,
                    CostPhase.FOREGROUND,
                    canonical({"request": name}),
                    f"group-{index % 2}",
                    canonical({"answerable": name != "omission_false_acceptance"}),
                )
                for index, name in enumerate(WORKFLOW_SLICES)
            ),
        ),
    )
    plan = workflow_feature_ablations(plan)
    hashes = dict(inputs.artifacts)
    protocol = RealWorkflowAcceptancePlan(
        plan,
        hashes["corpus"],
        hashes["gold"],
        hashes["pricing"],
        tuple((name, (name,)) for name in WORKFLOW_SLICES),
    )
    return inputs, protocol


class ContractAdapter:
    """No model, corpus or network use; the real evidence gate must stay blocked."""

    instances = []

    def __init__(self, inputs, arm):
        assert all(item.gold_json is None for item in inputs.items)
        assert not hasattr(inputs, "judge_sha256")
        self.adapter_revision = inputs.adapter_revision
        self.configuration_json = inputs.configuration_json
        self.initial_snapshot_json = inputs.initial_snapshot_json
        self.acceptance_controls_sha256 = digest(json.loads(arm.controls_json))
        self.phases = []
        self.closed = False
        self.instances.append(self)

    async def execute(self, phase, item):
        if item is not None:
            assert item.gold_json is None
        self.phases.append(phase)
        return {"test_only": True}

    async def inspect(self, phase):
        self.phases.append(phase)
        return {"absent_phase_inspected": phase.value}

    async def finish(self):
        return (), {}, ()

    async def close(self):
        self.closed = True


def judge(item, output, latency_ms):
    assert output["test_only"]
    answerable = json.loads(item.gold_json)["answerable"]
    return RequestResult(
        item.identity,
        item.group_id,
        answerable,
        AnswerOutcome.CORRECT_ANSWER if answerable else AnswerOutcome.REASONABLE_UNKNOWN,
        True,
        True,
        True,
        True,
        latency_ms,
    )


async def authorize(inputs, protocol):
    return True


def run(inputs, protocol, *, factory=ContractAdapter, authority=authorize, scorer=judge):
    judge.configuration_sha256 = protocol.experiment.judge_sha256
    return asyncio.run(
        run_real_workflow_acceptance(
            inputs,
            protocol,
            factory,
            scorer,
            authorize_inputs=authority,
            expected_protocol_sha256=protocol.fingerprint,
        )
    )


def test_feature_parameterization_preserves_baseline_and_isolates_three_controls(tmp_path):
    _, protocol = workflow_fixture(tmp_path)
    plan = protocol.experiment
    arms = {arm.name: arm for arm in plan.arms}
    assert len(arms) == 8
    assert {arm.strategy for arm in arms.values()} == {
        "on_demand_full",
        "coalesced_full",
        "delta_proof",
        "exact_cache",
    }
    baseline = arms[protocol.baseline_arm]
    assert all(json.loads(baseline.controls_json)[key] is False for key in WORKFLOW_FEATURES)
    for feature in WORKFLOW_FEATURES:
        candidate = arms[f"exact_cache+{feature}"]
        assert candidate.strategy == baseline.strategy
        controls = json.loads(candidate.controls_json)
        assert [key for key in WORKFLOW_FEATURES if controls[key]] == [feature]
        assert ExperimentContrast(candidate.name, baseline.name, feature, feature) in plan.contrasts
    assert all(
        json.loads(arms["exact_cache+all_features"].controls_json)[key] for key in WORKFLOW_FEATURES
    )
    with pytest.raises(ValueError, match="already_present"):
        workflow_feature_ablations(plan)


def test_isolated_workflow_contrast_cannot_hide_strategy_change(tmp_path):
    _, protocol = workflow_fixture(tmp_path)
    plan = protocol.experiment
    arms = tuple(
        replace(arm, strategy="coalesced_full") if arm.name == "exact_cache+shared_proof" else arm
        for arm in plan.arms
    )
    with pytest.raises(ValueError, match="isolated_workflow_feature"):
        replace(protocol, experiment=replace(plan, arms=arms))


def test_existing_a9_runner_keeps_full_costs_gold_separation_and_business_gate_blocked(tmp_path):
    inputs, protocol = workflow_fixture(tmp_path)
    ContractAdapter.instances = []
    report = run(inputs, protocol)
    assert len(ContractAdapter.instances) == 8
    assert all(adapter.closed for adapter in ContractAdapter.instances)
    assert all(set(adapter.phases) == set(CostPhase) for adapter in ContractAdapter.instances)
    assert not report["cost_utility_ready"] and not report["production_benefit_claim"]
    assert report["real_acceptance"]["coverage_is_descriptive"]
    assert not report["real_acceptance"]["feature_quality_promotion_assessed"]
    assert report["real_acceptance"]["protocol_sha256"] == protocol.fingerprint
    for arm, summary in report["summaries"].items():
        assert summary.total_actual_microunits is None
        assert summary.cost_per_request_microunits is None
        assert summary.cost_per_effective_answer_microunits is None
        assert summary.unresolved_cost_entries > 0
        assert summary.all_requests == len(WORKFLOW_SLICES)
        slices = report["real_acceptance"]["coverage"][arm]
        assert set(slices) == set(WORKFLOW_SLICES)
        assert slices["omission_recall"]["effective_answers"] == 1
        assert slices["omission_false_acceptance"]["outcomes"]["reasonable_unknown"] == 1
    assert any(
        "semantic_model_execution_unproven" in reason
        for comparison in report["comparisons"]
        for reason in comparison["reasons"]
    )


@pytest.mark.parametrize("field", ["corpus_sha256", "gold_sha256", "pricing_sha256"])
def test_changed_artifact_binding_is_rejected_before_adapter(tmp_path, field):
    inputs, protocol = workflow_fixture(tmp_path)
    protocol = replace(protocol, **{field: "f" * 64})
    with pytest.raises(ValueError, match="input_mismatch"):
        run(inputs, protocol, factory=lambda *_: pytest.fail("adapter started"))


@pytest.mark.parametrize("change", ["judge", "license", "calibration"])
def test_changed_host_binding_is_rejected_before_adapter(tmp_path, change):
    inputs, protocol = workflow_fixture(tmp_path)
    plan = protocol.experiment
    if change == "judge":
        plan = replace(plan, judge_sha256="f" * 64)
    elif change == "license":
        plan = replace(plan, license_reference="other-license")
    else:
        plan = replace(plan, acceptance=replace(plan.acceptance, calibration_reference="other"))
    with pytest.raises(ValueError, match="input_mismatch"):
        run(
            inputs,
            replace(protocol, experiment=plan),
            factory=lambda *_: pytest.fail("adapter started"),
        )


@pytest.mark.parametrize("change", ["synthetic", "no_semantic", "stub_generation"])
def test_annotation_only_or_synthetic_adapter_cannot_claim_raw_workflow(tmp_path, change):
    _, protocol = workflow_fixture(tmp_path)
    plan = protocol.experiment
    if change == "synthetic":
        plan = replace(plan, dataset_kind="synthetic")
    elif change == "no_semantic":
        plan = replace(plan, semantic_model=replace(plan.semantic_model, execution="not_used"))
    else:
        plan = replace(plan, generation_model=replace(plan.generation_model, execution="stub"))
    with pytest.raises(ValueError, match="raw_source_workflow"):
        replace(protocol, experiment=plan)


@pytest.mark.parametrize("change", ["missing", "duplicate", "unknown_request", "duplicate_id"])
def test_coverage_must_bind_all_required_slices_to_exact_requests(tmp_path, change):
    _, protocol = workflow_fixture(tmp_path)
    coverage = protocol.coverage
    if change == "missing":
        coverage = coverage[:-1]
    elif change == "duplicate":
        coverage = (coverage[0], *coverage[:-1])
    elif change == "unknown_request":
        coverage = ((coverage[0][0], ("absent",)), *coverage[1:])
    else:
        coverage = ((coverage[0][0], coverage[0][1] * 2), *coverage[1:])
    with pytest.raises(ValueError, match="coverage"):
        replace(protocol, coverage=coverage)


@pytest.mark.parametrize(
    "change", ["missing", "diagnostic", "negative_recall", "positive_negative"]
)
def test_omission_requires_both_positive_and_negative_nondiagnostic_gold(tmp_path, change):
    _, protocol = workflow_fixture(tmp_path)
    target = "omission_false_acceptance" if change == "positive_negative" else "omission_recall"
    if change == "missing":
        gold = None
    else:
        gold = canonical(
            {"answerable": change != "negative_recall", "diagnostic_only": change == "diagnostic"}
        )
    items = tuple(
        replace(item, gold_json=gold) if item.identity == target else item
        for item in protocol.experiment.items
    )
    with pytest.raises(ValueError, match="gold_required"):
        replace(protocol, experiment=replace(protocol.experiment, items=items))


def test_controls_must_be_attested_by_actual_host_adapter(tmp_path):
    inputs, protocol = workflow_fixture(tmp_path)

    def unattested(execution, arm):
        adapter = ContractAdapter(execution, arm)
        del adapter.acceptance_controls_sha256
        return adapter

    with pytest.raises(ValueError, match="controls_not_attested"):
        run(inputs, protocol, factory=unattested)


@pytest.mark.parametrize("late", [False, True])
def test_host_rejection_before_execution_or_delivery_never_returns_report(tmp_path, late):
    inputs, protocol = workflow_fixture(tmp_path)
    calls = []

    async def reject(inputs, protocol):
        calls.append(protocol.fingerprint)
        return late and len(calls) == 1

    ContractAdapter.instances = []
    with pytest.raises(PermissionError, match="not_authorized"):
        run(inputs, protocol, authority=reject)
    assert len(ContractAdapter.instances) == (8 if late else 0)
    assert len(calls) == (2 if late else 1)


def test_authorizer_cannot_mutate_frozen_protocol_before_model_execution(tmp_path):
    inputs, protocol = workflow_fixture(tmp_path)

    async def mutate(inputs, protocol):
        object.__setattr__(protocol.experiment, "experiment_id", "changed-after-approval")
        return True

    with pytest.raises(ValueError, match="protocol_changed"):
        run(inputs, protocol, authority=mutate, factory=lambda *_: pytest.fail("adapter started"))


def test_artifact_change_during_work_prevents_delivery(tmp_path):
    inputs, protocol = workflow_fixture(tmp_path)

    class ChangingAdapter(ContractAdapter):
        async def close(self):
            await super().close()
            (tmp_path / "gold.json").write_text('{"changed":true}')

    with pytest.raises(ValueError, match="artifact_changed"):
        run(inputs, protocol, factory=ChangingAdapter)


@pytest.mark.parametrize("artifact", ["gold", "pricing"])
@pytest.mark.parametrize("late", [False, True])
def test_artifact_mutation_during_authorization_blocks_execution_or_delivery(
    tmp_path, artifact, late
):
    inputs, protocol = workflow_fixture(tmp_path)
    calls = []

    async def mutate(inputs, protocol):
        calls.append(protocol.fingerprint)
        await asyncio.sleep(0)
        if len(calls) == (2 if late else 1):
            (tmp_path / f"{artifact}.json").write_text('{"changed_during_authorization":true}')
        return True

    ContractAdapter.instances = []
    with pytest.raises(ValueError, match="artifact_changed"):
        run(inputs, protocol, authority=mutate)
    assert len(ContractAdapter.instances) == (8 if late else 0)
    assert len(calls) == (2 if late else 1)


def test_failed_requests_are_visible_in_slice_counts_and_whole_denominator(tmp_path):
    inputs, protocol = workflow_fixture(tmp_path)

    class FailingAdapter(ContractAdapter):
        async def execute(self, phase, item):
            if item is not None and item.identity == "omission_false_acceptance":
                raise RuntimeError("private-test-payload")
            return await super().execute(phase, item)

    report = run(inputs, protocol, factory=FailingAdapter)
    for arm, summary in report["summaries"].items():
        assert summary.all_requests == len(WORKFLOW_SLICES)
        counts = report["real_acceptance"]["coverage"][arm]["omission_false_acceptance"]
        assert counts["outcomes"]["system_error"] == 1
        assert counts["outcomes"]["reasonable_unknown"] == 0
    assert not report["cost_utility_ready"]


@pytest.mark.parametrize("change", ["baseline_enabled", "missing_isolated", "missing_combined"])
def test_fixed_baseline_and_complete_feature_contrasts_are_required(tmp_path, change):
    _, protocol = workflow_fixture(tmp_path)
    plan = protocol.experiment
    if change == "baseline_enabled":
        arms = []
        for arm in plan.arms:
            controls = json.loads(arm.controls_json)
            if arm.name == protocol.baseline_arm:
                controls["shared_proof"] = True
            arms.append(replace(arm, controls_json=canonical(controls)))
        # Remove isolated contrasts here so the wrapper owns the baseline check.
        plan = replace(
            plan,
            arms=tuple(arms),
            contrasts=tuple(contrast for contrast in plan.contrasts if not contrast.varied_control),
        )
        expected = "ablation_controls"
    else:
        candidate = (
            "exact_cache+shared_proof"
            if change == "missing_isolated"
            else "exact_cache+all_features"
        )
        plan = replace(
            plan,
            contrasts=tuple(
                contrast for contrast in plan.contrasts if contrast.candidate != candidate
            ),
        )
        expected = "workflow.*contrast"
    with pytest.raises(ValueError, match=expected):
        replace(protocol, experiment=plan)


@pytest.mark.parametrize("part", ["manifest", "artifacts", "artifact"])
def test_malformed_input_shapes_fail_as_preflight_errors(tmp_path, part):
    path, doc = fixture(tmp_path)
    if part == "manifest":
        doc = []
    elif part == "artifacts":
        doc["artifacts"] = []
    else:
        doc["artifacts"]["corpus"] = []
    path.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="acceptance"):
        load_inputs(path, expected_manifest_sha256=sha256(path.read_bytes()).hexdigest())


def test_existing_retrieval_wrapper_is_unchanged_and_contract_arms_stay_unpromoted(tmp_path):
    from test_controlled_retrieval_promotion import ARMS, ContractArm, assess
    from test_controlled_retrieval_promotion import plan as retrieval_plan

    from agent_memory.evaluation.real_acceptance import run_real_acceptance

    path, _ = fixture(tmp_path)
    inputs = load_inputs(path, expected_manifest_sha256=sha256(path.read_bytes()).hexdigest())
    hashes = dict(inputs.artifacts)
    plan = replace(
        retrieval_plan(),
        corpus_sha256=hashes["corpus"],
        evidence_annotations_sha256=hashes["gold"],
        judge_sha256=hashes["judge"],
    )
    arms = tuple(ContractArm(name, plan) for name in ARMS)
    report = asyncio.run(
        run_real_acceptance(
            inputs,
            plan,
            arms,
            authorize_inputs=authorize,
            expected_protocol_sha256=plan.fingerprint,
        )
    )
    assert len(report.rows) == len(plan.cases) * 4 * 2
    assert all(not decision.eligible for decision in assess(plan, report))
