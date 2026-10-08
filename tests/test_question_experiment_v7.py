"""A9 runner/statistics contracts plus actual offline SQLite runtime arms."""

import asyncio
import json
from dataclasses import replace

import pytest
from test_question_cost_v7 import request
from test_question_cost_v7 import run as fixture_run

from agent_memory.evaluation.evidence import _digest
from agent_memory.evaluation.question_cost import (
    AnswerOutcome,
    CostEntry,
    CostPhase,
    ModelEvidence,
    PendingResponsibility,
    RequestResult,
)
from agent_memory.evaluation.question_experiment import (
    canonical,
    run_experiment,
)
from agent_memory.evaluation.question_fixture import fixture_plan, lifecycle_plan, run_fixture
from agent_memory.evaluation.question_statistics import BootstrapProtocol, paired_bootstrap
from agent_memory.serialization import to_jsonable


def paired(cost=90):
    records = tuple(request(f"r{i}", group_id=f"g{i // 2}") for i in range(6))
    baseline = fixture_run(cost=180, requests=records)
    candidate = fixture_run(cost=cost, requests=records, strategy="coalesced_full")
    return candidate, baseline


def bootstrap(candidate, baseline, protocol=None):
    protocol = protocol or BootstrapProtocol(17, 200, 0.95, 3, 6)
    return paired_bootstrap(
        candidate, baseline, protocol, expected_protocol_sha256=protocol.fingerprint
    )


def test_group_bootstrap_reproducible_and_matches_constant_analytic_difference():
    candidate, baseline = paired()
    first, evidence = bootstrap(candidate, baseline)
    second, again = bootstrap(candidate, baseline)
    assert first == second and evidence == again
    assert evidence.group_count == 3
    assert evidence.effective_rate_difference_lower == evidence.effective_rate_difference_upper == 0
    assert (
        evidence.cost_difference_lower_microunits
        == evidence.cost_difference_upper_microunits
        == -15
    )
    other, _ = bootstrap(candidate, baseline, BootstrapProtocol(18, 200, 0.95, 3, 6))
    assert other["draws_sha256"] != first["draws_sha256"]


def test_cluster_ratios_pool_requests_and_keep_diagnostics_cost_in_all_denominator():
    records = (
        request("r1", group_id="a"),
        request("r2", group_id="b"),
        request("r3", group_id="b", outcome=AnswerOutcome.INCORRECT_REFUSAL),
        request("diagnostic", group_id="b", diagnostic_only=True),
    )
    baseline = fixture_run(cost=400, requests=records)
    candidate = fixture_run(cost=200, requests=records, strategy="coalesced_full")
    report, _ = bootstrap(candidate, baseline, BootstrapProtocol(1, 100, 0.95, 2, 4))
    assert report["metrics"]["cost_per_request_microunits"]["difference"] == -50
    assert report["metrics"]["cost_per_effective_answer_microunits"]["difference"] == -100
    assert report["group_count"] == 2 and report["request_count"] == 4


def test_zero_effective_cluster_resamples_block_without_dropping_failed_draws():
    records = (
        request("r1", group_id="a"),
        request("r2", group_id="b", outcome=AnswerOutcome.SYSTEM_ERROR),
    )
    candidate, baseline = (
        fixture_run(cost=100, requests=records),
        fixture_run(cost=200, requests=records),
    )
    report, evidence = bootstrap(candidate, baseline, BootstrapProtocol(1, 100, 0.95, 2, 2))
    assert evidence is None
    metric = report["metrics"]["cost_per_effective_answer_microunits"]
    assert metric["difference"] == -100
    assert 0 < metric["undefined_resamples"] < 100
    assert metric["interval"] is None
    assert report["metrics"]["effective_rate"]["interval"] == (0, 0)


@pytest.mark.parametrize("change", ["unknown", "pending", "missing_phase"])
def test_unknown_or_unfinished_cost_has_no_fabricated_cost_interval(change):
    candidate, baseline = paired()
    costs = candidate.costs
    if change == "unknown":
        costs = replace(costs, entries=(CostEntry("cpu", CostPhase.FOREGROUND, cpu_ms=1),))
    elif change == "pending":
        costs = replace(costs, pending=(PendingResponsibility("remaining", (), None),))
    else:
        costs = replace(costs, accounted_phases=(CostPhase.FOREGROUND,))
    report, evidence = bootstrap(replace(candidate, costs=costs), baseline)
    assert evidence is None and "unknown_or_pending_total_cost" in report["reasons"]
    assert report["metrics"]["cost_per_request_microunits"]["interval"] is None
    assert report["metrics"]["effective_rate"]["interval"] == (0, 0)


def test_no_samples_no_effective_answers_or_insufficient_groups_cannot_pass():
    candidate, baseline = paired()
    report, evidence = bootstrap(candidate, baseline, BootstrapProtocol(1, 10, 0.95, 4, 7))
    assert evidence is None
    assert {"insufficient_groups", "insufficient_requests"} <= set(report["reasons"])


@pytest.mark.parametrize(
    "change", ["group", "answerable", "dataset", "model", "judge", "currency", "workload"]
)
def test_statistical_comparison_rejects_nonpaired_inputs(change):
    candidate, baseline = paired()
    if change in {"group", "answerable"}:
        first = candidate.requests[0]
        new = (
            replace(first, group_id="other")
            if change == "group"
            else replace(first, answerable=False, outcome=AnswerOutcome.REASONABLE_UNKNOWN)
        )
        candidate = replace(candidate, requests=(new, *candidate.requests[1:]))
    elif change == "dataset":
        candidate = replace(candidate, dataset_sha256="f" * 64)
    elif change == "model":
        candidate = replace(candidate, semantic_model=ModelEvidence("f" * 64, "not_used"))
    elif change == "judge":
        candidate = replace(candidate, judge_sha256="f" * 64)
    elif change == "currency":
        candidate = replace(candidate, costs=replace(candidate.costs, currency="EUR"))
    else:
        candidate = replace(
            candidate, workload=replace(candidate.workload, security_sha256="f" * 64)
        )
    with pytest.raises(ValueError, match="incomparable"):
        bootstrap(candidate, baseline)


def test_statistics_and_plan_mutations_after_freeze_rejected():
    protocol = BootstrapProtocol(1, 10, 0.95, 1, 1)
    with pytest.raises(ValueError, match="after freeze"):
        paired_bootstrap(
            *paired(), replace(protocol, seed=2), expected_protocol_sha256=protocol.fingerprint
        )
    plan = fixture_plan(include_ablations=False)
    with pytest.raises(ValueError, match="after freeze"):
        asyncio.run(
            run_experiment(
                replace(plan, experiment_id="changed"),
                None,
                None,
                expected_plan_sha256=plan.fingerprint,
            )
        )


class Adapter:
    """Small runner contract double; actual runtime coverage is below."""

    instances = []

    def __init__(self, inputs, arm):
        # Golden labels cannot reach execution, including through factory input.
        assert all(item.gold_json is None for item in inputs.items)
        assert not hasattr(inputs, "acceptance") and not hasattr(inputs, "judge_sha256")
        self.adapter_revision = inputs.adapter_revision
        self.configuration_json = inputs.configuration_json
        self.initial_snapshot_json = inputs.initial_snapshot_json
        self.arm, self.observed, self.closed = arm.name, [], False
        self.instances.append(self)

    async def execute(self, phase, item):
        if item:
            assert item.gold_json is None
        self.observed.append((phase, _digest(item)))
        if item and item.identity == "failed":
            raise RuntimeError("private-payload-do-not-export")
        return {"text": "observed"}

    async def inspect(self, phase):
        return {"absent_phase_inspected": phase}

    async def finish(self):
        return (), {}, ()

    async def close(self):
        self.closed = True


def judge(item, output, latency):
    gold = json.loads(item.gold_json)
    return RequestResult(
        item.identity,
        item.group_id,
        gold["answerable"],
        AnswerOutcome.CORRECT_ANSWER,
        True,
        True,
        True,
        True,
        latency,
    )


def contract_run(plan=None, factory=Adapter, scorer=judge):
    plan = plan or fixture_plan(include_ablations=False, resamples=20)
    judge.configuration_sha256 = plan.judge_sha256
    return asyncio.run(run_experiment(plan, factory, scorer, expected_plan_sha256=plan.fingerprint))


def test_runner_freezes_same_inputs_orders_all_phases_and_retains_failed_requests():
    Adapter.instances = []
    result = contract_run()
    assert len(Adapter.instances) == 4 and all(a.closed for a in Adapter.instances)
    assert all(a.observed == Adapter.instances[0].observed for a in Adapter.instances)
    assert len({run.workload.fingerprint for run in result["runs"].values()}) == 1
    for arm, run in result["runs"].items():
        assert len(run.requests) == 8
        assert sum(r.effective_answer for r in run.requests) == 7
        assert set(run.costs.accounted_phases) == set(CostPhase)
        assert any(o["failed"] for o in result["observations"][arm])
        assert result["observations"][arm][-1]["evidence"] == "close isolated experiment resources"
        assert result["summaries"][arm].total_actual_microunits is None
        assert result["summaries"][arm].cost_per_effective_answer_microunits is None
        assert all(
            o["unmeasured_resources"] == ["gpu", "io", "storage", "network"]
            for o in result["observations"][arm]
        )
    assert "private-payload-do-not-export" not in json.dumps(to_jsonable(result))
    assert not result["cost_utility_ready"] and not result["production_benefit_claim"]
    assert all("missing_thresholds" in row["reasons"] for row in result["comparisons"])


def test_missing_gold_and_missing_judge_never_self_score_runtime_answer():
    plan = fixture_plan(include_ablations=False, resamples=20)
    no_gold = replace(plan, items=tuple(replace(item, gold_json=None) for item in plan.items))
    result = contract_run(no_gold)
    for arm in result["runs"]:
        assert result["summaries"][arm].effective_answers == 0
        assert "missing_gold" in result["blockers"][arm]
    result = contract_run(scorer=None)
    assert all("missing_judge" in reasons for reasons in result["blockers"].values())


@pytest.mark.parametrize("field", ["baseline_report_sha256", "candidate_configuration_sha256"])
def test_prebound_identity_is_never_silently_replaced(field):
    plan = fixture_plan(include_ablations=False, resamples=10)
    plan = replace(plan, acceptance=replace(plan.acceptance, **{field: "f" * 64}))
    with pytest.raises(ValueError, match="prebound"):
        contract_run(plan)


def test_changed_judge_config_shared_adapter_or_configuration_rejected():
    plan = fixture_plan(include_ablations=False, resamples=10)
    judge.configuration_sha256 = "f" * 64
    with pytest.raises(ValueError, match="judge configuration"):
        asyncio.run(run_experiment(plan, Adapter, judge, expected_plan_sha256=plan.fingerprint))
    adapter = None

    def shared(inputs, arm):
        nonlocal adapter
        adapter = adapter or Adapter(inputs, arm)
        return adapter

    with pytest.raises(ValueError, match="share an adapter"):
        contract_run(factory=shared)

    def changed(inputs, arm):
        adapter = Adapter(inputs, arm)
        adapter.configuration_json = "{}"
        return adapter

    with pytest.raises(ValueError, match="changed frozen"):
        contract_run(factory=changed)


def test_finalization_and_cleanup_failures_retain_explicit_unknown_debt():
    class Broken(Adapter):
        async def finish(self):
            raise RuntimeError("unavailable ledger")

        async def close(self):
            raise RuntimeError("failed cleanup")

    result = contract_run(factory=Broken)
    for arm in result["runs"]:
        assert result["summaries"][arm].pending_responsibilities == 2
        assert set(result["blockers"][arm]) == {
            "final_accounting_unavailable",
            "resource_cleanup_failed",
        }
        assert not any(row["ready"] for row in result["comparisons"])


def test_isolated_ablation_must_change_only_declared_control():
    plan = fixture_plan(resamples=10)
    left = plan.arms[1]
    controls = json.loads(left.controls_json)
    controls.update(full_compute=False, prewarm=False)
    with pytest.raises(ValueError, match="exactly its declared"):
        replace(
            plan,
            arms=tuple(
                replace(arm, controls_json=canonical(controls)) if arm == left else arm
                for arm in plan.arms
            ),
        )


@pytest.fixture(scope="module")
def runtime_report():
    return asyncio.run(run_fixture(fixture_plan(resamples=20)))


def test_actual_runtime_arms_use_full_delta_proof_cache_and_retain_failure(runtime_report):
    report = runtime_report
    for arm, run in report["runs"].items():
        assert report["blockers"][arm] == []
        assert len(run.requests) == 8 and sum(r.effective_answer for r in run.requests) == 7
        assert [r.request_id for r in run.requests if r.outcome == AnswerOutcome.SYSTEM_ERROR] == [
            "failed"
        ]
        summary = report["summaries"][arm]
        assert summary.total_actual_microunits is None and summary.pending_responsibilities == 0
        assert summary.unknown_token_calls == summary.model_calls
        assert any(e.phase == CostPhase.FAILURE and e.model_call for e in run.costs.entries)
        assert any(e.phase == CostPhase.RETRY and e.model_call for e in run.costs.entries)
    for arm in ("on_demand_full", "coalesced_full"):
        assert {
            o["compute_mode"]
            for o in report["runtime_operations"][arm]
            if o["event"] == "refresh_completed"
        } == {"full"}
    for arm in ("delta_proof", "exact_cache"):
        assert {"full", "delta", "proof_reuse"} <= {
            o["compute_mode"]
            for o in report["runtime_operations"][arm]
            if o["event"] == "refresh_completed"
        }
    assert any(o.get("cache_hit") for o in report["runtime_operations"]["exact_cache"])
    assert not any(
        o.get("cache_hit")
        for arm, rows in report["runtime_operations"].items()
        if arm != "exact_cache"
        for o in rows
    )
    assert (
        report["summaries"]["exact_cache"].model_calls
        < report["summaries"]["delta_proof"].model_calls
    )
    assert not report["production_benefit_claim"]


def test_actual_runtime_ablation_work_lag_waste_and_route_are_observed(runtime_report):
    operations = runtime_report["runtime_operations"]

    def refreshed(arm):
        return [o for o in operations[arm] if o["event"] == "refresh_completed"]

    assert len(refreshed("on_demand_full")) > len(refreshed("coalesced_full"))
    assert len(refreshed("eager_full")) > len(refreshed("coalesced_full"))
    assert any(o["oldest_obligation_lag_ms"] >= 2000 for o in refreshed("coalesced_full"))
    assert any(
        o["unused_by_foreground"] and o["phase"] == "drain" for o in refreshed("delta_proof")
    )
    assert any(
        o["event"] == "route" and o["route"] == "question" for o in operations["routed_full"]
    )
    assert any(o["phase"] == "background" for o in refreshed("coalesced_full"))
    assert not any(o["phase"] == "background" for o in refreshed("cold_full"))
    assert {c["varied_control"] for c in runtime_report["comparisons"]} >= {
        "full_compute",
        "exact_cache",
        "eager_after_write",
        "route_alias",
        "refresh_mode",
    }


@pytest.fixture(scope="module")
def lifecycle_report():
    return asyncio.run(run_fixture(lifecycle_plan(resamples=20)))


def test_all_templates_time_late_membership_revocation_and_erasure(lifecycle_report):
    report = lifecycle_report
    for arm, run_record in report["runs"].items():
        assert report["blockers"][arm] == []
        assert len(run_record.requests) == 13
        assert sum(r.effective_answer for r in run_record.requests) == 10
        assert not any(
            r.outcome
            in {
                AnswerOutcome.SYSTEM_ERROR,
                AnswerOutcome.INCORRECT_ANSWER,
                AnswerOutcome.INCORRECT_REFUSAL,
            }
            for r in run_record.requests
        )
        output = {
            row["item"]: row["output"]
            for row in report["traces"][arm]
            if row.get("item") and not row.get("failure_type")
        }
        assert json.loads(output["status-before"]["text"])["values"] == ["active"]
        assert output["status-before"]["rendered_qualifiers"][0]["exceptions"]
        assert output["risk-before"]["matched_ids"] == ["risk-1"]
        assert output["promise-before"]["matched_ids"] == []
        assert output["promise-before"]["text"] == "empty_known_scope"
        assert output["promise-overdue"]["matched_ids"] == ["promise-1"]
        assert output["owner-expired"]["text"] == "unknown"
        assert output["late-observed"]["text"] == '["Carol"]'
        assert output["old-membership-empty"]["matched_ids"] == []
        assert output["old-membership-empty"]["text"] == "empty_known_scope"
        assert output["new-membership"]["matched_ids"] == ["promise-1"]
        assert output["revoked-denial"]["denied"] == "project_processing_denied"
        assert output["erased-denial"]["denied"] == "question_erased"
        assert output["erase-owner"]["affected_events"] == 1
        assert report["summaries"][arm].pending_responsibilities == 0
        assert report["summaries"][arm].total_actual_microunits is None
        # No model attempt was attributed to either actual pre-dispatch denial.
        assert not any(
            set(e.request_ids) & {"revoked-denial", "erased-denial"}
            for e in run_record.costs.entries
            if e.model_call
        )


def test_guard_denial_is_not_blanket_success_or_gold_free_refusal():
    from agent_memory.evaluation.question_fixture import fixture_judge

    item = next(
        item for item in lifecycle_plan(resamples=10).items if item.identity == "revoked-denial"
    )
    output = dict(
        denied="project_processing_denied", evidence_complete=False, safe=True, fresh=True
    )
    assert fixture_judge(item, output, 1).outcome == AnswerOutcome.REASONABLE_UNKNOWN
    wrong = {**output, "denied": "some_unrelated_error"}
    assert fixture_judge(item, wrong, 1).outcome == AnswerOutcome.INCORRECT_ANSWER
    answerable = replace(item, gold_json=canonical({"answerable": True, "text": "answer"}))
    assert fixture_judge(answerable, output, 1).outcome == AnswerOutcome.INCORRECT_ANSWER


def test_qualifier_fidelity_is_independent_of_value_correctness_and_diagnostics():
    from agent_memory.evaluation.question_fixture import fixture_judge

    item = next(
        item for item in lifecycle_plan(resamples=10).items if item.identity == "status-before"
    )
    gold = json.loads(item.gold_json)
    output = dict(
        text=gold["text"],
        evidence_complete=True,
        evidence_source_ids=["status"],
        safe=True,
        fresh=True,
        rendered_qualifiers=gold["qualifiers"],
    )
    assert fixture_judge(item, output, 1).effective_answer
    # Even equal value/text cannot certify conditions or exceptions on its own.
    dropped = {**output, "rendered_qualifiers": []}
    result = fixture_judge(item, dropped, 1)
    assert result.outcome == AnswerOutcome.CORRECT_ANSWER
    assert not result.qualifiers_preserved and not result.effective_answer
    wrong_exception = json.loads(canonical(gold["qualifiers"]))
    wrong_exception[0]["exceptions"] = []
    assert not fixture_judge(
        item, {**output, "rendered_qualifiers": wrong_exception}, 1
    ).qualifiers_preserved
    missing = replace(
        item,
        gold_json=canonical({key: value for key, value in gold.items() if key != "qualifiers"}),
    )
    assert not fixture_judge(missing, output, 1).qualifiers_preserved
    diagnostic = replace(item, gold_json=canonical({**gold, "diagnostic_only": True}))
    result = fixture_judge(diagnostic, output, 1)
    assert result.diagnostic_only and not result.effective_answer


def resource_plan():
    from agent_memory.evaluation.question_resources import (
        ResourcePricingProtocol,
        ResourceRate,
        ResourceTariff,
    )

    tariff = ResourceTariff(
        "USD",
        "synthetic-test-only-not-an-actual-tariff",
        tuple(
            ResourceRate(kind, unit, "1")
            for kind, unit in (
                ("cpu", "ms"),
                ("gpu", "ms"),
                ("io", "bytes"),
                ("storage", "byte_seconds"),
                ("network", "bytes"),
            )
        ),
    )
    return replace(
        fixture_plan(resamples=20, include_ablations=False),
        resource_pricing=ResourcePricingProtocol("USD", "e" * 64, tariff),
    )


class SyntheticObserver:
    configuration_sha256 = "e" * 64

    def __init__(self, *_):
        self.started, self.finished, self.finalized = 0, 0, False

    def begin(self, phase, request_ids):
        self.started += 1
        return (phase, request_ids)

    def end(self, token, observation):
        from agent_memory.evaluation.question_resources import (
            PhaseResourceObservation,
            ResourceMeasurement,
        )

        assert token[0] == observation.phase
        self.finished += 1
        return PhaseResourceObservation(
            observation,
            tuple(
                ResourceMeasurement(
                    kind, unit, "0", "explicit synthetic counter fixture; not host telemetry"
                )
                for kind, unit in (
                    ("gpu", "ms"),
                    ("io", "bytes"),
                    ("storage", "byte_seconds"),
                    ("network", "bytes"),
                )
            ),
        )

    def finalize(self):
        assert self.started == self.finished
        self.finalized = True


def observed_run(observer_factory=SyntheticObserver):
    plan = resource_plan()
    judge.configuration_sha256 = plan.judge_sha256
    return asyncio.run(
        run_experiment(
            plan,
            Adapter,
            judge,
            expected_plan_sha256=plan.fingerprint,
            observer_factory=observer_factory,
        )
    )


def test_frozen_observer_brackets_every_phase_and_explicit_tariff_can_price_complete_records():
    observers = []

    def factory(*args):
        observer = SyntheticObserver(*args)
        observers.append(observer)
        return observer

    result = observed_run(factory)
    assert all(
        observer.finalized and observer.started == observer.finished for observer in observers
    )
    for arm, run_record in result["runs"].items():
        assert result["summaries"][arm].total_actual_microunits is not None
        assert result["summaries"][arm].unresolved_cost_entries == 0
        assert len(result["resource_reports"][arm]) == len(result["observations"][arm])
        assert all(
            record.pricing == "priced" and not record.missing_resources
            for record in result["resource_reports"][arm]
        )
        assert sum(entry.priced_resource_microunits for entry in run_record.costs.entries) == (
            result["summaries"][arm].total_actual_microunits
        )
    # A synthetic observer/rate contract test is never production benefit evidence.
    assert not result["production_benefit_claim"] and not result["cost_utility_ready"]


@pytest.mark.parametrize(
    "broken", ["begin", "end", "finalize", "false_finalize", "missing_dimension"]
)
def test_failed_or_incomplete_observer_never_prices_unknown_resources(broken):
    class Broken(SyntheticObserver):
        def begin(self, *args):
            if broken == "begin":
                raise OSError("private telemetry failure body")
            return super().begin(*args)

        def end(self, *args):
            if broken == "end":
                raise OSError("private telemetry failure body")
            record = super().end(*args)
            return (
                replace(record, measurements=record.measurements[:-1])
                if broken == "missing_dimension"
                else record
            )

        def finalize(self):
            if broken == "finalize":
                raise OSError("private telemetry failure body")
            if broken == "false_finalize":
                return False
            return super().finalize()

    result = observed_run(Broken)
    for arm in result["runs"]:
        assert result["summaries"][arm].total_actual_microunits is None
        assert not any(record.pricing == "priced" for record in result["resource_reports"][arm])
    assert "private telemetry failure body" not in json.dumps(to_jsonable(result))


def test_resource_observer_configuration_and_cross_arm_instance_are_checked_before_use():
    class Wrong(SyntheticObserver):
        configuration_sha256 = "f" * 64

    with pytest.raises(ValueError, match="observer differs"):
        observed_run(Wrong)
    shared = SyntheticObserver()
    with pytest.raises(ValueError, match="share a resource observer"):
        observed_run(lambda *_: shared)


def test_changed_observer_or_tariff_changes_shared_comparability_and_arm_identity():
    plan = resource_plan()
    protocol = plan.resource_pricing
    changed = replace(
        plan,
        resource_pricing=replace(
            protocol, tariff=replace(protocol.tariff, reference="different-synthetic-tariff")
        ),
    )
    assert plan.workload != changed.workload
    assert plan.arm_configuration(plan.arms[0]) != changed.arm_configuration(changed.arms[0])
    first = observed_run()["runs"]["coalesced_full"]
    with pytest.raises(ValueError, match="incomparable"):
        bootstrap(first, replace(first, workload=changed.workload))


def configured_port_test_plan():
    """Synthetic preflight data only: never execute an actual model endpoint."""
    from dataclasses import asdict

    from agent_memory.evaluation.question_fixture import TEMPLATE, model_configuration

    plan = resource_plan()
    cfg = replace(model_configuration(), provider="ollama", endpoint="http://127.0.0.1:1")
    config = json.loads(plan.configuration_json)
    config.update(
        model=asdict(cfg), public_template=TEMPLATE, host_review_reference="synthetic-host-review"
    )
    initial = json.loads(plan.initial_snapshot_json)
    initial["source_authority"] = dict(
        source_id="test-host",
        kind="tool_observation",
        subjects=["project-a", "project-b"],
        predicates=["project.owner"],
    )
    for source in initial["sources"]:
        text = source["value"]
        source.update(
            text=text,
            reviewed_span=dict(start=0, end=len(text), quote=text),
            review_reference="synthetic-source-review",
            occurred_at="2026-01-01T00:00:00+00:00",
        )
    return replace(
        plan,
        configuration_json=canonical(config),
        initial_snapshot_json=canonical(initial),
        items=plan.items[:3],
        dataset_kind="licensed_real",
        license_reference="synthetic-contract-test-only",
        generation_model=ModelEvidence(cfg.fingerprint, "real"),
    )


def test_real_port_binding_is_opt_in_and_missing_provenance_blocks_before_network(monkeypatch):
    from agent_memory.evaluation import question_fixture as fixture

    plan = configured_port_test_plan()
    invoked = []

    async def forbidden(*args, **kwargs):
        invoked.append(True)
        raise AssertionError("incomplete preflight reached execution")

    monkeypatch.setattr(fixture, "run_experiment", forbidden)
    for changed, options, pattern in (
        (plan, {}, "explicit opt-in"),
        (replace(plan, license_reference=None), {"allow_real_model": True}, "licensed dataset"),
        (
            replace(plan, items=tuple(replace(item, gold_json=None) for item in plan.items)),
            {"allow_real_model": True},
            "frozen gold",
        ),
        (replace(plan, resource_pricing=None), {"allow_real_model": True}, "resource observer"),
    ):
        with pytest.raises(ValueError, match=pattern):
            asyncio.run(
                fixture.run_ollama_experiment(
                    changed,
                    fixture.fixture_judge,
                    SyntheticObserver,
                    expected_plan_sha256=changed.fingerprint,
                    **options,
                )
            )
    assert not invoked


def test_configured_real_binding_constructs_existing_ollama_port_without_executing_it(monkeypatch):
    from agent_memory.evaluation import question_fixture as fixture
    from agent_memory.evaluation.question_experiment import ExecutionInputs
    from agent_memory.retrieval.ollama import OllamaPort

    plan = configured_port_test_plan()
    constructed = []

    async def inspect_only(requested, factory, judge, **options):
        inputs = ExecutionInputs(
            requested.adapter_revision,
            requested.configuration_json,
            requested.initial_snapshot_json,
            tuple(replace(item, gold_json=None) for item in requested.items),
            requested.dataset_kind,
            requested.semantic_model,
            requested.generation_model,
            requested.workload,
        )
        adapter = factory(inputs, requested.arms[0])
        try:
            await adapter.execute(CostPhase.COLD_START, None)
            await adapter.execute(CostPhase.REGISTRATION, None)
            assert type(adapter.port) is OllamaPort
            assert (
                adapter.port.configuration.fingerprint
                == requested.generation_model.configuration_sha256
            )
            constructed.append(True)
        finally:
            await adapter.close()
        return {"production_benefit_claim": False}

    def network_forbidden(*args, **kwargs):
        raise AssertionError("this test never calls an endpoint")

    monkeypatch.setattr(fixture, "run_experiment", inspect_only)
    monkeypatch.setattr(OllamaPort, "_http", network_forbidden)
    report = asyncio.run(
        fixture.run_ollama_experiment(
            plan,
            fixture.fixture_judge,
            SyntheticObserver,
            expected_plan_sha256=plan.fingerprint,
            allow_real_model=True,
        )
    )
    assert constructed and not report["production_benefit_claim"]
