"""Sequential A9 workload orchestration, kept outside all production imports.

Adapters own their isolated runtime, finite work queue and real provider receipts.
The runner owns ordering, phase measurements, failure retention, frozen inputs,
paired statistics and fail-closed acceptance. Adapter/judge declarations are
trusted host inputs, not proofs of licensing or real model execution.
"""

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from inspect import isawaitable
from time import perf_counter_ns
from typing import Protocol

from ..serialization import to_jsonable
from .evidence import _digest, _text
from .model_experiment import ModelExperimentAccounting
from .question_cost import (
    AnswerOutcome,
    CostPhase,
    ModelEvidence,
    PendingResponsibility,
    QuestionAcceptanceProfile,
    QuestionCostRun,
    RequestResult,
    WorkloadManifest,
    evaluate_question_acceptance,
    request_allocations,
    summarize_costs,
)
from .question_resources import (
    PhaseResourceObservation,
    ResourcePricingProtocol,
    reconcile_resources,
    resource_observation_request,
)
from .question_statistics import BootstrapProtocol, paired_bootstrap

ARMS = ("on_demand_full", "coalesced_full", "delta_proof", "exact_cache")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True, slots=True)
class ExperimentArm:
    name: str
    strategy: str
    controls_json: str = "{}"

    def __post_init__(self):
        _text(self.name, "arm name")
        if self.strategy not in ARMS:
            raise ValueError("unsupported strategy")
        value = json.loads(self.controls_json)
        if not isinstance(value, dict):
            raise ValueError("arm controls must be an object")
        object.__setattr__(self, "controls_json", canonical(value))


@dataclass(frozen=True, slots=True)
class ExperimentContrast:
    candidate: str
    baseline: str
    attribution: str
    varied_control: str | None = None

    def __post_init__(self):
        for name in ("candidate", "baseline", "attribution"):
            _text(getattr(self, name), name)
        if self.candidate == self.baseline:
            raise ValueError("contrast requires distinct arms")


DEFAULT_ARMS = tuple(ExperimentArm(name, name) for name in ARMS)
DEFAULT_CONTRASTS = (
    ExperimentContrast("coalesced_full", "on_demand_full", "bundled_maintenance_policy"),
    ExperimentContrast("delta_proof", "coalesced_full", "delta_proof"),
    ExperimentContrast("exact_cache", "delta_proof", "exact_model_cache"),
    ExperimentContrast("delta_proof", "on_demand_full", "bundled_maintenance_policy"),
    ExperimentContrast("exact_cache", "on_demand_full", "bundled_maintenance_policy"),
)


@dataclass(frozen=True, slots=True)
class WorkItem:
    identity: str
    phase: CostPhase
    payload_json: str
    group_id: str | None = None
    gold_json: str | None = None

    def __post_init__(self):
        _text(self.identity, "identity")
        object.__setattr__(self, "phase", CostPhase(self.phase))
        if self.phase not in {
            CostPhase.WRITE,
            CostPhase.BACKGROUND,
            CostPhase.FOREGROUND,
            CostPhase.FAILURE,
            CostPhase.RETRY,
        }:
            raise ValueError("workload item cannot override lifecycle ordering")
        for name in ("payload_json", "gold_json"):
            value = getattr(self, name)
            if value is not None:
                decoded = json.loads(value)
                if not isinstance(decoded, dict):
                    raise ValueError(f"{name} must encode an object")
                object.__setattr__(self, name, canonical(decoded))
        if self.is_request:
            _text(self.group_id, "group_id")
        elif self.group_id is not None or self.gold_json is not None:
            raise ValueError("only requests have groups and gold")

    @property
    def is_request(self):
        return self.phase in {CostPhase.FOREGROUND, CostPhase.FAILURE, CostPhase.RETRY}


@dataclass(frozen=True, slots=True)
class ExperimentPlan:
    experiment_id: str
    adapter_revision: str
    configuration_json: str
    initial_snapshot_json: str
    items: tuple[WorkItem, ...]
    dataset_kind: str
    license_reference: str | None
    judge_sha256: str
    semantic_model: ModelEvidence
    generation_model: ModelEvidence
    statistics: BootstrapProtocol
    acceptance: QuestionAcceptanceProfile
    currency: str = "USD"
    arms: tuple[ExperimentArm, ...] = DEFAULT_ARMS
    contrasts: tuple[ExperimentContrast, ...] = DEFAULT_CONTRASTS
    resource_pricing: ResourcePricingProtocol | None = None

    def __post_init__(self):
        for name in ("experiment_id", "adapter_revision"):
            _text(getattr(self, name), name)
        if self.dataset_kind not in {"synthetic", "licensed_real"}:
            raise ValueError("invalid dataset kind")
        if (
            type(self.statistics) is not BootstrapProtocol
            or type(self.acceptance) is not QuestionAcceptanceProfile
        ):
            raise ValueError("typed statistics and acceptance profile required")
        if any(
            type(model) is not ModelEvidence
            for model in (self.semantic_model, self.generation_model)
        ):
            raise ValueError("typed model evidence required")
        for name in ("configuration_json", "initial_snapshot_json"):
            object.__setattr__(self, name, canonical(json.loads(getattr(self, name))))
        object.__setattr__(self, "items", tuple(self.items))
        if not self.items or any(type(item) is not WorkItem for item in self.items):
            raise ValueError("nonempty typed workload required")
        if len({item.identity for item in self.items}) != len(self.items):
            raise ValueError("duplicate workload item")
        if not any(item.is_request for item in self.items):
            raise ValueError("at least one request required")
        config = json.loads(self.configuration_json)
        required = {
            "semantics",
            "security",
            "budget",
            "hardware",
            "drain_policy",
            "model",
            "code",
            "backend",
        }
        if not isinstance(config, dict) or not required <= config.keys():
            raise ValueError("complete shared configuration required")
        # Thresholds, sample requirements and judge cannot be patched after observing runs.
        if self.acceptance.statistics_protocol_sha256 not in (None, self.statistics.fingerprint):
            raise ValueError("acceptance statistics differ from plan")
        if self.resource_pricing is not None and (
            type(self.resource_pricing) is not ResourcePricingProtocol
            or self.resource_pricing.currency != self.currency
        ):
            raise ValueError("resource pricing must match the frozen currency")
        object.__setattr__(self, "arms", tuple(self.arms))
        object.__setattr__(self, "contrasts", tuple(self.contrasts))
        if (
            not self.arms
            or any(type(arm) is not ExperimentArm for arm in self.arms)
            or len({arm.name for arm in self.arms}) != len(self.arms)
        ):
            raise ValueError("distinct typed arms required")
        if not set(ARMS) <= {arm.strategy for arm in self.arms}:
            raise ValueError("four primary strategies required")
        arms = {arm.name: arm for arm in self.arms}
        if not self.contrasts:
            raise ValueError("contrasts required")
        for contrast in self.contrasts:
            if (
                type(contrast) is not ExperimentContrast
                or not {contrast.candidate, contrast.baseline} <= arms.keys()
            ):
                raise ValueError("contrast references unknown arms")
            if contrast.varied_control:
                left, right = (
                    json.loads(arms[name].controls_json)
                    for name in (contrast.candidate, contrast.baseline)
                )
                changed = {
                    key for key in left.keys() | right.keys() if left.get(key) != right.get(key)
                }
                if changed != {contrast.varied_control}:
                    raise ValueError("isolated contrast must vary exactly its declared control")

    @property
    def fingerprint(self):
        return _digest(self)

    @property
    def dataset_sha256(self):
        return _digest((self.initial_snapshot_json, self.items))

    @property
    def workload(self):
        config = json.loads(self.configuration_json)
        # The same counters under a different observer/tariff are not a paired
        # cost experiment, even if all source events and requests match.
        config["budget"] = [config["budget"], self.resource_pricing]
        requests = tuple(item for item in self.items if item.is_request)
        return WorkloadManifest(
            tuple(item.identity for item in requests),
            _digest(self.items),
            _digest(self.initial_snapshot_json),
            _digest(tuple((item.identity, item.phase, item.group_id) for item in self.items)),
            *(
                _digest(config[name])
                for name in ("semantics", "security", "budget", "hardware", "drain_policy")
            ),
        )

    def arm_configuration(self, arm):
        if arm not in self.arms:
            raise ValueError("unknown arm")
        return _digest((self.adapter_revision, self.configuration_json, arm, self.resource_pricing))


class WorkloadAdapter(Protocol):
    """All adapters must create isolated state and preserve the supplied inputs.

    inspect() inspects absent phases, failures and finite outstanding work; it
    must not silently synthesize zero bills. execute() actually performs a phase.
    finish() returns immutable runtime model rows, per-call request bindings and
    pending responsibilities after the declared drain. close() cleans up, inside
    the measured drain. Neither may discard unresolved billed calls.
    """

    adapter_revision: str
    configuration_json: str
    initial_snapshot_json: str

    async def execute(self, phase, item): ...
    async def inspect(self, phase): ...
    async def finish(self): ...
    async def close(self): ...


@dataclass(frozen=True, slots=True)
class ExecutionInputs:
    """Gold-free execution surface. Neither factory nor runtime receives labels."""

    adapter_revision: str
    configuration_json: str
    initial_snapshot_json: str
    items: tuple[WorkItem, ...]
    dataset_kind: str
    semantic_model: ModelEvidence
    generation_model: ModelEvidence
    workload: WorkloadManifest


async def _call_observer(operation, *arguments):
    value = operation(*arguments)
    return await value if isawaitable(value) else value


class _ResourceCapture:
    """Measurement hooks carry no gold or result-selection authority."""

    def __init__(self, protocol, observer):
        self.protocol, self.observer = protocol, observer
        self.records, self.reasons = [], []
        self.finalized, self.finalization_error = False, None
        if observer is not None and (
            protocol is None
            or observer.configuration_sha256 != protocol.observer_configuration_sha256
        ):
            raise ValueError("observer differs from frozen resource protocol")

    @asynccontextmanager
    async def measure(self, accounting, phase, *, request_ids=(), evidence):
        token, failure = None, None
        if self.observer is not None:
            try:
                token = await _call_observer(self.observer.begin, phase, tuple(request_ids))
            except Exception as error:
                failure = "observer_error:" + type(error).__name__
        try:
            with accounting.measure(phase, request_ids=request_ids, evidence=evidence):
                yield
        finally:
            if self.protocol is not None:
                request = resource_observation_request(
                    accounting.observations[-1],
                    self.protocol,
                    expected_protocol_sha256=self.protocol.fingerprint,
                )
                if self.observer is not None and failure is None:
                    try:
                        record = await _call_observer(self.observer.end, token, request)
                    except Exception as error:
                        failure = "observer_error:" + type(error).__name__
                else:
                    failure = failure or "observer_missing"
                if failure:
                    record = PhaseResourceObservation(
                        request, finalized=False, failure_reason=failure
                    )
                    self.reasons.append("resource_observation_failed")
                self.records.append(record)

    async def finish(self):
        if self.observer is None:
            return
        try:
            if await _call_observer(self.observer.finalize) is not None:
                raise ValueError("observer finalization must explicitly complete without a value")
            self.finalized = True
        except Exception as error:
            self.finalization_error = "observer_error:" + type(error).__name__
            self.reasons.append("resource_observer_finalization_failed")

    def reconcile(self, accounting, snapshot):
        if self.protocol is None:
            return snapshot, None
        try:
            report = reconcile_resources(
                accounting.observations,
                snapshot,
                self.protocol,
                expected_protocol_sha256=self.protocol.fingerprint,
                records=tuple(self.records),
                observer_finalized=self.finalized,
                observer_finalization_error=self.finalization_error,
            )
            return report.snapshot, report.phases
        except (ValueError, TypeError):
            self.reasons.append("invalid_resource_evidence")
            return snapshot, None


async def run_experiment(plan, factory, judge, *, expected_plan_sha256, observer_factory=None):
    """Execute all four arms in isolated adapters, then paired nested ablations.

    factory(execution_inputs, arm) must only construct; initialization belongs to
    cold_start. judge(item, output, latency_ms) returns a RequestResult against
    the frozen gold. Missing gold never permits self-scoring adapter output.
    Real model repetitions must be separate requests in their original group.
    """
    if plan.fingerprint != expected_plan_sha256:
        raise ValueError("experiment plan changed after freeze")
    if judge is not None and getattr(judge, "configuration_sha256", None) != plan.judge_sha256:
        raise ValueError("judge configuration differs from frozen plan")
    runs, observations, traces, blockers, resource_reports = {}, {}, {}, {}, {}
    execution = ExecutionInputs(
        plan.adapter_revision,
        plan.configuration_json,
        plan.initial_snapshot_json,
        tuple(replace(item, gold_json=None) for item in plan.items),
        plan.dataset_kind,
        plan.semantic_model,
        plan.generation_model,
        plan.workload,
    )
    adapters, observers = [], []
    for arm in plan.arms:
        adapter = factory(execution, arm)
        if any(adapter is old for old in adapters):
            raise ValueError("experiment arms must not share an adapter instance")
        adapters.append(adapter)
        if (
            adapter.adapter_revision,
            adapter.configuration_json,
            adapter.initial_snapshot_json,
        ) != (plan.adapter_revision, plan.configuration_json, plan.initial_snapshot_json):
            raise ValueError("adapter changed frozen shared inputs")
        accounting = ModelExperimentAccounting(plan.currency)
        observer = observer_factory(execution, arm) if observer_factory is not None else None
        if observer is not None and any(observer is previous for previous in observers):
            raise ValueError("experiment arms must not share a resource observer")
        observers.append(observer)
        resources = _ResourceCapture(plan.resource_pricing, observer)
        results, trace, reasons, visited = [], [], [], set()

        async def execute(
            phase,
            item=None,
            *,
            inspect=False,
            adapter=adapter,
            accounting=accounting,
            visited=visited,
            trace=trace,
            reasons=reasons,
            results=results,
            resources=resources,
        ):
            visited.add(phase)
            ids = (item.identity,) if item and item.is_request else ()
            started = perf_counter_ns()
            output, failed = None, False
            try:
                async with resources.measure(
                    accounting,
                    phase,
                    request_ids=ids,
                    evidence=(
                        f"{plan.adapter_revision}: {'inspect' if inspect else 'execute'} "
                        f"{item.identity if item else phase.value}"
                    ),
                ):
                    clean_item = replace(item, gold_json=None) if item else None
                    output = await (
                        adapter.inspect(phase) if inspect else adapter.execute(phase, clean_item)
                    )
                    output = json.loads(canonical(to_jsonable(output)))
            except Exception as error:
                failed = True
                # Exception bodies may include private source or provider payloads.
                trace.append(
                    dict(
                        phase=phase.value,
                        item=item.identity if item else None,
                        failure_type=type(error).__name__,
                    )
                )
                if not item or not item.is_request:
                    reasons.append(f"{phase.value}_execution_failed")
            elapsed = (perf_counter_ns() - started) / 1_000_000
            if item and item.is_request:
                gold = json.loads(item.gold_json) if item.gold_json else None
                if gold is None:
                    reasons.append("missing_gold")
                if failed or gold is None or judge is None:
                    if judge is None:
                        reasons.append("missing_judge")
                    result = RequestResult(
                        item.identity,
                        item.group_id,
                        gold.get("answerable", True) if gold else True,
                        AnswerOutcome.SYSTEM_ERROR,
                        False,
                        False,
                        False,
                        True,
                        elapsed,
                        gold.get("diagnostic_only", False) if gold else False,
                    )
                else:
                    result = judge(item, json.loads(canonical(output)), elapsed)
                    if (
                        result.request_id,
                        result.group_id,
                        result.answerable,
                        result.diagnostic_only,
                    ) != (
                        item.identity,
                        item.group_id,
                        gold["answerable"],
                        gold.get("diagnostic_only", False),
                    ):
                        raise ValueError("judge changed frozen request manifest")
                results.append(result)
            trace.append(
                dict(
                    phase=phase.value,
                    item=item.identity if item else None,
                    input_sha256=_digest(item) if item else None,
                    output=output,
                    failed=failed,
                )
            )

        calls, bindings, pending = (), {}, ()
        try:
            for phase in (CostPhase.COLD_START, CostPhase.REGISTRATION, CostPhase.PREWARM):
                await execute(phase)
            for item in plan.items:
                await execute(item.phase, item)
                if item.phase == CostPhase.WRITE:
                    await execute(CostPhase.DEPENDENCY)
            for phase in CostPhase:
                if phase not in visited and phase != CostPhase.DRAIN:
                    await execute(phase, inspect=True)
            await execute(CostPhase.DRAIN)
            try:
                async with resources.measure(
                    accounting, CostPhase.DRAIN, evidence="snapshot remaining calls and finite debt"
                ):
                    calls, bindings, pending = await adapter.finish()
            except Exception:
                reasons.append("final_accounting_unavailable")
                pending = (PendingResponsibility("final-accounting-unavailable", (), None),)
        finally:
            try:
                async with resources.measure(
                    accounting, CostPhase.DRAIN, evidence="close isolated experiment resources"
                ):
                    await adapter.close()
            except Exception:
                reasons.append("resource_cleanup_failed")
                pending = (*pending, PendingResponsibility("resource-cleanup-failed", (), None))
            await resources.finish()
        costs = accounting.snapshot(calls, request_bindings=bindings, pending=pending)
        costs, resource_reports[arm.name] = resources.reconcile(accounting, costs)
        reasons.extend(resources.reasons)
        run = QuestionCostRun(
            f"{plan.experiment_id}:{arm.name}",
            arm.strategy,
            plan.arm_configuration(arm),
            plan.dataset_sha256,
            plan.dataset_kind,
            plan.license_reference,
            plan.judge_sha256,
            plan.semantic_model,
            plan.generation_model,
            plan.workload,
            tuple(results),
            costs,
        )
        runs[arm.name], observations[arm.name], traces[arm.name], blockers[arm.name] = (
            run,
            accounting.observations,
            trace,
            sorted(set(reasons)),
        )
    comparisons = []
    # Demand versus maintained-full is a bundled scheduling policy comparison;
    # adjacent maintained arms isolate delta and exact model caching.
    # Also report each optimization against the simple on-demand reference.
    for contrast in plan.contrasts:
        candidate_name, baseline_name = contrast.candidate, contrast.baseline
        candidate, baseline = runs[candidate_name], runs[baseline_name]
        stats, evidence = paired_bootstrap(
            candidate,
            baseline,
            plan.statistics,
            expected_protocol_sha256=plan.statistics.fingerprint,
        )
        # Only result identities are bound post-run. No floors/model/judge/statistical
        # choices are changed; the pre-run template is part of plan.fingerprint.
        if plan.acceptance.baseline_report_sha256 not in (None, baseline.fingerprint):
            raise ValueError("baseline report differs from prebound acceptance profile")
        if plan.acceptance.candidate_configuration_sha256 not in (
            None,
            candidate.configuration_sha256,
        ):
            raise ValueError("candidate differs from prebound acceptance profile")
        profile = replace(
            plan.acceptance,
            baseline_report_sha256=baseline.fingerprint,
            candidate_configuration_sha256=candidate.configuration_sha256,
        )
        accepted = evaluate_question_acceptance(
            profile,
            candidate,
            baseline,
            expected_profile_sha256=profile.fingerprint,
            comparison=evidence,
        )
        reasons = sorted(
            set((*accepted.reasons, *blockers[candidate_name], *blockers[baseline_name]))
        )
        comparisons.append(
            dict(
                candidate=candidate_name,
                baseline=baseline_name,
                statistics=stats,
                acceptance=accepted,
                ready=accepted.ready and not reasons,
                reasons=reasons,
                contrast=contrast.attribution,
                varied_control=contrast.varied_control,
            )
        )
    return dict(
        schema="question-workload-experiment/1",
        plan_sha256=plan.fingerprint,
        plan=plan,
        runs=runs,
        observations=observations,
        resource_reports=resource_reports,
        traces=traces,
        blockers=blockers,
        summaries={key: summarize_costs(run) for key, run in runs.items()},
        allocations={key: request_allocations(run) for key, run in runs.items()},
        comparisons=comparisons,
        # A cost/utility slice is never blanket deployment or production approval.
        cost_utility_ready=all(c["ready"] for c in comparisons),
        production_benefit_claim=False,
    )
