"""Frozen four-arm promotion experiment, separate from contract/efficacy claims.

The host supplies real local arms and an independently pinned answer/refusal
judge. This harness performs no model calls, downloads, or synthetic judging.
Unknown observations and failed calls are preserved and block promotion.
"""

from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from hashlib import sha256
from time import monotonic
from typing import Protocol

from ..retrieval.feature_gate import RetrievalFeatureApproval, checked_digest
from ..serialization import to_jsonable
from .retrieval import RetrievalEvalCase

ARMS = ("baseline-b3", "b3+rank", "b3+cache", "b3+both")
REGIMES = ("cold", "warm")
BASELINE_SEMANTICS = "b3-existing-question-service-read-and-reuse/1"
QUALITY = (
    "candidate_recall",
    "final_context_recall",
    "answer_score",
    "refusal_correct",
    "supporting_span_coverage",
    "qualifier_fidelity",
)
COSTS = (
    "model_calls",
    "cost_microunits",
    "cpu_ms",
    "peak_rss_bytes",
    "retained_bytes",
    "input_tokens",
    "output_tokens",
    "embedding_calls",
    "reranking_calls",
    "generation_calls",
    "source_rows_hydrated",
)
LOWER = ("latency_ms", "whole_latency_ms", *COSTS)
OBSERVED_METRICS = (*QUALITY, *LOWER)
TAIL_METRICS = ("latency_p95_ms", "whole_latency_p95_ms")
METRICS = (*OBSERVED_METRICS, *TAIL_METRICS)


def fingerprint(value):
    return sha256(
        json.dumps(
            to_jsonable(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    ).hexdigest()


def _finite(value, *, nonnegative=True):
    if type(value) not in (float, int) or not math.isfinite(value) or nonnegative and value < 0:
        raise ValueError("expected a finite measurement")
    return value


@dataclass(frozen=True, slots=True)
class ControlledRetrievalCase:
    case: RetrievalEvalCase
    group_id: str
    answerable: bool

    def __post_init__(self):
        if type(self.case) is not RetrievalEvalCase:
            raise TypeError("bounded retrieval case required")
        if type(self.group_id) is not str or not 1 <= len(self.group_id) <= 128:
            raise ValueError("bounded project/session group identity required")
        if type(self.answerable) is not bool:
            raise TypeError("answerability must be independently annotated")
        if self.answerable and not self.case.relevant_memory_ids:
            raise ValueError("answerable cases require annotated relevant evidence")


@dataclass(frozen=True, slots=True)
class PromotionBenefit:
    arm: str
    metric: str
    minimum_improvement: float

    def __post_init__(self):
        if self.arm not in ARMS[1:] or self.metric not in METRICS:
            raise ValueError("invalid promotion target")
        _finite(self.minimum_improvement)


@dataclass(frozen=True, slots=True)
class ControlledRetrievalPlan:
    cases: tuple[ControlledRetrievalCase, ...]
    corpus_sha256: str
    model_sha256: str
    judge_sha256: str
    baseline_configuration_sha256: str
    rank_configuration_sha256: str
    reuse_configuration_sha256: str
    # Positive values are maximum allowed regression; all required metrics must
    # be frozen, including resources. Zero means non-inferiority without slack.
    regression_limits: tuple[tuple[str, float], ...]
    benefits: tuple[PromotionBenefit, ...]
    seed: int = 0
    resamples: int = 2_000
    confidence_level: float = 0.95
    minimum_groups: int = 5
    minimum_cases: int = 20
    baseline_semantics: str = BASELINE_SEMANTICS
    evidence_annotations_sha256: str | None = None
    schema: str = "controlled-retrieval-plan/2"

    def __post_init__(self):
        if type(self.cases) is not tuple or not 1 <= len(self.cases) <= 10_000:
            raise ValueError("expected an immutable bounded case manifest")
        if any(type(value) is not ControlledRetrievalCase for value in self.cases):
            raise TypeError("invalid controlled case")
        if len({value.case.case_id for value in self.cases}) != len(self.cases):
            raise ValueError("controlled case identities must be unique")
        for name in (
            "corpus_sha256",
            "model_sha256",
            "judge_sha256",
            "baseline_configuration_sha256",
            "rank_configuration_sha256",
            "reuse_configuration_sha256",
        ):
            checked_digest(getattr(self, name))
        if (
            self.baseline_semantics != BASELINE_SEMANTICS
            or self.schema != "controlled-retrieval-plan/2"
        ):
            raise ValueError("existing optimized B3 baseline semantics are required")
        if self.evidence_annotations_sha256 is not None:
            checked_digest(self.evidence_annotations_sha256)
        if type(self.regression_limits) is not tuple or len(self.regression_limits) != len(METRICS):
            raise ValueError("freeze a regression limit for every metric")
        if {key for key, _ in self.regression_limits} != set(METRICS):
            raise ValueError("regression limits contain missing or duplicate metrics")
        for _, value in self.regression_limits:
            _finite(value)
        if (
            type(self.benefits) is not tuple
            or len(self.benefits) != 3
            or any(type(value) is not PromotionBenefit for value in self.benefits)
            or {value.arm for value in self.benefits} != set(ARMS[1:])
        ):
            raise ValueError("each experimental arm requires one precommitted benefit")
        for name in ("seed", "resamples", "minimum_groups", "minimum_cases"):
            value = getattr(self, name)
            if type(value) is not int or value < (0 if name == "seed" else 1):
                raise ValueError("invalid paired uncertainty bounds")
        if not 100 <= self.resamples <= 100_000 or not 0.8 <= _finite(self.confidence_level) < 1:
            raise ValueError("invalid resampling protocol")
        groups = len({value.group_id for value in self.cases})
        if self.resamples * groups * len(METRICS) * 6 > 50_000_000:
            raise ValueError("paired resampling exceeds the bounded evaluation work budget")

    @property
    def fingerprint(self):
        return fingerprint(self)

    @property
    def dataset_sha256(self):
        return fingerprint(self.cases)

    def arm_configuration(self, arm):
        if arm not in ARMS:
            raise ValueError("unknown controlled arm")
        return fingerprint(
            dict(
                baseline=self.baseline_configuration_sha256,
                rank=self.rank_configuration_sha256 if arm in ("b3+rank", "b3+both") else None,
                reuse=self.reuse_configuration_sha256 if arm in ("b3+cache", "b3+both") else None,
                semantics=self.baseline_semantics,
            )
        )


@dataclass(frozen=True, slots=True)
class ControlledWorkCost:
    model_calls: int | None = None
    cost_microunits: int | None = None
    cpu_ms: float | None = None
    peak_rss_bytes: int | None = None
    retained_bytes: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    embedding_calls: int | None = None
    reranking_calls: int | None = None
    generation_calls: int | None = None
    source_rows_hydrated: int | None = None

    def __post_init__(self):
        for name in COSTS:
            value = getattr(self, name)
            if value is None:
                continue
            _finite(value)
            if name != "cpu_ms" and (type(value) is not int or value > 2**63 - 1):
                raise ValueError("cost counts must be bounded integers or unknown")
        calls = (self.embedding_calls, self.reranking_calls, self.generation_calls)
        if self.model_calls is not None and all(value is not None for value in calls):
            if sum(calls) > self.model_calls:
                raise ValueError("call type counts exceed total model calls")


@dataclass(frozen=True, slots=True)
class ControlledPreparation:
    corpus_sha256: str
    state_verified: bool
    baseline_reuse_enabled: bool
    costs: ControlledWorkCost

    def __post_init__(self):
        checked_digest(self.corpus_sha256)
        if type(self.state_verified) is not bool or type(self.baseline_reuse_enabled) is not bool:
            raise TypeError("explicit cold/warm state and existing reuse attestations required")
        if type(self.costs) is not ControlledWorkCost:
            raise TypeError("preparation costs must be recorded")


@dataclass(frozen=True, slots=True)
class ControlledRetrievalObservation:
    candidate_memory_ids: tuple[str, ...]
    final_memory_ids: tuple[str, ...]
    answer_score: float | None
    refused: bool | None
    costs: ControlledWorkCost
    # Independently judge the actual delivered supporting spans and qualifiers;
    # document identity overlap cannot supply either measurement.
    supporting_span_coverage: float | None = None
    qualifier_fidelity: float | None = None
    evidence_judgment_sha256: str | None = None
    unsafe_deliveries: int | None = None
    stale_deliveries: int | None = None

    def __post_init__(self):
        for name in ("candidate_memory_ids", "final_memory_ids"):
            values = getattr(self, name)
            if (
                type(values) is not tuple
                or len(values) > 256
                or any(type(value) is not str or not 1 <= len(value) <= 128 for value in values)
                or len(values) != len(set(values))
            ):
                raise ValueError("observed identities must be unique and bounded")
        if not set(self.final_memory_ids) <= set(self.candidate_memory_ids):
            raise ValueError("final evidence must come from the candidate set")
        if self.answer_score is not None and not 0 <= _finite(self.answer_score) <= 1:
            raise ValueError("answer score must be independently judged in [0, 1]")
        for name in ("supporting_span_coverage", "qualifier_fidelity"):
            value = getattr(self, name)
            if value is not None and not 0 <= _finite(value) <= 1:
                raise ValueError(f"{name} must be independently judged in [0, 1]")
        if self.evidence_judgment_sha256 is not None:
            checked_digest(self.evidence_judgment_sha256)
        for name in ("unsafe_deliveries", "stale_deliveries"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or not 0 <= value <= 256):
                raise ValueError(f"{name} must be a measured bounded count or unknown")
        if self.refused is not None and type(self.refused) is not bool:
            raise TypeError("refusal must be independently observed or unknown")
        if type(self.costs) is not ControlledWorkCost:
            raise TypeError("query and inline maintenance costs must be recorded")


class ControlledRetrievalArm(Protocol):
    name: str
    configuration_sha256: str
    model_sha256: str
    judge_sha256: str
    # Contract fakes MUST declare contract-only; a real-local declaration is
    # authenticated by the host verifier before accepting any approval receipt.
    measurement_kind: str

    async def prepare(self, regime: str) -> ControlledPreparation: ...

    async def run(
        self, case: ControlledRetrievalCase, regime: str
    ) -> ControlledRetrievalObservation: ...

    async def finish(self, regime: str) -> ControlledWorkCost: ...


@dataclass(frozen=True, slots=True)
class ControlledRetrievalRow:
    arm: str
    regime: str
    case_id: str
    group_id: str
    values: tuple[tuple[str, float | None], ...]
    forbidden_hits: int | None
    error_type: str | None = None
    foreground_costs: ControlledWorkCost | None = None
    evidence_judgment_sha256: str | None = None
    unsafe_deliveries: int | None = None
    stale_deliveries: int | None = None


@dataclass(frozen=True, slots=True)
class ControlledLifecycle:
    arm: str
    regime: str
    preparation_ms: float
    finish_ms: float
    preparation: ControlledPreparation | None
    finish_costs: ControlledWorkCost


@dataclass(frozen=True, slots=True)
class ControlledRetrievalReport:
    protocol_sha256: str
    dataset_sha256: str
    rows: tuple[ControlledRetrievalRow, ...]
    measurement_kinds: tuple[tuple[str, str], ...]
    blockers: tuple[str, ...]
    lifecycle: tuple[ControlledLifecycle, ...]
    schema: str = "controlled-retrieval-report/2"

    @property
    def fingerprint(self):
        return fingerprint(self)


def _merged_costs(query, prepare, finish, count):
    values = {}
    for name in COSTS:
        parts = [getattr(value, name) for value in (query, prepare, finish)]
        if any(value is None for value in parts):
            values[name] = None
        elif name in ("peak_rss_bytes", "retained_bytes"):
            values[name] = float(max(parts))
        else:
            # Frozen equal-case allocation includes all preparation, index/model
            # loading, warmup, refresh, drain, and teardown work in the workload.
            values[name] = float(parts[0] + (parts[1] + parts[2]) / count)
    return values


async def run_controlled_retrieval(plan, arms, *, expected_protocol_sha256):
    """Run all four exact arms for both regimes without manufacturing metrics.

    prepare(cold) resets query caches; prepare(warm) establishes the frozen warm
    state. Both report all work. finish drains maintenance and reports its costs.
    The host must preserve the existing QuestionService optimized reuse baseline.
    """
    if type(plan) is not ControlledRetrievalPlan or plan.fingerprint != expected_protocol_sha256:
        raise ValueError("controlled protocol changed after freeze")
    if type(arms) is not tuple or len(arms) != 4 or {arm.name for arm in arms} != set(ARMS):
        raise ValueError("exactly baseline-b3, b3+rank, b3+cache, b3+both are required")
    by_name = {arm.name: arm for arm in arms}
    kinds = []
    for name, arm in by_name.items():
        if (
            arm.configuration_sha256 != plan.arm_configuration(name)
            or arm.model_sha256 != plan.model_sha256
            or arm.judge_sha256 != plan.judge_sha256
        ):
            raise ValueError("arm changed frozen corpus/model/judge/configuration controls")
        if arm.measurement_kind not in ("real-local", "contract-only"):
            raise ValueError("explicit measurement provenance required")
        kinds.append((name, arm.measurement_kind))
    rows, blockers, lifecycle = [], [], []
    cases = sorted(plan.cases, key=lambda value: value.case.case_id)
    for regime in REGIMES:
        # Counterbalance arm order deterministically to avoid always timing the
        # baseline first. Every arm owns isolated, explicitly prepared state.
        order = list(ARMS)
        random.Random(plan.seed + REGIMES.index(regime)).shuffle(order)
        for name in order:
            arm, prepared = by_name[name], None
            started = monotonic()
            try:
                prepared = await arm.prepare(regime)
                if type(prepared) is not ControlledPreparation or not prepared.state_verified:
                    raise ValueError("cold/warm state unverified")
                if (
                    prepared.corpus_sha256 != plan.corpus_sha256
                    or not prepared.baseline_reuse_enabled
                ):
                    raise ValueError("corpus or existing optimized baseline changed")
            except Exception as error:
                blockers.append(f"{name}:{regime}:prepare:{type(error).__name__}")
                prepared = None
            preparation_ms = (monotonic() - started) * 1000
            observations = []
            for case in cases:
                observed, error_type = None, None
                started = monotonic()
                try:
                    if prepared is None:
                        raise RuntimeError("preparation_failed")
                    observed = await arm.run(case, regime)
                    if type(observed) is not ControlledRetrievalObservation:
                        raise TypeError("invalid controlled observation")
                except Exception as error:
                    observed = None
                    error_type = type(error).__name__
                # Failed calls are timed too; their unreported costs stay unknown.
                observations.append((case, observed, error_type, (monotonic() - started) * 1000))
            started = monotonic()
            try:
                finished = await arm.finish(regime)
                if type(finished) is not ControlledWorkCost:
                    raise TypeError("invalid maintenance costs")
            except Exception as error:
                blockers.append(f"{name}:{regime}:finish:{type(error).__name__}")
                finished = ControlledWorkCost()
            finish_ms = (monotonic() - started) * 1000
            lifecycle.append(
                ControlledLifecycle(name, regime, preparation_ms, finish_ms, prepared, finished)
            )
            if (
                arm.configuration_sha256 != plan.arm_configuration(name)
                or arm.model_sha256 != plan.model_sha256
                or arm.judge_sha256 != plan.judge_sha256
                or arm.measurement_kind != dict(kinds)[name]
            ):
                blockers.append(f"{name}:{regime}:frozen_controls_changed")
            for case, observed, error_type, elapsed in observations:
                values = {metric: None for metric in OBSERVED_METRICS}
                values["latency_ms"] = elapsed
                values["whole_latency_ms"] = elapsed + (preparation_ms + finish_ms) / len(cases)
                forbidden = None
                if observed is not None:
                    relevant = set(case.case.relevant_memory_ids)
                    candidates, final = (
                        set(observed.candidate_memory_ids),
                        set(observed.final_memory_ids),
                    )
                    if relevant:
                        values["candidate_recall"] = len(relevant & candidates) / len(relevant)
                        values["final_context_recall"] = len(relevant & final) / len(relevant)
                    values["answer_score"] = observed.answer_score if case.answerable else None
                    for metric in ("supporting_span_coverage", "qualifier_fidelity"):
                        values[metric] = getattr(observed, metric) if case.answerable else None
                    values["refusal_correct"] = (
                        float(observed.refused)
                        if observed.refused is not None and not case.answerable
                        else None
                    )
                    forbidden = len((candidates | final) & set(case.case.forbidden_memory_ids))
                    values.update(
                        _merged_costs(observed.costs, prepared.costs, finished, len(cases))
                    )
                rows.append(
                    ControlledRetrievalRow(
                        name,
                        regime,
                        case.case.case_id,
                        case.group_id,
                        tuple(sorted(values.items())),
                        forbidden,
                        error_type,
                        observed.costs if observed is not None else None,
                        observed.evidence_judgment_sha256 if observed is not None else None,
                        observed.unsafe_deliveries if observed is not None else None,
                        observed.stale_deliveries if observed is not None else None,
                    )
                )
    return ControlledRetrievalReport(
        plan.fingerprint,
        plan.dataset_sha256,
        tuple(sorted(rows, key=lambda row: (row.arm, row.regime, row.case_id))),
        tuple(sorted(kinds)),
        tuple(sorted(blockers)),
        tuple(sorted(lifecycle, key=lambda value: (value.arm, value.regime))),
    )


def _interval(values, confidence):
    if not values or any(value is None for value in values):
        return None
    ordered = sorted(values)

    def quantile(fraction):
        position = (len(ordered) - 1) * fraction
        left, right = math.floor(position), math.ceil(position)
        return ordered[left] + (ordered[right] - ordered[left]) * (position - left)

    alpha = (1 - confidence) / 2
    return quantile(alpha), quantile(1 - alpha)


@dataclass(frozen=True, slots=True)
class ControlledPromotionDecision:
    arm: str
    eligible: bool
    blockers: tuple[str, ...]
    # regime, metric, paired delta, lower CI, upper CI; None is not zero.
    intervals: tuple[tuple[str, str, float | None, float | None, float | None], ...]
    approval: RetrievalFeatureApproval | None = None


def assess_controlled_retrieval(plan, report, *, expected_protocol_sha256, rollback_id):
    """Precommitted paired group bootstrap; no mocks can issue approval receipts.

    This verifies structural evidence, not host identity. A production feature
    additionally requires a trusted host verifier for the exact frozen artifact.
    """
    if (
        type(plan) is not ControlledRetrievalPlan
        or type(report) is not ControlledRetrievalReport
        or plan.fingerprint != expected_protocol_sha256
        or report.schema != "controlled-retrieval-report/2"
        or report.protocol_sha256 != plan.fingerprint
        or report.dataset_sha256 != plan.dataset_sha256
    ):
        raise ValueError("controlled report does not bind the frozen protocol")
    expected = {
        (arm, regime, case.case.case_id)
        for arm in ARMS
        for regime in REGIMES
        for case in plan.cases
    }
    actual = {(row.arm, row.regime, row.case_id) for row in report.rows}
    if actual != expected or len(actual) != len(report.rows):
        raise ValueError("controlled report has duplicate, missing, or extra paired rows")
    lifecycle = {(value.arm, value.regime): value for value in report.lifecycle}
    if len(report.lifecycle) != 8 or set(lifecycle) != {
        (arm, regime) for arm in ARMS for regime in REGIMES
    }:
        raise ValueError("missing or duplicate whole-lifecycle cost records")
    for value in report.lifecycle:
        _finite(value.preparation_ms)
        _finite(value.finish_ms)
        if type(value.finish_costs) is not ControlledWorkCost:
            raise TypeError("invalid whole-lifecycle maintenance costs")
        if value.preparation is not None and (
            type(value.preparation) is not ControlledPreparation
            or value.preparation.corpus_sha256 != plan.corpus_sha256
            or not value.preparation.state_verified
            or not value.preparation.baseline_reuse_enabled
        ):
            raise ValueError("unverified lifecycle corpus or baseline state")
    cases = {value.case.case_id: value for value in plan.cases}
    common = list(report.blockers)
    if plan.evidence_annotations_sha256 is None:
        common.append("missing_frozen_evidence_annotations")
    if len(report.measurement_kinds) != 4 or set(report.measurement_kinds) != {
        (arm, "real-local") for arm in ARMS
    }:
        common.append("contract_only_or_unverified_measurements")
    groups = sorted({value.group_id for value in plan.cases})
    if len(groups) < plan.minimum_groups or len(cases) < plan.minimum_cases:
        common.append("insufficient_independent_groups_or_cases")
    rows = {}
    for row in report.rows:
        case, values = cases[row.case_id], dict(row.values)
        if (
            row.group_id != case.group_id
            or len(row.values) != len(OBSERVED_METRICS)
            or set(values) != set(OBSERVED_METRICS)
        ):
            raise ValueError("controlled measurement schema or group binding changed")
        lifecycle_row = lifecycle[row.arm, row.regime]
        if row.foreground_costs is not None:
            if (
                type(row.foreground_costs) is not ControlledWorkCost
                or lifecycle_row.preparation is None
            ):
                raise ValueError("unbound foreground or preparation costs")
            allocated = _merged_costs(
                row.foreground_costs,
                lifecycle_row.preparation.costs,
                lifecycle_row.finish_costs,
                len(cases),
            )
            if any(values[metric] != allocated[metric] for metric in COSTS):
                raise ValueError("whole-cost allocation changed")
        elif any(values[metric] is not None for metric in COSTS):
            raise ValueError("unobserved foreground costs must remain unknown")
        elapsed = values["latency_ms"]
        whole = values["whole_latency_ms"]
        if elapsed is not None and whole != elapsed + (
            lifecycle_row.preparation_ms + lifecycle_row.finish_ms
        ) / len(cases):
            raise ValueError("whole-lifecycle latency allocation changed")
        if row.error_type is not None:
            common.append(f"{row.arm}:{row.regime}:failed_observation")
        if type(row.forbidden_hits) is not int or row.forbidden_hits != 0:
            common.append(f"{row.arm}:{row.regime}:forbidden_or_unknown")
        for metric in ("unsafe_deliveries", "stale_deliveries"):
            value = getattr(row, metric)
            if type(value) is not int or value != 0:
                common.append(f"{row.arm}:{row.regime}:{metric}_or_unknown")
        if case.answerable:
            if row.evidence_judgment_sha256 is None:
                common.append(f"{row.arm}:{row.regime}:unknown:evidence_judgment")
            else:
                checked_digest(row.evidence_judgment_sha256)
        for metric, value in values.items():
            applicable = (
                bool(case.case.relevant_memory_ids)
                if metric in QUALITY[:2]
                else case.answerable
                if metric in ("answer_score", "supporting_span_coverage", "qualifier_fidelity")
                else not case.answerable
                if metric == "refusal_correct"
                else True
            )
            if not applicable:
                if value is not None:
                    raise ValueError("inapplicable metric must remain unmeasured")
            elif value is None:
                common.append(f"{row.arm}:{row.regime}:unknown:{metric}")
            else:
                _finite(value)
                if metric in QUALITY and value > 1:
                    raise ValueError("invalid normalized quality measurement")
        rows[row.arm, row.regime, row.case_id] = values
    limits, benefits = dict(plan.regression_limits), {value.arm: value for value in plan.benefits}
    decisions = []
    for arm in ARMS[1:]:
        reasons, intervals = list(common), []
        for regime in REGIMES:
            metric_intervals = {}
            for metric in METRICS:
                grouped = {group: [] for group in groups}
                source_metric = {
                    "latency_p95_ms": "latency_ms",
                    "whole_latency_p95_ms": "whole_latency_ms",
                }.get(metric, metric)
                for case in plan.cases:
                    candidate = rows[arm, regime, case.case.case_id][source_metric]
                    baseline = rows[ARMS[0], regime, case.case.case_id][source_metric]
                    if candidate is not None and baseline is not None:
                        grouped[case.group_id].append((candidate, baseline))
                summaries = {
                    group: (sum(left - right for left, right in values), len(values))
                    for group, values in grouped.items()
                }

                def difference(sample, metric=metric, grouped=grouped, summaries=summaries):
                    if metric in ("peak_rss_bytes", "retained_bytes"):
                        values = [value for group in sample for value in grouped[group]]
                        return (
                            max(value[0] for value in values) - max(value[1] for value in values)
                            if values
                            else None
                        )
                    if metric in TAIL_METRICS:
                        values = [value for group in sample for value in grouped[group]]
                        if not values:
                            return None
                        index = math.ceil(len(values) * 0.95) - 1
                        return (
                            sorted(value[0] for value in values)[index]
                            - sorted(value[1] for value in values)[index]
                        )
                    count = sum(summaries[group][1] for group in sample)
                    return sum(summaries[group][0] for group in sample) / count if count else None

                rng = random.Random(plan.seed)
                draws = [
                    difference([groups[rng.randrange(len(groups))] for _ in groups])
                    for _ in range(plan.resamples)
                ]
                interval = _interval(draws, plan.confidence_level)
                point = difference(groups)
                intervals.append((regime, metric, point, *(interval or (None, None))))
                metric_intervals[metric] = interval
                if interval is None:
                    reasons.append(f"{regime}:undefined_paired_interval:{metric}")
                elif (
                    metric in QUALITY
                    and interval[0] < -limits[metric]
                    or metric in (*LOWER, *TAIL_METRICS)
                    and interval[1] > limits[metric]
                ):
                    reasons.append(f"{regime}:regression:{metric}")
            benefit = benefits[arm]
            interval = metric_intervals[benefit.metric]
            if interval is None or (
                interval[0] <= benefit.minimum_improvement
                if benefit.metric in QUALITY
                else interval[1] >= -benefit.minimum_improvement
            ):
                reasons.append(f"{regime}:benefit_not_established:{benefit.metric}")
        reasons = tuple(sorted(set(reasons)))
        feature, config = {
            "b3+rank": ("pair-reranker", plan.rank_configuration_sha256),
            "b3+cache": ("evidence-pack-reuse", plan.reuse_configuration_sha256),
            "b3+both": ("rank-and-reuse", plan.arm_configuration("b3+both")),
        }[arm]
        approval = (
            None
            if reasons
            else RetrievalFeatureApproval(
                feature,
                config,
                report.fingerprint,
                plan.fingerprint,
                rollback_id,
            )
        )
        decisions.append(
            ControlledPromotionDecision(arm, not reasons, reasons, tuple(intervals), approval)
        )
    return tuple(decisions)
