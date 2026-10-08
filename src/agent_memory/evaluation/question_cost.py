"""V7-B0 full-cost accounting and frozen acceptance contracts (AM70-T13).

This module scores trusted experiment records; it neither instruments production
nor proves that a provider, judge, or model actually ran. Runtime code must not
import it. One operation ID denotes one possibly billable attempt, not each
budget account constraining that attempt. Retries and losing calls get new IDs.
Money uses integer millionths of the declared currency; CPU/I/O/storage remain
separate unless an explicit tariff supplies a priced-resource entry.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from enum import StrEnum

from .acceptance import _hash
from .evidence import _digest, _finite, _strings, _text

COST_CONTRACT_VERSION = "question-cost/1"


class CostPhase(StrEnum):
    COLD_START = "cold_start"
    REGISTRATION = "registration"
    PREWARM = "prewarm"
    WRITE = "write"
    DEPENDENCY = "dependency"
    BACKGROUND = "background"
    FOREGROUND = "foreground"
    FAILURE = "failure"
    RETRY = "retry"
    DRAIN = "drain"


class TokenBasis(StrEnum):
    MEASURED = "measured"
    ESTIMATED = "estimated"
    UNKNOWN = "unknown"


def _count(value: int, name: str) -> int:
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ValueError(f"{name} must be a nonnegative signed-64-bit integer")
    return value


def _optional_hash(value: str | None, name: str) -> None:
    if value is not None:
        _hash(value, name)


def _unique(items: tuple, attribute: str, name: str) -> None:
    keys = [getattr(item, attribute) for item in items]
    if len(keys) != len(set(keys)):
        raise ValueError(f"duplicate {name}")


@dataclass(frozen=True, slots=True)
class CostEntry:
    operation_id: str
    phase: CostPhase
    request_ids: tuple[str, ...] = ()
    model_call: bool = False
    provider: str | None = None
    provider_request_id: str | None = None
    provider_billed_microunits: int | None = None
    priced_resource_microunits: int | None = None
    billing_reference: str | None = None
    tariff_reference: str | None = None
    estimated_microunits: int | None = None
    reservation_microunits: int | None = None
    tokens: int | None = None
    token_basis: TokenBasis = TokenBasis.UNKNOWN
    cpu_ms: float = 0.0
    io_bytes: int = 0
    storage_byte_seconds: int = 0
    network_bytes: int = 0
    model_role: str | None = None
    model_configuration_sha256: str | None = None

    def __post_init__(self) -> None:
        _text(self.operation_id, "operation_id")
        object.__setattr__(self, "phase", CostPhase(self.phase))
        object.__setattr__(self, "request_ids", _strings(self.request_ids, "request_ids", 10_000))
        object.__setattr__(self, "token_basis", TokenBasis(self.token_basis))
        if type(self.model_call) is not bool:
            raise ValueError("model_call must be boolean")
        if (self.model_role is None) != (self.model_configuration_sha256 is None):
            raise ValueError("model role and configuration must be supplied together")
        if self.model_role is not None:
            if not self.model_call or self.model_role not in {
                "semantic_model",
                "generation_model",
            }:
                raise ValueError("model role must bind an actual semantic or generation call")
            _hash(self.model_configuration_sha256, "model_configuration_sha256")
        for name in ("provider", "provider_request_id", "billing_reference", "tariff_reference"):
            if getattr(self, name) is not None:
                _text(getattr(self, name), name, 2048)
        if (self.model_call or self.provider_request_id is not None) and self.provider is None:
            raise ValueError("model calls and provider request IDs require a provider")
        for name in (
            "provider_billed_microunits",
            "priced_resource_microunits",
            "estimated_microunits",
            "reservation_microunits",
            "tokens",
        ):
            if getattr(self, name) is not None:
                _count(getattr(self, name), name)
        if self.provider_billed_microunits is not None:
            if self.provider is None or self.billing_reference is None:
                raise ValueError("provider billing requires a provider and billing reference")
            if self.priced_resource_microunits is not None:
                raise ValueError("one cost entry cannot count both a bill and a resource tariff")
        if self.priced_resource_microunits is not None:
            if self.tariff_reference is None or self.model_call:
                raise ValueError(
                    "resource pricing requires a tariff and cannot replace a model bill"
                )
        if self.actual_microunits is None and self.reservation_microunits == 0:
            raise ValueError("unresolved cost cannot have a zero reservation")
        if (self.tokens is None) != (self.token_basis == TokenBasis.UNKNOWN):
            raise ValueError(
                "missing tokens must be unknown; known tokens need a measurement basis"
            )
        object.__setattr__(self, "cpu_ms", _finite(self.cpu_ms, "cpu_ms"))
        for name in ("io_bytes", "storage_byte_seconds", "network_bytes"):
            _count(getattr(self, name), name)

    @property
    def actual_microunits(self) -> int | None:
        if self.provider_billed_microunits is not None:
            return self.provider_billed_microunits
        return self.priced_resource_microunits


@dataclass(frozen=True, slots=True)
class PendingResponsibility:
    """Finite unexecuted work, separate from already dispatched call debt."""

    responsibility_id: str
    request_ids: tuple[str, ...]
    maximum_microunits: int | None
    phase: CostPhase = CostPhase.DRAIN

    def __post_init__(self) -> None:
        _text(self.responsibility_id, "responsibility_id")
        object.__setattr__(self, "phase", CostPhase(self.phase))
        object.__setattr__(self, "request_ids", _strings(self.request_ids, "request_ids", 10_000))
        if self.maximum_microunits is not None:
            _count(self.maximum_microunits, "maximum_microunits")
            if self.maximum_microunits == 0:
                raise ValueError("unfinished responsibility cannot silently have zero debt")


@dataclass(frozen=True, slots=True)
class CostSnapshot:
    currency: str
    entries: tuple[CostEntry, ...]
    pending: tuple[PendingResponsibility, ...]
    accounted_phases: tuple[CostPhase, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.currency, str)
            or len(self.currency) != 3
            or not all("A" <= char <= "Z" for char in self.currency)
        ):
            raise ValueError("currency must be a three-letter uppercase code")
        for name, expected in (("entries", CostEntry), ("pending", PendingResponsibility)):
            values = tuple(getattr(self, name))
            if any(not isinstance(value, expected) for value in values):
                raise TypeError(f"{name} contains an invalid record")
            object.__setattr__(self, name, values)
        _unique(self.entries, "operation_id", "operation_id")
        _unique(self.pending, "responsibility_id", "responsibility_id")
        billed_ids = [
            (entry.provider, entry.provider_request_id)
            for entry in self.entries
            if entry.provider_request_id is not None
        ]
        if len(set(billed_ids)) != len(billed_ids):
            raise ValueError("a provider request cannot be charged as multiple attempts")
        phases = tuple(CostPhase(phase) for phase in self.accounted_phases)
        if len(phases) != len(set(phases)):
            raise ValueError("accounted phases must be unique")
        object.__setattr__(self, "accounted_phases", tuple(sorted(phases)))
        if any(entry.phase not in phases for entry in self.entries):
            raise ValueError("entries must belong to an accounted phase")

    @property
    def fingerprint(self) -> str:
        return _digest(self)


class QuestionCostLedger:
    """In-memory evaluation ledger; not a durable reservation/dispatch authority.

    Shared operation costs are counted once globally. request_allocations uses
    equal shares over recorded triggering requests; request-less setup work is
    shared equally across all requests. The frozen rule cannot be selected after
    seeing which requests happened to produce correct answers.
    """

    def __init__(self, currency: str) -> None:
        CostSnapshot(currency, (), (), ())
        self.currency = currency
        self._entries: dict[str, CostEntry] = {}

    def record(self, entry: CostEntry) -> bool:
        if not isinstance(entry, CostEntry):
            raise TypeError("entry must be CostEntry")
        old = self._entries.get(entry.operation_id)
        if old is not None:
            if old != entry:
                raise ValueError("conflicting operation replay")
            return False
        CostSnapshot(self.currency, (*self._entries.values(), entry), (), tuple(CostPhase))
        self._entries[entry.operation_id] = entry
        return True

    def settle(
        self,
        operation_id: str,
        *,
        provider_request_id: str,
        billed_microunits: int,
        billing_reference: str,
        measured_tokens: int | None = None,
    ) -> bool:
        """Reconcile one attempt, including a later authoritative usage receipt.

        Replaying an unchanged bill may enrich unknown/estimated tokens with
        measured usage. Conflicting bills or conflicting measured usage fail;
        an omitted token receipt never invents zero usage.
        """
        old = self._entries[operation_id]
        if (
            measured_tokens is not None
            and old.token_basis == TokenBasis.MEASURED
            and old.tokens != measured_tokens
        ):
            raise ValueError("conflicting measured usage replay")
        new = replace(
            old,
            provider_request_id=provider_request_id,
            provider_billed_microunits=billed_microunits,
            billing_reference=billing_reference,
            tokens=old.tokens if measured_tokens is None else measured_tokens,
            token_basis=old.token_basis if measured_tokens is None else TokenBasis.MEASURED,
        )
        if old.provider_request_id not in (None, provider_request_id):
            raise ValueError("settlement changed provider request identity")
        if old.provider_billed_microunits is not None and (
            old.provider_billed_microunits != billed_microunits
            or old.billing_reference != billing_reference
        ):
            raise ValueError("conflicting settlement replay")
        if old == new:
            return False
        entries = tuple(
            new if e.operation_id == operation_id else e for e in self._entries.values()
        )
        CostSnapshot(self.currency, entries, (), tuple(CostPhase))
        self._entries[operation_id] = new
        return True

    def snapshot(
        self,
        *,
        accounted_phases: tuple[CostPhase, ...],
        pending: tuple[PendingResponsibility, ...] = (),
    ) -> CostSnapshot:
        return CostSnapshot(self.currency, tuple(self._entries.values()), pending, accounted_phases)


class AnswerOutcome(StrEnum):
    CORRECT_ANSWER = "correct_answer"
    REASONABLE_UNKNOWN = "reasonable_unknown"
    INCORRECT_ANSWER = "incorrect_answer"
    INCORRECT_REFUSAL = "incorrect_refusal"
    SYSTEM_ERROR = "system_error"


@dataclass(frozen=True, slots=True)
class RequestResult:
    request_id: str
    group_id: str
    answerable: bool
    outcome: AnswerOutcome
    evidence_complete: bool
    qualifiers_preserved: bool
    fresh: bool
    safe: bool
    latency_ms: float
    diagnostic_only: bool = False

    def __post_init__(self) -> None:
        for name in ("request_id", "group_id"):
            _text(getattr(self, name), name)
        object.__setattr__(self, "outcome", AnswerOutcome(self.outcome))
        for name in (
            "answerable",
            "evidence_complete",
            "qualifiers_preserved",
            "fresh",
            "safe",
            "diagnostic_only",
        ):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a trusted boolean annotation")
        object.__setattr__(self, "latency_ms", _finite(self.latency_ms, "latency_ms"))
        if self.outcome == AnswerOutcome.CORRECT_ANSWER and not self.answerable:
            raise ValueError("an unanswerable request cannot have a correct factual answer")
        if self.outcome == AnswerOutcome.REASONABLE_UNKNOWN and self.answerable:
            raise ValueError("unknown on an answerable request is an incorrect refusal")

    @property
    def effective_answer(self) -> bool:
        return (
            not self.diagnostic_only
            and self.outcome == AnswerOutcome.CORRECT_ANSWER
            and self.evidence_complete
            and self.qualifiers_preserved
            and self.fresh
            and self.safe
        )


@dataclass(frozen=True, slots=True)
class WorkloadManifest:
    """Comparable work excludes strategy/configuration, which each run records.

    Hash inputs bind event order, initial snapshot, request/change distribution,
    semantic/security rules, shared budgets and hardware. The drain policy fixes
    injection cutoff, measurement window and how remaining finite work is closed.
    """

    request_ids: tuple[str, ...]
    event_stream_sha256: str
    initial_snapshot_sha256: str
    distribution_sha256: str
    semantics_sha256: str
    security_sha256: str
    budget_sha256: str
    hardware_sha256: str
    drain_policy_sha256: str

    def __post_init__(self) -> None:
        ids = _strings(self.request_ids, "request_ids", 100_000)
        object.__setattr__(self, "request_ids", ids)
        for name in self.__dataclass_fields__:
            if name != "request_ids":
                _hash(getattr(self, name), name)

    @property
    def fingerprint(self) -> str:
        return _digest(self)


@dataclass(frozen=True, slots=True)
class ModelEvidence:
    configuration_sha256: str
    execution: str

    def __post_init__(self) -> None:
        _hash(self.configuration_sha256, "configuration_sha256")
        if self.execution not in {"real", "stub", "not_used"}:
            raise ValueError("model execution must be real, stub or not_used")


@dataclass(frozen=True, slots=True)
class QuestionCostRun:
    run_id: str
    strategy: str
    configuration_sha256: str
    dataset_sha256: str
    dataset_kind: str
    dataset_license_reference: str | None
    judge_sha256: str
    semantic_model: ModelEvidence
    generation_model: ModelEvidence
    workload: WorkloadManifest
    requests: tuple[RequestResult, ...]
    costs: CostSnapshot

    def __post_init__(self) -> None:
        _text(self.run_id, "run_id")
        if self.strategy not in {"on_demand_full", "coalesced_full", "delta_proof", "exact_cache"}:
            raise ValueError("unsupported experiment strategy")
        for name in ("configuration_sha256", "dataset_sha256", "judge_sha256"):
            _hash(getattr(self, name), name)
        if self.dataset_kind not in {"synthetic", "licensed_real"}:
            raise ValueError("dataset_kind must be synthetic or licensed_real")
        if self.dataset_license_reference is not None:
            _text(self.dataset_license_reference, "dataset_license_reference", 2048)
        for name, expected in (
            ("semantic_model", ModelEvidence),
            ("generation_model", ModelEvidence),
            ("workload", WorkloadManifest),
            ("costs", CostSnapshot),
        ):
            if not isinstance(getattr(self, name), expected):
                raise TypeError(f"{name} must be {expected.__name__}")
        requests = tuple(self.requests)
        if any(not isinstance(request, RequestResult) for request in requests):
            raise TypeError("requests must contain RequestResult records")
        if tuple(request.request_id for request in requests) != self.workload.request_ids:
            raise ValueError("every workload request must be reported, in workload order")
        object.__setattr__(self, "requests", requests)
        ids = set(self.workload.request_ids)
        records = (*self.costs.entries, *self.costs.pending)
        if any(not set(entry.request_ids) <= ids for entry in records):
            raise ValueError("cost attribution refers to a request outside the workload")
        model_calls = tuple(entry for entry in self.costs.entries if entry.model_call)
        if model_calls and all(
            getattr(self, role).execution == "not_used"
            for role in ("semantic_model", "generation_model")
        ):
            raise ValueError("not_used model declarations contradict recorded model calls")
        for entry in model_calls:
            # Older/unbound records remain inspectable, but cannot pass the gate.
            if entry.model_role is None:
                continue
            model = getattr(self, entry.model_role)
            if model.execution == "not_used":
                raise ValueError("not_used model declaration contradicts a role-bound call")
            if entry.model_configuration_sha256 != model.configuration_sha256:
                raise ValueError("model call configuration differs from its declared role")

    @property
    def fingerprint(self) -> str:
        return _digest(self)


@dataclass(frozen=True, slots=True)
class QuestionCostSummary:
    all_requests: int
    effective_answers: int
    model_calls: int
    measured_tokens: int
    estimated_tokens: int
    unknown_token_calls: int
    estimated_token_calls: int
    unaccounted_phases: tuple[CostPhase, ...]
    provider_billed_microunits: int
    priced_resource_microunits: int
    estimated_microunits: int
    unresolved_cost_entries: int
    pending_responsibilities: int
    outstanding_upper_bound_microunits: int | None
    total_actual_microunits: int | None
    cost_per_request_microunits: float | None
    cost_per_effective_answer_microunits: float | None


def summarize_costs(run: QuestionCostRun) -> QuestionCostSummary:
    entries = run.costs.entries
    unresolved = [entry for entry in entries if entry.actual_microunits is None]
    debt = [entry.reservation_microunits for entry in unresolved]
    debt.extend(item.maximum_microunits for item in run.costs.pending)
    missing = tuple(phase for phase in CostPhase if phase not in run.costs.accounted_phases)
    bounded_debt = sum(debt) if not missing and all(value is not None for value in debt) else None
    bills = sum(entry.provider_billed_microunits or 0 for entry in entries)
    priced = sum(entry.priced_resource_microunits or 0 for entry in entries)
    actual = None if unresolved or run.costs.pending or missing else bills + priced
    count = len(run.requests)
    effective = sum(request.effective_answer for request in run.requests)
    return QuestionCostSummary(
        all_requests=count,
        effective_answers=effective,
        model_calls=sum(entry.model_call for entry in entries),
        measured_tokens=sum(
            entry.tokens or 0 for entry in entries if entry.token_basis == TokenBasis.MEASURED
        ),
        estimated_tokens=sum(
            entry.tokens or 0 for entry in entries if entry.token_basis == TokenBasis.ESTIMATED
        ),
        unknown_token_calls=sum(entry.model_call and entry.tokens is None for entry in entries),
        estimated_token_calls=sum(
            entry.model_call and entry.token_basis == TokenBasis.ESTIMATED for entry in entries
        ),
        unaccounted_phases=missing,
        provider_billed_microunits=bills,
        priced_resource_microunits=priced,
        estimated_microunits=sum(entry.estimated_microunits or 0 for entry in entries),
        unresolved_cost_entries=len(unresolved),
        pending_responsibilities=len(run.costs.pending),
        outstanding_upper_bound_microunits=bounded_debt,
        total_actual_microunits=actual,
        cost_per_request_microunits=actual / count if actual is not None and count else None,
        cost_per_effective_answer_microunits=(
            actual / effective if actual is not None and effective else None
        ),
    )


def request_allocations(run: QuestionCostRun) -> dict[str, dict[str, float | None]]:
    """Fixed equal-share attribution; unknown allocation remains unknown.

    No requests means no allocation, never discarded global setup cost. Pending
    work makes each affected request's final cost unknown as well.
    """
    complete = set(run.costs.accounted_phases) == set(CostPhase)
    result = {
        key: {"known_microunits": 0.0, "total_microunits": 0.0 if complete else None}
        for key in run.workload.request_ids
    }
    for item in (*run.costs.entries, *run.costs.pending):
        targets = item.request_ids or run.workload.request_ids
        value = item.actual_microunits if isinstance(item, CostEntry) else None
        for key in targets:
            row = result[key]
            if value is None:
                row["total_microunits"] = None
            else:
                share = value / len(targets)
                row["known_microunits"] += share
                if row["total_microunits"] is not None:
                    row["total_microunits"] += share
    return result


@dataclass(frozen=True, slots=True)
class QuestionQualityThresholds:
    minimum_answerable_coverage: float
    minimum_evidence_coverage: float
    minimum_qualifier_fidelity: float
    maximum_error_rate: float
    maximum_effective_rate_drop: float
    maximum_latency_p95_ms: float
    maximum_cost_per_request_microunits: float
    maximum_cost_per_effective_answer_microunits: float

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = _finite(getattr(self, name), name)
            if not name.endswith(("_ms", "_microunits")) and value > 1:
                raise ValueError(f"{name} must be a rate between zero and one")
            object.__setattr__(self, name, value)


@dataclass(frozen=True, slots=True)
class QuestionAcceptanceProfile:
    """None is an explicit calibration gap, never an implicit permissive default."""

    profile_id: str
    version: str
    minimum_requests: int | None
    minimum_groups: int | None
    confidence_level: float | None
    dataset_sha256: str | None = None
    dataset_license_reference: str | None = None
    workload_sha256: str | None = None
    candidate_configuration_sha256: str | None = None
    baseline_report_sha256: str | None = None
    judge_sha256: str | None = None
    semantic_model: ModelEvidence | None = None
    generation_model: ModelEvidence | None = None
    thresholds: QuestionQualityThresholds | None = None
    calibration_reference: str | None = None
    statistics_protocol_sha256: str | None = None
    contract_version: str = COST_CONTRACT_VERSION
    allocation_rule: str = "equal_triggering_requests_else_all_requests/1"

    def __post_init__(self) -> None:
        for name in ("profile_id", "version", "contract_version", "allocation_rule"):
            _text(getattr(self, name), name)
        for name in self.__dataclass_fields__:
            if name.endswith("sha256"):
                _optional_hash(getattr(self, name), name)
        for name in ("dataset_license_reference", "calibration_reference"):
            if getattr(self, name) is not None:
                _text(getattr(self, name), name, 2048)
        for name in ("minimum_requests", "minimum_groups"):
            value = getattr(self, name)
            if value is not None and _count(value, name) == 0:
                raise ValueError(f"{name} must be positive")
        if (
            self.minimum_groups is not None
            and self.minimum_requests is not None
            and self.minimum_groups > self.minimum_requests
        ):
            raise ValueError("minimum groups cannot exceed minimum requests")
        if self.confidence_level is not None and (
            type(self.confidence_level) not in (int, float) or not 0 < self.confidence_level < 1
        ):
            raise ValueError("confidence_level must be between zero and one")
        for name, expected in (
            ("semantic_model", ModelEvidence),
            ("generation_model", ModelEvidence),
            ("thresholds", QuestionQualityThresholds),
        ):
            if getattr(self, name) is not None and not isinstance(getattr(self, name), expected):
                raise TypeError(f"{name} must be {expected.__name__} or None")

    @property
    def gaps(self) -> tuple[str, ...]:
        return tuple(name for name in self.__dataclass_fields__ if getattr(self, name) is None)

    @property
    def fingerprint(self) -> str:
        return _digest(self)


@dataclass(frozen=True, slots=True)
class PairedComparisonEvidence:
    """Trusted grouped resampling artifact, supplied by the experiment runner.

    This B0 evaluator checks binding/intervals, not the resampling computation.
    Difference is candidate minus baseline. Cost difference uses cost per
    effective answer; quality difference uses effective-answer/answerable rate.
    """

    candidate_report_sha256: str
    baseline_report_sha256: str
    statistics_protocol_sha256: str
    artifact_reference: str
    group_count: int
    confidence_level: float
    effective_rate_difference_lower: float
    effective_rate_difference_upper: float
    cost_difference_lower_microunits: float
    cost_difference_upper_microunits: float

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if name.endswith("sha256"):
                _hash(value, name)
            elif name not in {"artifact_reference", "group_count"}:
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError(f"{name} must be finite")
        _text(self.artifact_reference, "artifact_reference", 2048)
        if _count(self.group_count, "group_count") == 0:
            raise ValueError("group_count must be positive")
        if not 0 < self.confidence_level < 1:
            raise ValueError("confidence level must be between zero and one")
        if not (
            -1 <= self.effective_rate_difference_lower <= self.effective_rate_difference_upper <= 1
        ):
            raise ValueError("effective-rate interval must be ordered within [-1, 1]")
        if self.cost_difference_lower_microunits > self.cost_difference_upper_microunits:
            raise ValueError("cost interval must be ordered")


@dataclass(frozen=True, slots=True)
class QuestionAcceptanceResult:
    profile_sha256: str
    candidate_report_sha256: str
    ready: bool
    reasons: tuple[str, ...]
    candidate: QuestionCostSummary
    baseline: QuestionCostSummary


def _quality(run: QuestionCostRun) -> dict[str, float]:
    semantic = [request for request in run.requests if not request.diagnostic_only]
    answerable = [request for request in semantic if request.answerable]
    count = len(answerable)
    latencies = sorted(request.latency_ms for request in run.requests)
    errors = {
        AnswerOutcome.INCORRECT_ANSWER,
        AnswerOutcome.INCORRECT_REFUSAL,
        AnswerOutcome.SYSTEM_ERROR,
    }
    return {
        "answerable_coverage": (
            sum(r.effective_answer for r in answerable) / count if count else 0.0
        ),
        "evidence_coverage": sum(r.evidence_complete for r in answerable) / count if count else 0.0,
        "qualifier_fidelity": (
            sum(r.qualifiers_preserved for r in answerable) / count if count else 0.0
        ),
        "error_rate": (
            sum(r.outcome in errors for r in semantic) / len(semantic) if semantic else 1.0
        ),
        "latency_p95_ms": latencies[math.ceil(len(latencies) * 0.95) - 1] if latencies else 0.0,
    }


def evaluate_question_acceptance(
    profile: QuestionAcceptanceProfile,
    candidate: QuestionCostRun,
    baseline: QuestionCostRun,
    *,
    expected_profile_sha256: str,
    comparison: PairedComparisonEvidence | None = None,
) -> QuestionAcceptanceResult:
    """Fail closed on missing evidence. A pass is not operational release approval."""
    if not isinstance(profile, QuestionAcceptanceProfile):
        raise TypeError("profile must be QuestionAcceptanceProfile")
    if any(not isinstance(run, QuestionCostRun) for run in (candidate, baseline)):
        raise TypeError("runs must be QuestionCostRun")
    if comparison is not None and not isinstance(comparison, PairedComparisonEvidence):
        raise TypeError("comparison must be PairedComparisonEvidence or None")
    _hash(expected_profile_sha256, "expected_profile_sha256")
    reasons: list[str] = []
    if profile.fingerprint != expected_profile_sha256:
        reasons.append("profile_changed_after_freeze")
    if profile.contract_version != COST_CONTRACT_VERSION:
        reasons.append("unsupported_cost_contract")
    if profile.allocation_rule != "equal_triggering_requests_else_all_requests/1":
        reasons.append("unsupported_allocation_rule")
    reasons.extend(f"missing_{gap}" for gap in profile.gaps)
    if (
        candidate.workload != baseline.workload
        or candidate.workload.fingerprint != profile.workload_sha256
    ):
        reasons.append("workload_incomparable")
    if candidate.costs.currency != baseline.costs.currency:
        reasons.append("currency_incomparable")
    if candidate.configuration_sha256 != profile.candidate_configuration_sha256:
        reasons.append("candidate_configuration_mismatch")
    if baseline.fingerprint != profile.baseline_report_sha256:
        reasons.append("baseline_report_mismatch")
    manifests = [
        tuple((r.request_id, r.group_id, r.answerable, r.diagnostic_only) for r in run.requests)
        for run in (candidate, baseline)
    ]
    if manifests[0] != manifests[1]:
        reasons.append("request_judging_manifest_mismatch")
    summaries = [summarize_costs(run) for run in (candidate, baseline)]
    for label, run, summary in zip(
        ("candidate", "baseline"), (candidate, baseline), summaries, strict=True
    ):
        if run.dataset_kind != "licensed_real":
            reasons.append(f"{label}_synthetic_data_only")
        if run.dataset_sha256 != profile.dataset_sha256:
            reasons.append(f"{label}_dataset_mismatch")
        if (
            run.dataset_license_reference != profile.dataset_license_reference
            or not run.dataset_license_reference
        ):
            reasons.append(f"{label}_license_missing_or_mismatch")
        if run.judge_sha256 != profile.judge_sha256:
            reasons.append(f"{label}_judge_mismatch")
        model_calls = tuple(entry for entry in run.costs.entries if entry.model_call)
        if any(entry.model_role is None for entry in model_calls):
            reasons.append(f"{label}_unbound_model_calls")
        for role in ("semantic_model", "generation_model"):
            model = getattr(run, role)
            if model != getattr(profile, role) or model.execution == "stub":
                reasons.append(f"{label}_{role}_unverified")
            if model.execution != "not_used" and not any(
                entry.model_role == role for entry in model_calls
            ):
                reasons.append(f"{label}_{role}_execution_unproven")
        if set(run.costs.accounted_phases) != set(CostPhase):
            reasons.append(f"{label}_unaccounted_phases")
        if summary.unresolved_cost_entries:
            reasons.append(f"{label}_unresolved_cost")
        if summary.pending_responsibilities:
            reasons.append(f"{label}_unfinished_responsibilities")
        if summary.unknown_token_calls or summary.estimated_token_calls:
            reasons.append(f"{label}_unmeasured_model_tokens")
        if profile.minimum_requests is not None and summary.all_requests < profile.minimum_requests:
            reasons.append(f"{label}_insufficient_requests")
        groups = len({r.group_id for r in run.requests if not r.diagnostic_only})
        if profile.minimum_groups is not None and groups < profile.minimum_groups:
            reasons.append(f"{label}_insufficient_groups")
        if not summary.effective_answers:
            reasons.append(f"{label}_no_effective_answers")
        if any(not r.safe for r in run.requests):
            reasons.append(f"{label}_unsafe_delivery")
        if any(not r.fresh for r in run.requests):
            reasons.append(f"{label}_stale_delivery")
        if any(r.diagnostic_only and r.outcome == AnswerOutcome.SYSTEM_ERROR for r in run.requests):
            reasons.append(f"{label}_diagnostic_error")
    metrics = _quality(candidate)
    thresholds = profile.thresholds
    if thresholds is not None:
        for name in ("answerable_coverage", "evidence_coverage", "qualifier_fidelity"):
            if metrics[name] < getattr(thresholds, f"minimum_{name}"):
                reasons.append(f"{name}_below_floor")
        for name in ("error_rate", "latency_p95_ms"):
            if metrics[name] > getattr(thresholds, f"maximum_{name}"):
                reasons.append(f"{name}_over_limit")
        for name in ("cost_per_request_microunits", "cost_per_effective_answer_microunits"):
            value = getattr(summaries[0], name)
            if value is not None and value > getattr(thresholds, f"maximum_{name}"):
                reasons.append(f"{name}_over_limit")
        effective_drop = _quality(baseline)["answerable_coverage"] - metrics["answerable_coverage"]
        if effective_drop > thresholds.maximum_effective_rate_drop:
            reasons.append("effective_answer_rate_regressed")
    if comparison is None:
        reasons.append("missing_paired_confidence_intervals")
    else:
        if (
            comparison.candidate_report_sha256 != candidate.fingerprint
            or comparison.baseline_report_sha256 != baseline.fingerprint
        ):
            reasons.append("comparison_report_mismatch")
        if comparison.statistics_protocol_sha256 != profile.statistics_protocol_sha256:
            reasons.append("statistics_protocol_mismatch")
        groups = len({r.group_id for r in candidate.requests if not r.diagnostic_only})
        if comparison.group_count != groups or (
            profile.minimum_groups is not None and comparison.group_count < profile.minimum_groups
        ):
            reasons.append("comparison_group_count_mismatch")
        if comparison.confidence_level != profile.confidence_level:
            reasons.append("comparison_confidence_level_mismatch")
        if (
            thresholds
            and comparison.effective_rate_difference_lower < -thresholds.maximum_effective_rate_drop
        ):
            reasons.append("quality_nonregression_not_established")
        if comparison.cost_difference_upper_microunits >= 0:
            reasons.append("cost_benefit_not_established")
    # No-cost or more-expensive candidates cannot pass on a contradictory CI artifact.
    actual_costs = [summary.cost_per_effective_answer_microunits for summary in summaries]
    if all(value is not None for value in actual_costs) and actual_costs[0] >= actual_costs[1]:
        reasons.append("no_observed_cost_benefit")
    return QuestionAcceptanceResult(
        profile.fingerprint,
        candidate.fingerprint,
        not reasons,
        tuple(reasons),
        *summaries,
    )
