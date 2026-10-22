"""Host-attested paired measurements for existing refresh policy decisions.

Work units are measured tokens, CPU nanoseconds, I/O bytes or storage byte-seconds;
they are never converted to money. Model money remains in ModelBudget. Production
does not import the evaluation ledger or treat missing phases as zero. The host
owns the measuring harness and independent quality verdict; model text cannot
create a trusted observation simply by returning this schema.

ModelBudget attempts have no memory scope or workload membership. The trusted
host must attest that each arm's call census covers the whole measured workload,
including failures, retries and background work, and assign every attempt once.
Resolving a receipt proves its settled amount, not that the host supplied every
receipt or that it belongs to this workload. An omitted/unknown census blocks
policy changes; measured work savings are never reported as currency savings.
"""

from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta

from ..derived.model import DerivedError, digest, timestamp
from ..derived.service import open_derived
from ..domain import MemoryScope
from .decision_records import DecisionJournal, PolicyDecisionRecord
from .model_budget import ModelBudget
from .refresh_policy import RefreshPolicy, temperature_decision

WORK_PHASES = frozenset(
    {
        "cold_start",
        "registration",
        "prewarm",
        "write",
        "dependency",
        "background",
        "foreground",
        "failure",
        "retry",
        "drain",
    }
)
WORK_UNITS = frozenset({"model_tokens", "cpu_ns", "io_bytes", "storage_byte_seconds"})


def _count(value, *, optional=False):
    if optional and value is None:
        return
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ValueError("nonnegative integer measurement required")


def _hash(value):
    if (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValueError("measurement SHA256 reference required")


@dataclass(frozen=True, slots=True)
class StageWork:
    phase: str
    work_units: int | None
    model_cost_keys: tuple[str, ...] = ()
    model_calls: int | None = None

    def __post_init__(self):
        if self.phase not in WORK_PHASES:
            raise ValueError("unknown cost phase")
        _count(self.work_units, optional=True)
        values = tuple(self.model_cost_keys)
        if len(values) > 128 or len(set(values)) != len(values):
            raise ValueError("bounded unique model attempts required")
        for key in values:
            _hash(key)
        object.__setattr__(self, "model_cost_keys", values)
        _count(self.model_calls, optional=True)
        if self.model_calls is not None and self.model_calls != len(values):
            raise ValueError("complete model call census must bind every attempt")


@dataclass(frozen=True, slots=True)
class WorkloadObservation:
    workload_sha256: str
    definition_sha256: str
    environment_sha256: str
    policy_sha256: str
    policy_mode: str
    window_start: datetime
    window_end: datetime
    unit: str
    stages: tuple[StageWork, ...]
    receipt_sha256: str
    quality_protocol_sha256: str
    quality_receipt_sha256: str
    quality_passed: bool | None
    p95_latency_ns: int | None
    pending_operations: int = 0

    def __post_init__(self):
        for name in (
            "workload_sha256",
            "definition_sha256",
            "environment_sha256",
            "policy_sha256",
            "receipt_sha256",
            "quality_protocol_sha256",
            "quality_receipt_sha256",
        ):
            _hash(getattr(self, name))
        if self.policy_mode not in {"on_change", "on_demand", "scheduled", "hybrid"}:
            raise ValueError("unknown observed policy mode")
        for key in ("window_start", "window_end"):
            object.__setattr__(self, key, timestamp(getattr(self, key)).astimezone(UTC))
        if not timedelta(0) < self.window_end - self.window_start <= timedelta(days=1):
            raise ValueError("bounded closed observation window required")
        if self.unit not in WORK_UNITS:
            raise ValueError("explicit measured work unit required")
        stages = tuple(self.stages)
        if len(stages) > len(WORK_PHASES) or any(type(s) is not StageWork for s in stages):
            raise ValueError("typed bounded stages required")
        if len({s.phase for s in stages}) != len(stages):
            raise ValueError("duplicate observed phase")
        keys = [key for s in stages for key in s.model_cost_keys]
        if len(keys) > 128 or len(set(keys)) != len(keys):
            raise ValueError("one model attempt cannot be counted in multiple phases")
        object.__setattr__(self, "stages", tuple(sorted(stages, key=lambda s: s.phase)))
        if self.quality_passed is not None and type(self.quality_passed) is not bool:
            raise ValueError("explicit host quality verdict required")
        _count(self.p95_latency_ns, optional=True)
        _count(self.pending_operations)
        if sum(s.work_units or 0 for s in stages) > 2**63 - 1:
            raise ValueError("measurement total exceeds signed integer capacity")

    @property
    def total_work(self):
        if (
            self.pending_operations
            or {s.phase for s in self.stages} != WORK_PHASES
            or any(s.work_units is None for s in self.stages)
        ):
            return None
        return sum(s.work_units for s in self.stages)

    def payload(self):
        value = asdict(self)
        for key in ("window_start", "window_end"):
            value[key] = value[key].isoformat()
        return value


@dataclass(frozen=True, slots=True)
class PairedWorkObservation:
    scope: MemoryScope
    facet_id: str
    baseline: WorkloadObservation
    candidate: WorkloadObservation
    latency_limit_ns: int

    def __post_init__(self):
        if (
            type(self.scope) is not MemoryScope
            or type(self.facet_id) is not str
            or not (1 <= len(self.facet_id) <= 256)
        ):
            raise ValueError("host scope and facet required")
        if any(type(v) is not WorkloadObservation for v in (self.baseline, self.candidate)):
            raise ValueError("typed paired observations required")
        for name in (
            "workload_sha256",
            "definition_sha256",
            "environment_sha256",
            "unit",
            "window_start",
            "window_end",
            "quality_protocol_sha256",
        ):
            if getattr(self.baseline, name) != getattr(self.candidate, name):
                raise ValueError(
                    "paired measurements must freeze workload, environment and protocol"
                )
        if self.baseline.policy_mode == self.candidate.policy_mode:
            raise ValueError("paired measurements require distinct refresh modes")
        _count(self.latency_limit_ns)

    @property
    def fingerprint(self):
        return digest(
            {
                "scope": self.scope.partition_key(),
                "facet_id": self.facet_id,
                "baseline": self.baseline.payload(),
                "candidate": self.candidate.payload(),
                "latency_limit_ns": self.latency_limit_ns,
            }
        )

    @property
    def net_work_saved(self):
        before, after = self.baseline.total_work, self.candidate.total_work
        return None if before is None or after is None else before - after

    def measurements(self):
        result = []
        for name, observation in (("baseline", self.baseline), ("candidate", self.candidate)):
            stages = {s.phase: s.work_units for s in observation.stages}
            calls = {s.phase: s.model_calls for s in observation.stages}
            result.extend((f"{name}.{phase}", stages.get(phase)) for phase in sorted(WORK_PHASES))
            result.extend(
                (f"{name}.calls.{phase}", calls.get(phase)) for phase in sorted(WORK_PHASES)
            )
            result.extend(
                (
                    (f"{name}.total", observation.total_work),
                    (f"{name}.p95_latency_ns", observation.p95_latency_ns),
                    (f"{name}.pending_operations", observation.pending_operations),
                )
            )
        result.extend(
            (("net_work_saved", self.net_work_saved), ("latency_limit_ns", self.latency_limit_ns))
        )
        return tuple(result)


async def observation_context(queue, facet_id, *, processor_key=None):
    """Get the exact host policy/input binding before a paired measuring run."""
    from .refresh_demand import _compatibility, observed_clock

    processor = queue._processor(processor_key)
    async with queue.repository.unit_of_work() as uow:
        await open_derived(uow, processor.scope)
        await observed_clock(uow, processor.scope, queue.clock)
        definition = await processor.definition(uow, facet_id)
        binding = await uow.derived_get(processor.scope, "refresh_policy", facet_id)
        if not binding or binding.get("state") != "configured":
            raise DerivedError("refresh_policy_unavailable")
        return deepcopy(
            {
                "scope": processor.scope,
                "facet_id": facet_id,
                "definition_sha256": _compatibility(definition),
                "policy": binding["policy"],
                "stats": binding["stats"],
                "previous_window": binding.get("previous_window"),
            }
        )


async def observe_temperature(
    queue,
    facet_id,
    observation,
    *,
    expected_revision,
    next_revision,
    budget_available,
    apply=False,
    processor_key=None,
):
    """Record a measured recommendation; explicitly apply with one transactional CAS.

    The current counters come from guarded reads. Caller-supplied traffic counts are
    never accepted. Model receipts are resolved from the existing finance ledger under
    its lock before taking lifecycle locks. An unknown or failed quality/cost gate keeps
    the existing policy. A stale snapshot or revision rejects the operation entirely.
    """
    from .refresh_demand import _compatibility, observed_clock, record_dirty

    if (
        type(observation) is not PairedWorkObservation
        or type(apply) is not bool
        or (type(budget_available) is not bool)
    ):
        raise TypeError("typed observation and explicit host controls required")
    processor = queue._processor(processor_key)
    if observation.scope != processor.scope or observation.facet_id != facet_id:
        raise DerivedError("refresh_observation_scope_mismatch")
    for value in (expected_revision, next_revision):
        if type(value) is not str or not 1 <= len(value) <= 128:
            raise ValueError("bounded policy revision required")
    if expected_revision == next_revision:
        raise DerivedError("refresh_policy_revision_must_change")
    journal = DecisionJournal(queue.repository, processor.scope)
    attempts = [
        (stage.phase, key)
        for item in (observation.baseline, observation.candidate)
        for stage in item.stages
        for key in stage.model_cost_keys
    ]
    async with queue.repository.unit_of_work() as uow:
        if attempts:
            await ModelBudget.lock(uow)
        calls = {key: await uow.model_budget_get("call", key) for _, key in attempts}
        finance_version = digest(
            {
                key: {
                    name: row.get(name)
                    for name in (
                        "state",
                        "actual_microunits",
                        "receipt_sha256",
                        "currency",
                        "phase",
                    )
                }
                if row
                else None
                for key, row in calls.items()
            }
        )
        identity = digest(
            [
                "temperature",
                observation.fingerprint,
                expected_revision,
                next_revision,
                budget_available,
                apply,
                finance_version,
            ]
        )
        await open_derived(uow, processor.scope)
        now = await observed_clock(uow, processor.scope, queue.clock)
        old = await journal.get_in(uow, identity)
        if old is not None:
            return old  # Immutable replay acknowledges the prior decision; never reapplies it.
        definition = await processor.definition(uow, facet_id)
        binding = await uow.derived_get(processor.scope, "refresh_policy", facet_id)
        if (
            not binding
            or binding.get("state") != "configured"
            or binding.get("adapter_key") != processor.key
            or binding.get("epoch") != definition["epoch"]
        ):
            raise DerivedError("refresh_policy_unavailable")
        policy = RefreshPolicy.from_payload(binding["policy"])
        baseline, candidate = observation.baseline, observation.candidate
        if (
            policy.revision != expected_revision
            or digest(policy.payload()) != baseline.policy_sha256
        ):
            raise DerivedError("refresh_policy_revision_conflict")
        proposed = replace(policy, mode=candidate.policy_mode, revision=next_revision)
        if digest(proposed.payload()) != candidate.policy_sha256:
            raise DerivedError("refresh_candidate_policy_changed")
        if baseline.definition_sha256 != _compatibility(definition):
            raise DerivedError("refresh_observation_inputs_changed")
        if baseline.window_end > now or baseline.window_end - baseline.window_start != timedelta(
            seconds=policy.hotness_window_seconds
        ):
            raise DerivedError("refresh_observation_window_incomplete")
        windows = [v for v in (binding.get("stats"), binding.get("previous_window")) if v]
        stats = next(
            (
                v
                for v in windows
                if datetime.fromisoformat(v["window_started_at"]) == baseline.window_start
            ),
            None,
        )
        if stats is None or now - baseline.window_end > timedelta(
            seconds=policy.hotness_window_seconds
        ):
            raise DerivedError("refresh_observation_window_stale")
        gaps = []
        if observation.net_work_saved is None:
            gaps.append("work_phases_unknown_or_pending")
        elif observation.net_work_saved <= 0:
            gaps.append("measured_work_not_saved")
        if any(v.quality_passed is not True for v in (baseline, candidate)):
            gaps.append("quality_gate_not_passed")
        if any(
            stage.model_calls is None for item in (baseline, candidate) for stage in item.stages
        ):
            gaps.append("model_call_census_unknown")
        if (
            candidate.p95_latency_ns is None
            or candidate.p95_latency_ns > observation.latency_limit_ns
        ):
            gaps.append("latency_gate_not_passed")
        currencies = set()
        for phase, key in attempts:
            call = calls[key]
            if not call or call["state"] != "settled" or call["actual_microunits"] is None:
                gaps.append("model_cost_unsettled")
            elif call["phase"] != phase:
                raise DerivedError("refresh_model_cost_phase_mismatch")
            else:
                currencies.add(call["currency"])
        if len(currencies) > 1:
            gaps.append("model_cost_currency_mismatch")
        money = []
        for item in (baseline, candidate):
            keys = {key for stage in item.stages for key in stage.model_cost_keys}
            total = (
                sum(calls[key]["actual_microunits"] for key in keys)
                if {stage.phase for stage in item.stages} == WORK_PHASES
                and not item.pending_operations
                and all(stage.model_calls is not None for stage in item.stages)
                and all(
                    calls[key]
                    and calls[key]["state"] == "settled"
                    and calls[key]["actual_microunits"] is not None
                    for key in keys
                )
                else None
            )
            _count(total, optional=True)
            money.append(total)
        if len(currencies) <= 1 and all(v is not None for v in money) and money[1] > money[0]:
            gaps.append("model_cost_regression")
        measured = observation.net_work_saved if not gaps else None
        chosen = temperature_decision(
            policy,
            {**stats, "measured_net_work_saved": measured},
            now=now,
            budget_available=budget_available,
        )
        if gaps:
            chosen = policy.mode
        if chosen != policy.mode and chosen != candidate.policy_mode:
            raise DerivedError("refresh_unmeasured_policy_target")
        changed = chosen != policy.mode
        reasons = tuple(
            dict.fromkeys(
                (
                    *gaps,
                    "paired_host_measurement",
                    f"unit.{baseline.unit}",
                    *(f"currency.{currency}" for currency in sorted(currencies)),
                    "temperature_threshold_met" if changed else "temperature_policy_unchanged",
                )
            )
        )
        record = PolicyDecisionRecord(
            id=identity,
            scope=processor.scope,
            stage="refresh",
            action=chosen,
            reasons=reasons,
            policy_version=policy.revision,
            input_versions=tuple(
                dict.fromkeys(
                    (
                        baseline.definition_sha256,
                        baseline.policy_sha256,
                        candidate.policy_sha256,
                        finance_version,
                    )
                )
            ),
            evidence_references=tuple(
                dict.fromkeys(
                    (
                        observation.fingerprint,
                        baseline.receipt_sha256,
                        candidate.receipt_sha256,
                        baseline.quality_protocol_sha256,
                        baseline.quality_receipt_sha256,
                        candidate.quality_receipt_sha256,
                    )
                )
            ),
            cost_references=tuple(sorted({key for _, key in attempts})),
            measurements=(
                *observation.measurements(),
                ("baseline.model_microunits", money[0]),
                ("candidate.model_microunits", money[1]),
            ),
            created_at=now,
            outcome="applied" if changed and apply else "proposed" if changed else "skipped",
        )
        await journal.append_in(uow, record)
        stats["measured_net_work_saved"] = measured
        binding["last_temperature_decision"] = record.id
        if changed and apply:
            binding.update(policy=proposed.payload(), configured_at=now.isoformat())
            binding["stats"]["changed_at"] = now.isoformat()
        await uow.derived_put(processor.scope, "refresh_policy", facet_id, binding)
        if changed and apply:
            definition["dirty"] = True
            await uow.derived_put(processor.scope, "definition", facet_id, definition)
            demand = await record_dirty(
                uow, processor.scope, definition, at=now, reason="temperature_policy"
            )
            await queue._admit(uow, processor, demand)
        return record
