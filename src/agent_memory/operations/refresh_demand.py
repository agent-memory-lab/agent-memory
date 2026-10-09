"""Durable coalesced responsibility, independent from processor business logic.

An unsealed dirty batch may collect changes. A receipt or claim seals its exact
identity; changes afterward create a distinct successor. Only an atomic checked
publication can discharge those identities. Slot maxima are never coverage.
"""

from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from secrets import token_urlsafe
from typing import Protocol

from ..derived.model import DerivedError, digest, identity, timestamp
from ..derived.question_model import CoverageFrontier, CoverageTarget, QuestionTime, TimeMode
from ..derived.service import open_derived
from ..domain import utc_now
from .facet_refresh import stale, valid_completion
from .refresh_policy import RefreshLimits, RefreshPolicy
from .refresh_processor import ObservationRefreshProcessor as ObservationRefreshProcessor
from .worker_tasks import WorkerFailureDisposition, WorkerLease, WorkerTask, WorkerTaskStatus

CONTRACT = "durable-coalescing/1"
TASK_TYPE = "memory.facet_refresh"
MAX_REQUESTS = 4096
DEFER_CODES = {
    "derived_parent_unavailable",
    "derived_parent_stale",
    "derived_parent_proof_missing",
    "derived_refresh_backpressure",
    "derived_authority_unavailable",
    "derived_authority_expired",
    "derived_authority_denied",
    "derived_authority_rollback",
    "trusted_authority_version_required",
    "derived_query_unavailable",
    "derived_query_configuration_changed",
    "page_backend_unsupported",
    "refresh_quota",
    "refresh_backend_unsupported",
    "refresh_clock_discontinuity",
    "refresh_legacy_lease_active",
}


def supported(uow):
    return getattr(uow, "refresh_scheduler_contract", None) == CONTRACT and all(
        callable(getattr(uow, method, None))
        for method in (
            "refresh_scheduler_lock",
            "refresh_scheduler_config",
            "refresh_scheduler_due",
            "refresh_scheduler_usage",
            "refresh_scheduler_expired",
            "refresh_scheduler_reserve",
            "refresh_scheduler_release",
            "refresh_scheduler_turn",
            "refresh_scheduler_observe_clock",
        )
    )


async def scheduler_open(uow, scope):
    if not supported(uow):
        raise DerivedError("refresh_backend_unsupported")
    await uow.refresh_scheduler_lock(scope)


async def observed_clock(uow, scope, clock):
    """A database-wide wall high-water survives independent worker restarts."""
    await scheduler_open(uow, scope)
    now = _utc(clock())  # Sample after the shared lock, never before waiting for it.
    if not await uow.refresh_scheduler_observe_clock(scope, now=now.isoformat()):
        raise DerivedError("refresh_clock_discontinuity")
    return now


def _compatibility(definition):
    # A current finite coverage target may be fulfilled later, but never across
    # definition, permission/erasure, or context changes. Query generations remain
    # exact processor inputs, not a fabricated continuous frontier.
    return digest(
        [
            definition["fingerprint"],
            definition["generation"],
            definition["epoch"],
            definition["safety_generation"],
            definition["spec"].get("context"),
            definition["spec"].get("history_mode"),
        ]
    )


def _utc(value):
    return timestamp(value).astimezone(UTC)


def _parse(value):
    return datetime.fromisoformat(value) if value is not None else None


async def admit_demand(uow, scope, row, config):
    """Quota excess defers computation, never discards transactional dirty state."""
    if row["status"] not in {"pending", "retry"}:
        return
    limits = config.get("limits")
    if limits is None:
        raise DerivedError("refresh_policy_limits_missing")
    await uow.refresh_scheduler_config(limits)
    usage = await uow.refresh_scheduler_usage(
        now=row["last_dirty_at"], tenant_id=scope.tenant_id, instance_key=row["instance_key"]
    )
    if any(
        usage[name] > limits[name]
        for name in ("global_pending", "tenant_pending", "instance_pending")
    ):
        row.update(status="deferred", reason="refresh_pending_quota")
        await uow.derived_put(scope, "refresh_demand", row["id"], row)


def _receipt(row):
    # The externally returned receipt is immutable. Publication pointers and
    # accumulated partial coverage are internal status bookkeeping only.
    return deepcopy(
        {
            key: row[key]
            for key in (
                "schema",
                "target_id",
                "target",
                "facet_id",
                "adapter_key",
                "epoch",
                "deadline",
            )
        }
    )


def _due(row, policy, now, *, explicit=False):
    if row.get("active_execution"):
        return  # The immutable execution owns its lease due; successor stays durable.
    boundaries = [v for v in (row.get("next_transition_at"), row.get("scheduled_at")) if v]
    if not row["requested"]:
        row.update(status="idle", due_at=min(boundaries) if boundaries else None, active_since=None)
        return
    if not explicit and not row.get("explicit") and policy.mode == "on_demand":
        row.update(status="dirty", due_at=row.get("next_transition_at"), active_since=None)
        return
    if row.get("active_since") is None:
        row.update(active_since=now.isoformat(), progress_at=now.isoformat())
    due = policy.due(
        _parse(row["first_dirty_at"]),
        _parse(row["last_dirty_at"]),
        boundary=_parse(row.get("next_transition_at")),
        deadline=_parse(row.get("deadline")),
    )
    if policy.mode == "scheduled" and not explicit and not row.get("explicit"):
        row["scheduled_at"] = (
            row.get("scheduled_at")
            or (
                _parse(row["first_dirty_at"]) + timedelta(seconds=policy.schedule_seconds)
            ).isoformat()
        )
        times = [
            _parse(row["scheduled_at"]),
            _parse(row["first_dirty_at"]) + timedelta(seconds=policy.max_wait_seconds),
        ]
        times.extend(_parse(row[key]) for key in ("next_transition_at", "deadline") if row.get(key))
        due = min(times)
    row.update(status="pending", due_at=due.isoformat())
    if explicit:
        row["due_at"] = min(row["due_at"], now.isoformat())


def _exhausted(row, policy, now):
    return row.get("active_since") is not None and (
        now - _parse(row["active_since"]) >= timedelta(seconds=policy.max_age_seconds)
        or now - _parse(row["progress_at"]) >= timedelta(seconds=policy.max_no_progress_seconds)
        or row.get("failed_attempts", 0) >= policy.max_attempts
    )


def _recover_explicit(row, policy, now):
    # Dirty writes may change the runnable projection without granting another
    # execution budget. An explicit new request can revive exhausted, unowned
    # responsibility even when a later write changed its status from dead.
    if not row.get("active_execution") and (
        row["status"] == "dead" or _exhausted(row, policy, now)
    ):
        row.update(
            recovery_generation=row.get("recovery_generation", 0) + 1,
            failed_attempts=0,
            active_since=now.isoformat(),
            progress_at=now.isoformat(),
            runnable_at=None,
            reason=None,
        )


async def record_dirty(uow, scope, definition, *, at=None, reason="changed"):
    """Same original write UoW. No capacity limit can discard invalidation.

    This hook is opt-in. Managed definitions fail closed on old/unknown adapters;
    an erased policy leaves a durable unavailable marker, never legacy fallback.
    """
    if not definition.get("refresh_managed"):
        return None
    await scheduler_open(uow, scope)
    config = await uow.derived_get(scope, "refresh_policy", definition["facet_id"])
    if not config or config.get("state") == "erased":
        return None  # Definition remains dirty/unavailable until explicit reconfiguration.
    if config.get("schema") != "refresh-policy-binding/1":
        raise DerivedError("refresh_policy_schema_unsupported")
    policy = RefreshPolicy.from_payload(config["policy"])
    now = _utc(at or utc_now())
    compatibility = _compatibility(definition)
    key = "refresh-demand:" + digest([scope.partition_key(), definition["facet_id"], compatibility])
    row = await uow.derived_get(scope, "refresh_demand", key)
    if row is None or row.get("status") == "erased":
        row = dict(
            schema="refresh-demand/1",
            id=key,
            facet_id=definition["facet_id"],
            scope=asdict(scope),
            tenant_id=scope.tenant_id,
            instance_key=config["instance_key"],
            adapter_key=config["adapter_key"],
            epoch=definition["epoch"],
            compatibility=compatibility,
            policy=policy.payload(),
            priority=policy.priority,
            aging_seconds=policy.aging_seconds,
            requested=[],
            obligation_at={},
            active_execution=None,
            first_dirty_at=now.isoformat(),
            last_dirty_at=now.isoformat(),
            created_at=now.isoformat(),
            progress_at=now.isoformat(),
            next_transition_at=definition.get("next_transition_at"),
            due_at=None,
            status="dirty",
            sequence=0,
            explicit=False,
            unsealed=None,
        )
    if not row["requested"]:
        row["first_dirty_at"] = now.isoformat()
        row["progress_at"] = now.isoformat()
    row["last_dirty_at"] = max(row["last_dirty_at"], now.isoformat())
    if not row.get("unsealed"):
        row["sequence"] += 1
        obligation = "refresh-obligation:" + digest([key, row["sequence"], now.isoformat()])
        row["requested"].append(obligation)
        row.setdefault("obligation_at", {})[obligation] = now.isoformat()
        row["unsealed"] = obligation
    row["last_reason"] = reason
    row["policy"] = policy.payload()
    row.update(priority=policy.priority, aging_seconds=policy.aging_seconds)
    if policy.mode not in {"scheduled", "hybrid"} or policy.schedule_seconds is None:
        row["scheduled_at"] = None
    row["next_transition_at"] = (
        _utc(_parse(definition["next_transition_at"])).isoformat()
        if definition.get("next_transition_at")
        else None
    )
    _due(row, policy, now)
    await uow.derived_put(scope, "refresh_demand", key, row)
    await admit_demand(uow, scope, row, config)
    return row


async def activate_exact_request(uow, scope, definition, *, at):
    """Route legacy exact demand through shared admission without changing its wire."""
    if not definition.get("refresh_managed"):
        return
    await scheduler_open(uow, scope)
    config = await uow.derived_get(scope, "refresh_policy", definition["facet_id"])
    if not config or config.get("state") == "erased":
        raise DerivedError("refresh_policy_unavailable")
    key = "refresh-demand:" + digest(
        [scope.partition_key(), definition["facet_id"], _compatibility(definition)]
    )
    row = await uow.derived_get(scope, "refresh_demand", key)
    if not row or not row.get("requested"):
        row = await record_dirty(uow, scope, definition, at=at, reason="exact_request")
    policy = RefreshPolicy.from_payload(row["policy"])
    _recover_explicit(row, policy, _utc(at))
    row.update(explicit=True, unsealed=None)
    _due(row, policy, _utc(at), explicit=True)
    await uow.derived_put(scope, "refresh_demand", key, row)
    await admit_demand(uow, scope, row, config)


async def record_guarded_read(uow, scope, definition, result, *, at):
    """Count only reads that actually passed service authorization and freshness.

    Store two aggregate windows at most; no reader identities or request bodies.
    No counterfactual expense is invented from these traffic counters.
    """
    if not definition.get("refresh_managed"):
        return
    await scheduler_open(uow, scope)
    binding = await uow.derived_get(scope, "refresh_policy", definition["facet_id"])
    if not binding or binding.get("state") == "erased":
        return
    policy = RefreshPolicy.from_payload(binding["policy"])
    now = _utc(at)
    stats = binding.setdefault(
        "stats",
        dict(
            window_started_at=now.isoformat(),
            changed_at=binding["configured_at"],
            authorized_reads=0,
            valid_reuses=0,
            measured_net_work_saved=None,
        ),
    )
    if now >= _parse(stats["window_started_at"]) + timedelta(seconds=policy.hotness_window_seconds):
        binding["previous_window"] = deepcopy(stats)
        stats = dict(
            window_started_at=now.isoformat(),
            changed_at=stats["changed_at"],
            authorized_reads=0,
            valid_reuses=0,
            measured_net_work_saved=None,
        )
        binding["stats"] = stats
    stats["authorized_reads"] = min(stats["authorized_reads"] + 1, 2**63 - 1)
    stats["valid_reuses"] = min(
        stats["valid_reuses"] + int(result["state"] in {"ready", "empty"}), 2**63 - 1
    )
    await uow.derived_put(scope, "refresh_policy", definition["facet_id"], binding)


class RefreshProcessor(Protocol):
    """Adapter contract for future question processors; scheduler has no templates."""

    key: str
    scope: object
    repository: object
    task_type: str

    def instance_id(self, definition): ...
    def target_metadata(self, definition): ...
    async def definition(self, uow, facet_id): ...
    async def freeze(self, uow, definition): ...
    async def snapshot(self, task): ...
    def prepare(self, snapshot): ...
    async def publish(self, task, snapshot, prepared): ...
    async def verify_coverage(self, uow, execution): ...
    async def authorize(self, uow, definition, actor): ...
    async def required_parents(self, uow, definition): ...
    async def check_task(self, uow, task, *, completed=False): ...


async def publish_coverage(uow, service, job, definition, *, manifest, now):
    """Called only inside the processor's checked content-publication transaction."""
    execution_id = job.get("refresh_execution")
    if execution_id is None:
        return
    commit_at = await observed_clock(uow, service.scope, service.clock)
    execution = await uow.derived_get(service.scope, "refresh_execution", execution_id)
    if (
        not execution
        or execution.get("status") != "running"
        or (
            execution["unit"] != job["unit"]
            or execution["fence"] != job["fence"]
            or execution["generation"] != job["generation"]
            or execution["compatibility"] != _compatibility(definition)
            or not valid_completion(service.scope, job)
            or not manifest.get("query_complete")
            or manifest.get("unit") != job["unit"]
        )
    ):
        raise DerivedError("refresh_publication_unverified")
    if commit_at >= _parse(execution["lease_until"]) or commit_at >= _parse(
        execution["expires_at"]
    ):
        raise stale()
    if definition.get("next_transition_at") and (
        _parse(definition["next_transition_at"]) <= commit_at
    ):
        raise DerivedError("derived_time_coverage_expired")
    row = await uow.derived_get(service.scope, "refresh_demand", execution["demand_id"])
    if (
        not row
        or row.get("active_execution") != execution_id
        or not set(execution["claimed"]).issubset(row["requested"])
    ):
        raise DerivedError("refresh_responsibility_changed")
    now = commit_at
    publication = dict(
        schema="refresh-publication/1",
        id=execution_id,
        facet_id=definition["facet_id"],
        state="committed",
        claimed=execution["claimed"],
        unit=job["unit"],
        compatibility=execution["compatibility"],
        manifest_sha256=digest(manifest),
        commit_token=job["commit_token"],
        outcome=("noop_verified" if job["outcome"] == "noop" else "applied_full"),
        published_at=now.isoformat(),
        epoch=execution["epoch"],
    )
    publication["sha256"] = digest(publication)
    await uow.derived_put(service.scope, "refresh_publication", execution_id, publication)
    execution.update(status="completed", completed_at=now.isoformat(), progress_at=now.isoformat())
    await uow.derived_put(service.scope, "refresh_execution", execution_id, execution)
    claimed = set(execution["claimed"])
    row["requested"] = [key for key in row["requested"] if key not in claimed]
    row["obligation_at"] = {
        key: value for key, value in row["obligation_at"].items() if key not in claimed
    }
    row.update(active_execution=None, progress_at=now.isoformat(), reason=None, failed_attempts=0)
    if row["requested"]:
        row["first_dirty_at"] = min(row["obligation_at"][key] for key in row["requested"])
        row["active_since"] = max(
            row.get("active_since") or row["first_dirty_at"], row["first_dirty_at"]
        )
    if not row["requested"]:
        row.update(explicit=False, unsealed=None, deadline=None)
    row["next_transition_at"] = (
        _utc(_parse(definition["next_transition_at"])).isoformat()
        if definition.get("next_transition_at")
        else None
    )
    policy = RefreshPolicy.from_payload(row["policy"])
    if policy.schedule_seconds is not None and policy.mode in {"scheduled", "hybrid"}:
        row["scheduled_at"] = (now + timedelta(seconds=policy.schedule_seconds)).isoformat()
    _due(row, policy, now)
    await uow.derived_put(service.scope, "refresh_demand", row["id"], row)
    config = await uow.derived_get(service.scope, "refresh_policy", row["facet_id"])
    await admit_demand(uow, service.scope, row, config)
    definition["dirty"] = bool(row["requested"])
    await uow.derived_put(service.scope, "definition", definition["facet_id"], definition)
    pending_deadlines, pending_explicit = [], False
    for item in await uow.derived_records(service.scope, "coverage_request"):
        receipt = item["payload"]
        if receipt.get("demand_id") != row["id"] or receipt.get("state") == "erased":
            continue
        required = set(receipt["target"]["required_frontier"]["units"])
        covered = required.intersection(claimed)
        if covered:
            receipt.setdefault("publications", {})[execution_id] = sorted(covered)
            await uow.derived_put(service.scope, "coverage_request", item["identity"], receipt)
        discharged = {member for members in receipt["publications"].values() for member in members}
        if required - discharged:
            pending_explicit = True
            if receipt.get("deadline"):
                pending_deadlines.append(receipt["deadline"])
    # A fulfilled finite reader does not turn a cold instance permanently hot or
    # keep chasing unrelated successor changes. A still-waiting child can issue
    # another bounded parent demand through the same governed path.
    row["explicit"] = pending_explicit
    row["deadline"] = min(pending_deadlines) if pending_deadlines else None
    _due(row, policy, now)
    await uow.derived_put(service.scope, "refresh_demand", row["id"], row)
    await admit_demand(uow, service.scope, row, config)
    await uow.refresh_scheduler_release(service.scope, execution_id)


class RefreshDemandQueue:
    """Multi-processor worker queue with one database-wide admission budget."""

    def __init__(self, processors, *, limits=None, clock=utc_now, default_processor_key=None):
        processors = tuple(processors)
        if (
            not processors
            or len(processors) > 128
            or len({p.key for p in processors}) != len(processors)
        ):
            raise ValueError("one to 128 distinct refresh processors required")
        if any(p.repository is not processors[0].repository for p in processors):
            raise ValueError("refresh processors must share one repository")
        self.processors = {p.key: p for p in processors}
        if default_processor_key is not None and default_processor_key not in self.processors:
            raise ValueError("default refresh processor must be registered")
        self.default_processor_key = default_processor_key
        self.repository = processors[0].repository
        self.limits = limits or RefreshLimits()
        self.clock = clock
        self.stopping = False

    def _processor(self, key=None):
        if key is None:
            key = self.default_processor_key
        if key is None and len(self.processors) == 1:
            return next(iter(self.processors.values()))
        if key not in self.processors:
            raise DerivedError("refresh_processor_unsupported")
        return self.processors[key]

    async def configure(self, facet_id, policy, *, processor_key=None):
        if not isinstance(policy, RefreshPolicy):
            raise TypeError("RefreshPolicy required")
        processor = self._processor(processor_key)
        now = _utc(self.clock())
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, processor.scope)
            now = await observed_clock(uow, processor.scope, self.clock)
            await uow.refresh_scheduler_config(self.limits.payload())
            definition = await processor.definition(uow, facet_id)
            legacy_pending = False
            for item in await uow.derived_records(processor.scope, "job"):
                job = item["payload"]
                legacy_pending = legacy_pending or (
                    job.get("unit", {}).get("facet_id") == facet_id
                    and job.get("status") in {"pending", "retry"}
                    and not job.get("refresh_execution")
                )
                if (
                    job.get("unit", {}).get("facet_id") == facet_id
                    and job.get("status") == "running"
                    and not job.get("refresh_execution")
                    and _parse(job["lease_until"]) > now
                ):
                    # Coordinated opt-in must drain old unaccounted leases first.
                    # Never relabel in-flight legacy work as governed concurrency.
                    raise DerivedError("refresh_legacy_lease_active")
            old = await uow.derived_get(processor.scope, "refresh_policy", facet_id)
            if old and old.get("state") != "erased" and old.get("adapter_key") != processor.key:
                raise DerivedError("refresh_processor_configuration_changed")
            row = dict(
                schema="refresh-policy-binding/1",
                facet_id=facet_id,
                adapter_key=processor.key,
                instance_key=processor.instance_id(definition),
                policy=policy.payload(),
                state="configured",
                epoch=definition["epoch"],
                configured_at=now.isoformat(),
                limits=self.limits.payload(),
                stats=(old or {}).get("stats")
                or dict(
                    window_started_at=now.isoformat(),
                    changed_at=now.isoformat(),
                    authorized_reads=0,
                    valid_reuses=0,
                    measured_net_work_saved=None,
                ),
            )
            if old and old.get("policy", {}).get("mode") != policy.mode:
                row["stats"]["changed_at"] = now.isoformat()
            await uow.derived_put(processor.scope, "refresh_policy", facet_id, row)
            definition.update(refresh_managed=True, dirty=True)
            await uow.derived_put(processor.scope, "definition", facet_id, definition)
            demand = await record_dirty(
                uow, processor.scope, definition, at=now, reason="configure"
            )
            if legacy_pending:
                await activate_exact_request(uow, processor.scope, definition, at=now)
                demand = await uow.derived_get(processor.scope, "refresh_demand", demand["id"])
            await self._admit(uow, processor, demand)
            return deepcopy(demand)

    async def _admit(self, uow, processor, row):
        await uow.refresh_scheduler_config(self.limits.payload())
        if row["status"] not in {"pending", "retry"}:
            return
        usage = await uow.refresh_scheduler_usage(
            now=timestamp(self.clock()).isoformat(),
            tenant_id=processor.scope.tenant_id,
            instance_key=row["instance_key"],
        )
        # The candidate has already been projected by the original write hook.
        if any(
            usage[key] > getattr(self.limits, key)
            for key in ("global_pending", "tenant_pending", "instance_pending")
        ):
            row.update(status="deferred", reason="refresh_pending_quota")
            await uow.derived_put(processor.scope, "refresh_demand", row["id"], row)

    async def request(self, facet_id, *, dedupe_key, actor, processor_key=None, deadline=None):
        identity(dedupe_key)
        processor = self._processor(processor_key)
        now = _utc(self.clock())
        if deadline is not None:
            deadline = _utc(deadline)
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, processor.scope)
            now = await observed_clock(uow, processor.scope, self.clock)
            await uow.refresh_scheduler_config(self.limits.payload())
            definition = await processor.definition(uow, facet_id)
            await self._authorize(processor, uow, definition, actor)
            key = "refresh-target:" + digest([processor.scope.partition_key(), epoch, dedupe_key])
            old = await uow.derived_get(processor.scope, "coverage_request", key)
            if old:
                if old.get("deadline") != (deadline.isoformat() if deadline is not None else None):
                    raise DerivedError("derived_target_conflict")
                if old.get("facet_id") != facet_id or old.get("adapter_key") != processor.key:
                    raise DerivedError("derived_target_conflict")
                return _receipt(old)
            if len(await uow.derived_records(processor.scope, "coverage_request")) >= MAX_REQUESTS:
                raise DerivedError("derived_target_capacity")
            if not definition.get("refresh_managed"):
                raise DerivedError("refresh_policy_unconfigured")
            config = await uow.derived_get(processor.scope, "refresh_policy", facet_id)
            if not config or config.get("state") == "erased":
                raise DerivedError("refresh_policy_unavailable")
            # Requests are finite even if further writes continue indefinitely.
            demand_id = "refresh-demand:" + digest(
                [processor.scope.partition_key(), facet_id, _compatibility(definition)]
            )
            row = await uow.derived_get(processor.scope, "refresh_demand", demand_id)
            if not row or not row.get("requested"):
                row = await record_dirty(uow, processor.scope, definition, at=now, reason="request")
            if len(row["requested"]) > 4096:
                raise DerivedError("derived_target_capacity")
            policy = RefreshPolicy.from_payload(row["policy"])
            _recover_explicit(row, policy, now)
            row.update(explicit=True, unsealed=None)
            if deadline is not None:
                row["deadline"] = min(
                    row.get("deadline") or deadline.isoformat(), deadline.isoformat()
                )
            policy = RefreshPolicy.from_payload(row["policy"])
            _due(row, policy, now, explicit=True)
            await uow.derived_put(processor.scope, "refresh_demand", row["id"], row)
            await self._admit(uow, processor, row)
            target = CoverageTarget(
                instance_id=row["instance_key"],
                scope=processor.scope,
                time=QuestionTime(TimeMode.CURRENT, None, None),
                requested_at=now,
                required_frontier=CoverageFrontier("exact_units", {}, tuple(row["requested"])),
                **processor.target_metadata(definition),
            )
            receipt = dict(
                schema="coverage-receipt/1",
                target_id=key,
                target=target.payload(),
                facet_id=facet_id,
                adapter_key=processor.key,
                demand_id=row["id"],
                epoch=epoch,
                readers=definition["spec"]["readers"],
                publications={},
                state="pending",
                compatibility=row["compatibility"],
                deadline=deadline.isoformat() if deadline is not None else None,
            )
            await uow.derived_put(processor.scope, "coverage_request", key, receipt)
            return _receipt(receipt)

    @staticmethod
    async def _authorize(processor, uow, definition, actor):
        await processor.authorize(uow, definition, actor)

    async def status(self, target_id, *, actor, processor_key=None):
        processor = self._processor(processor_key)
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, processor.scope)
            receipt = await uow.derived_get(
                processor.scope, "coverage_request", identity(target_id)
            )
            if not receipt or receipt.get("state") == "erased" or receipt.get("epoch") != epoch:
                raise DerivedError("derived_target_unavailable")
            if receipt.get("adapter_key") != processor.key:
                raise DerivedError("refresh_processor_unsupported")
            definition = await processor.definition(uow, receipt["facet_id"])
            await self._authorize(processor, uow, definition, actor)
            if actor not in receipt["readers"]:
                raise DerivedError("derived_read_denied")
            target = CoverageTarget.from_payload(receipt["target"])
            covered = set()
            coverage_at = {}
            for execution_id, members in receipt["publications"].items():
                execution = await uow.derived_get(
                    processor.scope, "refresh_execution", execution_id
                )
                publication = await processor.verify_coverage(uow, execution) if execution else None
                if (
                    publication
                    and set(members).issubset(publication["claimed"])
                    and (publication["compatibility"] == receipt["compatibility"])
                ):
                    covered.update(members)
                    for member in members:
                        at = _parse(publication["published_at"])
                        coverage_at[member] = min(coverage_at.get(member, at), at)
            complete = set(target.required_frontier.units).issubset(covered)
            row = await uow.derived_get(processor.scope, "refresh_demand", receipt["demand_id"])
            return dict(
                target_id=target_id,
                schema="coverage-status/1",
                complete=complete,
                state="completed" if complete else (row or {}).get("status", "unavailable"),
                reason=(row or {}).get("reason"),
                required=target.required_frontier.payload(),
                covered=sorted(covered),
                current_ready=None,
                deadline_missed=bool(
                    receipt.get("deadline")
                    and _parse(receipt["deadline"]) < (
                        max(coverage_at[member] for member in target.required_frontier.units)
                        if complete else self.clock()
                    )
                ),
            )

    async def initialize(self):
        """Validate every route and the current erase epoch before accepting work."""
        for processor in self.processors.values():
            async with self.repository.unit_of_work() as uow:
                await open_derived(uow, processor.scope)
                await observed_clock(uow, processor.scope, self.clock)
                await uow.refresh_scheduler_config(self.limits.payload())
                initialize = getattr(processor, "initialize", None)
                if initialize is not None:
                    await initialize(uow)
        self.stopping = False

    async def claim(self, worker_id, *, lease_seconds, target_id=None, processor_key=None):
        identity(worker_id)
        if type(lease_seconds) is not int or not 5 <= lease_seconds <= 86400:
            raise ValueError("invalid refresh lease duration")
        if self.stopping:
            return None
        now = _utc(self.clock())
        target_demand_id = None
        # Indexed candidate hints only. Unsupported routes are filtered before LIMIT.
        async with self.repository.unit_of_work() as uow:
            if not supported(uow):
                raise DerivedError("refresh_backend_unsupported")
            discovery_scope = (
                self._processor(processor_key).scope
                if target_id is not None else next(iter(self.processors.values())).scope
            )
            now = await observed_clock(uow, discovery_scope, self.clock)
            if target_id is not None:
                # A bounded direct answer spends its step on its exact finite
                # demand. It still enters the same admission/fence path below.
                processor = self._processor(processor_key)
                receipt = await uow.derived_get(
                    processor.scope, "coverage_request", identity(target_id)
                )
                if (not receipt or receipt.get("state") == "erased"
                        or receipt.get("adapter_key") != processor.key):
                    raise DerivedError("derived_target_unavailable")
                row = await uow.derived_get(
                    processor.scope, "refresh_demand", receipt["demand_id"]
                )
                target_demand_id = receipt["demand_id"]
                # Expired unrelated reservations still consume the shared budget.
                # Reconcile them below, but never execute their work for this reader.
                expired = await uow.refresh_scheduler_expired(
                    now=now.isoformat(), adapter_keys=tuple(self.processors), limit=128,
                )
                due = [dict(partition_key=processor.scope.partition_key(),
                            identity=receipt["demand_id"], payload=row)] if row else []
            else:
                expired = await uow.refresh_scheduler_expired(
                    now=now.isoformat(),
                    adapter_keys=tuple(self.processors),
                    limit=128,
                )
                due = await uow.refresh_scheduler_due(
                    now=now.isoformat(),
                    adapter_keys=tuple(self.processors),
                    limit=128,
                )
        seen = set()
        for hint in (*expired, *due):
            if self.stopping:
                return None
            key = hint["identity"]
            if key in seen:
                continue
            seen.add(key)
            processor = self.processors.get(hint["payload"].get("adapter_key"))
            if processor is None or hint["partition_key"] != processor.scope.partition_key():
                continue
            async with self.repository.unit_of_work() as uow:
                epoch = await open_derived(uow, processor.scope)
                now = await observed_clock(uow, processor.scope, self.clock)
                await uow.refresh_scheduler_config(self.limits.payload())
                row = await uow.derived_get(processor.scope, "refresh_demand", key)
                if (
                    not row
                    or row.get("status") in {"erased", "dead", "superseded"}
                    or (row.get("epoch") != epoch or row.get("adapter_key") != processor.key)
                ):
                    continue
                if row.get("active_execution"):
                    if _parse(row.get("lease_until")) > now:
                        continue
                    await self._expire(uow, processor, row, now)
                    if row["status"] == "dead":
                        continue
                if target_demand_id is not None and key != target_demand_id:
                    continue
                if _parse(row.get("runnable_at") or row.get("due_at")) is None or (
                    _parse(row.get("runnable_at") or row["due_at"]) > now
                ):
                    continue
                policy = RefreshPolicy.from_payload(row["policy"])
                if self._exhausted(row, policy, now):
                    row.update(status="dead", reason="refresh_budget_exhausted", due_at=None)
                    await uow.derived_put(processor.scope, "refresh_demand", key, row)
                    continue
                try:
                    definition = await processor.definition(uow, row["facet_id"])
                    if row["compatibility"] != _compatibility(definition):
                        row.update(status="superseded", reason="refresh_compatibility_changed")
                        await uow.derived_put(processor.scope, "refresh_demand", key, row)
                        await record_dirty(
                            uow, processor.scope, definition, at=now, reason="compatibility_changed"
                        )
                        continue
                    boundary = definition.get("next_transition_at")
                    if boundary is not None and _parse(boundary) <= now:
                        definition.update(
                            time_generation=definition["time_generation"] + 1,
                            next_transition_at=None,
                            dirty=True,
                        )
                        await uow.derived_put(
                            processor.scope, "definition", row["facet_id"], definition
                        )
                        row = await record_dirty(
                            uow, processor.scope, definition, at=now, reason="time_boundary"
                        )
                    policy = RefreshPolicy.from_payload(row["policy"])
                    if row.get("scheduled_at") and _parse(row["scheduled_at"]) <= now:
                        row = await record_dirty(
                            uow, processor.scope, definition, at=now, reason="scheduled_tick"
                        )
                        row["scheduled_at"] = None
                        _due(row, policy, now, explicit=True)
                        await uow.derived_put(processor.scope, "refresh_demand", key, row)
                        await self._admit(uow, processor, row)
                    if row["status"] == "dirty" and not row.get("explicit"):
                        continue  # Cold boundary invalidates without expensive work.
                    if not row["requested"]:
                        continue
                    if self._exhausted(row, policy, now):
                        row.update(status="dead", reason="refresh_budget_exhausted", due_at=None)
                        await uow.derived_put(processor.scope, "refresh_demand", key, row)
                        continue
                    job = await processor.freeze(uow, definition)
                except DerivedError as error:
                    if error.code in DEFER_CODES:
                        if error.code in {
                            "derived_parent_unavailable",
                            "derived_parent_stale",
                            "derived_parent_proof_missing",
                        }:
                            try:
                                await self._demand_parents(uow, processor, row, now)
                            except DerivedError as parent_error:
                                if parent_error.code not in DEFER_CODES:
                                    raise
                                await self._defer(uow, processor, row, parent_error.code, now)
                                continue
                        await self._defer(uow, processor, row, error.code, now)
                        continue
                    row.update(status="dead", reason=error.code)
                    await uow.derived_put(processor.scope, "refresh_demand", key, row)
                    continue
                if job["status"] == "running" and _parse(job["lease_until"]) > now:
                    await self._defer(uow, processor, row, "refresh_resource_busy", now)
                    continue
                if job["status"] == "completed":
                    # Never retrofit responsibility onto a publication that did not
                    # claim it. Advance the ordinary time unit for a fresh census.
                    definition["time_generation"] += 1
                    await uow.derived_put(
                        processor.scope, "definition", row["facet_id"], definition
                    )
                    job = await processor.freeze(uow, definition)
                # A deferred candidate moves directly to running in this atomic
                # transaction. Requiring a pending slot here would let a busy
                # tenant refill that slot forever and defeat fair selection.
                # Running admission below remains the shared hard execution cap.
                claimed = sorted(row["requested"][:4096])
                execution_id = "refresh-execution:" + digest(
                    [
                        row["id"],
                        claimed,
                        job["unit"],
                        row.get("recovery_generation", 0),
                        policy.payload(),
                    ]
                )
                if not await uow.refresh_scheduler_reserve(
                    processor.scope,
                    execution_id,
                    instance_key=row["instance_key"],
                    units=1,
                    limits=self.limits.payload(),
                ):
                    await self._defer(uow, processor, row, "refresh_running_quota", now)
                    continue
                old = await uow.derived_get(processor.scope, "refresh_execution", execution_id)
                fence, generation = token_urlsafe(24), job.get("generation", 0) + 1
                expires_at = _parse(row["active_since"]) + timedelta(seconds=policy.max_age_seconds)
                lease_until = min(
                    now + timedelta(seconds=lease_seconds),
                    expires_at,
                    _parse(row["progress_at"]) + timedelta(seconds=policy.max_no_progress_seconds),
                ).isoformat()
                execution = old or dict(
                    schema="refresh-execution/1",
                    id=execution_id,
                    demand_id=row["id"],
                    facet_id=row["facet_id"],
                    adapter_key=processor.key,
                    epoch=epoch,
                    compatibility=row["compatibility"],
                    policy=policy.payload(),
                    claimed=claimed,
                    unit_id=job["id"],
                    unit=job["unit"],
                    created_at=now.isoformat(),
                    expires_at=expires_at.isoformat(),
                )
                execution.update(
                    status="running",
                    attempts=(old or {}).get("attempts", 0) + 1,
                    generation=generation,
                    fence=fence,
                    lease_until=lease_until,
                    worker_id=worker_id,
                    updated_at=now.isoformat(),
                )
                job.update(
                    status="running",
                    attempts=job["attempts"] + 1,
                    generation=generation,
                    fence=fence,
                    lease_until=lease_until,
                    refresh_execution=execution_id,
                    expires_at=execution["expires_at"],
                )
                row.update(
                    status="running",
                    active_execution=execution_id,
                    lease_until=lease_until,
                    unsealed=None,
                    runnable_at=None,
                )
                await uow.derived_put(processor.scope, "job", job["id"], job)
                await uow.derived_put(processor.scope, "refresh_execution", execution_id, execution)
                await uow.derived_put(processor.scope, "refresh_demand", key, row)
                await uow.refresh_scheduler_turn(processor.scope)
                task = WorkerTask(
                    job["id"],
                    job["id"],
                    processor.scope,
                    processor.task_type,
                    dict(
                        unit=job["unit"],
                        generation=generation,
                        fence=fence,
                        refresh_execution=execution_id,
                        adapter_key=processor.key,
                    ),
                    WorkerTaskStatus.LEASED,
                    execution["attempts"],
                    policy.max_attempts,
                    now,
                    _parse(execution["created_at"]),
                    now,
                    worker_id,
                    _parse(lease_until),
                )
                return WorkerLease(task, fence)
        return None

    _exhausted = staticmethod(_exhausted)

    async def _expire(self, uow, processor, row, now):
        execution_id = row["active_execution"]
        execution = await uow.derived_get(processor.scope, "refresh_execution", execution_id)
        if not execution:
            row.update(status="dead", reason="refresh_execution_missing")
        else:
            execution.update(status="retry", reason="refresh_lease_expired")
            await uow.derived_put(processor.scope, "refresh_execution", execution_id, execution)
            row.update(status="retry", runnable_at=now.isoformat())
            row["failed_attempts"] = row.get("failed_attempts", 0) + 1
        await uow.refresh_scheduler_release(processor.scope, execution_id)
        row.update(active_execution=None, lease_until=None)
        await uow.derived_put(processor.scope, "refresh_demand", row["id"], row)
        await self._admit(uow, processor, row)

    async def _defer(self, uow, processor, row, reason, now):
        policy = RefreshPolicy.from_payload(row["policy"])
        row.update(
            status="deferred",
            reason=reason,
            runnable_at=(now + timedelta(seconds=policy.retry_seconds)).isoformat(),
        )
        await uow.derived_put(processor.scope, "refresh_demand", row["id"], row)

    async def _demand_parents(self, uow, processor, row, now):
        definition = await processor.definition(uow, row["facet_id"])
        for parent_id in await processor.required_parents(uow, definition):
            parent = await processor.definition(uow, parent_id)
            config = await uow.derived_get(processor.scope, "refresh_policy", parent_id)
            if not config:
                for item in await uow.derived_records(processor.scope, "job"):
                    old = item["payload"]
                    if (
                        old.get("unit", {}).get("facet_id") == parent_id
                        and old.get("status") == "running"
                        and not old.get("refresh_execution")
                        and _parse(old["lease_until"]) > now
                    ):
                        raise DerivedError("refresh_legacy_lease_active")
                # Cold dependency demand has attribution and uses this same queue;
                # it never permanently changes the parent's on-demand policy.
                policy = RefreshPolicy.from_payload({**row["policy"], "mode": "on_demand"})
                await uow.derived_put(
                    processor.scope,
                    "refresh_policy",
                    parent_id,
                    dict(
                        schema="refresh-policy-binding/1",
                        facet_id=parent_id,
                        adapter_key=processor.key,
                        instance_key=processor.instance_id(parent),
                        policy=policy.payload(),
                        state="configured",
                        epoch=parent["epoch"],
                        configured_at=now.isoformat(),
                        limits=self.limits.payload(),
                    ),
                )
                parent.update(refresh_managed=True, dirty=True)
                await uow.derived_put(processor.scope, "definition", parent_id, parent)
            demand_id = "refresh-demand:" + digest(
                [processor.scope.partition_key(), parent_id, _compatibility(parent)]
            )
            demand = await uow.derived_get(processor.scope, "refresh_demand", demand_id)
            if not demand or not demand.get("requested"):
                demand = await record_dirty(
                    uow, processor.scope, parent, at=now, reason="cold_parent"
                )
            if demand is None:
                continue
            demand.update(explicit=True, temporary_parent_for=row["id"], unsealed=None)
            _due(demand, RefreshPolicy.from_payload(demand["policy"]), now, explicit=True)
            await uow.derived_put(processor.scope, "refresh_demand", demand["id"], demand)
            await self._admit(uow, processor, demand)

    def _task_processor(self, task):
        processor = self._processor(task.payload.get("adapter_key"))
        if processor.scope != task.scope or task.task_type != processor.task_type:
            raise stale()
        return processor

    async def checkpoint(self, lease, value):
        if value:
            raise DerivedError("derived_checkpoint_unsupported")
        processor = self._task_processor(lease.task)
        if lease.token != lease.task.payload.get("fence"):
            raise stale()
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, processor.scope)
            await processor.check_task(uow, lease.task)

    async def complete(self, lease):
        processor = self._task_processor(lease.task)
        if lease.token != lease.task.payload.get("fence"):
            raise stale()
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, processor.scope)
            await processor.check_task(uow, lease.task, completed=True)
            execution = await uow.derived_get(
                processor.scope, "refresh_execution", lease.task.payload["refresh_execution"]
            )
            if not execution or not await processor.verify_coverage(uow, execution):
                raise DerivedError("refresh_publication_unverified")

    @asynccontextmanager
    async def _clock_guard(self, processor):
        async def observe():
            async with self.repository.unit_of_work() as uow:
                await open_derived(uow, processor.scope)
                await observed_clock(uow, processor.scope, self.clock)

        await observe()
        try:
            yield
        except Exception:
            # Keep an observed expired fence expired after a failed lease UoW,
            # including across a new worker process with no in-memory clock state.
            await observe()
            raise

    async def heartbeat(self, lease, *, lease_seconds):
        if type(lease_seconds) is not int or not 5 <= lease_seconds <= 86400:
            raise ValueError("invalid refresh lease duration")
        processor = self._task_processor(lease.task)
        if lease.token != lease.task.payload.get("fence"):
            raise stale()
        now = _utc(self.clock())
        async with self._clock_guard(processor):
            async with self.repository.unit_of_work() as uow:
                await open_derived(uow, processor.scope)
                now = await observed_clock(uow, processor.scope, self.clock)
                job = await processor.check_task(uow, lease.task, completed=True)
                execution = await uow.derived_get(
                    processor.scope, "refresh_execution", lease.task.payload["refresh_execution"]
                )
                if job["status"] == "completed":
                    # A renewal can have waited behind the publication UoW.
                    # Committed work needs no further lease; do not reopen it or
                    # cancel the processor's successful return. Validate the
                    # exact fenced completion instead of trusting the status.
                    if not execution or not await processor.verify_coverage(uow, execution):
                        raise DerivedError("refresh_publication_unverified")
                    return
                row = await uow.derived_get(
                    processor.scope, "refresh_demand", execution["demand_id"]
                )
                policy = RefreshPolicy.from_payload(execution["policy"])
                if self._exhausted(row, policy, now):
                    raise DerivedError("refresh_budget_exhausted")
                until = min(
                    now + timedelta(seconds=lease_seconds),
                    _parse(execution["expires_at"]),
                    _parse(row["progress_at"]) + timedelta(seconds=policy.max_no_progress_seconds),
                )
                for kind, key, value in (
                    ("job", job["id"], job),
                    ("refresh_execution", execution["id"], execution),
                    ("refresh_demand", row["id"], row),
                ):
                    value["lease_until"] = until.isoformat()
                    await uow.derived_put(processor.scope, kind, key, value)
                # No progress timestamps change. Lease liveness is not useful work.

    async def fail(self, lease, error):
        import asyncio

        processor = self._task_processor(lease.task)
        if lease.token != lease.task.payload.get("fence"):
            raise stale()
        now = _utc(self.clock())
        async with self._clock_guard(processor):
            async with self.repository.unit_of_work() as uow:
                await open_derived(uow, processor.scope)
                now = await observed_clock(uow, processor.scope, self.clock)
                job = await processor.check_task(uow, lease.task)
                execution_id = lease.task.payload["refresh_execution"]
                execution = await uow.derived_get(
                    processor.scope, "refresh_execution", execution_id
                )
                row = await uow.derived_get(
                    processor.scope, "refresh_demand", execution["demand_id"]
                )
                code = getattr(error, "code", "refresh_processing_failed")
                deferred = code in DEFER_CODES or isinstance(error, asyncio.CancelledError)
                # Superseded snapshots retain finite responsibility for a current full
                # successor. A failed/partial attempt never manufactures coverage.
                superseded = code in {
                    "derived_snapshot_changed",
                    "derived_head_conflict",
                    "derived_input_changed",
                    "derived_source_changed",
                    "derived_time_coverage_expired",
                }
                if not deferred and not superseded:
                    row["failed_attempts"] = row.get("failed_attempts", 0) + 1
                policy = RefreshPolicy.from_payload(execution["policy"])
                dead = self._exhausted(row, policy, now)
                status = "dead" if dead else "deferred" if deferred else "retry"
                execution.update(status="superseded" if superseded else status, reason=code)
                job.update(status="superseded" if superseded else status, reason=code)
                row.update(
                    status=status,
                    reason=(
                        "refresh_cancelled" if isinstance(error, asyncio.CancelledError) else code
                    ),
                    active_execution=None,
                    lease_until=None,
                    runnable_at=(now + timedelta(seconds=policy.retry_seconds)).isoformat(),
                )
                await uow.derived_put(processor.scope, "job", job["id"], job)
                await uow.derived_put(processor.scope, "refresh_execution", execution_id, execution)
                await uow.derived_put(processor.scope, "refresh_demand", row["id"], row)
                await uow.refresh_scheduler_release(processor.scope, execution_id)
                await self._admit(uow, processor, row)
                return WorkerFailureDisposition.DEFERRED if deferred else None

    async def apply(self, task, checkpoint=None):
        processor = self._task_processor(task)
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, processor.scope)
            await observed_clock(uow, processor.scope, self.clock)
        snapshot = await processor.snapshot(task)
        return await processor.publish(task, snapshot, processor.prepare(snapshot))
