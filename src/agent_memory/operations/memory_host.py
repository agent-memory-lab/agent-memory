"""Explicit host assembly for durable capture/extraction/verification/refresh.

Construction starts no process. The host supplies authenticated scope, source
policy, model grants, tools and optional registered QuestionService. Existing
queues, ledgers and deletion fences remain the durable owners.
"""

import asyncio
from collections import Counter
from dataclasses import asdict
from ipaddress import ip_address
from urllib.parse import urlsplit

from ..domain import utc_now
from ..retrieval.model_contracts import ModelError
from .extraction_worker import DurableAtomHandler, ExtractionQueue, processing_configuration_sha256
from .refresh_host import RefreshHost
from .retention import DurableReceiver
from .worker_runtime import BoundedWorker
from .worker_tasks import WorkerLimits


class MemoryHost:
    def __init__(
        self,
        repository,
        scope,
        pipeline,
        policy,
        source_authority,
        *,
        on_accept,
        verification=None,
        questions=None,
        project_bridge=None,
        evolutions=(),
        clock=utc_now,
        worker_id="memory-host",
        limits=None,
    ):
        if not callable(on_accept):
            raise ValueError("explicit trusted source-acceptance/grant callback required")
        self.repository, self.scope, self.clock = repository, scope, clock
        self.pipeline, self.policy, self.source_authority = pipeline, policy, source_authority
        self.on_accept, self.verification, self.questions = on_accept, verification, questions
        self.worker_id = worker_id
        self.limits = limits or WorkerLimits(
            max_concurrency=1, lease_seconds=120, task_timeout_seconds=90
        )
        if self.limits.lease_seconds <= self.limits.task_timeout_seconds:
            raise ValueError("host lease must exceed bounded task timeout")
        for adapter in (pipeline.generator, pipeline.reviewer):
            calls = getattr(adapter, "calls", None)
            if calls is not None:
                parsed = urlsplit(calls.port.configuration.endpoint)
                try:
                    local = (
                        parsed.hostname == "localhost" or ip_address(parsed.hostname).is_loopback
                    )
                except ValueError:
                    local = False
                if (
                    not local
                    or calls.service.repository is not repository
                    or calls.service.scope != scope
                ):
                    raise ValueError(
                        "host requires explicitly governed local exact-scope extraction"
                    )
        if verification is not None and (
            verification.repository is not repository or verification.scope != scope
        ):
            raise ValueError("verification host scope/repository mismatch")
        if questions is not None and (
            questions.repository is not repository or questions.scope != scope
        ):
            raise ValueError("question host scope/repository mismatch")
        if project_bridge is not None and (
            questions is None or project_bridge.admission is not questions.admission
        ):
            raise ValueError("project bridge requires the exact registered question admission")
        if project_bridge is not None:
            from ..consolidation.business_policy import BusinessAdmissionPolicy

            if source_authority != project_bridge.admission._authority(
                project_bridge.source_authority_id
            ):
                raise ValueError("project bridge requires the registered capture authority")
            if isinstance(policy, BusinessAdmissionPolicy) and (
                verification is None
                or getattr(verification.publisher, "business_policy_sha256", None)
                != policy.fingerprint
            ):
                raise ValueError("project verification requires the bound business policy guard")
        self.project_bridge = project_bridge
        self.evolutions = tuple(evolutions)
        if len(self.evolutions) > 16:
            raise ValueError("bounded evolution inventory required")
        refresh_queue = questions.queue if questions else None
        for evolution in self.evolutions:
            if (
                evolution.repository is not repository
                or evolution.scope != scope
                or not callable(getattr(evolution, "discover_changes", None))
            ):
                raise ValueError("evolution host scope/repository mismatch")
            if refresh_queue is None:
                refresh_queue = evolution.queue
            if evolution.queue is not refresh_queue:
                raise ValueError("evolution requires the shared refresh scheduler")
        self.configuration_sha256 = processing_configuration_sha256(
            pipeline, policy, source_authority, project_bridge=project_bridge
        )
        self.receiver = DurableReceiver(repository, clock=clock)
        self.queue = ExtractionQueue(repository, scope, self.configuration_sha256, clock=clock)
        handler = DurableAtomHandler(
            self.queue,
            pipeline,
            policy,
            source_authority,
            local_only=True,
            project_bridge=project_bridge,
        )
        self.worker = BoundedWorker(
            self.queue, {"memory.extract": handler}, worker_id=worker_id, limits=self.limits
        )
        self.refresh = (
            RefreshHost(refresh_queue, worker_id=worker_id + ":refresh") if refresh_queue else None
        )
        self._stop = asyncio.Event()
        self._initialized = False
        self._extraction_initialized = False
        self._last_error = None
        self._stage_errors = {}
        self._verification_backpressured = False

    async def initialize(self):
        if not self._initialized:
            await self.repository.initialize()
            self._initialized = True
        return self

    async def submit(self, event, *, request_id, producer_id):
        if event.scope != self.scope:
            raise ValueError("host capture scope mismatch")
        await self.initialize()
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            ticket = await self.receiver.issue_ticket(
                event,
                request_id=request_id,
                producer_id=producer_id,
                configuration_sha256=self.configuration_sha256,
                _unit_of_work=uow,
            )
            receipt = await self.receiver.submit(
                event,
                ticket=ticket,
                producer_id=producer_id,
                configuration_sha256=self.configuration_sha256,
                _unit_of_work=uow,
            )
            if not receipt.duplicate and await self.on_accept(uow, event) is not True:
                raise PermissionError("host rejected source acceptance")
        return receipt

    async def submit_project(self, event, drafts, *, request_id, membership_ids=None):
        """Trusted typed project staging; source/membership authority stays host-owned.

        These inputs always remain pending until domain field qualification. This
        is not an API for a model to choose a project, authority or membership.
        """
        if self.questions is None or event.scope != self.scope:
            raise ValueError("configured exact-scope project host required")
        await self.initialize()
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            receipt = await self.questions.admission.stage_source(
                event,
                drafts,
                request_id=request_id,
                source_authority_id=self.source_authority.source_id,
                membership_ids=membership_ids,
                _unit_of_work=uow,
            )
            if not receipt.duplicate and await self.on_accept(uow, event) is not True:
                raise PermissionError("host rejected project source acceptance")
        return receipt

    async def preview(self, event_id):
        """Model preview may incur audited calls; it does not publish candidates."""
        await self.initialize()
        async with self.repository.unit_of_work() as uow:
            source = await uow.get_source_event(self.scope, event_id)
        if source is None:
            raise ValueError("preview source unavailable")
        return await self.pipeline.prepare(
            source, authority=self.source_authority, policy=self.policy
        )

    async def _schedule_verification(self):
        self._verification_backpressured = False
        if self.verification is None:
            return 0
        # One keyset page per cycle. Terminal/unrelated/qualified records are
        # excluded by the database index before the LIMIT, never by a full scan.
        async with self.repository.unit_of_work() as uow:
            cursor = await uow.derived_get(self.scope, "verification_discovery", "scope") or {}
            after = cursor.get("after")
            rows = await uow.verification_candidates(self.scope, after=after)
            if not rows and after is not None:
                rows = await uow.verification_candidates(self.scope)
                after = None
        count = 0
        for row in rows:
            try:
                if (
                    self.project_bridge is not None
                    and not self.project_bridge.verification_eligible(row)
                ):
                    after = row["id"]
                    continue
                draft = row["payload"]["draft"]
                matches = [
                    spec
                    for spec in self.verification.specs.values()
                    if draft["subject_id"] in spec.subjects
                    and draft["predicate"] in spec.predicates
                ]
                if len(matches) == 1:
                    spec = matches[0]
                    await self.verification.schedule(
                        row["id"],
                        row["version"],
                        tool_id=spec.id,
                        request_id=f"auto:{row['id']}:{row['version']}:{spec.fingerprint}",
                    )
                    count += 1
            except Exception as error:
                if isinstance(error, ModelError) and error.code == "verification_capacity":
                    # Leave this row for the next cycle; claim/refresh still run.
                    self._verification_backpressured = True
                    break
                # A malformed/stale/unauthorized candidate cannot starve later
                # candidates. Revisit it on the next sweep, without exposing input.
                self._stage_errors["verification_discovery"] = (
                    "memory_host_verification_discovery_failed"
                )
            after = row["id"]
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            # A source may be erased while scheduling awaits. Do not restore
            # its candidate identity into a cursor after the erasure commits.
            if after and await uow.get_admission_record(self.scope, after) is None:
                after = None
            await uow.derived_put(
                self.scope,
                "verification_discovery",
                "scope",
                {"after": after, "parents": ["atom:" + after] if after else []},
            )
        return count

    async def run_once(self):
        if self._stop.is_set():
            return {"state": "stopped"}
        await self.initialize()
        self._stage_errors = {}

        async def stage(name, call, fallback=None):
            try:
                return await call()
            except Exception:
                # Detailed provider exceptions may repeat private source bodies.
                self._stage_errors[name] = f"memory_host_{name}_failed"
                return fallback

        async def extract():
            if not self._extraction_initialized:
                await self.queue.initialize()
                self._extraction_initialized = True
            return await self.worker.run_batch(max_tasks=1)

        extraction = await stage("extraction", extract)
        scheduled = await stage("verification_discovery", self._schedule_verification, 0)
        verification = (
            await stage(
                "verification",
                lambda: self.verification.run_once(
                    self.worker_id + ":verify",
                    lease_seconds=max(30, self.verification.timeout + 10),
                ),
                "failed",
            )
            if self.verification
            else "disabled"
        )

        async def discover_evolution():
            total = 0
            for evolution in self.evolutions:
                if (
                    evolution.repository is not self.repository
                    or evolution.scope != self.scope
                    or evolution.queue is not self.refresh.queue
                ):
                    raise ValueError("evolution registration changed")
                total += len(await evolution.discover_changes())
            return total

        evolved = await stage("evolution", discover_evolution, 0) if self.evolutions else 0
        refresh = await stage("refresh", self.refresh.run_once) if self.refresh else None
        self._last_error = "memory_host_stage_failed" if self._stage_errors else None
        return {
            "extraction": asdict(extraction) if extraction is not None else None,
            "verification_scheduled": scheduled,
            "verification_backpressured": self._verification_backpressured,
            "verification": verification,
            **({"evolution_discovered": evolved} if self.evolutions else {}),
            "refresh": asdict(refresh) if refresh else None,
            "stage_errors": dict(self._stage_errors),
        }

    async def metrics(self):
        """Aggregate scope-local lifecycle state; no source IDs, quotes or model bodies."""
        await self.initialize()
        backlog = await self.verification.backlog() if self.verification else None
        async with self.repository.unit_of_work() as uow:
            requests = await uow.retention_active(self.scope)
            refresh = await uow.derived_records(self.scope, "refresh_demand")
        return {
            "schema": "memory-host-metrics/2",
            "processing": dict(Counter(r["status"] for r in requests)),
            "verification": backlog["states"] if backlog else {},
            "verification_backlog": backlog,
            "verification_discovery_backpressured": self._verification_backpressured,
            "stage_errors": dict(self._stage_errors),
            "refresh_pending": sum(bool(r["payload"].get("requested")) for r in refresh),
            "stopped": self._stop.is_set(),
            "last_error_code": self._last_error,
            "model_cost": "use_authoritative_model_budget_ledger",
        }

    async def run(self, *, poll_seconds=1):
        if not 0.01 <= poll_seconds <= 60:
            raise ValueError("invalid host poll interval")
        self._stop.clear()
        if self.refresh:
            # Restart the owned refresh host, preserving its durable queue.
            self.refresh = RefreshHost(self.refresh.queue, worker_id=self.worker_id + ":refresh")
        try:
            while not self._stop.is_set():
                try:
                    await self.run_once()
                except Exception:
                    # Detailed provider exceptions may repeat private inputs.
                    self._last_error = "memory_host_cycle_failed"
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=poll_seconds)
                except TimeoutError:
                    pass
        finally:
            self.stop()

    def stop(self):
        """Stop new work; current bounded extraction/verification may finish."""
        self._stop.set()
        if self.refresh:
            self.refresh.stop()
