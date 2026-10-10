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
from ..serialization import to_jsonable
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
        self.configuration_sha256 = processing_configuration_sha256(
            pipeline, policy, source_authority
        )
        self.receiver = DurableReceiver(repository, clock=clock)
        self.queue = ExtractionQueue(repository, scope, self.configuration_sha256, clock=clock)
        handler = DurableAtomHandler(
            self.queue, pipeline, policy, source_authority, local_only=True
        )
        self.worker = BoundedWorker(
            self.queue, {"memory.extract": handler}, worker_id=worker_id, limits=self.limits
        )
        self.refresh = (
            RefreshHost(questions.queue, worker_id=worker_id + ":refresh") if questions else None
        )
        self._stop = asyncio.Event()
        self._initialized = False
        self._last_error = None

    async def initialize(self):
        if not self._initialized:
            await self.repository.initialize()
            await self.queue.initialize()
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
        if self.verification is None:
            return 0
        rows = await self.repository.admission_records(self.scope)
        if len(rows) > 128:
            raise ValueError("host verification discovery capacity exceeded")
        count = 0
        for row in rows:
            if row["scope"] != to_jsonable(self.scope) or row["payload"]["action"] not in {
                "PENDING_VERIFICATION",
                "CONTESTED",
            }:
                continue
            if row["payload"].get("qualification") or row["payload"].get(
                "project_candidate", {}
            ).get("review"):
                continue
            draft = row["payload"]["draft"]
            matches = [
                spec
                for spec in self.verification.specs.values()
                if draft["subject_id"] in spec.subjects and draft["predicate"] in spec.predicates
            ]
            if len(matches) != 1:
                continue
            spec = matches[0]
            await self.verification.schedule(
                row["id"],
                row["version"],
                tool_id=spec.id,
                request_id=f"auto:{row['id']}:{row['version']}:{spec.fingerprint}",
            )
            count += 1
        return count

    async def run_once(self):
        if self._stop.is_set():
            return {"state": "stopped"}
        await self.initialize()
        extraction = await self.worker.run_batch(max_tasks=1)
        scheduled = await self._schedule_verification()
        verification = (
            await self.verification.run_once(
                self.worker_id + ":verify", lease_seconds=max(30, self.verification.timeout + 10)
            )
            if self.verification
            else "disabled"
        )
        refresh = await self.refresh.run_once() if self.refresh else None
        return {
            "extraction": asdict(extraction),
            "verification_scheduled": scheduled,
            "verification": verification,
            "refresh": asdict(refresh) if refresh else None,
        }

    async def metrics(self):
        """Aggregate scope-local lifecycle state; no source IDs, quotes or model bodies."""
        async with self.repository.unit_of_work() as uow:
            requests = await uow.retention_active(self.scope)
            verification = await uow.derived_records(self.scope, "domain_verification_task")
            refresh = await uow.derived_records(self.scope, "refresh_demand")
        return {
            "schema": "memory-host-metrics/1",
            "processing": dict(Counter(r["status"] for r in requests)),
            "verification": dict(
                Counter(r["payload"].get("state", "unknown") for r in verification)
            ),
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
            self.refresh = RefreshHost(self.questions.queue, worker_id=self.worker_id + ":refresh")
        try:
            while not self._stop.is_set():
                try:
                    await self.run_once()
                    self._last_error = None
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
