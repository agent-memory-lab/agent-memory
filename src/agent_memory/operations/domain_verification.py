"""Durable host-owned domain verification with fenced atomic publication.

Tools supply authenticated evidence, not model confidence. Unknown results never
reject a fact; refutation is distinct. Source erasure scrubs the task via the
shared derived erasure path. A publisher executes in the lease transaction.
"""

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from secrets import token_urlsafe

from ..consolidation.admission import draft_from_payload
from ..consolidation.admission_runtime import AdmissionEngine
from ..domain import MemoryEvent, SourceAuthority, utc_now
from ..retrieval.model_contracts import ModelError, digest
from .refresh_demand import observed_clock
from .source_revisions import source_is_current

KIND = "domain_verification_task"


@dataclass(frozen=True, slots=True)
class VerificationToolSpec:
    id: str
    version: str
    authority: SourceAuthority
    subjects: tuple[str, ...]
    predicates: tuple[str, ...]

    def __post_init__(self):
        for value in (self.id, self.version, *self.subjects, *self.predicates):
            if type(value) is not str or not 1 <= len(value) <= 256:
                raise ValueError("bounded verification identities required")
        if (
            type(self.authority) is not SourceAuthority
            or self.authority.kind not in {"document", "tool_observation"}
            or not self.subjects
            or not self.predicates
        ):
            raise ValueError("registered authoritative tool required")

    @property
    def fingerprint(self):
        from ..serialization import to_jsonable

        return digest(to_jsonable(self))


@dataclass(frozen=True, slots=True)
class VerificationFinding:
    disposition: str
    event: MemoryEvent | None = None
    quote: str | None = None
    support_from: object = None
    support_to: object = None
    supported_fields: tuple[str, ...] = ()
    conditions: tuple = ()
    exceptions: tuple = ()

    def __post_init__(self):
        from ..conditions import Condition
        from ..fact_qualification import EVIDENCE_FIELDS
        from ..lifecycle import is_memory_context

        if (
            len(self.conditions) > 16
            or len(self.exceptions) > 16
            or any(type(c) is not Condition for c in (*self.conditions, *self.exceptions))
            or len(set(self.supported_fields)) != len(self.supported_fields)
            or not set(self.supported_fields) <= EVIDENCE_FIELDS
        ):
            raise ValueError("invalid typed verification fields or conditions")
        if self.event is not None and (
            is_memory_context(self.event)
            or self.event.metadata.get("lifecycle", {}).get("origin") == "model"
        ):
            raise ValueError("model or memory echo is not verification evidence")
        if self.support_from is not None:
            from ..evidence_support import SupportRange

            SupportRange(self.support_from, self.support_to)
        elif self.support_to is not None:
            raise ValueError("support end requires start")
        if self.disposition not in {"supported", "refuted", "unknown"}:
            raise ValueError("invalid verification disposition")
        if self.disposition == "unknown":
            if self.event is not None:
                raise ValueError("unknown is not authenticated supporting evidence")
        elif (
            type(self.event) is not MemoryEvent
            or type(self.quote) is not str
            or not self.quote
            or self.quote not in self.event.content
        ):
            raise ValueError("exact authoritative evidence required")


class DomainVerificationQueue:
    def __init__(
        self,
        repository,
        scope,
        tools,
        *,
        authorize,
        publisher,
        clock=utc_now,
        max_attempts=3,
        capacity=128,
        timeout_seconds=20,
    ):
        tools = tuple(tools)
        if not 1 <= len(tools) <= 32 or not callable(authorize) or not callable(publisher):
            raise ValueError("bounded registered tools, authorization and publisher required")
        if len({t.spec.id for t in tools}) != len(tools):
            raise ValueError("duplicate verification tool")
        if any(type(t.spec) is not VerificationToolSpec or not callable(t.verify) for t in tools):
            raise ValueError("invalid verification tool")
        if (
            not 1 <= max_attempts <= 10
            or not 1 <= capacity <= 4096
            or not 1 <= timeout_seconds <= 300
        ):
            raise ValueError("invalid verification limits")
        self.repository, self.scope, self.clock = repository, scope, clock
        self.tools = {t.spec.id: t for t in tools}
        self.specs = {t.spec.id: t.spec for t in tools}
        self.authorize, self.publisher = authorize, publisher
        self.max_attempts, self.capacity, self.timeout = max_attempts, capacity, timeout_seconds

    async def _guard(self, uow, task):
        await observed_clock(uow, self.scope, self.clock)
        if await self.authorize(uow, self.scope, task["tool_id"]) is not True:
            raise ModelError("verification_authority_unavailable")
        tool = self.tools.get(task["tool_id"])
        if (
            tool is None
            or tool.spec != self.specs[task["tool_id"]]
            or tool.spec.fingerprint != task["tool_sha256"]
        ):
            raise ModelError("verification_tool_changed")
        if task["epoch"] != await uow.retention_epoch(self.scope):
            raise ModelError("verification_erased")
        row = await uow.get_admission_record(self.scope, task["candidate_id"])
        if (
            not row
            or row["payload"].get("deleted")
            or row["version"] != task["candidate_version"]
            or digest(row["payload"]["draft"]) != task["target_sha256"]
        ):
            raise ModelError("verification_candidate_changed")
        if row["payload"]["action"] not in {"PENDING_VERIFICATION", "CONTESTED"}:
            raise ModelError("verification_candidate_already_decided")
        for source_id in task["sources"]:
            source = await uow.get_source_event(self.scope, source_id)
            if source is None or (
                "_retention" in source.metadata and not await source_is_current(uow, source)
            ):
                raise ModelError("verification_source_unavailable")
        return row

    async def schedule(self, candidate_id, expected_version, *, tool_id, request_id):
        if (
            type(expected_version) is not int
            or expected_version < 1
            or any(type(v) is not str or not 1 <= len(v) <= 512 for v in (candidate_id, request_id))
        ):
            raise ValueError("bounded candidate/version/request required")
        if tool_id not in self.tools:
            raise ValueError("unregistered verification tool")
        spec = self.specs[tool_id]
        identity = "verification:" + digest([self.scope.partition_key(), request_id])
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            if await self.authorize(uow, self.scope, tool_id) is not True:
                raise ModelError("verification_authority_unavailable")
            candidate = await uow.get_admission_record(self.scope, candidate_id)
            if not candidate or candidate["version"] != expected_version:
                raise ModelError("verification_candidate_changed")
            draft = draft_from_payload(candidate["payload"]["draft"])
            if draft.subject_id not in spec.subjects or draft.predicate not in spec.predicates:
                raise ModelError("verification_tool_outside_domain")
            task = dict(
                schema="domain-verification-task/1",
                candidate_id=candidate_id,
                candidate_version=expected_version,
                target_sha256=digest(candidate["payload"]["draft"]),
                tool_id=tool_id,
                tool_sha256=spec.fingerprint,
                epoch=await uow.retention_epoch(self.scope),
                sources=sorted(AdmissionEngine.source_dependencies(candidate["payload"])),
                parents=["atom:" + candidate_id],
                state="pending",
                attempts=0,
                due_at=self.clock().isoformat(),
            )
            await self._guard(uow, task)
            previous = await uow.derived_get(self.scope, KIND, identity)
            if previous:
                if any(
                    previous.get(k) != task[k]
                    for k in ("candidate_id", "candidate_version", "tool_sha256", "epoch")
                ):
                    raise ModelError("verification_request_conflict")
                return identity
            if len(await uow.derived_records(self.scope, KIND)) >= self.capacity:
                raise ModelError("verification_capacity")
            await uow.derived_put(self.scope, KIND, identity, task)
            return identity

    async def claim(self, worker_id, *, lease_seconds=30):
        if type(worker_id) is not str or not 1 <= len(worker_id) <= 256:
            raise ValueError("bounded worker identity required")
        if type(lease_seconds) is not int or not self.timeout < lease_seconds <= 600:
            raise ValueError("verification lease must outlive bounded tool timeout")
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            await observed_clock(uow, self.scope, self.clock)
            now = self.clock()
            rows = await uow.derived_records(self.scope, KIND)
            for item in sorted(rows, key=lambda r: r["identity"]):
                task = item["payload"]
                if task.get("state") not in {"pending", "retry", "running"}:
                    continue
                from datetime import datetime

                if datetime.fromisoformat(task.get("lease_until", task["due_at"])) > now:
                    continue
                try:
                    candidate = await self._guard(uow, task)
                except ModelError as error:
                    if error.code == "verification_authority_unavailable":
                        continue
                    await uow.derived_put(
                        self.scope,
                        KIND,
                        item["identity"],
                        {"state": "cancelled", "reason": error.code},
                    )
                    continue
                if task["attempts"] >= self.max_attempts:
                    task["state"] = "dead"
                    await uow.derived_put(self.scope, KIND, item["identity"], task)
                    continue
                task.update(
                    state="running",
                    token=token_urlsafe(24),
                    worker_id=worker_id,
                    lease_until=(now + timedelta(seconds=lease_seconds)).isoformat(),
                    attempts=task["attempts"] + 1,
                )
                await uow.derived_put(self.scope, KIND, item["identity"], task)
                return {
                    "id": item["identity"],
                    "task": deepcopy(task),
                    "candidate": deepcopy(candidate),
                }
        return None

    async def publish(self, lease, finding):
        if type(finding) is not VerificationFinding:
            raise ValueError("typed authoritative finding required")
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            task = await uow.derived_get(self.scope, KIND, lease["id"])
            from datetime import datetime

            if (
                not task
                or task.get("state") != "running"
                or task.get("token") != lease["task"]["token"]
                or datetime.fromisoformat(task["lease_until"]) <= self.clock()
            ):
                raise ModelError("verification_lease_fenced")
            candidate = await self._guard(uow, task)
            if finding.event is not None:
                origin = finding.event.metadata.get("lifecycle", {}).get("origin")
                expected = {"document": "host", "tool_observation": "tool"}[
                    self.specs[task["tool_id"]].authority.kind
                ]
                if origin is not None and origin != expected:
                    raise ModelError("verification_origin_authority_mismatch")
            if finding.event is not None and finding.event.scope != self.scope:
                raise ModelError("verification_evidence_scope_mismatch")
            if finding.event is not None and finding.event.occurred_at > self.clock():
                raise ModelError("verification_future_observation")
            if finding.disposition != "unknown":
                if finding.disposition == "supported" and not {
                    "subject_id",
                    "predicate",
                    "value",
                    "valid_from",
                } <= set(finding.supported_fields):
                    raise ModelError("verification_field_support_incomplete")
                await self.publisher(uow, candidate, finding, self.specs[task["tool_id"]])
            # The publisher may await: recheck live authority and lease at commit.
            await observed_clock(uow, self.scope, self.clock)
            tool = self.tools.get(task["tool_id"])
            if (
                datetime.fromisoformat(task["lease_until"]) <= self.clock()
                or task["epoch"] != await uow.retention_epoch(self.scope)
                or await self.authorize(uow, self.scope, task["tool_id"]) is not True
                or tool is None
                or tool.spec.fingerprint != task["tool_sha256"]
            ):
                raise ModelError("verification_commit_fenced")
            # Tool completion and actual fact publication share the transaction.
            if finding.event is not None:
                task["sources"] = sorted(set(task["sources"]) | {finding.event.id})
            task.update(state="completed", disposition=finding.disposition)
            task.pop("token", None)
            await uow.derived_put(self.scope, KIND, lease["id"], task)
            return finding.disposition

    async def run_once(self, worker_id, *, lease_seconds=30):
        lease = await self.claim(worker_id, lease_seconds=lease_seconds)
        if lease is None:
            return "idle"
        try:
            async with asyncio.timeout(self.timeout):
                result = await self.tools[lease["task"]["tool_id"]].verify(
                    deepcopy(lease["candidate"])
                )
            return await self.publish(lease, result)
        except Exception as error:
            async with self.repository.unit_of_work() as uow:
                await uow.lock_admission_scope(self.scope)
                task = await uow.derived_get(self.scope, KIND, lease["id"])
                if task and task.get("token") == lease["task"]["token"]:
                    task.update(
                        state="retry",
                        due_at=(self.clock() + timedelta(seconds=2)).isoformat(),
                        reason=error.code
                        if isinstance(error, ModelError)
                        else "verification_tool_failed",
                    )
                    task.pop("token", None)
                    task.pop("lease_until", None)
                    await uow.derived_put(self.scope, KIND, lease["id"], task)
            if isinstance(error, ModelError):
                raise
            raise ModelError("verification_tool_failed") from None


def admission_publisher(engine, policy):
    """Ordinary candidate publisher; conditional/project inputs require their native publisher."""

    async def publish(uow, candidate, finding, spec):
        await engine.resolve(
            engine_scope(candidate),
            candidate["id"],
            event=finding.event,
            authority=spec.authority,
            policy=policy,
            expected_version=candidate["version"],
            accept=finding.disposition == "supported",
            source_quote=finding.quote,
            support_from=finding.support_from,
            support_to=finding.support_to,
            _unit_of_work=uow,
        )

    return publish


def engine_scope(candidate):
    from ..domain import MemoryScope

    return MemoryScope(**candidate["scope"])


def project_publisher(admission, *, accept_evidence):
    """Native project field qualification, with host evidence grants in the same UoW.

    Conditions/exceptions require a specialized host publisher; this adapter only
    handles unqualified assertions. Unknown never becomes a failed semantic review.
    """
    from ..consolidation.qualification import target_fingerprint
    from ..evidence_support import EvidenceLink, FieldSupport, SupportRange
    from ..fact_qualification import SourceSpan

    if not callable(accept_evidence):
        raise ValueError("trusted evidence capture/grant callback required")

    async def publish(uow, candidate, finding, spec):
        draft = draft_from_payload(candidate["payload"]["draft"])
        if (
            not candidate["payload"].get("project_candidate")
            or draft.conditions
            or draft.exceptions
            or finding.support_from is None
        ):
            raise ModelError("verification_specialized_publisher_required")
        if await accept_evidence(uow, finding.event) is not True:
            raise ModelError("verification_evidence_not_authorized")
        source = await uow.get_source_event(admission.scope, finding.event.id)
        if source is None or source.content_hash != finding.event.content_hash:
            raise ModelError("verification_evidence_unavailable")
        if finding.disposition == "refuted":
            await admission.reject(
                candidate["id"],
                expected_version=candidate["version"],
                review_id="domain-refutation:" + digest([candidate["id"], spec.fingerprint]),
                reasons=("authoritative_refutation",),
                _unit_of_work=uow,
            )
            return
        start = source.content.find(finding.quote)
        span = SourceSpan(source.id, start, start + len(finding.quote), finding.quote)
        link = EvidenceLink(
            "domain:" + digest([spec.fingerprint, candidate["id"], source.id]),
            finding.supported_fields,
            target_fingerprint(draft),
            span,
            spec.authority,
            SupportRange(finding.support_from, finding.support_to),
        )
        await admission.qualify(
            candidate["id"],
            expected_version=candidate["version"],
            review_id="domain-review:" + digest([candidate["id"], spec.fingerprint]),
            applicability_id="domain:" + spec.id,
            links=(link,),
            field_support=tuple(FieldSupport(f, ((link.id,),)) for f in finding.supported_fields),
            _unit_of_work=uow,
        )

    return publish


def contextual_publisher(contextual, admission_policy, projection_policy, *, accept_evidence):
    """Typed host tools may qualify original conditions; no LLM permission/confidence."""
    from ..consolidation.qualification import target_fingerprint
    from ..evidence_support import EvidenceLink, FieldSupport, SupportRange
    from ..fact_qualification import SourceSpan

    if not callable(accept_evidence):
        raise ValueError("trusted evidence callback required")

    async def publish(uow, candidate, finding, spec):
        if finding.disposition != "supported" or finding.support_from is None:
            raise ModelError("verification_specialized_refutation_required")
        if await accept_evidence(uow, finding.event) is not True:
            raise ModelError("verification_evidence_not_authorized")
        source = await uow.get_source_event(contextual.scope, finding.event.id)
        if source is None or source.content_hash != finding.event.content_hash:
            raise ModelError("verification_evidence_unavailable")
        draft = draft_from_payload(candidate["payload"]["draft"])
        start = source.content.find(finding.quote)
        link = EvidenceLink(
            "domain:" + digest([spec.fingerprint, candidate["id"], source.id]),
            finding.supported_fields,
            target_fingerprint(draft),
            SourceSpan(source.id, start, start + len(finding.quote), finding.quote),
            spec.authority,
            SupportRange(finding.support_from, finding.support_to),
        )
        await contextual.qualify(
            candidate["id"],
            expected_version=candidate["version"],
            admission_policy=admission_policy,
            projection_policy=projection_policy,
            applicability_id="domain:" + spec.id,
            conditions=finding.conditions,
            exceptions=finding.exceptions,
            links=(link,),
            field_support=tuple(FieldSupport(f, ((link.id,),)) for f in finding.supported_fields),
            _unit_of_work=uow,
        )

    return publish
