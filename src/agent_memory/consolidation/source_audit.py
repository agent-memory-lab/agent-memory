"""Opt-in, source-first omission checks; coverage observations are not recall proofs.

The trusted host selects small high-value ranges and supplies live source fences.
The auditor proposes candidates only. Candidate review and admission still own
publication. This contract has no model transport, global cache or scheduler.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any, Protocol

from ..domain import ExtractedAtom, MemoryEvent, SourceAuthority, canonical_json, utc_now
from ..serialization import to_jsonable
from .admission import authority_to_payload, draft_to_payload


def _text(value, name, limit=128):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError(f"invalid {name}")


def _digest(value):
    return sha256(canonical_json(value).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class SourceAuditTarget:
    """Host-selected exact character range and bounded question/predicate scope."""

    target_id: str
    question: str
    predicates: tuple[str, ...]
    source_start: int
    source_end: int
    trigger: str = "unaudited"

    def __post_init__(self):
        _text(self.target_id, "target identity")
        _text(self.question, "audit question", 512)
        if (
            not isinstance(self.predicates, (tuple, list))
            or not 1 <= len(self.predicates) <= 4
            or len(set(self.predicates)) != len(self.predicates)
        ):
            raise ValueError("audit requires one to four distinct predicates")
        for predicate in self.predicates:
            _text(predicate, "target predicate")
        object.__setattr__(self, "predicates", tuple(self.predicates))
        if (
            type(self.source_start) is not int
            or type(self.source_end) is not int
            or not 0 <= self.source_start < self.source_end
            or self.source_end - self.source_start > 8000
        ):
            raise ValueError("audit target requires a nonempty bounded character span")
        if self.trigger not in {"changed", "unaudited", "high_value"}:
            raise ValueError("audit target requires a selective trigger")


@dataclass(frozen=True, slots=True)
class SourceAuditFence:
    """Trusted host's current authorization for the exact event passed to it.

    The host must reject missing, erased, superseded or inaccessible sources, and
    synchronize its final authorization check with the supplied admission UoW.
    Versions and time axes are stable snapshot identities, not model arguments.
    """

    source_version: str
    authorization_version: str
    erasure_epoch: int
    valid_at: datetime
    known_at: datetime
    expires_at: datetime

    def __post_init__(self):
        _text(self.source_version, "source version")
        _text(self.authorization_version, "authorization version")
        if type(self.erasure_epoch) is not int or self.erasure_epoch < 0:
            raise ValueError("invalid erasure epoch")
        for name in ("valid_at", "known_at", "expires_at"):
            value = getattr(self, name)
            if not isinstance(value, datetime) or value.utcoffset() is None:
                raise ValueError("audit fence times must include a timezone")
            object.__setattr__(self, name, value.astimezone(UTC))
        if self.expires_at <= self.known_at:
            raise ValueError("audit fence expiry must follow known_at")


@dataclass(frozen=True, slots=True)
class SourceAuditRange:
    target: SourceAuditTarget
    text: str


@dataclass(frozen=True, slots=True)
class SourceAuditRequest:
    source_event_id: str
    source_version: str
    valid_at: datetime
    known_at: datetime
    ranges: tuple[SourceAuditRange, ...]
    candidates: tuple[ExtractedAtom, ...]
    omitted_candidate_count: int = 0


@dataclass(frozen=True, slots=True)
class SourceAuditObservation:
    """One untrusted finding per requested target/predicate pair.

    no_additional_candidate means only that the auditor proposed none. It is not
    evidence of absence, source completeness, or a measured recall guarantee.
    """

    target_id: str
    predicate: str
    status: str
    reasons: tuple[str, ...]
    candidates: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self):
        _text(self.target_id, "observation target")
        _text(self.predicate, "observation predicate")
        if self.status not in {
            "no_additional_candidate",
            "missing_candidate",
            "conflict",
            "unresolved",
        }:
            raise ValueError("invalid source-audit finding")
        if not isinstance(self.reasons, (tuple, list)) or not 1 <= len(self.reasons) <= 8:
            raise ValueError("audit observation requires bounded reasons")
        for reason in self.reasons:
            _text(reason, "audit reason")
        if (
            not isinstance(self.candidates, (tuple, list))
            or len(self.candidates) > 4
            or any(not isinstance(value, Mapping) for value in self.candidates)
        ):
            raise ValueError("invalid audit proposals")
        if self.status in {"no_additional_candidate", "unresolved"} and self.candidates:
            raise ValueError("this observation cannot propose candidates")
        if self.status == "missing_candidate" and not self.candidates:
            raise ValueError("missing-candidate finding requires a proposal")
        object.__setattr__(self, "reasons", tuple(self.reasons))
        object.__setattr__(self, "candidates", tuple(deepcopy(self.candidates)))


class SourceAuditHost(Protocol):
    version: str

    def select_targets(self, event: MemoryEvent) -> Sequence[SourceAuditTarget]: ...

    async def authorize_source(
        self, event: MemoryEvent, authority: SourceAuthority, *, unit_of_work=None
    ) -> SourceAuditFence | None:
        """Authenticate exact event/version/time; None denies all use, including reuse."""
        ...


class RetainedSourceAuditHost:
    """Add repository-backed live revision/erasure checks to host authorization.

    Use this adapter for retained sources, including durable extraction workers.
    It uses the caller's admission transaction when supplied; it never opens a
    second connection inside that transaction. The wrapped host still owns auth.
    """

    def __init__(self, repository, host: SourceAuditHost):
        self.repository, self.host = repository, host
        _text(host.version, "wrapped host version", 96)

    @property
    def version(self):
        return "retained-audit/1:" + self.host.version

    def select_targets(self, event):
        return self.host.select_targets(event)

    async def authorize_source(self, event, authority, *, unit_of_work=None):
        context = (
            nullcontext(unit_of_work)
            if unit_of_work is not None
            else self.repository.unit_of_work()
        )
        async with context as uow:
            await uow.lock_admission_scope(event.scope)
            epoch = await uow.retention_epoch(event.scope)
            if not await _retained_current(uow, event, epoch):
                return None
            received = await uow.retention_get(
                event.scope, "request", event.metadata["_retention"]["request_id"]
            )
            fence = await self.host.authorize_source(event, authority, unit_of_work=uow)
            if (
                not isinstance(fence, SourceAuditFence)
                or fence.erasure_epoch != epoch
                or received is None
                or datetime.fromisoformat(received["received_at"]) > fence.known_at
            ):
                return None
            return fence


async def _retained_current(uow, event, epoch):
    from ..operations.source_revisions import source_is_current

    current = await uow.get_source_event(event.scope, event.id)
    return (
        current is not None
        and "_retention" in current.metadata
        and current.content == event.content
        and current.occurred_at == event.occurred_at
        and current.actor == event.actor
        and current.source_uri == event.source_uri
        and current.metadata == event.metadata
        and current.event_type == event.event_type
        and current.sensitivity == event.sensitivity
        and current.retention_class == event.retention_class
        and current.schema_version == event.schema_version
        and await source_is_current(uow, current)
        and epoch == await uow.retention_epoch(event.scope)
    )


class SourceAuditor(Protocol):
    version: str

    async def audit_source(
        self, request: SourceAuditRequest
    ) -> Sequence[SourceAuditObservation]: ...


class SourceAuditUnavailable(ValueError):
    """Generic fail-closed error; never includes supplier exceptions or source text."""


@dataclass(frozen=True, slots=True)
class SourceAuditApproval:
    """Host-verifiable controlled-real evidence, never self-issued by an auditor."""

    configuration_sha256: str
    evidence_sha256: str
    protocol_sha256: str
    rollback_id: str
    qualification: str = "controlled-real"
    schema: str = "source-audit-approval/1"

    def __post_init__(self):
        for name in ("configuration_sha256", "evidence_sha256", "protocol_sha256"):
            value = getattr(self, name)
            if (
                type(value) is not str
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError("source audit approval requires SHA-256 digests")
        _text(self.rollback_id, "audit rollback identity")
        if self.qualification != "controlled-real" or self.schema != "source-audit-approval/1":
            raise ValueError("source audit activation requires controlled-real qualification")


def audit_review_required(payload):
    """Persistent same-source slot hold; model withdrawal does not verify it.

    Only explicit host verification of the candidate can clear its hold. A
    historical initially-pending record remains relevant after it leaves the
    current interpretation head. Rejected/transient original audit proposals
    that never became pending do not create a new hold.
    """
    if payload.get("resolved_at") and payload.get("action") == "ACCEPT":
        return False
    if payload.get("source_audit_hold"):
        return True
    if payload.get("initial", payload).get("action") not in {
        "PENDING_VERIFICATION",
        "CONTESTED",
    }:
        return False
    return any(
        "source_audit_target" in report or "source_audit_conflict" in report.get("reasons", ())
        for report in payload.get("extraction", {}).get("reports", ())
    )


class SourceOmissionAudit:
    """Host-local opt-in adapter, with exact first-result/checkpoint reuse only.

    local_only is a host assertion, not transport isolation. External dispatch
    authorization, model budgets and real-model quality promotion are separate.
    """

    version = "source-omission-audit/1"

    def __init__(
        self,
        auditor: SourceAuditor,
        host: SourceAuditHost,
        *,
        local_only,
        enabled=False,
        contract_test_only=False,
        approval=None,
        verify_approval=None,
        clock=utc_now,
    ):
        if local_only is not True:
            raise ValueError("source audit requires explicitly host-local execution")
        if type(enabled) is not bool or type(contract_test_only) is not bool:
            raise TypeError("source audit activation flags must be boolean")
        for adapter in (auditor, host):
            _text(adapter.version, "audit adapter version")
        self.auditor, self.host, self.clock = auditor, host, clock
        self._auditor_identity, self._host_identity = auditor, host
        self._enabled, self._contract_test_only = enabled, contract_test_only
        self._approval, self._verify_approval = approval, verify_approval
        self._verifier_identity = verify_approval
        self.configuration_sha256 = _digest(self._configuration())
        self._configuration_identity = self.configuration_sha256
        self._approval_identity = _digest(asdict(approval)) if approval is not None else None
        self.activation_mode = (
            "disabled"
            if not enabled
            else "contract-test-only"
            if contract_test_only
            else "controlled-real"
        )
        self._activation_identity = (enabled, contract_test_only, self.activation_mode)
        if enabled:
            self.require_current_approval()

    def require_current_approval(self):
        """Synchronous live promotion/rollback check, repeated after every await."""

        def unchanged():
            return (
                self.auditor is self._auditor_identity
                and self.host is self._host_identity
                and self._verify_approval is self._verifier_identity
                and _digest(self._configuration()) == self._configuration_identity
                and self.configuration_sha256 == self._configuration_identity
                and (self._enabled, self._contract_test_only, self.activation_mode)
                == self._activation_identity
                and (_digest(asdict(self._approval)) if self._approval is not None else None)
                == self._approval_identity
            )

        if not unchanged():
            raise SourceAuditUnavailable("source_audit_activation_changed")
        if not self._enabled:
            raise SourceAuditUnavailable("source_audit_disabled")
        if self._contract_test_only:
            if self._approval is not None or self._verify_approval is not None:
                raise SourceAuditUnavailable("source_audit_contract_mode_has_approval")
            return
        if type(self._approval) is not SourceAuditApproval or not callable(self._verify_approval):
            raise SourceAuditUnavailable("source_audit_qualified_approval_required")
        if (
            self._approval.configuration_sha256 != self._configuration_identity
            or inspect.iscoroutinefunction(self._verify_approval)
        ):
            raise SourceAuditUnavailable("source_audit_approval_mismatched")
        try:
            self._approval.__post_init__()
            verified = self._verify_approval(self._approval)
            if inspect.iscoroutine(verified):
                verified.close()
            if verified is not True:
                raise SourceAuditUnavailable("source_audit_approval_revoked")
        except SourceAuditUnavailable:
            raise
        except Exception:
            raise SourceAuditUnavailable("source_audit_approval_unavailable") from None
        if not unchanged():
            raise SourceAuditUnavailable("source_audit_activation_changed")

    def config_payload(self):
        return {
            **self._configuration(),
            "configuration_sha256": self._configuration_identity,
            "activation_mode": self.activation_mode,
            "approval_sha256": self._approval_identity,
        }

    def _configuration(self):
        return {
            "schema": self.version,
            "auditor_version": self.auditor.version,
            "host_version": self.host.version,
            "execution": "host-local",
            "max_targets": 8,
            "max_predicates_per_target": 4,
            "max_characters_per_range": 8000,
            "max_source_characters": 16000,
            "max_proposals": 16,
            "max_proposals_per_observation": 4,
            "max_response_bytes": 48000,
            "maximum_stage_timeout_seconds": 30,
            "recovery_policy": "explicit-host-verification/1",
            "max_candidate_context_bytes": 32000,
        }

    def _input(self, event, authority, policy):
        return _digest(
            {
                "event_id": event.id,
                "scope": asdict(event.scope),
                "content_sha256": sha256(event.content.encode()).hexdigest(),
                "source_uri": event.source_uri,
                "actor": event.actor,
                "event_type": event.event_type,
                "metadata_sha256": _digest(event.metadata),
                "sensitivity": event.sensitivity,
                "retention_class": event.retention_class,
                "schema_version": event.schema_version,
                "occurred_at": event.occurred_at.astimezone(UTC).isoformat(),
                "authority": authority_to_payload(authority),
                "admission": policy.config_payload(),
            }
        )

    def check_expiry(self, payload):
        now = self.clock()
        if not isinstance(now, datetime) or now.utcoffset() is None:
            raise SourceAuditUnavailable("source_audit_clock_invalid")
        fence = payload["fence"]
        if (
            not datetime.fromisoformat(fence["known_at"])
            <= now
            < datetime.fromisoformat(fence["expires_at"])
        ):
            raise SourceAuditUnavailable("source_audit_fence_expired")

    async def _authorize(self, event, authority, *, unit_of_work=None):
        try:
            fence = await self.host.authorize_source(
                deepcopy(event), authority, unit_of_work=unit_of_work
            )
        except Exception:
            raise SourceAuditUnavailable("source_audit_authorization_failed") from None
        if not isinstance(fence, SourceAuditFence):
            raise SourceAuditUnavailable("source_audit_source_unavailable")
        return to_jsonable(fence)

    async def begin(self, event, authority, policy):
        self.require_current_approval()
        config = self.config_payload()
        fence = await self._authorize(event, authority)
        targets = self._targets(event, policy)
        payload = {
            "config": config,
            "input_sha256": self._input(event, authority, policy),
            "source_event_id": event.id,
            "source_content_sha256": sha256(event.content.encode()).hexdigest(),
            "fence": fence,
            "targets": [asdict(t) for t in targets],
            "scope_sha256": _digest([asdict(t) for t in targets]),
            "recall_proven": False,
            "world_negative_proof": False,
            "calls": 0,
            "observations": [],
        }
        await self.validate(event, authority, policy, payload)
        return payload

    def _targets(self, event, policy):
        targets = self.host.select_targets(deepcopy(event))
        if not isinstance(targets, Sequence) or isinstance(targets, (str, bytes)):
            raise ValueError("invalid source audit plan")
        registered = {spec["predicate"] for spec in policy.config_payload()["predicates"]}
        if (
            len(targets) > 8
            or any(not isinstance(t, SourceAuditTarget) for t in targets)
            or len({t.target_id for t in targets}) != len(targets)
            or sum(t.source_end - t.source_start for t in targets) > 16000
            or any(t.source_end > len(event.content) for t in targets)
            or any(not set(t.predicates) <= registered for t in targets)
        ):
            raise ValueError("invalid source audit plan")
        return tuple(targets)

    async def validate(self, event, authority, policy, payload, *, unit_of_work=None):
        self.require_current_approval()
        if (
            payload["config"] != self.config_payload()
            or payload["input_sha256"] != self._input(event, authority, policy)
            or payload["source_event_id"] != event.id
            or payload["source_content_sha256"] != sha256(event.content.encode()).hexdigest()
            or payload["scope_sha256"] != _digest(payload["targets"])
        ):
            raise SourceAuditUnavailable("source_audit_input_changed")
        if unit_of_work is not None and "_retention" in event.metadata:
            if not await _retained_current(unit_of_work, event, payload["fence"]["erasure_epoch"]):
                raise SourceAuditUnavailable("source_audit_source_unavailable")
        fence = await self._authorize(event, authority, unit_of_work=unit_of_work)
        if fence != payload["fence"]:
            raise SourceAuditUnavailable("source_audit_fence_changed")
        # No awaited work after the final host check. Its policy must synchronize
        # revocation with this transaction; wall-clock/config checks are local.
        self.check_expiry(payload)
        if payload["config"] != self.config_payload():
            raise SourceAuditUnavailable("source_audit_configuration_changed")
        if payload["scope_sha256"] != _digest([asdict(t) for t in self._targets(event, policy)]):
            raise SourceAuditUnavailable("source_audit_targets_changed")
        self.require_current_approval()
        self.check_expiry(payload)
        if payload["config"] != self.config_payload():
            raise SourceAuditUnavailable("source_audit_configuration_changed")

    async def run(self, event, authority, policy, payload, candidates, *, timeout_seconds):
        """Return bounded untrusted proposals; no findings are admitted here."""
        await self.validate(event, authority, policy, payload)
        targets = tuple(SourceAuditTarget(**value) for value in payload["targets"])
        expected = {(t.target_id, p) for t in targets for p in t.predicates}
        relevant, omitted, context_bytes = [], 0, 0
        for candidate in candidates:
            if not any(candidate.draft.predicate in t.predicates for t in targets):
                continue
            # Do not leak an unmatched/overlapping quote outside selected ranges.
            contained = any(
                candidate.draft.predicate in t.predicates
                and candidate.source_start is not None
                and t.source_start <= candidate.source_start < candidate.source_end <= t.source_end
                and event.content[candidate.source_start : candidate.source_end]
                == candidate.draft.source_quote
                for t in targets
            )
            size = len(canonical_json(draft_to_payload(candidate.draft)).encode())
            if not contained or context_bytes + size > 32000:
                omitted += 1
                continue
            relevant.append(candidate)
            context_bytes += size
        relevant = tuple(relevant)
        payload["omitted_candidate_count"] = omitted
        payload["candidate_sha256"] = _digest(
            [
                {"draft": draft_to_payload(c.draft), "start": c.source_start, "end": c.source_end}
                for c in relevant
            ]
        )
        if not targets:
            payload["processing_state"] = "not_selected"
            return (), ()
        request = SourceAuditRequest(
            event.id,
            payload["fence"]["source_version"],
            datetime.fromisoformat(payload["fence"]["valid_at"]),
            datetime.fromisoformat(payload["fence"]["known_at"]),
            tuple(
                SourceAuditRange(t, event.content[t.source_start : t.source_end]) for t in targets
            ),
            deepcopy(relevant),
            omitted,
        )
        self.require_current_approval()
        self.check_expiry(payload)
        payload["calls"] = 1
        failure = None
        try:
            async with asyncio.timeout(timeout_seconds):
                observations = await self.auditor.audit_source(request)
            if (
                not isinstance(observations, Sequence)
                or isinstance(observations, (str, bytes))
                or len(observations) != len(expected)
                or any(not isinstance(o, SourceAuditObservation) for o in observations)
                or {(o.target_id, o.predicate) for o in observations} != expected
                or sum(len(o.candidates) for o in observations) > 16
                or len(canonical_json(to_jsonable(observations)).encode()) > 48000
            ):
                raise ValueError("invalid audit response")
        except Exception as error:
            failure = (
                "source_audit_timeout" if isinstance(error, TimeoutError) else "source_audit_failed"
            )
            observations = tuple(
                SourceAuditObservation(target, predicate, "unresolved", (failure,))
                for target, predicate in sorted(expected)
            )
        # A late response cannot renew source permission or restore erased data.
        await self.validate(event, authority, policy, payload)
        proposals = []
        for observation in observations:
            report = {
                "target_id": observation.target_id,
                "predicate": observation.predicate,
                "status": observation.status,
                "finding_status": observation.status,
                "reasons": list(observation.reasons),
                "candidate_indexes": [],
            }
            payload["observations"].append(report)
            target = next(t for t in targets if t.target_id == observation.target_id)
            proposals.extend((deepcopy(raw), target, report) for raw in observation.candidates)
        payload["processing_state"] = "failed" if failure else "processed"
        return tuple(proposals), (failure,) if failure else ()
