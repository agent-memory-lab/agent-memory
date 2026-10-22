"""Policy observations on the existing feedback ledger, never a second decision store.

The sink is host-only. It records bounded codes, hashes and measured integer units,
not source text, prompts or generated answers. Native memory IDs retain the existing
feedback erasure edges. A receipt reports a decision, not authority or factual truth.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime

from ..derived.model import DerivedError, digest, timestamp
from ..domain import (
    DecisionRecord,
    FeedbackStatus,
    MemoryScope,
    MemoryUsage,
    PolicyDecisionDetails,
    utc_now,
)


@dataclass(frozen=True, slots=True)
class PolicyDecisionRecord:
    id: str
    scope: MemoryScope
    stage: str
    action: str
    reasons: tuple[str, ...]
    policy_version: str
    input_versions: tuple[str, ...]
    evidence_references: tuple[str, ...] = ()
    cost_references: tuple[str, ...] = ()
    outcome: str = "proposed"
    model_version: str | None = None
    memory_ids: tuple[str, ...] = ()
    measurements: tuple[tuple[str, int | None], ...] = ()
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self):
        if type(self.scope) is not MemoryScope:
            raise ValueError("host memory scope required")
        for name in ("id", "action", "policy_version"):
            value = getattr(self, name)
            if (
                type(value) is not str
                or not 1 <= len(value) <= 128
                or any(not c.isascii() or not (c.isalnum() or c in "_.:/-") for c in value)
            ):
                raise ValueError("bounded policy identifiers required")
        timestamp(self.created_at)
        for name in (
            "reasons",
            "input_versions",
            "evidence_references",
            "cost_references",
            "measurements",
            "memory_ids",
        ):
            object.__setattr__(self, name, tuple(getattr(self, name)))
        if (
            len(self.memory_ids) > 128
            or len(set(self.memory_ids)) != len(self.memory_ids)
            or any(
                type(value) is not str or not 1 <= len(value) <= 128 for value in self.memory_ids
            )
        ):
            raise ValueError("bounded unique native memory IDs required")
        # Validate and freeze nested sequences through the shared domain contract.
        details = self.details
        object.__setattr__(self, "measurements", details.measurements)

    @property
    def details(self):
        return PolicyDecisionDetails(
            self.stage,
            self.reasons,
            self.input_versions,
            self.evidence_references,
            self.cost_references,
            self.outcome,
            self.model_version,
            self.measurements,
        )

    @property
    def fingerprint(self):
        return digest(
            {
                "scope": self.scope.partition_key(),
                "id": self.id,
                "action": self.action,
                "policy_version": self.policy_version,
                "memory_ids": self.memory_ids,
                "details": asdict(self.details),
            }
        )

    def to_decision(self):
        return DecisionRecord(
            self.scope,
            self.action,
            self.memory_ids,
            id=self.id,
            policy_version=self.policy_version,
            context_hash=self.fingerprint,
            memory_usage=MemoryUsage.UNKNOWN if self.memory_ids else MemoryUsage.NONE,
            idempotency_key=f"policy:{self.id}",
            created_at=self.created_at,
            policy_details=self.details,
        )

    @classmethod
    def from_payload(cls, scope, payload):
        details = PolicyDecisionDetails(**payload["policy_details"])
        values = asdict(details)
        values.pop("schema")
        return cls(
            id=payload["id"],
            scope=scope,
            action=payload["action"],
            policy_version=payload["policy_version"],
            memory_ids=tuple(payload["memory_ids"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
            **values,
        )


class DecisionJournal:
    """Scoped adapter over save_decision/find_feedback_record; supports atomic callers."""

    def __init__(self, repository, scope):
        if type(scope) is not MemoryScope:
            raise ValueError("host memory scope required")
        self.repository, self.scope = repository, scope

    async def append(self, record):
        async with self.repository.unit_of_work() as uow:
            return await self.append_in(uow, record)

    async def get(self, identity):
        async with self.repository.unit_of_work() as uow:
            return await self.get_in(uow, identity)

    async def get_in(self, uow, identity):
        await uow.lock_admission_scope(self.scope)
        row = await uow.find_feedback_record(identity, "decision")
        if row is None:
            return None
        if row["partition_key"] != self.scope.partition_key():
            raise DerivedError("policy_decision_scope_mismatch")
        if row.get("invalidated_at") or row["feedback_status"] != FeedbackStatus.ACCEPTED:
            raise DerivedError("policy_decision_unavailable")
        payload = row["payload"]
        if not payload.get("policy_details") or payload.get("redacted"):
            raise DerivedError("policy_decision_unavailable")
        record = PolicyDecisionRecord.from_payload(self.scope, payload)
        if payload.get("context_hash") != record.fingerprint:
            raise DerivedError("policy_decision_payload_changed")
        await self._validate_references(uow, record)
        return record

    async def _validate_references(self, uow, record):
        if not record.memory_ids:
            return
        validate = getattr(uow, "policy_references_available", None)
        if not callable(validate) or not await validate(self.scope, record.memory_ids):
            raise DerivedError("policy_decision_sources_unavailable")

    async def append_in(self, uow, record):
        if type(record) is not PolicyDecisionRecord or record.scope != self.scope:
            raise DerivedError("policy_decision_scope_mismatch")
        await uow.lock_admission_scope(self.scope)
        old = await self.get_in(uow, record.id)
        if old is not None:
            if old.fingerprint != record.fingerprint:
                raise DerivedError("policy_decision_idempotency_conflict")
            return old
        # Lock the existing feedback identity on PostgreSQL as well as SQLite.
        duplicate = await uow.find_feedback_by_idempotency(
            self.scope, "decision", f"policy:{record.id}"
        )
        if duplicate is not None:
            raise DerivedError("policy_decision_idempotency_conflict")
        await self._validate_references(uow, record)
        await uow.save_decision(record.to_decision())
        return record


def business_decision_record(
    decision, *, id, scope, input_versions, memory_ids=(), evidence_references=(), created_at=None
):
    """Adapt BusinessAdmissionPolicy.assess without changing its admission semantics."""
    from ..consolidation.business_policy import BusinessDecision

    if type(decision) is not BusinessDecision:
        raise TypeError("BusinessDecision required")
    return PolicyDecisionRecord(
        id=id,
        scope=scope,
        stage="storage",
        action=decision.action,
        reasons=tuple(
            dict.fromkeys(
                (
                    *decision.reasons,
                    f"storage.{decision.storage}",
                    f"verification.{decision.verification}",
                )
            )
        ),
        policy_version=decision.policy_sha256,
        input_versions=input_versions,
        evidence_references=evidence_references,
        memory_ids=memory_ids,
        outcome="proposed",
        created_at=created_at or utc_now(),
    )


def boolean_decision_record(
    passed,
    *,
    id,
    scope,
    stage,
    policy_version,
    input_versions,
    passed_reason,
    rejected_reason,
    memory_ids=(),
    created_at=None,
):
    """Adapt a legacy boolean policy without inventing model confidence or verification."""
    if type(passed) is not bool:
        raise TypeError("boolean policy result required")
    return PolicyDecisionRecord(
        id=id,
        scope=scope,
        stage=stage,
        action="allow" if passed else "deny",
        reasons=(passed_reason if passed else rejected_reason,),
        policy_version=policy_version,
        input_versions=input_versions,
        memory_ids=memory_ids,
        outcome="proposed",
        created_at=created_at or utc_now(),
    )
