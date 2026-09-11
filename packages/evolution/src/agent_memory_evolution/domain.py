from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from uuid import uuid4

from agent_memory.domain import MemoryScope, Procedure, utc_now


class EvolutionState(StrEnum):
    CANDIDATE = "candidate"
    EVALUATED = "evaluated"
    SHADOW = "shadow"
    CANARY = "canary"
    ACTIVATING = "activating"
    ACTIVE = "active"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"
    REJECTED = "rejected"
    INVALIDATED = "invalidated"


class EvaluationStage(StrEnum):
    OFFLINE = "offline"
    SHADOW = "shadow"
    CANARY = "canary"


@dataclass(frozen=True, slots=True)
class GeneratedProcedure:
    procedure: Procedure
    source_episode_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EvolutionCandidate:
    scope: MemoryScope
    procedure: Procedure
    source_episode_ids: tuple[str, ...]
    id: str = field(default_factory=lambda: str(uuid4()))
    state: EvolutionState = EvolutionState.CANDIDATE
    baseline_version: str | None = None
    generator: str = "external"
    generator_version: str = "1"
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.source_episode_ids:
            raise ValueError("evolution candidate requires source episodes")
        if self.procedure.scope.partition_key() != self.scope.partition_key():
            raise ValueError("candidate and procedure scopes must match")


@dataclass(frozen=True, slots=True)
class EvaluationReport:
    candidate_id: str
    candidate_version: int
    scope_partition_key: str
    stage: EvaluationStage
    dataset_id: str
    dataset_version: str
    evaluator_id: str
    evaluator_version: str
    rubric_id: str
    rubric_version: str
    sample_size: int
    metrics: Mapping[str, float]
    evidence_digest: str
    id: str = field(default_factory=lambda: str(uuid4()))
    safety_violations: int = 0
    passed: bool | None = None
    gate_reasons: tuple[str, ...] = ()
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if self.sample_size < 0:
            raise ValueError("sample_size cannot be negative")
        provenance = (
            self.scope_partition_key,
            self.dataset_id,
            self.dataset_version,
            self.evaluator_id,
            self.evaluator_version,
            self.rubric_id,
            self.rubric_version,
            self.evidence_digest,
        )
        if not all(value.strip() for value in provenance):
            raise ValueError("evaluation provenance fields are required")
        if self.candidate_version < 1:
            raise ValueError("candidate_version must be positive")
        if self.safety_violations < 0:
            raise ValueError("safety_violations cannot be negative")


@dataclass(frozen=True, slots=True)
class GateDecision:
    passed: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PromotionApproval:
    approver: str
    approval_ref: str
    reason: str
    candidate_id: str = ""
    candidate_version: int = 0
    scope_partition_key: str = ""
    expires_at: datetime | None = None
    approved_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.approver or not self.approval_ref or not self.reason:
            raise ValueError("approval requires approver, reference, and reason")


@dataclass(frozen=True, slots=True)
class PromotionRecord:
    candidate_id: str
    from_state: EvolutionState
    to_state: EvolutionState
    actor: str
    reason: str
    id: str = field(default_factory=lambda: str(uuid4()))
    evaluation_id: str | None = None
    approval_ref: str | None = None
    idempotency_key: str | None = None
    created_at: datetime = field(default_factory=utc_now)
