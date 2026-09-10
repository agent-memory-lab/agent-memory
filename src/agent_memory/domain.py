from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from json import dumps
from typing import Any
from uuid import uuid4

PROTOCOL_VERSION = "0.1"
SCHEMA_VERSION = 1


def utc_now() -> datetime:
    return datetime.now(UTC)


def canonical_json(value: Any) -> str:
    return dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)


class ScopeLevel(StrEnum):
    TENANT = "tenant"
    USER = "user"
    AGENT = "agent"
    WORKSPACE = "workspace"
    SESSION = "session"


class ClaimStatus(StrEnum):
    CANDIDATE = "candidate"
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    ARCHIVED = "archived"
    REJECTED = "rejected"


class ArtifactStatus(StrEnum):
    CANDIDATE = "candidate"
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    ARCHIVED = "archived"
    RETIRED = "retired"


class MemoryKind(StrEnum):
    EVENT = "event"
    CLAIM = "claim"
    BLOCK = "block"
    EPISODE = "episode"
    PROCEDURE = "procedure"
    LATENT_REFERENCE = "latent_reference"


class MemoryChannel(StrEnum):
    SEMANTIC = "semantic"
    EPISODIC = "episodic"
    PROCEDURAL = "procedural"


class ForgetMode(StrEnum):
    ARCHIVE = "archive"
    ERASE = "erase"


class ProposalStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class MemoryScope:
    tenant_id: str
    namespace: str = "default"
    user_id: str | None = None
    agent_id: str | None = None
    workspace_id: str | None = None
    session_id: str | None = None

    def __post_init__(self) -> None:
        if not self.tenant_id.strip():
            raise ValueError("tenant_id must not be empty")
        if not self.namespace.strip():
            raise ValueError("namespace must not be empty")

    def project(self, level: ScopeLevel) -> MemoryScope:
        if level == ScopeLevel.TENANT:
            return MemoryScope(self.tenant_id, self.namespace)
        if level == ScopeLevel.USER:
            if not self.user_id:
                raise ValueError("user_id is required for user-scoped memory")
            return MemoryScope(self.tenant_id, self.namespace, self.user_id)
        if level == ScopeLevel.AGENT:
            if not self.agent_id:
                raise ValueError("agent_id is required for agent-scoped memory")
            return MemoryScope(self.tenant_id, self.namespace, self.user_id, self.agent_id)
        if level == ScopeLevel.WORKSPACE:
            if not self.workspace_id:
                raise ValueError("workspace_id is required for workspace-scoped memory")
            return MemoryScope(
                self.tenant_id,
                self.namespace,
                self.user_id,
                self.agent_id,
                self.workspace_id,
            )
        if not self.session_id:
            raise ValueError("session_id is required for session-scoped memory")
        return self

    def partition_key(self) -> str:
        return sha256(canonical_json(asdict(self)).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class Provenance:
    source_event_ids: tuple[str, ...] = ()
    extractor: str = "deterministic"
    provider: str = "local"
    model: str | None = None
    prompt_version: str | None = None
    source_uri: str | None = None
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class MemoryEvent:
    scope: MemoryScope
    event_type: str
    content: str
    id: str = field(default_factory=lambda: str(uuid4()))
    metadata: Mapping[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=utc_now)
    ingested_at: datetime = field(default_factory=utc_now)
    idempotency_key: str | None = None
    actor: str = "agent"
    source_uri: str | None = None
    sensitivity: str = "internal"
    retention_class: str = "standard"
    schema_version: int = SCHEMA_VERSION
    content_hash: str = ""

    def __post_init__(self) -> None:
        if not self.event_type.strip():
            raise ValueError("event_type must not be empty")
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        if not self.content_hash:
            payload = f"{self.event_type}:{self.content}:{canonical_json(self.metadata)}"
            object.__setattr__(self, "content_hash", sha256(payload.encode("utf-8")).hexdigest())


@dataclass(frozen=True, slots=True)
class ClaimDraft:
    key: str
    value: Any
    text: str
    confidence: float = 1.0
    importance: float = 0.5
    scope_level: ScopeLevel = ScopeLevel.SESSION
    valid_from: datetime | None = None
    provenance: Provenance | None = None

    def __post_init__(self) -> None:
        if not self.key.strip() or not self.text.strip():
            raise ValueError("claim key and text must not be empty")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("confidence must be between 0 and 1")
        if not 0.0 <= self.importance <= 1.0:
            raise ValueError("importance must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class Claim:
    id: str
    scope: MemoryScope
    key: str
    value: Any
    text: str
    confidence: float
    importance: float
    status: ClaimStatus
    provenance: Provenance
    valid_from: datetime
    created_at: datetime
    version: int = 1
    valid_to: datetime | None = None
    supersedes: str | None = None
    superseded_by: str | None = None


@dataclass(frozen=True, slots=True)
class StateDelta:
    id: str
    scope: MemoryScope
    key: str
    operation: str
    source_event_id: str
    current_claim_id: str
    previous_claim_id: str | None = None
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class Episode:
    scope: MemoryScope
    observation: str
    action: str
    outcome: str
    lesson: str
    id: str = field(default_factory=lambda: str(uuid4()))
    quality: float = 0.5
    status: ArtifactStatus = ArtifactStatus.CANDIDATE
    provenance: Provenance = field(default_factory=Provenance)
    occurred_at: datetime = field(default_factory=utc_now)
    version: int = 1


@dataclass(frozen=True, slots=True)
class Procedure:
    scope: MemoryScope
    name: str
    trigger: str
    steps: tuple[str, ...]
    success_conditions: tuple[str, ...]
    id: str = field(default_factory=lambda: str(uuid4()))
    failure_patterns: tuple[str, ...] = ()
    status: ArtifactStatus = ArtifactStatus.CANDIDATE
    provenance: Provenance = field(default_factory=Provenance)
    version: int = 1
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class MemoryBlock:
    """A compact, evidence-backed memory unit managed independently by an agent."""

    scope: MemoryScope
    title: str
    content: str
    event_ids: tuple[str, ...]
    id: str = field(default_factory=lambda: str(uuid4()))
    channel: MemoryChannel = MemoryChannel.SEMANTIC
    token_budget: int = 256
    status: ArtifactStatus = ArtifactStatus.ACTIVE
    metadata: Mapping[str, Any] = field(default_factory=dict)
    provenance: Provenance = field(default_factory=Provenance)
    version: int = 1
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.title.strip() or not self.content.strip():
            raise ValueError("memory block title and content must not be empty")
        if not self.event_ids:
            raise ValueError("memory block requires source event evidence")
        if not 16 <= self.token_budget <= 4096:
            raise ValueError("memory block token_budget must be between 16 and 4096")
        if self.version < 1:
            raise ValueError("memory block version must be positive")


@dataclass(frozen=True, slots=True)
class DecisionRecord:
    scope: MemoryScope
    action: str
    memory_ids: tuple[str, ...]
    id: str = field(default_factory=lambda: str(uuid4()))
    procedure_ids: tuple[str, ...] = ()
    policy_version: str = "trusted-default"
    context_hash: str = ""
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class OutcomeEvent:
    scope: MemoryScope
    decision_id: str
    outcome: str
    success: bool
    id: str = field(default_factory=lambda: str(uuid4()))
    score: float | None = None
    metrics: Mapping[str, float] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class RewardSignal:
    scope: MemoryScope
    outcome_id: str
    value: float
    formula_version: str
    id: str = field(default_factory=lambda: str(uuid4()))
    components: Mapping[str, float] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class MemoryProposal:
    scope: MemoryScope
    key: str
    value: Any
    text: str
    source_event_ids: tuple[str, ...]
    expected_version: int
    id: str = field(default_factory=lambda: str(uuid4()))
    scope_level: ScopeLevel = ScopeLevel.SESSION
    confidence: float = 1.0
    importance: float = 0.5
    actor: str = "agent"
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.key.strip() or not self.text.strip():
            raise ValueError("proposal key and text must not be empty")
        if not self.source_event_ids:
            raise ValueError("proposal requires source evidence")
        if self.expected_version < 0:
            raise ValueError("expected_version must be zero or greater")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("confidence must be between 0 and 1")
        if not 0.0 <= self.importance <= 1.0:
            raise ValueError("importance must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class ProposalResult:
    proposal_id: str
    status: ProposalStatus
    claim_id: str | None = None
    state_delta_id: str | None = None
    superseded_claim_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class MemoryQuery:
    scope: MemoryScope
    text: str
    limit: int = 8
    token_budget: int = 1200
    include_current_state: bool = True
    channels: tuple[MemoryChannel, ...] = (
        MemoryChannel.SEMANTIC,
        MemoryChannel.EPISODIC,
        MemoryChannel.PROCEDURAL,
    )

    def __post_init__(self) -> None:
        if not 1 <= self.limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        if self.token_budget < 64:
            raise ValueError("token_budget must be at least 64")


@dataclass(frozen=True, slots=True)
class MemoryItem:
    id: str
    kind: MemoryKind
    text: str
    score: float
    occurred_at: datetime
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Citation:
    memory_id: str
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MemoryCapabilities:
    current_state: bool = True
    semantic_memory: bool = True
    episodic_memory: bool = True
    procedural_memory: bool = True
    supersession: bool = True
    archive: bool = True
    legal_erase: bool = True
    decision_lineage: bool = True
    outcome_feedback: bool = True
    self_edit_proposals: bool = True
    mcp_tools: bool = True
    agent_lifecycle_adapter: bool = True
    automatic_extraction: bool = False
    memory_blocks: bool = False
    semantic_reranking: bool = False
    background_consolidation: bool = False
    graph_memory: bool = False
    latent_memory: bool = False
    learned_policy: bool = False
    policy_joint_training: bool = False
    test_time_learning: bool = False


@dataclass(frozen=True, slots=True)
class ProviderManifest:
    name: str
    version: str
    protocol_version: str
    schema_version: int
    capabilities: MemoryCapabilities


@dataclass(frozen=True, slots=True)
class MemoryBundle:
    current_state: tuple[Claim, ...]
    relevant_memories: tuple[MemoryItem, ...]
    episodes: tuple[MemoryItem, ...]
    procedures: tuple[MemoryItem, ...]
    citations: tuple[Citation, ...]
    token_estimate: int
    retrieval_metadata: Mapping[str, Any]
    capability_snapshot: MemoryCapabilities


@dataclass(frozen=True, slots=True)
class IngestResult:
    event_id: str
    claim_ids: tuple[str, ...]
    state_delta_ids: tuple[str, ...]
    duplicate: bool
    superseded_claim_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ForgetRequest:
    scope: MemoryScope
    memory_ids: tuple[str, ...] = ()
    all_in_scope: bool = False
    mode: ForgetMode = ForgetMode.ARCHIVE

    def __post_init__(self) -> None:
        if not self.memory_ids and not self.all_in_scope:
            raise ValueError("memory_ids or all_in_scope is required")


@dataclass(frozen=True, slots=True)
class ForgetResult:
    affected_events: int
    affected_claims: int
    affected_artifacts: int
    mode: ForgetMode
