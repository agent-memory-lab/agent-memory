from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from types import TracebackType
from typing import Any, Protocol

from .domain import (
    AtomReview,
    Claim,
    ClaimDraft,
    DecisionRecord,
    Episode,
    EvaluationRecord,
    ExtractedAtom,
    FeedbackPage,
    FeedbackReceipt,
    ForgetMode,
    ForgetRequest,
    ForgetResult,
    IngestResult,
    MemoryBlock,
    MemoryBundle,
    MemoryChannel,
    MemoryEvent,
    MemoryItem,
    MemoryProposal,
    MemoryQuery,
    MemoryScope,
    OutcomeEvent,
    Procedure,
    ProposalResult,
    ProviderManifest,
    RetrievalTrace,
    RewardSignal,
    StateDelta,
)


class MemoryUnitOfWork(Protocol):
    async def __aenter__(self) -> MemoryUnitOfWork: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    async def find_event_by_idempotency(
        self, scope: MemoryScope, idempotency_key: str
    ) -> MemoryEvent | None: ...

    async def claim_ids_for_event(self, event_id: str) -> Sequence[str]: ...

    async def events_exist(self, scope: MemoryScope, event_ids: Sequence[str]) -> bool: ...

    async def find_proposal_result(self, proposal_id: str) -> ProposalResult | None: ...

    async def find_feedback_record(
        self, record_id: str, record_type: str
    ) -> Mapping[str, object] | None: ...

    async def find_feedback_by_idempotency(
        self, scope: MemoryScope, record_type: str, idempotency_key: str
    ) -> Mapping[str, object] | None: ...

    async def activate_pending_children(
        self, scope: MemoryScope, parent_id: str
    ) -> Sequence[str]: ...

    async def supersede_feedback(self, scope: MemoryScope, record_id: str) -> None: ...

    async def pending_feedback_count(self, scope: MemoryScope) -> int: ...

    async def append_event(self, event: MemoryEvent) -> None: ...

    async def find_current_claim(self, scope: MemoryScope, key: str) -> Claim | None: ...

    async def save_claim(self, claim: Claim) -> None: ...

    async def replace_current_claim(self, previous: Claim, current: Claim) -> None: ...

    async def add_claim_source(self, claim_id: str, event_id: str) -> None: ...

    async def save_state_delta(self, delta: StateDelta) -> None: ...

    async def save_episode(self, episode: Episode) -> None: ...

    async def save_procedure(self, procedure: Procedure) -> None: ...

    async def save_block(self, block: MemoryBlock, expected_version: int) -> MemoryBlock: ...

    async def save_decision(self, decision: DecisionRecord) -> None: ...

    async def save_outcome(self, outcome: OutcomeEvent) -> None: ...

    async def save_evaluation(self, evaluation: EvaluationRecord) -> None: ...

    async def save_reward(self, reward: RewardSignal) -> None: ...

    async def save_retrieval_trace(self, trace: RetrievalTrace) -> None: ...

    async def save_proposal(self, proposal: MemoryProposal, result: ProposalResult) -> None: ...


class MemoryRepository(Protocol):
    async def initialize(self) -> None: ...

    def unit_of_work(self) -> MemoryUnitOfWork: ...

    async def current_claims(self, scope: MemoryScope) -> Sequence[Claim]: ...

    async def search(self, query: MemoryQuery, limit: int) -> Sequence[MemoryItem]: ...

    async def read_block(self, scope: MemoryScope, block_id: str) -> MemoryBlock | None: ...

    async def search_blocks(
        self,
        scope: MemoryScope,
        text: str,
        channels: Sequence[MemoryChannel],
        limit: int,
    ) -> Sequence[MemoryBlock]: ...

    async def forget(self, request: ForgetRequest) -> ForgetResult: ...

    async def list_feedback(
        self,
        scope: MemoryScope,
        record_type: str,
        limit: int,
        after_id: str | None = None,
    ) -> Sequence[Mapping[str, object]]: ...


class AdmissionUnitOfWork(MemoryUnitOfWork, Protocol):
    """Optional typed-admission extension; all writes share the event transaction."""

    async def lock_admission_scope(self, scope: MemoryScope) -> None: ...

    async def get_admission_record(
        self, scope: MemoryScope, record_id: str,
    ) -> dict[str, Any] | None: ...

    async def list_admission_records(
        self, scope: MemoryScope, slot_key: str | None = None,
    ) -> tuple[dict[str, Any], ...]: ...

    async def save_admission_record(
        self, scope: MemoryScope, record_id: str, event_id: str, slot_key: str,
        payload: dict[str, Any], expected_version: int,
    ) -> int: ...


class AdmissionRepository(MemoryRepository, Protocol):
    """Optional snapshot and deletion-fence contracts for admission providers.

    Current records include id, event_id, slot_key, scope, payload, version,
    recorded_at. Version rows include payload, version and recorded_at. Times
    are timezone-aware ISO strings; deleted records never expose their bodies.
    """

    def unit_of_work(self) -> AdmissionUnitOfWork: ...

    async def admission_records(
        self, scope: MemoryScope, *, slot_key: str | None = None,
    ) -> tuple[dict[str, Any], ...]: ...

    async def admission_record(
        self, scope: MemoryScope, record_id: str,
    ) -> dict[str, Any] | None: ...

    async def admission_record_versions(
        self, scope: MemoryScope, record_id: str,
    ) -> tuple[dict[str, Any], ...]: ...

    async def admission_protected_sources(self, scope: MemoryScope) -> tuple[str, ...]: ...

    async def admission_snapshot(self, scope: MemoryScope) -> tuple[dict[str, Any], ...]:
        """Read visible records plus their versions within one database snapshot."""
        ...


class RetentionUnitOfWork(AdmissionUnitOfWork, Protocol):
    """Optional durable-receive ledger; methods share the active event transaction."""

    async def retention_update(self, scope: MemoryScope, request_id: str, payload: dict[str, Any]) -> None: ...

    async def retention_active(self, scope: MemoryScope) -> tuple[dict[str, Any], ...]: ...

    async def producer_get(self, scope: MemoryScope, producer_id: str) -> dict[str, Any] | None: ...

    async def producer_put(self, scope: MemoryScope, producer_id: str, payload: dict[str, Any]) -> None: ...

    async def retention_epoch(self, scope: MemoryScope) -> int: ...

    async def retention_get(
        self, scope: MemoryScope, kind: str, request_id: str,
    ) -> dict[str, Any] | None: ...

    async def retention_insert(
        self, scope: MemoryScope, kind: str, request_id: str, payload: dict[str, Any],
    ) -> None: ...

    async def retention_count(self, scope: MemoryScope, kind: str) -> int: ...

    async def retention_identity_owner(
        self, scope: MemoryScope, event_id: str, idempotency_key: str,
    ) -> str | None: ...


class RetentionRepository(MemoryRepository, Protocol):
    def unit_of_work(self) -> RetentionUnitOfWork: ...


class ClaimExtractor(Protocol):
    async def extract(self, event: MemoryEvent) -> Sequence[ClaimDraft]: ...


class ClaimGenerator(Protocol):
    """Vendor-neutral structured generation boundary for automatic extraction."""

    async def generate_claims(
        self, event: MemoryEvent
    ) -> Sequence[Mapping[str, object]]: ...


class AtomGenerator(Protocol):
    """Generate candidates only; version identifies immutable model/prompt/config."""

    version: str

    async def generate_atoms(self, event: MemoryEvent) -> Sequence[Mapping[str, Any]]: ...


class AtomReviewer(Protocol):
    """Review full source and every semantic field; never authenticate the source.

    Return one explicitly indexed verdict per candidate. Model-backed reviewers
    remain fallible; source-specific evidence admission still runs afterwards.
    """

    version: str

    async def review_atoms(
        self, event: MemoryEvent, candidates: Sequence[ExtractedAtom],
    ) -> Sequence[AtomReview]: ...


class MemoryPolicy(Protocol):
    async def should_extract(self, event: MemoryEvent) -> bool: ...

    async def accept_claim(self, event: MemoryEvent, claim: ClaimDraft) -> bool: ...


class Reranker(Protocol):
    async def rerank(
        self, query: MemoryQuery, candidates: Sequence[MemoryItem]
    ) -> Sequence[MemoryItem]: ...


class EmbeddingProvider(Protocol):
    @property
    def dimensions(self) -> int: ...

    async def embed(self, texts: Sequence[str]) -> Sequence[Sequence[float]]: ...


class GraphProvider(Protocol):
    async def expand(
        self, query: MemoryQuery, seeds: Sequence[MemoryItem]
    ) -> Sequence[MemoryItem]: ...


class EvolutionProvider(Protocol):
    async def propose_procedures(
        self, episodes: Sequence[Episode], rewards: Sequence[RewardSignal]
    ) -> Sequence[Procedure]: ...


class ConsolidationScheduler(Protocol):
    async def enqueue_event(self, event: MemoryEvent, result: IngestResult) -> str: ...

    async def cancel_forget(self, request: ForgetRequest) -> int: ...


class MemoryProvider(Protocol):
    async def initialize(self) -> None: ...

    async def ingest_event(self, event: MemoryEvent) -> IngestResult: ...

    async def retrieve(self, query: MemoryQuery) -> MemoryBundle: ...

    async def get_state(self, scope: MemoryScope) -> tuple[Claim, ...]: ...

    async def forget(self, request: ForgetRequest) -> ForgetResult: ...

    async def forget_block(
        self, scope: MemoryScope, block_id: str, mode: ForgetMode = ForgetMode.ARCHIVE
    ) -> ForgetResult: ...

    async def propose(self, proposal: MemoryProposal) -> ProposalResult: ...

    async def record_episode(self, episode: Episode) -> str: ...

    async def publish_procedure(self, procedure: Procedure) -> str: ...

    async def write_block(self, block: MemoryBlock, expected_version: int = 0) -> MemoryBlock: ...

    async def read_block(self, scope: MemoryScope, block_id: str) -> MemoryBlock | None: ...

    async def search_blocks(
        self,
        scope: MemoryScope,
        text: str,
        channels: Sequence[MemoryChannel] = (),
        limit: int = 8,
    ) -> tuple[MemoryBlock, ...]: ...

    async def record_decision(self, decision: DecisionRecord) -> str: ...

    async def record_outcome(self, outcome: OutcomeEvent) -> str: ...

    async def record_evaluation(self, evaluation: EvaluationRecord) -> str: ...

    async def record_reward(self, reward: RewardSignal) -> str: ...

    async def feedback_status(
        self, scope: MemoryScope, record_id: str, record_type: str
    ) -> FeedbackReceipt | None: ...

    async def feedback_history(
        self,
        scope: MemoryScope,
        record_type: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> FeedbackPage: ...

    def manifest(self) -> ProviderManifest: ...


class BitemporalMemoryRepository(Protocol):
    """Optional Claim history port; advertised by bitemporal_claims capability."""

    async def claims_at(
        self, scope: MemoryScope, *, valid_at: datetime, known_at: datetime
    ) -> Sequence[Claim]: ...


class BitemporalMemoryProvider(Protocol):
    """Optional provider state query over both independent time axes."""

    async def get_state_at(
        self, scope: MemoryScope, *, valid_at: datetime, known_at: datetime
    ) -> tuple[Claim, ...]: ...
