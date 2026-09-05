from __future__ import annotations

from types import TracebackType
from typing import Protocol, Sequence

from .domain import (
    Claim,
    ClaimDraft,
    DecisionRecord,
    Episode,
    ForgetRequest,
    ForgetResult,
    IngestResult,
    MemoryBundle,
    MemoryEvent,
    MemoryItem,
    MemoryProposal,
    MemoryQuery,
    MemoryScope,
    OutcomeEvent,
    Procedure,
    ProposalResult,
    ProviderManifest,
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

    async def append_event(self, event: MemoryEvent) -> None: ...

    async def find_current_claim(self, scope: MemoryScope, key: str) -> Claim | None: ...

    async def save_claim(self, claim: Claim) -> None: ...

    async def replace_current_claim(self, previous: Claim, current: Claim) -> None: ...

    async def add_claim_source(self, claim_id: str, event_id: str) -> None: ...

    async def save_state_delta(self, delta: StateDelta) -> None: ...

    async def save_episode(self, episode: Episode) -> None: ...

    async def save_procedure(self, procedure: Procedure) -> None: ...

    async def save_decision(self, decision: DecisionRecord) -> None: ...

    async def save_outcome(self, outcome: OutcomeEvent) -> None: ...

    async def save_reward(self, reward: RewardSignal) -> None: ...

    async def save_proposal(
        self, proposal: MemoryProposal, result: ProposalResult
    ) -> None: ...


class MemoryRepository(Protocol):
    async def initialize(self) -> None: ...

    def unit_of_work(self) -> MemoryUnitOfWork: ...

    async def current_claims(self, scope: MemoryScope) -> Sequence[Claim]: ...

    async def search(self, query: MemoryQuery, limit: int) -> Sequence[MemoryItem]: ...

    async def forget(self, request: ForgetRequest) -> ForgetResult: ...


class ClaimExtractor(Protocol):
    async def extract(self, event: MemoryEvent) -> Sequence[ClaimDraft]: ...


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
    async def expand(self, query: MemoryQuery, seeds: Sequence[MemoryItem]) -> Sequence[MemoryItem]: ...


class EvolutionProvider(Protocol):
    async def propose_procedures(
        self, episodes: Sequence[Episode], rewards: Sequence[RewardSignal]
    ) -> Sequence[Procedure]: ...


class ConsolidationScheduler(Protocol):
    async def enqueue_event(self, event: MemoryEvent, result: IngestResult) -> str: ...


class MemoryProvider(Protocol):
    async def initialize(self) -> None: ...

    async def ingest_event(self, event: MemoryEvent) -> IngestResult: ...

    async def retrieve(self, query: MemoryQuery) -> MemoryBundle: ...

    async def get_state(self, scope: MemoryScope) -> tuple[Claim, ...]: ...

    async def forget(self, request: ForgetRequest) -> ForgetResult: ...

    async def propose(self, proposal: MemoryProposal) -> ProposalResult: ...

    async def record_episode(self, episode: Episode) -> str: ...

    async def publish_procedure(self, procedure: Procedure) -> str: ...

    async def record_decision(self, decision: DecisionRecord) -> str: ...

    async def record_outcome(self, outcome: OutcomeEvent) -> str: ...

    async def record_reward(self, reward: RewardSignal) -> str: ...

    def manifest(self) -> ProviderManifest: ...
