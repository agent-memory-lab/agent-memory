from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from agent_memory.domain import Episode, MemoryScope

from .domain import (
    EvaluationReport,
    EvaluationStage,
    EvolutionCandidate,
    EvolutionState,
    GeneratedProcedure,
    PromotionApproval,
    PromotionRecord,
)


class ProcedureCandidateGenerator(Protocol):
    async def generate(
        self, scope: MemoryScope, episodes: Sequence[Episode]
    ) -> Sequence[GeneratedProcedure]: ...


class EvolutionRegistry(Protocol):
    async def initialize(self) -> None: ...
    async def register(self, candidate: EvolutionCandidate) -> str: ...
    async def get(self, candidate_id: str) -> EvolutionCandidate: ...
    async def append_evaluation(self, report: EvaluationReport) -> str: ...
    async def latest_evaluation(
        self, candidate_id: str, stage: EvaluationStage
    ) -> EvaluationReport | None: ...
    async def transition(
        self,
        candidate_id: str,
        expected: EvolutionState,
        target: EvolutionState,
        *,
        actor: str,
        reason: str,
        evaluation_id: str | None = None,
        approval: PromotionApproval | None = None,
    ) -> PromotionRecord: ...
    async def history(self, candidate_id: str) -> Sequence[PromotionRecord]: ...
    async def promotion_by_idempotency_key(
        self, candidate_id: str, idempotency_key: str
    ) -> PromotionRecord | None: ...
    async def candidates_in_states(
        self, states: Sequence[EvolutionState]
    ) -> Sequence[EvolutionCandidate]: ...
    async def set_active_pointer(self, candidate: EvolutionCandidate) -> None: ...
    async def clear_active_pointer(self, scope: MemoryScope, candidate_id: str) -> None: ...
    async def active_candidate_id(self, scope: MemoryScope) -> str | None: ...
    async def invalidate_sources(
        self, source_event_ids: Sequence[str], *, actor: str, reason: str
    ) -> Sequence[PromotionRecord]: ...


class ProcedureDeployment(Protocol):
    async def activate(self, candidate: EvolutionCandidate) -> None: ...
    async def deactivate(self, candidate: EvolutionCandidate) -> None: ...
