from __future__ import annotations

from typing import Protocol, Sequence

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


class ProcedureDeployment(Protocol):
    async def activate(self, candidate: EvolutionCandidate) -> None: ...
    async def deactivate(self, candidate: EvolutionCandidate) -> None: ...

