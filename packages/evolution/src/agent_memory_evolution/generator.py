from __future__ import annotations

from collections.abc import Sequence

from agent_memory.domain import Episode, MemoryScope
from agent_memory.plugin_protocol import ConsolidationRequest
from agent_memory.procedure_induction import (
    ProcedureInductionLimits,
    induce_procedure_candidates,
)

from .domain import GeneratedProcedure


class RuleBasedProcedureGenerator:
    """Conservative baseline generator that can only produce candidate artifacts."""

    def __init__(self, *, minimum_support: int = 3, minimum_quality: float = 0.75) -> None:
        if minimum_support < 2:
            raise ValueError("minimum_support must be at least 2")
        if not 0 <= minimum_quality <= 1:
            raise ValueError("minimum_quality must be between zero and one")
        self._minimum_support = minimum_support
        self._minimum_quality = minimum_quality

    async def generate(
        self, scope: MemoryScope, episodes: Sequence[Episode]
    ) -> Sequence[GeneratedProcedure]:
        occurred_at = max((episode.occurred_at for episode in episodes), default=None)
        if occurred_at is None:
            return ()
        plan = induce_procedure_candidates(
            ConsolidationRequest(scope=scope, episodes=tuple(episodes)),
            limits=ProcedureInductionLimits(
                minimum_successful_episodes=self._minimum_support,
                minimum_quality=self._minimum_quality,
            ),
            now=occurred_at,
        )
        return tuple(
            GeneratedProcedure(
                procedure=procedure,
                source_episode_ids=procedure.source_episode_ids,
            )
            for procedure in plan.procedures
        )
