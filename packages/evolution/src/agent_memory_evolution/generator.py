from __future__ import annotations

from collections import defaultdict
from typing import Sequence

from agent_memory.domain import ArtifactStatus, Episode, MemoryScope, Procedure, Provenance

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
        groups: dict[str, list[Episode]] = defaultdict(list)
        partition = scope.partition_key()
        for episode in episodes:
            if episode.scope.partition_key() != partition or episode.quality < self._minimum_quality:
                continue
            groups[episode.action.strip().casefold()].append(episode)

        generated: list[GeneratedProcedure] = []
        for normalized_action, members in sorted(groups.items()):
            if not normalized_action or len(members) < self._minimum_support:
                continue
            source_event_ids = tuple(dict.fromkeys(
                event_id
                for episode in members
                for event_id in episode.provenance.source_event_ids
            ))
            if not source_event_ids:
                continue
            representative = max(members, key=lambda item: item.quality)
            outcomes = tuple(dict.fromkeys(item.outcome for item in members if item.outcome))[:3]
            procedure = Procedure(
                scope=scope,
                name=f"Candidate procedure: {representative.action[:80]}",
                trigger=representative.observation,
                steps=(representative.action,),
                success_conditions=outcomes or (representative.outcome,),
                status=ArtifactStatus.CANDIDATE,
                provenance=Provenance(
                    source_event_ids=source_event_ids,
                    extractor=type(self).__name__,
                    provider="agent-memory-evolution",
                ),
            )
            generated.append(GeneratedProcedure(
                procedure=procedure,
                source_episode_ids=tuple(item.id for item in members),
            ))
        return tuple(generated)

