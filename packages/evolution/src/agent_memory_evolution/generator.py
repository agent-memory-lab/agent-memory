from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from uuid import NAMESPACE_URL, uuid5

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
            if (
                episode.scope.partition_key() != partition
                or episode.quality < self._minimum_quality
            ):
                continue
            groups[episode.action.strip().casefold()].append(episode)

        generated: list[GeneratedProcedure] = []
        for normalized_action, members in sorted(groups.items()):
            successful = [item for item in members if self._is_success(item)]
            failures = [item for item in members if not self._is_success(item)]
            if not normalized_action or len(successful) < self._minimum_support:
                continue
            source_event_ids = tuple(
                dict.fromkeys(
                    event_id
                    for episode in members
                    for event_id in episode.provenance.source_event_ids
                )
            )
            if not source_event_ids:
                continue
            representative = max(successful, key=lambda item: item.quality)
            outcomes = tuple(
                dict.fromkeys(item.outcome for item in successful if item.outcome)
            )[:3]
            failure_patterns = tuple(
                dict.fromkeys(item.lesson for item in failures if item.lesson)
            )[:3]
            member_ids = tuple(sorted(item.id for item in members))
            procedure_id = str(
                uuid5(
                    NAMESPACE_URL,
                    ":".join((partition, normalized_action, *member_ids)),
                )
            )
            procedure = Procedure(
                id=procedure_id,
                scope=scope,
                name=f"Candidate procedure: {representative.action[:80]}",
                trigger=representative.observation,
                steps=(representative.action,),
                success_conditions=outcomes or (representative.outcome,),
                failure_patterns=failure_patterns,
                status=ArtifactStatus.CANDIDATE,
                provenance=Provenance(
                    source_event_ids=source_event_ids,
                    extractor=type(self).__name__,
                    provider="agent-memory-evolution",
                ),
            )
            generated.append(
                GeneratedProcedure(
                    procedure=procedure,
                    source_episode_ids=member_ids,
                )
            )
        return tuple(generated)

    @staticmethod
    def _is_success(episode: Episode) -> bool:
        status = episode.outcome.partition(":")[0]
        return status not in {"failed", "cancelled", "timed_out", "unknown"}
