from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from agent_memory import MemoryBlock, MemoryChannel, MemoryProvider

from .jobs import ConsolidationJob, JobHandler


@dataclass(frozen=True, slots=True)
class BlockConsolidationPolicy:
    """Bounds deterministic trajectory-to-block compaction."""

    minimum_claims: int = 2
    maximum_claims: int = 6
    token_budget: int = 256

    def __post_init__(self) -> None:
        if self.minimum_claims < 1:
            raise ValueError("minimum_claims must be positive")
        if self.maximum_claims < self.minimum_claims:
            raise ValueError("maximum_claims must be at least minimum_claims")
        if not 32 <= self.token_budget <= 4096:
            raise ValueError("token_budget must be between 32 and 4096")


class TrajectoryBlockConsolidator:
    """Create a compact, evidence-backed block from one accepted trajectory job.

    This handler is deterministic: it never calls a model and it promotes no
    procedures. It only compacts claims that remain current when the job runs.
    """

    def __init__(
        self,
        provider: MemoryProvider,
        policy: BlockConsolidationPolicy | None = None,
    ) -> None:
        self._provider = provider
        self._policy = policy or BlockConsolidationPolicy()

    async def handle(self, job: ConsolidationJob) -> None:
        if job.job_type != "memory.consolidate":
            raise ValueError(f"unsupported consolidation job type: {job.job_type}")
        event_id = self._event_id(job.payload)
        claim_ids = self._claim_ids(job.payload)
        if event_id is None or len(claim_ids) < self._policy.minimum_claims:
            return

        current = await self._provider.get_state(job.scope)
        selected = [claim for claim in current if claim.id in claim_ids]
        selected = selected[: self._policy.maximum_claims]
        if len(selected) < self._policy.minimum_claims:
            return

        block_id = str(uuid5(NAMESPACE_URL, f"memory-block:{job.id}"))
        if await self._provider.read_block(job.scope, block_id) is not None:
            return

        title = "Consolidated trajectory"
        content = self._bounded_content(
            title,
            tuple(f"- {claim.key}: {claim.text}" for claim in selected),
        )
        block = MemoryBlock(
            id=block_id,
            scope=job.scope,
            title=title,
            content=content,
            event_ids=(event_id,),
            channel=MemoryChannel.SEMANTIC,
            token_budget=self._policy.token_budget,
        )
        await self._provider.write_block(block, expected_version=0)

    def as_handler(self) -> JobHandler:
        return self.handle

    @staticmethod
    def _event_id(payload: Mapping[str, object]) -> str | None:
        event_id = payload.get("event_id")
        return event_id if isinstance(event_id, str) and event_id else None

    @staticmethod
    def _claim_ids(payload: Mapping[str, object]) -> set[str]:
        value = payload.get("claim_ids", ())
        if not isinstance(value, (list, tuple)):
            return set()
        return {item for item in value if isinstance(item, str) and item}

    def _bounded_content(self, title: str, lines: tuple[str, ...]) -> str:
        selected: list[str] = []
        used = self._token_estimate(title)
        for line in lines:
            cost = self._token_estimate(line)
            if used + cost > self._policy.token_budget:
                break
            selected.append(line)
            used += cost
        if not selected:
            raise ValueError("consolidated memory block has no content within its budget")
        return "\n".join(selected)

    @staticmethod
    def _token_estimate(text: str) -> int:
        latin = sum(1 for character in text if ord(character) < 128)
        non_latin = len(text) - latin
        return max(1, non_latin + (latin + 3) // 4)
