import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from agent_memory_postgres.consolidation import TrajectoryBlockConsolidator
from agent_memory_postgres.jobs import ConsolidationJob, JobStatus

from agent_memory import MemoryScope


class FakeProvider:
    def __init__(self, claims):
        self._claims = claims
        self.blocks = []

    async def get_state(self, scope):
        return self._claims

    async def read_block(self, scope, block_id):
        return None

    async def write_block(self, block, *, expected_version):
        self.blocks.append((block, expected_version))
        return block


def test_consolidator_writes_one_evidence_backed_block_for_current_claims():
    async def scenario():
        scope = MemoryScope(tenant_id="test", session_id="one")
        provider = FakeProvider(
            (
                SimpleNamespace(id="claim-1", key="language", text="Use Chinese."),
                SimpleNamespace(id="claim-2", key="format", text="Use short answers."),
            )
        )
        job = ConsolidationJob(
            id="job-1",
            job_key="event:event-1:consolidate",
            scope=scope,
            job_type="memory.consolidate",
            payload={"event_id": "event-1", "claim_ids": ["claim-1", "claim-2"]},
            status=JobStatus.RUNNING,
            attempts=1,
            max_attempts=5,
            next_attempt_at=datetime.now(UTC),
        )

        await TrajectoryBlockConsolidator(provider).handle(job)

        assert len(provider.blocks) == 1
        block, expected_version = provider.blocks[0]
        assert block.event_ids == ("event-1",)
        assert "language: Use Chinese." in block.content
        assert expected_version == 0

    asyncio.run(scenario())
