import asyncio

import pytest
from agent_memory_evolution.domain import EvolutionState
from agent_memory_evolution.engine import EvolutionEngine, EvolutionGateError
from agent_memory_evolution.policy import DeterministicPromotionPolicy
from agent_memory_evolution.registry import SQLiteEvolutionRegistry

from agent_memory import ArtifactStatus, MemoryScope, Procedure, Provenance


class NoopDeployment:
    async def activate(self, candidate) -> None:
        return None

    async def deactivate(self, candidate) -> None:
        return None


def test_source_deletion_invalidates_candidate_and_blocks_promotion(tmp_path) -> None:
    async def scenario() -> None:
        registry = SQLiteEvolutionRegistry(tmp_path / "evolution.db")
        engine = EvolutionEngine(
            registry,
            DeterministicPromotionPolicy(),
            NoopDeployment(),
        )
        await engine.initialize()
        scope = MemoryScope("tenant", session_id="session")
        procedure = Procedure(
            scope=scope,
            name="candidate",
            trigger="request",
            steps=("act",),
            success_conditions=("accepted",),
            status=ArtifactStatus.CANDIDATE,
            provenance=Provenance(source_event_ids=("event-1",)),
        )
        candidate = await engine.register_procedure(procedure, ("episode-1",))

        records = await engine.invalidate_sources(
            ("event-1",), actor="privacy-worker", reason="source erased"
        )
        assert len(records) == 1
        assert records[0].to_state == EvolutionState.INVALIDATED
        assert (await registry.get(candidate.id)).state == EvolutionState.INVALIDATED
        assert await engine.invalidate_sources(
            ("event-1",), actor="privacy-worker", reason="retry"
        ) == ()

        with pytest.raises(EvolutionGateError, match="cannot be promoted"):
            await engine.promote(candidate.id, actor="host")

    asyncio.run(scenario())
