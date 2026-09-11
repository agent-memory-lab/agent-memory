import asyncio

from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    EvolutionEngine,
    EvolutionState,
    NullProcedureDeployment,
    SQLiteEvolutionRegistry,
)

from agent_memory import ArtifactStatus, MemoryScope, Procedure, Provenance


def test_restart_recovers_incomplete_activation(tmp_path) -> None:
    async def scenario() -> None:
        registry = SQLiteEvolutionRegistry(tmp_path / "evolution.db")
        engine = EvolutionEngine(
            registry, DeterministicPromotionPolicy(), NullProcedureDeployment()
        )
        await engine.initialize()
        scope = MemoryScope("tenant", session_id="session")
        candidate = await engine.register_procedure(
            Procedure(
                scope=scope,
                name="candidate",
                trigger="request",
                steps=("act",),
                success_conditions=("success",),
                status=ArtifactStatus.CANDIDATE,
                provenance=Provenance(source_event_ids=("event-1",)),
            ),
            ("episode-1",),
        )
        await registry.transition(
            candidate.id,
            EvolutionState.CANDIDATE,
            EvolutionState.EVALUATED,
            actor="test",
            reason="setup",
        )
        await registry.transition(
            candidate.id,
            EvolutionState.EVALUATED,
            EvolutionState.SHADOW,
            actor="test",
            reason="setup",
        )
        await registry.transition(
            candidate.id,
            EvolutionState.SHADOW,
            EvolutionState.CANARY,
            actor="test",
            reason="setup",
        )
        await registry.transition(
            candidate.id,
            EvolutionState.CANARY,
            EvolutionState.ACTIVATING,
            actor="test",
            reason="simulated interruption",
        )

        records = await engine.recover()
        assert len(records) == 1
        assert records[0].to_state == EvolutionState.CANARY
        assert (await registry.get(candidate.id)).state == EvolutionState.CANARY
        assert await engine.active_candidate(scope) is None
        assert await engine.recover() == ()

    asyncio.run(scenario())
