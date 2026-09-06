import asyncio

from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    EvaluationReport,
    EvaluationStage,
    EvolutionEngine,
    EvolutionGateError,
    EvolutionState,
    NullProcedureDeployment,
    PromotionApproval,
    SQLiteEvolutionRegistry,
)

from agent_memory.domain import ArtifactStatus, MemoryScope, Procedure, Provenance


def test_candidate_cannot_skip_gates_and_can_rollback(tmp_path) -> None:
    async def scenario() -> None:
        registry = SQLiteEvolutionRegistry(tmp_path / "evolution.db")
        engine = EvolutionEngine(
            registry, DeterministicPromotionPolicy(), NullProcedureDeployment()
        )
        await engine.initialize()
        scope = MemoryScope(tenant_id="test", session_id="one")
        candidate = await engine.register_procedure(
            Procedure(
                scope=scope,
                name="Use concise answers",
                trigger="User asks a question",
                steps=("Answer concisely",),
                success_conditions=("Question answered",),
                status=ArtifactStatus.CANDIDATE,
                provenance=Provenance(source_event_ids=("event-1",)),
            ),
            ("episode-1", "episode-2", "episode-3"),
        )
        try:
            await engine.promote(candidate.id, actor="test")
            raise AssertionError("promotion unexpectedly skipped offline evaluation")
        except EvolutionGateError:
            pass

        metrics = {
            "task_success_rate": 0.90,
            "task_success_delta": 0.10,
            "cross_scope_leakage": 0.0,
            "safety_violation_rate": 0.0,
        }
        for stage, samples in (
            (EvaluationStage.OFFLINE, 30),
            (EvaluationStage.SHADOW, 50),
            (EvaluationStage.CANARY, 100),
        ):
            decided = await engine.submit_evaluation(
                EvaluationReport(
                    candidate_id=candidate.id,
                    stage=stage,
                    dataset_id=f"dataset-{stage}",
                    evaluator_version="1",
                    sample_size=samples,
                    metrics=metrics,
                    evidence_digest=f"sha256:{stage}",
                ),
                actor="evaluator",
            )
            assert decided.passed is True
            if stage == EvaluationStage.CANARY:
                approval = PromotionApproval("reviewer", "ticket-1", "reviewed evidence")
                await engine.promote(candidate.id, actor="release", approval=approval)
            else:
                await engine.promote(candidate.id, actor="release")

        assert (await registry.get(candidate.id)).state == EvolutionState.ACTIVE
        await engine.rollback(candidate.id, actor="operator", reason="regression detected")
        assert (await registry.get(candidate.id)).state == EvolutionState.ROLLED_BACK

    asyncio.run(scenario())
