import asyncio
from datetime import timedelta

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

from agent_memory.domain import ArtifactStatus, MemoryScope, Procedure, Provenance, utc_now


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
                    candidate_version=candidate.procedure.version,
                    scope_partition_key=scope.partition_key(),
                    stage=stage,
                    dataset_id=f"dataset-{stage}",
                    dataset_version="1",
                    evaluator_id="evaluator",
                    evaluator_version="1",
                    rubric_id="task-success",
                    rubric_version="1",
                    sample_size=samples,
                    metrics=metrics,
                    evidence_digest=f"sha256:{stage}",
                ),
                actor="evaluator",
            )
            assert decided.passed is True
            if stage == EvaluationStage.CANARY:
                approval = PromotionApproval(
                    "reviewer",
                    "ticket-1",
                    "reviewed evidence",
                    candidate_id=candidate.id,
                    candidate_version=candidate.procedure.version,
                    scope_partition_key=scope.partition_key(),
                    expires_at=utc_now() + timedelta(hours=1),
                )
                promoted = await engine.promote(
                    candidate.id,
                    actor="release",
                    approval=approval,
                    idempotency_key="activate-once",
                )
                assert (
                    await engine.promote(
                        candidate.id,
                        actor="release",
                        approval=approval,
                        idempotency_key="activate-once",
                    )
                ).id == promoted.id
            else:
                await engine.promote(candidate.id, actor="release")

        assert (await registry.get(candidate.id)).state == EvolutionState.ACTIVE
        assert (await engine.active_candidate(scope)).id == candidate.id
        await engine.rollback(candidate.id, actor="operator", reason="regression detected")
        assert (await registry.get(candidate.id)).state == EvolutionState.ROLLED_BACK
        assert await engine.active_candidate(scope) is None

    asyncio.run(scenario())
