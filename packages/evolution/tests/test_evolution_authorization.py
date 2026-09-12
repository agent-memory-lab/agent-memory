import asyncio
import sqlite3
from contextlib import closing

import pytest
from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    EvaluationReport,
    EvaluationStage,
    EvolutionEngine,
    EvolutionGateError,
    NullProcedureDeployment,
    SQLiteEvolutionRegistry,
)

from agent_memory import ArtifactStatus, MemoryScope, Procedure, Provenance


def test_legacy_registry_schema_is_upgraded(tmp_path) -> None:
    path = tmp_path / "legacy.db"
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            """
            CREATE TABLE evolution_evaluations (
                id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, stage TEXT NOT NULL,
                dataset_id TEXT NOT NULL, evaluator_version TEXT NOT NULL,
                sample_size INTEGER NOT NULL, metrics_json TEXT NOT NULL,
                evidence_digest TEXT NOT NULL, safety_violations INTEGER NOT NULL,
                passed INTEGER NOT NULL, gate_reasons_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE evolution_promotions (
                id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL, from_state TEXT NOT NULL,
                to_state TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL,
                evaluation_id TEXT, approval_ref TEXT, created_at TEXT NOT NULL
            );
            """
        )
    asyncio.run(SQLiteEvolutionRegistry(path).initialize())
    with closing(sqlite3.connect(path)) as connection:
        evaluation_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(evolution_evaluations)")
        }
        promotion_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(evolution_promotions)")
        }
    assert {
        "candidate_version",
        "scope_partition_key",
        "dataset_version",
        "evaluator_id",
        "rubric_id",
        "rubric_version",
    } <= evaluation_columns
    assert "idempotency_key" in promotion_columns


def test_evaluation_requires_matching_scope_version_and_trusted_actor(tmp_path) -> None:
    async def scenario() -> None:
        scope = MemoryScope("tenant", session_id="session")
        engine = EvolutionEngine(
            SQLiteEvolutionRegistry(tmp_path / "evolution.db"),
            DeterministicPromotionPolicy(trusted_evaluators=("trusted",)),
            NullProcedureDeployment(),
        )
        await engine.initialize()
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

        def report(**overrides) -> EvaluationReport:
            values = {
                "candidate_id": candidate.id,
                "candidate_version": candidate.procedure.version,
                "scope_partition_key": scope.partition_key(),
                "stage": EvaluationStage.OFFLINE,
                "dataset_id": "fixed-set",
                "dataset_version": "1",
                "evaluator_id": "trusted",
                "evaluator_version": "1",
                "rubric_id": "task-success",
                "rubric_version": "1",
                "sample_size": 30,
                "metrics": {
                    "task_success_rate": 0.9,
                    "task_success_delta": 0.1,
                    "cross_scope_leakage": 0.0,
                    "safety_violation_rate": 0.0,
                },
                "evidence_digest": "sha256:evidence",
            }
            values.update(overrides)
            return EvaluationReport(**values)

        with pytest.raises(EvolutionGateError, match="version"):
            await engine.submit_evaluation(report(candidate_version=2), actor="trusted")
        with pytest.raises(EvolutionGateError, match="scope"):
            await engine.submit_evaluation(report(scope_partition_key="wrong"), actor="trusted")
        with pytest.raises(EvolutionGateError, match="actor"):
            await engine.submit_evaluation(report(), actor="spoofed")
        with pytest.raises(EvolutionGateError, match="not trusted"):
            await engine.submit_evaluation(
                report(evaluator_id="untrusted"), actor="untrusted"
            )

        assert (await engine.submit_evaluation(report(), actor="trusted")).passed is True

    asyncio.run(scenario())
