from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from agent_memory.domain import ArtifactStatus, MemoryScope, Procedure, Provenance
from agent_memory.serialization import to_jsonable

from .domain import (
    EvaluationReport,
    EvaluationStage,
    EvolutionCandidate,
    EvolutionState,
    PromotionApproval,
    PromotionRecord,
)

ALLOWED_TRANSITIONS = {
    EvolutionState.CANDIDATE: {EvolutionState.EVALUATED, EvolutionState.REJECTED},
    EvolutionState.EVALUATED: {EvolutionState.SHADOW, EvolutionState.ROLLED_BACK},
    EvolutionState.SHADOW: {EvolutionState.CANARY, EvolutionState.ROLLED_BACK},
    EvolutionState.CANARY: {EvolutionState.ACTIVATING, EvolutionState.ROLLED_BACK},
    EvolutionState.ACTIVATING: {EvolutionState.ACTIVE, EvolutionState.CANARY},
    EvolutionState.ACTIVE: {EvolutionState.ROLLING_BACK},
    EvolutionState.ROLLING_BACK: {EvolutionState.ROLLED_BACK, EvolutionState.ACTIVE},
    EvolutionState.ROLLED_BACK: set(),
    EvolutionState.REJECTED: set(),
}


class EvolutionConflict(RuntimeError):
    pass


class EvolutionNotFound(KeyError):
    pass


class SQLiteEvolutionRegistry:
    def __init__(self, path: str | Path) -> None:
        self._path = str(path)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path)
        connection.row_factory = sqlite3.Row
        return connection

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize)

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript("""
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS evolution_candidates (
                    id TEXT PRIMARY KEY,
                    scope_json TEXT NOT NULL,
                    procedure_json TEXT NOT NULL,
                    source_episode_ids_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    baseline_version TEXT,
                    generator TEXT NOT NULL,
                    generator_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS evolution_evaluations (
                    id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL REFERENCES evolution_candidates(id),
                    candidate_version INTEGER NOT NULL,
                    scope_partition_key TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    dataset_version TEXT NOT NULL,
                    evaluator_id TEXT NOT NULL,
                    evaluator_version TEXT NOT NULL,
                    rubric_id TEXT NOT NULL,
                    rubric_version TEXT NOT NULL,
                    sample_size INTEGER NOT NULL,
                    metrics_json TEXT NOT NULL,
                    evidence_digest TEXT NOT NULL,
                    safety_violations INTEGER NOT NULL,
                    passed INTEGER NOT NULL,
                    gate_reasons_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_evolution_evaluations_latest
                    ON evolution_evaluations(candidate_id, stage, created_at DESC);
                CREATE TABLE IF NOT EXISTS evolution_promotions (
                    id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL REFERENCES evolution_candidates(id),
                    from_state TEXT NOT NULL,
                    to_state TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    evaluation_id TEXT,
                    approval_ref TEXT,
                    idempotency_key TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS evolution_active_pointers (
                    scope_partition_key TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL REFERENCES evolution_candidates(id),
                    procedure_version INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
            self._ensure_column(
                connection,
                "evolution_evaluations",
                "candidate_version",
                "INTEGER NOT NULL DEFAULT 1",
            )
            for column in (
                "scope_partition_key",
                "dataset_version",
                "evaluator_id",
                "rubric_id",
                "rubric_version",
            ):
                self._ensure_column(
                    connection,
                    "evolution_evaluations",
                    column,
                    "TEXT NOT NULL DEFAULT ''",
                )
            self._ensure_column(
                connection, "evolution_promotions", "idempotency_key", "TEXT"
            )
            connection.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_evolution_promotions_idempotency
                   ON evolution_promotions(candidate_id, idempotency_key)
                   WHERE idempotency_key IS NOT NULL"""
            )

    @staticmethod
    def _ensure_column(
        connection: sqlite3.Connection, table: str, column: str, definition: str
    ) -> None:
        columns = {
            row["name"] for row in connection.execute(f"PRAGMA table_info({table})")
        }
        if column not in columns:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    async def register(self, candidate: EvolutionCandidate) -> str:
        return await asyncio.to_thread(self._register, candidate)

    def _register(self, candidate: EvolutionCandidate) -> str:
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO evolution_candidates
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    candidate.id,
                    json.dumps(to_jsonable(candidate.scope), sort_keys=True),
                    json.dumps(to_jsonable(candidate.procedure), sort_keys=True),
                    json.dumps(candidate.source_episode_ids),
                    candidate.state.value,
                    candidate.baseline_version,
                    candidate.generator,
                    candidate.generator_version,
                    candidate.created_at.isoformat(),
                    candidate.updated_at.isoformat(),
                ),
            )
        return candidate.id

    async def get(self, candidate_id: str) -> EvolutionCandidate:
        return await asyncio.to_thread(self._get, candidate_id)

    def _get(self, candidate_id: str) -> EvolutionCandidate:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM evolution_candidates WHERE id = ?", (candidate_id,)
            ).fetchone()
        if row is None:
            raise EvolutionNotFound(candidate_id)
        return self._candidate(row)

    async def append_evaluation(self, report: EvaluationReport) -> str:
        return await asyncio.to_thread(self._append_evaluation, report)

    def _append_evaluation(self, report: EvaluationReport) -> str:
        if report.passed is None:
            raise ValueError("policy decision must be attached before storing evaluation")
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO evolution_evaluations (
                     id, candidate_id, candidate_version, scope_partition_key,
                     stage, dataset_id, dataset_version, evaluator_id, evaluator_version,
                     rubric_id, rubric_version, sample_size, metrics_json, evidence_digest,
                     safety_violations, passed, gate_reasons_json, created_at
                   ) VALUES
                   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    report.id,
                    report.candidate_id,
                    report.candidate_version,
                    report.scope_partition_key,
                    report.stage.value,
                    report.dataset_id,
                    report.dataset_version,
                    report.evaluator_id,
                    report.evaluator_version,
                    report.rubric_id,
                    report.rubric_version,
                    report.sample_size,
                    json.dumps(dict(report.metrics), sort_keys=True),
                    report.evidence_digest,
                    report.safety_violations,
                    int(report.passed),
                    json.dumps(report.gate_reasons),
                    report.created_at.isoformat(),
                ),
            )
        return report.id

    async def latest_evaluation(
        self, candidate_id: str, stage: EvaluationStage
    ) -> EvaluationReport | None:
        return await asyncio.to_thread(self._latest_evaluation, candidate_id, stage)

    def _latest_evaluation(
        self, candidate_id: str, stage: EvaluationStage
    ) -> EvaluationReport | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM evolution_evaluations
                   WHERE candidate_id = ? AND stage = ? ORDER BY created_at DESC LIMIT 1""",
                (candidate_id, stage.value),
            ).fetchone()
        return self._evaluation(row) if row else None

    async def transition(
        self,
        candidate_id: str,
        expected: EvolutionState,
        target: EvolutionState,
        *,
        actor: str,
        reason: str,
        evaluation_id: str | None = None,
        approval: PromotionApproval | None = None,
        idempotency_key: str | None = None,
    ) -> PromotionRecord:
        if target not in ALLOWED_TRANSITIONS[expected]:
            raise ValueError(f"invalid evolution transition: {expected} -> {target}")
        record = PromotionRecord(
            candidate_id=candidate_id,
            from_state=expected,
            to_state=target,
            actor=actor,
            reason=reason,
            evaluation_id=evaluation_id,
            approval_ref=approval.approval_ref if approval else None,
            idempotency_key=idempotency_key,
        )
        await asyncio.to_thread(self._transition, record)
        return record

    def _transition(self, record: PromotionRecord) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """UPDATE evolution_candidates SET state = ?, updated_at = ?
                   WHERE id = ? AND state = ?""",
                (
                    record.to_state.value,
                    record.created_at.isoformat(),
                    record.candidate_id,
                    record.from_state.value,
                ),
            )
            if cursor.rowcount != 1:
                raise EvolutionConflict(
                    f"candidate {record.candidate_id} is no longer {record.from_state}"
                )
            connection.execute(
                """INSERT INTO evolution_promotions (
                     id, candidate_id, from_state, to_state, actor, reason,
                     evaluation_id, approval_ref, idempotency_key, created_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record.id,
                    record.candidate_id,
                    record.from_state.value,
                    record.to_state.value,
                    record.actor,
                    record.reason,
                    record.evaluation_id,
                    record.approval_ref,
                    record.idempotency_key,
                    record.created_at.isoformat(),
                ),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    async def history(self, candidate_id: str) -> Sequence[PromotionRecord]:
        return await asyncio.to_thread(self._history, candidate_id)

    async def promotion_by_idempotency_key(
        self, candidate_id: str, idempotency_key: str
    ) -> PromotionRecord | None:
        return await asyncio.to_thread(
            self._promotion_by_idempotency_key, candidate_id, idempotency_key
        )

    def _promotion_by_idempotency_key(
        self, candidate_id: str, idempotency_key: str
    ) -> PromotionRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM evolution_promotions
                   WHERE candidate_id = ? AND idempotency_key = ?""",
                (candidate_id, idempotency_key),
            ).fetchone()
        return self._promotion(row) if row else None

    async def candidates_in_states(
        self, states: Sequence[EvolutionState]
    ) -> Sequence[EvolutionCandidate]:
        return await asyncio.to_thread(self._candidates_in_states, states)

    def _candidates_in_states(
        self, states: Sequence[EvolutionState]
    ) -> Sequence[EvolutionCandidate]:
        if not states:
            return ()
        placeholders = ",".join("?" for _ in states)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM evolution_candidates WHERE state IN ({placeholders})",
                tuple(state.value for state in states),
            ).fetchall()
        return tuple(self._candidate(row) for row in rows)

    async def set_active_pointer(self, candidate: EvolutionCandidate) -> None:
        await asyncio.to_thread(self._set_active_pointer, candidate)

    def _set_active_pointer(self, candidate: EvolutionCandidate) -> None:
        if candidate.state != EvolutionState.ACTIVE:
            raise EvolutionConflict("active pointer requires an active candidate")
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO evolution_active_pointers VALUES (?, ?, ?, ?)
                   ON CONFLICT(scope_partition_key) DO UPDATE SET
                     candidate_id=excluded.candidate_id,
                     procedure_version=excluded.procedure_version,
                     updated_at=excluded.updated_at""",
                (
                    candidate.scope.partition_key(),
                    candidate.id,
                    candidate.procedure.version,
                    candidate.updated_at.isoformat(),
                ),
            )

    async def clear_active_pointer(
        self, scope: MemoryScope, candidate_id: str
    ) -> None:
        await asyncio.to_thread(self._clear_active_pointer, scope, candidate_id)

    def _clear_active_pointer(self, scope: MemoryScope, candidate_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """DELETE FROM evolution_active_pointers
                   WHERE scope_partition_key = ? AND candidate_id = ?""",
                (scope.partition_key(), candidate_id),
            )

    async def active_candidate_id(self, scope: MemoryScope) -> str | None:
        return await asyncio.to_thread(self._active_candidate_id, scope)

    def _active_candidate_id(self, scope: MemoryScope) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT candidate_id FROM evolution_active_pointers
                   WHERE scope_partition_key = ?""",
                (scope.partition_key(),),
            ).fetchone()
        return row["candidate_id"] if row else None

    async def invalidate_sources(
        self,
        source_event_ids: Sequence[str],
        *,
        actor: str,
        reason: str,
    ) -> Sequence[PromotionRecord]:
        return await asyncio.to_thread(
            self._invalidate_sources,
            set(source_event_ids),
            actor,
            reason,
        )

    def _invalidate_sources(
        self,
        source_event_ids: set[str],
        actor: str,
        reason: str,
    ) -> Sequence[PromotionRecord]:
        connection = self._connect()
        records: list[PromotionRecord] = []
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT * FROM evolution_candidates WHERE state != ?",
                (EvolutionState.INVALIDATED,),
            ).fetchall()
            for row in rows:
                candidate = self._candidate(row)
                if not (
                    set(candidate.procedure.provenance.source_event_ids) & source_event_ids
                ):
                    continue
                record = PromotionRecord(
                    candidate_id=candidate.id,
                    from_state=candidate.state,
                    to_state=EvolutionState.INVALIDATED,
                    actor=actor,
                    reason=reason,
                )
                cursor = connection.execute(
                    """
                    UPDATE evolution_candidates SET state = ?, updated_at = ?
                    WHERE id = ? AND state = ?
                    """,
                    (
                        EvolutionState.INVALIDATED,
                        record.created_at.isoformat(),
                        candidate.id,
                        candidate.state,
                    ),
                )
                if cursor.rowcount != 1:
                    raise EvolutionConflict(
                        f"candidate {candidate.id} changed during invalidation"
                    )
                connection.execute(
                    """INSERT INTO evolution_promotions (
                         id, candidate_id, from_state, to_state, actor, reason,
                         evaluation_id, approval_ref, idempotency_key, created_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        record.id,
                        record.candidate_id,
                        record.from_state,
                        record.to_state,
                        record.actor,
                        record.reason,
                        None,
                        None,
                        None,
                        record.created_at.isoformat(),
                    ),
                )
                records.append(record)
            connection.commit()
            return tuple(records)
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _history(self, candidate_id: str) -> Sequence[PromotionRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM evolution_promotions
                   WHERE candidate_id = ? ORDER BY created_at""",
                (candidate_id,),
            ).fetchall()
        return tuple(self._promotion(row) for row in rows)

    @staticmethod
    def _scope(data: dict[str, Any]) -> MemoryScope:
        return MemoryScope(**data)

    @classmethod
    def _procedure(cls, data: dict[str, Any]) -> Procedure:
        provenance = data["provenance"]
        return Procedure(
            id=data["id"],
            scope=cls._scope(data["scope"]),
            name=data["name"],
            trigger=data["trigger"],
            steps=tuple(data["steps"]),
            success_conditions=tuple(data["success_conditions"]),
            failure_patterns=tuple(data["failure_patterns"]),
            status=ArtifactStatus(data["status"]),
            provenance=Provenance(
                source_event_ids=tuple(provenance["source_event_ids"]),
                extractor=provenance["extractor"],
                provider=provenance["provider"],
                model=provenance["model"],
                prompt_version=provenance["prompt_version"],
                source_uri=provenance["source_uri"],
                created_at=datetime.fromisoformat(provenance["created_at"]),
            ),
            version=data["version"],
            created_at=datetime.fromisoformat(data["created_at"]),
        )

    @classmethod
    def _candidate(cls, row: sqlite3.Row) -> EvolutionCandidate:
        scope = cls._scope(json.loads(row["scope_json"]))
        return EvolutionCandidate(
            id=row["id"],
            scope=scope,
            procedure=cls._procedure(json.loads(row["procedure_json"])),
            source_episode_ids=tuple(json.loads(row["source_episode_ids_json"])),
            state=EvolutionState(row["state"]),
            baseline_version=row["baseline_version"],
            generator=row["generator"],
            generator_version=row["generator_version"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _evaluation(row: sqlite3.Row) -> EvaluationReport:
        return EvaluationReport(
            id=row["id"],
            candidate_id=row["candidate_id"],
            candidate_version=row["candidate_version"],
            scope_partition_key=row["scope_partition_key"],
            stage=EvaluationStage(row["stage"]),
            dataset_id=row["dataset_id"],
            dataset_version=row["dataset_version"],
            evaluator_id=row["evaluator_id"],
            evaluator_version=row["evaluator_version"],
            rubric_id=row["rubric_id"],
            rubric_version=row["rubric_version"],
            sample_size=row["sample_size"],
            metrics=json.loads(row["metrics_json"]),
            evidence_digest=row["evidence_digest"],
            safety_violations=row["safety_violations"],
            passed=bool(row["passed"]),
            gate_reasons=tuple(json.loads(row["gate_reasons_json"])),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _promotion(row: sqlite3.Row) -> PromotionRecord:
        return PromotionRecord(
            id=row["id"],
            candidate_id=row["candidate_id"],
            from_state=EvolutionState(row["from_state"]),
            to_state=EvolutionState(row["to_state"]),
            actor=row["actor"],
            reason=row["reason"],
            evaluation_id=row["evaluation_id"],
            approval_ref=row["approval_ref"],
            idempotency_key=row["idempotency_key"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )
