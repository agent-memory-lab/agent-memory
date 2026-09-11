from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from types import TracebackType
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from agent_memory.domain import (
    ArtifactStatus,
    Claim,
    ClaimStatus,
    DecisionRecord,
    Episode,
    EvaluationRecord,
    FeedbackStatus,
    ForgetMode,
    ForgetRequest,
    ForgetResult,
    MemoryBlock,
    MemoryChannel,
    MemoryEvent,
    MemoryItem,
    MemoryKind,
    MemoryProposal,
    MemoryQuery,
    MemoryScope,
    OutcomeEvent,
    Procedure,
    ProposalResult,
    ProposalStatus,
    Provenance,
    RetrievalTrace,
    RewardSignal,
    StateDelta,
    utc_now,
)
from agent_memory.serialization import to_jsonable


def _json(value: Any) -> str:
    return json.dumps(to_jsonable(value), ensure_ascii=False, separators=(",", ":"))


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, dict):
            return decoded
    raise TypeError("expected a JSON object")


def _provenance(value: Any) -> Provenance:
    payload = _object(value)
    payload["source_event_ids"] = tuple(payload.get("source_event_ids", ()))
    created_at = payload.get("created_at")
    if isinstance(created_at, str):
        payload["created_at"] = datetime.fromisoformat(created_at)
    return Provenance(**payload)


def _scope_values(scope: MemoryScope) -> tuple[str | None, ...]:
    return (
        scope.partition_key(),
        scope.tenant_id,
        scope.namespace,
        scope.user_id,
        scope.agent_id,
        scope.workspace_id,
        scope.session_id,
    )


class PostgresMemoryUnitOfWork:
    def __init__(self, repository: PostgresMemoryRepository) -> None:
        self._repository = repository
        self._connection_context: Any = None
        self._transaction_context: Any = None
        self.connection: Any = None

    async def __aenter__(self) -> PostgresMemoryUnitOfWork:
        self._connection_context = self._repository.pool.connection()
        self.connection = await self._connection_context.__aenter__()
        self._transaction_context = self.connection.transaction()
        await self._transaction_context.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self._transaction_context.__aexit__(exc_type, exc, traceback)
        finally:
            await self._connection_context.__aexit__(exc_type, exc, traceback)
            self.connection = None

    async def find_event_by_idempotency(
        self, scope: MemoryScope, idempotency_key: str
    ) -> MemoryEvent | None:
        partition_key = scope.partition_key()
        await self._lock_idempotency("event", partition_key, idempotency_key)
        cursor = await self.connection.execute(
            """
            SELECT * FROM agent_memory_events
            WHERE partition_key = %s AND idempotency_key = %s
            """,
            (partition_key, idempotency_key),
        )
        row = await cursor.fetchone()
        return self._repository._event_from_row(row) if row else None

    async def find_feedback_record(
        self, record_id: str, record_type: str
    ) -> dict[str, object] | None:
        await self._expire_pending_feedback()
        cursor = await self.connection.execute(
            """
            SELECT * FROM agent_memory_evolution_records
            WHERE id = %s AND record_type = %s
            """,
            (record_id, record_type),
        )
        row = await cursor.fetchone()
        return self._repository._feedback_from_row(row) if row else None

    async def find_feedback_by_idempotency(
        self, scope: MemoryScope, record_type: str, idempotency_key: str
    ) -> dict[str, object] | None:
        partition_key = scope.partition_key()
        await self._lock_idempotency(record_type, partition_key, idempotency_key)
        await self._expire_pending_feedback()
        cursor = await self.connection.execute(
            """
            SELECT * FROM agent_memory_evolution_records
            WHERE partition_key = %s AND record_type = %s AND idempotency_key = %s
            """,
            (partition_key, record_type, idempotency_key),
        )
        row = await cursor.fetchone()
        return self._repository._feedback_from_row(row) if row else None

    async def _lock_idempotency(
        self, record_type: str, partition_key: str, idempotency_key: str
    ) -> None:
        await self.connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"agent-memory:{record_type}:{partition_key}:{idempotency_key}",),
        )

    async def activate_pending_children(
        self, scope: MemoryScope, parent_id: str
    ) -> Sequence[str]:
        cursor = await self.connection.execute(
            """
            UPDATE agent_memory_evolution_records SET feedback_status = %s
            WHERE partition_key = %s AND parent_id = %s AND feedback_status = %s
              AND invalidated_at IS NULL
            RETURNING id
            """,
            (
                FeedbackStatus.ACCEPTED,
                scope.partition_key(),
                parent_id,
                FeedbackStatus.PENDING,
            ),
        )
        return tuple(row["id"] for row in await cursor.fetchall())

    async def supersede_feedback(self, scope: MemoryScope, record_id: str) -> None:
        await self.connection.execute(
            """
            UPDATE agent_memory_evolution_records SET feedback_status = %s
            WHERE partition_key = %s AND id = %s AND feedback_status != %s
            """,
            (
                FeedbackStatus.SUPERSEDED,
                scope.partition_key(),
                record_id,
                FeedbackStatus.INVALIDATED,
            ),
        )
        await self._repository._invalidate_feedback(
            self.connection,
            scope.partition_key(),
            {record_id},
            erase=False,
            all_in_scope=False,
        )
        await self.connection.execute(
            """
            UPDATE agent_memory_evolution_records
            SET feedback_status = %s, invalidated_at = NULL
            WHERE partition_key = %s AND id = %s
            """,
            (FeedbackStatus.SUPERSEDED, scope.partition_key(), record_id),
        )

    async def pending_feedback_count(self, scope: MemoryScope) -> int:
        await self._expire_pending_feedback()
        cursor = await self.connection.execute(
            """
            SELECT COUNT(*) AS count FROM agent_memory_evolution_records
            WHERE partition_key = %s AND feedback_status = %s
            """,
            (scope.partition_key(), FeedbackStatus.PENDING),
        )
        row = await cursor.fetchone()
        return int(row["count"] if row else 0)

    async def _expire_pending_feedback(self) -> None:
        await self.connection.execute(
            """
            UPDATE agent_memory_evolution_records SET feedback_status = %s
            WHERE feedback_status = %s AND expires_at IS NOT NULL AND expires_at <= now()
            """,
            (FeedbackStatus.EXPIRED, FeedbackStatus.PENDING),
        )

    async def claim_ids_for_event(self, event_id: str) -> Sequence[str]:
        cursor = await self.connection.execute(
            """
            SELECT id FROM agent_memory_claims
            WHERE provenance_json -> 'source_event_ids' ? %s
            ORDER BY created_at
            """,
            (event_id,),
        )
        return tuple(row["id"] for row in await cursor.fetchall())

    async def events_exist(self, scope: MemoryScope, event_ids: Sequence[str]) -> bool:
        if not event_ids:
            return False
        where, params = self._repository._visible_scope_clause(scope)
        cursor = await self.connection.execute(
            f"""
            SELECT count(DISTINCT id) AS count FROM agent_memory_events
            WHERE {where} AND archived_at IS NULL AND id = ANY(%s)
            """,
            (*params, list(event_ids)),
        )
        row = await cursor.fetchone()
        return bool(row and row["count"] == len(set(event_ids)))

    async def find_proposal_result(self, proposal_id: str) -> ProposalResult | None:
        cursor = await self.connection.execute(
            "SELECT * FROM agent_memory_proposals WHERE id = %s", (proposal_id,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return ProposalResult(
            proposal_id=row["id"],
            status=ProposalStatus(row["status"]),
            claim_id=row["claim_id"],
            state_delta_id=row["state_delta_id"],
            superseded_claim_id=row["superseded_claim_id"],
            reason=row["reason"],
        )

    async def append_event(self, event: MemoryEvent) -> None:
        await self.connection.execute(
            """
            INSERT INTO agent_memory_events (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, event_type, content, metadata_json,
                occurred_at, ingested_at, idempotency_key, actor, source_uri,
                sensitivity, retention_class, schema_version, content_hash
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                event.id,
                *_scope_values(event.scope),
                event.event_type,
                event.content,
                _json(event.metadata),
                event.occurred_at,
                event.ingested_at,
                event.idempotency_key,
                event.actor,
                event.source_uri,
                event.sensitivity,
                event.retention_class,
                event.schema_version,
                event.content_hash,
            ),
        )

    async def find_current_claim(self, scope: MemoryScope, key: str) -> Claim | None:
        partition_key = scope.partition_key()
        await self._lock_idempotency("claim", partition_key, key)
        cursor = await self.connection.execute(
            """
            SELECT * FROM agent_memory_claims
            WHERE partition_key = %s AND claim_key = %s
              AND status = 'active' AND archived_at IS NULL
            ORDER BY version DESC LIMIT 1
            FOR UPDATE
            """,
            (partition_key, key),
        )
        row = await cursor.fetchone()
        return self._repository._claim_from_row(row) if row else None

    async def save_claim(self, claim: Claim) -> None:
        await self._repository._insert_claim(self.connection, claim)

    async def replace_current_claim(self, previous: Claim, current: Claim) -> None:
        cursor = await self.connection.execute(
            """
            UPDATE agent_memory_claims
            SET status = 'superseded', valid_to = %s, superseded_by = %s
            WHERE id = %s AND status = 'active' AND version = %s
            """,
            (current.valid_from, current.id, previous.id, previous.version),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("claim update conflict")
        await self._repository._insert_claim(self.connection, current)

    async def add_claim_source(self, claim_id: str, event_id: str) -> None:
        cursor = await self.connection.execute(
            "SELECT provenance_json FROM agent_memory_claims WHERE id = %s FOR UPDATE",
            (claim_id,),
        )
        row = await cursor.fetchone()
        if not row:
            raise KeyError(f"claim not found: {claim_id}")
        provenance = _provenance(row["provenance_json"])
        if event_id in provenance.source_event_ids:
            return
        updated = Provenance(
            source_event_ids=(*provenance.source_event_ids, event_id),
            extractor=provenance.extractor,
            provider=provenance.provider,
            model=provenance.model,
            prompt_version=provenance.prompt_version,
            source_uri=provenance.source_uri,
            created_at=provenance.created_at,
        )
        await self.connection.execute(
            "UPDATE agent_memory_claims SET provenance_json = %s::jsonb WHERE id = %s",
            (_json(updated), claim_id),
        )

    async def save_state_delta(self, delta: StateDelta) -> None:
        await self.connection.execute(
            """
            INSERT INTO agent_memory_state_deltas (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, claim_key, operation, source_event_id,
                current_claim_id, previous_claim_id, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                delta.id,
                *_scope_values(delta.scope),
                delta.key,
                delta.operation,
                delta.source_event_id,
                delta.current_claim_id,
                delta.previous_claim_id,
                delta.created_at,
            ),
        )

    async def save_episode(self, episode: Episode) -> None:
        text = "\n".join(
            (
                f"Observation: {episode.observation}",
                f"Action: {episode.action}",
                f"Outcome: {episode.outcome}",
                f"Lesson: {episode.lesson}",
            )
        )
        await self._repository._insert_artifact(
            self.connection,
            episode.id,
            episode.scope,
            MemoryKind.EPISODE,
            text,
            episode,
            episode.status,
            episode.version,
            episode.quality,
            episode.provenance,
            episode.occurred_at,
        )

    async def save_procedure(self, procedure: Procedure) -> None:
        text = "\n".join((procedure.name, procedure.trigger, *procedure.steps))
        quality = 1.0 if procedure.status == ArtifactStatus.ACTIVE else 0.5
        await self._repository._insert_artifact(
            self.connection,
            procedure.id,
            procedure.scope,
            MemoryKind.PROCEDURE,
            text,
            procedure,
            procedure.status,
            procedure.version,
            quality,
            procedure.provenance,
            procedure.created_at,
        )

    async def save_block(self, block: MemoryBlock, expected_version: int) -> MemoryBlock:
        cursor = await self.connection.execute(
            "SELECT * FROM agent_memory_artifacts WHERE id = %s FOR UPDATE",
            (block.id,),
        )
        row = await cursor.fetchone()
        now = utc_now()
        if row is None:
            if expected_version != 0:
                raise RuntimeError(
                    f"memory block version conflict: expected {expected_version}, current 0"
                )
            stored = replace(block, version=1, created_at=now, updated_at=now)
            await self._repository._insert_artifact(
                self.connection,
                stored.id,
                stored.scope,
                MemoryKind.BLOCK,
                f"{stored.title}\n{stored.content}",
                stored,
                stored.status,
                stored.version,
                1.0,
                stored.provenance,
                stored.updated_at,
            )
            return stored

        if (
            MemoryKind(row["kind"]) != MemoryKind.BLOCK
            or row["partition_key"] != block.scope.partition_key()
            or row["archived_at"] is not None
        ):
            raise RuntimeError("memory block id conflicts with an existing memory artifact")
        current = self._repository._block_from_row(row)
        if current.version != expected_version:
            raise RuntimeError(
                f"memory block version conflict: expected {expected_version}, "
                f"current {current.version}"
            )
        stored = replace(
            block,
            version=current.version + 1,
            created_at=current.created_at,
            updated_at=now,
        )
        cursor = await self.connection.execute(
            """
            UPDATE agent_memory_artifacts
            SET text = %s, payload_json = %s::jsonb, status = %s, version = %s,
                quality = %s, provenance_json = %s::jsonb, occurred_at = %s
            WHERE id = %s AND partition_key = %s AND kind = %s AND version = %s
              AND archived_at IS NULL
            """,
            (
                f"{stored.title}\n{stored.content}",
                _json(stored),
                str(stored.status),
                stored.version,
                1.0,
                _json(stored.provenance),
                stored.updated_at,
                stored.id,
                stored.scope.partition_key(),
                str(MemoryKind.BLOCK),
                expected_version,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("memory block update conflict")
        return stored

    async def save_decision(self, decision: DecisionRecord) -> None:
        await self._repository._insert_evolution_record(
            self.connection, decision.id, decision.scope, "decision", decision, decision.created_at
        )

    async def save_outcome(self, outcome: OutcomeEvent) -> None:
        await self._repository._insert_evolution_record(
            self.connection, outcome.id, outcome.scope, "outcome", outcome, outcome.occurred_at
        )

    async def save_evaluation(self, evaluation: EvaluationRecord) -> None:
        await self._repository._insert_evolution_record(
            self.connection,
            evaluation.id,
            evaluation.scope,
            "evaluation",
            evaluation,
            evaluation.created_at,
        )

    async def save_reward(self, reward: RewardSignal) -> None:
        await self._repository._insert_evolution_record(
            self.connection, reward.id, reward.scope, "reward", reward, reward.created_at
        )

    async def save_retrieval_trace(self, trace: RetrievalTrace) -> None:
        await self._repository._insert_evolution_record(
            self.connection,
            trace.id,
            trace.scope,
            "retrieval",
            trace,
            trace.created_at,
        )

    async def save_proposal(self, proposal: MemoryProposal, result: ProposalResult) -> None:
        await self.connection.execute(
            """
            INSERT INTO agent_memory_proposals (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, payload_json, status, claim_id,
                state_delta_id, superseded_claim_id, reason, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                proposal.id,
                *_scope_values(proposal.scope),
                _json(proposal),
                str(result.status),
                result.claim_id,
                result.state_delta_id,
                result.superseded_claim_id,
                result.reason,
                proposal.created_at,
            ),
        )


class PostgresMemoryRepository:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        *,
        migrations_path: str | Path | None = None,
    ) -> None:
        self.pool = pool
        packaged = Path(__file__).resolve().parent / "migrations"
        source = Path(__file__).resolve().parents[2] / "migrations"
        self._migrations_path = (
            Path(migrations_path)
            if migrations_path
            else (packaged if packaged.exists() else source)
        )

    @classmethod
    def from_dsn(
        cls,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        migrations_path: str | Path | None = None,
    ) -> PostgresMemoryRepository:
        pool = AsyncConnectionPool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row},
        )
        return cls(pool, migrations_path=migrations_path)

    async def initialize(self) -> None:
        await self.pool.open()
        async with self.pool.connection() as connection:
            async with connection.transaction():
                for migration in sorted(self._migrations_path.glob("*.sql")):
                    if migration.name == "002_pgvector.sql":
                        continue
                    await connection.execute(
                        migration.read_text(encoding="utf-8"), prepare=False
                    )

    async def close(self) -> None:
        await self.pool.close()

    def unit_of_work(self) -> PostgresMemoryUnitOfWork:
        return PostgresMemoryUnitOfWork(self)

    async def current_claims(self, scope: MemoryScope) -> Sequence[Claim]:
        where, params = self._visible_scope_clause(scope)
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                f"""
                SELECT * FROM agent_memory_claims
                WHERE {where} AND status = 'active' AND archived_at IS NULL
                ORDER BY importance DESC, confidence DESC, created_at DESC
                """,
                params,
            )
            return tuple(self._claim_from_row(row) for row in await cursor.fetchall())

    async def list_feedback(
        self,
        scope: MemoryScope,
        record_type: str,
        limit: int,
        after_id: str | None = None,
    ) -> Sequence[dict[str, object]]:
        partition_key = scope.partition_key()
        async with self.pool.connection() as connection:
            cursor_values: tuple[object, ...] = ()
            cursor_clause = ""
            if after_id:
                anchor_cursor = await connection.execute(
                    """
                    SELECT occurred_at, id FROM agent_memory_evolution_records
                    WHERE partition_key = %s AND record_type = %s AND id = %s
                    """,
                    (partition_key, record_type, after_id),
                )
                anchor = await anchor_cursor.fetchone()
                if anchor is None:
                    raise ValueError("feedback pagination cursor is invalid")
                cursor_clause = (
                    "AND (occurred_at < %s OR (occurred_at = %s AND id < %s))"
                )
                cursor_values = (
                    anchor["occurred_at"],
                    anchor["occurred_at"],
                    anchor["id"],
                )
            cursor = await connection.execute(
                f"""
                SELECT * FROM agent_memory_evolution_records
                WHERE partition_key = %s AND record_type = %s {cursor_clause}
                ORDER BY occurred_at DESC, id DESC LIMIT %s
                """,
                (partition_key, record_type, *cursor_values, limit),
            )
            rows = await cursor.fetchall()
        return tuple(self._feedback_from_row(row) for row in rows)

    async def search(self, query: MemoryQuery, limit: int) -> Sequence[MemoryItem]:
        where, params = self._visible_scope_clause(query.scope)
        text = query.text.strip()
        candidates: list[MemoryItem] = []
        async with self.pool.connection() as connection:
            if MemoryChannel.SEMANTIC in query.channels:
                claims = await self._search_rows(
                    connection,
                    "agent_memory_claims",
                    where + " AND status = 'active' AND archived_at IS NULL",
                    params,
                    text,
                    limit,
                )
                for row in claims:
                    provenance = _provenance(row["provenance_json"])
                    candidates.append(
                        MemoryItem(
                            id=row["id"],
                            kind=MemoryKind.CLAIM,
                            text=row["text"],
                            score=float(row["rank"])
                            + 0.25 * row["importance"]
                            + 0.20 * row["confidence"],
                            occurred_at=row["created_at"],
                            metadata={
                                "channel": MemoryChannel.SEMANTIC,
                                "key": row["claim_key"],
                                "source_event_ids": provenance.source_event_ids,
                            },
                        )
                    )
                events = await self._search_rows(
                    connection,
                    "agent_memory_events",
                    where + " AND archived_at IS NULL",
                    params,
                    text,
                    limit,
                )
                for row in events:
                    candidates.append(
                        MemoryItem(
                            id=row["id"],
                            kind=MemoryKind.EVENT,
                            text=row["content"],
                            score=float(row["rank"]),
                            occurred_at=row["occurred_at"],
                            metadata={
                                "channel": MemoryChannel.SEMANTIC,
                                "event_type": row["event_type"],
                                "source_event_ids": (row["id"],),
                            },
                        )
                    )

            artifact_kinds: list[str] = []
            if MemoryChannel.EPISODIC in query.channels:
                artifact_kinds.append(str(MemoryKind.EPISODE))
            if MemoryChannel.PROCEDURAL in query.channels:
                artifact_kinds.append(str(MemoryKind.PROCEDURE))
            if query.channels:
                artifact_kinds.append(str(MemoryKind.BLOCK))
            if artifact_kinds:
                artifacts = await self._search_rows(
                    connection,
                    "agent_memory_artifacts",
                    where + " AND archived_at IS NULL AND kind = ANY(%s)",
                    (*params, artifact_kinds),
                    text,
                    limit,
                )
                for row in artifacts:
                    kind = MemoryKind(row["kind"])
                    provenance = _provenance(row["provenance_json"])
                    if kind == MemoryKind.BLOCK:
                        block = self._block_from_row(row)
                        channel = block.channel
                        if channel not in query.channels:
                            continue
                    else:
                        channel = (
                            MemoryChannel.EPISODIC
                            if kind == MemoryKind.EPISODE
                            else MemoryChannel.PROCEDURAL
                        )
                    candidates.append(
                        MemoryItem(
                            id=row["id"],
                            kind=kind,
                            text=row["text"],
                            score=float(row["rank"]) + 0.25 * row["quality"],
                            occurred_at=row["occurred_at"],
                            metadata={
                                "channel": channel,
                                "status": row["status"],
                                "version": row["version"],
                                "token_budget": (
                                    block.token_budget if kind == MemoryKind.BLOCK else None
                                ),
                                "source_event_ids": provenance.source_event_ids,
                            },
                        )
                    )
        return tuple(sorted(candidates, key=lambda item: item.score, reverse=True)[:limit])

    async def read_block(self, scope: MemoryScope, block_id: str) -> MemoryBlock | None:
        where, params = self._visible_scope_clause(scope)
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                f"SELECT * FROM agent_memory_artifacts WHERE {where} AND id = %s "
                "AND kind = %s AND archived_at IS NULL",
                (*params, block_id, str(MemoryKind.BLOCK)),
            )
            row = await cursor.fetchone()
        return self._block_from_row(row) if row else None

    async def search_blocks(
        self,
        scope: MemoryScope,
        text: str,
        channels: Sequence[MemoryChannel],
        limit: int,
    ) -> Sequence[MemoryBlock]:
        where, params = self._visible_scope_clause(scope)
        async with self.pool.connection() as connection:
            rows = await self._search_rows(
                connection,
                "agent_memory_artifacts",
                where + " AND kind = %s AND status = 'active' AND archived_at IS NULL",
                (*params, str(MemoryKind.BLOCK)),
                text.strip(),
                min(limit * 4, 500),
            )
        allowed = set(channels)
        return tuple(
            block
            for block in (self._block_from_row(row) for row in rows)
            if block.channel in allowed
        )[:limit]

    async def forget(self, request: ForgetRequest) -> ForgetResult:
        table_names = (
            "agent_memory_events",
            "agent_memory_claims",
            "agent_memory_artifacts",
        )
        async with self.pool.connection() as connection:
            async with connection.transaction():
                if request.all_in_scope:
                    where = "partition_key = %s"
                    params: tuple[Any, ...] = (request.scope.partition_key(),)
                else:
                    where = "partition_key = %s AND id = ANY(%s)"
                    params = (request.scope.partition_key(), list(request.memory_ids))
                counts: dict[str, int] = {}
                for table in table_names:
                    cursor = await connection.execute(
                        f"SELECT count(*) AS count FROM {table} WHERE {where}", params
                    )
                    counts[table] = (await cursor.fetchone())["count"]

                impacted_memory_ids = set(request.memory_ids)
                if request.all_in_scope:
                    target_event_ids: set[str] = set()
                else:
                    cursor = await connection.execute(
                        """
                        SELECT id FROM agent_memory_events
                        WHERE partition_key = %s AND id = ANY(%s)
                        """,
                        (request.scope.partition_key(), list(request.memory_ids)),
                    )
                    target_event_ids = {row["id"] for row in await cursor.fetchall()}
                    if target_event_ids:
                        for table in ("agent_memory_claims", "agent_memory_artifacts"):
                            cursor = await connection.execute(
                                f"""
                                SELECT id FROM {table}
                                WHERE partition_key = %s
                                  AND provenance_json -> 'source_event_ids' ?| %s
                                """,
                                (request.scope.partition_key(), list(target_event_ids)),
                            )
                            impacted_memory_ids.update(
                                row["id"] for row in await cursor.fetchall()
                            )
                impacted_memory_ids.update(target_event_ids)

                changed_block_ids: set[str] = set()
                blocks_to_drop: set[str] = set()
                if not request.all_in_scope:
                    cursor = await connection.execute(
                        f"SELECT id FROM agent_memory_events WHERE {where}", params
                    )
                    target_event_ids = {row["id"] for row in await cursor.fetchall()}
                    if target_event_ids:
                        cursor = await connection.execute(
                            """
                            SELECT * FROM agent_memory_artifacts
                            WHERE partition_key = %s AND kind = %s AND archived_at IS NULL
                            FOR UPDATE
                            """,
                            (request.scope.partition_key(), str(MemoryKind.BLOCK)),
                        )
                        for row in await cursor.fetchall():
                            block = self._block_from_row(row)
                            remaining_event_ids = tuple(
                                event_id
                                for event_id in block.event_ids
                                if event_id not in target_event_ids
                            )
                            if len(remaining_event_ids) == len(block.event_ids):
                                continue
                            changed_block_ids.add(block.id)
                            if not remaining_event_ids:
                                blocks_to_drop.add(block.id)
                                continue
                            updated_at = utc_now()
                            provenance = replace(
                                block.provenance,
                                source_event_ids=remaining_event_ids,
                            )
                            updated_block = replace(
                                block,
                                event_ids=remaining_event_ids,
                                provenance=provenance,
                                version=block.version + 1,
                                updated_at=updated_at,
                            )
                            await connection.execute(
                                """
                                UPDATE agent_memory_artifacts
                                SET payload_json = %s::jsonb, provenance_json = %s::jsonb,
                                    version = %s, occurred_at = %s
                                WHERE id = %s AND version = %s AND archived_at IS NULL
                                """,
                                (
                                    _json(updated_block),
                                    _json(provenance),
                                    updated_block.version,
                                    updated_at,
                                    block.id,
                                    block.version,
                                ),
                            )

                if blocks_to_drop:
                    if request.mode == ForgetMode.ARCHIVE:
                        await connection.execute(
                            """
                            UPDATE agent_memory_artifacts
                            SET archived_at = now(), status = 'archived'
                            WHERE id = ANY(%s)
                            """,
                            (list(blocks_to_drop),),
                        )
                    else:
                        await connection.execute(
                            "DELETE FROM agent_memory_artifacts WHERE id = ANY(%s)",
                            (list(blocks_to_drop),),
                        )

                if request.mode == ForgetMode.ARCHIVE:
                    await connection.execute(
                        f"UPDATE agent_memory_events SET archived_at = now() WHERE {where}", params
                    )
                    await connection.execute(
                        f"""
                        UPDATE agent_memory_claims
                        SET archived_at = now(), status = 'archived' WHERE {where}
                        """,
                        params,
                    )
                    await connection.execute(
                        f"""
                        UPDATE agent_memory_artifacts
                        SET archived_at = now(), status = 'archived' WHERE {where}
                        """,
                        params,
                    )
                else:
                    await connection.execute(
                        f"DELETE FROM agent_memory_artifacts WHERE {where}", params
                    )
                    await connection.execute(
                        f"DELETE FROM agent_memory_claims WHERE {where}", params
                    )
                    await connection.execute(
                        f"DELETE FROM agent_memory_events WHERE {where}", params
                    )
                await self._invalidate_feedback(
                    connection,
                    request.scope.partition_key(),
                    impacted_memory_ids,
                    erase=request.mode == ForgetMode.ERASE,
                    all_in_scope=request.all_in_scope,
                )
                return ForgetResult(
                    affected_events=counts["agent_memory_events"],
                    affected_claims=counts["agent_memory_claims"],
                    affected_artifacts=(
                        counts["agent_memory_artifacts"]
                        + len(changed_block_ids - set(request.memory_ids))
                    ),
                    mode=request.mode,
                )

    @staticmethod
    async def _invalidate_feedback(
        connection: Any,
        partition_key: str,
        impacted_ids: set[str],
        *,
        erase: bool,
        all_in_scope: bool,
    ) -> None:
        cursor = await connection.execute(
            """
            SELECT * FROM agent_memory_evolution_records
            WHERE partition_key = %s AND invalidated_at IS NULL
            ORDER BY occurred_at, id
            FOR UPDATE
            """,
            (partition_key,),
        )
        rows = await cursor.fetchall()
        invalidated = (
            {row["id"] for row in rows}
            if all_in_scope
            else {row["id"] for row in rows if row["id"] in impacted_ids}
        )
        changed = True
        while changed:
            changed = False
            known_impacts = impacted_ids | invalidated
            for row in rows:
                if row["id"] in invalidated:
                    continue
                payload = _object(row["payload_json"])
                references: set[str] = set()
                for field in (
                    "returned_memory_ids",
                    "memory_ids",
                    "procedure_ids",
                    "source_event_ids",
                ):
                    value = payload.get(field, ())
                    if isinstance(value, (list, tuple)):
                        references.update(map(str, value))
                for field in (
                    "bundle_id",
                    "decision_id",
                    "outcome_id",
                    "evaluation_id",
                ):
                    value = payload.get(field)
                    if value:
                        references.add(str(value))
                if row["parent_id"]:
                    references.add(str(row["parent_id"]))
                if references & known_impacts:
                    invalidated.add(row["id"])
                    changed = True

        for row in rows:
            if row["id"] not in invalidated:
                continue
            if erase:
                payload_json = _json(
                    {
                        "id": row["id"],
                        "record_type": row["record_type"],
                        "redacted": True,
                        "reason": "source_erased",
                    }
                )
                await connection.execute(
                    """
                    UPDATE agent_memory_evolution_records
                    SET feedback_status = %s, invalidated_at = now(),
                        payload_json = %s::jsonb, payload_hash = %s
                    WHERE id = %s
                    """,
                    (
                        FeedbackStatus.INVALIDATED,
                        payload_json,
                        sha256(payload_json.encode("utf-8")).hexdigest(),
                        row["id"],
                    ),
                )
            else:
                await connection.execute(
                    """
                    UPDATE agent_memory_evolution_records
                    SET feedback_status = %s, invalidated_at = now() WHERE id = %s
                    """,
                    (FeedbackStatus.INVALIDATED, row["id"]),
                )

    async def _insert_claim(self, connection: Any, claim: Claim) -> None:
        await connection.execute(
            """
            INSERT INTO agent_memory_claims (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, claim_key, value_json, text,
                confidence, importance, status, provenance_json, valid_from,
                valid_to, created_at, version, supersedes, superseded_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s,
                %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                claim.id,
                *_scope_values(claim.scope),
                claim.key,
                _json(claim.value),
                claim.text,
                claim.confidence,
                claim.importance,
                str(claim.status),
                _json(claim.provenance),
                claim.valid_from,
                claim.valid_to,
                claim.created_at,
                claim.version,
                claim.supersedes,
                claim.superseded_by,
            ),
        )

    async def _insert_artifact(
        self,
        connection: Any,
        artifact_id: str,
        scope: MemoryScope,
        kind: MemoryKind,
        text: str,
        payload: Any,
        status: ArtifactStatus,
        version: int,
        quality: float,
        provenance: Provenance,
        occurred_at: datetime,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO agent_memory_artifacts (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, kind, text, payload_json, status,
                version, quality, provenance_json, occurred_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                %s, %s, %s, %s::jsonb, %s
            )
            """,
            (
                artifact_id,
                *_scope_values(scope),
                str(kind),
                text,
                _json(payload),
                str(status),
                version,
                quality,
                _json(provenance),
                occurred_at,
            ),
        )

    async def _insert_evolution_record(
        self,
        connection: Any,
        record_id: str,
        scope: MemoryScope,
        record_type: str,
        payload: Any,
        occurred_at: datetime,
    ) -> None:
        payload_json = _json(payload)
        payload_object = _object(payload_json)
        parent_id = None
        if record_type == "outcome":
            parent_id = payload_object.get("decision_id")
        elif record_type == "evaluation":
            parent_id = payload_object.get("outcome_id")
        elif record_type == "reward":
            parent_id = payload_object.get("evaluation_id") or payload_object.get("outcome_id")
        await connection.execute(
            """
            INSERT INTO agent_memory_evolution_records (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, record_type, payload_json, feedback_status,
                parent_id, idempotency_key, payload_hash, corrects_id, expires_at, occurred_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                record_id,
                *_scope_values(scope),
                record_type,
                payload_json,
                payload_object.get("feedback_status", FeedbackStatus.ACCEPTED),
                parent_id,
                payload_object.get("idempotency_key"),
                sha256(payload_json.encode("utf-8")).hexdigest(),
                payload_object.get("corrects_id"),
                payload_object.get("expires_at"),
                occurred_at,
            ),
        )

    @staticmethod
    def _feedback_from_row(row: dict[str, Any]) -> dict[str, object]:
        return {
            "id": row["id"],
            "partition_key": row["partition_key"],
            "record_type": row["record_type"],
            "payload": _object(row["payload_json"]),
            "feedback_status": row["feedback_status"],
            "parent_id": row["parent_id"],
            "idempotency_key": row["idempotency_key"],
            "payload_hash": row["payload_hash"],
            "corrects_id": row["corrects_id"],
            "expires_at": row["expires_at"],
            "invalidated_at": row["invalidated_at"],
        }

    @staticmethod
    async def _search_rows(
        connection: Any,
        table: str,
        where: str,
        where_params: Sequence[Any],
        text: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if text:
            cursor = await connection.execute(
                f"""
                SELECT *, ts_rank(search_document, plainto_tsquery('simple', %s)) AS rank
                FROM {table}
                WHERE {where} AND search_document @@ plainto_tsquery('simple', %s)
                ORDER BY rank DESC LIMIT %s
                """,
                (text, *where_params, text, limit),
            )
        else:
            cursor = await connection.execute(
                f"SELECT *, 0.0 AS rank FROM {table} WHERE {where} LIMIT %s",
                (*where_params, limit),
            )
        return list(await cursor.fetchall())

    @staticmethod
    def _visible_scope_clause(scope: MemoryScope) -> tuple[str, tuple[Any, ...]]:
        clauses = ["tenant_id = %s", "namespace = %s"]
        params: list[Any] = [scope.tenant_id, scope.namespace]
        for column, value in (
            ("user_id", scope.user_id),
            ("agent_id", scope.agent_id),
            ("workspace_id", scope.workspace_id),
            ("session_id", scope.session_id),
        ):
            clauses.append(f"({column} IS NULL OR {column} = %s)")
            params.append(value)
        return " AND ".join(clauses), tuple(params)

    @staticmethod
    def _scope_from_row(row: dict[str, Any]) -> MemoryScope:
        return MemoryScope(
            tenant_id=row["tenant_id"],
            namespace=row["namespace"],
            user_id=row["user_id"],
            agent_id=row["agent_id"],
            workspace_id=row["workspace_id"],
            session_id=row["session_id"],
        )

    def _event_from_row(self, row: dict[str, Any]) -> MemoryEvent:
        return MemoryEvent(
            id=row["id"],
            scope=self._scope_from_row(row),
            event_type=row["event_type"],
            content=row["content"],
            metadata=_object(row["metadata_json"]),
            occurred_at=row["occurred_at"],
            ingested_at=row["ingested_at"],
            idempotency_key=row["idempotency_key"],
            actor=row["actor"],
            source_uri=row["source_uri"],
            sensitivity=row["sensitivity"],
            retention_class=row["retention_class"],
            schema_version=row["schema_version"],
            content_hash=row["content_hash"],
        )

    def _claim_from_row(self, row: dict[str, Any]) -> Claim:
        return Claim(
            id=row["id"],
            scope=self._scope_from_row(row),
            key=row["claim_key"],
            value=row["value_json"],
            text=row["text"],
            confidence=row["confidence"],
            importance=row["importance"],
            status=ClaimStatus(row["status"]),
            provenance=_provenance(row["provenance_json"]),
            valid_from=row["valid_from"],
            valid_to=row["valid_to"],
            created_at=row["created_at"],
            version=row["version"],
            supersedes=row["supersedes"],
            superseded_by=row["superseded_by"],
        )

    def _block_from_row(self, row: dict[str, Any]) -> MemoryBlock:
        payload = _object(row["payload_json"])
        provenance = _provenance(row["provenance_json"])

        def timestamp(key: str, fallback: datetime) -> datetime:
            value = payload.get(key)
            return datetime.fromisoformat(value) if isinstance(value, str) else fallback

        return MemoryBlock(
            id=row["id"],
            scope=self._scope_from_row(row),
            title=str(payload["title"]),
            content=str(payload["content"]),
            event_ids=tuple(payload.get("event_ids", provenance.source_event_ids)),
            channel=MemoryChannel(payload.get("channel", MemoryChannel.SEMANTIC)),
            token_budget=int(payload.get("token_budget", 256)),
            status=ArtifactStatus(row["status"]),
            metadata=payload.get("metadata", {}),
            provenance=provenance,
            version=int(row["version"]),
            created_at=timestamp("created_at", row["occurred_at"]),
            updated_at=timestamp("updated_at", row["occurred_at"]),
        )
