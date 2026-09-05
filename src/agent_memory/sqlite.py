from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Any, Sequence

from .domain import (
    ArtifactStatus,
    Claim,
    ClaimStatus,
    DecisionRecord,
    Episode,
    ForgetMode,
    ForgetRequest,
    ForgetResult,
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
    RewardSignal,
    StateDelta,
    canonical_json,
    utc_now,
)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _tokens(text: str) -> set[str]:
    latin = {token.lower() for token in re.findall(r"[A-Za-z0-9_-]+", text) if len(token) > 1}
    cjk = re.findall(r"[\u3400-\u9fff]", text)
    return latin | set(cjk) | {"".join(cjk[index : index + 2]) for index in range(len(cjk) - 1)}


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


def _provenance_json(provenance: Provenance) -> str:
    payload = asdict(provenance)
    payload["created_at"] = _iso(provenance.created_at)
    return canonical_json(payload)


def _provenance(value: str) -> Provenance:
    payload = json.loads(value)
    payload["source_event_ids"] = tuple(payload.get("source_event_ids", ()))
    payload["created_at"] = _datetime(payload.get("created_at")) or utc_now()
    return Provenance(**payload)


class SQLiteMemoryUnitOfWork:
    def __init__(self, repository: SQLiteMemoryRepository) -> None:
        self._repository = repository
        self._connection: sqlite3.Connection | None = None

    async def __aenter__(self) -> SQLiteMemoryUnitOfWork:
        await self._repository._write_lock.acquire()
        try:
            self._connection = self._repository._connect()
            self._connection.execute("BEGIN IMMEDIATE")
            return self
        except BaseException:
            self._repository._write_lock.release()
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        assert self._connection is not None
        try:
            if exc_type is None:
                self._connection.commit()
            else:
                self._connection.rollback()
        finally:
            self._connection.close()
            self._connection = None
            self._repository._write_lock.release()

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("Unit of Work is not active")
        return self._connection

    async def find_event_by_idempotency(
        self, scope: MemoryScope, idempotency_key: str
    ) -> MemoryEvent | None:
        row = self.connection.execute(
            "SELECT * FROM events WHERE partition_key = ? AND idempotency_key = ?",
            (scope.partition_key(), idempotency_key),
        ).fetchone()
        return self._repository._event_from_row(row) if row else None

    async def claim_ids_for_event(self, event_id: str) -> Sequence[str]:
        rows = self.connection.execute(
            "SELECT id, provenance_json FROM claims ORDER BY created_at"
        ).fetchall()
        return tuple(
            row["id"]
            for row in rows
            if event_id in _provenance(row["provenance_json"]).source_event_ids
        )

    async def events_exist(self, scope: MemoryScope, event_ids: Sequence[str]) -> bool:
        if not event_ids:
            return False
        where, params = self._repository._visible_scope_clause(scope)
        placeholders = ",".join("?" for _ in event_ids)
        row = self.connection.execute(
            f"""
            SELECT COUNT(DISTINCT id) AS count FROM events
            WHERE {where} AND archived_at IS NULL AND id IN ({placeholders})
            """,
            (*params, *event_ids),
        ).fetchone()
        return bool(row and row["count"] == len(set(event_ids)))

    async def find_proposal_result(self, proposal_id: str) -> ProposalResult | None:
        row = self.connection.execute(
            "SELECT * FROM proposals WHERE id = ?", (proposal_id,)
        ).fetchone()
        if row is None:
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
        self.connection.execute(
            """
            INSERT INTO events (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, event_type, content, metadata_json,
                occurred_at, ingested_at, idempotency_key, actor, source_uri,
                sensitivity, retention_class, schema_version, content_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.id,
                *_scope_values(event.scope),
                event.event_type,
                event.content,
                canonical_json(event.metadata),
                _iso(event.occurred_at),
                _iso(event.ingested_at),
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
        row = self.connection.execute(
            """
            SELECT * FROM claims
            WHERE partition_key = ? AND claim_key = ? AND status = ? AND archived_at IS NULL
            ORDER BY version DESC LIMIT 1
            """,
            (scope.partition_key(), key, ClaimStatus.ACTIVE),
        ).fetchone()
        return self._repository._claim_from_row(row) if row else None

    async def save_claim(self, claim: Claim) -> None:
        self._repository._insert_claim(self.connection, claim)

    async def replace_current_claim(self, previous: Claim, current: Claim) -> None:
        cursor = self.connection.execute(
            """
            UPDATE claims SET status = ?, valid_to = ?, superseded_by = ?
            WHERE id = ? AND status = ? AND version = ?
            """,
            (
                ClaimStatus.SUPERSEDED,
                _iso(current.valid_from),
                current.id,
                previous.id,
                ClaimStatus.ACTIVE,
                previous.version,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("claim update conflict")
        self._repository._insert_claim(self.connection, current)

    async def add_claim_source(self, claim_id: str, event_id: str) -> None:
        row = self.connection.execute(
            "SELECT provenance_json FROM claims WHERE id = ?", (claim_id,)
        ).fetchone()
        if row is None:
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
        self.connection.execute(
            "UPDATE claims SET provenance_json = ? WHERE id = ?",
            (_provenance_json(updated), claim_id),
        )

    async def save_state_delta(self, delta: StateDelta) -> None:
        self.connection.execute(
            """
            INSERT INTO state_deltas (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, claim_key, operation, source_event_id,
                current_claim_id, previous_claim_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delta.id,
                *_scope_values(delta.scope),
                delta.key,
                delta.operation,
                delta.source_event_id,
                delta.current_claim_id,
                delta.previous_claim_id,
                _iso(delta.created_at),
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
        self._repository._insert_artifact(
            self.connection,
            episode.id,
            episode.scope,
            MemoryKind.EPISODE,
            text,
            asdict(episode),
            episode.status,
            episode.version,
            episode.quality,
            episode.provenance,
            episode.occurred_at,
        )

    async def save_procedure(self, procedure: Procedure) -> None:
        text = "\n".join((procedure.name, procedure.trigger, *procedure.steps))
        self._repository._insert_artifact(
            self.connection,
            procedure.id,
            procedure.scope,
            MemoryKind.PROCEDURE,
            text,
            asdict(procedure),
            procedure.status,
            procedure.version,
            1.0 if procedure.status == ArtifactStatus.ACTIVE else 0.5,
            procedure.provenance,
            procedure.created_at,
        )

    async def save_decision(self, decision: DecisionRecord) -> None:
        self._repository._insert_evolution_record(
            self.connection, decision.id, decision.scope, "decision", asdict(decision), decision.created_at
        )

    async def save_outcome(self, outcome: OutcomeEvent) -> None:
        self._repository._insert_evolution_record(
            self.connection, outcome.id, outcome.scope, "outcome", asdict(outcome), outcome.occurred_at
        )

    async def save_reward(self, reward: RewardSignal) -> None:
        self._repository._insert_evolution_record(
            self.connection, reward.id, reward.scope, "reward", asdict(reward), reward.created_at
        )

    async def save_proposal(
        self, proposal: MemoryProposal, result: ProposalResult
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO proposals (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, payload_json, status, claim_id,
                state_delta_id, superseded_claim_id, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                proposal.id,
                *_scope_values(proposal.scope),
                canonical_json(asdict(proposal)),
                result.status,
                result.claim_id,
                result.state_delta_id,
                result.superseded_claim_id,
                result.reason,
                _iso(proposal.created_at),
            ),
        )


class SQLiteMemoryRepository:
    def __init__(self, database_path: str | Path) -> None:
        self._path = str(database_path)
        self._write_lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS memory_schema (
                    schema_version INTEGER PRIMARY KEY,
                    installed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    user_id TEXT,
                    agent_id TEXT,
                    workspace_id TEXT,
                    session_id TEXT,
                    event_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    idempotency_key TEXT,
                    actor TEXT NOT NULL,
                    source_uri TEXT,
                    sensitivity TEXT NOT NULL,
                    retention_class TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    archived_at TEXT
                );

                CREATE UNIQUE INDEX IF NOT EXISTS events_idempotency_idx
                ON events(partition_key, idempotency_key)
                WHERE idempotency_key IS NOT NULL;

                CREATE INDEX IF NOT EXISTS events_scope_idx
                ON events(tenant_id, namespace, user_id, agent_id, workspace_id, session_id);

                CREATE TABLE IF NOT EXISTS claims (
                    id TEXT PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    user_id TEXT,
                    agent_id TEXT,
                    workspace_id TEXT,
                    session_id TEXT,
                    claim_key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    text TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    importance REAL NOT NULL,
                    status TEXT NOT NULL,
                    provenance_json TEXT NOT NULL,
                    valid_from TEXT NOT NULL,
                    valid_to TEXT,
                    created_at TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    supersedes TEXT,
                    superseded_by TEXT,
                    archived_at TEXT
                );

                CREATE INDEX IF NOT EXISTS claims_current_idx
                ON claims(partition_key, claim_key, status, version);

                CREATE TABLE IF NOT EXISTS state_deltas (
                    id TEXT PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    user_id TEXT,
                    agent_id TEXT,
                    workspace_id TEXT,
                    session_id TEXT,
                    claim_key TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    source_event_id TEXT NOT NULL,
                    current_claim_id TEXT NOT NULL,
                    previous_claim_id TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(source_event_id) REFERENCES events(id) ON DELETE CASCADE,
                    FOREIGN KEY(current_claim_id) REFERENCES claims(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    user_id TEXT,
                    agent_id TEXT,
                    workspace_id TEXT,
                    session_id TEXT,
                    kind TEXT NOT NULL,
                    text TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    quality REAL NOT NULL,
                    provenance_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    archived_at TEXT
                );

                CREATE INDEX IF NOT EXISTS artifacts_scope_idx
                ON artifacts(tenant_id, namespace, user_id, agent_id, workspace_id, session_id, kind);

                CREATE TABLE IF NOT EXISTS evolution_records (
                    id TEXT PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    user_id TEXT,
                    agent_id TEXT,
                    workspace_id TEXT,
                    session_id TEXT,
                    record_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS proposals (
                    id TEXT PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    user_id TEXT,
                    agent_id TEXT,
                    workspace_id TEXT,
                    session_id TEXT,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    claim_id TEXT,
                    state_delta_id TEXT,
                    superseded_claim_id TEXT,
                    reason TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS proposals_scope_idx
                ON proposals(tenant_id, namespace, user_id, agent_id, workspace_id, session_id);

                INSERT OR IGNORE INTO memory_schema(schema_version, installed_at)
                VALUES (1, CURRENT_TIMESTAMP);
                """
            )

    def unit_of_work(self) -> SQLiteMemoryUnitOfWork:
        return SQLiteMemoryUnitOfWork(self)

    async def current_claims(self, scope: MemoryScope) -> Sequence[Claim]:
        return await asyncio.to_thread(self._current_claims_sync, scope)

    def _current_claims_sync(self, scope: MemoryScope) -> Sequence[Claim]:
        where, params = self._visible_scope_clause(scope)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM claims
                WHERE {where} AND status = ? AND archived_at IS NULL
                ORDER BY importance DESC, confidence DESC, created_at DESC
                """,
                (*params, ClaimStatus.ACTIVE),
            ).fetchall()
        return tuple(self._claim_from_row(row) for row in rows)

    async def search(self, query: MemoryQuery, limit: int) -> Sequence[MemoryItem]:
        return await asyncio.to_thread(self._search_sync, query, limit)

    def _search_sync(self, query: MemoryQuery, limit: int) -> Sequence[MemoryItem]:
        where, params = self._visible_scope_clause(query.scope)
        with self._connect() as connection:
            claim_rows = connection.execute(
                f"SELECT * FROM claims WHERE {where} AND status = ? AND archived_at IS NULL LIMIT 500",
                (*params, ClaimStatus.ACTIVE),
            ).fetchall()
            event_rows = connection.execute(
                f"SELECT * FROM events WHERE {where} AND archived_at IS NULL ORDER BY occurred_at DESC LIMIT 500",
                params,
            ).fetchall()
            artifact_rows = connection.execute(
                f"SELECT * FROM artifacts WHERE {where} AND archived_at IS NULL LIMIT 500",
                params,
            ).fetchall()

        query_tokens = _tokens(query.text)
        candidates: list[MemoryItem] = []
        if MemoryChannel.SEMANTIC in query.channels:
            for row in claim_rows:
                overlap = self._overlap(query_tokens, f"{row['claim_key']} {row['text']} {row['value_json']}")
                candidates.append(
                    MemoryItem(
                        id=row["id"],
                        kind=MemoryKind.CLAIM,
                        text=row["text"],
                        score=0.55 * overlap + 0.25 * row["importance"] + 0.20 * row["confidence"],
                        occurred_at=_datetime(row["created_at"]) or utc_now(),
                        metadata={
                            "channel": MemoryChannel.SEMANTIC,
                            "key": row["claim_key"],
                            "source_event_ids": _provenance(row["provenance_json"]).source_event_ids,
                        },
                    )
                )
            for row in event_rows:
                overlap = self._overlap(query_tokens, row["content"])
                if query_tokens and overlap == 0:
                    continue
                candidates.append(
                    MemoryItem(
                        id=row["id"],
                        kind=MemoryKind.EVENT,
                        text=row["content"],
                        score=0.8 * overlap + 0.05,
                        occurred_at=_datetime(row["occurred_at"]) or utc_now(),
                        metadata={
                            "channel": MemoryChannel.SEMANTIC,
                            "event_type": row["event_type"],
                            "source_event_ids": (row["id"],),
                        },
                    )
                )

        enabled_kinds: set[MemoryKind] = set()
        if MemoryChannel.EPISODIC in query.channels:
            enabled_kinds.add(MemoryKind.EPISODE)
        if MemoryChannel.PROCEDURAL in query.channels:
            enabled_kinds.add(MemoryKind.PROCEDURE)
        for row in artifact_rows:
            kind = MemoryKind(row["kind"])
            if kind not in enabled_kinds:
                continue
            channel = (
                MemoryChannel.EPISODIC if kind == MemoryKind.EPISODE else MemoryChannel.PROCEDURAL
            )
            provenance = _provenance(row["provenance_json"])
            candidates.append(
                MemoryItem(
                    id=row["id"],
                    kind=kind,
                    text=row["text"],
                    score=0.75 * self._overlap(query_tokens, row["text"]) + 0.25 * row["quality"],
                    occurred_at=_datetime(row["occurred_at"]) or utc_now(),
                    metadata={
                        "channel": channel,
                        "status": row["status"],
                        "version": row["version"],
                        "source_event_ids": provenance.source_event_ids,
                    },
                )
            )
        return tuple(sorted(candidates, key=lambda item: item.score, reverse=True)[:limit])

    async def forget(self, request: ForgetRequest) -> ForgetResult:
        async with self._write_lock:
            return await asyncio.to_thread(self._forget_sync, request)

    def _forget_sync(self, request: ForgetRequest) -> ForgetResult:
        with self._connect() as connection:
            if request.all_in_scope:
                where = "partition_key = ?"
                params: tuple[Any, ...] = (request.scope.partition_key(),)
            else:
                placeholders = ",".join("?" for _ in request.memory_ids)
                where = f"partition_key = ? AND id IN ({placeholders})"
                params = (request.scope.partition_key(), *request.memory_ids)

            counts = {
                table: connection.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", params).fetchone()[0]
                for table in ("events", "claims", "artifacts")
            }
            if request.mode == ForgetMode.ARCHIVE:
                archived_at = _iso(utc_now())
                connection.execute(f"UPDATE events SET archived_at = ? WHERE {where}", (archived_at, *params))
                connection.execute(
                    f"UPDATE claims SET archived_at = ?, status = ? WHERE {where}",
                    (archived_at, ClaimStatus.ARCHIVED, *params),
                )
                connection.execute(
                    f"UPDATE artifacts SET archived_at = ?, status = ? WHERE {where}",
                    (archived_at, ArtifactStatus.ARCHIVED, *params),
                )
            else:
                connection.execute(f"DELETE FROM artifacts WHERE {where}", params)
                connection.execute(f"DELETE FROM claims WHERE {where}", params)
                connection.execute(f"DELETE FROM events WHERE {where}", params)
            return ForgetResult(
                affected_events=counts["events"],
                affected_claims=counts["claims"],
                affected_artifacts=counts["artifacts"],
                mode=request.mode,
            )

    def _insert_claim(self, connection: sqlite3.Connection, claim: Claim) -> None:
        connection.execute(
            """
            INSERT INTO claims (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, claim_key, value_json, text,
                confidence, importance, status, provenance_json, valid_from,
                valid_to, created_at, version, supersedes, superseded_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                claim.id,
                *_scope_values(claim.scope),
                claim.key,
                canonical_json(claim.value),
                claim.text,
                claim.confidence,
                claim.importance,
                claim.status,
                _provenance_json(claim.provenance),
                _iso(claim.valid_from),
                _iso(claim.valid_to) if claim.valid_to else None,
                _iso(claim.created_at),
                claim.version,
                claim.supersedes,
                claim.superseded_by,
            ),
        )

    def _insert_artifact(
        self,
        connection: sqlite3.Connection,
        artifact_id: str,
        scope: MemoryScope,
        kind: MemoryKind,
        text: str,
        payload: dict[str, Any],
        status: ArtifactStatus,
        version: int,
        quality: float,
        provenance: Provenance,
        occurred_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO artifacts (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, kind, text, payload_json, status,
                version, quality, provenance_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_id,
                *_scope_values(scope),
                kind,
                text,
                canonical_json(payload),
                status,
                version,
                quality,
                _provenance_json(provenance),
                _iso(occurred_at),
            ),
        )

    def _insert_evolution_record(
        self,
        connection: sqlite3.Connection,
        record_id: str,
        scope: MemoryScope,
        record_type: str,
        payload: dict[str, Any],
        occurred_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO evolution_records (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, record_type, payload_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_id,
                *_scope_values(scope),
                record_type,
                canonical_json(payload),
                _iso(occurred_at),
            ),
        )

    @staticmethod
    def _overlap(query_tokens: set[str], text: str) -> float:
        if not query_tokens:
            return 0.0
        return len(query_tokens & _tokens(text)) / len(query_tokens)

    @staticmethod
    def _visible_scope_clause(scope: MemoryScope) -> tuple[str, tuple[Any, ...]]:
        clauses = ["tenant_id = ?", "namespace = ?"]
        params: list[Any] = [scope.tenant_id, scope.namespace]
        for column, value in (
            ("user_id", scope.user_id),
            ("agent_id", scope.agent_id),
            ("workspace_id", scope.workspace_id),
            ("session_id", scope.session_id),
        ):
            clauses.append(f"({column} IS NULL OR {column} = ?)")
            params.append(value)
        return " AND ".join(clauses), tuple(params)

    @staticmethod
    def _scope_from_row(row: sqlite3.Row) -> MemoryScope:
        return MemoryScope(
            tenant_id=row["tenant_id"],
            namespace=row["namespace"],
            user_id=row["user_id"],
            agent_id=row["agent_id"],
            workspace_id=row["workspace_id"],
            session_id=row["session_id"],
        )

    def _event_from_row(self, row: sqlite3.Row) -> MemoryEvent:
        return MemoryEvent(
            id=row["id"],
            scope=self._scope_from_row(row),
            event_type=row["event_type"],
            content=row["content"],
            metadata=json.loads(row["metadata_json"]),
            occurred_at=_datetime(row["occurred_at"]) or utc_now(),
            ingested_at=_datetime(row["ingested_at"]) or utc_now(),
            idempotency_key=row["idempotency_key"],
            actor=row["actor"],
            source_uri=row["source_uri"],
            sensitivity=row["sensitivity"],
            retention_class=row["retention_class"],
            schema_version=row["schema_version"],
            content_hash=row["content_hash"],
        )

    def _claim_from_row(self, row: sqlite3.Row) -> Claim:
        return Claim(
            id=row["id"],
            scope=self._scope_from_row(row),
            key=row["claim_key"],
            value=json.loads(row["value_json"]),
            text=row["text"],
            confidence=row["confidence"],
            importance=row["importance"],
            status=ClaimStatus(row["status"]),
            provenance=_provenance(row["provenance_json"]),
            valid_from=_datetime(row["valid_from"]) or utc_now(),
            valid_to=_datetime(row["valid_to"]),
            created_at=_datetime(row["created_at"]) or utc_now(),
            version=row["version"],
            supersedes=row["supersedes"],
            superseded_by=row["superseded_by"],
        )
