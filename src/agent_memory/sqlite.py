from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import time
from collections import deque
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from itertools import product
from pathlib import Path
from types import TracebackType
from typing import Any

from .domain import (
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
    ScopeLevel,
    StateDelta,
    canonical_json,
    utc_now,
)
from .operations import sqlite_retention
from .retrieval.temporal_history import SQLiteClaimHistory, temporal_candidates


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


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
        self._admission_batch_times: dict[tuple[str, str], str] = {}

    async def __aenter__(self) -> SQLiteMemoryUnitOfWork:
        await self._repository._write_lock.acquire()
        try:
            self._connection = self._repository._connect()
            self._connection.execute("BEGIN IMMEDIATE")
            self._admission_batch_times = {}
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
        if self.connection.execute(
            "SELECT 1 FROM admission_tombstones WHERE partition_key = ? AND idempotency_key = ?",
            (scope.partition_key(), idempotency_key),
        ).fetchone():
            raise ValueError("event was forgotten and cannot be replayed")
        row = self.connection.execute(
            "SELECT * FROM events WHERE partition_key = ? AND idempotency_key = ?",
            (scope.partition_key(), idempotency_key),
        ).fetchone()
        return self._repository._event_from_row(row) if row else None

    async def claim_ids_for_event(self, event_id: str) -> Sequence[str]:
        rows = self.connection.execute(
            "SELECT claim_id FROM claim_sources WHERE event_id = ? ORDER BY rowid",
            (event_id,),
        ).fetchall()
        return tuple(row["claim_id"] for row in rows)

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

    async def find_feedback_record(
        self, record_id: str, record_type: str
    ) -> dict[str, object] | None:
        self._expire_pending_feedback()
        row = self.connection.execute(
            "SELECT * FROM evolution_records WHERE id = ? AND record_type = ?",
            (record_id, record_type),
        ).fetchone()
        return self._repository._feedback_from_row(row) if row else None

    async def find_feedback_by_idempotency(
        self, scope: MemoryScope, record_type: str, idempotency_key: str
    ) -> dict[str, object] | None:
        self._expire_pending_feedback()
        row = self.connection.execute(
            """
            SELECT * FROM evolution_records
            WHERE partition_key = ? AND record_type = ? AND idempotency_key = ?
            """,
            (scope.partition_key(), record_type, idempotency_key),
        ).fetchone()
        return self._repository._feedback_from_row(row) if row else None

    async def activate_pending_children(
        self, scope: MemoryScope, parent_id: str
    ) -> Sequence[str]:
        rows = self.connection.execute(
            """
            SELECT id FROM evolution_records
            WHERE partition_key = ? AND parent_id = ? AND feedback_status = ?
              AND invalidated_at IS NULL
            """,
            (scope.partition_key(), parent_id, FeedbackStatus.PENDING),
        ).fetchall()
        self.connection.execute(
            """
            UPDATE evolution_records SET feedback_status = ?
            WHERE partition_key = ? AND parent_id = ? AND feedback_status = ?
              AND invalidated_at IS NULL
            """,
            (
                FeedbackStatus.ACCEPTED,
                scope.partition_key(),
                parent_id,
                FeedbackStatus.PENDING,
            ),
        )
        return tuple(row["id"] for row in rows)

    async def supersede_feedback(self, scope: MemoryScope, record_id: str) -> None:
        self.connection.execute(
            """
            UPDATE evolution_records SET feedback_status = ?
            WHERE partition_key = ? AND id = ? AND feedback_status != ?
            """,
            (
                FeedbackStatus.SUPERSEDED,
                scope.partition_key(),
                record_id,
                FeedbackStatus.INVALIDATED,
            ),
        )
        self._repository._invalidate_feedback(
            self.connection,
            scope.partition_key(),
            {record_id},
            erase=False,
        )
        self.connection.execute(
            """
            UPDATE evolution_records
            SET feedback_status = ?, invalidated_at = NULL
            WHERE partition_key = ? AND id = ?
            """,
            (FeedbackStatus.SUPERSEDED, scope.partition_key(), record_id),
        )

    async def pending_feedback_count(self, scope: MemoryScope) -> int:
        self._expire_pending_feedback()
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS count FROM evolution_records
            WHERE partition_key = ? AND feedback_status = ?
            """,
            (scope.partition_key(), FeedbackStatus.PENDING),
        ).fetchone()
        return int(row["count"] if row else 0)

    def _expire_pending_feedback(self) -> None:
        self.connection.execute(
            """
            UPDATE evolution_records SET feedback_status = ?
            WHERE feedback_status = ? AND expires_at IS NOT NULL AND expires_at <= ?
            """,
            (FeedbackStatus.EXPIRED, FeedbackStatus.PENDING, _iso(utc_now())),
        )

    async def append_event(self, event: MemoryEvent) -> None:
        deleted = self.connection.execute(
            """SELECT 1 FROM admission_tombstones
               WHERE event_id = ? OR (partition_key = ? AND idempotency_key = ?)
               LIMIT 1""",
            (event.id, event.scope.partition_key(), event.idempotency_key),
        ).fetchone()
        if deleted:
            raise ValueError("event was forgotten and cannot be replayed")
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

    async def retention_head_get(self, scope, kind, identity):
        return sqlite_retention.head_get(self.connection, scope, kind, identity)

    async def retention_head_put(self, scope, kind, identity, payload, expected_generation):
        return sqlite_retention.head_put(
            self.connection, scope, kind, identity, payload, expected_generation
        )

    async def get_source_event(self, scope, event_id):
        row = self.connection.execute(
            "SELECT * FROM events WHERE partition_key=? AND id=? AND archived_at IS NULL",
            (scope.partition_key(), event_id),
        ).fetchone()
        return self._repository._event_from_row(row) if row else None

    async def retention_update(self, scope, request_id, payload):
        return sqlite_retention.update(self.connection, scope, request_id, payload)

    async def retention_active(self, scope):
        return sqlite_retention.active(self.connection, scope)

    async def delivery_get(self, scope, kind, identity):
        from .operations import sqlite_delivery

        return sqlite_delivery.get(self.connection, scope, kind, identity)

    async def delivery_insert(self, scope, kind, identity, payload):
        from .operations import sqlite_delivery

        return sqlite_delivery.insert(self.connection, scope, kind, identity, payload)

    async def delivery_count(self, scope, kind):
        from .operations import sqlite_delivery

        return sqlite_delivery.count(self.connection, scope, kind)

    async def refresh_get(self, scope, kind, identity):
        from .operations import sqlite_refresh

        return sqlite_refresh.get(self.connection, scope, kind, identity)

    async def refresh_put(self, scope, kind, identity, payload):
        from .operations import sqlite_refresh

        return sqlite_refresh.put(self.connection, scope, kind, identity, payload)

    async def refresh_records(self, scope, kind):
        from .operations import sqlite_refresh

        return sqlite_refresh.records(self.connection, scope, kind)

    async def index_recovery_get(self, scope, channel, epoch, kind, identity):
        from .operations import sqlite_index

        return sqlite_index.recovery_get(self.connection, scope, channel, epoch, kind, identity)

    async def index_recovery_put(self, scope, channel, epoch, kind, identity, payload):
        from .operations import sqlite_index

        return sqlite_index.recovery_put(
            self.connection, scope, channel, epoch, kind, identity, payload
        )

    async def index_recovery_count(self, scope, channel, epoch, kind):
        from .operations import sqlite_index

        return sqlite_index.recovery_count(self.connection, scope, channel, epoch, kind)

    async def retention_requests(self, scope):
        return sqlite_retention.requests(self.connection, scope)

    async def index_job_position(self, scope, channel, epoch, publication_id):
        from .operations import sqlite_index

        return sqlite_index.position(self.connection, scope, channel, epoch, publication_id)

    async def index_job_get(self, scope, channel, epoch, token_id):
        from .operations import sqlite_index
        return sqlite_index.job_get(self.connection, scope, channel, epoch, token_id)

    async def index_job_put(self, scope, row):
        from .operations import sqlite_index
        return sqlite_index.job_put(self.connection, scope, row)

    async def index_jobs(self, scope, channel, epoch):
        from .operations import sqlite_index
        return sqlite_index.jobs(self.connection, scope, channel, epoch)

    async def index_document_get(self, scope, channel, candidate_id):
        from .operations import sqlite_index
        return sqlite_index.document_get(self.connection, scope, channel, candidate_id)

    async def index_document_put(self, scope, channel, candidate_id, document):
        from .operations import sqlite_index
        return sqlite_index.document_put(self.connection, scope, channel, candidate_id, document)

    async def index_lookup(self, scope, channel, slot_key, limit):
        from .operations import sqlite_index
        return sqlite_index.lookup(self.connection, scope, channel, slot_key, limit)

    async def forget_for_restore(self, request):
        return self._repository._forget_on_connection(
            self.connection, request, replay=True
        )

    async def purge_import(self, scope, entry):
        from .operations import sqlite_purge

        return sqlite_purge.import_entry(self.connection, scope, entry)

    async def purge_restore_get(self, scope, identity):
        from .operations import sqlite_purge

        return sqlite_purge.restore_get(self.connection, scope, identity)

    async def purge_restore_put(self, scope, identity, payload):
        from .operations import sqlite_purge

        return sqlite_purge.restore_put(self.connection, scope, identity, payload)

    async def purge_restore_count(self, scope):
        from .operations import sqlite_purge

        return sqlite_purge.restore_count(self.connection, scope)

    async def purge_head(self, scope):
        from .operations import sqlite_purge

        return sqlite_purge.head(self.connection, scope)

    async def purge_page(self, scope, after, limit):
        from .operations import sqlite_purge

        return sqlite_purge.page(self.connection, scope, after, limit)

    async def source_erased(self, scope, identity):
        from .operations import sqlite_purge

        return sqlite_purge.erased(self.connection, scope, identity)

    async def producer_get(self, scope, producer_id):
        return sqlite_retention.producer_get(self.connection, scope, producer_id)

    async def producer_put(self, scope, producer_id, payload):
        return sqlite_retention.producer_put(self.connection, scope, producer_id, payload)

    async def retention_epoch(self, scope):
        return sqlite_retention.epoch(self.connection, scope)

    async def retention_get(self, scope, kind, request_id):
        return sqlite_retention.get(self.connection, scope, kind, request_id)

    async def retention_insert(self, scope, kind, request_id, payload):
        sqlite_retention.insert(self.connection, scope, kind, request_id, payload)

    async def retention_count(self, scope, kind):
        return sqlite_retention.count(self.connection, scope, kind)

    async def retention_identity_owner(self, scope, event_id, idempotency_key):
        return sqlite_retention.identity_owner(self.connection, scope, event_id, idempotency_key)

    async def lock_admission_scope(self, scope: MemoryScope) -> None:
        # BEGIN IMMEDIATE already serializes publication and deletion across connections.
        _ = self.connection

    async def get_admission_record(
        self, scope: MemoryScope, record_id: str
    ) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM admission_records WHERE partition_key = ? AND record_id = ?",
            (scope.partition_key(), record_id),
        ).fetchone()
        return self._repository._admission_from_row(row) if row else None

    async def list_admission_records(
        self, scope: MemoryScope, slot_key: str | None = None
    ) -> tuple[dict[str, Any], ...]:
        return self._repository._read_admission_records(
            self.connection, scope, slot_key=slot_key, exact=True
        )

    async def list_admission_barriers(self, scope: MemoryScope, slot_key: str):
        rows = self._repository._read_admission_records(
            self.connection, scope, slot_key=slot_key, exact=True, barriers=True
        )
        return tuple(r for r in rows if "contribution_barrier" in r["payload"])

    async def save_admission_record(
        self,
        scope: MemoryScope,
        record_id: str,
        event_id: str,
        slot_key: str,
        payload: dict[str, Any],
        expected_version: int,
    ) -> int:
        if type(expected_version) is not int or expected_version < 0:
            raise ValueError("expected_version must be a non-negative integer")
        if not record_id or not event_id or not slot_key or not isinstance(payload, dict):
            raise ValueError("admission identity and dictionary payload are required")
        if payload.get("deleted"):
            raise ValueError("admission tombstones are reserved for deletion")
        claim_id = payload.get("claim_id")
        if claim_id is not None:
            claim = self.connection.execute(
                "SELECT 1 FROM claims WHERE id = ? AND partition_key = ? AND archived_at IS NULL",
                (claim_id, scope.partition_key()),
            ).fetchone()
            if claim is None:
                raise ValueError("published admission claim is missing or outside the scope")
        event_ids = self._repository._admission_source_ids(payload) | {event_id}
        for source_id in event_ids:
            event = self.connection.execute(
                "SELECT * FROM events WHERE id = ? AND archived_at IS NULL", (source_id,)
            ).fetchone()
            if event is None:
                raise ValueError("admission source is missing or archived")
            source_scope = self._repository._scope_from_row(event)
            allowed = {source_scope}
            for level in ScopeLevel:
                try:
                    allowed.add(source_scope.project(level))
                except ValueError:
                    continue
            if scope not in allowed:
                raise ValueError("admission source cannot publish into this scope")
            if self.connection.execute(
                "SELECT 1 FROM admission_tombstones WHERE event_id = ? LIMIT 1",
                (source_id,),
            ).fetchone():
                raise ValueError("admission source was forgotten")
        existing = self.connection.execute(
            "SELECT * FROM admission_records WHERE record_id = ?", (record_id,)
        ).fetchone()
        if existing is not None:
            if (
                existing["partition_key"] != scope.partition_key()
                or existing["event_id"] != event_id
                or existing["slot_key"] != slot_key
                or existing["version"] != expected_version
                or json.loads(existing["payload_json"]).get("deleted")
            ):
                raise ValueError("admission update conflict or deleted record")
        elif expected_version != 0:
            raise ValueError("admission update conflict")
        version = expected_version + 1
        namespace = (scope.tenant_id, scope.namespace)
        if namespace not in self._admission_batch_times:
            self._admission_batch_times[namespace] = self._repository._admission_publication_time(
                self.connection, scope
            )
        recorded_at = self._admission_batch_times[namespace]
        if existing is not None and recorded_at < existing["recorded_at"]:
            raise ValueError("admission publication time precedes an existing version")
        payload_json = canonical_json(payload)
        if existing is None:
            self.connection.execute(
                """INSERT INTO admission_records
                   (record_id, partition_key, event_id, slot_key, scope_json,
                    payload_json, version, recorded_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (record_id, scope.partition_key(), event_id, slot_key,
                 canonical_json(asdict(scope)), payload_json, version, recorded_at),
            )
        else:
            changed = self.connection.execute(
                """UPDATE admission_records SET payload_json = ?, version = ?, recorded_at = ?
                   WHERE record_id = ? AND partition_key = ? AND version = ?""",
                (payload_json, version, recorded_at, record_id,
                 scope.partition_key(), expected_version),
            ).rowcount
            if changed != 1:
                raise ValueError("admission update conflict")
        self.connection.execute(
            """INSERT INTO admission_versions
               (record_id, version, partition_key, payload_json, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            (record_id, version, scope.partition_key(), payload_json, recorded_at),
        )
        return version

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

    async def claim_is_effective(self, claim: Claim, valid_at: datetime) -> bool:
        claims = SQLiteClaimHistory(self._repository).read(
            self.connection, claim.scope, valid_at, utc_now()
        )
        return any(current.id == claim.id for current in claims)

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
                (
                    _iso(current.valid_from)
                    if current.valid_from > previous.valid_from
                    else (_iso(previous.valid_to) if previous.valid_to else None)
                ),
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
        self.connection.execute(
            "INSERT OR IGNORE INTO claim_sources (claim_id, event_id) VALUES (?, ?)",
            (claim_id, event_id),
        )

        SQLiteClaimHistory(self._repository).refresh_evidence(self.connection, claim_id)

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

    async def save_block(self, block: MemoryBlock, expected_version: int) -> MemoryBlock:
        row = self.connection.execute(
            "SELECT * FROM artifacts WHERE id = ?", (block.id,)
        ).fetchone()
        now = utc_now()
        if row is None:
            if expected_version != 0:
                raise RuntimeError(
                    f"memory block version conflict: expected {expected_version}, current 0"
                )
            stored = replace(block, version=1, created_at=now, updated_at=now)
            self._repository._insert_artifact(
                self.connection,
                stored.id,
                stored.scope,
                MemoryKind.BLOCK,
                f"{stored.title}\n{stored.content}",
                asdict(stored),
                stored.status,
                stored.version,
                1.0,
                stored.provenance,
                stored.updated_at,
            )
            return stored

        if (
            row["kind"] != MemoryKind.BLOCK
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
        cursor = self.connection.execute(
            """
            UPDATE artifacts
            SET text = ?, payload_json = ?, status = ?, version = ?, quality = ?,
                provenance_json = ?, occurred_at = ?
            WHERE id = ? AND partition_key = ? AND kind = ? AND version = ?
              AND archived_at IS NULL
            """,
            (
                f"{stored.title}\n{stored.content}",
                canonical_json(asdict(stored)),
                stored.status,
                stored.version,
                1.0,
                _provenance_json(stored.provenance),
                _iso(stored.updated_at),
                stored.id,
                stored.scope.partition_key(),
                MemoryKind.BLOCK,
                expected_version,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("memory block update conflict")
        return stored

    async def save_decision(self, decision: DecisionRecord) -> None:
        self._repository._insert_evolution_record(
            self.connection,
            decision.id,
            decision.scope,
            "decision",
            asdict(decision),
            decision.created_at,
        )

    async def save_outcome(self, outcome: OutcomeEvent) -> None:
        self._repository._insert_evolution_record(
            self.connection,
            outcome.id,
            outcome.scope,
            "outcome",
            asdict(outcome),
            outcome.occurred_at,
        )

    async def save_evaluation(self, evaluation: EvaluationRecord) -> None:
        self._repository._insert_evolution_record(
            self.connection,
            evaluation.id,
            evaluation.scope,
            "evaluation",
            asdict(evaluation),
            evaluation.created_at,
        )

    async def save_reward(self, reward: RewardSignal) -> None:
        self._repository._insert_evolution_record(
            self.connection, reward.id, reward.scope, "reward", asdict(reward), reward.created_at
        )

    async def save_retrieval_trace(self, trace: RetrievalTrace) -> None:
        self._repository._insert_evolution_record(
            self.connection,
            trace.id,
            trace.scope,
            "retrieval",
            asdict(trace),
            trace.created_at,
        )

    async def save_proposal(self, proposal: MemoryProposal, result: ProposalResult) -> None:
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
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        self._enable_wal()
        with self._connection() as connection:
            connection.executescript(sqlite_retention.SCHEMA)
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

                CREATE INDEX IF NOT EXISTS events_recent_scope_idx
                ON events(partition_key, occurred_at DESC, id DESC)
                WHERE archived_at IS NULL;

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

                CREATE TABLE IF NOT EXISTS claim_sources (
                    claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
                    event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
                    PRIMARY KEY (claim_id, event_id)
                );

                CREATE INDEX IF NOT EXISTS claim_sources_event_idx
                ON claim_sources(event_id);

                CREATE TABLE IF NOT EXISTS admission_records (
                    record_id TEXT PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    slot_key TEXT NOT NULL,
                    scope_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version > 0),
                    recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS admission_records_scope_slot_idx
                ON admission_records(partition_key, slot_key, record_id);
                CREATE INDEX IF NOT EXISTS admission_records_event_idx
                ON admission_records(event_id);
                CREATE TABLE IF NOT EXISTS admission_versions (
                    record_id TEXT NOT NULL
                        REFERENCES admission_records(record_id) ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    partition_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY(record_id, version)
                );
                CREATE TABLE IF NOT EXISTS admission_tombstones (
                    partition_key TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    idempotency_key TEXT,
                    PRIMARY KEY(partition_key, event_id)
                );
                CREATE INDEX IF NOT EXISTS admission_tombstones_event_idx
                ON admission_tombstones(event_id);
                CREATE INDEX IF NOT EXISTS admission_tombstones_idempotency_idx
                ON admission_tombstones(partition_key, idempotency_key)
                WHERE idempotency_key IS NOT NULL;

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
                ON artifacts(
                    tenant_id, namespace, user_id, agent_id, workspace_id, session_id, kind
                );

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
                    feedback_status TEXT NOT NULL DEFAULT 'accepted',
                    parent_id TEXT,
                    idempotency_key TEXT,
                    payload_hash TEXT,
                    corrects_id TEXT,
                    expires_at TEXT,
                    invalidated_at TEXT,
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
            self._migrate_claim_sources(connection)
            self._migrate_evolution_records(connection)
            self._ensure_current_claim_index(connection)
            SQLiteClaimHistory(self).initialize(connection)

    def _enable_wal(self) -> None:
        for attempt in range(8):
            connection = self._connect()
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as error:
                if "locked" not in str(error).casefold() or attempt == 7:
                    raise
                time.sleep(0.01 * (2**attempt))
            finally:
                connection.close()
    @staticmethod
    def _migrate_evolution_records(connection: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(evolution_records)").fetchall()
        }
        additions = {
            "feedback_status": "TEXT NOT NULL DEFAULT 'accepted'",
            "parent_id": "TEXT",
            "idempotency_key": "TEXT",
            "payload_hash": "TEXT",
            "corrects_id": "TEXT",
            "expires_at": "TEXT",
            "invalidated_at": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE evolution_records ADD COLUMN {name} {declaration}"
                )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS evolution_records_idempotency_idx
            ON evolution_records(partition_key, record_type, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS evolution_records_parent_idx
            ON evolution_records(partition_key, parent_id, feedback_status)
            """
        )
        connection.execute(
            "INSERT OR IGNORE INTO memory_schema(schema_version, installed_at) "
            "VALUES (2, CURRENT_TIMESTAMP)"
        )

    def _migrate_claim_sources(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            "SELECT id, tenant_id, namespace, provenance_json FROM claims"
        ).fetchall()
        for row in rows:
            for event_id in dict.fromkeys(_provenance(row["provenance_json"]).source_event_ids):
                event = connection.execute(
                    """
                    SELECT 1 FROM events
                    WHERE id = ? AND tenant_id = ? AND namespace = ?
                    """,
                    (event_id, row["tenant_id"], row["namespace"]),
                ).fetchone()
                if event:
                    connection.execute(
                        "INSERT OR IGNORE INTO claim_sources (claim_id, event_id) VALUES (?, ?)",
                        (row["id"], event_id),
                    )

        orphan_ids = {
            row["id"]
            for row in connection.execute(
                """
                SELECT id FROM claims
                WHERE NOT EXISTS (
                    SELECT 1 FROM claim_sources WHERE claim_sources.claim_id = claims.id
                )
                """
            ).fetchall()
        }
        self._delete_claim_rows(connection, orphan_ids)

        for row in connection.execute("SELECT id FROM claims").fetchall():
            self._sync_claim_source_json(connection, row["id"])

    def _ensure_current_claim_index(self, connection: sqlite3.Connection) -> None:
        connection.execute("DROP INDEX IF EXISTS claims_current_idx")
        duplicate_groups = connection.execute(
            """
            SELECT partition_key, claim_key
            FROM claims
            WHERE status = ?
            GROUP BY partition_key, claim_key
            HAVING COUNT(*) > 1
            """,
            (ClaimStatus.ACTIVE,),
        ).fetchall()

        for group in duplicate_groups:
            rows = connection.execute(
                """
                SELECT id, valid_from, created_at, supersedes
                FROM claims
                WHERE partition_key = ? AND claim_key = ? AND status = ?
                ORDER BY version DESC, valid_from DESC, created_at DESC, id DESC
                """,
                (group["partition_key"], group["claim_key"], ClaimStatus.ACTIVE),
            ).fetchall()
            winner = rows[0]
            losers = rows[1:]
            for loser in losers:
                connection.execute(
                    """
                    UPDATE claims
                    SET status = ?, valid_to = ?, superseded_by = ?
                    WHERE id = ?
                    """,
                    (
                        ClaimStatus.SUPERSEDED,
                        winner["valid_from"],
                        winner["id"],
                        loser["id"],
                    ),
                )
            if losers and not winner["supersedes"]:
                connection.execute(
                    "UPDATE claims SET supersedes = ? WHERE id = ?",
                    (losers[0]["id"], winner["id"]),
                )

        connection.execute(
            """
            CREATE UNIQUE INDEX claims_current_idx
            ON claims(partition_key, claim_key)
            WHERE status = 'active'
            """
        )

    @staticmethod
    def _delete_claim_rows(connection: sqlite3.Connection, claim_ids: set[str]) -> None:
        if not claim_ids:
            return
        placeholders = ",".join("?" for _ in claim_ids)
        params = tuple(claim_ids)
        connection.execute(
            f"UPDATE claims SET supersedes = NULL WHERE supersedes IN ({placeholders})",
            params,
        )
        connection.execute(
            f"UPDATE claims SET superseded_by = NULL WHERE superseded_by IN ({placeholders})",
            params,
        )
        connection.execute(
            f"DELETE FROM claims WHERE id IN ({placeholders})",
            params,
        )

    @staticmethod
    def _sync_claim_source_json(connection: sqlite3.Connection, claim_id: str) -> None:
        row = connection.execute(
            "SELECT provenance_json FROM claims WHERE id = ?", (claim_id,)
        ).fetchone()
        if row is None:
            return
        source_ids = [
            row["event_id"]
            for row in connection.execute(
                "SELECT event_id FROM claim_sources WHERE claim_id = ? ORDER BY rowid",
                (claim_id,),
            ).fetchall()
        ]
        provenance = _provenance(row["provenance_json"])
        updated = Provenance(
            source_event_ids=tuple(source_ids),
            extractor=provenance.extractor,
            provider=provenance.provider,
            model=provenance.model,
            prompt_version=provenance.prompt_version,
            source_uri=provenance.source_uri,
            created_at=provenance.created_at,
        )
        connection.execute(
            "UPDATE claims SET provenance_json = ? WHERE id = ?",
            (_provenance_json(updated), claim_id),
        )

    def unit_of_work(self) -> SQLiteMemoryUnitOfWork:
        return SQLiteMemoryUnitOfWork(self)

    @staticmethod
    def _admission_publication_time(
        connection: sqlite3.Connection, scope: MemoryScope
    ) -> str:
        row = connection.execute(
            """SELECT MAX(recorded_at) AS last FROM admission_records
               WHERE json_extract(scope_json, '$.tenant_id') = ?
                 AND json_extract(scope_json, '$.namespace') = ?""",
            (scope.tenant_id, scope.namespace),
        ).fetchone()
        now = utc_now()
        if row["last"] is not None:
            now = max(now, datetime.fromisoformat(row["last"]) + timedelta(microseconds=1))
        return _iso(now)

    @staticmethod
    def _admission_from_row(row: sqlite3.Row) -> dict[str, Any] | None:
        payload = json.loads(row["payload_json"])
        if payload.get("deleted"):
            return None
        return {
            "id": row["record_id"],
            "event_id": row["event_id"],
            "slot_key": row["slot_key"],
            "scope": json.loads(row["scope_json"]),
            "payload": payload,
            "version": row["version"],
            "recorded_at": row["recorded_at"],
        }

    @staticmethod
    def _admission_source_ids(payload: dict[str, Any]) -> set[str]:
        sources: set[str] = set()

        def visit(value: Any) -> None:
            if isinstance(value, dict):
                for key, item in value.items():
                    if key == "source_event_id" and isinstance(item, str):
                        sources.add(item)
                    elif key == "source_event_ids" and isinstance(item, (list, tuple)):
                        sources.update(source for source in item if isinstance(source, str))
                    elif isinstance(item, (dict, list, tuple)):
                        visit(item)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    visit(item)

        visit(payload)
        return sources

    @staticmethod
    def _admission_scope_clause(scope: MemoryScope) -> tuple[str, tuple[Any, ...]]:
        clauses = ["json_extract(scope_json, '$.tenant_id') = ?",
                   "json_extract(scope_json, '$.namespace') = ?"]
        params: list[Any] = [scope.tenant_id, scope.namespace]
        for key in ("user_id", "agent_id", "workspace_id", "session_id"):
            clauses.append(
                f"(json_extract(scope_json, '$.{key}') IS NULL "
                f"OR json_extract(scope_json, '$.{key}') = ?)"
            )
            params.append(getattr(scope, key))
        return " AND ".join(clauses), tuple(params)

    def _read_admission_records(
        self,
        connection: sqlite3.Connection,
        scope: MemoryScope,
        *,
        slot_key: str | None = None,
        exact: bool = False,
        barriers: bool = False,
    ) -> tuple[dict[str, Any], ...]:
        if exact:
            where, params = "partition_key = ?", (scope.partition_key(),)
        else:
            where, params = self._admission_scope_clause(scope)
        if slot_key is not None:
            where += " AND slot_key = ?"
            params = (*params, slot_key)
        visibility = "COALESCE(json_extract(payload_json, '$.deleted'), 0) = 0"
        if barriers:
            visibility += " OR json_type(payload_json, '$.contribution_barrier') = 'array'"
        rows = connection.execute(
            f"SELECT * FROM admission_records WHERE {where} AND ({visibility}) "
            "ORDER BY record_id LIMIT 1025",
            params,
        ).fetchall()
        if len(rows) > 1024:
            raise ValueError("admission record limit exceeded; narrow the requested slot")
        return tuple(
            self._admission_from_row(row) or {
                "id": row["record_id"], "event_id": row["event_id"],
                "slot_key": row["slot_key"], "scope": json.loads(row["scope_json"]),
                "payload": json.loads(row["payload_json"]), "version": row["version"],
                "recorded_at": row["recorded_at"],
            } for row in rows
        )

    async def admission_records(
        self, scope: MemoryScope, *, slot_key: str | None = None
    ) -> tuple[dict[str, Any], ...]:
        def read():
            with self._connection() as connection:
                return self._read_admission_records(connection, scope, slot_key=slot_key)
        return await asyncio.to_thread(read)

    async def admission_snapshot(self, scope: MemoryScope) -> tuple[dict[str, Any], ...]:
        """Read visible records and their versions from one database snapshot."""
        def read():
            with self._connection() as connection:
                connection.execute("BEGIN")
                records = self._read_admission_records(connection, scope, barriers=True)
                if not records:
                    return ()
                ids = tuple(record["id"] for record in records)
                placeholders = ",".join("?" for _ in ids)
                versions = connection.execute(
                    "SELECT record_id, version, payload_json, recorded_at FROM admission_versions "
                    f"WHERE record_id IN ({placeholders}) ORDER BY record_id, version", ids,
                ).fetchall()
                by_id: dict[str, list[dict[str, Any]]] = {identity: [] for identity in ids}
                for version in versions:
                    by_id[version["record_id"]].append({
                        "version": version["version"],
                        "payload": json.loads(version["payload_json"]),
                        "recorded_at": version["recorded_at"],
                    })
                return tuple({**record, "versions": by_id[record["id"]]} for record in records)
        return await asyncio.to_thread(read)

    async def admission_record(
        self, scope: MemoryScope, record_id: str
    ) -> dict[str, Any] | None:
        def read():
            where, params = self._admission_scope_clause(scope)
            with self._connection() as connection:
                row = connection.execute(
                    f"SELECT * FROM admission_records WHERE {where} AND record_id = ?",
                    (*params, record_id),
                ).fetchone()
                return self._admission_from_row(row) if row else None
        return await asyncio.to_thread(read)

    async def admission_record_versions(
        self, scope: MemoryScope, record_id: str
    ) -> tuple[dict[str, Any], ...]:
        def read():
            where, params = self._admission_scope_clause(scope)
            with self._connection() as connection:
                connection.execute("BEGIN")
                row = connection.execute(
                    f"SELECT * FROM admission_records WHERE {where} AND record_id = ?",
                    (*params, record_id),
                ).fetchone()
                if row is None or self._admission_from_row(row) is None:
                    return ()
                rows = connection.execute(
                    "SELECT * FROM admission_versions WHERE record_id = ? ORDER BY version",
                    (record_id,),
                ).fetchall()
                return tuple({
                    "id": item["record_id"],
                    "version": item["version"],
                    "payload": json.loads(item["payload_json"]),
                    "recorded_at": item["recorded_at"],
                } for item in rows)
        return await asyncio.to_thread(read)

    async def admission_protected_sources(self, scope: MemoryScope) -> tuple[str, ...]:
        """Return all governed source IDs, including terminal records and erased events.

        No content leaves this boundary, and no limit may silently omit IDs from
        the final retrieval guard. Ancestor partitions follow ordinary visibility.
        """
        choices = [(None,) if value is None else (None, value) for value in (
            scope.user_id, scope.agent_id, scope.workspace_id, scope.session_id
        )]
        partitions = tuple(sorted({
            MemoryScope(scope.tenant_id, scope.namespace, *values).partition_key()
            for values in product(*choices)
        }))

        def read():
            placeholders = ",".join("?" for _ in partitions)
            with self._connection() as connection:
                connection.execute("BEGIN")
                rows = connection.execute(
                    "SELECT id FROM events "
                    f"WHERE partition_key IN ({placeholders}) "
                    "AND event_type IN ('memory.atom', 'memory.atom.verification') "
                    "UNION SELECT event_id AS id FROM admission_tombstones "
                    f"WHERE partition_key IN ({placeholders}) "
                    "UNION SELECT event_id AS id FROM admission_records "
                    f"WHERE partition_key IN ({placeholders})",
                    (*partitions, *partitions, *partitions),
                ).fetchall()
                protected = {row["id"] for row in rows}
                records = connection.execute(
                    "SELECT payload_json FROM admission_records "
                    f"WHERE partition_key IN ({placeholders})", partitions,
                ).fetchall()
                for record in records:
                    protected.update(self._admission_source_ids(json.loads(record["payload_json"])))
                references = connection.execute(
                    "SELECT id, metadata_json FROM events "
                    f"WHERE partition_key IN ({placeholders}) "
                    "AND json_type(metadata_json, '$.source_event_ids') = 'array'",
                    partitions,
                ).fetchall()
                dependents: dict[str, set[str]] = {}
                for reference in references:
                    for source_id in json.loads(reference["metadata_json"])["source_event_ids"]:
                        if isinstance(source_id, str):
                            dependents.setdefault(source_id, set()).add(reference["id"])
                pending = deque(protected)
                while pending:
                    for dependent in dependents.get(pending.popleft(), ()):
                        if dependent not in protected:
                            protected.add(dependent)
                            pending.append(dependent)
                return tuple(sorted(protected))
        return await asyncio.to_thread(read)

    def load_recent_event_evidence(self, scope: MemoryScope, *, limit: int):
        """Read a bounded, exact-scope window of source events for lexical ranking.

        This is a recent-window source, not a full-text index. Stored scope fields
        are returned independently so the caller can reject inconsistent rows.
        """
        from datetime import datetime

        from .domain import MemoryChannel, MemoryItem, MemoryKind
        from .retrieval.lexical import EvidenceItem
        from .retrieval.scoped_lexical import ScopedEvidenceItem

        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 512:
            raise ValueError("limit must be between 1 and 512")
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT id, tenant_id, namespace, user_id, agent_id, workspace_id,
                       session_id, content, occurred_at
                FROM events
                WHERE partition_key = ? AND archived_at IS NULL
                  AND length(content) BETWEEN 1 AND 2048
                ORDER BY occurred_at DESC, id DESC
                LIMIT ?
                """,
                (scope.partition_key(), limit),
            ).fetchall()
        return tuple(
            ScopedEvidenceItem(
                scope=MemoryScope(
                    row["tenant_id"],
                    namespace=row["namespace"],
                    user_id=row["user_id"],
                    agent_id=row["agent_id"],
                    workspace_id=row["workspace_id"],
                    session_id=row["session_id"],
                ),
                evidence=EvidenceItem(
                    item=MemoryItem(
                        row["id"],
                        MemoryKind.EVENT,
                        row["content"],
                        0.5,
                        datetime.fromisoformat(row["occurred_at"]),
                    ),
                    channel=MemoryChannel.EPISODIC,
                    source_event_ids=(row["id"],),
                ),
            )
            for row in rows
        )

    async def claims_at(self, scope, *, valid_at, known_at):
        return await asyncio.to_thread(self._claims_at_sync, scope, valid_at, known_at)

    def _claims_at_sync(self, scope, valid_at, known_at):
        with self._connection() as connection:
            connection.execute("BEGIN")
            return SQLiteClaimHistory(self).read(connection, scope, valid_at, known_at)

    async def current_claims(self, scope: MemoryScope) -> Sequence[Claim]:
        return await asyncio.to_thread(self._current_claims_sync, scope)

    async def list_feedback(
        self,
        scope: MemoryScope,
        record_type: str,
        limit: int,
        after_id: str | None = None,
    ) -> Sequence[dict[str, object]]:
        return await asyncio.to_thread(
            self._list_feedback_sync, scope, record_type, limit, after_id
        )

    def _list_feedback_sync(
        self,
        scope: MemoryScope,
        record_type: str,
        limit: int,
        after_id: str | None,
    ) -> Sequence[dict[str, object]]:
        partition_key = scope.partition_key()
        with self._connection() as connection:
            cursor_clause = ""
            params: list[object] = [partition_key, record_type]
            if after_id:
                anchor = connection.execute(
                    """
                    SELECT occurred_at, id FROM evolution_records
                    WHERE partition_key = ? AND record_type = ? AND id = ?
                    """,
                    (partition_key, record_type, after_id),
                ).fetchone()
                if anchor is None:
                    raise ValueError("feedback pagination cursor is invalid")
                cursor_clause = (
                    "AND (occurred_at < ? OR (occurred_at = ? AND id < ?))"
                )
                params.extend((anchor["occurred_at"], anchor["occurred_at"], anchor["id"]))
            rows = connection.execute(
                f"""
                SELECT * FROM evolution_records
                WHERE partition_key = ? AND record_type = ? {cursor_clause}
                ORDER BY occurred_at DESC, id DESC LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return tuple(self._feedback_from_row(row) for row in rows)

    def _current_claims_sync(self, scope: MemoryScope) -> Sequence[Claim]:
        now = utc_now()
        return self._claims_at_sync(scope, now, now)

    async def search(self, query: MemoryQuery, limit: int) -> Sequence[MemoryItem]:
        return await asyncio.to_thread(self._search_sync, query, limit)

    def _search_sync(self, query: MemoryQuery, limit: int) -> Sequence[MemoryItem]:
        if query.valid_at is not None or query.known_at is not None:
            now = utc_now()
            claims = self._claims_at_sync(query.scope, query.valid_at or now, query.known_at or now)
            return (temporal_candidates(claims, query.text, limit)
                    if MemoryChannel.SEMANTIC in query.channels else ())
        where, params = self._visible_scope_clause(query.scope)
        with self._connection() as connection:
            event_rows = connection.execute(
                f"SELECT * FROM events WHERE {where} "
                "AND archived_at IS NULL ORDER BY occurred_at DESC, id DESC",
                params,
            ).fetchall()
            artifact_rows = connection.execute(
                f"SELECT * FROM artifacts WHERE {where} AND archived_at IS NULL "
                "ORDER BY occurred_at DESC, id DESC",
                params,
            ).fetchall()

        query_tokens = _tokens(query.text)
        candidates: list[MemoryItem] = []
        if MemoryChannel.SEMANTIC in query.channels:
            now = utc_now()
            candidates.extend(temporal_candidates(
                self._claims_at_sync(query.scope, now, now), query.text, limit
            ))
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
        if query.channels:
            enabled_kinds.add(MemoryKind.BLOCK)
        for row in artifact_rows:
            kind = MemoryKind(row["kind"])
            if kind not in enabled_kinds:
                continue
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
                        "token_budget": (
                            block.token_budget if kind == MemoryKind.BLOCK else None
                        ),
                        "source_event_ids": provenance.source_event_ids,
                    },
                )
            )
        # Apply the limit only after scoring every visible item. A LIMIT on the
        # SQL rows silently made claim-key index order decide recall eligibility.
        return tuple(sorted(candidates, key=lambda item: item.score, reverse=True)[:limit])

    async def read_block(self, scope: MemoryScope, block_id: str) -> MemoryBlock | None:
        return await asyncio.to_thread(self._read_block_sync, scope, block_id)

    def _read_block_sync(self, scope: MemoryScope, block_id: str) -> MemoryBlock | None:
        where, params = self._visible_scope_clause(scope)
        with self._connection() as connection:
            row = connection.execute(
                f"SELECT * FROM artifacts WHERE {where} AND id = ? AND kind = ? "
                "AND archived_at IS NULL",
                (*params, block_id, MemoryKind.BLOCK),
            ).fetchone()
        return self._block_from_row(row) if row else None

    async def search_blocks(
        self,
        scope: MemoryScope,
        text: str,
        channels: Sequence[MemoryChannel],
        limit: int,
    ) -> Sequence[MemoryBlock]:
        return await asyncio.to_thread(self._search_blocks_sync, scope, text, channels, limit)

    def _search_blocks_sync(
        self,
        scope: MemoryScope,
        text: str,
        channels: Sequence[MemoryChannel],
        limit: int,
    ) -> Sequence[MemoryBlock]:
        where, params = self._visible_scope_clause(scope)
        with self._connection() as connection:
            rows = connection.execute(
                f"SELECT * FROM artifacts WHERE {where} AND kind = ? AND status = ? "
                "AND archived_at IS NULL LIMIT 500",
                (*params, MemoryKind.BLOCK, ArtifactStatus.ACTIVE),
            ).fetchall()
        query_tokens = _tokens(text)
        allowed = set(channels)
        ranked = [
            (self._overlap(query_tokens, row["text"]), self._block_from_row(row))
            for row in rows
        ]
        return tuple(
            block
            for _, block in sorted(ranked, key=lambda item: item[0], reverse=True)
            if block.channel in allowed
        )[:limit]

    async def forget(self, request: ForgetRequest) -> ForgetResult:
        async with self._write_lock:
            return await asyncio.to_thread(self._forget_sync, request)

    def _forget_admission_records(
        self, connection: sqlite3.Connection, request: ForgetRequest
    ) -> set[str]:
        where = "partition_key = ?"
        params: tuple[Any, ...] = (request.scope.partition_key(),)
        if not request.all_in_scope:
            placeholders = ",".join("?" for _ in request.memory_ids)
            where += f" AND id IN ({placeholders})"
            params = (*params, *request.memory_ids)
        events = connection.execute(
            f"SELECT id, partition_key, idempotency_key FROM events WHERE {where}", params
        ).fetchall()
        event_ids = {row["id"] for row in events}
        connection.executemany(
            """INSERT OR IGNORE INTO admission_tombstones
               (partition_key, event_id, idempotency_key) VALUES (?, ?, ?)""",
            ((row["partition_key"], row["id"], row["idempotency_key"]) for row in events),
        )
        claim_ids = {row["id"] for row in connection.execute(
            f"SELECT id FROM claims WHERE {where}", params
        ).fetchall()}
        rows = connection.execute(
            """SELECT * FROM admission_records
               WHERE json_extract(scope_json, '$.tenant_id') = ?
                 AND json_extract(scope_json, '$.namespace') = ?
                 AND COALESCE(json_extract(payload_json, '$.deleted'), 0) = 0""",
            (request.scope.tenant_id, request.scope.namespace),
        ).fetchall()
        from .contribution_state import erased_payload, scrub_transitions
        from .evidence_support import scrub_support

        def scrub(payload):
            first = scrub_support(payload, event_ids)
            return scrub_transitions(payload, event_ids) or first

        cleaned_rows = []
        scrubbed_at = None
        for original in rows:
            row = dict(original)
            payload = json.loads(row["payload_json"])
            if (payload.get("qualification") or payload.get("transitions")) and (
                row["event_id"] not in event_ids
            ):
                versions = connection.execute(
                    "SELECT version,payload_json FROM admission_versions WHERE record_id=?",
                    (row["record_id"],),
                ).fetchall()
                changed = scrub(payload)
                for version in versions:
                    past = json.loads(version["payload_json"])
                    if scrub(past):
                        changed = True
                        connection.execute(
                            "UPDATE admission_versions SET payload_json=? "
                            "WHERE record_id=? AND version=?",
                            (canonical_json(past), row["record_id"], version["version"]),
                        )
                if changed:
                    scrubbed_at = scrubbed_at or self._admission_publication_time(
                        connection, request.scope
                    )
                    row.update(
                        payload_json=canonical_json(payload),
                        version=row["version"] + 1,
                        recorded_at=scrubbed_at,
                    )
                    connection.execute(
                        "UPDATE admission_records SET payload_json=?,version=?,recorded_at=? "
                        "WHERE record_id=?",
                        (row["payload_json"], row["version"], scrubbed_at, row["record_id"]),
                    )
                    connection.execute(
                        "INSERT INTO admission_versions "
                        "(record_id,version,partition_key,payload_json,recorded_at) "
                        "VALUES (?,?,?,?,?)",
                        (
                            row["record_id"],
                            row["version"],
                            row["partition_key"],
                            row["payload_json"],
                            scrubbed_at,
                        ),
                    )
            cleaned_rows.append(row)
        rows = cleaned_rows
        dependencies = {}
        for row in rows:
            snapshots = [json.loads(row["payload_json"])]
            snapshots.extend(json.loads(version["payload_json"]) for version in connection.execute(
                "SELECT payload_json FROM admission_versions WHERE record_id = ?",
                (row["record_id"],),
            ).fetchall())
            sources = {row["event_id"]}
            published = set()
            for snapshot in snapshots:
                sources.update(self._admission_source_ids(snapshot))
                if isinstance(snapshot.get("claim_id"), str):
                    published.add(snapshot["claim_id"])
            dependencies[row["record_id"]] = (sources, published)
        invalidated: set[str] = set()
        invalidated_slots: set[tuple[str, str]] = set()
        changed = True
        while changed:
            changed = False
            for row in rows:
                if row["record_id"] in invalidated:
                    continue
                sources, published = dependencies[row["record_id"]]
                targeted = row["partition_key"] == request.scope.partition_key() and (
                    request.all_in_scope or row["record_id"] in request.memory_ids
                )
                slot = (row["partition_key"], row["slot_key"])
                if (
                    targeted or sources & event_ids or published & claim_ids
                    or slot in invalidated_slots
                ):
                    invalidated.add(row["record_id"])
                    # Without rebuilding from independent evidence, removing a later
                    # update must not silently resurrect an older value in this slot.
                    if not json.loads(row["payload_json"]).get("contribution"):
                        invalidated_slots.add(slot)
                    claim_ids.update(published)
                    changed = True
        deleted_at = (
            self._admission_publication_time(connection, request.scope) if invalidated else None
        )
        payload_by_id = {r["record_id"]: json.loads(r["payload_json"]) for r in rows}
        for record_id in invalidated:
            # Keep only terminal identity metadata so a stale worker cannot recreate this ID.
            connection.execute(
                "DELETE FROM admission_versions WHERE record_id = ?", (record_id,)
            )
            connection.execute(
                """UPDATE admission_records
                   SET payload_json = ?, version = version + 1, recorded_at = ?
                   WHERE record_id = ?""",
                (canonical_json(erased_payload(payload_by_id[record_id])), deleted_at, record_id),
            )
        from .operations import sqlite_index

        sqlite_index.invalidate_records(connection, invalidated)
        return claim_ids

    def _forget_sync(self, request: ForgetRequest) -> ForgetResult:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            return self._forget_on_connection(connection, request)

    def _forget_on_connection(self, connection, request, *, replay=False):
        sqlite_retention.forget(connection, request, journal=not replay)
        dependent_claim_ids = self._forget_admission_records(connection, request)

        def finish(result: ForgetResult) -> ForgetResult:
            return self._finish_admission_forget(
                connection, request, dependent_claim_ids, result
            )

        if request.all_in_scope:
            where = "partition_key = ?"
            params: tuple[Any, ...] = (request.scope.partition_key(),)
            counts = {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {where}", params
                ).fetchone()[0]
                for table in ("events", "claims", "artifacts")
            }
            if request.mode == ForgetMode.ARCHIVE:
                archived_at = _iso(utc_now())
                connection.execute(
                    f"UPDATE events SET archived_at = ? WHERE {where}",
                    (archived_at, *params),
                )
                connection.execute(
                    f"UPDATE claims SET archived_at = ?, status = ? WHERE {where}",
                    (archived_at, ClaimStatus.ARCHIVED, *params),
                )
                connection.execute(
                    f"UPDATE artifacts SET archived_at = ?, status = ? WHERE {where}",
                    (archived_at, ArtifactStatus.ARCHIVED, *params),
                )
                self._invalidate_feedback(
                    connection,
                    request.scope.partition_key(),
                    set(request.memory_ids),
                    erase=False,
                    all_in_scope=True,
                )
            else:
                self._invalidate_feedback(
                    connection,
                    request.scope.partition_key(),
                    set(request.memory_ids),
                    erase=True,
                    all_in_scope=True,
                )
                connection.execute(f"DELETE FROM artifacts WHERE {where}", params)
                connection.execute(f"DELETE FROM claims WHERE {where}", params)
                connection.execute(f"DELETE FROM events WHERE {where}", params)
            return finish(ForgetResult(
                affected_events=counts["events"],
                affected_claims=counts["claims"],
                affected_artifacts=counts["artifacts"],
                mode=request.mode,
            ))

        if not request.memory_ids:
            return finish(ForgetResult(0, 0, 0, request.mode))

        placeholders = ",".join("?" for _ in request.memory_ids)
        where = f"partition_key = ? AND id IN ({placeholders})"
        params = (request.scope.partition_key(), *request.memory_ids)

        event_rows = connection.execute(
            f"SELECT id FROM events WHERE {where}", params
        ).fetchall()
        target_event_ids = tuple(row["id"] for row in event_rows)
        if not target_event_ids:
            counts = {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {where}", params
                ).fetchone()[0]
                for table in ("claims", "artifacts")
            }
            if request.mode == ForgetMode.ARCHIVE:
                archived_at = _iso(utc_now())
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
            self._invalidate_feedback(
                connection,
                request.scope.partition_key(),
                set(request.memory_ids),
                erase=request.mode == ForgetMode.ERASE,
            )
            return finish(ForgetResult(0, counts["claims"], counts["artifacts"], request.mode))

        target_event_set = set(target_event_ids)
        event_placeholders = ",".join("?" for _ in target_event_ids)
        impacted_claim_ids = {
            row["claim_id"]
            for row in connection.execute(
                "SELECT DISTINCT claim_id FROM claim_sources "
                f"WHERE event_id IN ({event_placeholders})",
                target_event_ids,
            ).fetchall()
        }
        partition_claim_ids = {
            row["id"]
            for row in connection.execute(
                f"SELECT id FROM claims WHERE {where}", params
            ).fetchall()
        }
        partition_artifact_ids = {
            row["id"]
            for row in connection.execute(
                f"SELECT id FROM artifacts WHERE {where}", params
            ).fetchall()
        }

        claims_to_drop: set[str] = set()
        for claim_id in impacted_claim_ids:
            row = connection.execute(
                "SELECT provenance_json FROM claims WHERE id = ?", (claim_id,)
            ).fetchone()
            if row is None:
                continue
            provenance = _provenance(row["provenance_json"])
            updated_sources = tuple(
                source_id
                for source_id in provenance.source_event_ids
                if source_id not in target_event_set
            )
            if len(updated_sources) == len(provenance.source_event_ids):
                continue
            if not updated_sources:
                claims_to_drop.add(claim_id)
                continue
            scrubbed = [
                source_id
                for source_id in provenance.source_event_ids
                if source_id in target_event_set
            ]
            scrubbed_placeholders = ",".join("?" for _ in scrubbed)
            connection.execute(
                "DELETE FROM claim_sources "
                f"WHERE claim_id = ? AND event_id IN ({scrubbed_placeholders})",
                (claim_id, *scrubbed),
            )
            updated = Provenance(
                source_event_ids=updated_sources,
                extractor=provenance.extractor,
                provider=provenance.provider,
                model=provenance.model,
                prompt_version=provenance.prompt_version,
                source_uri=provenance.source_uri,
                created_at=provenance.created_at,
            )
            connection.execute(
                "UPDATE claims SET provenance_json = ? WHERE id = ?",
                (_provenance_json(updated), claim_id),
            )
            self._sync_claim_source_json(connection, claim_id)
            SQLiteClaimHistory(self).scrub_sources(connection, claim_id, target_event_set)

        changed_block_ids: set[str] = set()
        blocks_to_drop: set[str] = set()
        block_rows = connection.execute(
            """
            SELECT * FROM artifacts
            WHERE partition_key = ? AND kind = ? AND archived_at IS NULL
            """,
            (request.scope.partition_key(), MemoryKind.BLOCK),
        ).fetchall()
        for row in block_rows:
            block = self._block_from_row(row)
            remaining_event_ids = tuple(
                event_id for event_id in block.event_ids if event_id not in target_event_set
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
            connection.execute(
                """
                UPDATE artifacts
                SET payload_json = ?, provenance_json = ?, version = ?, occurred_at = ?
                WHERE id = ? AND version = ? AND archived_at IS NULL
                """,
                (
                    canonical_json(asdict(updated_block)),
                    _provenance_json(provenance),
                    updated_block.version,
                    _iso(updated_at),
                    block.id,
                    block.version,
                ),
            )

        artifact_rows = connection.execute(
            """
            SELECT * FROM artifacts
            WHERE partition_key = ? AND kind != ? AND archived_at IS NULL
            """,
            (request.scope.partition_key(), MemoryKind.BLOCK),
        ).fetchall()
        for row in artifact_rows:
            provenance = _provenance(row["provenance_json"])
            remaining_event_ids = tuple(
                event_id
                for event_id in provenance.source_event_ids
                if event_id not in target_event_set
            )
            if len(remaining_event_ids) == len(provenance.source_event_ids):
                continue
            changed_block_ids.add(row["id"])
            if not remaining_event_ids:
                blocks_to_drop.add(row["id"])
                continue
            updated_provenance = replace(
                provenance,
                source_event_ids=remaining_event_ids,
            )
            payload = json.loads(row["payload_json"])
            if isinstance(payload.get("provenance"), dict):
                payload["provenance"]["source_event_ids"] = list(remaining_event_ids)
            connection.execute(
                """
                UPDATE artifacts
                SET payload_json = ?, provenance_json = ?, version = version + 1
                WHERE id = ? AND archived_at IS NULL
                """,
                (
                    canonical_json(payload),
                    _provenance_json(updated_provenance),
                    row["id"],
                ),
            )

        counts = {
            table: connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {where}", params
            ).fetchone()[0]
            for table in ("events", "claims", "artifacts")
        }
        if claims_to_drop:
            extra_counts = len(claims_to_drop - partition_claim_ids)
            if request.mode == ForgetMode.ARCHIVE:
                archived_at = _iso(utc_now())
                placeholders = ",".join("?" for _ in claims_to_drop)
                connection.execute(
                    "UPDATE claims SET archived_at = ?, status = ? "
                    f"WHERE id IN ({placeholders})",
                    (archived_at, ClaimStatus.ARCHIVED, *tuple(claims_to_drop)),
                )
                connection.execute(
                    f"DELETE FROM claim_sources WHERE claim_id IN ({placeholders})",
                    tuple(claims_to_drop),
                )
            else:
                self._delete_claim_rows(connection, claims_to_drop)
        else:
            extra_counts = 0

        if blocks_to_drop:
            block_placeholders = ",".join("?" for _ in blocks_to_drop)
            if request.mode == ForgetMode.ARCHIVE:
                connection.execute(
                    "UPDATE artifacts SET archived_at = ?, status = ? "
                    f"WHERE id IN ({block_placeholders})",
                    (
                        _iso(utc_now()),
                        ArtifactStatus.ARCHIVED,
                        *tuple(blocks_to_drop),
                    ),
                )
            else:
                connection.execute(
                    f"DELETE FROM artifacts WHERE id IN ({block_placeholders})",
                    tuple(blocks_to_drop),
                )

        if request.mode == ForgetMode.ARCHIVE:
            archived_at = _iso(utc_now())
            connection.execute(
                f"UPDATE events SET archived_at = ? WHERE {where}",
                (archived_at, *params),
            )
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

        impacted_memory_ids = (
            set(request.memory_ids)
            | target_event_set
            | impacted_claim_ids
            | changed_block_ids
        )
        self._invalidate_feedback(
            connection,
            request.scope.partition_key(),
            impacted_memory_ids,
            erase=request.mode == ForgetMode.ERASE,
        )

        affected_claims = counts["claims"] + extra_counts
        if request.mode == ForgetMode.ARCHIVE:
            for claim_id in claims_to_drop:
                if claim_id in partition_claim_ids:
                    # already counted by claims WHERE ... in counts["claims"]
                    continue
                affected_claims += 1
        return finish(ForgetResult(
            affected_events=counts["events"],
            affected_claims=affected_claims,
            affected_artifacts=(
                counts["artifacts"]
                + len(changed_block_ids - partition_artifact_ids)
            ),
            mode=request.mode,
        ))


    def _finish_admission_forget(
        self,
        connection: sqlite3.Connection,
        request: ForgetRequest,
        dependent_claim_ids: set[str],
        result: ForgetResult,
    ) -> ForgetResult:
        extra_claims = 0
        for claim_id in dependent_claim_ids:
            row = connection.execute(
                "SELECT partition_key, archived_at FROM claims WHERE id = ?", (claim_id,)
            ).fetchone()
            if row is None:
                continue
            if request.mode == ForgetMode.ARCHIVE:
                if row["archived_at"] is None:
                    extra_claims += 1
                connection.execute(
                    "UPDATE claims SET archived_at = ?, status = ? WHERE id = ?",
                    (_iso(utc_now()), ClaimStatus.ARCHIVED, claim_id),
                )
            else:
                extra_claims += 1
                self._delete_claim_rows(connection, {claim_id})
            self._invalidate_feedback(
                connection, row["partition_key"], {claim_id},
                erase=request.mode == ForgetMode.ERASE,
            )
        return replace(result, affected_claims=result.affected_claims + extra_claims)

    @staticmethod
    def _invalidate_feedback(
        connection: sqlite3.Connection,
        partition_key: str,
        impacted_ids: set[str],
        *,
        erase: bool,
        all_in_scope: bool = False,
    ) -> None:
        rows = connection.execute(
            """
            SELECT * FROM evolution_records
            WHERE partition_key = ? AND invalidated_at IS NULL
            ORDER BY occurred_at, id
            """,
            (partition_key,),
        ).fetchall()
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
                payload = json.loads(row["payload_json"])
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

        invalidated_at = _iso(utc_now())
        for row in rows:
            if row["id"] not in invalidated:
                continue
            if erase:
                payload_json = canonical_json(
                    {
                        "id": row["id"],
                        "record_type": row["record_type"],
                        "redacted": True,
                        "reason": "source_erased",
                    }
                )
                connection.execute(
                    """
                    UPDATE evolution_records
                    SET feedback_status = ?, invalidated_at = ?, payload_json = ?,
                        payload_hash = ?
                    WHERE id = ?
                    """,
                    (
                        FeedbackStatus.INVALIDATED,
                        invalidated_at,
                        payload_json,
                        sha256(payload_json.encode("utf-8")).hexdigest(),
                        row["id"],
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE evolution_records
                    SET feedback_status = ?, invalidated_at = ? WHERE id = ?
                    """,
                    (FeedbackStatus.INVALIDATED, invalidated_at, row["id"]),
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
        SQLiteClaimHistory(self).record(connection, claim)
        for source_event_id in claim.provenance.source_event_ids:
            connection.execute(
                "INSERT OR IGNORE INTO claim_sources (claim_id, event_id) VALUES (?, ?)",
                (claim.id, source_event_id),
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
        payload_json = canonical_json(payload)
        provenance_json = _provenance_json(provenance)
        occurred_at_text = _iso(occurred_at)
        existing = connection.execute(
            """
            SELECT partition_key, kind, text, payload_json, status, version,
                   quality, provenance_json, occurred_at
            FROM artifacts WHERE id = ?
            """,
            (artifact_id,),
        ).fetchone()
        if existing is not None:
            if existing["partition_key"] != scope.partition_key() or existing["kind"] != kind:
                raise ValueError("artifact identity conflicts with an existing artifact")
            unchanged = (
                existing["text"] == text
                and existing["payload_json"] == payload_json
                and existing["status"] == status
                and int(existing["version"]) == version
                and float(existing["quality"]) == quality
                and existing["provenance_json"] == provenance_json
                and existing["occurred_at"] == occurred_at_text
            )
            if unchanged:
                return
            if version != int(existing["version"]) + 1:
                raise ValueError("artifact update must increment version by one")
            connection.execute(
                """
                UPDATE artifacts
                SET text=?, payload_json=?, status=?, version=?, quality=?,
                    provenance_json=?, occurred_at=?, archived_at=NULL
                WHERE id=?
                """,
                (
                    text,
                    payload_json,
                    status,
                    version,
                    quality,
                    provenance_json,
                    occurred_at_text,
                    artifact_id,
                ),
            )
            return
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
                payload_json,
                status,
                version,
                quality,
                provenance_json,
                occurred_at_text,
            ),
        )

    def _block_from_row(self, row: sqlite3.Row) -> MemoryBlock:
        payload = json.loads(row["payload_json"])
        created_at = _datetime(payload.get("created_at"))
        updated_at = _datetime(payload.get("updated_at"))
        provenance = _provenance(row["provenance_json"])
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
            created_at=created_at or (_datetime(row["occurred_at"]) or utc_now()),
            updated_at=updated_at or (_datetime(row["occurred_at"]) or utc_now()),
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
        payload_json = canonical_json(payload)
        parent_id = None
        if record_type == "outcome":
            parent_id = payload.get("decision_id")
        elif record_type == "evaluation":
            parent_id = payload.get("outcome_id")
        elif record_type == "reward":
            parent_id = payload.get("evaluation_id") or payload.get("outcome_id")
        connection.execute(
            """
            INSERT INTO evolution_records (
                id, partition_key, tenant_id, namespace, user_id, agent_id,
                workspace_id, session_id, record_type, payload_json, feedback_status,
                parent_id, idempotency_key, payload_hash, corrects_id, expires_at, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_id,
                *_scope_values(scope),
                record_type,
                payload_json,
                str(payload.get("feedback_status", FeedbackStatus.ACCEPTED)),
                parent_id,
                payload.get("idempotency_key"),
                sha256(payload_json.encode("utf-8")).hexdigest(),
                payload.get("corrects_id"),
                (
                    _iso(payload["expires_at"])
                    if isinstance(payload.get("expires_at"), datetime)
                    else payload.get("expires_at")
                ),
                _iso(occurred_at),
            ),
        )

    @staticmethod
    def _feedback_from_row(row: sqlite3.Row) -> dict[str, object]:
        return {
            "id": row["id"],
            "partition_key": row["partition_key"],
            "record_type": row["record_type"],
            "payload": json.loads(row["payload_json"]),
            "feedback_status": row["feedback_status"],
            "parent_id": row["parent_id"],
            "idempotency_key": row["idempotency_key"],
            "payload_hash": row["payload_hash"],
            "corrects_id": row["corrects_id"],
            "expires_at": _datetime(row["expires_at"]),
            "invalidated_at": row["invalidated_at"],
        }

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
