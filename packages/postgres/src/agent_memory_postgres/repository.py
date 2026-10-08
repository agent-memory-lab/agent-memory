from __future__ import annotations

import json
from collections.abc import Sequence
from copy import deepcopy
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
from agent_memory.operations.artifact_dependencies import (
    ArtifactValidity,
    affected_memory_keys,
    dependency_ids,
)
from agent_memory.retrieval.temporal_history import temporal_candidates
from agent_memory.serialization import to_jsonable

from . import admission, retention
from .temporal_history import PostgresClaimHistory


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
    derived_coverage_contract = "write-hooks/1"
    derived_parent_contract = "processing-graph/1"
    derived_page_contract = "page-full-rebuild/1"

    def __init__(self, repository: PostgresMemoryRepository) -> None:
        self._repository = repository
        self._connection_context: Any = None
        self._transaction_context: Any = None
        self.connection: Any = None
        self._admission_batch_times: dict[tuple[str, str], datetime] = {}

    async def __aenter__(self) -> PostgresMemoryUnitOfWork:
        self._connection_context = self._repository.pool.connection()
        self.connection = await self._connection_context.__aenter__()
        self._transaction_context = self.connection.transaction()
        await self._transaction_context.__aenter__()
        self._admission_batch_times = {}
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
        await self.lock_admission_scope(scope)
        await admission.check_event_identity(self.connection, scope, None, idempotency_key)
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
        await admission.lock_scope(self.connection, scope)
        cursor = await self.connection.execute(
            "SELECT feedback_status, invalidated_at FROM agent_memory_evolution_records "
            "WHERE partition_key=%s AND id=%s FOR UPDATE", (scope.partition_key(), record_id),
        )
        previous = await cursor.fetchone()
        if previous and (previous["feedback_status"] == FeedbackStatus.INVALIDATED
                         or previous["invalidated_at"] is not None):
            raise ValueError("corrected feedback is no longer valid")
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
        # Hold the same namespace fence as forget until the artifact commit.
        await admission.lock_scope(self.connection, scope)
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
        event = deepcopy(event)
        await self.lock_admission_scope(event.scope)
        await admission.check_event_identity(
            self.connection, event.scope, event.id, event.idempotency_key
        )
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

    async def derived_get(self, scope, kind, identity):
        from . import derived

        return await derived.get(self.connection, scope, kind, identity)

    async def derived_put(self, scope, kind, identity, payload):
        payload = deepcopy(payload)
        from . import derived

        return await derived.put(self.connection, scope, kind, identity, payload)

    async def derived_records(self, scope, kind):
        from . import derived

        return await derived.records(self.connection, scope, kind)

    async def derived_header(self, scope, record_id):
        from . import derived

        return await derived.get_header(self.connection, scope, record_id)

    async def derived_headers(self, scope):
        await self.lock_admission_scope(scope)
        from . import derived

        return await derived.headers(self.connection, scope)

    async def derived_candidates(self, scope, slots):
        from . import derived

        return await derived.candidates(self.connection, scope, slots)

    async def derived_edges(self, scope, revision_id, values):
        values = deepcopy(tuple(values))
        from . import derived

        return await derived.edges(self.connection, scope, revision_id, values)

    async def derived_reverse(self, scope, parent):
        from . import derived

        return await derived.reverse(self.connection, scope, parent)

    async def retention_head_get(self, scope, kind, identity):
        return await retention.head_get(self.connection, scope, kind, identity)

    async def retention_head_put(self, scope, kind, identity, payload, expected_generation):
        payload = deepcopy(payload)
        await self.lock_admission_scope(scope)
        result = await retention.head_put(
            self.connection, scope, kind, identity, payload, expected_generation
        )
        if kind == "interpretation":
            from agent_memory.derived.service import interpretation_changed

            batch_at = self._admission_batch_times.get((scope.tenant_id, scope.namespace))
            await interpretation_changed(
                self, scope, identity, at=max(utc_now(), batch_at) if batch_at else utc_now()
            )
        elif kind == "document":
            from agent_memory.derived.service import document_changed

            await document_changed(self, scope, at=utc_now())
        return result

    async def get_source_event(self, scope, event_id):
        cursor = await self.connection.execute(
            "SELECT * FROM agent_memory_events "
            "WHERE partition_key=%s AND id=%s AND archived_at IS NULL",
            (scope.partition_key(), event_id),
        )
        row = await cursor.fetchone()
        return self._repository._event_from_row(row) if row else None

    async def retention_update(self, scope, request_id, payload):
        return await retention.update(self.connection, scope, request_id, payload)

    async def retention_active(self, scope):
        return await retention.active(self.connection, scope)

    async def delivery_get(self, scope, kind, identity):
        from . import delivery

        return await delivery.get(self.connection, scope, kind, identity)

    async def delivery_insert(self, scope, kind, identity, payload):
        from . import delivery

        return await delivery.insert(self.connection, scope, kind, identity, payload)

    async def delivery_count(self, scope, kind):
        from . import delivery

        return await delivery.count(self.connection, scope, kind)

    async def refresh_get(self, scope, kind, identity):
        from . import refresh

        return await refresh.get(self.connection, scope, kind, identity)

    async def refresh_put(self, scope, kind, identity, payload):
        from . import refresh

        return await refresh.put(self.connection, scope, kind, identity, payload)

    async def refresh_records(self, scope, kind):
        from . import refresh

        return await refresh.records(self.connection, scope, kind)

    async def index_recovery_get(self, scope, channel, epoch, kind, identity):
        from . import index

        return await index.recovery_get(self.connection, scope, channel, epoch, kind, identity)

    async def index_recovery_put(self, scope, channel, epoch, kind, identity, payload):
        from . import index

        return await index.recovery_put(
            self.connection, scope, channel, epoch, kind, identity, payload
        )

    async def index_recovery_count(self, scope, channel, epoch, kind):
        from . import index

        return await index.recovery_count(self.connection, scope, channel, epoch, kind)

    async def retention_requests(self, scope):
        return await retention.requests(self.connection, scope)

    async def index_job_position(self, scope, channel, epoch, publication_id):
        from . import index

        return await index.position(self.connection, scope, channel, epoch, publication_id)

    async def index_job_get(self, scope, channel, epoch, token_id):
        from . import index
        return await index.job_get(self.connection, scope, channel, epoch, token_id)

    async def index_job_put(self, scope, row):
        from . import index
        return await index.job_put(self.connection, scope, row)

    async def index_jobs(self, scope, channel, epoch):
        from . import index
        return await index.jobs(self.connection, scope, channel, epoch)

    async def index_document_get(self, scope, channel, candidate_id):
        from . import index
        return await index.document_get(self.connection, scope, channel, candidate_id)

    async def index_document_put(self, scope, channel, candidate_id, document):
        from . import index
        return await index.document_put(self.connection, scope, channel, candidate_id, document)

    async def index_lookup(self, scope, channel, slot_key, limit):
        from . import index
        return await index.lookup(self.connection, scope, channel, slot_key, limit)

    async def forget_for_restore(self, request):
        return await self._repository._forget_on_connection(
            self.connection, request, replay=True
        )

    async def purge_import(self, scope, entry):
        from . import purge

        return await purge.import_entry(self.connection, scope, entry)

    async def purge_restore_get(self, scope, identity):
        from . import purge

        return await purge.restore_get(self.connection, scope, identity)

    async def purge_restore_put(self, scope, identity, payload):
        from . import purge

        return await purge.restore_put(self.connection, scope, identity, payload)

    async def purge_restore_count(self, scope):
        from . import purge

        return await purge.restore_count(self.connection, scope)

    async def purge_head(self, scope):
        from . import purge

        return await purge.head(self.connection, scope)

    async def purge_page(self, scope, after, limit):
        from . import purge

        return await purge.page(self.connection, scope, after, limit)

    async def source_erased(self, scope, identity):
        from . import purge

        return await purge.erased(self.connection, scope, identity)

    async def producer_get(self, scope, producer_id):
        return await retention.producer_get(self.connection, scope, producer_id)

    async def producer_put(self, scope, producer_id, payload):
        return await retention.producer_put(self.connection, scope, producer_id, payload)

    async def retention_epoch(self, scope):
        return await retention.epoch(self.connection, scope)

    async def retention_get(self, scope, kind, request_id):
        return await retention.get(self.connection, scope, kind, request_id)

    async def retention_insert(self, scope, kind, request_id, payload):
        await retention.insert(self.connection, scope, kind, request_id, payload)

    async def retention_count(self, scope, kind):
        return await retention.count(self.connection, scope, kind)

    async def retention_identity_owner(self, scope, event_id, idempotency_key):
        return await retention.identity_owner(self.connection, scope, event_id, idempotency_key)

    async def lock_admission_scope(self, scope: MemoryScope) -> None:
        await admission.lock_scope(self.connection, scope)

    async def get_admission_record(self, scope: MemoryScope, record_id: str):
        rows = await admission.read_records(
            self.connection, scope, visible=False, record_id=record_id
        )
        return rows[0] if rows else None

    async def list_admission_records(self, scope: MemoryScope, slot_key: str | None = None):
        return await admission.read_records(
            self.connection, scope, visible=False, slot_key=slot_key
        )

    async def list_admission_barriers(self, scope: MemoryScope, slot_key: str):
        rows = await admission.read_records(
            self.connection, scope, visible=False, slot_key=slot_key, barriers=True
        )
        return tuple(r for r in rows if "contribution_barrier" in r["payload"])

    async def save_admission_record(
        self, scope: MemoryScope, record_id: str, event_id: str, slot_key: str,
        payload: dict[str, Any], expected_version: int,
    ) -> int:
        payload = deepcopy(payload)
        await self.lock_admission_scope(scope)
        namespace = (scope.tenant_id, scope.namespace)
        if namespace not in self._admission_batch_times:
            self._admission_batch_times[namespace] = await admission.publication_time(
                self.connection, scope
            )
        old_header = await self.derived_header(scope, record_id)
        result = await admission.save_record(
            self.connection, scope, record_id, event_id, slot_key, payload, expected_version,
            recorded_at=self._admission_batch_times[namespace],
        )
        from agent_memory.derived.service import mark_slot_changed

        from . import derived

        # Read only the row just written. Public snapshot readers have their own
        # consistency/observation hooks and must not be re-entered by this write.
        cursor = await self.connection.execute(
            "SELECT payload_json,version FROM agent_memory_admission_records "
            "WHERE partition_key=%s AND record_id=%s",
            (scope.partition_key(), record_id),
        )
        persisted = await cursor.fetchone()
        if persisted is None or persisted["version"] != result:
            raise ValueError("admission publication disappeared before routing")
        await derived.header(
            self.connection, scope, record_id, event_id, slot_key, persisted["payload_json"], result
        )
        await mark_slot_changed(
            self, scope, slot_key, at=self._admission_batch_times[namespace],
            old_header=old_header, new_header=await self.derived_header(scope, record_id),
        )
        return result

    async def find_current_claim(self, scope: MemoryScope, key: str) -> Claim | None:
        # Match ingest and deletion: namespace lock must precede Claim locks.
        await self.lock_admission_scope(scope)
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

    async def claim_is_effective(self, claim: Claim, valid_at: datetime) -> bool:
        claims = await PostgresClaimHistory(self._repository).read(
            self.connection, claim.scope, valid_at, utc_now()
        )
        return any(current.id == claim.id for current in claims)

    async def save_claim(self, claim: Claim) -> None:
        await self._repository._insert_claim(self.connection, claim)

    async def replace_current_claim(self, previous: Claim, current: Claim) -> None:
        cursor = await self.connection.execute(
            """
            UPDATE agent_memory_claims
            SET status = 'superseded', valid_to = %s, superseded_by = %s
            WHERE id = %s AND status = 'active' AND version = %s
            """,
            (
                current.valid_from
                if current.valid_from > previous.valid_from else previous.valid_to,
                current.id, previous.id, previous.version,
            ),
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
        await PostgresClaimHistory(self._repository).refresh_evidence(self.connection, claim_id)

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
        await self._repository._validate_artifact_write(self.connection, episode)
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
        await self._repository._validate_artifact_write(self.connection, procedure)
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
        await self._repository._validate_artifact_write(self.connection, block)
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
                await PostgresClaimHistory(self).initialize(connection)
                from . import derived

                cursor = await connection.execute(
                    "SELECT r.* FROM agent_memory_admission_records r "
                    "LEFT JOIN agent_memory_derived_atom_headers h "
                    "ON r.partition_key=h.partition_key AND r.record_id=h.identity "
                    "WHERE h.identity IS NULL"
                )
                for row in await cursor.fetchall():
                    await derived.header(
                        connection,
                        MemoryScope(**row["scope_json"]),
                        row["record_id"],
                        row["event_id"],
                        row["slot_key"],
                        row["payload_json"],
                        row["version"],
                        routes=False,
                    )

    async def close(self) -> None:
        await self.pool.close()

    def unit_of_work(self) -> PostgresMemoryUnitOfWork:
        return PostgresMemoryUnitOfWork(self)

    async def admission_records(self, scope: MemoryScope, *, slot_key: str | None = None):
        async with self.pool.connection() as connection:
            return await admission.read_records(connection, scope, visible=True, slot_key=slot_key)

    async def admission_snapshot(self, scope: MemoryScope) -> tuple[dict[str, Any], ...]:
        async with self.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                return await admission.read_snapshot(connection, scope)

    async def admission_record(self, scope: MemoryScope, record_id: str):
        async with self.pool.connection() as connection:
            rows = await admission.read_records(
                connection, scope, visible=True, record_id=record_id
            )
            return rows[0] if rows else None

    async def admission_record_versions(self, scope: MemoryScope, record_id: str):
        async with self.pool.connection() as connection:
            return await admission.read_versions(connection, scope, record_id)

    async def admission_protected_sources(self, scope: MemoryScope) -> tuple[str, ...]:
        async with self.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                return await admission.protected_sources(connection, scope)

    async def claims_at(self, scope, *, valid_at, known_at):
        async with self.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                return await PostgresClaimHistory(self).read(connection, scope, valid_at, known_at)

    async def current_claims(self, scope: MemoryScope) -> Sequence[Claim]:
        now = utc_now()
        return await self.claims_at(scope, valid_at=now, known_at=now)

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
        if query.valid_at is not None or query.known_at is not None:
            now = utc_now()
            claims = await self.claims_at(
                query.scope, valid_at=query.valid_at or now, known_at=query.known_at or now
            )
            return (
                temporal_candidates(claims, query.text, limit)
                if MemoryChannel.SEMANTIC in query.channels else ()
            )
        where, params = self._visible_scope_clause(query.scope)
        text = query.text.strip()
        candidates: list[MemoryItem] = []
        current_claims = (
            await self.current_claims(query.scope)
            if MemoryChannel.SEMANTIC in query.channels else ()
        )
        canonical = {
            item.id: item
            for item in temporal_candidates(current_claims, text, len(current_claims))
        }
        async with self.pool.connection() as connection:
            if MemoryChannel.SEMANTIC in query.channels:
                # Resolve effective IDs before acquiring this connection so even
                # a pool of size one can search. Keep PostgreSQL full-text ranking.
                claims = await self._search_rows(
                    connection,
                    "agent_memory_claims",
                    where + " AND archived_at IS NULL AND id = ANY(%s)",
                    (*params, list(canonical)),
                    text,
                    limit,
                )
                for row in claims:
                    candidates.append(
                        replace(
                            canonical[row["id"]],
                            score=float(row["rank"]) + 0.25 * row["importance"]
                            + 0.20 * row["confidence"],
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
                artifacts = await self._search_live_artifacts(
                    connection,
                    query.scope,
                    where + f" AND {self._artifact_validity_sql()} AND kind = ANY(%s)",
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

    @staticmethod
    def _artifact_validity_sql() -> str:
        """Fail closed on stale evidence before all direct and ranked reads."""
        table = "agent_memory_artifacts"
        visibility = " AND ".join(
            f"(source.{field} IS NULL OR source.{field} = {table}.{field})"
            for field in ("user_id", "agent_id", "workspace_id", "session_id")
        )

        def sources_valid(document, field):
            value = f"{table}.{document} -> '{field}'"
            array = f"CASE WHEN jsonb_typeof({value})='array' THEN {value} ELSE '[]'::jsonb END"
            return (
                f"jsonb_array_length({array}) > 0 "
                f"AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements({array}) AS evidence(value) "
                "WHERE jsonb_typeof(evidence.value) != 'string' OR NOT EXISTS "
                "(SELECT 1 FROM agent_memory_events source "
                "WHERE source.id = evidence.value #>> '{}' AND source.archived_at IS NULL "
                f"AND source.tenant_id = {table}.tenant_id "
                f"AND source.namespace = {table}.namespace AND {visibility}))"
            )

        provenance = sources_valid("provenance_json", "source_event_ids")
        block_sources = sources_valid("payload_json", "event_ids")
        return (
            f"{table}.archived_at IS NULL AND {table}.status IN ('active', 'candidate') "
            f"AND ({provenance}) AND ({table}.kind != 'block' OR "
            f"({table}.status = 'active' AND {block_sources}))"
        )

    async def read_block(self, scope: MemoryScope, block_id: str) -> MemoryBlock | None:
        where, params = self._visible_scope_clause(scope)
        async with self.pool.connection() as connection:
            cursor = await connection.execute(
                f"SELECT * FROM agent_memory_artifacts WHERE {where} AND id = %s "
                f"AND kind = %s AND {self._artifact_validity_sql()}",
                (*params, block_id, str(MemoryKind.BLOCK)),
            )
            row = await cursor.fetchone()
            if row and not await self._live_artifact_rows(connection, scope, [row]):
                row = None
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
            rows = await self._search_live_artifacts(
                connection,
                scope,
                where + f" AND kind = %s AND {self._artifact_validity_sql()} "
                "AND COALESCE(payload_json ->> 'channel', 'semantic')=ANY(%s)",
                (*params, str(MemoryKind.BLOCK), list(map(str, channels))),
                text.strip(),
                limit,
            )
        allowed = set(channels)
        return tuple(
            block
            for block in (self._block_from_row(row) for row in rows)
            if block.channel in allowed
        )[:limit]

    async def forget(self, request: ForgetRequest) -> ForgetResult:
        async with self.pool.connection() as connection:
            async with connection.transaction():
                return await self._forget_on_connection(connection, request)

    async def _artifact_dependency_rows(
        self, connection, scope, identities=None, *, tombstones=False, lock=False
    ):
        rows = []
        tables = [
            ("events", ", archived_at"), ("claims", ", provenance_json, archived_at, status"),
            ("artifacts", ", payload_json, provenance_json, archived_at, status, kind"),
            ("evolution_records", ", payload_json, parent_id, feedback_status, invalidated_at"),
        ]
        if tombstones:
            tables.append(("memory_tombstones", ", memory_table"))
        for table, columns in tables:
            filter_sql = " AND id=ANY(%s)" if identities is not None else ""
            params = (scope.tenant_id, scope.namespace)
            if identities is not None:
                params += (list(identities),)
            cursor = await connection.execute(
                "SELECT id, partition_key, tenant_id, namespace, user_id, agent_id, "
                f"workspace_id, session_id{columns} FROM agent_memory_{table} "
                "WHERE tenant_id=%s AND namespace=%s" + filter_sql
                + (" FOR UPDATE" if lock else ""),
                params,
            )
            for original in await cursor.fetchall():
                row = dict(original, table="feedback" if table == "evolution_records" else table)
                for field in ("payload", "provenance"):
                    row[field] = row.pop(field + "_json", {})
                rows.append(row)
        return rows

    async def _search_live_artifacts(self, connection, scope, where, params, text, limit):
        """Hydrate bounded matching pages; revoked entries never consume the limit."""
        accepted, seen = [], set()
        batch_size = min(max(limit, 32), 256)
        while len(accepted) < limit:
            rows = await self._search_rows(
                connection, "agent_memory_artifacts", where + " AND NOT (id=ANY(%s))",
                (*params, list(seen)), text, batch_size,
            )
            if not rows:
                break
            seen.update(row["id"] for row in rows)
            accepted.extend(await self._live_artifact_rows(connection, scope, rows))
            if len(rows) < batch_size:
                break
        return accepted[:limit]

    async def _live_artifact_rows(self, connection, scope, rows):
        candidates = [dict(
            row, table="artifacts", payload=_object(row["payload_json"]),
            provenance=_object(row["provenance_json"]),
        ) for row in rows]
        validity = await self._artifact_validity(connection, scope, candidates)
        return [row for row, candidate in zip(rows, candidates, strict=True)
                if validity.accepts(candidate)]

    async def _artifact_validity(self, connection, scope, candidates):
        graph = []
        pending = {row["id"] for row in candidates}
        for row in candidates:
            pending.update(dependency_ids(row))
        seen = set()
        while pending:
            batch = set(sorted(pending)[:256])
            pending.difference_update(batch)
            seen.update(batch)
            rows = await self._artifact_dependency_rows(
                connection, scope, batch, tombstones=True
            )
            graph.extend(rows)
            for row in rows:
                pending.update(dependency_ids(row) - seen)
        graph.extend(candidates)
        return ArtifactValidity(graph)

    async def _validate_artifact_write(self, connection, item):
        await admission.lock_scope(connection, item.scope)
        payload = to_jsonable(item)
        row = dict(
            **to_jsonable(item.scope), partition_key=item.scope.partition_key(),
            id=item.id, table="artifacts", kind=("block" if isinstance(item, MemoryBlock)
                else "episode" if isinstance(item, Episode) else "procedure"),
            payload=payload, provenance=payload["provenance"], status=str(item.status),
        )
        validity = await self._artifact_validity(connection, item.scope, [row])
        if not validity.accepts(row, writing=True):
            raise ValueError(
                "artifact dependencies are missing, inactive, or outside authorized scope"
            )

    async def _forget_artifact_dependencies(self, connection, request, claim_ids, rows):
        direct = {(row["table"], row["id"]) for row in rows if (
            row["partition_key"] == request.scope.partition_key()
            and (request.all_in_scope or row["id"] in request.memory_ids)
        )}
        impacted = direct | {("claims", identity) for identity in claim_ids}
        event_ids = {identity for table, identity in direct if table == "events"}
        impacted.update(("claims", row["id"]) for row in rows if (
            row["table"] == "claims"
            and event_ids.intersection(row["provenance"].get("source_event_ids", ()))
        ))
        affected = affected_memory_keys(rows, impacted)
        dependent = {identity for table, identity in affected - direct if table == "artifacts"}
        erased = set(direct)
        erased.update(("artifacts", identity) for identity in dependent)
        erased.update(("claims", identity) for identity in claim_ids)
        erased.update(("claims", row["id"]) for row in rows if (
            row["table"] == "claims"
            and event_ids.intersection(row["provenance"].get("source_event_ids", ()))
            and not set(row["provenance"].get("source_event_ids", ())) - event_ids
        ))
        if request.mode == ForgetMode.ERASE:
            for row in rows:
                if row["table"] in {"events", "claims", "artifacts"} and (
                    row["table"], row["id"]
                ) in erased:
                    await connection.execute(
                        "INSERT INTO agent_memory_memory_tombstones VALUES "
                        "(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (row["partition_key"], row["id"], row["table"], *(
                            row[field] for field in (
                                "tenant_id", "namespace", "user_id", "agent_id",
                                "workspace_id", "session_id",
                            )
                        )),
                    )
        if dependent:
            if request.mode == ForgetMode.ERASE:
                await connection.execute(
                    "DELETE FROM agent_memory_artifacts WHERE id=ANY(%s)", (list(dependent),)
                )
            else:
                await connection.execute(
                    "UPDATE agent_memory_artifacts SET archived_at=now(), status='archived' "
                    "WHERE id=ANY(%s)", (list(dependent),),
                )
        for partition in {row["partition_key"] for row in rows if row["table"] == "feedback"}:
            identities = {row["id"] for row in rows if (
                row["table"] == "feedback" and row["partition_key"] == partition
                and ("feedback", row["id"]) in affected
            )}
            if identities:
                await self._invalidate_feedback(
                    connection, partition, identities, erase=request.mode == ForgetMode.ERASE,
                    all_in_scope=False, exact=True,
                )
        return len(dependent)

    async def _forget_on_connection(self, connection, request, *, replay=False):
        table_names = (
            "agent_memory_events", "agent_memory_claims", "agent_memory_artifacts",
        )
        await admission.lock_scope(connection, request.scope)
        from . import derived

        checkpoint = await derived.admission_checkpoint(connection, request.scope)
        await derived.forget(connection, request)
        await retention.forget(connection, request, journal=not replay)
        dependency_rows = await self._artifact_dependency_rows(
            connection, request.scope, lock=True
        )
        admission_claim_ids, extra_admission_claims = await admission.forget_records(
            connection, request
        )
        # Primary deletion also changes admitted projections in broader scopes.
        # Translate only actually changed candidates, never the all-in-scope flag.
        affected_scopes = {request.scope}
        for target, identities in await derived.changed_admission_scopes(
            connection, request.scope, checkpoint
        ):
            if target == request.scope:
                continue
            projected = replace(
                request, scope=target, all_in_scope=False,
                memory_ids=tuple(sorted(set(request.memory_ids).union(identities))),
            )
            await derived.forget(connection, projected)
            affected_scopes.add(target)
        for target in sorted(affected_scopes, key=lambda value: value.partition_key()):
            await derived.reconcile_headers(connection, target)
            await derived.scrub_routes(connection, target)
        dependent_artifacts = await self._forget_artifact_dependencies(
            connection, request, admission_claim_ids, dependency_rows
        )
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

        cursor = await connection.execute(
            f"SELECT id FROM agent_memory_events WHERE {where}", params
        )
        target_event_ids = {row["id"] for row in await cursor.fetchall()}

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
        # Events are gone or archived, but JSON provenance has no FK.
        # Scrub dependent claims in the same transaction, in bounded
        # pages. Directly targeted claims are already counted above.
        if target_event_ids:
            after_id = None
            while True:
                cursor = await connection.execute(
                    """SELECT id, provenance_json FROM agent_memory_claims
                    WHERE tenant_id = %s AND namespace = %s
                      AND provenance_json -> 'source_event_ids' ?| %s
                      AND (%s OR archived_at IS NULL)
                      AND NOT (partition_key = %s AND (%s OR id = ANY(%s)))
                      AND (%s::text IS NULL OR id > %s)
                    ORDER BY id LIMIT 128 FOR UPDATE""",
                    (request.scope.tenant_id, request.scope.namespace, list(target_event_ids),
                     request.mode == ForgetMode.ERASE,
                     request.scope.partition_key(), request.all_in_scope,
                     list(request.memory_ids), after_id, after_id),
                )
                rows = await cursor.fetchall()
                if not rows:
                    break
                for row in rows:
                    await PostgresClaimHistory(self).scrub_sources(
                        connection, row["id"], target_event_ids
                    )
                    provenance = _provenance(row["provenance_json"])
                    remaining = tuple(event_id for event_id in provenance.source_event_ids
                                      if event_id not in target_event_ids)
                    if not remaining and request.mode == ForgetMode.ERASE:
                        await connection.execute(
                            "DELETE FROM agent_memory_claims WHERE id = %s",
                            (row["id"],),
                        )
                    else:
                        await connection.execute(
                            """UPDATE agent_memory_claims
                            SET provenance_json = %s::jsonb,
                                archived_at = CASE WHEN %s THEN now() ELSE archived_at END,
                                status = CASE WHEN %s THEN 'archived' ELSE status END
                            WHERE id = %s""",
                            (
                                _json(
                                    replace(provenance, source_event_ids=remaining)
                                    if remaining else provenance
                                ),
                                not remaining, not remaining, row["id"],
                            ),
                        )
                    counts["agent_memory_claims"] += 1
                after_id = rows[-1]["id"]
        return ForgetResult(
            affected_events=counts["agent_memory_events"],
            affected_claims=counts["agent_memory_claims"] + extra_admission_claims,
            affected_artifacts=counts["agent_memory_artifacts"] + dependent_artifacts,
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
        exact: bool = False,
    ) -> None:
        cursor = await connection.execute(
            """
            SELECT * FROM agent_memory_evolution_records
            WHERE partition_key = %s AND (%s OR invalidated_at IS NULL)
            ORDER BY occurred_at, id
            FOR UPDATE
            """,
            (partition_key, erase),
        )
        rows = await cursor.fetchall()
        invalidated = (
            {row["id"] for row in rows}
            if all_in_scope
            else {row["id"] for row in rows if row["id"] in impacted_ids}
        )
        changed = not exact
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
        await PostgresClaimHistory(self).record(connection, claim)


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
        limit: int | None,
    ) -> list[dict[str, Any]]:
        limit_sql = " LIMIT %s" if limit is not None else ""
        limit_params = (limit,) if limit is not None else ()
        if text:
            cursor = await connection.execute(
                f"""
                SELECT *, ts_rank(search_document, plainto_tsquery('simple', %s)) AS rank
                FROM {table}
                WHERE {where} AND search_document @@ plainto_tsquery('simple', %s)
                ORDER BY rank DESC
                """ + limit_sql,
                (text, *where_params, text, *limit_params),
            )
        else:
            cursor = await connection.execute(
                f"SELECT *, 0.0 AS rank FROM {table} WHERE {where}" + limit_sql,
                (*where_params, *limit_params),
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
