"""PostgreSQL persistence for generic ingestion's at-least-once scheduler handoff."""

import json
from dataclasses import asdict
from datetime import UTC, timedelta
from uuid import uuid4

from agent_memory.domain import MemoryScope, canonical_json, utc_now
from agent_memory.operations.consolidation_outbox import (
    ConsolidationCancellation,
    ConsolidationDelivery,
    restore_ingest_result,
)


async def save(connection, event, result):
    if result.event_id != event.id:
        raise ValueError("consolidation intent event/result mismatch")
    cursor = await connection.execute(
        "SELECT content_hash FROM agent_memory_events WHERE id=%s AND partition_key=%s "
        "AND archived_at IS NULL",
        (event.id, event.scope.partition_key()),
    )
    source = await cursor.fetchone()
    if source is None or source["content_hash"] != event.content_hash:
        raise ValueError("consolidation requires a live committed source")
    await connection.execute(
        "INSERT INTO agent_memory_consolidation_outbox "
        "(event_id, partition_key, source_hash, result_json, status, created_at) "
        "VALUES (%s,%s,%s,%s::jsonb, 'pending', %s) ON CONFLICT(event_id) DO NOTHING",
        (
            event.id,
            event.scope.partition_key(),
            event.content_hash,
            canonical_json(asdict(result)),
            utc_now().astimezone(UTC),
        ),
    )


async def claim(connection, repository, *, scope=None, event_id=None, lease_seconds=60):
    if type(lease_seconds) is not int or not 5 <= lease_seconds <= 3600:
        raise ValueError("outbox lease must be between 5 and 3600 seconds")
    now = utc_now().astimezone(UTC)
    where = "(o.status='pending' OR (o.status='delivering' AND o.lease_expires_at<=%s))"
    params = [now]
    if scope is not None:
        where += " AND o.partition_key=%s"
        params.append(scope.partition_key())
    if event_id is not None:
        where += " AND o.event_id=%s"
        params.append(event_id)
    # Lock only the intent. Source content is rechecked after the lease commits;
    # no database transaction is held while awaiting the scheduler's queue.
    cursor = await connection.execute(
        "SELECT o.* FROM agent_memory_consolidation_outbox o "
        "JOIN agent_memory_events e ON e.id=o.event_id AND e.partition_key=o.partition_key "
        "AND e.archived_at IS NULL AND e.content_hash=o.source_hash "
        f"WHERE {where} ORDER BY o.created_at, o.event_id LIMIT 1 "
        "FOR UPDATE OF o SKIP LOCKED",
        tuple(params),
    )
    row = await cursor.fetchone()
    if row is None:
        return None
    cursor = await connection.execute(
        "SELECT * FROM agent_memory_events WHERE id=%s AND partition_key=%s "
        "AND archived_at IS NULL AND content_hash=%s",
        (row["event_id"], row["partition_key"], row["source_hash"]),
    )
    event = await cursor.fetchone()
    if event is None:
        await connection.execute(
            "DELETE FROM agent_memory_consolidation_outbox WHERE event_id=%s", (row["event_id"],)
        )
        return None
    result = row["result_json"]
    result = restore_ingest_result(json.loads(result) if isinstance(result, str) else result)
    token, expires = str(uuid4()), now + timedelta(seconds=lease_seconds)
    await connection.execute(
        "UPDATE agent_memory_consolidation_outbox SET status='delivering', "
        "attempts=attempts+1, lease_token=%s, lease_expires_at=%s, last_error_code=NULL "
        "WHERE event_id=%s",
        (token, expires, row["event_id"]),
    )
    return ConsolidationDelivery(repository._event_from_row(event), result, token, expires)


async def source_is_live(connection, delivery):
    cursor = await connection.execute(
        "SELECT 1 FROM agent_memory_events WHERE id=%s AND partition_key=%s "
        "AND archived_at IS NULL AND content_hash=%s AND NOT EXISTS "
        "(SELECT 1 FROM agent_memory_consolidation_outbox o "
        "WHERE o.event_id=agent_memory_events.id AND o.status='cancelled')",
        (
            delivery.event.id,
            delivery.event.scope.partition_key(),
            delivery.event.content_hash,
        ),
    )
    return await cursor.fetchone() is not None


async def delivery_is_current(connection, delivery):
    if not await source_is_live(connection, delivery):
        return False
    cursor = await connection.execute(
        "SELECT 1 FROM agent_memory_consolidation_outbox WHERE event_id=%s "
        "AND partition_key=%s AND status='delivering' AND lease_token=%s "
        "AND lease_expires_at>%s",
        (
            delivery.event.id,
            delivery.event.scope.partition_key(),
            delivery.token,
            utc_now().astimezone(UTC),
        ),
    )
    return await cursor.fetchone() is not None


async def complete(connection, delivery):
    # Use a single checked UPDATE so source and lease conditions refer to the
    # same statement snapshot. Deletion then removes this row atomically.
    cursor = await connection.execute(
        "UPDATE agent_memory_consolidation_outbox o SET status='delivered', "
        "result_json=jsonb_build_object('claim_ids',"
        "COALESCE(o.result_json->'claim_ids','[]'::jsonb)), "
        "lease_token=NULL, lease_expires_at=NULL, "
        "last_error_code=NULL, delivered_at=%s WHERE o.event_id=%s AND o.partition_key=%s "
        "AND o.source_hash=%s AND o.status='delivering' AND o.lease_token=%s "
        "AND o.lease_expires_at>%s "
        "AND EXISTS (SELECT 1 FROM agent_memory_events e WHERE e.id=o.event_id "
        "AND e.partition_key=o.partition_key AND e.archived_at IS NULL "
        "AND e.content_hash=o.source_hash)",
        (
            utc_now().astimezone(UTC),
            delivery.event.id,
            delivery.event.scope.partition_key(),
            delivery.event.content_hash,
            delivery.token,
            utc_now().astimezone(UTC),
        ),
    )
    return cursor.rowcount == 1


async def fail(connection, delivery, error_code):
    if not isinstance(error_code, str) or not 1 <= len(error_code) <= 128:
        raise ValueError("bounded outbox error code required")
    cursor = await connection.execute(
        "UPDATE agent_memory_consolidation_outbox SET status='pending', lease_token=NULL, "
        "lease_expires_at=NULL, last_error_code=%s WHERE event_id=%s AND partition_key=%s "
        "AND status='delivering' AND lease_token=%s",
        (error_code, delivery.event.id, delivery.event.scope.partition_key(), delivery.token),
    )
    return cursor.rowcount == 1


async def forget(connection, request):
    claim_where, claim_params = "c.partition_key=%s", [request.scope.partition_key()]
    if not request.all_in_scope:
        claim_where += " AND c.id=ANY(%s)"
        claim_params.append(list(request.memory_ids))
    await connection.execute(
        "UPDATE agent_memory_consolidation_outbox SET status='cancelled', result_json='{}'::jsonb, "
        "cancel_pending=TRUE,cancel_generation=cancel_generation+1,lease_token=NULL, "
        "lease_expires_at=NULL,last_error_code=NULL WHERE status IN "
        "('pending','delivering','delivered') AND event_id IN (SELECT sources.value "
        "FROM agent_memory_claims c CROSS JOIN LATERAL jsonb_array_elements_text("
        "COALESCE(c.provenance_json->'source_event_ids','[]'::jsonb)) AS sources(value) "
        "WHERE " + claim_where + ")",
        tuple(claim_params),
    )
    if request.all_in_scope:
        await connection.execute(
            "DELETE FROM agent_memory_consolidation_outbox WHERE partition_key=%s",
            (request.scope.partition_key(),),
        )
        await connection.execute(
            "UPDATE agent_memory_consolidation_outbox o SET status='cancelled', "
            "result_json='{}'::jsonb, lease_token=NULL, lease_expires_at=NULL, "
            "cancel_pending=TRUE,cancel_generation=cancel_generation+1, "
            "last_error_code=NULL WHERE o.status IN ('pending','delivering') AND EXISTS "
            "(SELECT 1 FROM jsonb_array_elements_text("
            "COALESCE(o.result_json->'claim_ids','[]'::jsonb)) AS ids(value) "
            "WHERE ids.value IN (SELECT id FROM agent_memory_claims WHERE partition_key=%s))",
            (request.scope.partition_key(),),
        )
    elif request.memory_ids:
        await connection.execute(
            "DELETE FROM agent_memory_consolidation_outbox WHERE partition_key=%s "
            "AND event_id=ANY(%s)",
            (request.scope.partition_key(), list(request.memory_ids)),
        )
        await connection.execute(
            "UPDATE agent_memory_consolidation_outbox o SET status='cancelled', "
            "result_json='{}'::jsonb, lease_token=NULL, lease_expires_at=NULL, "
            "cancel_pending=TRUE,cancel_generation=cancel_generation+1, "
            "last_error_code=NULL WHERE o.status IN ('pending','delivering') AND EXISTS "
            "(SELECT 1 FROM jsonb_array_elements_text("
            "COALESCE(o.result_json->'claim_ids','[]'::jsonb)) AS ids(value) "
            "WHERE (o.partition_key=%s AND ids.value=ANY(%s)) OR ids.value IN "
            "(SELECT id FROM agent_memory_claims WHERE partition_key=%s AND id=ANY(%s)))",
            (
                request.scope.partition_key(),
                list(request.memory_ids),
                request.scope.partition_key(),
                list(request.memory_ids),
            ),
        )


async def status(connection, scope=None):
    where, params = (" WHERE partition_key=%s", (scope.partition_key(),)) if scope else ("", ())
    cursor = await connection.execute(
        f"SELECT status, COUNT(*) n FROM agent_memory_consolidation_outbox{where} GROUP BY status",
        params,
    )
    totals = dict(pending=0, delivering=0, delivered=0, cancelled=0)
    totals.update({row["status"]: row["n"] for row in await cursor.fetchall()})
    return totals


async def intent_state(connection, scope, event_id):
    cursor = await connection.execute(
        "SELECT o.status FROM agent_memory_consolidation_outbox o "
        "JOIN agent_memory_events e ON e.id=o.event_id AND e.partition_key=o.partition_key "
        "WHERE o.partition_key=%s AND o.event_id=%s AND e.archived_at IS NULL "
        "AND e.content_hash=o.source_hash",
        (scope.partition_key(), event_id),
    )
    row = await cursor.fetchone()
    return ("leased" if row["status"] == "delivering" else row["status"]) if row else None


async def pending_cancellations(connection, *, scope=None, limit=64):
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ValueError("outbox cancellation batch limit must be between 1 and 256")
    where, params = (" AND o.partition_key=%s", (scope.partition_key(),)) if scope else ("", ())
    cursor = await connection.execute(
        "SELECT o.event_id,o.cancel_generation,e.tenant_id,e.namespace,e.user_id, "
        "e.agent_id,e.workspace_id,e.session_id FROM agent_memory_consolidation_outbox o "
        "JOIN agent_memory_events e ON e.id=o.event_id AND e.partition_key=o.partition_key "
        "WHERE o.status='cancelled' AND o.cancel_pending "
        + where
        + " ORDER BY o.created_at,o.event_id LIMIT %s",
        (*params, limit),
    )
    rows = await cursor.fetchall()
    return tuple(
        ConsolidationCancellation(
            MemoryScope(
                **{
                    name: row[name]
                    for name in (
                        "tenant_id",
                        "namespace",
                        "user_id",
                        "agent_id",
                        "workspace_id",
                        "session_id",
                    )
                }
            ),
            row["event_id"],
            row["cancel_generation"],
        )
        for row in rows
    )


async def request_cancellation(connection, scope, event_id):
    cursor = await connection.execute(
        "UPDATE agent_memory_consolidation_outbox SET cancel_pending=TRUE, "
        "cancel_generation=cancel_generation+1 WHERE event_id=%s AND partition_key=%s "
        "AND status='cancelled' RETURNING cancel_generation",
        (event_id, scope.partition_key()),
    )
    row = await cursor.fetchone()
    return ConsolidationCancellation(scope, event_id, row["cancel_generation"]) if row else None


async def ack_cancellation(connection, cancellation):
    cursor = await connection.execute(
        "UPDATE agent_memory_consolidation_outbox SET cancel_pending=FALSE WHERE event_id=%s "
        "AND partition_key=%s AND status='cancelled' AND cancel_pending AND cancel_generation=%s",
        (cancellation.event_id, cancellation.scope.partition_key(), cancellation.generation),
    )
    return cursor.rowcount == 1
