"""SQLite storage for the generic kernel's transactional consolidation intents."""

import json
from dataclasses import asdict
from datetime import UTC, timedelta
from uuid import uuid4

from ..domain import MemoryScope, canonical_json, utc_now
from .consolidation_outbox import (
    ConsolidationCancellation,
    ConsolidationDelivery,
    restore_ingest_result,
    retry_delay_seconds,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS consolidation_outbox (
    event_id TEXT PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
    partition_key TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    result_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'delivering', 'delivered', 'cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT,
    lease_expires_at TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    cancel_pending INTEGER NOT NULL DEFAULT 0,
    cancel_generation INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS consolidation_outbox_claim_idx
ON consolidation_outbox(status, created_at, event_id);
CREATE INDEX IF NOT EXISTS consolidation_outbox_scope_idx
ON consolidation_outbox(partition_key, status);
"""


def initialize(connection):
    connection.executescript(SCHEMA)
    columns = {row["name"] for row in connection.execute("PRAGMA table_info(consolidation_outbox)")}
    for name in ("cancel_pending", "cancel_generation"):
        if name not in columns:
            connection.execute(
                f"ALTER TABLE consolidation_outbox ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0"
            )
    if "next_attempt_at" not in columns:
        connection.execute("ALTER TABLE consolidation_outbox ADD COLUMN next_attempt_at TEXT")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS consolidation_outbox_retry_idx "
        "ON consolidation_outbox(next_attempt_at, created_at, event_id) WHERE status='pending'"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS consolidation_outbox_cancel_idx "
        "ON consolidation_outbox(created_at,event_id) WHERE status='cancelled' AND cancel_pending=1"
    )


def save(connection, event, result):
    if result.event_id != event.id:
        raise ValueError("consolidation intent event/result mismatch")
    source = connection.execute(
        "SELECT content_hash FROM events WHERE id=? AND partition_key=? AND archived_at IS NULL",
        (event.id, event.scope.partition_key()),
    ).fetchone()
    if source is None or source["content_hash"] != event.content_hash:
        raise ValueError("consolidation requires a live committed source")
    connection.execute(
        "INSERT OR IGNORE INTO consolidation_outbox "
        "(event_id, partition_key, source_hash, result_json, status, created_at) "
        "VALUES (?,?,?,?, 'pending', ?)",
        (
            event.id,
            event.scope.partition_key(),
            event.content_hash,
            canonical_json(asdict(result)),
            utc_now().astimezone(UTC).isoformat(),
        ),
    )


def claim(
    connection, repository, *, scope=None, event_id=None, lease_seconds=60,
    exclude_event_ids=(),
):
    if type(lease_seconds) is not int or not 5 <= lease_seconds <= 3600:
        raise ValueError("outbox lease must be between 5 and 3600 seconds")
    now = utc_now().astimezone(UTC)
    where = "(o.status='pending' OR (o.status='delivering' AND o.lease_expires_at<=?))"
    params = [now.isoformat()]
    if event_id is None:
        where += " AND (o.status='delivering' OR o.next_attempt_at IS NULL OR o.next_attempt_at<=?)"
        params.append(now.isoformat())
    if exclude_event_ids:
        where += " AND o.event_id NOT IN (" + ",".join("?" for _ in exclude_event_ids) + ")"
        params.extend(exclude_event_ids)
    if scope is not None:
        where += " AND o.partition_key=?"
        params.append(scope.partition_key())
    if event_id is not None:
        where += " AND o.event_id=?"
        params.append(event_id)
    row = connection.execute(
        "SELECT o.* FROM consolidation_outbox o JOIN events e ON e.id=o.event_id "
        "AND e.partition_key=o.partition_key AND e.archived_at IS NULL "
        f"AND e.content_hash=o.source_hash WHERE {where} "
        "ORDER BY COALESCE(o.next_attempt_at, o.created_at), o.created_at, o.event_id LIMIT 1",
        params,
    ).fetchone()
    if row is None:
        return None
    event_row = connection.execute(
        "SELECT * FROM events WHERE id=? AND partition_key=? AND archived_at IS NULL "
        "AND content_hash=?",
        (row["event_id"], row["partition_key"], row["source_hash"]),
    ).fetchone()
    result = restore_ingest_result(json.loads(row["result_json"]))
    token, expires = str(uuid4()), now + timedelta(seconds=lease_seconds)
    connection.execute(
        "UPDATE consolidation_outbox SET status='delivering', attempts=attempts+1, "
        "lease_token=?, lease_expires_at=?, last_error_code=NULL WHERE event_id=?",
        (token, expires.isoformat(), row["event_id"]),
    )
    return ConsolidationDelivery(
        repository._event_from_row(event_row), result, token, expires, row["attempts"] + 1
    )


def source_is_live(connection, delivery):
    return (
        connection.execute(
            "SELECT 1 FROM events WHERE id=? AND partition_key=? AND archived_at IS NULL "
            "AND content_hash=? AND NOT EXISTS (SELECT 1 FROM consolidation_outbox o "
            "WHERE o.event_id=events.id AND o.status='cancelled')",
            (
                delivery.event.id,
                delivery.event.scope.partition_key(),
                delivery.event.content_hash,
            ),
        ).fetchone()
        is not None
    )


def delivery_is_current(connection, delivery):
    return (
        source_is_live(connection, delivery)
        and connection.execute(
            "SELECT 1 FROM consolidation_outbox WHERE event_id=? AND partition_key=? "
            "AND status='delivering' AND lease_token=? AND lease_expires_at>?",
            (
                delivery.event.id,
                delivery.event.scope.partition_key(),
                delivery.token,
                utc_now().astimezone(UTC).isoformat(),
            ),
        ).fetchone()
        is not None
    )


def complete(connection, delivery):
    if not delivery_is_current(connection, delivery):
        return False
    changed = connection.execute(
        "UPDATE consolidation_outbox SET status='delivered', "
        "result_json=json_object('claim_ids',json(COALESCE("
        "json_extract(result_json, '$.claim_ids'),'[]'))), "
        "lease_token=NULL, lease_expires_at=NULL, next_attempt_at=NULL, "
        "last_error_code=NULL, delivered_at=? "
        "WHERE event_id=? AND lease_token=?",
        (utc_now().astimezone(UTC).isoformat(), delivery.event.id, delivery.token),
    )
    return changed.rowcount == 1


def fail(connection, delivery, error_code):
    if not isinstance(error_code, str) or not 1 <= len(error_code) <= 128:
        raise ValueError("bounded outbox error code required")
    # Fenced failure cannot release a newer delivery after lease takeover.
    retry_at = utc_now().astimezone(UTC) + timedelta(seconds=retry_delay_seconds(delivery.attempts))
    return (
        connection.execute(
            "UPDATE consolidation_outbox SET status='pending', lease_token=NULL, "
            "lease_expires_at=NULL, last_error_code=?, next_attempt_at=? WHERE event_id=? "
            "AND partition_key=? AND status='delivering' AND lease_token=?",
            (
                error_code,
                retry_at.isoformat(),
                delivery.event.id,
                delivery.event.scope.partition_key(),
                delivery.token,
            ),
        ).rowcount
        == 1
    )


def forget(connection, request):
    claim_where, claim_params = "c.partition_key=?", [request.scope.partition_key()]
    if not request.all_in_scope:
        marks = ",".join("?" for _ in request.memory_ids)
        claim_where += f" AND c.id IN ({marks})"
        claim_params.extend(request.memory_ids)
    # Existing claim_sources is the indexed reverse dependency for delivered
    # handoffs too. Read it before Claim erasure removes these associations.
    connection.execute(
        "UPDATE consolidation_outbox SET status='cancelled', result_json='{}', "
        "cancel_pending=1, cancel_generation=cancel_generation+1, lease_token=NULL, "
        "lease_expires_at=NULL, last_error_code=NULL WHERE status IN "
        "('pending','delivering','delivered') AND event_id IN (SELECT cs.event_id "
        "FROM claim_sources cs JOIN claims c ON c.id=cs.claim_id WHERE " + claim_where + ")",
        claim_params,
    )
    if request.all_in_scope:
        connection.execute(
            "DELETE FROM consolidation_outbox WHERE partition_key=?",
            (request.scope.partition_key(),),
        )
        connection.execute(
            "UPDATE consolidation_outbox SET status='cancelled', result_json='{}', "
            "cancel_pending=1, cancel_generation=cancel_generation+1, "
            "lease_token=NULL, lease_expires_at=NULL, last_error_code=NULL "
            "WHERE status IN ('pending','delivering') AND EXISTS "
            "(SELECT 1 FROM json_each(result_json, '$.claim_ids') ids WHERE ids.value IN "
            "(SELECT id FROM claims WHERE partition_key=?))",
            (request.scope.partition_key(),),
        )
    elif request.memory_ids:
        marks = ",".join("?" for _ in request.memory_ids)
        connection.execute(
            f"DELETE FROM consolidation_outbox WHERE partition_key=? AND event_id IN ({marks})",
            (request.scope.partition_key(), *request.memory_ids),
        )
        # Source content can remain live when only a Claim is forgotten. Keep
        # an ID-only cancellation marker so duplicate ingestion cannot create
        # another handoff for that source. Broader-scope claims can be referenced
        # by narrower source events; authorize those IDs against the target scope.
        connection.execute(
            "UPDATE consolidation_outbox SET status='cancelled', result_json='{}', "
            "cancel_pending=1, cancel_generation=cancel_generation+1, "
            "lease_token=NULL, lease_expires_at=NULL, last_error_code=NULL "
            "WHERE status IN ('pending','delivering') AND EXISTS "
            "(SELECT 1 FROM json_each(result_json, '$.claim_ids') ids WHERE "
            f"(consolidation_outbox.partition_key=? AND ids.value IN ({marks})) OR "
            "ids.value IN (SELECT id FROM claims WHERE partition_key=? "
            f"AND id IN ({marks})))",
            (
                request.scope.partition_key(),
                *request.memory_ids,
                request.scope.partition_key(),
                *request.memory_ids,
            ),
        )


def status(connection, scope=None):
    where, params = (" WHERE partition_key=?", (scope.partition_key(),)) if scope else ("", ())
    rows = connection.execute(
        f"SELECT status, COUNT(*) n FROM consolidation_outbox{where} GROUP BY status", params
    ).fetchall()
    totals = dict(pending=0, delivering=0, delivered=0, cancelled=0)
    totals.update({row["status"]: row["n"] for row in rows})
    return totals


def intent_state(connection, scope, event_id):
    row = connection.execute(
        "SELECT o.status FROM consolidation_outbox o JOIN events e ON e.id=o.event_id "
        "WHERE o.partition_key=? AND o.event_id=? AND e.archived_at IS NULL "
        "AND e.content_hash=o.source_hash",
        (scope.partition_key(), event_id),
    ).fetchone()
    return ("leased" if row["status"] == "delivering" else row["status"]) if row else None


def pending_cancellations(connection, *, scope=None, limit=64):
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ValueError("outbox cancellation batch limit must be between 1 and 256")
    where, params = (" AND o.partition_key=?", (scope.partition_key(),)) if scope else ("", ())
    rows = connection.execute(
        "SELECT o.event_id, o.cancel_generation, e.tenant_id, e.namespace, e.user_id, "
        "e.agent_id, e.workspace_id, e.session_id FROM consolidation_outbox o "
        "JOIN events e ON e.id=o.event_id AND e.partition_key=o.partition_key "
        "WHERE o.status='cancelled' AND o.cancel_pending=1 "
        + where
        + " ORDER BY o.created_at,o.event_id LIMIT ?",
        (*params, limit),
    ).fetchall()
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


def request_cancellation(connection, scope, event_id):
    row = connection.execute(
        "UPDATE consolidation_outbox SET cancel_pending=1, "
        "cancel_generation=cancel_generation+1 WHERE event_id=? AND partition_key=? "
        "AND status='cancelled' RETURNING cancel_generation",
        (event_id, scope.partition_key()),
    ).fetchone()
    return ConsolidationCancellation(scope, event_id, row["cancel_generation"]) if row else None


def ack_cancellation(connection, cancellation):
    return (
        connection.execute(
            "UPDATE consolidation_outbox SET cancel_pending=0 WHERE event_id=? "
            "AND partition_key=? AND status='cancelled' AND cancel_pending=1 "
            "AND cancel_generation=?",
            (cancellation.event_id, cancellation.scope.partition_key(), cancellation.generation),
        ).rowcount
        == 1
    )
