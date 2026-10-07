"""Durable receive ledger SQL; every call uses the caller's active transaction."""

import json

from ..domain import canonical_json
from . import sqlite_delivery, sqlite_index, sqlite_purge, sqlite_refresh

SCHEMA = (
    sqlite_refresh.SCHEMA + sqlite_index.SCHEMA + sqlite_delivery.SCHEMA + sqlite_purge.SCHEMA
    + """
CREATE TABLE IF NOT EXISTS retention_heads (
    partition_key TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('document','interpretation')),
    identity TEXT NOT NULL, generation INTEGER NOT NULL, payload_json TEXT NOT NULL,
    PRIMARY KEY(partition_key, kind, identity)
);
CREATE TABLE IF NOT EXISTS retention_producers (
    partition_key TEXT NOT NULL, producer_id TEXT NOT NULL, payload_json TEXT NOT NULL,
    PRIMARY KEY(partition_key, producer_id)
);
CREATE TABLE IF NOT EXISTS retention_epochs (
    partition_key TEXT PRIMARY KEY, epoch INTEGER NOT NULL CHECK(epoch >= 0)
);
CREATE TABLE IF NOT EXISTS retention_entries (
    partition_key TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('ticket', 'request')),
    request_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    idempotency_key TEXT,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(partition_key, kind, request_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS retention_ticket_event_idx
ON retention_entries(partition_key, event_id) WHERE kind = 'ticket';
CREATE UNIQUE INDEX IF NOT EXISTS retention_ticket_key_idx
ON retention_entries(partition_key, idempotency_key) WHERE kind = 'ticket';
CREATE INDEX IF NOT EXISTS retention_pending_idx
ON retention_entries(partition_key, kind, status);
"""
)


def kind(value):
    if value not in {"ticket", "request"}:
        raise ValueError("invalid retention record kind")
    return value


def epoch(connection, scope):
    key = scope.partition_key()
    connection.execute("INSERT OR IGNORE INTO retention_epochs VALUES (?, 0)", (key,))
    return connection.execute(
        "SELECT epoch FROM retention_epochs WHERE partition_key = ?", (key,)
    ).fetchone()[0]


def get(connection, scope, record_kind, request_id):
    row = connection.execute(
        "SELECT payload_json FROM retention_entries "
        "WHERE partition_key=? AND kind=? AND request_id=?",
        (scope.partition_key(), kind(record_kind), request_id),
    ).fetchone()
    return json.loads(row[0]) if row else None


def insert(connection, scope, record_kind, request_id, payload):
    connection.execute(
        "INSERT INTO retention_entries VALUES (?,?,?,?,?,?,?)",
        (
            scope.partition_key(),
            kind(record_kind),
            request_id,
            payload["event_id"],
            payload.get("idempotency_key"),
            payload.get("status", "issued"),
            canonical_json(payload),
        ),
    )


def count(connection, scope, record_kind):
    condition = (
        " AND status IN ('queued','running','retry_wait')" if kind(record_kind) == "request" else ""
    )
    return connection.execute(
        "SELECT count(*) FROM retention_entries WHERE partition_key=? AND kind=?" + condition,
        (scope.partition_key(), record_kind),
    ).fetchone()[0]


def identity_owner(connection, scope, event_id, idempotency_key):
    row = connection.execute(
        """SELECT request_id FROM retention_entries WHERE partition_key=? AND kind='ticket'
           AND (event_id=? OR idempotency_key=?) LIMIT 1""",
        (scope.partition_key(), event_id, idempotency_key),
    ).fetchone()
    return row[0] if row else None


def forget(connection, request):
    """Runs inside the same lock/transaction as source archive or erasure."""
    key = request.scope.partition_key()
    if request.all_in_scope:
        epoch(connection, request.scope)
        connection.execute(
            "UPDATE retention_epochs SET epoch=epoch+1 WHERE partition_key=?", (key,)
        )
    elif not request.memory_ids:
        return
    sqlite_refresh.forget(connection, request)
    sqlite_index.forget(connection, request)
    sqlite_purge.record(connection, request, epoch(connection, request.scope))
    condition = "partition_key=?"
    params = (key,)
    if not request.all_in_scope:
        condition += f" AND event_id IN ({','.join('?' for _ in request.memory_ids)})"
        params += tuple(request.memory_ids)
    rows = connection.execute(
        f"SELECT kind, request_id, payload_json FROM retention_entries WHERE {condition}",
        params,
    ).fetchall()
    for row in rows:
        payload = json.loads(row["payload_json"])
        for field in ("prepared", "result", "input_manifest", "lease_token", "publication_manifest"):
            payload.pop(field, None)
        status = "revoked" if row["kind"] == "ticket" else "cancelled"
        payload["revoked" if row["kind"] == "ticket" else "status"] = (
            True if row["kind"] == "ticket" else status
        )
        connection.execute(
            """UPDATE retention_entries SET status=?, payload_json=?
               WHERE partition_key=? AND kind=? AND request_id=?""",
            (status, canonical_json(payload), key, row["kind"], row["request_id"]),
        )


def update(connection, scope, request_id, payload):
    cursor = connection.execute(
        "UPDATE retention_entries SET payload_json=?, status=? "
        "WHERE partition_key=? AND kind='request' AND request_id=?",
        (canonical_json(payload), payload["status"], scope.partition_key(), request_id),
    )
    if cursor.rowcount != 1:
        raise ValueError("retention request is missing")


def active(connection, scope):
    rows = connection.execute(
        "SELECT payload_json FROM retention_entries WHERE partition_key=? AND kind='request' "
        "AND status IN ('queued','running','retry_wait') ORDER BY request_id LIMIT 100001",
        (scope.partition_key(),),
    ).fetchall()
    if len(rows) > 100000:
        raise ValueError("retention active request limit exceeded")
    return tuple(json.loads(row[0]) for row in rows)


def producer_get(connection, scope, producer_id):
    row = connection.execute(
        "SELECT payload_json FROM retention_producers WHERE partition_key=? AND producer_id=?",
        (scope.partition_key(), producer_id),
    ).fetchone()
    return json.loads(row[0]) if row else None


def producer_put(connection, scope, producer_id, payload):
    connection.execute(
        "INSERT INTO retention_producers VALUES (?,?,?) ON CONFLICT(partition_key,producer_id) "
        "DO UPDATE SET payload_json=excluded.payload_json",
        (scope.partition_key(), producer_id, canonical_json(payload)),
    )


def head_get(connection, scope, head_kind, identity):
    row = connection.execute(
        "SELECT generation,payload_json FROM retention_heads "
        "WHERE partition_key=? AND kind=? AND identity=?",
        (scope.partition_key(), head_kind, identity),
    ).fetchone()
    return {"generation": row[0], "payload": json.loads(row[1])} if row else None


def head_put(connection, scope, head_kind, identity, payload, expected_generation):
    from .retention import RetentionError

    if expected_generation == 0:
        cursor = connection.execute(
            "INSERT OR IGNORE INTO retention_heads VALUES (?,?,?,?,?)",
            (scope.partition_key(), head_kind, identity, 1, canonical_json(payload)),
        )
    else:
        cursor = connection.execute(
            "UPDATE retention_heads SET generation=generation+1,payload_json=? "
            "WHERE partition_key=? AND kind=? AND identity=? AND generation=?",
            (
                canonical_json(payload),
                scope.partition_key(),
                head_kind,
                identity,
                expected_generation,
            ),
        )
    if cursor.rowcount != 1:
        raise RetentionError(head_kind + "_head_changed")
    return expected_generation + 1


def requests(connection, scope):
    rows = connection.execute(
        "SELECT payload_json FROM retention_entries WHERE partition_key=? AND kind='request' "
        "ORDER BY request_id LIMIT 10001", (scope.partition_key(),),
    ).fetchall()
    if len(rows) > 10000:
        raise ValueError("index recovery request capacity exceeded")
    return tuple(json.loads(row[0]) for row in rows)
