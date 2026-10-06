"""Bounded resource refresh records on the caller's transaction connection."""

import json

from ..domain import canonical_json

SCHEMA = """
CREATE TABLE IF NOT EXISTS resource_refresh (
    partition_key TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('resource','request')),
    identity TEXT NOT NULL, payload_json TEXT NOT NULL,
    PRIMARY KEY(partition_key,kind,identity)
);
"""


def get(connection, scope, kind, identity):
    row = connection.execute(
        "SELECT payload_json FROM resource_refresh WHERE partition_key=? AND kind=? AND identity=?",
        (scope.partition_key(), kind, identity),
    ).fetchone()
    return json.loads(row[0]) if row else None


def put(connection, scope, kind, identity, payload):
    connection.execute(
        "INSERT INTO resource_refresh VALUES (?,?,?,?) "
        "ON CONFLICT(partition_key,kind,identity) DO UPDATE SET payload_json=excluded.payload_json",
        (scope.partition_key(), kind, identity, canonical_json(payload)),
    )


def records(connection, scope, kind):
    rows = connection.execute(
        "SELECT payload_json FROM resource_refresh WHERE partition_key=? AND kind=? "
        "ORDER BY identity LIMIT 4097",
        (scope.partition_key(), kind),
    ).fetchall()
    if len(rows) > 4096:
        raise ValueError("refresh ledger capacity exceeded")
    return tuple(json.loads(row[0]) for row in rows)


def forget(connection, request):
    ids, affected = set(request.memory_ids), set()
    for row in records(connection, request.scope, "resource"):
        sources = {s for values in row["units"].values() for s in values}
        if request.all_in_scope or ids & sources:
            affected.add(row["resource_id"])
            row.update(
                status="cancelled",
                reason="source_unavailable",
                units={},
                requested_through=[],
                claimed_through=[],
                completed_through=[],
                commits={},
                unit_commits={},
                checkpoint={},
            )
            for field in ("lease_token", "lease_until", "last_completed_fence"):
                row.pop(field, None)
            put(connection, request.scope, "resource", row["resource_id"], row)
    for row in records(connection, request.scope, "request"):
        if row["resource_id"] in affected:
            row.update(invalidated=True, units={})
            put(connection, request.scope, "request", row["request_id"], row)
