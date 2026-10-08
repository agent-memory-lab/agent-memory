"""Minimal finance audit; independent from erased content and dependency keys."""

import json

from ..retrieval.model_contracts import ModelError, canonical, hash_value

SCHEMA = """
CREATE TABLE IF NOT EXISTS model_budget_entries (
 kind TEXT NOT NULL CHECK(kind IN ('account','call','receipt')),
 identity TEXT NOT NULL, payload_json TEXT NOT NULL,
 PRIMARY KEY(kind,identity)
);
"""


def get(connection, kind, key):
    row = connection.execute(
        "SELECT payload_json FROM model_budget_entries WHERE kind=? AND identity=?", (kind, key)
    ).fetchone()
    return json.loads(row[0]) if row else None


def put(connection, kind, key, payload):
    hash_value(key)
    if (
        get(connection, kind, key) is None
        and connection.execute(
            "SELECT count(*) FROM model_budget_entries WHERE kind=?", (kind,)
        ).fetchone()[0]
        >= 4096
    ):
        raise ModelError("model_budget_audit_capacity")
    connection.execute(
        "INSERT INTO model_budget_entries VALUES (?,?,?) ON CONFLICT(kind,identity) "
        "DO UPDATE SET payload_json=excluded.payload_json",
        (kind, key, canonical(payload)),
    )


def records(connection, kind):
    return [
        json.loads(r[0])
        for r in connection.execute(
            "SELECT payload_json FROM model_budget_entries WHERE kind=? ORDER BY identity", (kind,)
        )
    ]
