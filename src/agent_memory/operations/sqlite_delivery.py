"""Identity-only delivery contracts in the caller's receive/publication transaction."""

import json

from ..domain import canonical_json

SCHEMA = """
CREATE TABLE IF NOT EXISTS retention_delivery (
    partition_key TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('target','sequence')),
    identity TEXT NOT NULL, payload_json TEXT NOT NULL,
    PRIMARY KEY(partition_key,kind,identity)
);
"""


def get(connection, scope, kind, identity):
    row = connection.execute(
        "SELECT payload_json FROM retention_delivery "
        "WHERE partition_key=? AND kind=? AND identity=?",
        (scope.partition_key(), kind, identity),
    ).fetchone()
    return json.loads(row[0]) if row else None


def insert(connection, scope, kind, identity, payload):
    connection.execute(
        "INSERT INTO retention_delivery VALUES (?,?,?,?)",
        (scope.partition_key(), kind, identity, canonical_json(payload)),
    )


def count(connection, scope, kind):
    return connection.execute(
        "SELECT count(*) FROM retention_delivery WHERE partition_key=? AND kind=?",
        (scope.partition_key(), kind),
    ).fetchone()[0]
