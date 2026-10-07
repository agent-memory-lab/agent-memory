"""Identity-only deletion journal, sharing the source deletion transaction."""

import json

from ..domain import canonical_json

SCHEMA = """
CREATE TABLE IF NOT EXISTS retention_purge_heads (
    partition_key TEXT PRIMARY KEY, cursor INTEGER NOT NULL CHECK(cursor >= 0)
);
CREATE TABLE IF NOT EXISTS retention_purges (
    partition_key TEXT NOT NULL, cursor INTEGER NOT NULL,
    source_event_id TEXT NOT NULL, epoch INTEGER NOT NULL,
    all_in_scope INTEGER NOT NULL CHECK(all_in_scope IN (0,1)), mode TEXT NOT NULL,
    PRIMARY KEY(partition_key,cursor)
);
CREATE TABLE IF NOT EXISTS retention_purge_restores (
    partition_key TEXT NOT NULL, identity TEXT NOT NULL, payload_json TEXT NOT NULL,
    PRIMARY KEY(partition_key,identity)
);
CREATE INDEX IF NOT EXISTS retention_purge_source_idx
ON retention_purges(partition_key,source_event_id);
"""


def head(connection, scope):
    row = connection.execute(
        "SELECT cursor FROM retention_purge_heads WHERE partition_key=?", (scope.partition_key(),)
    ).fetchone()
    return row[0] if row else 0


def record(connection, request, epoch):
    identities = ("",) if request.all_in_scope else tuple(sorted(set(request.memory_ids)))
    if not identities:
        return
    key, cursor = request.scope.partition_key(), head(connection, request.scope)
    connection.executemany(
        "INSERT INTO retention_purges VALUES (?,?,?,?,?,?)",
        (
            (key, cursor + index, identity, epoch, int(request.all_in_scope), request.mode.value)
            for index, identity in enumerate(identities, 1)
        ),
    )
    connection.execute(
        "INSERT INTO retention_purge_heads VALUES (?,?) ON CONFLICT(partition_key) "
        "DO UPDATE SET cursor=excluded.cursor",
        (key, cursor + len(identities)),
    )


def page(connection, scope, after, limit):
    rows = connection.execute(
        "SELECT cursor,source_event_id,epoch,all_in_scope,mode FROM retention_purges "
        "WHERE partition_key=? AND cursor>? ORDER BY cursor LIMIT ?",
        (scope.partition_key(), after, limit),
    ).fetchall()
    return tuple(
        {
            "cursor": r[0],
            "source_event_id": r[1],
            "epoch": r[2],
            "all_in_scope": bool(r[3]),
            "mode": r[4],
        }
        for r in rows
    )


def erased(connection, scope, identity):
    return (
        connection.execute(
            "SELECT 1 FROM retention_purges WHERE partition_key=? AND source_event_id=? LIMIT 1",
            (scope.partition_key(), identity),
        ).fetchone()
        is not None
    )


def import_entry(connection, scope, entry):
    if head(connection, scope) != entry["cursor"] - 1:
        raise ValueError("purge import cursor conflict")
    connection.execute(
        "INSERT INTO retention_purges VALUES (?,?,?,?,?,?)",
        (scope.partition_key(), entry["cursor"], entry["source_event_id"], entry["epoch"],
         int(entry["all_in_scope"]), entry["mode"]),
    )
    connection.execute(
        "INSERT INTO retention_purge_heads VALUES (?,?) ON CONFLICT(partition_key) "
        "DO UPDATE SET cursor=excluded.cursor", (scope.partition_key(), entry["cursor"]),
    )


def restore_get(connection, scope, identity):
    row = connection.execute(
        "SELECT payload_json FROM retention_purge_restores WHERE partition_key=? AND identity=?",
        (scope.partition_key(), identity),
    ).fetchone()
    return json.loads(row[0]) if row else None


def restore_put(connection, scope, identity, payload):
    connection.execute(
        "INSERT INTO retention_purge_restores VALUES (?,?,?)",
        (scope.partition_key(), identity, canonical_json(payload)),
    )


def restore_count(connection, scope):
    return connection.execute(
        "SELECT count(*) FROM retention_purge_restores WHERE partition_key=?",
        (scope.partition_key(),),
    ).fetchone()[0]
