"""Identity-only deletion journal, sharing the source deletion transaction."""

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
