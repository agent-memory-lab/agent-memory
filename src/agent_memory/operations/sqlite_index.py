"""Candidate locator SQL, on the publication/deletion transaction connection."""

import json

from ..domain import canonical_json

SCHEMA = """
CREATE TABLE IF NOT EXISTS index_recovery (
    partition_key TEXT NOT NULL, channel TEXT NOT NULL, epoch INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('head','stream','rollover','repair')),
    identity TEXT NOT NULL, payload_json TEXT NOT NULL,
    PRIMARY KEY(partition_key,channel,epoch,kind,identity)
);
CREATE TABLE IF NOT EXISTS index_jobs (
    partition_key TEXT NOT NULL, channel TEXT NOT NULL, epoch INTEGER NOT NULL,
    token_id TEXT NOT NULL, sequence INTEGER NOT NULL, event_id TEXT NOT NULL,
    status TEXT NOT NULL, payload_json TEXT NOT NULL,
    PRIMARY KEY(partition_key,channel,epoch,token_id),
    UNIQUE(partition_key,channel,epoch,sequence)
);
CREATE TABLE IF NOT EXISTS index_documents (
    partition_key TEXT NOT NULL, channel TEXT NOT NULL, candidate_id TEXT NOT NULL,
    slot_key TEXT NOT NULL, event_id TEXT NOT NULL, payload_json TEXT NOT NULL,
    PRIMARY KEY(partition_key,channel,candidate_id)
);
CREATE INDEX IF NOT EXISTS index_slot_idx
ON index_documents(partition_key,channel,slot_key);
"""


def job_get(connection, scope, channel, epoch, token_id):
    row = connection.execute(
        "SELECT payload_json FROM index_jobs WHERE partition_key=? AND channel=? "
        "AND epoch=? AND token_id=?",
        (scope.partition_key(), channel, epoch, token_id),
    ).fetchone()
    return json.loads(row[0]) if row else None


def job_put(connection, scope, row):
    connection.execute(
        "INSERT INTO index_jobs VALUES (?,?,?,?,?,?,?,?) "
        "ON CONFLICT(partition_key,channel,epoch,token_id) DO UPDATE SET "
        "status=excluded.status,payload_json=excluded.payload_json",
        (
            scope.partition_key(),
            row["channel"],
            row["epoch"],
            row["token"]["id"],
            row["sequence"],
            row["event_id"],
            row["status"],
            canonical_json(row),
        ),
    )


def jobs(connection, scope, channel, epoch):
    rows = connection.execute(
        "SELECT payload_json FROM index_jobs WHERE partition_key=? AND channel=? AND epoch=? "
        "ORDER BY sequence LIMIT 100001",
        (scope.partition_key(), channel, epoch),
    ).fetchall()
    if len(rows) > 100000:
        raise ValueError("index ledger capacity exceeded")
    return tuple(json.loads(r[0]) for r in rows)


def document_get(connection, scope, channel, candidate_id):
    row = connection.execute(
        "SELECT payload_json FROM index_documents WHERE partition_key=? AND channel=? "
        "AND candidate_id=?",
        (scope.partition_key(), channel, candidate_id),
    ).fetchone()
    return json.loads(row[0]) if row else None


def document_put(connection, scope, channel, candidate_id, document):
    if document is None:
        connection.execute(
            "DELETE FROM index_documents WHERE partition_key=? AND channel=? AND candidate_id=?",
            (scope.partition_key(), channel, candidate_id),
        )
    else:
        connection.execute(
            "INSERT INTO index_documents VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(partition_key,channel,candidate_id) DO UPDATE SET "
            "slot_key=excluded.slot_key,event_id=excluded.event_id,payload_json=excluded.payload_json",
            (
                scope.partition_key(),
                channel,
                candidate_id,
                document["slot_key"],
                document["event_id"],
                canonical_json(document),
            ),
        )


def lookup(connection, scope, channel, slot_key, limit):
    rows = connection.execute(
        "SELECT payload_json FROM index_documents WHERE partition_key=? AND channel=? "
        "AND slot_key=? "
        "ORDER BY candidate_id LIMIT ?",
        (scope.partition_key(), channel, slot_key, limit),
    ).fetchall()
    return tuple(json.loads(r[0]) for r in rows)


def forget(connection, request):
    condition, params = "partition_key=?", (request.scope.partition_key(),)
    if not request.all_in_scope:
        if not request.memory_ids:
            return
        condition += f" AND event_id IN ({','.join('?' for _ in request.memory_ids)})"
        params += tuple(request.memory_ids)
    connection.execute(f"DELETE FROM index_documents WHERE {condition}", params)
    rows = connection.execute(
        f"SELECT payload_json FROM index_jobs WHERE {condition}",
        params,
    ).fetchall()
    for value in rows:
        row = json.loads(value[0])
        row["status"] = "cancelled"
        for field in ("proof", "lease_token", "applied"):
            row.pop(field, None)
        job_put(connection, request.scope, row)


def invalidate_records(connection, candidate_ids):
    """Erase cascaded locator metadata, including proofs held by another source's job."""
    identities = sorted(candidate_ids)
    for offset in range(0, len(identities), 128):
        chunk = identities[offset : offset + 128]
        placeholders = ",".join("?" for _ in chunk)
        connection.execute(
            f"DELETE FROM index_documents WHERE candidate_id IN ({placeholders})",
            chunk,
        )
        rows = connection.execute(
            "SELECT partition_key,payload_json FROM index_jobs j WHERE EXISTS ("
            "SELECT 1 FROM json_each(j.payload_json,'$.dispositions') d "
            f"WHERE json_extract(d.value,'$.candidate_id') IN ({placeholders}))",
            chunk,
        ).fetchall()
        for value in rows:
            row = json.loads(value["payload_json"])
            row["status"] = "cancelled"
            for field in ("proof", "lease_token", "applied"):
                row.pop(field, None)
            connection.execute(
                "UPDATE index_jobs SET status='cancelled',payload_json=? "
                "WHERE partition_key=? AND channel=? AND epoch=? AND token_id=?",
                (
                    canonical_json(row),
                    value["partition_key"],
                    row["channel"],
                    row["epoch"],
                    row["token"]["id"],
                ),
            )


def recovery_get(connection, scope, channel, epoch, kind, identity):
    row = connection.execute(
        "SELECT payload_json FROM index_recovery WHERE partition_key=? AND channel=? "
        "AND epoch=? AND kind=? AND identity=?",
        (scope.partition_key(), channel, epoch, kind, identity),
    ).fetchone()
    return json.loads(row[0]) if row else None


def recovery_put(connection, scope, channel, epoch, kind, identity, payload):
    connection.execute(
        "INSERT INTO index_recovery VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(partition_key,channel,epoch,kind,identity) "
        "DO UPDATE SET payload_json=excluded.payload_json",
        (scope.partition_key(), channel, epoch, kind, identity, canonical_json(payload)),
    )


def recovery_count(connection, scope, channel, epoch, kind):
    return connection.execute(
        "SELECT count(*) FROM index_recovery WHERE partition_key=? AND channel=? "
        "AND epoch=? AND kind=?", (scope.partition_key(), channel, epoch, kind),
    ).fetchone()[0]


def position(connection, scope, channel, epoch, publication_id):
    row = connection.execute(
        "SELECT sequence FROM index_jobs WHERE partition_key=? AND channel=? "
        "AND epoch=? AND token_id=?", (scope.partition_key(), channel, epoch, publication_id),
    ).fetchone()
    return row[0] if row else None
