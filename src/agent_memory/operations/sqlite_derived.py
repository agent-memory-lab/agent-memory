"""Derived ledger, bounded query headers and indexed dependency edges on caller UoW."""

import json

from ..derived.model import erase_rows, source_ids
from ..domain import canonical_json

SCHEMA = """
CREATE TABLE IF NOT EXISTS derived_entries (
 partition_key TEXT NOT NULL, kind TEXT NOT NULL, identity TEXT NOT NULL,
 payload_json TEXT NOT NULL, PRIMARY KEY(partition_key,kind,identity)
);
CREATE TABLE IF NOT EXISTS derived_dependencies (
 partition_key TEXT NOT NULL, revision_id TEXT NOT NULL, parent_id TEXT NOT NULL,
 edge_kind TEXT NOT NULL CHECK(edge_kind IN ('support','processing','query')),
 PRIMARY KEY(partition_key,revision_id,parent_id,edge_kind)
);
CREATE INDEX IF NOT EXISTS derived_reverse_idx ON derived_dependencies(partition_key,parent_id);
CREATE TABLE IF NOT EXISTS derived_atom_headers (
 partition_key TEXT NOT NULL, identity TEXT NOT NULL, slot_key TEXT NOT NULL,
 payload_json TEXT NOT NULL, PRIMARY KEY(partition_key,identity)
);
CREATE INDEX IF NOT EXISTS derived_atom_slot_idx ON derived_atom_headers(partition_key,slot_key);
"""


def get(connection, scope, kind, identity):
    row = connection.execute(
        "SELECT payload_json FROM derived_entries WHERE partition_key=? AND kind=? AND identity=?",
        (scope.partition_key(), kind, identity),
    ).fetchone()
    return json.loads(row[0]) if row else None


def put(connection, scope, kind, identity, payload):
    connection.execute(
        "INSERT INTO derived_entries VALUES (?,?,?,?) ON CONFLICT "
        "(partition_key,kind,identity) DO UPDATE SET payload_json=excluded.payload_json",
        (scope.partition_key(), kind, identity, canonical_json(payload)),
    )


def records(connection, scope, kind):
    rows = connection.execute(
        "SELECT identity,payload_json FROM derived_entries "
        "WHERE partition_key=? AND kind=? ORDER BY identity LIMIT 4097",
        (scope.partition_key(), kind),
    ).fetchall()
    if len(rows) > 4096:
        raise ValueError("derived ledger capacity exceeded")
    return tuple(dict(identity=r[0], payload=json.loads(r[1])) for r in rows)


def header(connection, scope, record_id, event_id, slot_key, payload, version):
    if payload.get("deleted"):
        connection.execute(
            "DELETE FROM derived_atom_headers WHERE partition_key=? AND identity=?",
            (scope.partition_key(), record_id),
        )
        return
    data = dict(
        id=record_id,
        event_id=event_id,
        source_ids=sorted({event_id, *source_ids(payload)}),
        version=version,
        claim_id=payload.get("claim_id"),
        slot_key=slot_key,
    )
    connection.execute(
        "INSERT INTO derived_atom_headers VALUES (?,?,?,?) ON CONFLICT "
        "(partition_key,identity) DO UPDATE SET slot_key=excluded.slot_key, "
        "payload_json=excluded.payload_json",
        (scope.partition_key(), record_id, slot_key, canonical_json(data)),
    )


def candidates(connection, scope, slots):
    marks = ",".join("?" for _ in slots)
    rows = connection.execute(
        "SELECT payload_json FROM derived_atom_headers WHERE partition_key=? "
        f"AND slot_key IN ({marks}) ORDER BY identity LIMIT 65",
        (scope.partition_key(), *slots),
    ).fetchall()
    return tuple(json.loads(r[0]) for r in rows)


def edges(connection, scope, revision_id, values):
    connection.execute(
        "DELETE FROM derived_dependencies WHERE partition_key=? AND revision_id=?",
        (scope.partition_key(), revision_id),
    )
    connection.executemany(
        "INSERT INTO derived_dependencies VALUES (?,?,?,?)",
        [(scope.partition_key(), revision_id, parent, kind) for kind, parent in values],
    )


def reverse(connection, scope, parent):
    rows = connection.execute(
        "SELECT DISTINCT revision_id FROM derived_dependencies "
        "WHERE partition_key=? AND parent_id=? ORDER BY revision_id LIMIT 4097",
        (scope.partition_key(), parent),
    ).fetchall()
    if len(rows) > 4096:
        raise ValueError("derived reverse capacity exceeded")
    return tuple(r[0] for r in rows)


def forget(connection, request):
    rows = []
    for kind in (
        "authority",
        "query",
        "grant",
        "revision",
        "head",
        "definition",
        "job",
        "request",
        "history_point",
    ):
        rows.extend(dict(kind=kind, **r) for r in records(connection, request.scope, kind))
    ids = set(request.memory_ids)
    headers = connection.execute(
        "SELECT identity,slot_key,payload_json FROM derived_atom_headers WHERE partition_key=?",
        (request.scope.partition_key(),),
    ).fetchall()
    parents = (
        {"source:" + key for key in ids}
        | {"atom:" + key for key in ids}
        | {"derived:" + key for key in ids}
    )
    slots = set()
    for key, slot, data in headers:
        if (
            request.all_in_scope
            or key in ids
            or json.loads(data).get("claim_id") in ids
            or ids.intersection(json.loads(data)["source_ids"])
        ):
            parents.add("atom:" + key)
            slots.add(slot)
            connection.execute(
                "DELETE FROM derived_atom_headers WHERE partition_key=? AND identity=?",
                (request.scope.partition_key(), key),
            )
    changes, affected = erase_rows(rows, parents, request.all_in_scope, slots)
    for kind, key, payload in changes:
        put(connection, request.scope, kind, key, payload)
    for slot in slots:
        barrier = get(connection, request.scope, "barrier", slot) or {"generation": 0}
        put(connection, request.scope, "barrier", slot, {"generation": barrier["generation"] + 1})
    for item in rows:
        if item["kind"] == "revision" and (
            request.all_in_scope or item["payload"].get("facet_id") in affected
        ):
            edges(connection, request.scope, item["identity"], [])


def reconcile_headers(connection, scope):
    """Primary deletion can conservatively tombstone other rows in the same slot."""
    rows = connection.execute(
        "SELECT * FROM admission_records WHERE partition_key=?", (scope.partition_key(),)
    ).fetchall()
    for row in rows:
        header(
            connection,
            scope,
            row["record_id"],
            row["event_id"],
            row["slot_key"],
            json.loads(row["payload_json"]),
            row["version"],
        )
