"""Derived ledger, bounded query headers and indexed dependency edges on caller UoW."""

import json

from agent_memory.operations.refresh_schedule_contract import (
    KINDS,
    checked_record,
    immutable,
    projection,
    snapshot,
)

from ..derived.model import erase_rows
from ..derived.subscriptions import HEADER_SCHEMA, candidate_owner, header_data, source_key
from ..domain import MemoryScope, canonical_json
from . import sqlite_refresh_schedule as refresh_schedule

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


SCHEMA += refresh_schedule.SCHEMA


def get(connection, scope, kind, identity):
    row = connection.execute(
        "SELECT payload_json FROM derived_entries WHERE partition_key=? AND kind=? AND identity=?",
        (scope.partition_key(), kind, identity),
    ).fetchone()
    return checked_record(kind, json.loads(row[0]) if row else None)


def put(connection, scope, kind, identity, payload):
    if kind in KINDS:
        payload = checked_record(kind, snapshot(payload))
    if kind == "refresh_demand":
        projection(scope, identity, payload)
    if kind == "refresh_execution":
        immutable(get(connection, scope, kind, identity), payload)
    connection.execute(
        "INSERT INTO derived_entries VALUES (?,?,?,?) ON CONFLICT "
        "(partition_key,kind,identity) DO UPDATE SET payload_json=excluded.payload_json",
        (scope.partition_key(), kind, identity, canonical_json(payload)),
    )

    if kind == "refresh_demand":
        refresh_schedule.project(connection, scope, identity, payload)


def records(connection, scope, kind):
    rows = connection.execute(
        "SELECT identity,payload_json FROM derived_entries "
        "WHERE partition_key=? AND kind=? ORDER BY identity LIMIT 4097",
        (scope.partition_key(), kind),
    ).fetchall()
    if len(rows) > 4096:
        raise ValueError("derived ledger capacity exceeded")
    return tuple(dict(identity=r[0], payload=checked_record(kind, json.loads(r[1]))) for r in rows)


def get_header(connection, scope, record_id):
    row = connection.execute(
        "SELECT payload_json FROM derived_atom_headers WHERE partition_key=? AND identity=?",
        (scope.partition_key(), record_id),
    ).fetchone()
    return json.loads(row[0]) if row else None


def headers(connection, scope):
    """Bounded metadata census; 4097 signals overflow to the scope fallback gate."""
    rows = connection.execute(
        "SELECT payload_json FROM derived_atom_headers "
        "WHERE partition_key=? ORDER BY identity LIMIT 4097",
        (scope.partition_key(),),
    ).fetchall()
    result = [json.loads(row[0]) for row in rows]
    if len(result) <= 4096:
        for index, old in enumerate(result):
            if old.get("schema") == HEADER_SCHEMA:
                continue
            row = connection.execute(
                "SELECT event_id,slot_key,payload_json,version "
                "FROM admission_records WHERE partition_key=? AND record_id=?",
                (scope.partition_key(), old["id"]),
            ).fetchone()
            if row is None or json.loads(row["payload_json"]).get("deleted"):
                raise ValueError("derived candidate header has no live admission record")
            header(
                connection,
                scope,
                old["id"],
                row["event_id"],
                row["slot_key"],
                json.loads(row["payload_json"]),
                row["version"],
                routes=False,
            )
            result[index] = get_header(connection, scope, old["id"])
    return tuple(result)


def header(connection, scope, record_id, event_id, slot_key, payload, version, *, routes=True):
    from . import sqlite_project_index

    if payload.get("deleted"):
        if routes:
            sqlite_project_index.replace(connection, scope, record_id, None)
        connection.execute(
            "DELETE FROM derived_atom_headers WHERE partition_key=? AND identity=?",
            (scope.partition_key(), record_id),
        )
        if routes:
            edges(connection, scope, candidate_owner(record_id), ())
        return
    data = header_data(record_id, event_id, slot_key, payload, version)
    if routes:
        sqlite_project_index.replace(connection, scope, record_id, data)
    connection.execute(
        "INSERT INTO derived_atom_headers VALUES (?,?,?,?) ON CONFLICT "
        "(partition_key,identity) DO UPDATE SET slot_key=excluded.slot_key, "
        "payload_json=excluded.payload_json",
        (scope.partition_key(), record_id, slot_key, canonical_json(data)),
    )
    if routes:
        edges(
            connection,
            scope,
            candidate_owner(record_id),
            [("query", source_key(key)) for key in data["source_ids"]],
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
    from agent_memory.derived.project_index import erasure_header_keys
    from agent_memory.derived.question_erasure import QUESTION_KINDS, erase_question_rows

    rows = []
    for kind in (
        "authority",
        "query",
        "grant",
        "revision",
        "revision_header",
        "page_block",
        "head",
        "definition",
        "job",
        "request",
        "history_point",
        "history_interval",
        "model_cache_header", "model_cache_body", "model_flight",
        "model_authorization", "model_processing_grant",
        *QUESTION_KINDS,
    ):
        # Scheduler records are scrubbed by the outer B2 scope hook. Do not
        # deserialize their unsupported future schemas on the deletion path.
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
    slots, project_routes = set(), set()
    for key, slot, data in headers:
        if (
            request.all_in_scope
            or key in ids
            or json.loads(data).get("claim_id") in ids
            or ids.intersection(json.loads(data)["source_ids"])
        ):
            parents.add("atom:" + key)
            slots.add(slot)
            project_routes.update(erasure_header_keys(json.loads(data)))
            connection.execute(
                "DELETE FROM derived_atom_headers WHERE partition_key=? AND identity=?",
                (request.scope.partition_key(), key),
            )
    question_changes, question_affected, question_owners = erase_question_rows(
        rows, parents, request.all_in_scope, project_routes=project_routes
    )
    # A certificate/content erase retires its whole question instance. Carry
    # that derived ancestry into model-cache erasure, including older revisions.
    parents.update("derived:" + key for key in question_affected)
    changes, affected = erase_rows(rows, parents, request.all_in_scope, slots)
    affected.update(question_affected)
    # Scheduler metadata is physically scrubbed by the outer B2 scope hook.
    # Bypass neither immutable execution guards nor indexed-demand validation.
    changes.extend((kind, key, value) for kind, key, value in question_changes
                   if kind not in KINDS)
    for kind, key, payload in changes:
        put(connection, request.scope, kind, key, payload)
    for kind, key, payload in changes:
        if kind == "model_cache_header":
            edges(connection, request.scope, "model-cache:" + key, [])
    for owner in question_owners:
        edges(connection, request.scope, owner, [])
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


def scrub_routes(connection, scope):
    """Erase derived routing selectors and fence their exact-scope rebuild."""
    from . import sqlite_project_index

    sqlite_project_index.scrub(connection, scope)
    connection.execute(
        "DELETE FROM derived_dependencies WHERE partition_key=? AND revision_id LIKE 'route:%'",
        (scope.partition_key(),),
    )
    connection.execute(
        "DELETE FROM derived_entries WHERE partition_key=? AND kind='subscription'",
        (scope.partition_key(),),
    )
    old = get(connection, scope, "subscription_index", "scope") or {}
    put(
        connection,
        scope,
        "subscription_index",
        "scope",
        {
            "schema": "derived-subscription-index/1",
            "state": "needs_backfill",
            "generation": old.get("generation", 0) + 1,
        },
    )
    barrier = get(connection, scope, "barrier", "route:fallback") or {}
    put(
        connection,
        scope,
        "barrier",
        "route:fallback",
        {
            "generation": barrier.get("generation", 0) + 1,
        },
    )


def admission_checkpoint(connection, scope):
    return connection.execute(
        "SELECT MAX(recorded_at) FROM admission_records "
        "WHERE json_extract(scope_json, '$.tenant_id')=? "
        "AND json_extract(scope_json, '$.namespace')=?",
        (scope.tenant_id, scope.namespace),
    ).fetchone()[0]


def changed_admission_scopes(connection, scope, checkpoint):
    """Primary scrubs/tombstones advance this namespace-locked publication boundary."""
    if checkpoint is None:
        return ()
    rows = connection.execute(
        "SELECT record_id,scope_json FROM admission_records "
        "WHERE json_extract(scope_json, '$.tenant_id')=? "
        "AND json_extract(scope_json, '$.namespace')=? AND recorded_at>? "
        "ORDER BY partition_key,record_id",
        (scope.tenant_id, scope.namespace, checkpoint),
    ).fetchall()
    groups = {}
    for row in rows:
        target = MemoryScope(**json.loads(row["scope_json"]))
        groups.setdefault(target, []).append(row["record_id"])
    return tuple((target, tuple(ids)) for target, ids in groups.items())


def gc_snapshot(connection, scope, *, max_records, max_edges, max_bytes):
    """Complete exact-scope census, refusing overflow before loading any bodies."""
    from ..derived.question_gc import census_limits

    census_limits(max_records, max_edges, max_bytes)
    args = (scope.partition_key(),)
    sizes = connection.execute(
        "SELECT length(CAST(payload_json AS BLOB)) + length(CAST(kind AS BLOB)) "
        "+ length(CAST(identity AS BLOB)) FROM derived_entries "
        "WHERE partition_key=? ORDER BY kind,identity LIMIT ?", (*args, max_records + 1)
    ).fetchall()
    edge_sizes = connection.execute(
        "SELECT length(CAST(revision_id AS BLOB)) + length(CAST(parent_id AS BLOB)) "
        "+ length(CAST(edge_kind AS BLOB)) FROM derived_dependencies "
        "WHERE partition_key=? ORDER BY revision_id,parent_id,edge_kind LIMIT ?",
        (*args, max_edges + 1),
    ).fetchall()
    reservation_sizes = connection.execute(
        "SELECT length(CAST(execution_id AS BLOB)) FROM refresh_schedule_reservations "
        "WHERE partition_key=? ORDER BY execution_id LIMIT ?", (*args, max_records + 1)
    ).fetchall()
    if (len(sizes) > max_records or len(edge_sizes) > max_edges
            or len(reservation_sizes) > max_records
            or sum(r[0] for r in (*sizes, *edge_sizes, *reservation_sizes)) > max_bytes):
        return None
    try:
        rows = connection.execute(
            "SELECT kind,identity,payload_json FROM derived_entries "
            "WHERE partition_key=? ORDER BY kind,identity LIMIT ?", (*args, max_records + 1)
        ).fetchall()
        edges = connection.execute(
            "SELECT revision_id,parent_id,edge_kind FROM derived_dependencies "
            "WHERE partition_key=? ORDER BY revision_id,parent_id,edge_kind LIMIT ?",
            (*args, max_edges + 1),
        ).fetchall()
        reservations = connection.execute(
            "SELECT execution_id FROM refresh_schedule_reservations "
            "WHERE partition_key=? ORDER BY execution_id LIMIT ?", (*args, max_records + 1)
        ).fetchall()
        return dict(
            rows=tuple(dict(kind=r[0], identity=r[1], payload=json.loads(r[2])) for r in rows),
            edges=tuple(dict(revision_id=r[0], parent_id=r[1], edge_kind=r[2]) for r in edges),
            reservations=tuple(r[0] for r in reservations),
        )
    except (ValueError, RecursionError):
        return None


def gc_delete(connection, scope, kind, identity):
    from ..derived.question_gc import COLLECTIBLE_KINDS

    if kind not in COLLECTIBLE_KINDS:
        raise ValueError("unsupported question GC kind")
    connection.execute(
        "DELETE FROM derived_entries WHERE partition_key=? AND kind=? AND identity=?",
        (scope.partition_key(), kind, identity),
    )
    # Edges are keyed only by identity. Another kind sharing that identity still
    # owns them; never remove its reverse-erasure or qualification dependencies.
    connection.execute(
        "DELETE FROM derived_dependencies WHERE partition_key=? AND revision_id=? "
        "AND NOT EXISTS (SELECT 1 FROM derived_entries WHERE partition_key=? AND identity=?)",
        (scope.partition_key(), identity, scope.partition_key(), identity),
    )
