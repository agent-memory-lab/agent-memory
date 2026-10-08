"""Exact-scope project candidate indexes on the admission writer connection."""

import json

from ..derived.model import DerivedError
from ..derived.project_index import (
    INDEX_SCHEMA,
    MAX_BACKFILL,
    checked_gate,
    checked_headers,
    header_keys,
    query_keys,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS derived_project_routes (
 partition_key TEXT NOT NULL, route_key TEXT NOT NULL, candidate_id TEXT NOT NULL,
 PRIMARY KEY(partition_key,route_key,candidate_id)
);
CREATE INDEX IF NOT EXISTS derived_project_candidate_idx
 ON derived_project_routes(partition_key,candidate_id);
"""


def replace(connection, scope, candidate_id, header):
    connection.execute(
        "DELETE FROM derived_project_routes WHERE partition_key=? AND candidate_id=?",
        (scope.partition_key(), candidate_id),
    )
    connection.executemany(
        "INSERT INTO derived_project_routes VALUES (?,?,?)",
        [(scope.partition_key(), key, candidate_id) for key in header_keys(header)],
    )


def ensure(connection, scope):
    """Caller holds BEGIN IMMEDIATE; a ready gate makes all later reads indexed."""
    from . import sqlite_derived as derived

    old = checked_gate(derived.get(connection, scope, "project_index", "scope"))
    if old and old["state"] in {"ready", "scope"}:
        return old
    rows = connection.execute(
        "SELECT record_id,event_id,slot_key,payload_json,version FROM admission_records "
        "WHERE partition_key=? ORDER BY record_id LIMIT ?",
        (scope.partition_key(), MAX_BACKFILL + 1),
    ).fetchall()
    connection.execute(
        "DELETE FROM derived_project_routes WHERE partition_key=?", (scope.partition_key(),)
    )
    state = "scope" if len(rows) > MAX_BACKFILL else "ready"
    if state == "ready":
        for row in rows:
            derived.header(
                connection,
                scope,
                row["record_id"],
                row["event_id"],
                row["slot_key"],
                json.loads(row["payload_json"]),
                row["version"],
            )
    value = dict(schema=INDEX_SCHEMA, state=state, generation=(old or {}).get("generation", 0) + 1)
    derived.put(connection, scope, "project_index", "scope", value)
    barrier = derived.get(connection, scope, "barrier", "route:fallback") or {"generation": 0}
    derived.put(
        connection, scope, "barrier", "route:fallback", {"generation": barrier["generation"] + 1}
    )
    return value


def candidates(connection, scope, contract, project_id):
    if ensure(connection, scope)["state"] != "ready":
        raise DerivedError("project_candidate_index_incomplete")
    keys = query_keys(contract, project_id)
    # Bound each indexed bucket before union/deduplication: even a huge wildcard
    # population visits at most 195 selectors, then returns the 65th sentinel.
    buckets = " UNION ".join(
        "SELECT candidate_id FROM (SELECT candidate_id FROM derived_project_routes "
        "WHERE partition_key=? AND route_key=? ORDER BY candidate_id LIMIT 65)"
        for _ in keys
    )
    parameters = tuple(value for key in keys for value in (scope.partition_key(), key))
    rows = connection.execute(
        "SELECT h.payload_json FROM (" + buckets + " ORDER BY candidate_id LIMIT 65) r "
        "LEFT JOIN derived_atom_headers h ON h.partition_key=? AND h.identity=r.candidate_id "
        "ORDER BY r.candidate_id",
        (*parameters, scope.partition_key()),
    ).fetchall()
    return checked_headers(
        (json.loads(row[0]) if row[0] is not None else None for row in rows), contract, project_id
    )


def scrub(connection, scope):
    """Erase selectors in exactly the scopes affected by B1 primary deletion."""
    from . import sqlite_derived as derived

    connection.execute(
        "DELETE FROM derived_project_routes WHERE partition_key=?", (scope.partition_key(),)
    )
    connection.execute(
        "DELETE FROM derived_entries WHERE partition_key=? AND kind='barrier' "
        "AND (identity LIKE 'route:project:%' OR identity LIKE 'route:project-unbound:%' "
        "OR identity='route:project-wildcard')",
        (scope.partition_key(),),
    )
    old = derived.get(connection, scope, "project_index", "scope") or {}
    derived.put(
        connection,
        scope,
        "project_index",
        "scope",
        dict(
            schema=INDEX_SCHEMA,
            state="needs_backfill",
            generation=old.get("generation", 0) + 1,
        ),
    )


def source_metadata(connection, scope, source_id):
    from ..derived.project_index import source_proof
    from . import sqlite_retention

    row = connection.execute(
        "SELECT content_hash,json_extract(metadata_json,'$._retention.document_id') AS document_id,"
        "json_extract(metadata_json,'$._retention.revision') AS revision,"
        "json_type(metadata_json,'$._retention') AS retained_type "
        "FROM events WHERE partition_key=? AND id=? AND archived_at IS NULL",
        (scope.partition_key(), source_id),
    ).fetchone()
    if row is None:
        return None
    if row["retained_type"] not in {None, "object"}:
        raise DerivedError("project_source_revision_changed")
    retained = (
        {"document_id": row["document_id"], "revision": row["revision"]}
        if row["retained_type"]
        else None
    )
    head = (
        sqlite_retention.head_get(connection, scope, "document", row["document_id"])
        if retained
        else None
    )
    return source_proof(source_id, row["content_hash"], retained, head)
