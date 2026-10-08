"""PostgreSQL project candidate census; caller owns namespace lock/transaction."""

from agent_memory.derived.model import DerivedError
from agent_memory.derived.project_index import (
    INDEX_SCHEMA,
    MAX_BACKFILL,
    checked_gate,
    checked_headers,
    header_keys,
    query_keys,
)


async def replace(connection, scope, candidate_id, header):
    await connection.execute(
        "DELETE FROM agent_memory_derived_project_routes "
        "WHERE partition_key=%s AND candidate_id=%s",
        (scope.partition_key(), candidate_id),
    )
    for key in header_keys(header):
        await connection.execute(
            "INSERT INTO agent_memory_derived_project_routes VALUES (%s,%s,%s)",
            (scope.partition_key(), key, candidate_id),
        )


async def ensure(connection, scope):
    from . import derived

    old = checked_gate(await derived.get(connection, scope, "project_index", "scope"))
    if old and old["state"] in {"ready", "scope"}:
        return old
    cursor = await connection.execute(
        "SELECT record_id,event_id,slot_key,payload_json,version "
        "FROM agent_memory_admission_records "
        "WHERE partition_key=%s ORDER BY record_id LIMIT %s",
        (scope.partition_key(), MAX_BACKFILL + 1),
    )
    rows = await cursor.fetchall()
    await connection.execute(
        "DELETE FROM agent_memory_derived_project_routes WHERE partition_key=%s",
        (scope.partition_key(),),
    )
    state = "scope" if len(rows) > MAX_BACKFILL else "ready"
    if state == "ready":
        for row in rows:
            await derived.header(
                connection,
                scope,
                row["record_id"],
                row["event_id"],
                row["slot_key"],
                row["payload_json"],
                row["version"],
            )
    value = dict(schema=INDEX_SCHEMA, state=state, generation=(old or {}).get("generation", 0) + 1)
    await derived.put(connection, scope, "project_index", "scope", value)
    barrier = await derived.get(connection, scope, "barrier", "route:fallback") or {"generation": 0}
    await derived.put(
        connection, scope, "barrier", "route:fallback", {"generation": barrier["generation"] + 1}
    )
    return value


async def candidates(connection, scope, contract, project_id):
    if (await ensure(connection, scope))["state"] != "ready":
        raise DerivedError("project_candidate_index_incomplete")
    keys = query_keys(contract, project_id)
    buckets = " UNION ".join(
        "SELECT candidate_id FROM (SELECT candidate_id "
        "FROM agent_memory_derived_project_routes "
        "WHERE partition_key=%s AND route_key=%s ORDER BY candidate_id LIMIT 65) bucket"
        for _ in keys
    )
    parameters = tuple(value for key in keys for value in (scope.partition_key(), key))
    cursor = await connection.execute(
        "SELECT h.payload_json FROM (" + buckets + " ORDER BY candidate_id LIMIT 65) r "
        "LEFT JOIN agent_memory_derived_atom_headers h "
        "ON h.partition_key=%s AND h.identity=r.candidate_id ORDER BY r.candidate_id",
        (*parameters, scope.partition_key()),
    )
    return checked_headers(
        (row["payload_json"] for row in await cursor.fetchall()), contract, project_id
    )


async def scrub(connection, scope):
    from . import derived

    await connection.execute(
        "DELETE FROM agent_memory_derived_project_routes WHERE partition_key=%s",
        (scope.partition_key(),),
    )
    await connection.execute(
        "DELETE FROM agent_memory_derived_entries WHERE partition_key=%s AND kind='barrier' "
        "AND (identity LIKE 'route:project:%%' OR identity LIKE 'route:project-unbound:%%' "
        "OR identity='route:project-wildcard')",
        (scope.partition_key(),),
    )
    old = await derived.get(connection, scope, "project_index", "scope") or {}
    await derived.put(
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


async def source_metadata(connection, scope, source_id):
    from agent_memory.derived.project_index import source_proof

    from . import retention

    cursor = await connection.execute(
        "SELECT content_hash,metadata_json #> '{_retention,document_id}' AS document_id,"
        "metadata_json #> '{_retention,revision}' AS revision,"
        "jsonb_typeof(metadata_json -> '_retention') AS retained_type "
        "FROM agent_memory_events WHERE partition_key=%s AND id=%s AND archived_at IS NULL",
        (scope.partition_key(), source_id),
    )
    row = await cursor.fetchone()
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
        await retention.head_get(connection, scope, "document", row["document_id"])
        if retained
        else None
    )
    return source_proof(source_id, row["content_hash"], retained, head)
