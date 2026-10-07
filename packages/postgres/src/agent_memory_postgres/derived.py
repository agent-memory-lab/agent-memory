"""PostgreSQL derived SQL; the caller owns transaction and namespace lock."""

from agent_memory.derived.model import erase_rows, source_ids
from agent_memory.domain import canonical_json


async def get(connection, scope, kind, identity):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_derived_entries "
        "WHERE partition_key=%s AND kind=%s AND identity=%s",
        (scope.partition_key(), kind, identity),
    )
    row = await cursor.fetchone()
    return row["payload_json"] if row else None


async def put(connection, scope, kind, identity, payload):
    await connection.execute(
        "INSERT INTO agent_memory_derived_entries VALUES (%s,%s,%s,%s::jsonb) "
        "ON CONFLICT (partition_key,kind,identity) DO UPDATE SET "
        "payload_json=excluded.payload_json",
        (scope.partition_key(), kind, identity, canonical_json(payload)),
    )


async def records(connection, scope, kind):
    cursor = await connection.execute(
        "SELECT identity,payload_json FROM agent_memory_derived_entries "
        "WHERE partition_key=%s AND kind=%s ORDER BY identity LIMIT 4097",
        (scope.partition_key(), kind),
    )
    rows = await cursor.fetchall()
    if len(rows) > 4096:
        raise ValueError("derived ledger capacity exceeded")
    return tuple(dict(identity=r["identity"], payload=r["payload_json"]) for r in rows)


async def header(connection, scope, record_id, event_id, slot_key, payload, version):
    if payload.get("deleted"):
        await connection.execute(
            "DELETE FROM agent_memory_derived_atom_headers WHERE partition_key=%s AND identity=%s",
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
    await connection.execute(
        "INSERT INTO agent_memory_derived_atom_headers VALUES (%s,%s,%s,%s::jsonb) "
        "ON CONFLICT (partition_key,identity) DO UPDATE SET slot_key=excluded.slot_key, "
        "payload_json=excluded.payload_json",
        (scope.partition_key(), record_id, slot_key, canonical_json(data)),
    )


async def candidates(connection, scope, slots):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_derived_atom_headers "
        "WHERE partition_key=%s AND slot_key=ANY(%s) ORDER BY identity LIMIT 65",
        (scope.partition_key(), list(slots)),
    )
    return tuple(r["payload_json"] for r in await cursor.fetchall())


async def edges(connection, scope, revision_id, values):
    await connection.execute(
        "DELETE FROM agent_memory_derived_dependencies WHERE partition_key=%s AND revision_id=%s",
        (scope.partition_key(), revision_id),
    )
    for kind, parent in values:
        await connection.execute(
            "INSERT INTO agent_memory_derived_dependencies VALUES (%s,%s,%s,%s)",
            (scope.partition_key(), revision_id, parent, kind),
        )


async def reverse(connection, scope, parent):
    cursor = await connection.execute(
        "SELECT DISTINCT revision_id FROM agent_memory_derived_dependencies "
        "WHERE partition_key=%s AND parent_id=%s ORDER BY revision_id LIMIT 4097",
        (scope.partition_key(), parent),
    )
    rows = await cursor.fetchall()
    if len(rows) > 4096:
        raise ValueError("derived reverse capacity exceeded")
    return tuple(r["revision_id"] for r in rows)


async def forget(connection, request):
    rows = []
    for kind in (
        "authority",
        "query",
        "grant",
        "revision",
        "revision_header",
        "head",
        "definition",
        "job",
        "request",
        "history_point",
        "history_interval",
    ):
        rows.extend(dict(kind=kind, **r) for r in await records(connection, request.scope, kind))
    ids = set(request.memory_ids)
    cursor = await connection.execute(
        "SELECT identity,slot_key,payload_json "
        "FROM agent_memory_derived_atom_headers WHERE partition_key=%s",
        (request.scope.partition_key(),),
    )
    parents = (
        {"source:" + key for key in ids}
        | {"atom:" + key for key in ids}
        | {"derived:" + key for key in ids}
    )
    slots = set()
    for row in await cursor.fetchall():
        if (
            request.all_in_scope
            or row["identity"] in ids
            or row["payload_json"].get("claim_id") in ids
            or ids.intersection(row["payload_json"]["source_ids"])
        ):
            parents.add("atom:" + row["identity"])
            slots.add(row["slot_key"])
            await connection.execute(
                "DELETE FROM agent_memory_derived_atom_headers "
                "WHERE partition_key=%s AND identity=%s",
                (request.scope.partition_key(), row["identity"]),
            )
    changes, affected = erase_rows(rows, parents, request.all_in_scope, slots)
    for kind, key, payload in changes:
        await put(connection, request.scope, kind, key, payload)
    for slot in slots:
        barrier = await get(connection, request.scope, "barrier", slot) or {"generation": 0}
        await put(
            connection, request.scope, "barrier", slot, {"generation": barrier["generation"] + 1}
        )
    for item in rows:
        if item["kind"] == "revision" and (
            request.all_in_scope or item["payload"].get("facet_id") in affected
        ):
            await edges(connection, request.scope, item["identity"], [])


async def reconcile_headers(connection, scope):
    """Synchronize all exact-scope deletion effects before the caller commits."""
    cursor = await connection.execute(
        "SELECT * FROM agent_memory_admission_records WHERE partition_key=%s",
        (scope.partition_key(),),
    )
    for row in await cursor.fetchall():
        await header(
            connection,
            scope,
            row["record_id"],
            row["event_id"],
            row["slot_key"],
            row["payload_json"],
            row["version"],
        )
