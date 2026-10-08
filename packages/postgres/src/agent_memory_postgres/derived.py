"""PostgreSQL derived SQL; the caller owns transaction and namespace lock."""

from copy import deepcopy

from agent_memory.derived.model import erase_rows
from agent_memory.derived.subscriptions import (
    HEADER_SCHEMA,
    candidate_owner,
    header_data,
    source_key,
)
from agent_memory.domain import MemoryScope, canonical_json
from agent_memory.operations.refresh_schedule_contract import (
    KINDS,
    checked_record,
    immutable,
    projection,
    snapshot,
)

from . import refresh_schedule


async def get(connection, scope, kind, identity):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_derived_entries "
        "WHERE partition_key=%s AND kind=%s AND identity=%s",
        (scope.partition_key(), kind, identity),
    )
    row = await cursor.fetchone()
    return checked_record(kind, row["payload_json"] if row else None)


async def put(connection, scope, kind, identity, payload):
    if kind in KINDS:
        payload = checked_record(kind, snapshot(payload))
    if kind == "refresh_demand":
        projection(scope, identity, payload)
    if kind in KINDS:
        await refresh_schedule.lock(connection, scope)
    if kind == "refresh_execution":
        immutable(await get(connection, scope, kind, identity), payload)
    await connection.execute(
        "INSERT INTO agent_memory_derived_entries VALUES (%s,%s,%s,%s::jsonb) "
        "ON CONFLICT (partition_key,kind,identity) DO UPDATE SET "
        "payload_json=excluded.payload_json",
        (scope.partition_key(), kind, identity, canonical_json(payload)),
    )

    if kind == "refresh_demand":
        await refresh_schedule.project(connection, scope, identity, payload)


async def records(connection, scope, kind):
    cursor = await connection.execute(
        "SELECT identity,payload_json FROM agent_memory_derived_entries "
        "WHERE partition_key=%s AND kind=%s ORDER BY identity LIMIT 4097",
        (scope.partition_key(), kind),
    )
    rows = await cursor.fetchall()
    if len(rows) > 4096:
        raise ValueError("derived ledger capacity exceeded")
    return tuple(
        dict(identity=r["identity"], payload=checked_record(kind, r["payload_json"]))
        for r in rows
    )


async def get_header(connection, scope, record_id):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_derived_atom_headers "
        "WHERE partition_key=%s AND identity=%s",
        (scope.partition_key(), record_id),
    )
    row = await cursor.fetchone()
    return row["payload_json"] if row else None


async def headers(connection, scope):
    """Bounded metadata census; 4097 signals overflow to the scope fallback gate."""
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_derived_atom_headers "
        "WHERE partition_key=%s ORDER BY identity LIMIT 4097",
        (scope.partition_key(),),
    )
    result = [row["payload_json"] for row in await cursor.fetchall()]
    if len(result) <= 4096:
        for index, old in enumerate(result):
            if old.get("schema") == HEADER_SCHEMA:
                continue
            cursor = await connection.execute(
                "SELECT event_id,slot_key,payload_json,version "
                "FROM agent_memory_admission_records WHERE partition_key=%s AND record_id=%s",
                (scope.partition_key(), old["id"]),
            )
            row = await cursor.fetchone()
            if row is None or row["payload_json"].get("deleted"):
                raise ValueError("derived candidate header has no live admission record")
            await header(
                connection,
                scope,
                old["id"],
                row["event_id"],
                row["slot_key"],
                row["payload_json"],
                row["version"],
                routes=False,
            )
            result[index] = await get_header(connection, scope, old["id"])
    return tuple(result)


async def header(
    connection, scope, record_id, event_id, slot_key, payload, version, *, routes=True
):
    payload = deepcopy(payload)
    if payload.get("deleted"):
        await connection.execute(
            "DELETE FROM agent_memory_derived_atom_headers WHERE partition_key=%s AND identity=%s",
            (scope.partition_key(), record_id),
        )
        if routes:
            await edges(connection, scope, candidate_owner(record_id), ())
        return
    data = header_data(record_id, event_id, slot_key, payload, version)
    await connection.execute(
        "INSERT INTO agent_memory_derived_atom_headers VALUES (%s,%s,%s,%s::jsonb) "
        "ON CONFLICT (partition_key,identity) DO UPDATE SET slot_key=excluded.slot_key, "
        "payload_json=excluded.payload_json",
        (scope.partition_key(), record_id, slot_key, canonical_json(data)),
    )
    if routes:
        await edges(
            connection,
            scope,
            candidate_owner(record_id),
            [("query", source_key(key)) for key in data["source_ids"]],
        )


async def candidates(connection, scope, slots):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_derived_atom_headers "
        "WHERE partition_key=%s AND slot_key=ANY(%s) ORDER BY identity LIMIT 65",
        (scope.partition_key(), list(slots)),
    )
    return tuple(r["payload_json"] for r in await cursor.fetchall())


async def edges(connection, scope, revision_id, values):
    values = deepcopy(tuple(values))
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
        "page_block",
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


async def scrub_routes(connection, scope):
    """Erase derived routing selectors and fence their exact-scope rebuild."""
    await connection.execute(
        "DELETE FROM agent_memory_derived_dependencies "
        "WHERE partition_key=%s AND revision_id LIKE 'route:%%'",
        (scope.partition_key(),),
    )
    await connection.execute(
        "DELETE FROM agent_memory_derived_entries WHERE partition_key=%s AND kind='subscription'",
        (scope.partition_key(),),
    )
    old = await get(connection, scope, "subscription_index", "scope") or {}
    await put(
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
    barrier = await get(connection, scope, "barrier", "route:fallback") or {}
    await put(
        connection,
        scope,
        "barrier",
        "route:fallback",
        {
            "generation": barrier.get("generation", 0) + 1,
        },
    )


async def admission_checkpoint(connection, scope):
    cursor = await connection.execute(
        "SELECT MAX(recorded_at) AS boundary FROM agent_memory_admission_records "
        "WHERE scope_json ->> 'tenant_id'=%s AND scope_json ->> 'namespace'=%s",
        (scope.tenant_id, scope.namespace),
    )
    return (await cursor.fetchone())["boundary"]


async def changed_admission_scopes(connection, scope, checkpoint):
    """Primary scrubs/tombstones advance this namespace-locked publication boundary."""
    if checkpoint is None:
        return ()
    cursor = await connection.execute(
        "SELECT record_id,scope_json FROM agent_memory_admission_records "
        "WHERE scope_json ->> 'tenant_id'=%s AND scope_json ->> 'namespace'=%s "
        "AND recorded_at>%s ORDER BY partition_key,record_id",
        (scope.tenant_id, scope.namespace, checkpoint),
    )
    groups = {}
    for row in await cursor.fetchall():
        target = MemoryScope(**row["scope_json"])
        groups.setdefault(target, []).append(row["record_id"])
    return tuple((target, tuple(ids)) for target, ids in groups.items())
