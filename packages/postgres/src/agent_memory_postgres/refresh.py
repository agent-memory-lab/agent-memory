"""Resource refresh SQL on the existing scope-locked transaction connection."""

from agent_memory.domain import canonical_json


async def get(connection, scope, kind, identity):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_resource_refresh "
        "WHERE partition_key=%s AND kind=%s AND identity=%s",
        (scope.partition_key(), kind, identity),
    )
    row = await cursor.fetchone()
    return row["payload_json"] if row else None


async def put(connection, scope, kind, identity, payload):
    await connection.execute(
        "INSERT INTO agent_memory_resource_refresh VALUES (%s,%s,%s,%s::jsonb) "
        "ON CONFLICT(partition_key,kind,identity) DO UPDATE SET payload_json=excluded.payload_json",
        (scope.partition_key(), kind, identity, canonical_json(payload)),
    )


async def records(connection, scope, kind):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_resource_refresh WHERE partition_key=%s AND kind=%s "
        "ORDER BY identity LIMIT 4097",
        (scope.partition_key(), kind),
    )
    rows = await cursor.fetchall()
    if len(rows) > 4096:
        raise ValueError("refresh ledger capacity exceeded")
    return tuple(row["payload_json"] for row in rows)


async def forget(connection, request):
    ids, affected = set(request.memory_ids), set()
    for row in await records(connection, request.scope, "resource"):
        sources = {s for values in row["units"].values() for s in values}
        if request.all_in_scope or ids & sources:
            affected.add(row["resource_id"])
            row.update(
                status="cancelled",
                reason="source_unavailable",
                units={},
                requested_through=[],
                claimed_through=[],
                completed_through=[],
                commits={},
                unit_commits={},
                checkpoint={},
            )
            for field in ("lease_token", "lease_until", "last_completed_fence"):
                row.pop(field, None)
            await put(connection, request.scope, "resource", row["resource_id"], row)
    for row in await records(connection, request.scope, "request"):
        if row["resource_id"] in affected:
            row.update(invalidated=True, units={})
            await put(connection, request.scope, "request", row["request_id"], row)
