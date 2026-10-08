"""PostgreSQL transaction adapter for the shared monetary budget contract."""

import json

from agent_memory.retrieval.model_contracts import ModelError, canonical, hash_value


async def lock(connection):
    # Repository-wide money authority: short transactions, stable lock order,
    # acquired before any source/scope lifecycle lock. Never held during HTTP.
    await connection.execute("SELECT pg_advisory_xact_lock(170700501)")


async def get(connection, kind, key):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_model_budget_entries WHERE kind=%s AND identity=%s",
        (kind, key),
    )
    row = await cursor.fetchone()
    return json.loads(row["payload_json"]) if row else None


async def put(connection, kind, key, payload):
    hash_value(key)
    if await get(connection, kind, key) is None:
        cursor = await connection.execute(
            "SELECT count(*) FROM agent_memory_model_budget_entries WHERE kind=%s", (kind,)
        )
        if (await cursor.fetchone())["count"] >= 4096:
            raise ModelError("model_budget_audit_capacity")
    await connection.execute(
        "INSERT INTO agent_memory_model_budget_entries VALUES (%s,%s,%s) "
        "ON CONFLICT(kind,identity) "
        "DO UPDATE SET payload_json=excluded.payload_json",
        (kind, key, canonical(payload)),
    )


async def records(connection, kind):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_model_budget_entries "
        "WHERE kind=%s ORDER BY identity",
        (kind,),
    )
    return [json.loads(r["payload_json"]) for r in await cursor.fetchall()]
