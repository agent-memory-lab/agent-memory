"""Identity-only delivery contracts under the existing admission scope lock."""

from agent_memory.domain import canonical_json


async def get(connection, scope, kind, identity):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_retention_delivery "
        "WHERE partition_key=%s AND kind=%s AND identity=%s",
        (scope.partition_key(), kind, identity),
    )
    row = await cursor.fetchone()
    return row["payload_json"] if row else None


async def insert(connection, scope, kind, identity, payload):
    await connection.execute(
        "INSERT INTO agent_memory_retention_delivery VALUES (%s,%s,%s,%s::jsonb)",
        (scope.partition_key(), kind, identity, canonical_json(payload)),
    )


async def count(connection, scope, kind):
    cursor = await connection.execute(
        "SELECT count(*) AS count FROM agent_memory_retention_delivery "
        "WHERE partition_key=%s AND kind=%s",
        (scope.partition_key(), kind),
    )
    return (await cursor.fetchone())["count"]
