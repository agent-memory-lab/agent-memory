"""Durable receive ledger using the admission UoW connection and namespace lock."""

from agent_memory.domain import canonical_json

from . import index, purge, refresh


def kind(value):
    if value not in {"ticket", "request"}:
        raise ValueError("invalid retention record kind")
    return value


async def epoch(connection, scope):
    key = scope.partition_key()
    await connection.execute(
        "INSERT INTO agent_memory_retention_epochs VALUES (%s,0) ON CONFLICT DO NOTHING",
        (key,),
    )
    cursor = await connection.execute(
        "SELECT epoch FROM agent_memory_retention_epochs WHERE partition_key=%s",
        (key,),
    )
    return (await cursor.fetchone())["epoch"]


async def get(connection, scope, record_kind, request_id):
    cursor = await connection.execute(
        """SELECT payload_json FROM agent_memory_retention_entries
           WHERE partition_key=%s AND kind=%s AND request_id=%s""",
        (scope.partition_key(), kind(record_kind), request_id),
    )
    row = await cursor.fetchone()
    return row["payload_json"] if row else None


async def insert(connection, scope, record_kind, request_id, payload):
    await connection.execute(
        "INSERT INTO agent_memory_retention_entries VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)",
        (
            scope.partition_key(),
            kind(record_kind),
            request_id,
            payload["event_id"],
            payload.get("idempotency_key"),
            payload.get("status", "issued"),
            canonical_json(payload),
        ),
    )


async def count(connection, scope, record_kind):
    condition = (
        " AND status IN ('queued','running','retry_wait')" if kind(record_kind) == "request" else ""
    )
    cursor = await connection.execute(
        "SELECT count(*) AS count FROM agent_memory_retention_entries "
        "WHERE partition_key=%s AND kind=%s" + condition,
        (scope.partition_key(), record_kind),
    )
    return (await cursor.fetchone())["count"]


async def identity_owner(connection, scope, event_id, idempotency_key):
    cursor = await connection.execute(
        """SELECT request_id FROM agent_memory_retention_entries
           WHERE partition_key=%s AND kind='ticket' AND (event_id=%s OR idempotency_key=%s)
           LIMIT 1""",
        (scope.partition_key(), event_id, idempotency_key),
    )
    row = await cursor.fetchone()
    return row["request_id"] if row else None


async def forget(connection, request, *, journal=True):
    # Caller holds admission.lock_scope until the surrounding deletion commits.
    key = request.scope.partition_key()
    if request.all_in_scope:
        await epoch(connection, request.scope)
        await connection.execute(
            "UPDATE agent_memory_retention_epochs SET epoch=epoch+1 WHERE partition_key=%s",
            (key,),
        )
    elif not request.memory_ids:
        return
    await refresh.forget(connection, request)
    await index.forget(connection, request)
    if journal:
        await purge.record(connection, request, await epoch(connection, request.scope))
    condition = "partition_key=%s"
    params = (key,)
    if not request.all_in_scope:
        condition += " AND event_id=ANY(%s)"
        params += (list(request.memory_ids),)
    cursor = await connection.execute(
        "SELECT kind, request_id, payload_json FROM agent_memory_retention_entries WHERE "
        + condition,
        params,
    )
    for row in await cursor.fetchall():
        payload = row["payload_json"]
        for field in ("prepared", "result", "input_manifest", "lease_token", "publication_manifest"):
            payload.pop(field, None)
        status = "revoked" if row["kind"] == "ticket" else "cancelled"
        payload["revoked" if row["kind"] == "ticket" else "status"] = (
            True if row["kind"] == "ticket" else status
        )
        await connection.execute(
            """UPDATE agent_memory_retention_entries SET status=%s, payload_json=%s::jsonb
               WHERE partition_key=%s AND kind=%s AND request_id=%s""",
            (status, canonical_json(payload), key, row["kind"], row["request_id"]),
        )


async def update(connection, scope, request_id, payload):
    cursor = await connection.execute(
        "UPDATE agent_memory_retention_entries SET payload_json=%s::jsonb, status=%s "
        "WHERE partition_key=%s AND kind='request' AND request_id=%s",
        (canonical_json(payload), payload["status"], scope.partition_key(), request_id),
    )
    if cursor.rowcount != 1:
        raise ValueError("retention request is missing")


async def active(connection, scope):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_retention_entries WHERE partition_key=%s "
        "AND kind='request' AND status IN ('queued','running','retry_wait') "
        "ORDER BY request_id LIMIT 100001",
        (scope.partition_key(),),
    )
    rows = await cursor.fetchall()
    if len(rows) > 100000:
        raise ValueError("retention active request limit exceeded")
    return tuple(row["payload_json"] for row in rows)


async def producer_get(connection, scope, producer_id):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_retention_producers "
        "WHERE partition_key=%s AND producer_id=%s",
        (scope.partition_key(), producer_id),
    )
    row = await cursor.fetchone()
    return row["payload_json"] if row else None


async def producer_put(connection, scope, producer_id, payload):
    await connection.execute(
        "INSERT INTO agent_memory_retention_producers VALUES (%s,%s,%s::jsonb) "
        "ON CONFLICT(partition_key,producer_id) DO UPDATE SET payload_json=excluded.payload_json",
        (scope.partition_key(), producer_id, canonical_json(payload)),
    )


async def head_get(connection, scope, head_kind, identity):
    cursor = await connection.execute(
        "SELECT generation,payload_json FROM agent_memory_retention_heads "
        "WHERE partition_key=%s AND kind=%s AND identity=%s",
        (scope.partition_key(), head_kind, identity),
    )
    row = await cursor.fetchone()
    return {"generation": row["generation"], "payload": row["payload_json"]} if row else None


async def head_put(connection, scope, head_kind, identity, payload, expected_generation):
    from agent_memory.operations.retention import RetentionError

    if expected_generation == 0:
        cursor = await connection.execute(
            "INSERT INTO agent_memory_retention_heads VALUES (%s,%s,%s,1,%s::jsonb) "
            "ON CONFLICT DO NOTHING",
            (scope.partition_key(), head_kind, identity, canonical_json(payload)),
        )
    else:
        cursor = await connection.execute(
            "UPDATE agent_memory_retention_heads "
            "SET generation=generation+1,payload_json=%s::jsonb "
            "WHERE partition_key=%s AND kind=%s AND identity=%s AND generation=%s",
            (
                canonical_json(payload),
                scope.partition_key(),
                head_kind,
                identity,
                expected_generation,
            ),
        )
    if cursor.rowcount != 1:
        raise RetentionError(head_kind + "_head_changed")
    return expected_generation + 1


async def requests(connection, scope):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_retention_entries WHERE partition_key=%s "
        "AND kind='request' ORDER BY request_id LIMIT 10001", (scope.partition_key(),),
    )
    rows = await cursor.fetchall()
    if len(rows) > 10000:
        raise ValueError("index recovery request capacity exceeded")
    return tuple(row["payload_json"] for row in rows)
