"""Candidate locator SQL, on the publication/deletion transaction connection."""

from agent_memory.domain import canonical_json


async def job_get(connection, scope, channel, epoch, token_id):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_index_jobs WHERE partition_key=%s AND channel=%s "
        "AND epoch=%s AND token_id=%s",
        (scope.partition_key(), channel, epoch, token_id),
    )
    row = await cursor.fetchone()
    return row["payload_json"] if row else None


async def job_put(connection, scope, row):
    await connection.execute(
        "INSERT INTO agent_memory_index_jobs VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb) "
        "ON CONFLICT(partition_key,channel,epoch,token_id) DO UPDATE SET "
        "status=excluded.status,payload_json=excluded.payload_json",
        (
            scope.partition_key(),
            row["channel"],
            row["epoch"],
            row["token"]["id"],
            row["sequence"],
            row["event_id"],
            row["status"],
            canonical_json(row),
        ),
    )


async def jobs(connection, scope, channel, epoch):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_index_jobs WHERE partition_key=%s AND channel=%s "
        "AND epoch=%s "
        "ORDER BY sequence LIMIT 100001",
        (scope.partition_key(), channel, epoch),
    )
    rows = await cursor.fetchall()
    if len(rows) > 100000:
        raise ValueError("index ledger capacity exceeded")
    return tuple(r["payload_json"] for r in rows)


async def document_get(connection, scope, channel, candidate_id):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_index_documents WHERE partition_key=%s "
        "AND channel=%s "
        "AND candidate_id=%s",
        (scope.partition_key(), channel, candidate_id),
    )
    row = await cursor.fetchone()
    return row["payload_json"] if row else None


async def document_put(connection, scope, channel, candidate_id, document):
    if document is None:
        await connection.execute(
            "DELETE FROM agent_memory_index_documents WHERE partition_key=%s AND channel=%s "
            "AND candidate_id=%s",
            (scope.partition_key(), channel, candidate_id),
        )
    else:
        await connection.execute(
            "INSERT INTO agent_memory_index_documents VALUES (%s,%s,%s,%s,%s,%s::jsonb) "
            "ON CONFLICT(partition_key,channel,candidate_id) DO UPDATE SET "
            "slot_key=excluded.slot_key,event_id=excluded.event_id,payload_json=excluded.payload_json",
            (
                scope.partition_key(),
                channel,
                candidate_id,
                document["slot_key"],
                document["event_id"],
                canonical_json(document),
            ),
        )


async def lookup(connection, scope, channel, slot_key, limit):
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_index_documents WHERE partition_key=%s "
        "AND channel=%s "
        "AND slot_key=%s "
        "ORDER BY candidate_id LIMIT %s",
        (scope.partition_key(), channel, slot_key, limit),
    )
    rows = await cursor.fetchall()
    return tuple(r["payload_json"] for r in rows)


async def forget(connection, request):
    condition, params = "partition_key=%s", (request.scope.partition_key(),)
    if not request.all_in_scope:
        if not request.memory_ids:
            return
        condition += f" AND event_id IN ({','.join('%s' for _ in request.memory_ids)})"
        params += tuple(request.memory_ids)
    await connection.execute(f"DELETE FROM agent_memory_index_documents WHERE {condition}", params)
    cursor = await connection.execute(
        f"SELECT payload_json FROM agent_memory_index_jobs WHERE {condition}",
        params,
    )
    rows = await cursor.fetchall()
    for value in rows:
        row = value["payload_json"]
        row["status"] = "cancelled"
        for field in ("proof", "lease_token", "applied"):
            row.pop(field, None)
        await job_put(connection, request.scope, row)


async def invalidate_records(connection, candidate_ids):
    """Affected identities are selected under the existing namespace deletion lock."""
    identities = sorted(candidate_ids)
    for offset in range(0, len(identities), 128):
        chunk = identities[offset : offset + 128]
        await connection.execute(
            "DELETE FROM agent_memory_index_documents WHERE candidate_id=ANY(%s)",
            (chunk,),
        )
        cursor = await connection.execute(
            "SELECT partition_key,payload_json FROM agent_memory_index_jobs j WHERE EXISTS ("
            "SELECT 1 FROM jsonb_array_elements(j.payload_json->'dispositions') d "
            "WHERE d->>'candidate_id'=ANY(%s))",
            (chunk,),
        )
        for value in await cursor.fetchall():
            row = value["payload_json"]
            row["status"] = "cancelled"
            for field in ("proof", "lease_token", "applied"):
                row.pop(field, None)
            await connection.execute(
                "UPDATE agent_memory_index_jobs SET status='cancelled',payload_json=%s::jsonb "
                "WHERE partition_key=%s AND channel=%s AND epoch=%s AND token_id=%s",
                (
                    canonical_json(row),
                    value["partition_key"],
                    row["channel"],
                    row["epoch"],
                    row["token"]["id"],
                ),
            )
