"""Identity-only purge journal under the caller's admission namespace lock."""


async def head(connection, scope):
    cursor = await connection.execute(
        "SELECT cursor FROM agent_memory_retention_purge_heads WHERE partition_key=%s",
        (scope.partition_key(),),
    )
    row = await cursor.fetchone()
    return row["cursor"] if row else 0


async def record(connection, request, epoch):
    identities = ("",) if request.all_in_scope else tuple(sorted(set(request.memory_ids)))
    if not identities:
        return
    key, start = request.scope.partition_key(), await head(connection, request.scope)
    async with connection.cursor() as cursor:
        await cursor.executemany(
            "INSERT INTO agent_memory_retention_purges VALUES (%s,%s,%s,%s,%s,%s)",
            (
                (key, start + index, identity, epoch, request.all_in_scope, request.mode.value)
                for index, identity in enumerate(identities, 1)
            ),
        )
    await connection.execute(
        "INSERT INTO agent_memory_retention_purge_heads VALUES (%s,%s) "
        "ON CONFLICT(partition_key) DO UPDATE SET cursor=excluded.cursor",
        (key, start + len(identities)),
    )


async def page(connection, scope, after, limit):
    cursor = await connection.execute(
        "SELECT cursor,source_event_id,epoch,all_in_scope,mode "
        "FROM agent_memory_retention_purges WHERE partition_key=%s AND cursor>%s "
        "ORDER BY cursor LIMIT %s",
        (scope.partition_key(), after, limit),
    )
    return tuple(await cursor.fetchall())


async def erased(connection, scope, identity):
    cursor = await connection.execute(
        "SELECT 1 FROM agent_memory_retention_purges "
        "WHERE partition_key=%s AND source_event_id=%s LIMIT 1",
        (scope.partition_key(), identity),
    )
    return await cursor.fetchone() is not None
