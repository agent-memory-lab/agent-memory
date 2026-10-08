"""The local-artifact identity fence upgrades an existing PostgreSQL store."""

import asyncio
import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import pytest
from agent_memory_postgres import build_postgres_kernel
from psycopg import AsyncConnection, sql

from agent_memory import ForgetMode, ForgetRequest, MemoryBlock, MemoryEvent, MemoryScope


def test_live_existing_schema_adds_identity_only_artifact_fences():
    async def scenario():
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN", "")
        if not dsn:
            pytest.skip("real PostgreSQL artifact migration requires a test DSN")
        parsed = urlsplit(dsn)
        if parsed.scheme not in {"postgres", "postgresql"} or "test" not in parsed.path.casefold():
            pytest.fail("artifact migration requires a PostgreSQL test database")
        schema = "artifact_fence_" + uuid4().hex
        async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
            await connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        query = dict(parse_qsl(parsed.query))
        query["options"] = "-csearch_path=" + schema
        scoped = urlunsplit(parsed._replace(query=urlencode(query)))
        kernel = build_postgres_kernel(scoped)
        reopened = None
        try:
            await kernel.initialize()
            scope = MemoryScope("artifact-upgrade", user_id="alice")
            event = MemoryEvent(scope, "user.message", "Existing evidence")
            await kernel.ingest_event(event)
            block = await kernel.write_block(
                MemoryBlock(
                    scope,
                    "Existing",
                    "Existing block",
                    (event.id,),
                )
            )
            async with kernel._repository.pool.connection() as connection:
                await connection.execute("DROP TABLE agent_memory_memory_tombstones")
            await kernel.close()
            reopened = build_postgres_kernel(scoped)
            await reopened.initialize()
            assert await reopened.read_block(scope, block.id) == block
            await reopened.forget(ForgetRequest(scope, (event.id,), mode=ForgetMode.ERASE))
            async with reopened._repository.pool.connection() as connection:
                cursor = await connection.execute(
                    "SELECT * FROM agent_memory_memory_tombstones WHERE memory_table='artifacts'"
                )
                row = await cursor.fetchone()
                assert row["id"] == block.id
                assert set(row) == {
                    "id",
                    "partition_key",
                    "memory_table",
                    "tenant_id",
                    "namespace",
                    "user_id",
                    "agent_id",
                    "workspace_id",
                    "session_id",
                }
            with pytest.raises(ValueError, match="dependencies"):
                async with reopened._repository.unit_of_work() as uow:
                    await uow.save_block(block, 0)
        finally:
            await kernel.close()
            if reopened:
                await reopened.close()
            async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
                await connection.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    asyncio.run(scenario())
