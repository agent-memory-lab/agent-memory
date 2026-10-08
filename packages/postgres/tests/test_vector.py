import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import pytest
from agent_memory_postgres.semantic import PgVectorBlockMemory
from agent_memory_postgres.vector import PgVectorIndex
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from agent_memory.composition import build_local_kernel
from agent_memory.domain import ForgetMode, MemoryBlock, MemoryEvent, MemoryScope


class RecordingConnection:
    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.calls = []

    async def execute(self, statement, parameters):
        self.calls.append((statement, parameters))
        return self


class RecordingPool:
    def __init__(self, rowcount=1):
        self.recording = RecordingConnection(rowcount)

    @asynccontextmanager
    async def connection(self):
        yield self.recording


def test_vector_delete_requires_exact_scope():
    async def scenario():
        pool = RecordingPool()
        index = PgVectorIndex(pool, 8)
        scope = MemoryScope("tenant", user_id="user", session_id="session")
        with pytest.raises(TypeError):
            await index.delete("block")
        assert pool.recording.calls == []
        await index.delete("block", scope=scope)
        statement, parameters = pool.recording.calls[0]
        assert "WHERE memory_id = %s AND partition_key = %s" in statement
        assert parameters == ("block", scope.partition_key())

    asyncio.run(scenario())


def test_vector_upsert_rejects_existing_identity_outside_scope():
    async def scenario():
        pool = RecordingPool(rowcount=0)
        index = PgVectorIndex(pool, 8)
        with pytest.raises(ValueError, match="outside the authorized scope"):
            await index.upsert("block", MemoryScope("other"), (1.0,) * 8, model="model")
        statement, _ = pool.recording.calls[0]
        assert "WHERE agent_memory_vectors.partition_key = excluded.partition_key" in statement
        assert "partition_key = excluded.partition_key," not in statement
        assert "tenant_id = excluded.tenant_id" not in statement

    asyncio.run(scenario())


async def _assert_sidecar_retry(pool, path, scope, mode):
    class Embeddings:
        dimensions = 8

        async def embed(self, texts):
            return tuple((1.0,) * 8 for _ in texts)

    class InitiallyFailingIndex(PgVectorIndex):
        async def delete(self, memory_id, *, scope):
            raise RuntimeError("vector cleanup interrupted")

    provider = build_local_kernel(path)
    await provider.initialize()
    try:
        event = MemoryEvent(scope=scope, event_type="user.message", content="Saved note.")
        await provider.ingest_event(event)
        sidecar = PgVectorBlockMemory(
            provider,
            InitiallyFailingIndex(pool, 8),
            Embeddings(),
            model="test",
        )
        block = await sidecar.write_block(
            MemoryBlock(
                scope=scope,
                title="Note",
                content=event.content,
                event_ids=(event.id,),
            )
        )
        with pytest.raises(RuntimeError, match="cleanup interrupted"):
            await sidecar.forget_block(scope, block.id, mode=mode)
        assert await provider.read_block(scope, block.id) is None
    finally:
        await provider.close()

    provider = build_local_kernel(path)
    await provider.initialize()
    try:
        index = PgVectorIndex(pool, 8)
        sidecar = PgVectorBlockMemory(provider, index, Embeddings(), model="test")
        other = replace(scope, tenant_id="other")
        assert (await sidecar.forget_block(other, block.id, mode=mode)).affected_artifacts == 0
        assert (await index.search(scope, (1.0,) * 8, limit=8))[0].memory_id == block.id
        assert (await sidecar.forget_block(scope, block.id, mode=mode)).affected_artifacts == 0
        assert await index.search(scope, (1.0,) * 8, limit=8) == ()
        assert (await sidecar.forget_block(scope, block.id, mode=mode)).affected_artifacts == 0
    finally:
        await provider.close()


def test_live_pgvector_mutations_preserve_other_scopes(tmp_path):
    async def scenario():
        dsn = os.getenv("AGENT_MEMORY_TEST_POSTGRES_DSN", "").strip()
        if not dsn:
            pytest.skip("AGENT_MEMORY_TEST_POSTGRES_DSN is not configured")
        parsed = urlsplit(dsn)
        if (
            parsed.scheme not in {"postgres", "postgresql"}
            or "test" not in unquote(parsed.path).casefold()
        ):
            pytest.fail("AGENT_MEMORY_TEST_POSTGRES_DSN must name a disposable test database")
        schema = f"vector_{uuid4().hex}"
        async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
            cursor = await connection.execute(
                "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'"
            )
            if await cursor.fetchone() is None:
                pytest.skip("the disposable PostgreSQL server does not provide pgvector")
            await connection.execute("CREATE EXTENSION IF NOT EXISTS vector")
            await connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            try:
                async with AsyncConnectionPool(
                    dsn,
                    kwargs={"row_factory": dict_row, "options": f"-csearch_path={schema},public"},
                    min_size=1,
                    max_size=2,
                    open=False,
                ) as pool:
                    index = PgVectorIndex(pool, 8)
                    await index.initialize()
                    scope = MemoryScope(
                        "tenant",
                        user_id="user",
                        agent_id="agent",
                        workspace_id="workspace",
                        session_id="session",
                    )
                    first = (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                    changed = (0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                    await index.upsert("block", scope, first, model="original")
                    others = [
                        replace(scope, **{field: "other"})
                        for field in (
                            "tenant_id",
                            "namespace",
                            "user_id",
                            "agent_id",
                            "workspace_id",
                            "session_id",
                        )
                    ]
                    others.extend((replace(scope, session_id=None), MemoryScope("tenant")))
                    for other in others:
                        await index.delete("block", scope=other)
                        with pytest.raises(ValueError, match="outside the authorized scope"):
                            await index.upsert("block", other, changed, model="unauthorized")
                        hits = await index.search(scope, first, limit=8)
                        assert len(hits) == 1 and hits[0].memory_id == "block"
                        assert hits[0].distance == pytest.approx(0.0)
                    await index.upsert("block", scope, changed, model="updated")
                    assert (await index.search(scope, changed, limit=8))[
                        0
                    ].distance == pytest.approx(0.0)
                    await index.delete("block", scope=scope)
                    assert await index.search(scope, changed, limit=8) == ()
                    for mode in ForgetMode:
                        await _assert_sidecar_retry(
                            pool,
                            tmp_path / f"primary-{mode}.db",
                            scope,
                            mode,
                        )
            finally:
                await connection.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    asyncio.run(scenario())
