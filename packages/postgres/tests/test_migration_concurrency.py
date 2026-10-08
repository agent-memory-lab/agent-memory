"""Independent startup transactions must serialize before acquiring DDL locks."""

import asyncio
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

from agent_memory_postgres import PostgresMemoryRepository
from psycopg import AsyncConnection, sql
from test_live_contract import _test_dsn


def test_concurrent_initializers_wait_before_first_schema_ddl(monkeypatch):
    async def run():
        dsn = _test_dsn()
        schema = "concurrent_migration_" + uuid4().hex
        async with await AsyncConnection.connect(dsn, autocommit=True) as admin:
            await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            parsed = urlsplit(dsn)
            query = dict(parse_qsl(parsed.query))
            query["options"] = "-csearch_path=" + schema
            target = urlunsplit(parsed._replace(query=urlencode(query)))
            one = PostgresMemoryRepository.from_dsn(target, max_size=1)
            two = PostgresMemoryRepository.from_dsn(target, max_size=1)
            first_ddl, second_ddl, second_lock, release = (asyncio.Event() for _ in range(4))
            original = AsyncConnection.execute
            ddl_calls, lock_calls = [], []

            async def execute(connection, statement, params=None, **kwargs):
                if params == ("agent-memory:migration:",):
                    lock_calls.append(connection.info.backend_pid)
                    if len(lock_calls) == 2:
                        second_lock.set()
                if (isinstance(statement, str)
                        and "CREATE TABLE IF NOT EXISTS agent_memory_schema" in statement):
                    ddl_calls.append(connection.info.backend_pid)
                    if len(ddl_calls) == 1:
                        first_ddl.set()
                        await release.wait()
                    else:
                        second_ddl.set()
                return await original(connection, statement, params, **kwargs)

            monkeypatch.setattr(AsyncConnection, "execute", execute)
            tasks, events = [], []
            try:
                tasks.append(asyncio.create_task(one.initialize()))
                await asyncio.wait_for(first_ddl.wait(), 5)
                tasks.append(asyncio.create_task(two.initialize()))
                events = [asyncio.create_task(e.wait()) for e in (second_lock, second_ddl)]
                done, _ = await asyncio.wait(events, timeout=5, return_when=asyncio.FIRST_COMPLETED)
                assert events[0] in done and not second_ddl.is_set()
                assert len(set(lock_calls)) == 2 and len(ddl_calls) == 1
                release.set()
                await asyncio.wait_for(asyncio.gather(*tasks), 15)
                assert second_ddl.is_set()
                async with two.pool.connection() as connection:
                    cursor = await connection.execute(
                        "SELECT to_regclass('agent_memory_schema') AS name"
                    )
                    assert (await cursor.fetchone())["name"] is not None
            finally:
                release.set()
                for task in [*tasks, *events]:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, *events, return_exceptions=True)
                await asyncio.gather(one.close(), two.close())
                await admin.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    asyncio.run(run())


def test_optional_schema_writers_take_shared_lock_before_ddl(monkeypatch):
    from contextlib import asynccontextmanager, contextmanager

    from agent_memory_postgres.migration import SCHEMA_LOCK_PARAMS, SCHEMA_LOCK_SQL
    from agent_memory_postgres.ontology_source import PostgresOntologySource
    from agent_memory_postgres.vector import PgVectorIndex

    calls = []

    class Connection:
        async def execute(self, statement, params=None):
            calls.append((statement, params))

        @asynccontextmanager
        async def transaction(self):
            yield self

    class Pool:
        @asynccontextmanager
        async def connection(self):
            yield Connection()

    asyncio.run(PgVectorIndex(Pool(), 8).initialize())
    assert calls[0] == (SCHEMA_LOCK_SQL, SCHEMA_LOCK_PARAMS)
    assert "CREATE TABLE" in calls[1][0]
    calls.clear()

    class SyncConnection:
        def execute(self, statement, params=None):
            calls.append((statement, params))

    @contextmanager
    def connection():
        yield SyncConnection()

    source = PostgresOntologySource("postgresql://localhost/unused_test")
    monkeypatch.setattr(source, "_connection", connection)
    source._initialize()
    assert calls[0] == (SCHEMA_LOCK_SQL, SCHEMA_LOCK_PARAMS)
    assert any("CREATE TABLE" in str(statement) for statement, _ in calls[1:])
