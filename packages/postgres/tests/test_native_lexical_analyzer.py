"""PostgreSQL's indexed native FTS remains a documented analyzer boundary."""

import asyncio
import os
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from agent_memory_postgres.repository import PostgresMemoryRepository

from agent_memory.retrieval.analyzer import lexical_terms


@pytest.mark.parametrize("text", ["上海", "上海 API", "A B 7", "request_id-v2"])
def test_nonempty_postgres_queries_preserve_native_indexed_fts(text):
    class Connection:
        calls = []

        async def execute(self, sql, parameters):
            self.calls.append((" ".join(sql.split()), parameters))
            return self

        async def fetchall(self):
            return [{"id": "match", "rank": 0.25}]

    async def scenario():
        connection = Connection()
        rows = await PostgresMemoryRepository._search_rows(
            connection, "agent_memory_events",
            "tenant_id = %s AND archived_at IS NULL", ("tenant",), text, 8,
        )
        assert rows == [{"id": "match", "rank": 0.25}]
        [(sql, parameters)] = connection.calls
        assert "ts_rank(search_document, plainto_tsquery('simple', %s)) AS rank" in sql
        assert "WHERE tenant_id = %s AND archived_at IS NULL" in sql
        assert "AND search_document @@ plainto_tsquery('simple', %s)" in sql
        assert "ORDER BY rank DESC LIMIT %s" in sql
        assert parameters == (text, "tenant", text, 8)

    asyncio.run(scenario())


def test_native_fts_documents_keep_their_gin_indexes():
    migration = Path(__file__).parents[1] / "migrations" / "001_core.sql"
    sql = migration.read_text(encoding="utf-8")
    for name in ("events", "claims", "artifacts"):
        assert f"ON agent_memory_{name} USING gin(search_document)" in sql
    assert sql.count("search_document tsvector GENERATED ALWAYS AS") == 3


def test_live_native_han_analysis_is_not_claimed_as_shared_term_parity():
    dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN", "")
    if not dsn:
        pytest.skip("native PostgreSQL analyzer check requires a disposable test DSN")
    parsed = urlsplit(dsn)
    if parsed.scheme not in {"postgres", "postgresql"} or "test" not in parsed.path.casefold():
        pytest.fail("native analyzer check requires a PostgreSQL database named test")

    async def scenario():
        from psycopg import AsyncConnection

        async with await AsyncConnection.connect(dsn) as connection:
            cursor = await connection.execute(
                "SELECT to_tsvector('simple', %s) @@ plainto_tsquery('simple', %s)",
                ("上海", "上"),
            )
            assert await cursor.fetchone() == (False,)
        assert set(lexical_terms("上海")) & set(lexical_terms("上")) == {"上"}

    asyncio.run(scenario())
