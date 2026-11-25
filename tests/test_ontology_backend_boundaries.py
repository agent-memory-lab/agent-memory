"""Backend independence and real concurrent read/write lock contracts."""

import asyncio
import os
from urllib.parse import urlsplit
from uuid import uuid4

import pytest


def test_postgres_ontology_uses_shared_operations_not_sqlite_adapters():
    pg = pytest.importorskip("agent_memory_postgres")
    from agent_memory.ontology.registry import SQLiteOntologyRegistry
    from agent_memory.ontology.store import SQLiteOntologyStore

    assert SQLiteOntologyStore not in pg.PostgresOntologyStore.__mro__
    assert SQLiteOntologyRegistry not in pg.PostgresOntologyRegistry.__mro__


def test_postgres_reads_can_overlap_but_exclude_mutations():
    dsn = os.environ.get("AGENT_MEMORY_ONTOLOGY_TEST_DSN")
    if not dsn:
        pytest.skip("requires isolated PostgreSQL")
    assert "test" in urlsplit(dsn).path.casefold()
    pg = pytest.importorskip("agent_memory_postgres")
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql

    namespace = "readers_" + uuid4().hex
    store = pg.PostgresOntologyStore(dsn, namespace=namespace, timeout_ms=300)
    try:
        asyncio.run(store.initialize())
        first = store._connect()
        try:
            second = store._connect()
            try:
                assert first.execute("SELECT index_id FROM ontology_index_identity").fetchone()
                assert second.execute("SELECT index_id FROM ontology_index_identity").fetchone()
                with psycopg.connect(dsn) as observer:
                    assert (
                        observer.execute(
                            "SELECT pg_try_advisory_xact_lock(hashtext(%s))", (namespace,)
                        ).fetchone()[0]
                        is False
                    )
            finally:
                second.close()
        finally:
            first.close()
        writer = store._connect_write()
        try:
            with psycopg.connect(dsn) as observer:
                assert (
                    observer.execute(
                        "SELECT pg_try_advisory_xact_lock_shared(hashtext(%s))", (namespace,)
                    ).fetchone()[0]
                    is False
                )
        finally:
            writer.close()
        with psycopg.connect(dsn) as observer:
            assert (
                observer.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtext(%s))", (namespace,)
                ).fetchone()[0]
                is True
            )
    finally:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(namespace))
            )
