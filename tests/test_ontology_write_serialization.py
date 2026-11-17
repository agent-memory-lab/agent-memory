"""Real SQLite schedules for ontology projection and source withdrawal races."""

import asyncio
import json
import sqlite3
import threading
from contextlib import closing
from dataclasses import replace

import pytest
from test_ontology_memory import _claim, _schema

from agent_memory.domain import MemoryScope
from agent_memory.ontology.model import project_claim_to_ontology
from agent_memory.ontology.store import SQLiteOntologyStore

SCOPE = MemoryScope("ontology-writes", user_id="alice", session_id="one")
SCHEMA = _schema()


def projection(source, alias):
    claim = replace(
        _claim(SCOPE, source),
        id="claim-" + source,
        value={
            "$ontology": {
                "subject": {
                    "id": "person:alice",
                    "class": "person",
                    "label": "Alice",
                    "aliases": (alias,),
                },
                "predicate": "display_name",
                "object": {"literal": "Alice"},
            }
        },
    )
    return project_claim_to_ontology(claim, SCHEMA)


async def setup(path, separate_handles):
    first = SQLiteOntologyStore(path)
    await first.initialize()
    await first.register_schema(SCHEMA)
    return first, SQLiteOntologyStore(path) if separate_handles else first


def state(path):
    with closing(sqlite3.connect(path)) as connection:
        row = connection.execute(
            "SELECT aliases_json, source_event_ids_json, archived_at FROM ontology_entities"
        ).fetchone()
        links = sorted(
            r[0] for r in connection.execute("SELECT event_id FROM ontology_entity_sources")
        )
        assertions = []
        for assertion_id, sources, archived_at in connection.execute(
            "SELECT assertion_id, source_event_ids_json, archived_at FROM ontology_assertions"
        ).fetchall():
            assertion_links = connection.execute(
                "SELECT event_id FROM ontology_assertion_sources WHERE assertion_id=?",
                (assertion_id,),
            ).fetchall()
            assertions.append(
                {
                    "sources": json.loads(sources),
                    "archived_at": archived_at,
                    "links": [value[0] for value in assertion_links],
                }
            )
    return {
        "aliases": json.loads(row[0]),
        "sources": json.loads(row[1]),
        "archived_at": row[2],
        "links": links,
        "assertions": assertions,
    }


def assert_consistent(result):
    assert set(result["sources"]) == set(result["links"]), result
    for assertion in result["assertions"]:
        assert set(assertion["sources"]) == set(assertion["links"]), result
        assert (assertion["archived_at"] is None) == bool(assertion["links"]), result
    assert (result["archived_at"] is None) == bool(result["links"]), result
    if not result["links"]:
        assert result["aliases"] == [], result


async def lookup(store, alias):
    return await store.find_entity_candidates(
        SCOPE,
        alias,
        ontology_id=SCHEMA.ontology_id,
        ontology_version=SCHEMA.version,
    )


class CursorProxy:
    def __init__(self, inner, *, after_one=None, after_all=None):
        self.inner, self.after_one, self.after_all = inner, after_one, after_all

    def fetchone(self):
        row = self.inner.fetchone()
        if self.after_one:
            self.after_one()
        return row

    def fetchall(self):
        rows = self.inner.fetchall()
        if self.after_all:
            self.after_all()
        return rows

    def __getattr__(self, name):
        return getattr(self.inner, name)


class ConnectionProxy:
    """Delay real SELECT results without replacing SQL or transaction behavior."""

    def __init__(self, inner, observe):
        self.inner, self.observe = inner, observe

    def __enter__(self):
        self.inner.__enter__()
        return self

    def __exit__(self, *args):
        return self.inner.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def execute(self, sql, *args, **kwargs):
        return self.observe(self.inner, sql, self.inner.execute(sql, *args, **kwargs))


@pytest.mark.parametrize("separate_handles", [False, True], ids=["shared", "independent"])
def test_concurrent_entity_upserts_preserve_every_successful_alias(
    tmp_path, monkeypatch, separate_handles
):
    async def run():
        path = tmp_path / "parallel.db"
        first, second = await setup(path, separate_handles)
        await first.upsert_projection(projection("s0", "Initial"))
        barrier = threading.Barrier(2)
        mutex = threading.Lock()
        scheduled = 0

        def observe(connection, sql, cursor):
            nonlocal scheduled
            if "SELECT class_id, label, aliases_json, source_event_ids_json" not in sql:
                return cursor
            # A writer that already owns its transaction must be allowed to
            # serialize. Otherwise force both initial reads before either write.
            if connection.in_transaction:
                return cursor
            with mutex:
                scheduled += 1
                first_pair = scheduled <= 2
            return CursorProxy(cursor, after_one=lambda: barrier.wait(10)) if first_pair else cursor

        for store in {first, second}:
            original = store._connect
            monkeypatch.setattr(
                store, "_connect", lambda connect=original: ConnectionProxy(connect(), observe)
            )
        results = await asyncio.gather(
            first.upsert_projection(projection("sA", "From A")),
            second.upsert_projection(projection("sB", "From B")),
            return_exceptions=True,
        )
        assert results == [None, None]
        result = state(path)
        assert set(result["aliases"]) == {"Initial", "From A", "From B"}, result
        assert set(result["sources"]) == {"s0", "sA", "sB"}, result
        assert_consistent(result)
        assert (await lookup(first, "From A")).status == "candidate"
        assert (await lookup(second, "From B")).status == "candidate"

    asyncio.run(run())


@pytest.mark.parametrize("separate_handles", [False, True], ids=["shared", "independent"])
def test_invalidation_and_projection_leave_a_serializable_retryable_state(
    tmp_path, monkeypatch, separate_handles
):
    async def run():
        path = tmp_path / "withdrawal.db"
        remover, writer = await setup(path, separate_handles)
        original = remover._connect
        selected, release = threading.Event(), threading.Event()

        def observe(connection, sql, cursor):
            if "SELECT DISTINCT ontology_id, ontology_version, entity_id" not in sql:
                return cursor

            def after_read():
                selected.set()
                # Accept a transaction that serializes withdrawal before the
                # write; do not force another writer to bypass a held write lock.
                if not connection.in_transaction:
                    assert release.wait(10), "controlled schedule timed out"

            return CursorProxy(cursor, after_all=after_read)

        monkeypatch.setattr(remover, "_connect", lambda: ConnectionProxy(original(), observe))
        deletion = asyncio.create_task(remover.invalidate_sources(SCOPE, ("s1",)))
        try:
            assert await asyncio.to_thread(selected.wait, 10)
            await writer.upsert_projection(projection("s1", "Withdrawn alias"))
        finally:
            release.set()
            await deletion
            monkeypatch.setattr(remover, "_connect", original)
        # Both serial orders are legal; dangling source JSON with no relation
        # rows is not. A subsequent withdrawal must remove any surviving write.
        before_retry = state(path)
        await remover.invalidate_sources(SCOPE, ("s1",))
        after_retry = state(path)
        assert_consistent(before_retry)
        assert_consistent(after_retry)
        assert (await lookup(writer, "Withdrawn alias")).status == "unknown"
        assert await remover.invalidate_sources(SCOPE, ("s1",)) == 0

    asyncio.run(run())
