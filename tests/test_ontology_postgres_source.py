import asyncio
from dataclasses import replace
from contextlib import asynccontextmanager
import os
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from agent_memory import (
    ForgetMode, ForgetRequest, LiveOntologyMemory, MemoryEvent, MemoryQuery,
    MemoryScope, SQLiteOntologyRegistry, SQLiteOntologySnapshot,
)
from test_ontology_p0 import Approval, SCHEMA, SCOPE, context, insert


@asynccontextmanager
async def configured(tmp_path):
    pg = pytest.importorskip("agent_memory_postgres")
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql
    from psycopg.conninfo import make_conninfo

    dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN", "")
    if not dsn:
        pytest.skip("AGENT_MEMORY_TEST_POSTGRES_DSN is not configured")
    if "test" not in urlsplit(dsn).path.casefold():
        pytest.fail("a dedicated test database URI is required")
    namespace = "source_test_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace)))
    repo = pg.PostgresMemoryRepository.from_dsn(
        make_conninfo(dsn, options="-csearch_path=" + namespace))
    try:
        await repo.initialize()
        source = pg.PostgresOntologySource(dsn, namespace=namespace)
        await source.initialize()
        registry = SQLiteOntologyRegistry(tmp_path / "registry.db")
        await registry.initialize()
        await registry.register(SCOPE, SCHEMA)
        await registry.activate(SCOPE, SCHEMA.ontology_id, SCHEMA.version,
            expected_generation=0, reason="integration test", authorizer=Approval())
        yield repo, source, registry
    finally:
        await repo.close()
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace)))


def runtime(source, registry, tmp_path, **kwargs):
    return LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(),
        work_directory=tmp_path / "work", **kwargs)


def test_postgres_source_revision_rollback_identity_and_truncate(tmp_path):
    async def scenario():
        async with configured(tmp_path) as (repo, source, _):
            identity = await source.identity()
            before = await source.revision()
            with pytest.raises(RuntimeError, match="rollback"):
                async with repo.unit_of_work() as uow:
                    await uow.append_event(MemoryEvent(scope=SCOPE, event_type="test", content="discard"))
                    raise RuntimeError("rollback")
            assert await source.revision() == before
            await insert(repo, 1)
            assert await source.revision() > before
            await source.initialize()
            assert await source.identity() == identity
            before = await source.revision()
            async with repo.pool.connection() as connection:
                await connection.execute("TRUNCATE agent_memory_events CASCADE")
            assert await source.revision() > before
    asyncio.run(scenario())


def test_postgres_source_snapshot_pages_and_evidence_isolation(tmp_path):
    async def scenario():
        async with configured(tmp_path) as (repo, source, _):
            empty = await source.snapshot(SCOPE, tmp_path / "empty.db")
            assert not (await empty.read_page(SCOPE, empty.snapshot_id, cursor=None, limit=1)).claims
            originals = [await insert(repo, number) for number in range(1, 131)]
            foreign = MemoryEvent(scope=MemoryScope("foreign"), event_type="test", content="private")
            async with repo.unit_of_work() as uow:
                await uow.append_event(foreign)
            snapshot = await source.snapshot(SCOPE, tmp_path / "snapshot.db")
            await insert(repo, 131)
            await repo.forget(ForgetRequest(SCOPE,
                memory_ids=originals[0].provenance.source_event_ids, mode=ForgetMode.ERASE))
            snapshot = await SQLiteOntologySnapshot.open(snapshot.path, SCOPE)
            page = await snapshot.read_page(SCOPE, snapshot.snapshot_id, cursor=None, limit=128)
            assert len(page.claims) == 128
            tail = await snapshot.read_page(SCOPE, snapshot.snapshot_id, cursor=page.next_cursor, limit=128)
            assert len(tail.claims) == 2 and tail.next_cursor is None
            assert {claim.id: claim for claim in (*page.claims, *tail.claims)} == {claim.id: claim for claim in originals}
            assert await snapshot.verify(SCOPE, originals[0].provenance.source_event_ids)
            assert not await snapshot.verify(SCOPE, (foreign.id,))
            assert await source.revision() > snapshot.revision
    asyncio.run(scenario())


def test_postgres_source_restart_resumes_checkpoint(tmp_path):
    async def scenario():
        async with configured(tmp_path) as (repo, source, registry):
            for number in (1, 2, 3):
                await insert(repo, number)
            async with runtime(source, registry, tmp_path, batch_size=1, max_batches=1) as first:
                assert not await first.refresh()
                index_id = first._pending["index_id"]
            async with runtime(source, registry, tmp_path, batch_size=1, max_batches=1) as second:
                assert not await second.refresh()
                assert second._pending["index_id"] == index_id
                assert await second.refresh()
                assert len((await second.retrieve(MemoryQuery(SCOPE, "Alice"), ())).relevant_memories) == 3
                assert second.last_acceptance.ready
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ERASE, ForgetMode.ARCHIVE])
def test_postgres_source_live_replace_and_forget(tmp_path, mode):
    async def scenario():
        async with configured(tmp_path) as (repo, source, registry):
            async with runtime(source, registry, tmp_path) as live:
                query = MemoryQuery(SCOPE, "Alice")
                assert not (await live.retrieve(query, ())).relevant_memories
                first = await insert(repo, 1, subject="person:alice")
                assert [item.text for item in (await live.retrieve(query, ())).relevant_memories] == ["Alice 1"]
                second = await insert(repo, 2, previous=first, subject="person:alice")
                assert [item.text for item in (await live.retrieve(query, ())).relevant_memories] == ["Alice 2"]
                await repo.forget(ForgetRequest(SCOPE, memory_ids=second.provenance.source_event_ids, mode=mode))
                assert not (await live.retrieve(query, ())).relevant_memories
                assert live.last_acceptance.ready
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ERASE, ForgetMode.ARCHIVE])
def test_postgres_forget_preserves_remaining_claim_evidence(tmp_path, mode):
    async def scenario():
        async with configured(tmp_path) as (repo, source, registry):
            original = await insert(repo, 1)
            extra = MemoryEvent(scope=SCOPE, event_type="test", content="independent evidence")
            async with repo.unit_of_work() as uow:
                await uow.append_event(extra)
                await uow.save_claim(replace(original, id="multi-source", key="multi-source",
                    provenance=replace(original.provenance,
                        source_event_ids=(*original.provenance.source_event_ids, extra.id))))
            result = await repo.forget(ForgetRequest(SCOPE,
                memory_ids=original.provenance.source_event_ids, mode=mode))
            assert result.affected_claims == 2
            snapshot = await source.snapshot(SCOPE, tmp_path / "remaining.db")
            page = await snapshot.read_page(SCOPE, snapshot.snapshot_id, cursor=None, limit=128)
            assert len(page.claims) == 1
            assert page.claims[0].id == "multi-source"
            assert page.claims[0].provenance.source_event_ids == (extra.id,)
            async with runtime(source, registry, tmp_path) as live:
                assert await live.refresh()
                assert live.last_acceptance.ready
            repeated = await repo.forget(ForgetRequest(SCOPE,
                memory_ids=original.provenance.source_event_ids, mode=mode))
            assert repeated.affected_claims == 0
    asyncio.run(scenario())
