import asyncio
from uuid import uuid4

import pytest

from agent_memory import ForgetMode, ForgetRequest, MemoryQuery
from agent_memory_postgres import PostgresHostedOntologyMemory, PostgresOntologyRegistry
from test_ontology_p0 import Approval, SCHEMA, SCOPE, context, insert
from test_ontology_postgres_source import configured


async def catalog_for(source):
    catalog = PostgresOntologyRegistry(source._dsn, namespace="catalog_test_" + uuid4().hex)
    await catalog.initialize()
    await catalog.register(SCOPE, SCHEMA)
    await catalog.activate(SCOPE, SCHEMA.ontology_id, SCHEMA.version,
        expected_generation=0, reason="integration test", authorizer=Approval())
    return catalog


def runtime(source, catalog, namespace, **kwargs):
    return PostgresHostedOntologyMemory(source, catalog, SCHEMA.ontology_id,
        context(), namespace=namespace, **kwargs)


def test_hosted_catalog_roundtrip_and_restart_resume(tmp_path):
    async def scenario():
        async with configured(tmp_path) as (repo, source, _):
            catalog = await catalog_for(source)
            assert await catalog.get(SCOPE, SCHEMA.ontology_id, SCHEMA.version) == SCHEMA
            for number in (1, 2, 3):
                await insert(repo, number)
            namespace = "hosted_test_" + uuid4().hex
            async with runtime(source, catalog, namespace, batch_size=1, max_batches=1) as first:
                assert not await first.refresh()
                with first._db() as db:
                    before = db.execute("SELECT id,index_id,processed FROM hosted_generations WHERE job=%s", (first.job,)).fetchone()
                assert before["processed"] == 1
            async with runtime(source, catalog, namespace, batch_size=1, max_batches=1) as second:
                assert not await second.refresh()
                assert await second.refresh()
                assert second.last_acceptance.ready
                assert second._current["row"]["index_id"] == before["index_id"]
                assert len((await second.retrieve(MemoryQuery(SCOPE, "Alice"), ())).relevant_memories) == 3
    asyncio.run(scenario())


def test_hosted_worker_exclusion_and_closed_lease(tmp_path):
    async def scenario():
        async with configured(tmp_path) as (_, source, catalog):
            namespace = "hosted_test_" + uuid4().hex
            async with runtime(source, catalog, namespace) as first:
                with pytest.raises(BlockingIOError):
                    async with runtime(source, catalog, namespace):
                        pytest.fail("two workers acquired the same job")
                await asyncio.to_thread(first._lease.close)
                with pytest.raises(RuntimeError, match="lease"):
                    await first.refresh()
            async with runtime(source, catalog, namespace) as resumed:
                assert await resumed.refresh()
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ERASE, ForgetMode.ARCHIVE])
def test_hosted_rebuild_forgets_evidence(tmp_path, mode):
    async def scenario():
        async with configured(tmp_path) as (repo, source, catalog):
            claim = await insert(repo, 1)
            async with runtime(source, catalog, "hosted_test_" + uuid4().hex) as live:
                query = MemoryQuery(SCOPE, "Alice")
                assert len((await live.retrieve(query, ())).relevant_memories) == 1
                await repo.forget(ForgetRequest(SCOPE, memory_ids=claim.provenance.source_event_ids, mode=mode))
                assert not (await live.retrieve(query, ())).relevant_memories
                assert live.last_acceptance.ready
    asyncio.run(scenario())
