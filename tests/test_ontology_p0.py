import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from agent_memory import (
    AgentMemory, Claim, ClaimStatus, ConsolidationRequest, ForgetMode, ForgetRequest,
    MemoryEvent, MemoryQuery, MemoryScope, OntologyClass, OntologyProperty,
    OntologySchema, PluginContext, PluginResourceLimits, Provenance,
    SQLiteOntologyRegistry, SQLiteOntologyStore, SQLiteOntologySource,
    SQLiteOntologySnapshot, LiveOntologyMemory, OntologySyncPending,
    OntologyProjectionConsolidatorPlugin, validate_ontology_index,
)
from agent_memory.sqlite import SQLiteMemoryRepository
from agent_memory.ontology_source import database


NOW = datetime(2026, 9, 23, tzinfo=UTC)
SCOPE = MemoryScope("p0", user_id="alice")
SCHEMA = OntologySchema(
    "p0.test", "1.0.0", (OntologyClass("person", "Person"),),
    (OntologyProperty("name", "Name", "person", functional=True),), created_at=NOW,
)


class Approval:
    async def authorize(self, request):
        return True


def context():
    return PluginContext(SCOPE, PluginResourceLimits(max_batch_size=16))


async def insert(repo, number, *, previous=None, subject=None, value=None):
    event = MemoryEvent(scope=SCOPE, event_type="test", content=f"Alice {number}")
    claim = Claim(
        id=f"claim-{number}", scope=SCOPE, key=previous.key if previous else f"person.{number}",
        value={"$ontology": {
            "subject": {"id": subject or f"person:p{number}", "class": "person", "label": "Person"},
            "predicate": "name", "object": {"literal": value or f"Alice {number}"},
        }}, text=f"Alice {number}", confidence=0.9, importance=0.5,
        status=ClaimStatus.ACTIVE, provenance=Provenance(source_event_ids=(event.id,)),
        valid_from=NOW + timedelta(seconds=number), created_at=NOW,
    )
    async with repo.unit_of_work() as uow:
        await uow.append_event(event)
        if previous:
            await uow.replace_current_claim(previous, claim)
        else:
            await uow.save_claim(claim)
    return claim


async def setup(tmp_path):
    path = tmp_path / "core.db"
    repo = SQLiteMemoryRepository(path)
    await repo.initialize()
    source = SQLiteOntologySource(path)
    await source.initialize()
    registry = SQLiteOntologyRegistry(tmp_path / "registry.db")
    await registry.initialize()
    await registry.register(SCOPE, SCHEMA)
    await registry.activate(SCOPE, SCHEMA.ontology_id, SCHEMA.version,
        expected_generation=0, reason="host permits managed shadow preparation", authorizer=Approval())
    return repo, source, registry


def test_real_snapshot_is_stable_paged_and_reopenable(tmp_path):
    async def scenario():
        repo, source, _ = await setup(tmp_path)
        first = await insert(repo, 1)
        second = await insert(repo, 2)
        snapshot = await source.snapshot(SCOPE, tmp_path / "snapshot.db")
        await insert(repo, 3)
        await repo.forget(ForgetRequest(SCOPE, memory_ids=first.provenance.source_event_ids, mode=ForgetMode.ERASE))
        reopened = await SQLiteOntologySnapshot.open(snapshot.path, SCOPE)
        page = await reopened.read_page(SCOPE, snapshot.snapshot_id, cursor=None, limit=1)
        assert page.claims == (first,)
        next_page = await reopened.read_page(SCOPE, snapshot.snapshot_id, cursor=page.next_cursor, limit=1)
        assert next_page.claims == (second,)
        assert next_page.next_cursor is None
        assert await reopened.verify(SCOPE, first.provenance.source_event_ids)
        assert await source.revision() > snapshot.revision
        with pytest.raises(ValueError, match="scope"):
            await SQLiteOntologySnapshot.open(snapshot.path, MemoryScope("foreign"))
    asyncio.run(scenario())


def test_change_revision_is_transactional(tmp_path):
    async def scenario():
        repo, source, _ = await setup(tmp_path)
        before = await source.revision()
        with pytest.raises(RuntimeError):
            async with repo.unit_of_work() as uow:
                await uow.append_event(MemoryEvent(scope=SCOPE, event_type="test", content="rolled back"))
                raise RuntimeError("rollback")
        assert await source.revision() == before
        await insert(repo, 1)
        assert await source.revision() > before
    asyncio.run(scenario())


def test_recall_automatically_observes_insert_replace_and_erase(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        async with LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(), work_directory=tmp_path / "work") as live:
            async with AgentMemory.local(source.path, scope=SCOPE, recall_pipeline=live) as memory:
                assert (await memory.recall("Alice")).relevant_memories == ()
                first = await insert(repo, 1, subject="person:alice")
                assert [v.text for v in (await memory.recall("Alice")).relevant_memories] == ["Alice 1"]
                second = await insert(repo, 2, previous=first, subject="person:alice")
                assert [v.text for v in (await memory.recall("Alice")).relevant_memories] == ["Alice 2"]
                await memory.forget(memory_ids=second.provenance.source_event_ids, erase=True)
                assert (await memory.recall("Alice")).relevant_memories == ()
                assert live.last_acceptance.ready
        assert list((tmp_path / "work").iterdir()) == []
    asyncio.run(scenario())


def test_recall_adopts_new_schema_and_rollback_without_reopening(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        await insert(repo, 1)
        async with LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(), work_directory=tmp_path / "work") as live:
            query = MemoryQuery(SCOPE, "Alice")
            first = await live.retrieve(query, ())
            next_schema = replace(SCHEMA, version="1.0.1")
            await registry.register(SCOPE, next_schema)
            await registry.activate(SCOPE, SCHEMA.ontology_id, "1.0.1", expected_generation=1, reason="ready", authorizer=Approval())
            second = await live.retrieve(query, ())
            assert first.relevant_memories[0].id != second.relevant_memories[0].id
            await registry.rollback(SCOPE, SCHEMA.ontology_id, "1.0.0", expected_generation=2, reason="rollback", authorizer=Approval())
            third = await live.retrieve(query, ())
            assert third.relevant_memories[0].id == first.relevant_memories[0].id
    asyncio.run(scenario())


def test_large_job_is_bounded_and_never_serves_stale_data(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        for number in range(1, 4):
            await insert(repo, number)
        async with LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(),
            work_directory=tmp_path / "work", batch_size=1, max_batches=1) as live:
            with pytest.raises(OntologySyncPending):
                await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())
            assert not await live.refresh()
            assert await live.refresh()
            assert len((await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())).relevant_memories) == 3
            await insert(repo, 4)
            with pytest.raises(OntologySyncPending):
                await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())
    asyncio.run(scenario())


def test_conflicts_block_managed_index_publication(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        await insert(repo, 1, subject="person:alice", value="one")
        await insert(repo, 2, subject="person:alice", value="two")
        async with LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(), work_directory=tmp_path / "work") as live:
            with pytest.raises(ValueError, match="acceptance failed"):
                await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())
            assert not live.last_acceptance.ready
    asyncio.run(scenario())


def test_archive_is_detected_without_manual_consolidation(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        claim = await insert(repo, 1)
        async with LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(), work_directory=tmp_path / "work") as live:
            assert (await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())).relevant_memories
            await repo.forget(ForgetRequest(SCOPE, memory_ids=claim.provenance.source_event_ids, mode=ForgetMode.ARCHIVE))
            assert (await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())).relevant_memories == ()
    asyncio.run(scenario())


def test_source_change_during_recall_discards_bundle(tmp_path, monkeypatch):
    from agent_memory.governed_recall import GovernedRecallPipeline
    original = GovernedRecallPipeline.retrieve

    async def scenario():
        repo, source, registry = await setup(tmp_path)
        claim = await insert(repo, 1)
        modified = False

        async def racing_retrieve(pipeline, query, current_state):
            nonlocal modified
            result = await original(pipeline, query, current_state)
            if not modified:
                modified = True
                await repo.forget(ForgetRequest(SCOPE, memory_ids=claim.provenance.source_event_ids, mode=ForgetMode.ERASE))
            return result

        monkeypatch.setattr(GovernedRecallPipeline, "retrieve", racing_retrieve)
        async with LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(), work_directory=tmp_path / "work") as live:
            with pytest.raises(OntologySyncPending, match="changed during recall"):
                await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())
            assert (await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())).relevant_memories == ()
    asyncio.run(scenario())


def test_failed_new_schema_never_falls_back_to_old_index(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        await insert(repo, 1)
        async with LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(), work_directory=tmp_path / "work") as live:
            assert (await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())).relevant_memories
            incompatible = replace(SCHEMA, version="2.0.0", properties=(OntologyProperty("other", "Other", "person"),))
            await registry.register(SCOPE, incompatible)
            await registry.activate(SCOPE, SCHEMA.ontology_id, "2.0.0", expected_generation=1, reason="host test", authorizer=Approval())
            with pytest.raises(ValueError):
                await live.retrieve(MemoryQuery(SCOPE, "Alice"), ())
        assert list((tmp_path / "work").iterdir()) == []
    asyncio.run(scenario())


def test_cancelled_refresh_settles_before_cleanup(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        await insert(repo, 1)
        entered, release = asyncio.Event(), asyncio.Event()

        class PausedSource(SQLiteOntologySource):
            async def snapshot(self, scope, destination):
                entered.set()
                await release.wait()
                return await super().snapshot(scope, destination)

        async with LiveOntologyMemory(PausedSource(source.path), registry, SCHEMA.ontology_id, context(), work_directory=tmp_path / "work") as live:
            task = asyncio.create_task(live.refresh())
            await entered.wait()
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert await live.refresh()
        assert list((tmp_path / "work").iterdir()) == []
    asyncio.run(scenario())


@pytest.mark.parametrize("damage", ["missing", "text", "evidence", "entity"])
def test_acceptance_detects_damaged_index(tmp_path, damage):
    async def scenario():
        repo, source, _ = await setup(tmp_path)
        claim = await insert(repo, 1)
        snapshot = await source.snapshot(SCOPE, tmp_path / "snapshot.db")
        index_path = tmp_path / "index.db"
        store = SQLiteOntologyStore(index_path)
        plugin = OntologyProjectionConsolidatorPlugin(store, SCHEMA, snapshot)
        ctx = context()
        await plugin.initialize(ctx)
        await plugin.consolidate(ConsolidationRequest(SCOPE, claims=(claim,)), ctx)
        await plugin.close()
        assert (await validate_ontology_index(snapshot, SCHEMA, index_path)).ready
        with database(index_path) as db:
            if damage == "missing":
                db.execute("DELETE FROM ontology_assertions")
            elif damage == "text":
                db.execute("UPDATE ontology_assertions SET text='tampered'")
            elif damage == "evidence":
                db.execute("UPDATE ontology_assertions SET source_event_ids_json='[]'")
            else:
                db.execute("DELETE FROM ontology_entities")
        assert not (await validate_ontology_index(snapshot, SCHEMA, index_path)).ready
    asyncio.run(scenario())
