import asyncio
from pathlib import Path

import pytest

from agent_memory import (
    LiveOntologyMemory, MemoryQuery, OntologyBackfillCheckpoint,
    SQLiteOntologyCheckpointSink, SQLiteOntologyStore, OntologyCheckpointConflict,
)
from test_ontology_p0 import setup, insert, SCHEMA, SCOPE, context


def live(source, registry, tmp_path):
    return LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(),
        work_directory=tmp_path / "work", batch_size=1, max_batches=1)


def test_restart_resumes_partial_shadow_and_keeps_index_identity(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        for i in (1, 2, 3):
            await insert(repo, i)
        async with live(source, registry, tmp_path) as first:
            assert not await first.refresh()
            index_id = first._pending["index_id"]
            assert (await first._pending["sink"].load()).processed_claims == 1
        async with live(source, registry, tmp_path) as second:
            assert not await second.refresh()
            assert second._pending["index_id"] == index_id
            assert (await second._pending["sink"].load()).processed_claims == 2
            assert await second.refresh()
        async with live(source, registry, tmp_path) as third:
            assert await third.refresh()
            assert third._active["index_id"] == index_id
            assert len((await third.retrieve(MemoryQuery(SCOPE, "Alice"), ())).relevant_memories) == 3
    asyncio.run(scenario())


def test_replaced_index_is_not_resumed(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        await insert(repo, 1)
        async with live(source, registry, tmp_path) as first:
            assert await first.refresh()
            path = first._active["path"]
        path.unlink()
        replacement = SQLiteOntologyStore(path)
        await replacement.initialize()
        async with live(source, registry, tmp_path) as second:
            with pytest.raises(ValueError, match="identity changed"):
                await second.refresh()
    asyncio.run(scenario())


def test_exclusive_owner_blocks_second_worker(tmp_path):
    async def scenario():
        _, source, registry = await setup(tmp_path)
        async with live(source, registry, tmp_path):
            with pytest.raises(BlockingIOError):
                async with live(source, registry, tmp_path):
                    pytest.fail("two workers must not own one workspace")
        async with live(source, registry, tmp_path) as reopened:
            assert await reopened.refresh()
    asyncio.run(scenario())


def test_checkpoints_cannot_be_reused_for_other_index(tmp_path):
    async def scenario():
        path = tmp_path / "checkpoint.db"
        sink = SQLiteOntologyCheckpointSink(path, scope=SCOPE, snapshot_id="s", schema_digest="d", target_index_id="index-a")
        await sink.initialize()
        await sink.save(OntologyBackfillCheckpoint(SCOPE, "s", "d", completed=True, target_index_id="index-a"))
        other = SQLiteOntologyCheckpointSink(path, scope=SCOPE, snapshot_id="s", schema_digest="d", target_index_id="index-b")
        with pytest.raises(OntologyCheckpointConflict, match="index instance"):
            await other.load()
    asyncio.run(scenario())


def test_new_source_revision_reclaims_old_generation(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        await insert(repo, 1)
        async with live(source, registry, tmp_path) as runtime:
            assert await runtime.refresh()
            old = Path(runtime._active["temporary"].name)
            await insert(repo, 2)
            while not await runtime.refresh():
                pass
            assert not old.exists()
            assert len(runtime._workspace.entries) == 1
    asyncio.run(scenario())
