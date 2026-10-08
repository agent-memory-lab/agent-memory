import asyncio
from dataclasses import dataclass, replace

import pytest
from agent_memory_postgres.semantic import PgVectorBlockMemory
from agent_memory_postgres.vector import VectorHit

from agent_memory.composition import build_local_kernel
from agent_memory.domain import ForgetMode, ForgetResult, MemoryBlock, MemoryEvent, MemoryScope


@dataclass(frozen=True)
class Block:
    id: str
    scope: MemoryScope
    title: str
    content: str


class FakeEmbeddings:
    dimensions = 2

    async def embed(self, texts):
        return tuple((1.0, 0.0) for _ in texts)


class FakeIndex:
    dimensions = 2

    def __init__(self):
        self.upserts = []
        self.deleted = []
        self.delete_attempts = []
        self.rows = {}

    async def initialize(self, *, install_extension=False):
        return None

    async def upsert(self, memory_id, scope, embedding, *, model):
        self.upserts.append((memory_id, scope, tuple(embedding), model))
        self.rows[memory_id] = (scope, tuple(embedding), model)

    async def search(self, scope, embedding, *, limit):
        assert tuple(embedding) == (1.0, 0.0)
        assert limit == 6
        return (VectorHit("missing", 0.01), VectorHit("block-1", 0.02))

    async def delete(self, memory_id, *, scope):
        self.delete_attempts.append((memory_id, scope))
        row = self.rows.get(memory_id)
        if row is not None and row[0] == scope:
            del self.rows[memory_id]
            self.deleted.append((memory_id, scope))


class FakeProvider:
    def __init__(self, block):
        self.block = block
        self.writes = []
        self.forgets = []

    async def write_block(self, block, *, expected_version=0):
        self.writes.append((block, expected_version))
        return block

    async def read_block(self, scope, block_id):
        if scope == self.block.scope and block_id == self.block.id:
            return self.block
        return None

    async def forget_block(self, scope, block_id, *, mode):
        self.forgets.append((scope, block_id, mode))
        return ForgetResult(0, 0, 1, mode)


def test_pgvector_block_sidecar_indexes_and_rechecks_provider_visibility():
    async def scenario():
        scope = MemoryScope(tenant_id="test", session_id="one")
        block = Block("block-1", scope, "Deployment", "Use blue-green releases.")
        provider = FakeProvider(block)
        index = FakeIndex()
        sidecar = PgVectorBlockMemory(provider, index, FakeEmbeddings(), model="test-v1")

        saved = await sidecar.write_block(block)
        assert saved == block
        assert index.upserts == [("block-1", scope, (1.0, 0.0), "test-v1")]

        assert await sidecar.search(scope, "release plan", limit=2) == (block,)
        assert await sidecar.forget_block(scope, block.id) == ForgetResult(
            0, 0, 1, ForgetMode.ARCHIVE
        )
        assert index.deleted == [("block-1", scope)]

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", list(ForgetMode))
def test_pgvector_sidecar_only_deletes_vectors_after_authorized_primary_forget(tmp_path, mode):
    async def scenario():
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        try:
            scope = MemoryScope(
                tenant_id="owner",
                user_id="user",
                agent_id="agent",
                workspace_id="workspace",
                session_id="session",
            )
            event = MemoryEvent(scope=scope, event_type="user.message", content="Keep my note.")
            await provider.ingest_event(event)
            index = FakeIndex()
            sidecar = PgVectorBlockMemory(provider, index, FakeEmbeddings(), model="test-v1")
            block = await sidecar.write_block(
                MemoryBlock(
                    scope=scope,
                    title="Note",
                    content=event.content,
                    event_ids=(event.id,),
                )
            )
            for field in (
                "tenant_id",
                "namespace",
                "user_id",
                "agent_id",
                "workspace_id",
                "session_id",
            ):
                other = replace(scope, **{field: "other"})
                result = await sidecar.forget_block(other, block.id, mode=mode)
                assert result.affected_artifacts == 0
                assert await provider.read_block(scope, block.id) is not None
                assert index.deleted == []
                assert index.rows[block.id][0] == scope
            assert (await sidecar.forget_block(scope, "missing", mode=mode)).affected_artifacts == 0
            assert index.deleted == []
            assert (await sidecar.forget_block(scope, block.id, mode=mode)).affected_artifacts == 1
            assert index.deleted == [(block.id, scope)]
            assert await provider.read_block(scope, block.id) is None
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_pgvector_sidecar_does_not_delete_on_primary_failure():
    class FailingProvider:
        async def forget_block(self, scope, block_id, *, mode):
            raise RuntimeError("primary unavailable")

    async def scenario():
        index = FakeIndex()
        sidecar = PgVectorBlockMemory(FailingProvider(), index, FakeEmbeddings(), model="test-v1")
        with pytest.raises(RuntimeError, match="primary unavailable"):
            await sidecar.forget_block(MemoryScope("tenant"), "block")
        assert index.deleted == []
        assert index.delete_attempts == []

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", list(ForgetMode))
def test_pgvector_sidecar_retries_interrupted_cleanup_after_restart(tmp_path, mode):
    class InitiallyFailingIndex(FakeIndex):
        fail_next = True

        async def delete(self, memory_id, *, scope):
            if self.fail_next:
                self.fail_next = False
                raise RuntimeError("vector storage temporarily unavailable")
            await super().delete(memory_id, scope=scope)

    async def scenario():
        path = tmp_path / "memory.db"
        scope = MemoryScope("tenant", user_id="owner", session_id="session")
        index = InitiallyFailingIndex()
        provider = build_local_kernel(path)
        await provider.initialize()
        try:
            event = MemoryEvent(scope=scope, event_type="user.message", content="Keep my note.")
            await provider.ingest_event(event)
            sidecar = PgVectorBlockMemory(provider, index, FakeEmbeddings(), model="test-v1")
            block = await sidecar.write_block(
                MemoryBlock(
                    scope=scope,
                    title="Note",
                    content=event.content,
                    event_ids=(event.id,),
                )
            )
            with pytest.raises(RuntimeError, match="temporarily unavailable"):
                await sidecar.forget_block(scope, block.id, mode=mode)
            assert await provider.read_block(scope, block.id) is None
            assert block.id in index.rows
        finally:
            await provider.close()

        # The primary deletion committed, but the index is a separate durable store.
        # A new provider/sidecar must finish cleanup without local retry state.
        provider = build_local_kernel(path)
        await provider.initialize()
        try:
            sidecar = PgVectorBlockMemory(provider, index, FakeEmbeddings(), model="test-v1")
            other = replace(scope, tenant_id="other")
            assert (await sidecar.forget_block(other, block.id, mode=mode)).affected_artifacts == 0
            assert block.id in index.rows
            result = await sidecar.forget_block(scope, block.id, mode=mode)
            assert result.affected_artifacts == 0
            assert block.id not in index.rows
            assert index.deleted == [(block.id, scope)]
            assert (await sidecar.forget_block(scope, block.id, mode=mode)).affected_artifacts == 0
            assert index.deleted == [(block.id, scope)]
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_pgvector_sidecar_preserves_visible_block_on_ineffective_primary_forget():
    class RefusingProvider(FakeProvider):
        async def forget_block(self, scope, block_id, *, mode):
            return ForgetResult(0, 0, 0, mode)

    async def scenario():
        scope = MemoryScope("tenant")
        block = Block("block", scope, "Note", "Keep my note.")
        index = FakeIndex()
        sidecar = PgVectorBlockMemory(
            RefusingProvider(block), index, FakeEmbeddings(), model="test-v1"
        )
        await sidecar.write_block(block)
        result = await sidecar.forget_block(scope, block.id)
        assert result.affected_artifacts == 0
        assert index.delete_attempts == []
        assert block.id in index.rows

    asyncio.run(scenario())
