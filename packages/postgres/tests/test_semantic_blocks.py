import asyncio
from dataclasses import dataclass

from agent_memory_postgres.semantic import PgVectorBlockMemory
from agent_memory_postgres.vector import VectorHit

from agent_memory import ForgetMode, MemoryScope


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

    async def initialize(self, *, install_extension=False):
        return None

    async def upsert(self, memory_id, scope, embedding, *, model):
        self.upserts.append((memory_id, scope, tuple(embedding), model))

    async def search(self, scope, embedding, *, limit):
        assert tuple(embedding) == (1.0, 0.0)
        assert limit == 6
        return (VectorHit("missing", 0.01), VectorHit("block-1", 0.02))

    async def delete(self, memory_id):
        self.deleted.append(memory_id)


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
        return "forgotten"


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
        assert await sidecar.forget_block(scope, block.id, mode=ForgetMode.ARCHIVE) == "forgotten"
        assert index.deleted == ["block-1"]

    asyncio.run(scenario())
