from __future__ import annotations

from collections.abc import Iterable

from agent_memory import ForgetMode, ForgetResult, MemoryBlock, MemoryScope
from agent_memory.ports import EmbeddingProvider, MemoryProvider

from .vector import PgVectorIndex


class PgVectorBlockMemory:
    """Optional durable semantic search for evidence-backed memory blocks.

    pgvector stores only vectors, block ids, and scope fields. The supplied provider
    remains authoritative for both block contents and visibility checks.
    """

    def __init__(
        self,
        provider: MemoryProvider,
        index: PgVectorIndex,
        embedding_provider: EmbeddingProvider,
        *,
        model: str,
    ) -> None:
        if index.dimensions != embedding_provider.dimensions:
            raise ValueError(
                "pgvector dimensions must match the embedding provider dimensions"
            )
        if not model.strip():
            raise ValueError("model is required")
        self._provider = provider
        self._index = index
        self._embedding_provider = embedding_provider
        self._model = model

    async def initialize(self, *, install_extension: bool = False) -> None:
        await self._index.initialize(install_extension=install_extension)

    async def write_block(
        self, block: MemoryBlock, *, expected_version: int = 0
    ) -> MemoryBlock:
        saved = await self._provider.write_block(block, expected_version=expected_version)
        await self.index_block(saved)
        return saved

    async def index_block(self, block: MemoryBlock) -> None:
        embedding = await self._embed_one(self._block_text(block))
        await self._index.upsert(block.id, block.scope, embedding, model=self._model)

    async def reindex(self, blocks: Iterable[MemoryBlock]) -> int:
        count = 0
        for block in blocks:
            await self.index_block(block)
            count += 1
        return count

    async def search(
        self,
        scope: MemoryScope,
        text: str,
        *,
        limit: int = 8,
    ) -> tuple[MemoryBlock, ...]:
        if not text.strip():
            raise ValueError("search text is required")
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        embedding = await self._embed_one(text)
        hits = await self._index.search(scope, embedding, limit=min(limit * 3, 1000))
        blocks: list[MemoryBlock] = []
        for hit in hits:
            block = await self._provider.read_block(scope, hit.memory_id)
            if block is not None:
                blocks.append(block)
            if len(blocks) == limit:
                break
        return tuple(blocks)

    async def forget_block(
        self,
        scope: MemoryScope,
        block_id: str,
        *,
        mode: ForgetMode = ForgetMode.ARCHIVE,
    ) -> ForgetResult:
        result = await self._provider.forget_block(scope, block_id, mode=mode)
        await self._index.delete(block_id)
        return result

    async def _embed_one(self, text: str) -> tuple[float, ...]:
        vectors = await self._embedding_provider.embed((text,))
        if len(vectors) != 1:
            raise ValueError("embedding provider must return exactly one vector")
        return tuple(vectors[0])

    @staticmethod
    def _block_text(block: MemoryBlock) -> str:
        return f"{block.title}\n{block.content}"
