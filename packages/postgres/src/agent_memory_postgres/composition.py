from __future__ import annotations

from agent_memory import MemoryCapabilities, MemoryKernel
from agent_memory.ports import ClaimExtractor, EmbeddingProvider, MemoryPolicy, Reranker
from agent_memory.providers import (
    EmbeddingReranker,
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)

from .jobs import PostgresConsolidationQueue
from .repository import PostgresMemoryRepository


def build_postgres_kernel(
    dsn: str,
    *,
    extractor: ClaimExtractor | None = None,
    policy: MemoryPolicy | None = None,
    reranker: Reranker | None = None,
    embedding_provider: EmbeddingProvider | None = None,
    min_pool_size: int = 1,
    max_pool_size: int = 10,
) -> MemoryKernel:
    base_reranker = reranker or ReciprocalRankFusionReranker()
    repository = PostgresMemoryRepository.from_dsn(
        dsn,
        min_size=min_pool_size,
        max_size=max_pool_size,
    )
    queue = PostgresConsolidationQueue(repository.pool)
    return MemoryKernel(
        repository=repository,
        extractor=extractor or MetadataClaimExtractor(),
        policy=policy or TrustedMemoryPolicy(),
        reranker=(
            EmbeddingReranker(embedding_provider, fallback=base_reranker)
            if embedding_provider
            else base_reranker
        ),
        provider_name="postgresql",
        capabilities=MemoryCapabilities(
            automatic_extraction=extractor is not None,
            memory_blocks=True,
            semantic_reranking=embedding_provider is not None,
            background_consolidation=True,
        ),
        consolidation_scheduler=queue,
    )
