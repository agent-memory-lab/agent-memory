from __future__ import annotations

from agent_memory import MemoryCapabilities, MemoryKernel
from agent_memory.ports import ClaimExtractor, MemoryPolicy, Reranker
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)

from .repository import PostgresMemoryRepository
from .jobs import PostgresConsolidationQueue


def build_postgres_kernel(
    dsn: str,
    *,
    extractor: ClaimExtractor | None = None,
    policy: MemoryPolicy | None = None,
    reranker: Reranker | None = None,
    min_pool_size: int = 1,
    max_pool_size: int = 10,
) -> MemoryKernel:
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
        reranker=reranker or ReciprocalRankFusionReranker(),
        provider_name="postgresql",
        capabilities=MemoryCapabilities(background_consolidation=True),
        consolidation_scheduler=queue,
    )
