from __future__ import annotations

from collections.abc import Collection

from agent_memory import MemoryCapabilities, MemoryKernel
from agent_memory.ports import ClaimExtractor, EmbeddingProvider, MemoryPolicy, Reranker
from agent_memory.providers import (
    EmbeddingReranker,
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.retrieval.config import NativeRetrievalOptions
from agent_memory.retrieval.lossless_packing import LosslessBudget

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
    trusted_evaluator_ids: Collection[str] | None = None,
    packing_version: str = "v2",
    passage_reranker: Reranker | None = None,
    enable_source_context: bool | None = None,
    lossless_packing_budget: LosslessBudget | None = None,
    require_delivery_guard: bool = False,
) -> MemoryKernel:
    # Omitted keeps the legacy "unsupported" diagnostic. Explicit activation
    # would require adjacent-source lookup, which this repository does not offer.
    if enable_source_context is True:
        raise ValueError("PostgreSQL does not support adjacent source-context expansion")
    source_context = True if enable_source_context is None else enable_source_context
    NativeRetrievalOptions(packing_version, passage_reranker, source_context,
                           lossless_packing_budget, require_delivery_guard)
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
        trusted_evaluator_ids=trusted_evaluator_ids,
        packing_version=packing_version,
        passage_reranker=passage_reranker,
        enable_source_context=source_context,
        lossless_packing_budget=lossless_packing_budget,
        require_delivery_guard=require_delivery_guard,
    )
