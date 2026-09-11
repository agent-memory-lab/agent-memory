from __future__ import annotations

from collections.abc import Collection
from pathlib import Path

from .domain import MemoryCapabilities
from .kernel import MemoryKernel
from .ports import ClaimExtractor, EmbeddingProvider, MemoryPolicy, Reranker
from .providers import (
    EmbeddingReranker,
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from .sqlite import SQLiteMemoryRepository


def build_local_kernel(
    database_path: str | Path,
    *,
    extractor: ClaimExtractor | None = None,
    policy: MemoryPolicy | None = None,
    reranker: Reranker | None = None,
    embedding_provider: EmbeddingProvider | None = None,
    trusted_evaluator_ids: Collection[str] | None = None,
) -> MemoryKernel:
    base_reranker = reranker or ReciprocalRankFusionReranker()
    return MemoryKernel(
        repository=SQLiteMemoryRepository(database_path),
        extractor=extractor or MetadataClaimExtractor(),
        policy=policy or TrustedMemoryPolicy(),
        reranker=(
            EmbeddingReranker(embedding_provider, fallback=base_reranker)
            if embedding_provider
            else base_reranker
        ),
        provider_name="sqlite-local",
        trusted_evaluator_ids=trusted_evaluator_ids,
        capabilities=MemoryCapabilities(
            automatic_extraction=extractor is not None,
            memory_blocks=True,
            semantic_reranking=embedding_provider is not None,
        ),
    )
