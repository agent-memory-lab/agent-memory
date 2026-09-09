from __future__ import annotations

from pathlib import Path

from .domain import MemoryCapabilities
from .kernel import MemoryKernel
from .ports import ClaimExtractor, MemoryPolicy, Reranker
from .providers import MetadataClaimExtractor, ReciprocalRankFusionReranker, TrustedMemoryPolicy
from .sqlite import SQLiteMemoryRepository


def build_local_kernel(
    database_path: str | Path,
    *,
    extractor: ClaimExtractor | None = None,
    policy: MemoryPolicy | None = None,
    reranker: Reranker | None = None,
) -> MemoryKernel:
    return MemoryKernel(
        repository=SQLiteMemoryRepository(database_path),
        extractor=extractor or MetadataClaimExtractor(),
        policy=policy or TrustedMemoryPolicy(),
        reranker=reranker or ReciprocalRankFusionReranker(),
        provider_name="sqlite-local",
        capabilities=MemoryCapabilities(
            automatic_extraction=extractor is not None,
            memory_blocks=True,
        ),
    )
