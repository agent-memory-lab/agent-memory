from __future__ import annotations

from pathlib import Path

from .kernel import MemoryKernel
from .providers import MetadataClaimExtractor, ReciprocalRankFusionReranker, TrustedMemoryPolicy
from .sqlite import SQLiteMemoryRepository


def build_local_kernel(database_path: str | Path) -> MemoryKernel:
    return MemoryKernel(
        repository=SQLiteMemoryRepository(database_path),
        extractor=MetadataClaimExtractor(),
        policy=TrustedMemoryPolicy(),
        reranker=ReciprocalRankFusionReranker(),
        provider_name="sqlite-local",
    )
