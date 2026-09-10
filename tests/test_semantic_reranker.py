from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime

from agent_memory import EmbeddingReranker, MemoryItem, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.providers import ReciprocalRankFusionReranker


class StaticEmbeddingProvider:
    dimensions = 2

    async def embed(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return tuple(
            (1.0, 0.0)
            if text.lower() == "vehicle transport" or "automobile" in text.lower()
            else (0.0, 1.0)
            for text in texts
        )


class InvalidEmbeddingProvider:
    dimensions = 2

    async def embed(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return ((1.0, 0.0),)


def test_embedding_reranker_promotes_semantic_match() -> None:
    async def scenario() -> None:
        scope = MemoryScope("tenant", session_id="session")
        query = MemoryQuery(scope, "vehicle transport")
        candidates = (
            MemoryItem(
                "lexical",
                MemoryKind.EVENT,
                "Vehicle maintenance calendar.",
                0.9,
                datetime.now(UTC),
            ),
            MemoryItem(
                "semantic",
                MemoryKind.EVENT,
                "An automobile needs refueling.",
                0.1,
                datetime.now(UTC),
            ),
        )
        reranked = await EmbeddingReranker(
            StaticEmbeddingProvider(),
            lexical_weight=0.2,
            semantic_weight=0.8,
        ).rerank(query, candidates)
        assert reranked[0].id == "semantic"
        assert reranked[0].metadata["semantic_reranker"] == "EmbeddingReranker"

    asyncio.run(scenario())


def test_embedding_reranker_fails_open_to_baseline() -> None:
    async def scenario() -> None:
        scope = MemoryScope("tenant", session_id="session")
        query = MemoryQuery(scope, "vehicle")
        candidates = (
            MemoryItem("first", MemoryKind.EVENT, "Vehicle note", 0.9, datetime.now(UTC)),
            MemoryItem("second", MemoryKind.EVENT, "Automobile note", 0.1, datetime.now(UTC)),
        )
        baseline = await ReciprocalRankFusionReranker().rerank(query, candidates)
        reranked = await EmbeddingReranker(InvalidEmbeddingProvider()).rerank(query, candidates)
        assert reranked == baseline

    asyncio.run(scenario())
