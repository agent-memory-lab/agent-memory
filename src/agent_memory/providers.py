from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from math import isfinite, sqrt

from .domain import ClaimDraft, MemoryEvent, MemoryItem, MemoryQuery, Provenance, ScopeLevel
from .ports import ClaimExtractor, ClaimGenerator, EmbeddingProvider, Reranker


class MetadataClaimExtractor:
    """Trusted deterministic extractor for SDK and adapter integrations."""

    async def extract(self, event: MemoryEvent) -> Sequence[ClaimDraft]:
        raw_claims = event.metadata.get("claims", ())
        if not isinstance(raw_claims, Sequence) or isinstance(raw_claims, (str, bytes)):
            return ()

        claims: list[ClaimDraft] = []
        for raw in raw_claims:
            if not isinstance(raw, Mapping):
                continue
            key = raw.get("key")
            text = raw.get("text")
            if not isinstance(key, str) or not isinstance(text, str):
                continue
            try:
                claims.append(
                    ClaimDraft(
                        key=key,
                        value=raw.get("value"),
                        text=text,
                        confidence=float(raw.get("confidence", 1.0)),
                        importance=float(raw.get("importance", 0.5)),
                        scope_level=ScopeLevel(str(raw.get("scope", ScopeLevel.SESSION))),
                        valid_from=event.occurred_at,
                    )
                )
            except (TypeError, ValueError):
                continue
        return tuple(claims)


class CompositeClaimExtractor:
    """Combine extractors in priority order and optionally isolate provider failures."""

    def __init__(self, *extractors: ClaimExtractor, fail_open: bool = True) -> None:
        if not extractors:
            raise ValueError("at least one claim extractor is required")
        self._extractors = extractors
        self._fail_open = fail_open

    async def extract(self, event: MemoryEvent) -> Sequence[ClaimDraft]:
        drafts: list[ClaimDraft] = []
        for extractor in self._extractors:
            try:
                drafts.extend(await extractor.extract(event))
            except Exception:
                if not self._fail_open:
                    raise
        return tuple(drafts)


class GeneratedTrajectoryClaimExtractor:
    """Validate structured model output before it enters the memory write policy."""

    DEFAULT_EVENT_TYPES = frozenset(
        {
            "user.message",
            "agent.model.completed",
            "agent.tool.completed",
        }
    )

    def __init__(
        self,
        generator: ClaimGenerator,
        *,
        provider: str = "external-generator",
        model: str | None = None,
        prompt_version: str = "trajectory-claims-v1",
        minimum_confidence: float = 0.8,
        max_claims: int = 8,
        event_types: Sequence[str] | None = None,
        fail_open: bool = True,
    ) -> None:
        if not 0.0 <= minimum_confidence <= 1.0:
            raise ValueError("minimum_confidence must be between 0 and 1")
        if max_claims < 1:
            raise ValueError("max_claims must be positive")
        self._generator = generator
        self._provider = provider
        self._model = model
        self._prompt_version = prompt_version
        self._minimum_confidence = minimum_confidence
        self._max_claims = max_claims
        self._event_types = frozenset(event_types or self.DEFAULT_EVENT_TYPES)
        self._fail_open = fail_open

    async def extract(self, event: MemoryEvent) -> Sequence[ClaimDraft]:
        if event.event_type not in self._event_types:
            return ()
        try:
            generated = await self._generator.generate_claims(event)
        except Exception:
            if self._fail_open:
                return ()
            raise
        if not isinstance(generated, Sequence) or isinstance(generated, (str, bytes)):
            return ()

        generated_event = replace(event, metadata={"claims": list(generated[: self._max_claims])})
        parsed = await MetadataClaimExtractor().extract(generated_event)
        accepted: list[ClaimDraft] = []
        for draft in parsed:
            if draft.confidence < self._minimum_confidence:
                continue
            try:
                event.scope.project(draft.scope_level)
            except ValueError:
                continue
            accepted.append(
                replace(
                    draft,
                    provenance=Provenance(
                        source_event_ids=(event.id,),
                        extractor=type(self).__name__,
                        provider=self._provider,
                        model=self._model,
                        prompt_version=self._prompt_version,
                        source_uri=event.source_uri,
                    ),
                )
            )
        return tuple(accepted)


def build_trajectory_extractor(
    generator: ClaimGenerator,
    **config: object,
) -> CompositeClaimExtractor:
    """Preserve trusted explicit claims while adding automatic trajectory extraction."""

    return CompositeClaimExtractor(
        MetadataClaimExtractor(),
        GeneratedTrajectoryClaimExtractor(generator, **config),
    )


class TrustedMemoryPolicy:
    async def should_extract(self, event: MemoryEvent) -> bool:
        return True

    async def accept_claim(self, event: MemoryEvent, claim: ClaimDraft) -> bool:
        return claim.confidence >= 0.5 and bool(claim.text.strip())


class ReciprocalRankFusionReranker:
    """Provider-independent rank fusion that avoids incomparable raw score scales."""

    def __init__(self, k: int = 60) -> None:
        self._k = k

    async def rerank(
        self, query: MemoryQuery, candidates: Sequence[MemoryItem]
    ) -> Sequence[MemoryItem]:
        by_channel: dict[str, list[MemoryItem]] = defaultdict(list)
        for candidate in candidates:
            by_channel[str(candidate.metadata.get("channel", "semantic"))].append(candidate)

        fused: dict[str, float] = defaultdict(float)
        canonical: dict[str, MemoryItem] = {}
        for channel_candidates in by_channel.values():
            ranked = sorted(channel_candidates, key=lambda item: item.score, reverse=True)
            for rank, candidate in enumerate(ranked, start=1):
                fused[candidate.id] += 1.0 / (self._k + rank)
                canonical[candidate.id] = candidate

        return tuple(
            MemoryItem(
                id=canonical[item_id].id,
                kind=canonical[item_id].kind,
                text=canonical[item_id].text,
                score=score,
                occurred_at=canonical[item_id].occurred_at,
                metadata={**canonical[item_id].metadata, "fusion": "rrf"},
            )
            for item_id, score in sorted(fused.items(), key=lambda pair: pair[1], reverse=True)
        )


class EmbeddingReranker:
    """Bounded hybrid reranking over a caller-provided embedding provider.

    Vectors are used only for the active request and are never retained by the core.
    """

    def __init__(
        self,
        embedding_provider: EmbeddingProvider,
        *,
        fallback: Reranker | None = None,
        lexical_weight: float = 0.35,
        semantic_weight: float = 0.65,
        max_candidates: int = 48,
        fail_open: bool = True,
    ) -> None:
        if lexical_weight < 0.0 or semantic_weight < 0.0:
            raise ValueError("reranking weights must be non-negative")
        if lexical_weight + semantic_weight <= 0.0:
            raise ValueError("at least one reranking weight must be positive")
        if not 1 <= max_candidates <= 500:
            raise ValueError("max_candidates must be between 1 and 500")
        self._embedding_provider = embedding_provider
        self._fallback = fallback or ReciprocalRankFusionReranker()
        self._lexical_weight = lexical_weight
        self._semantic_weight = semantic_weight
        self._max_candidates = max_candidates
        self._fail_open = fail_open

    async def rerank(
        self, query: MemoryQuery, candidates: Sequence[MemoryItem]
    ) -> Sequence[MemoryItem]:
        baseline = tuple(await self._fallback.rerank(query, candidates))
        selected = baseline[: self._max_candidates]
        if not query.text.strip() or len(selected) < 2:
            return baseline

        try:
            vectors = await self._embedding_provider.embed(
                (query.text, *(candidate.text for candidate in selected))
            )
            if len(vectors) != len(selected) + 1:
                raise ValueError("embedding provider returned an unexpected vector count")
            query_vector = vectors[0]
            reranked: list[MemoryItem] = []
            denominator = max(1, len(selected) - 1)
            total_weight = self._lexical_weight + self._semantic_weight
            for index, (candidate, vector) in enumerate(zip(selected, vectors[1:], strict=True)):
                lexical_score = 1.0 - (index / denominator)
                semantic_score = (self._cosine_similarity(query_vector, vector) + 1.0) / 2.0
                score = (
                    self._lexical_weight * lexical_score
                    + self._semantic_weight * semantic_score
                ) / total_weight
                reranked.append(
                    MemoryItem(
                        id=candidate.id,
                        kind=candidate.kind,
                        text=candidate.text,
                        score=score,
                        occurred_at=candidate.occurred_at,
                        metadata={
                            **candidate.metadata,
                            "semantic_score": semantic_score,
                            "semantic_dimensions": len(query_vector),
                            "semantic_reranker": type(self).__name__,
                        },
                    )
                )
            return tuple(sorted(reranked, key=lambda item: item.score, reverse=True)) + baseline[
                self._max_candidates :
            ]
        except (TypeError, ValueError, ArithmeticError):
            if self._fail_open:
                return baseline
            raise
        except Exception:
            if self._fail_open:
                return baseline
            raise

    @staticmethod
    def _cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
        if not left or len(left) != len(right):
            raise ValueError("embedding vectors must be non-empty and have equal dimensions")
        left_values = tuple(float(value) for value in left)
        right_values = tuple(float(value) for value in right)
        if not all(isfinite(value) for value in (*left_values, *right_values)):
            raise ValueError("embedding vectors must contain finite values")
        left_norm = sqrt(sum(value * value for value in left_values))
        right_norm = sqrt(sum(value * value for value in right_values))
        if left_norm == 0.0 or right_norm == 0.0:
            raise ValueError("embedding vectors must not be zero vectors")
        similarity = sum(
            left * right for left, right in zip(left_values, right_values, strict=True)
        )
        return max(-1.0, min(1.0, similarity / (left_norm * right_norm)))
