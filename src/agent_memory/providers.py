from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence

from .domain import ClaimDraft, MemoryEvent, MemoryItem, MemoryQuery, ScopeLevel


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


class TrustedMemoryPolicy:
    async def should_extract(self, event: MemoryEvent) -> bool:
        return bool(event.metadata.get("claims"))

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
