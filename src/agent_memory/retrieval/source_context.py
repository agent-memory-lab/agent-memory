"""Provider-neutral bounds and anchors for post-ranking source context."""

from collections.abc import Sequence
from math import isfinite

from ..domain import MemoryItem, MemoryKind, MemoryQuery
from .candidate_identity import candidate_identity

CONTEXT_POLICY = "reranked-adjacent-parent/1"
MAX_CONTEXT_SOURCES = 4
MAX_CONTEXT_CANDIDATES = 8


def context_anchors(query: MemoryQuery, ranked: Sequence[MemoryItem]) -> tuple[MemoryItem, ...]:
    """One ranked source per public EVENT ID; metadata cannot invent ownership."""
    result = []
    seen = set()
    for item in ranked:
        if item.kind != MemoryKind.EVENT or item.id in seen or not isfinite(item.score):
            continue
        identity = candidate_identity(item)
        if identity == item.id or not identity.startswith("source-span:"):
            continue
        seen.add(item.id)
        result.append(item)
        if len(result) >= min(MAX_CONTEXT_SOURCES, query.limit):
            break
    return tuple(result)
