"""Fail-closed scope boundary for a pluggable lexical evidence source.

The source is trusted to read only authorized evidence from storage and to label
each item with its stored scope. This module checks those labels before any text
is scored or exposed. Visibility is intentionally exact-scope only; inherited
scope access requires a separate, explicit authorization policy.
"""

from dataclasses import dataclass
from typing import Protocol, Sequence

from .domain import MemoryScope
from .lexical_retrieval import EvidenceItem, LexicalSearchResult, lexical_candidates


class ScopeIsolationError(ValueError):
    """A source returned evidence that cannot be safely used for this scope."""


@dataclass(frozen=True, slots=True)
class ScopedEvidenceItem:
    scope: MemoryScope
    evidence: EvidenceItem


class ScopedEvidenceSource(Protocol):
    """Storage adapter that returns bounded, scope-labelled evidence."""

    def load(self, scope: MemoryScope, *, limit: int) -> Sequence[ScopedEvidenceItem]:
        """Read evidence using the storage layer's authorization rules."""


def scoped_lexical_candidates(
    query_text: str,
    scope: MemoryScope,
    source: ScopedEvidenceSource,
    *,
    limit: int = 8,
    max_items: int = 128,
    max_item_chars: int = 2048,
) -> LexicalSearchResult:
    """Retrieve lexical candidates, rejecting the entire batch on scope drift.

    An adapter must derive item scopes from authoritative storage, not copy the
    requested scope onto unverified records. No cross-scope fallback is applied.
    """
    if not isinstance(scope, MemoryScope):
        raise TypeError("scope must be a MemoryScope")
    if isinstance(max_items, bool) or not isinstance(max_items, int) or not 1 <= max_items <= 512:
        raise ValueError("max_items must be between 1 and 512")

    records = source.load(scope, limit=max_items)
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise ScopeIsolationError("evidence source must return a bounded sequence")
    if len(records) > max_items:
        raise ScopeIsolationError("evidence source exceeded the requested item limit")

    evidence: list[EvidenceItem] = []
    for record in records:
        if not isinstance(record, ScopedEvidenceItem):
            raise ScopeIsolationError("evidence source returned an unlabelled item")
        if not isinstance(record.scope, MemoryScope) or record.scope != scope:
            raise ScopeIsolationError("evidence source returned an item from another scope")
        evidence.append(record.evidence)

    return lexical_candidates(
        query_text,
        evidence,
        limit=limit,
        max_items=max_items,
        max_item_chars=max_item_chars,
    )
