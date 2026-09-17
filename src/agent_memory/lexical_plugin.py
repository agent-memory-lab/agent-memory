"""Opt-in, async lexical candidate plugin for an agent's retrieval pipeline.

The plugin owns no memory store or background worker. Hosts can inject any
trusted scoped evidence source, or use the bundled SQLite recent-event source.
"""

import asyncio
from typing import TYPE_CHECKING

from .domain import MemoryScope
from .lexical_retrieval import LexicalSearchResult
from .scoped_lexical_retrieval import ScopedEvidenceSource, scoped_lexical_candidates

if TYPE_CHECKING:
    from .sqlite import SQLiteMemoryRepository


class LexicalCandidatePlugin:
    """An optional candidate source; it does not replace the default recall path."""

    def __init__(
        self,
        source: ScopedEvidenceSource,
        *,
        limit: int = 8,
        max_items: int = 128,
        max_item_chars: int = 2048,
    ) -> None:
        self._source = source
        self._limit = limit
        self._max_items = max_items
        self._max_item_chars = max_item_chars

    @classmethod
    def from_sqlite(
        cls,
        repository: "SQLiteMemoryRepository",
        *,
        limit: int = 8,
        max_items: int = 128,
    ) -> "LexicalCandidatePlugin":
        """Use a recent-event window without creating a duplicate index."""
        from .sqlite_evidence_source import SQLiteRecentEventEvidenceSource

        return cls(
            SQLiteRecentEventEvidenceSource(repository),
            limit=limit,
            max_items=max_items,
        )

    async def candidates(
        self, query_text: str, scope: MemoryScope
    ) -> LexicalSearchResult:
        """Return ranked, evidence-linked candidates within a strict scope."""
        return await asyncio.to_thread(
            scoped_lexical_candidates,
            query_text,
            scope,
            self._source,
            limit=self._limit,
            max_items=self._max_items,
            max_item_chars=self._max_item_chars,
        )
