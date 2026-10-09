"""SQLite adapter for bounded, indexed, exact-scope lexical event recall."""

from ..domain import MemoryScope
from .scoped_lexical import ScopedEvidenceItem
from ..sqlite import SQLiteMemoryRepository


class SQLiteRecentEventEvidenceSource:
    """Compatibility name for the query-aware, incremental lexical evidence source.

    ``search`` covers all live events, including exact spans in long documents.
    ``load`` remains a newest-first bounded browse operation for older host adapters.
    """

    def __init__(self, repository: SQLiteMemoryRepository) -> None:
        self._repository = repository

    def load(self, scope: MemoryScope, *, limit: int) -> tuple[ScopedEvidenceItem, ...]:
        return self._repository.load_recent_event_evidence(scope, limit=limit)

    def search(
        self, scope: MemoryScope, query_text: str, *, limit: int
    ) -> tuple[ScopedEvidenceItem, ...]:
        return self._repository.load_indexed_event_evidence(scope, query_text, limit=limit)
