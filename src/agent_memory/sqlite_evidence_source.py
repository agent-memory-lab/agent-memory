"""Optional SQLite adapter for bounded, exact-scope lexical event recall."""

from .domain import MemoryScope
from .scoped_lexical_retrieval import ScopedEvidenceItem
from .sqlite import SQLiteMemoryRepository


class SQLiteRecentEventEvidenceSource:
    """Use the canonical events table without duplicating memory into an index.

    Only the most recent 512 events can be scanned in one call; older matches
    outside the requested window will not be recalled by this adapter.
    """

    def __init__(self, repository: SQLiteMemoryRepository) -> None:
        self._repository = repository

    def load(self, scope: MemoryScope, *, limit: int) -> tuple[ScopedEvidenceItem, ...]:
        return self._repository.load_recent_event_evidence(scope, limit=limit)
