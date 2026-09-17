"""Contract tests for opt-in, scope-checked candidate fusion."""

import asyncio
from datetime import datetime, timezone

import pytest

from agent_memory.domain import MemoryChannel, MemoryItem, MemoryKind, MemoryScope
from agent_memory.hybrid_candidate_plugin import (
    HybridCandidatePlugin,
    ScopedRetrievalCandidate,
)
from agent_memory.lexical_plugin import LexicalCandidatePlugin
from agent_memory.lexical_retrieval import EvidenceItem
from agent_memory.plugin_protocol import RetrievalCandidate
from agent_memory.scoped_lexical_retrieval import ScopeIsolationError, ScopedEvidenceItem


def _item() -> MemoryItem:
    return MemoryItem(
        "event-1",
        MemoryKind.EVENT,
        "migration rollback checklist",
        0.5,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


class LexicalEvidence:
    def __init__(self, scope: MemoryScope, item: MemoryItem) -> None:
        self.scope = scope
        self.item = item

    def load(self, scope: MemoryScope, *, limit: int):
        return (
            ScopedEvidenceItem(
                self.scope,
                EvidenceItem(self.item, MemoryChannel.SEMANTIC, ("event-1",)),
            ),
        )


class ExtraSource:
    def __init__(self, records) -> None:
        self.records = records
        self.requested_limit = None

    async def candidates(self, query_text: str, scope: MemoryScope, *, limit: int):
        self.requested_limit = limit
        return self.records


def _extra(scope: MemoryScope, item: MemoryItem) -> ScopedRetrievalCandidate:
    return ScopedRetrievalCandidate(
        scope=scope,
        candidate=RetrievalCandidate(
            item=item,
            channel=MemoryChannel.SEMANTIC,
            rank=1,
            source_event_ids=("event-2",),
            retriever="embedding",
            retrieval_method="semantic",
        ),
    )


def test_same_memory_fuses_methods_and_provenance() -> None:
    scope = MemoryScope("tenant-a")
    item = _item()
    extra = ExtraSource((_extra(scope, item),))
    plugin = HybridCandidatePlugin(
        LexicalCandidatePlugin(LexicalEvidence(scope, item)),
        (extra,),
    )

    result = asyncio.run(plugin.candidates("migration", scope))

    assert len(result.fusion.candidates) == 1
    candidate = result.fusion.candidates[0]
    assert set(candidate.method_ranks) == {("lexical", 1), ("semantic", 1)}
    assert set(candidate.source_event_ids) == {"event-1", "event-2"}
    assert result.lexical_trace.returned_count == 1
    assert extra.requested_limit == 255


def test_foreign_scoped_source_fails_before_fusion() -> None:
    scope = MemoryScope("tenant-a")
    item = _item()
    plugin = HybridCandidatePlugin(
        LexicalCandidatePlugin(LexicalEvidence(scope, item)),
        (ExtraSource((_extra(MemoryScope("tenant-b"), item),)),),
    )

    with pytest.raises(ScopeIsolationError, match="another scope"):
        asyncio.run(plugin.candidates("migration", scope))


def test_additional_source_cannot_exceed_remaining_budget() -> None:
    scope = MemoryScope("tenant-a")
    item = _item()
    extra = ExtraSource((_extra(scope, item), _extra(scope, item)))
    plugin = HybridCandidatePlugin(
        LexicalCandidatePlugin(LexicalEvidence(scope, item)),
        (extra,),
        max_candidates=2,
        max_results=2,
    )

    with pytest.raises(ScopeIsolationError, match="candidate budget"):
        asyncio.run(plugin.candidates("migration", scope))
    assert extra.requested_limit == 1


def test_unlabelled_external_candidate_is_rejected() -> None:
    scope = MemoryScope("tenant-a")
    item = _item()
    plugin = HybridCandidatePlugin(
        LexicalCandidatePlugin(LexicalEvidence(scope, item)),
        (ExtraSource((_extra(scope, item).candidate,)),),
    )

    with pytest.raises(ScopeIsolationError, match="unlabelled"):
        asyncio.run(plugin.candidates("migration", scope))
