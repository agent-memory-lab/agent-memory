"""Focused contract tests for the optional, in-memory lexical candidate source."""

from datetime import datetime, timezone

import pytest

from agent_memory.candidate_fusion import fuse_candidates
from agent_memory.domain import MemoryChannel, MemoryItem, MemoryKind
from agent_memory.lexical_retrieval import EvidenceItem, lexical_candidates
from agent_memory.plugin_protocol import RetrievalCandidate


def _item(item_id: str, text: str) -> MemoryItem:
    return MemoryItem(
        item_id,
        MemoryKind.EVENT,
        text,
        0.5,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _evidence(item_id: str, text: str, *sources: str) -> EvidenceItem:
    return EvidenceItem(_item(item_id, text), MemoryChannel.SEMANTIC, sources)


def test_ascii_matching_is_case_insensitive_and_ranked() -> None:
    result = lexical_candidates(
        "POSTGRESQL rollback",
        [
            _evidence("best", "PostgreSQL migration rollback", "event-1"),
            _evidence("partial", "rollback checklist", "event-2"),
            _evidence("other", "network latency", "event-3"),
        ],
    )

    assert [candidate.item.id for candidate in result.candidates] == ["best", "partial"]
    assert [candidate.rank for candidate in result.candidates] == [1, 2]
    assert all(candidate.retrieval_method == "lexical" for candidate in result.candidates)
    assert result.trace.matched_count == 2


def test_chinese_matching_and_deterministic_tie_break() -> None:
    result = lexical_candidates(
        "迁移回滚",
        [
            _evidence("z", "数据库迁移回滚流程", "event-z"),
            _evidence("a", "数据库迁移回滚流程", "event-a"),
            _evidence("unrelated", "网络监控告警", "event-u"),
        ],
    )

    assert [candidate.item.id for candidate in result.candidates] == ["a", "z"]


def test_duplicate_id_merges_evidence_without_duplicate_candidate() -> None:
    result = lexical_candidates(
        "migration",
        [
            _evidence("same", "migration notes", "event-1"),
            _evidence("same", "migration notes", "event-2", "event-1"),
        ],
    )

    assert len(result.candidates) == 1
    assert set(result.candidates[0].source_event_ids) == {"event-1", "event-2"}
    assert result.trace.indexed_count == 1


def test_conflicting_same_id_is_excluded_entirely() -> None:
    result = lexical_candidates(
        "migration",
        [
            _evidence("same", "migration started", "event-1"),
            _evidence("same", "migration cancelled", "event-2"),
        ],
    )

    assert result.candidates == () or result.candidates == []
    assert result.trace.conflict_count == 1


def test_missing_evidence_is_rejected_and_no_match_returns_empty() -> None:
    result = lexical_candidates(
        "migration",
        [
            _evidence("invalid", "migration notes"),
            _evidence("valid", "network latency", "event-1"),
        ],
    )

    assert not result.candidates
    assert result.trace.rejected_count == 1
    assert result.trace.matched_count == 0


def test_bounded_inputs_and_output_limit() -> None:
    items = [
        _evidence("b", "migration notes", "event-b"),
        _evidence("a", "migration notes", "event-a"),
    ]
    result = lexical_candidates("migration", items, limit=1)
    assert [candidate.item.id for candidate in result.candidates] == ["a"]
    assert result.trace.returned_count == 1

    with pytest.raises(ValueError):
        lexical_candidates("migration", items, limit=0)
    with pytest.raises(ValueError):
        lexical_candidates("migration", items, max_items=1)
    with pytest.raises(ValueError):
        lexical_candidates("x" * 513, items)


def test_lexical_candidate_can_fuse_with_an_independent_retrieval_method() -> None:
    evidence = _evidence("shared", "migration rollback", "event-1")
    lexical = lexical_candidates("migration", [evidence]).candidates[0]
    semantic = RetrievalCandidate(
        item=evidence.item,
        channel=evidence.channel,
        rank=1,
        source_event_ids=("event-2",),
        retriever="embedding",
        retrieval_method="semantic",
    )

    fused = fuse_candidates([lexical, semantic])

    assert len(fused.candidates) == 1
    assert set(fused.candidates[0].method_ranks) == {("lexical", 1), ("semantic", 1)}
    assert set(fused.candidates[0].source_event_ids) == {"event-1", "event-2"}
