"""Retrieval methods and memory channels have different meanings."""

from datetime import datetime, timezone

import pytest

from agent_memory.candidate_fusion import fuse_candidates
from agent_memory.domain import MemoryChannel, MemoryItem, MemoryKind
from agent_memory.plugin_protocol import RetrievalCandidate
from agent_memory.plugins import PluginManifestError


_WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _candidate(
    item_id: str,
    method: str | None,
    *,
    rank: int = 1,
    text: str = "Evidence-backed fact",
    source: str = "event-1",
) -> RetrievalCandidate:
    return RetrievalCandidate(
        item=MemoryItem(item_id, MemoryKind.EVENT, text, 0.1, _WHEN),
        channel=MemoryChannel.SEMANTIC,
        rank=rank,
        source_event_ids=(source,),
        retriever=f"{method or 'legacy'}-retriever",
        retrieval_method=method,
    )


def test_distinct_retrieval_methods_fuse_even_with_same_memory_channel():
    result = fuse_candidates(
        (
            _candidate("shared", "lexical", rank=1, source="lexical-event"),
            _candidate("shared", "semantic", rank=2, source="semantic-event"),
            _candidate("single", "lexical", rank=1, source="single-event"),
        ),
        max_results=2,
    )
    shared = result.candidates[0]
    assert shared.item.id == "shared"
    assert shared.score == pytest.approx(1 / 61 + 1 / 62)
    assert shared.method_ranks == (("lexical", 1), ("semantic", 2))
    assert shared.source_event_ids == ("lexical-event", "semantic-event")
    assert result.trace.method_counts == (("lexical", 2), ("semantic", 1))


def test_same_method_duplicates_do_not_inflate_score():
    result = fuse_candidates(
        (_candidate("one", "lexical", rank=3), _candidate("one", "lexical", rank=1))
    )
    assert result.candidates[0].score == pytest.approx(1 / 61)
    assert result.candidates[0].method_ranks == (("lexical", 1),)
    assert result.trace.duplicate_count == 1


def test_conflicting_content_rejects_entire_id_within_or_across_methods():
    same_method = fuse_candidates(
        (_candidate("one", "lexical"), _candidate("one", "lexical", text="Conflicting fact"))
    )
    cross_method = fuse_candidates(
        (_candidate("one", "lexical"), _candidate("one", "semantic", text="Conflicting fact"))
    )
    for result in (same_method, cross_method):
        assert result.candidates == ()
        assert result.trace.conflict_count == 1


def test_legacy_candidates_are_compatible_but_not_assigned_a_false_method():
    legacy = _candidate("legacy", None)
    result = fuse_candidates((legacy,))
    assert result.candidates == ()
    assert result.trace.rejected_count == 1
    with pytest.raises(PluginManifestError):
        _candidate("bad", "Lexical")


def test_input_and_output_limits_are_enforced_without_consuming_all_sources():
    emitted = []

    def source():
        for index in range(10):
            emitted.append(index)
            yield _candidate(f"item-{index}", "lexical")

    result = fuse_candidates(source(), max_candidates=2, max_results=1)
    assert emitted == [0, 1, 2]
    assert result.trace.input_count == 3
    assert result.trace.accepted_count == 2
    assert result.trace.selected_count == 1
    assert result.trace.truncated
