"""Exact source interval coverage is not semantic or cross-revision deduplication."""

from dataclasses import replace

import pytest
from test_indexed_event_packing import SCOPE, indexed_item

from agent_memory.domain import MemoryKind, MemoryQuery
from agent_memory.retrieval.packing import (
    _event_fully_covered,
    _event_redundancy,
    _PreparedEvidence,
    pack_native_evidence,
)


def source():
    return (
        ("Archive north roster records. " * 40)[:800]
        + ("Archive south approval records. " * 40)[:800]
    )


def candidates(*, gap=0):
    text = source()
    separate = "Archive inspections are postponed until Monday."
    return (
        indexed_item(text, 0, 800),
        replace(indexed_item(text, 800 + gap, 1600), score=.999),
        replace(indexed_item(text, 400, 1200), score=.98),
        replace(indexed_item(separate, 0, len(separate), identity="independent"), score=.9),
    )


def test_a_window_covered_by_two_selected_windows_does_not_consume_another_slot():
    first, second, covered, independent = candidates()
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive", limit=3, token_budget=1024),
        (), (first, second, covered, independent),
    )
    assert packed.items == (first, second, independent)
    assert packed.metadata["packing_duplicate_sources"] == 1
    assert len(packed.metadata["citation_spans"]) == 3


def test_even_a_small_uncovered_interval_retains_its_candidate_opportunity():
    first, second, connecting, independent = candidates(gap=1)
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive", limit=3, token_budget=1024),
        (), (first, second, connecting, independent),
    )
    assert packed.items == (first, second, connecting)
    assert packed.metadata["packing_duplicate_sources"] == 0


def test_another_revision_or_claim_is_never_removed_by_source_interval_union():
    first, second, overlap, independent = candidates()
    revised = source()[:400] + "Z" + source()[401:]
    new_revision = replace(indexed_item(revised, 400, 1200), score=.98)
    claim = replace(overlap, kind=MemoryKind.CLAIM)
    other_source = replace(
        indexed_item(source(), 400, 1200, identity="independent-source"), score=.98,
    )
    for candidate in (new_revision, claim, other_source):
        packed = pack_native_evidence(
            MemoryQuery(SCOPE, "archive", limit=3, token_budget=1024),
            (), (first, second, candidate, independent),
        )
        assert packed.items == (first, second, candidate)


@pytest.mark.parametrize("side", ("candidate", "selected"))
@pytest.mark.parametrize("change", (
    {"source_span": {}},
    {"source_span": "invalid"},
    {"source_span": {"start": "0", "end": 100, "unit": "characters"}},
    {"source_span": {"start": True, "end": 100, "unit": "characters"}},
    {"source_span": {"start": -1, "end": 99, "unit": "characters"}},
    {"source_span": {"start": 100, "end": 0, "unit": "characters"}},
    {"source_span": {"start": 100, "end": 201, "unit": "characters"}},
    {"source_span": {"start": 100, "end": 200, "unit": "bytes"}},
    {"source_chars": None},
    {"source_chars": 10},
    {"source_revision": "unverified"},
))
def test_invalid_interval_metadata_never_proves_coverage_or_crashes_overlap(side, change):
    text = source()
    values = {
        "candidate": indexed_item(text, 100, 200),
        "selected": indexed_item(text, 0, 300),
    }
    original = values[side]
    values[side] = replace(original, metadata={**original.metadata, **change})

    def prepared(value):
        return _PreparedEvidence(value, 1, frozenset(), ("event",), ("event",), 1.0,
                                 frozenset(), 1.0, 1.0, 1.0)

    candidate, selected = prepared(values["candidate"]), prepared(values["selected"])
    assert _event_fully_covered(candidate, (selected,)) is False
    assert _event_redundancy(candidate, selected) == 0.0
