"""Boundary controls for canonical claims and complete legacy source proofs."""

from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256

import pytest

from agent_memory.domain import (
    Claim,
    ClaimStatus,
    MemoryItem,
    MemoryKind,
    MemoryQuery,
    MemoryScope,
    Provenance,
)
from agent_memory.retrieval.lossless_packing import (
    LosslessBudget,
    _preserves_event,
    pack_lossless_evidence,
)

NOW = datetime(2026, 11, 22, tzinfo=UTC)
SCOPE = MemoryScope("lossless-boundaries")
BUDGET = LosslessBudget(
    lambda state, items: sum(len(v.text) for v in (*state, *items)), "synthetic-characters"
)


def claim(text, identity="claim"):
    return Claim(
        identity,
        SCOPE,
        identity,
        text,
        text,
        1.0,
        0.5,
        ClaimStatus.ACTIVE,
        Provenance(("source",)),
        NOW,
        NOW,
    )


def candidate(value, *, text=None, kind=MemoryKind.CLAIM):
    return MemoryItem(
        value.id,
        kind,
        text or value.text,
        1.0,
        NOW,
        {"source_event_ids": value.provenance.source_event_ids},
    )


def test_ranked_claim_uses_one_authoritative_state_and_keeps_unranked_state():
    first, second = claim("contact email"), claim("language Chinese", "second")
    packed = pack_lossless_evidence(
        MemoryQuery(SCOPE, "contact", token_budget=128),
        (second, first),
        (candidate(first), candidate(first)),
        budget=BUDGET,
    )
    assert packed.state == (first, second)
    assert not packed.items
    assert len(packed.citations) == len(packed.metadata["citation_spans"]) == 2


def test_oversized_state_keeps_its_feasible_ranked_claim_representation():
    state = claim("contact email " * 50)
    short = candidate(state, text="contact email")
    packed = pack_lossless_evidence(
        MemoryQuery(SCOPE, "contact", token_budget=64),
        (state,),
        (short,),
        budget=BUDGET,
    )
    assert not packed.state
    assert packed.items == (short,)
    assert len(packed.citations) == 1


def test_state_and_event_with_same_public_id_remain_distinct():
    state = claim("contact email", "source")
    event = candidate(state, kind=MemoryKind.EVENT)
    packed = pack_lossless_evidence(
        MemoryQuery(SCOPE, "contact", token_budget=128),
        (state,),
        (event,),
        budget=BUDGET,
    )
    assert packed.state == (state,)
    assert packed.items == (event,)
    assert len(packed.metadata["citation_spans"]) == 2


def legacy_source_and_excerpt():
    text = "Invoice total is 300, not 300 more. " * 60
    full = MemoryItem("source", MemoryKind.EVENT, text, 1.0, NOW, {"source_event_ids": ("source",)})
    excerpt = replace(
        full,
        text=text[8:28],
        metadata={
            **full.metadata,
            "excerpt": True,
            "source_revision": sha256(text.encode()).hexdigest(),
            "source_chars": len(text),
            "source_span": {"start": 8, "end": 28, "unit": "characters"},
        },
    )
    return full, excerpt


def test_legacy_full_source_proves_its_exact_excerpt_without_index_locators():
    full, excerpt = legacy_source_and_excerpt()
    assert _preserves_event(excerpt, (full,))


@pytest.mark.parametrize(
    "change",
    [
        {"source_revision": "0" * 64},
        {"source_chars": 1},
        {"source_span": None},
        {"source_span": {"start": 8, "end": 28, "unit": "bytes"}},
        {"source_span": {"start": 0, "end": 20, "unit": "characters"}},
        {"source_span": {"start": True, "end": 21, "unit": "characters"}},
        {"source_span": {"start": -1, "end": 19, "unit": "characters"}},
        {"source_span": {"start": 8, "end": 29, "unit": "characters"}},
        {"source_span": {"start": 2040, "end": 2060, "unit": "characters"}},
        {"lexical_chunk_id": "forged"},
        {"retrieved_lexical_chunk_id": "forged"},
    ],
)
def test_legacy_expansion_rejects_invalid_excerpt_proofs(change):
    full, excerpt = legacy_source_and_excerpt()
    bad = replace(excerpt, metadata={**excerpt.metadata, **change})
    assert not _preserves_event(bad, (full,))


@pytest.mark.parametrize(
    "change",
    [
        {"excerpt": True},
        {"source_revision": "0" * 64},
        {"source_chars": 1},
        {"source_span": None},
        {"lexical_chunk_id": "forged"},
        {"retrieved_source_span": {"start": 0, "end": 100, "unit": "characters"}},
        {"retrieved_lexical_chunk_id": "forged"},
    ],
)
def test_legacy_expansion_does_not_treat_partial_or_misbound_sources_as_complete(change):
    full, excerpt = legacy_source_and_excerpt()
    bad = replace(full, metadata={**full.metadata, **change})
    assert not _preserves_event(excerpt, (bad,))


def test_legacy_expansion_rejects_changed_source_revision_with_same_excerpt():
    full, excerpt = legacy_source_and_excerpt()
    assert excerpt.text in full.text + "new revision"
    assert not _preserves_event(excerpt, (replace(full, text=full.text + "new revision"),))
