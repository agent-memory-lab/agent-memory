"""Source-fidelity and feasible packing contracts, not quality benchmarks."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from json import dumps

from agent_memory import MemoryEvent, build_local_kernel
from agent_memory.domain import (
    Claim,
    ClaimStatus,
    MemoryItem,
    MemoryKind,
    MemoryQuery,
    MemoryScope,
    Provenance,
)
from agent_memory.retrieval.bundle import BundleBudget, pack_memory_bundle
from agent_memory.retrieval.diversity import DiversityBudget
from agent_memory.retrieval.fusion import FusedCandidate
from agent_memory.retrieval.packing import contiguous_excerpt, pack_native_evidence

NOW = datetime(2026, 10, 1, tzinfo=UTC)
SCOPE = MemoryScope("coverage", session_id="session")


def claim(identity, text, *, value=None, key="owner", sources=("source",)):
    return Claim(
        identity,
        SCOPE,
        key,
        value or text,
        text,
        1.0,
        1.0,
        ClaimStatus.ACTIVE,
        Provenance(sources),
        NOW,
        NOW,
    )


def item(identity, text, *, kind=MemoryKind.EVENT, sources=("source",), score=1.0, **metadata):
    return MemoryItem(identity, kind, text, score, NOW, {"source_event_ids": sources, **metadata})


def test_oversized_first_claim_does_not_hide_feasible_relevant_state():
    query = MemoryQuery(SCOPE, "owner Alice", token_budget=64)
    large = claim("a", "owner " * 500)
    relevant = claim("z", "owner Alice")
    packed = pack_native_evidence(query, (large, relevant), ())
    assert [c.id for c in packed.state] == ["z"]
    assert packed.tokens <= 64
    assert packed.metadata["packing_infeasible"] == 1


def test_relevance_and_uncovered_query_terms_precede_unrelated_state():
    query = MemoryQuery(SCOPE, "launch owner risk", limit=2, token_budget=64)
    current = tuple(claim(str(n), "unrelated " * 10, key=str(n)) for n in range(20))
    packed = pack_native_evidence(
        query,
        current,
        (
            item("risk", "launch risk: staffing", sources=("s1",)),
            item("owner", "launch owner: Alice", sources=("s2",), score=0.9),
        ),
    )
    assert {v.id for v in packed.items} == {"risk", "owner"}
    assert packed.metadata["packing_query_terms_covered"] == 3
    assert packed.tokens <= query.token_budget


def test_shared_source_conflicts_survive_and_original_can_be_packed():
    alice = claim("alice", "owner Alice", value="Alice")
    bob = claim("bob", "owner Bob", value="Bob")
    source = item("source", "owner Alice. owner Bob.")
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "owner", token_budget=128), (alice, bob), (source, source)
    )
    assert {c.value for c in packed.state} == {"Alice", "Bob"}
    assert [i.id for i in packed.items] == ["source"]
    assert len(packed.citations) == 3
    assert all(c.source_event_ids == ("source",) for c in packed.citations)


def test_only_identical_event_evidence_is_deduplicated_not_other_passages():
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "owner", limit=8),
        (),
        (
            item("a", "owner Alice"),
            item("duplicate", "owner Alice"),
            item("b", "owner Bob"),
        ),
    )
    assert [value.id for value in packed.items] == ["a", "b"]
    assert packed.metadata["packing_duplicate_sources"] == 1


def test_indexed_chunk_kept_exact_when_it_fits_and_narrowed_with_lineage_otherwise():
    source = "x" * 100 + "文" * 200 + "x" * 600
    revision = sha256(source.encode("utf-8")).hexdigest()
    chunk_id = sha256(dumps(["events", "event", revision, 100, 300]).encode()).hexdigest()
    chunk = item(
        "event",
        "文" * 200,
        excerpt=True,
        lexical_chunk_id=chunk_id,
        source_revision=revision,
        source_chars=900,
        source_span={"start": 100, "end": 300, "unit": "characters"},
    )
    large = pack_native_evidence(MemoryQuery(SCOPE, "文", token_budget=256), (), (chunk,))
    assert large.items == (chunk,)
    assert large.items[0].metadata == chunk.metadata
    small = pack_native_evidence(
        MemoryQuery(SCOPE, "文", token_budget=64),
        (),
        (chunk, item("small", "文 evidence", score=0.5)),
    )
    narrowed = next(value for value in small.items if value.id == "event")
    assert (
        narrowed.text
        == source[
            narrowed.metadata["source_span"]["start"] : narrowed.metadata["source_span"]["end"]
        ]
    )
    assert narrowed.metadata["retrieved_lexical_chunk_id"] == chunk_id
    assert narrowed.metadata["retrieved_source_span"] == chunk.metadata["source_span"]
    assert "lexical_chunk_id" not in narrowed.metadata
    assert small.tokens <= 64


def test_legacy_excerpt_has_exact_original_offsets_and_revision():
    text = "ß" * 700 + "\n" + "Budget owner Alice.\n" + "unrelated " * 900
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "Budget owner Alice", token_budget=256), (), (item("event", text),)
    )
    excerpt = packed.items[0]
    span = excerpt.metadata["source_span"]
    assert excerpt.text == text[span["start"] : span["end"]]
    assert "Budget owner Alice" in excerpt.text
    assert excerpt.metadata["source_chars"] == len(text)
    assert excerpt.metadata["source_revision"] == sha256(text.encode()).hexdigest()
    assert packed.citations[0].memory_id == "event"
    assert packed.metadata["coverage"] == "partial"
    assert packed.metadata["world_negative"] is False
    assert contiguous_excerpt(text, "owner", 0) == ("", 0, 0)


def test_governed_source_quota_keeps_distinct_claims_and_supporting_event():
    candidates = tuple(
        FusedCandidate(
            item(identity, text, kind=kind),
            1 / (rank + 1),
            ("source",),
            (("lexical", rank + 1),),
            ("lexical",),
        )
        for rank, (identity, text, kind) in enumerate(
            (
                ("alice", "owner Alice", MemoryKind.CLAIM),
                ("bob", "owner Bob", MemoryKind.CLAIM),
                ("source", "owner Alice. owner Bob.", MemoryKind.EVENT),
                ("copy", "owner Alice. owner Bob.", MemoryKind.EVENT),
            )
        )
    )
    packed = pack_memory_bundle(
        SCOPE,
        candidates,
        budget=BundleBudget(max_items=4),
        diversity_budget=DiversityBudget(max_items=4, max_per_source_event=1),
    )
    assert [i.id for i in packed.bundle.relevant_memories] == ["alice", "bob", "source"]
    assert packed.diversity_trace.dropped_source_quota == 1


def test_native_kernel_delivers_traceable_evidence_and_partial_empty(tmp_path):
    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            text = "padding " * 500 + "\nlaunch risk staffing\n" + "padding " * 500
            event = MemoryEvent(SCOPE, "user.message", text)
            await kernel.ingest_event(event)
            bundle = await kernel.retrieve(
                MemoryQuery(
                    SCOPE, "launch risk staffing", token_budget=256, include_current_state=False
                )
            )
            evidence = next(i for i in bundle.relevant_memories if i.id == event.id)
            span = evidence.metadata["source_span"]
            assert evidence.text == text[span["start"] : span["end"]]
            assert any(c.memory_id == event.id for c in bundle.citations)
            empty = await kernel.retrieve(
                MemoryQuery(replace(SCOPE, tenant_id="empty"), "all risks")
            )
            assert empty.relevant_memories == ()
            assert empty.retrieval_metadata["coverage"] == "partial"
            assert empty.retrieval_metadata["world_negative"] is False
        finally:
            await kernel.close()

    asyncio.run(run())


def test_oversized_full_claim_does_not_erase_its_feasible_exact_chunk():
    full = claim("long-claim", "owner Alice " * 300)
    chunk = item(
        "long-claim",
        "owner Alice",
        kind=MemoryKind.CLAIM,
        source_span={"start": 0, "end": 11, "unit": "characters"},
        source_revision="full-revision",
        source_revision_id="claim-version",
        excerpt=True,
    )
    packed = pack_native_evidence(MemoryQuery(SCOPE, "owner", token_budget=64), (full,), (chunk,))
    assert packed.state == ()
    assert packed.items == (chunk,)
    assert packed.citations[0].memory_id == "long-claim"


def test_text_analysis_is_once_per_representation_not_per_selection(monkeypatch):
    from agent_memory.retrieval import packing

    calls = {"terms": 0, "tokens": 0}
    original_terms, original_tokens = packing.lexical_terms, packing.token_estimate

    def counted_terms(text):
        calls["terms"] += 1
        return original_terms(text)

    def counted_tokens(text):
        calls["tokens"] += 1
        return original_tokens(text)

    monkeypatch.setattr(packing, "lexical_terms", counted_terms)
    monkeypatch.setattr(packing, "token_estimate", counted_tokens)
    current = tuple(claim(f"state-{i}", f"owner {i}", key=str(i)) for i in range(80))
    items = tuple(item(f"item-{i}", f"owner risk {i}", sources=(str(i),)) for i in range(20))
    packed = packing.pack_native_evidence(
        MemoryQuery(SCOPE, "owner risk", limit=20, token_budget=10000), current, items
    )
    assert len(packed.state) == 64 and len(packed.items) == 20
    assert calls == {"terms": 101, "tokens": 120}
    assert packed.metadata["packing_analyzed_state"] == 80
    assert packed.metadata["packing_analyzed_items"] == 20


def test_one_legacy_excerpt_can_be_skipped_later_while_smaller_evidence_fits(monkeypatch):
    from agent_memory.retrieval import packing

    calls = []
    original = packing.contiguous_excerpt

    def counted(*args):
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(packing, "contiguous_excerpt", counted)
    state = claim("state", "owner risk " + "x" * 780)
    packed = packing.pack_native_evidence(
        MemoryQuery(SCOPE, "owner risk", limit=2, token_budget=256),
        (state,),
        (item("legacy", "risk " * 1000), item("small", "risk evidence", score=0.5)),
    )
    assert len(calls) == 1
    assert packed.state == (state,)
    assert [value.id for value in packed.items] == ["small"]
    assert packed.tokens <= 256
