"""Real indexed EVENT span narrowing under native-kernel token budgets."""

import asyncio
import random
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from json import dumps

import pytest

from agent_memory import MemoryEvent, build_local_kernel
from agent_memory.domain import MemoryItem, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.retrieval.packing import pack_native_evidence

SCOPE = MemoryScope("indexed-packing", session_id="session")
NOW = datetime(2026, 10, 1, tzinfo=UTC)


def indexed_item(source, start, end, *, identity="event"):
    revision = sha256(source.encode("utf-8")).hexdigest()
    chunk_id = sha256(dumps(["events", identity, revision, start, end]).encode("utf-8")).hexdigest()
    return MemoryItem(
        identity,
        MemoryKind.EVENT,
        source[start:end],
        1.0,
        NOW,
        {
            "source_event_ids": (identity,),
            "lexical_chunk_id": chunk_id,
            "source_revision": revision,
            "source_chars": len(source),
            "excerpt": True,
            "source_span": {"start": start, "end": end, "unit": "characters"},
        },
    )


def assert_source_lineage(delivered, retrieved, original):
    span = delivered.metadata["source_span"]
    source_span = retrieved.metadata["source_span"]
    assert delivered.id == retrieved.id
    assert delivered.text == original[span["start"] : span["end"]]
    assert source_span["start"] <= span["start"] < span["end"] <= source_span["end"]
    assert delivered.metadata["source_revision"] == retrieved.metadata["source_revision"]
    assert delivered.metadata["source_chars"] == len(original)
    if delivered.text != retrieved.text:
        assert "lexical_chunk_id" not in delivered.metadata
        assert (
            delivered.metadata["retrieved_lexical_chunk_id"]
            == retrieved.metadata["lexical_chunk_id"]
        )
        assert delivered.metadata["retrieved_source_span"] == source_span
        assert (
            delivered.metadata["retrieved_chunk_text_sha256"]
            == sha256(retrieved.text.encode()).hexdigest()
        )


@pytest.mark.parametrize("budget", (64, 128, 256, 512))
def test_real_index_and_kernel_preserve_han_long_tail_under_budget(tmp_path, budget):
    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            text = "例行设备维护与状态检查。" * 280 + "\n雪鸮凭证的恢复标记是 amber-47。"
            event = MemoryEvent(SCOPE, "user.message", text, id="long-tail")
            await kernel.ingest_event(event)
            await kernel.ingest_event(
                MemoryEvent(
                    SCOPE,
                    "user.message",
                    "meeting preference",
                    metadata={
                        "claims": [
                            {"key": "meeting", "value": "morning", "text": "morning meetings"}
                        ],
                    },
                )
            )
            query = MemoryQuery(SCOPE, "雪鸮 amber-47 恢复", limit=4, token_budget=budget)
            candidates = await kernel._repository.search(query, 24)
            retrieved = next(item for item in candidates if item.id == event.id)
            bundle = await kernel.retrieve(query)
            delivered = next(item for item in bundle.relevant_memories if item.id == event.id)
            assert "amber-47" in delivered.text
            assert_source_lineage(delivered, retrieved, text)
            assert bundle.token_estimate <= budget
            assert any(
                c.memory_id == event.id and c.source_event_ids == (event.id,)
                for c in bundle.citations
            )
            assert bundle.retrieval_metadata["coverage"] == "partial"
        finally:
            await kernel.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "change",
    (
        {"source_span": {"start": True, "end": 100, "unit": "characters"}},
        {"source_span": {"start": 1, "end": 100, "unit": "characters"}},
        {"source_span": {"start": 0, "end": 100, "unit": "bytes"}},
        {"source_span": {"start": -1, "end": 99, "unit": "characters"}},
        {"source_chars": 99},
        {"source_chars": True},
        {"source_revision": "invalid"},
        {"lexical_chunk_id": "0" * 64},
        {"lexical_chunk_id": None},
    ),
)
def test_malformed_or_unverifiable_indexed_span_is_skipped(change):
    item = indexed_item("文" * 100, 0, 100)
    item = replace(item, metadata={**item.metadata, **change})
    packed = pack_native_evidence(MemoryQuery(SCOPE, "文", token_budget=256), (), (item,))
    assert packed.items == ()


def test_randomized_unicode_indexed_offsets_and_budgets():
    rng = random.Random(20261009)
    for number in range(200):
        alphabet = "a文ß🙂\x00 e\u0301"
        prefix = "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 300)))
        body = "".join(rng.choice(alphabet) for _ in range(rng.randint(300, 850))) + "雪鸮 amber-47"
        original = prefix + body + " end"
        retrieved = indexed_item(
            original, len(prefix), len(prefix) + len(body), identity=str(number)
        )
        budget = rng.choice((64, 96, 128, 256))
        packed = pack_native_evidence(
            MemoryQuery(SCOPE, "雪鸮 amber-47", token_budget=budget), (), (retrieved,)
        )
        assert packed.items
        assert packed.tokens <= budget
        delivered = packed.items[0]
        assert "amber-47" in delivered.text
        assert_source_lineage(delivered, retrieved, original)


def test_indexed_event_can_narrow_after_state_consumes_most_of_initial_budget():
    from test_evidence_coverage_packing import claim

    source = "padding " * 80 + "risk"
    retrieved = indexed_item(source, 0, len(source))
    state = replace(claim("state", "owner risk " + "x" * 780, sources=("event",)), scope=SCOPE)
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "owner risk", token_budget=256), (state,), (retrieved,)
    )
    assert packed.state == (state,)
    assert packed.items and packed.items[0].text != retrieved.text
    assert "risk" in packed.items[0].text
    assert_source_lineage(packed.items[0], retrieved, source)
    assert packed.tokens <= 256


def test_claim_chunks_are_never_narrowed_by_native_packing():
    source = "文" * 200
    event_item = indexed_item(source, 0, len(source))
    chunk_id = sha256(
        dumps(
            ["claim_versions", "claim-revision", event_item.metadata["source_revision"], 0, 200]
        ).encode()
    ).hexdigest()
    claim_item = replace(
        event_item,
        kind=MemoryKind.CLAIM,
        metadata={
            **event_item.metadata,
            "source_revision_id": "claim-revision",
            "lexical_chunk_id": chunk_id,
        },
    )
    small = pack_native_evidence(MemoryQuery(SCOPE, "文", token_budget=64), (), (claim_item,))
    assert small.items == ()
    large = pack_native_evidence(MemoryQuery(SCOPE, "文", token_budget=256), (), (claim_item,))
    assert large.items == (claim_item,)


def test_available_full_source_hash_must_match_even_when_locator_hash_is_well_formed():
    item = indexed_item("文" * 100, 0, 100)
    revision = "0" * 64
    chunk_id = sha256(dumps(["events", item.id, revision, 0, 100]).encode()).hexdigest()
    forged = replace(
        item,
        metadata={
            **item.metadata,
            "source_revision": revision,
            "lexical_chunk_id": chunk_id,
        },
    )
    packed = pack_native_evidence(MemoryQuery(SCOPE, "文", token_budget=256), (), (forged,))
    assert packed.items == ()


@pytest.mark.parametrize("source,query", (("Nobody approves launch.", "no"), ("roadxxxx", "road")))
def test_full_fitting_chunk_is_preserved_even_if_a_slice_creates_a_query_token(source, query):
    original = indexed_item(source, 0, len(source))
    packed = pack_native_evidence(MemoryQuery(SCOPE, query, token_budget=64), (), (original,))
    assert packed.items == (original,)
    assert "retrieved_source_span" not in packed.items[0].metadata
