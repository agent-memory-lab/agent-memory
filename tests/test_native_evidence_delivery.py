"""Evidence survives the native packing -> host serialization boundary."""

import asyncio
import importlib.util
from datetime import UTC, datetime
from hashlib import sha256
from json import dumps
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_memory import MemoryItem, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.evaluation.comparison import AgentMemoryComparisonAdapter
from agent_memory.retrieval.candidate_identity import candidate_identity
from agent_memory.retrieval.packing import ordered_bundle_evidence, pack_native_evidence

SPEC = importlib.util.spec_from_file_location(
    "delivery_protocol", Path(__file__).parents[1] / "examples/memory_benchmark_protocol.py"
)
PROTOCOL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROTOCOL)
NOW = datetime(2026, 10, 11, tzinfo=UTC)
SCOPE = MemoryScope("delivery", user_id="reader", session_id="one")


def event(source, content, *, start=0, full=None, score=1.0):
    full = content if full is None else full
    revision = sha256(full.encode()).hexdigest()
    end = start + len(content)
    locator = sha256(dumps(["events", source, revision, start, end]).encode()).hexdigest()
    return MemoryItem(source, MemoryKind.EVENT, content, score, NOW, {
        "source_event_ids": (source,), "source_revision": revision,
        "source_chars": len(full), "lexical_chunk_id": locator,
        "source_span": {"start": start, "end": end, "unit": "characters"},
    })


def bundle(items, order):
    return SimpleNamespace(
        relevant_memories=tuple(item for item in items if item.kind == MemoryKind.EVENT),
        episodes=tuple(item for item in items if item.kind == MemoryKind.EPISODE),
        procedures=(),
        retrieval_metadata={"citation_spans": tuple(
            {"candidate_id": candidate_identity(item)} for item in order
        )},
    )


def test_cross_kind_order_preserves_packing_and_distinct_spans_of_one_source():
    text = "first passage; second passage"
    first = event("same-source", text[:14], full=text, score=0.99)
    second = event("same-source", text[14:], start=14, full=text, score=0.98)
    episode = MemoryItem("episode", MemoryKind.EPISODE, "complementary history", 0.9, NOW)
    result = bundle((first, second, episode), (first, episode, second))
    result.retrieval_metadata["citation_spans"] += ({"candidate_id": "unreturned-secret"},)
    assert ordered_bundle_evidence(result) == (first, episode, second)

    async def recall(*_args, **_kwargs):
        return result

    rows = asyncio.run(AgentMemoryComparisonAdapter(SimpleNamespace(recall=recall)).search("query"))
    assert [row["text"] for row in rows] == [first.text, episode.text, second.text]


def test_legacy_provider_without_selection_proofs_keeps_score_order():
    first = event("lower", "first", score=0.2)
    second = event("higher", "second", score=0.8)
    result = bundle((first, second), ())
    assert ordered_bundle_evidence(result) == (second, first)


@pytest.mark.parametrize("phrase", ("quartz records ", "石英记录 ", "🧪 quartz 石英 "))
def test_host_byte_budget_is_satisfied_before_shared_final_guard(phrase):
    limit, byte_budget = 8, 8192
    candidates = tuple(event(
        f"source-{number}", (phrase * 500)[:2816], score=1.0 - number * 0.03,
    ) for number in range(12))
    query = MemoryQuery(SCOPE, "quartz 石英 records", limit=limit,
                        token_budget=(byte_budget - 3 * limit) // 4,
                        include_current_state=False)
    packed = pack_native_evidence(query, (), candidates)
    rows = [{"text": item.text} for item in packed.items]
    assert len(rows) >= 2
    expected = "".join(("- " if index == 0 else "\n- ") + row["text"]
                       for index, row in enumerate(rows))
    assert len(expected.encode()) <= byte_budget
    assert PROTOCOL.bounded_evidence(rows, limit=limit, byte_budget=byte_budget) == expected
    for item, proof in zip(packed.items, packed.metadata["citation_spans"], strict=True):
        start, end = proof["source_span"]["start"], proof["source_span"]["end"]
        original = next(value for value in candidates if value.id == item.id)
        assert original.text[start:end] == item.text
