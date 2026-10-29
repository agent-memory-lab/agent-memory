"""Exact source coverage survives dependency grouping and final delivery."""

import asyncio
from dataclasses import replace

import pytest
from test_indexed_event_packing import NOW, SCOPE
from test_indexed_event_packing import indexed_item as indexed_window

from agent_memory import MemoryEvent, build_local_kernel
from agent_memory.domain import MemoryBundle, MemoryCapabilities, MemoryQuery
from agent_memory.retrieval.candidate_identity import candidate_identity
from agent_memory.retrieval.evidence_units import retain_complete_groups
from agent_memory.retrieval.packing import pack_native_evidence, token_estimate


def source_text():
    return (
        "user: " + "Background detail " * 80 + ".\n"
        "user: The signal is cobalt.\nassistant: " + "Background detail " * 90 + "."
    )


def indexed_item(*args, **kwargs):
    item = indexed_window(*args, **kwargs)
    return replace(item, metadata={**item.metadata, "event_type": "conversation"})


def context_child(anchor, child):
    return replace(
        child,
        metadata={
            **child.metadata,
            "context_only": True,
            "relevance_origin": "inherited_context_anchor",
            "context_anchor_candidate_id": candidate_identity(anchor),
        },
    )


def query():
    return MemoryQuery(
        SCOPE, "cobalt", limit=8, token_budget=64, include_current_state=False, trace_enabled=False
    )


async def validate_delivery(tmp_path, source, packed):
    # The experiment reader validates citations/ordering without calling a model.
    from experiments.public_longmemeval.reader_context import core_reader_context

    kernel = build_local_kernel(tmp_path / "memory.db")
    await kernel.initialize()
    try:
        await kernel.ingest_event(
            MemoryEvent(
                SCOPE,
                "conversation",
                source,
                id="event",
                occurred_at=NOW,
            )
        )
        proposed = MemoryBundle(
            (),
            packed.items,
            (),
            (),
            packed.citations,
            packed.tokens,
            packed.metadata,
            MemoryCapabilities(),
        )
        guarded = await kernel._repository.guard_retrieval_delivery(query(), proposed)
        assert guarded.relevant_memories == packed.items
        assert guarded.retrieval_metadata["delivery_guard"]["removed_count"] == 0
        reader = core_reader_context(guarded, {"event": "source"}, token_budget=64, item_limit=8)
        assert [row["text"] for row in reader.delivered] == [item.text for item in packed.items]
        assert reader.diagnostics["evidence_estimated_tokens"] == packed.tokens
        groups = guarded.retrieval_metadata.get("packing_dependency_groups", ())
        retained, complete = retain_complete_groups(guarded.relevant_memories, groups)
        assert retained == guarded.relevant_memories and complete == tuple(groups)
    finally:
        await kernel.close()


@pytest.mark.parametrize("version", ("v2", "v4", "v5"))
def test_narrowed_group_keeps_one_exact_span_and_valid_delivery(tmp_path, version):
    source = source_text()
    anchor = indexed_item(source, 0, 2200)
    neighbor = context_child(anchor, indexed_item(source, 500, len(source)))
    assert candidate_identity(anchor) != candidate_identity(neighbor)
    packed = pack_native_evidence(query(), (), (anchor, neighbor), version=version)

    assert [item.text for item in packed.items] == ["user: The signal is cobalt."]
    assert packed.tokens == token_estimate(packed.items[0].text) == 7
    assert len(packed.citations) == 1
    assert not packed.metadata.get("packing_dependency_groups")
    span = packed.items[0].metadata["source_span"]
    assert packed.items[0].text == source[span["start"] : span["end"]]
    asyncio.run(validate_delivery(tmp_path, source, packed))


@pytest.mark.parametrize("boundary", ("source", "revision"))
def test_identical_text_from_distinct_source_or_revision_is_not_merged(boundary):
    source = source_text()
    first = indexed_item(source, 0, 2200)
    second = (
        indexed_item(source, 0, 2200, identity="other-event")
        if boundary == "source"
        else indexed_item(source[:-1] + "!", 0, 2200)
    )
    packed = pack_native_evidence(query(), (), (first, second), version="v5")

    # Artificial mixed revisions exercise packing identity only; authority must
    # decide which revision is live before any real-world delivery.
    assert len(packed.items) == 2
    assert packed.items[0].text == packed.items[1].text == "user: The signal is cobalt."
    assert len({(item.id, item.metadata["source_revision"]) for item in packed.items}) == 2
    assert packed.tokens == 14 and len(packed.citations) == 2


def test_duplicate_context_does_not_consume_an_independent_evidence_slot():
    source = source_text()
    anchor = indexed_item(source, 0, 2200)
    neighbor = context_child(anchor, indexed_item(source, 500, len(source)))
    independent_text = "user: The cobalt backup is amber."
    independent = replace(
        indexed_item(independent_text, 0, len(independent_text), identity="backup"), score=0.9
    )
    packed = pack_native_evidence(
        replace(query(), limit=2), (), (anchor, independent, neighbor), version="v5"
    )

    assert {item.id for item in packed.items} == {"event", "backup"}
    assert independent_text in {item.text for item in packed.items}
    assert packed.tokens == sum(token_estimate(item.text) for item in packed.items) <= 64


@pytest.mark.parametrize("overlap", (False, True))
def test_context_with_distinct_information_keeps_complete_source_coverage(tmp_path, overlap):
    turns = ("user: Cobalt alpha.\n", "assistant: Cobalt beta.\n", "user: Cobalt gamma.")
    source = "".join(turns)
    anchor_end = len(turns[0]) + (len(turns[1]) if overlap else 0)
    anchor = indexed_item(source, 0, anchor_end)
    neighbor = context_child(anchor, indexed_item(source, len(turns[0]), len(source)))
    packed = pack_native_evidence(query(), (), (anchor, neighbor), version="v5")

    covered = set()
    for item in packed.items:
        span = item.metadata["source_span"]
        assert item.text == source[span["start"] : span["end"]]
        covered.update(range(span["start"], span["end"]))
    assert covered == set(range(len(source)))
    assert len(packed.items) == 2 and len(packed.metadata["packing_dependency_groups"]) == 1
    assert packed.tokens == sum(token_estimate(item.text) for item in packed.items)
    asyncio.run(validate_delivery(tmp_path, source, packed))
