"""A checked sibling must never authorize another item's body or provenance."""

import asyncio
from dataclasses import replace
from hashlib import sha256
from json import dumps

import pytest
from test_atom_admission import at
from test_atom_admission import store as store

from agent_memory.domain import (
    Citation,
    ForgetMode,
    ForgetRequest,
    MemoryBundle,
    MemoryEvent,
    MemoryItem,
    MemoryKind,
    MemoryQuery,
)
from agent_memory.retrieval.candidate_identity import candidate_identity
from agent_memory.serialization import to_jsonable


def bundle(kernel, items):
    return MemoryBundle(
        current_state=(), relevant_memories=tuple(items), episodes=(), procedures=(),
        citations=(), token_estimate=100, retrieval_metadata={},
        capability_snapshot=kernel.manifest().capabilities,
    )


@pytest.mark.parametrize("shape", ["full", "indexed", "narrowed"])
@pytest.mark.parametrize("pipeline", [False, True])
def test_native_event_proof_is_bound_to_exact_sibling_text(store, shape, pipeline):
    async def run():
        async with store() as (_, kernel, scope, clock):
            event = MemoryEvent(
                scope, "message", "Archive meeting at Harbor Hall.", occurred_at=at(1),
            )
            await kernel.ingest_event(event)
            clock[0] = at(2)
            query = MemoryQuery(scope, "archive meeting", include_current_state=False,
                                trace_enabled=False)
            # Build the SQLite lexical headers through the normal retrieval path.
            await kernel.retrieve(query)
            metadata = {"event_type": event.event_type, "source_event_ids": (event.id,)}
            text = event.content
            if shape != "full":
                revision = sha256(text.encode()).hexdigest()
                original_span = {"start": 0, "end": len(text), "unit": "characters"}
                locator = sha256(
                    dumps(["events", event.id, revision, 0, len(text)]).encode()
                ).hexdigest()
                metadata.update(source_revision=revision, source_chars=len(text),
                                source_span=original_span, lexical_chunk_id=locator)
                if shape == "narrowed":
                    metadata.pop("lexical_chunk_id")
                    metadata.update(retrieved_lexical_chunk_id=locator,
                                    retrieved_source_span=original_span,
                                    source_span={"start": 0, "end": 7, "unit": "characters"})
                    text = text[:7]
            good = MemoryItem(event.id, MemoryKind.EVENT, text, 1.0, event.occurred_at, metadata)
            bad = replace(good, text="x" * len(text))
            assert candidate_identity(good) == candidate_identity(bad)
            result = await kernel._repository.guard_retrieval_delivery(
                query, bundle(kernel, (good, bad)), pipeline=pipeline,
            )
            assert result.relevant_memories == (good,)
            assert result.retrieval_metadata["delivery_guard"]["removed_count"] == 1
    asyncio.run(run())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_external_proof_is_bound_to_exact_sibling_provenance(store, mode):
    async def run():
        async with store() as (_, kernel, scope, clock):
            live = MemoryEvent(scope, "message", "Live supporting source.", occurred_at=at(1))
            gone = MemoryEvent(scope, "message", "Withdrawn supporting source.", occurred_at=at(1))
            await kernel.ingest_event(live)
            await kernel.ingest_event(gone)
            clock[0] = at(2)
            await kernel.forget(ForgetRequest(scope, (gone.id,), mode=mode))
            good = MemoryItem("external-episode", MemoryKind.EPISODE, "External assertion.",
                              1.0, at(1), {"source_event_ids": (live.id,)})
            bad = replace(good, metadata={"source_event_ids": (gone.id,)})
            proposed = bundle(kernel, (good, bad))
            proposed = replace(proposed, retrieval_metadata={
                "citation_spans": tuple({"memory_id": value.id, "candidate_id": value.id,
                                         "source_event_ids": value.metadata["source_event_ids"]}
                                        for value in (good, bad)),
                "packing_selection_trace": ({"memory_id": bad.id, "candidate_id": bad.id,
                                              "source_event_ids": (gone.id,)},),
            })
            result = await kernel.guard_pipeline_delivery(
                MemoryQuery(scope, "assertion"), proposed,
            )
            assert gone.id not in dumps(to_jsonable(result))
            assert result.relevant_memories == (good,)
            guard = result.retrieval_metadata["delivery_guard"]
            assert guard["removed_count"] == 1 and guard["external_evidence_count"] == 1
    asyncio.run(run())


def test_native_event_cannot_attribute_its_body_to_a_different_live_source(store):
    async def run():
        async with store() as (_, kernel, scope, clock):
            first = MemoryEvent(scope, "message", "Actual source body.", occurred_at=at(1))
            other = MemoryEvent(scope, "message", "Unrelated source body.", occurred_at=at(1))
            await kernel.ingest_event(first)
            await kernel.ingest_event(other)
            clock[0] = at(2)
            good = MemoryItem(first.id, MemoryKind.EVENT, first.content, 1.0, at(1),
                              {"source_event_ids": (first.id,)})
            bad = replace(good, metadata={"source_event_ids": (other.id,)})
            result = await kernel.guard_pipeline_delivery(
                MemoryQuery(scope, "source"), bundle(kernel, (good, bad)),
            )
            assert len(result.relevant_memories) == 1
            assert result.relevant_memories[0].metadata["source_event_ids"] == (first.id,)
            assert result.retrieval_metadata["delivery_guard"]["removed_count"] == 1
    asyncio.run(run())


def test_delivery_snapshot_detaches_nested_read_only_metadata():
    from types import MappingProxyType

    from agent_memory.retrieval.delivery_guard import snapshot_delivery_bundle

    sources = ["external-source"]
    metadata = MappingProxyType({"source_event_ids": sources})
    item = MemoryItem("external", MemoryKind.EPISODE, "External text.", 1.0, at(1), metadata)
    detached = snapshot_delivery_bundle(item)
    sources[:] = ["withdrawn-source"]
    assert detached.metadata["source_event_ids"] == ["external-source"]


def test_external_metadata_mutation_during_ownership_lookup_cannot_add_unchecked_source(
    store, monkeypatch,
):
    async def run():
        async with store() as (_, kernel, scope, clock):
            foreign = MemoryEvent(replace(scope, tenant_id="other-tenant"), "message",
                                  "Foreign private source.", occurred_at=at(1))
            await kernel.ingest_event(foreign)
            clock[0] = at(2)
            metadata = {"source_event_ids": ["external-source"]}
            item = MemoryItem("external-episode", MemoryKind.EPISODE, "External text.",
                              1.0, at(1), metadata)
            observed = []
            if hasattr(kernel._repository, "pool"):
                from agent_memory_postgres import delivery_guard as adapter

                original = adapter._owned_ids

                async def change_sources(connection, identities):
                    observed.append(set(identities))
                    result = await original(connection, identities)
                    metadata["source_event_ids"][:] = [foreign.id]
                    return result
            else:
                from agent_memory.retrieval import sqlite_delivery_guard as adapter

                original = adapter._owned_ids

                def change_sources(connection, identities):
                    observed.append(set(identities))
                    result = original(connection, identities)
                    metadata["source_event_ids"][:] = [foreign.id]
                    return result
            monkeypatch.setattr(adapter, "_owned_ids", change_sources)
            result = await kernel.guard_pipeline_delivery(
                MemoryQuery(scope, "external"), bundle(kernel, (item,)),
            )
            assert observed == [{"external-episode", "external-source"}]
            assert metadata["source_event_ids"] == [foreign.id]
            assert foreign.id not in dumps(to_jsonable(result))
            assert result.relevant_memories[0].metadata["source_event_ids"] == ["external-source"]
    asyncio.run(run())


def test_citations_use_surviving_provenance_without_requiring_source_order(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            good = MemoryItem("external", MemoryKind.EPISODE, "External assertion.",
                              1.0, at(1), {"source_event_ids": ("source-a", "source-b")})
            proposed = replace(bundle(kernel, (good,)), citations=(
                Citation(good.id, ("source-b", "source-a")),
                Citation(good.id, ("withdrawn-source",)),
            ))
            result = await kernel._repository.guard_retrieval_delivery(
                MemoryQuery(scope, "assertion"), proposed, pipeline=True,
            )
            assert result.citations == (proposed.citations[0],)
            assert "withdrawn-source" not in dumps(to_jsonable(result))
    asyncio.run(run())
