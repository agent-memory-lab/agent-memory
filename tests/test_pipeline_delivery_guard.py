"""The runtime's external pipeline cannot bypass final local evidence authority."""

import asyncio
from dataclasses import replace

import pytest
from test_atom_admission import at
from test_atom_admission import store as store

from agent_memory import AgentMemory
from agent_memory.domain import (
    Citation,
    ForgetMode,
    ForgetRequest,
    MemoryBundle,
    MemoryEvent,
    MemoryItem,
    MemoryKind,
)


def bundle(kernel, *, state=(), items=(), sources=()):
    return MemoryBundle(
        current_state=tuple(state),
        relevant_memories=tuple(items),
        episodes=(),
        procedures=(),
        citations=tuple(Citation(item.id, tuple(sources)) for item in items) if sources else (),
        token_estimate=50,
        retrieval_metadata={},
        capability_snapshot=kernel.manifest().capabilities,
    )


async def add_fact(kernel, scope):
    event = MemoryEvent(
        scope,
        "fact",
        "The meeting is at Harbor Hall.",
        occurred_at=at(1),
        metadata={
            "claims": [
                {"key": "meeting", "value": "Harbor Hall", "text": "The meeting is at Harbor Hall."}
            ]
        },
    )
    receipt = await kernel.ingest_event(event)
    return event, receipt.claim_ids[0]


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_pipeline_cannot_return_current_claim_deleted_during_its_await(store, mode):
    async def run():
        async with store() as (_, kernel, scope, clock):
            _, claim_id = await add_fact(kernel, scope)
            clock[0] = at(2)

            class Pipeline:
                async def retrieve(self, query, current):
                    assert any(claim.id == claim_id for claim in current)
                    await kernel.forget(ForgetRequest(scope, (claim_id,), mode=mode))
                    return bundle(kernel, state=current)

            memory = AgentMemory(kernel, scope, recall_pipeline=Pipeline())
            await memory.initialize()
            result = await memory.recall("meeting")
            assert result.current_state == ()
            assert result.retrieval_metadata["delivery_guard"]["removed_count"] == 1
            assert await kernel.get_state(scope) == ()

    asyncio.run(run())


@pytest.mark.parametrize("mode", [None, ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_pipeline_local_event_requires_live_native_identity_even_with_external_citation(
    store, mode
):
    async def run():
        async with store() as (_, kernel, scope, clock):
            event = MemoryEvent(
                scope, "message", "Archive meeting at Harbor Hall.", occurred_at=at(1)
            )
            await kernel.ingest_event(event)
            clock[0] = at(2)

            class Pipeline:
                async def retrieve(self, query, current):
                    candidates = await kernel._repository.search(query, 10)
                    item = next(value for value in candidates if value.id == event.id)
                    if mode is not None:
                        await kernel.forget(ForgetRequest(scope, (event.id,), mode=mode))
                    return bundle(kernel, items=(item,), sources=(event.id,))

            memory = AgentMemory(kernel, scope, recall_pipeline=Pipeline())
            await memory.initialize()
            result = await memory.recall("archive meeting")
            assert bool(result.relevant_memories) == (mode is None)
            assert result.retrieval_metadata["delivery_guard"]["external_evidence_count"] == 0

    asyncio.run(run())


@pytest.mark.parametrize("change", ["none", "archive", "erase", "foreign_scope"])
def test_external_memory_keeps_resolver_content_but_rechecks_native_source(store, change):
    async def run():
        async with store() as (_, kernel, scope, clock):
            event_scope = (
                replace(scope, tenant_id="different-tenant") if change == "foreign_scope" else scope
            )
            event = MemoryEvent(
                event_scope, "message", "Supporting native source.", occurred_at=at(1)
            )
            await kernel.ingest_event(event)
            clock[0] = at(2)
            item = MemoryItem(
                "external-knowledge",
                MemoryKind.EPISODE,
                "Resolver-derived knowledge, different from source text.",
                1.0,
                at(1),
            )

            class Pipeline:
                async def retrieve(self, query, current):
                    if change in ("archive", "erase"):
                        await kernel.forget(
                            ForgetRequest(scope, (event.id,), mode=ForgetMode(change))
                        )
                    # Governed packers may carry provenance in citations only.
                    return bundle(kernel, items=(item,), sources=(event.id, "external-source"))

            memory = AgentMemory(kernel, scope, recall_pipeline=Pipeline())
            await memory.initialize()
            result = await memory.recall("knowledge")
            assert bool(result.relevant_memories) == (change == "none")
            guard = result.retrieval_metadata["delivery_guard"]
            if change == "none":
                assert result.relevant_memories[0].text == item.text
                assert guard["status"] == "mixed_authorities"
                assert guard["external_evidence_count"] == 1
                assert guard["external_content_authority"] == "configured_pipeline_resolver"
                assert not guard["external_versions_revalidated_by_native"]
            else:
                assert result.citations == () and result.token_estimate == 0

    asyncio.run(run())


def test_erased_local_claim_id_cannot_masquerade_as_external_knowledge(store):
    async def run():
        async with store() as (_, kernel, scope, clock):
            _, claim_id = await add_fact(kernel, scope)
            clock[0] = at(2)

            class Pipeline:
                async def retrieve(self, query, current):
                    await kernel.forget(ForgetRequest(scope, (claim_id,), mode=ForgetMode.ERASE))
                    item = MemoryItem(
                        claim_id, MemoryKind.EPISODE, "Stale local claim.", 1.0, at(1)
                    )
                    return bundle(kernel, items=(item,), sources=("external-source",))

            memory = AgentMemory(kernel, scope, recall_pipeline=Pipeline())
            await memory.initialize()
            result = await memory.recall("meeting")
            assert result.relevant_memories == () and result.citations == ()

    asyncio.run(run())


@pytest.mark.parametrize("provenance", ["missing", "mismatched"])
def test_pipeline_requires_consistent_source_evidence(store, provenance):
    async def run():
        async with store() as (_, kernel, scope, _):
            item = MemoryItem(
                "external-knowledge",
                MemoryKind.EPISODE,
                "External assertion.",
                1.0,
                at(1),
                {"source_event_ids": ("source-a",)} if provenance == "mismatched" else {},
            )

            class Pipeline:
                async def retrieve(self, query, current):
                    return bundle(
                        kernel,
                        items=(item,),
                        sources=("source-b",) if provenance == "mismatched" else (),
                    )

            memory = AgentMemory(kernel, scope, recall_pipeline=Pipeline())
            await memory.initialize()
            if provenance == "mismatched":
                with pytest.raises(ValueError, match="provenance disagree"):
                    await memory.recall("assertion")
            else:
                assert (await memory.recall("assertion")).relevant_memories == ()

    asyncio.run(run())


def test_pipeline_current_claim_cannot_keep_forged_extra_citation(store):
    async def run():
        async with store() as (_, kernel, scope, clock):
            await add_fact(kernel, scope)
            clock[0] = at(2)

            class Pipeline:
                async def retrieve(self, query, current):
                    return replace(
                        bundle(kernel, state=current),
                        citations=(Citation(current[0].id, ("unrelated-source",)),),
                    )

            memory = AgentMemory(kernel, scope, recall_pipeline=Pipeline())
            await memory.initialize()
            with pytest.raises(ValueError, match="provenance disagree"):
                await memory.recall("meeting")

    asyncio.run(run())


def test_pipeline_native_event_can_omit_optional_event_type_metadata(store):
    async def run():
        async with store() as (_, kernel, scope, clock):
            event = MemoryEvent(scope, "message", "Original archive meeting.", occurred_at=at(1))
            await kernel.ingest_event(event)
            clock[0] = at(2)

            class Pipeline:
                async def retrieve(self, query, current):
                    item = MemoryItem(event.id, MemoryKind.EVENT, event.content, 1.0, at(1))
                    return bundle(kernel, items=(item,), sources=(event.id,))

            memory = AgentMemory(kernel, scope, recall_pipeline=Pipeline())
            await memory.initialize()
            result = await memory.recall("archive meeting")
            assert result.relevant_memories[0].text == event.content
            assert result.relevant_memories[0].metadata["event_type"] == event.event_type
            assert result.retrieval_metadata["delivery_guard"]["status"] == "validated"

    asyncio.run(run())
