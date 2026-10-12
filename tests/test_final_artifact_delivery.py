"""Final-delivery guards preserve authorized artifacts with existing provenance."""

import asyncio

import pytest
from test_bitemporal_claims import store as temporal_store

from agent_memory import (
    Episode,
    MemoryBlock,
    MemoryChannel,
    MemoryEvent,
    MemoryQuery,
    Procedure,
    Provenance,
)

store = temporal_store


@pytest.mark.parametrize("kind", ["block", "episode", "procedure"])
@pytest.mark.parametrize("duplicate_sources", [False, True])
def test_live_artifacts_keep_existing_source_identity_semantics(store, kind, duplicate_sources):
    async def scenario():
        async with store() as (memory, _):
            kernel, scope = memory.provider, memory.scope
            event = MemoryEvent(scope, "message", "Banana release completed")
            await kernel.ingest_event(event)
            sources = (event.id, event.id) if duplicate_sources else (event.id,)
            provenance = Provenance(source_event_ids=sources)
            if kind == "block":
                artifact = await kernel.write_block(MemoryBlock(
                    scope, "Banana release", "Banana launch guide", sources,
                ))
                channel = MemoryChannel.SEMANTIC
                assert await kernel.read_block(scope, artifact.id) is not None
            elif kind == "episode":
                artifact = Episode(
                    scope, "Banana release", "Banana checks", "Banana completed", "Banana lesson",
                    provenance=provenance,
                )
                await kernel.record_episode(artifact)
                channel = MemoryChannel.EPISODIC
            else:
                artifact = Procedure(
                    scope, "Banana release", "Banana trigger", ("Banana checks",),
                    ("Banana passed",), provenance=provenance,
                )
                await kernel.publish_procedure(artifact)
                channel = MemoryChannel.PROCEDURAL
            query = MemoryQuery(
                scope, "Banana", channels=(channel,), include_current_state=False,
                limit=8, token_budget=1000,
            )
            candidates = await kernel._repository.search(query, 48)
            assert artifact.id in {item.id for item in candidates}
            bundle = await kernel.retrieve(query)
            delivered = (*bundle.relevant_memories, *bundle.episodes, *bundle.procedures)
            result = next(item for item in delivered if item.id == artifact.id)
            assert result.metadata["source_event_ids"] == sources
            assert bundle.retrieval_metadata["delivery_guard"]["removed_count"] == 0
            assert any(c.memory_id == artifact.id for c in bundle.citations)
    asyncio.run(scenario())
