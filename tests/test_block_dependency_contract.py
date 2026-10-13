"""Block provenance updates and current bitemporal membership, on both backends."""

import asyncio
from dataclasses import replace

import pytest
from agent_memory_sdk import EmbeddedMemoryClient
from test_bitemporal_claims import assert_fact, at
from test_bitemporal_claims import store as temporal_store

from agent_memory import ForgetMode, ForgetRequest, MemoryBlock, MemoryQuery
from agent_memory.mcp import MCPMemoryTools, MCPRequestContext

store = temporal_store


def block_for(memory, receipt, title="Current owner"):
    return MemoryBlock(
        memory.scope, title, "Mira is the owner", (receipt.event_id,),
        source_claim_ids=receipt.claim_ids,
    )


async def visible(memory, block):
    kernel, scope = memory.provider, memory.scope
    read = await kernel.read_block(scope, block.id)
    search = await kernel.search_blocks(scope, "owner")
    bundle = await kernel.retrieve(MemoryQuery(scope, "owner"))
    return read is not None, any(b.id == block.id for b in search), any(
        b.id == block.id for b in bundle.relevant_memories
    )


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
@pytest.mark.parametrize("interface", ["mcp", "sdk", "domain"])
def test_omitted_dependencies_survive_update_and_claim_forget(store, mode, interface):
    async def scenario():
        async with store() as (memory, clock):
            receipt = await assert_fact(memory, clock, "Mira", 1, 1, key="owner")
            kernel, scope = memory.provider, memory.scope
            tools = MCPMemoryTools(kernel)
            context = MCPRequestContext(scope)
            # Explicit dependencies traverse the same public contract as updates.
            created = (await tools.call_tool("memory_block_write", {
                "title": "Current owner", "content": "Mira is the owner",
                "event_ids": [receipt.event_id], "source_claim_ids": list(receipt.claim_ids),
            }, context))["block"]
            arguments = dict(
                block_id=created["id"], title="Updated owner",
                content="The owner is Mira", event_ids=[receipt.event_id], expected_version=1,
            )
            if interface == "mcp":
                updated = (await tools.call_tool("memory_block_write", arguments, context))["block"]
                assert updated["source_claim_ids"] == list(receipt.claim_ids)
            elif interface == "sdk":
                client = EmbeddedMemoryClient(kernel, context)
                updated = (await client.write_block(**arguments))["block"]
                assert updated["source_claim_ids"] == list(receipt.claim_ids)
            else:
                updated = await kernel.write_block(MemoryBlock(
                    scope, arguments["title"], arguments["content"], (receipt.event_id,),
                    id=created["id"],
                ), expected_version=1)
                assert updated.source_claim_ids == receipt.claim_ids
            current = await kernel.read_block(scope, created["id"])
            assert current.source_claim_ids == receipt.claim_ids
            # A normal serialization-style reconstruction keeps the dependency set.
            await kernel.write_block(replace(current, title="Round trip owner"), expected_version=2)
            await kernel.forget(ForgetRequest(scope, receipt.claim_ids, mode=mode))
            assert await kernel.read_block(scope, created["id"]) is None
            assert not await kernel.search_blocks(scope, "owner")
    asyncio.run(scenario())


def test_empty_update_cannot_strip_claim_dependencies_and_cas_precedes_merge(store):
    async def scenario():
        async with store() as (memory, clock):
            receipt = await assert_fact(memory, clock, "Mira", 1, 1)
            kernel = memory.provider
            block = await kernel.write_block(block_for(memory, receipt))
            with pytest.raises(ValueError, match="cannot clear"):
                await kernel.write_block(replace(block, source_claim_ids=()), expected_version=1)
            fresh = await kernel.write_block(replace(block, title="Current owner renamed"), 1)
            with pytest.raises(RuntimeError, match="version conflict"):
                await kernel.write_block(replace(block, source_claim_ids=None), 1)
            assert (await kernel.read_block(memory.scope, block.id)).source_claim_ids == (
                fresh.source_claim_ids
            )
    asyncio.run(scenario())


def test_future_replacement_and_temporary_override_restore_current_block(store):
    async def scenario():
        async with store() as (memory, clock):
            receipt = await assert_fact(memory, clock, "Mira", 1, 1, key="owner")
            kernel = memory.provider
            block = await kernel.write_block(block_for(memory, receipt))
            replacement = await assert_fact(memory, clock, "Kai", 5, 2, end=7, key="owner")
            assert [c.value for c in await kernel.get_state(memory.scope)] == ["Mira"]
            assert await visible(memory, block) == (True, True, True)
            await kernel.write_block(block_for(memory, receipt, "Owner copy before takeover"))
            with pytest.raises(ValueError, match="artifact dependencies"):
                await kernel.write_block(block_for(memory, replacement))
            clock[0] = at(5)
            assert await visible(memory, block) == (False, False, False)
            with pytest.raises(ValueError, match="artifact dependencies"):
                await kernel.write_block(block_for(memory, receipt))
            clock[0] = at(7)
            assert [c.value for c in await kernel.get_state(memory.scope)] == ["Mira"]
            assert await visible(memory, block) == (True, True, True)
            await kernel.write_block(block_for(memory, receipt, "Restored owner copy"))
            # Neither invalidity interval nor restoring visibility erases history.
            previous = await kernel.get_state_at(memory.scope, valid_at=at(5), known_at=at(6))
            assert [c.value for c in previous] == ["Kai"]
    asyncio.run(scenario())


def test_expiry_hides_blocks_and_rejects_publication_at_the_half_open_boundary(store):
    async def scenario():
        async with store() as (memory, clock):
            receipt = await assert_fact(memory, clock, "Mira", 1, 1, end=3, key="owner")
            kernel = memory.provider
            block = await kernel.write_block(block_for(memory, receipt))
            assert await visible(memory, block) == (True, True, True)
            clock[0] = at(3)
            assert await kernel.get_state(memory.scope) == ()
            assert await visible(memory, block) == (False, False, False)
            for candidate, version in ((block_for(memory, receipt), 0), (block, 1)):
                with pytest.raises(ValueError, match="artifact dependencies"):
                    await kernel.write_block(candidate, version)
            assert len(await kernel.get_state_at(
                memory.scope, valid_at=at(2), known_at=at(2)
            )) == 1
    asyncio.run(scenario())


def test_publication_checks_time_after_awaiting_the_write_transaction(store, monkeypatch):
    async def scenario():
        async with store() as (memory, clock):
            receipt = await assert_fact(memory, clock, "Mira", 1, 1, end=3)
            kernel = memory.provider
            uow_type = type(kernel._repository.unit_of_work())
            original = uow_type.events_exist

            async def cross_boundary(self, *args, **kwargs):
                result = await original(self, *args, **kwargs)
                clock[0] = at(3)
                return result

            monkeypatch.setattr(uow_type, "events_exist", cross_boundary)
            with pytest.raises(ValueError, match="artifact dependencies"):
                await kernel.write_block(block_for(memory, receipt))
    asyncio.run(scenario())


def test_explicit_claim_replacement_is_validated_and_old_clients_preserve_it(store):
    async def scenario():
        async with store() as (memory, clock):
            receipt = await assert_fact(memory, clock, "Mira", 1, 1, key="owner")
            kernel = memory.provider
            block = await kernel.write_block(block_for(memory, receipt))
            replacement = await assert_fact(memory, clock, "Kai", 2, 2, key="owner")
            assert not any(await visible(memory, block))
            updated = await kernel.write_block(replace(
                block, content="Kai is the owner", event_ids=(replacement.event_id,),
                source_claim_ids=replacement.claim_ids,
            ), expected_version=1)
            assert updated.source_claim_ids == replacement.claim_ids
            client = EmbeddedMemoryClient(kernel, MCPRequestContext(memory.scope))
            roundtrip = (await client.write_block(
                title=updated.title, content=updated.content, event_ids=list(updated.event_ids),
                block_id=updated.id, expected_version=2,
            ))["block"]
            assert roundtrip["source_claim_ids"] == list(replacement.claim_ids)
            # Erasing an old unrelated claim leaves the intentionally replaced block live.
            await kernel.forget(ForgetRequest(
                memory.scope, receipt.claim_ids, mode=ForgetMode.ERASE
            ))
            assert await kernel.read_block(memory.scope, updated.id) is not None
            await kernel.forget(ForgetRequest(
                memory.scope, replacement.claim_ids, mode=ForgetMode.ERASE
            ))
            assert await kernel.read_block(memory.scope, updated.id) is None
    asyncio.run(scenario())
