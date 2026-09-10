import asyncio

from agent_memory_sdk import EmbeddedMemoryClient

from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryScope
from agent_memory.mcp import MCPRequestContext


def test_embedded_client_uses_public_contract(tmp_path) -> None:
    async def scenario() -> None:
        client = EmbeddedMemoryClient(
            build_local_kernel(tmp_path / "memory.db"),
            MCPRequestContext(MemoryScope(tenant_id="sdk", session_id="session-1")),
        )
        await client.initialize()
        receipt = await client.ingest("sdk.test", "An event", idempotency_key="one")
        assert receipt["duplicate"] is False
        block = await client.write_block(
            title="SDK contract",
            content="The embedded client exposes memory blocks.",
            event_ids=[receipt["event_id"]],
            token_budget=64,
        )
        block_id = block["block"]["id"]
        assert (await client.read_block(block_id))["block"]["id"] == block_id
        assert (await client.search_blocks("embedded"))["blocks"][0]["id"] == block_id
        assert (await client.forget_block(block_id))["affected_artifacts"] == 1
        assert (await client.capabilities())["protocol_version"] == "0.1"

    asyncio.run(scenario())
