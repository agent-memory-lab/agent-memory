import asyncio

from agent_memory_mcp import StaticIdentityResolver, create_server
from mcp import Client

from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryScope
from agent_memory.mcp import MCPRequestContext


def test_in_memory_mcp_round_trip(tmp_path) -> None:
    async def scenario() -> None:
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        identity = StaticIdentityResolver(
            MCPRequestContext(
                scope=MemoryScope(tenant_id="contract", user_id="user-1", session_id="session-1")
            )
        )
        async with Client(create_server(provider, identity)) as client:
            result = await client.call_tool(
                "memory_ingest",
                {
                    "event_type": "contract.test",
                    "content": "Remember concise answers.",
                    "metadata": {
                        "claims": [
                            {
                                "key": "answer.style",
                                "value": "concise",
                                "text": "The user prefers concise answers.",
                                "scope": "user",
                            }
                        ]
                    },
                },
            )
            assert result.is_error is False
            state = await client.call_tool("memory_get_state", {})
            assert state.structured_content["current_state"][0]["key"] == "answer.style"

    asyncio.run(scenario())
