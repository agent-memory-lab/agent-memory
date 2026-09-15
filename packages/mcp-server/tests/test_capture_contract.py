import asyncio
from datetime import datetime, timezone

from agent_memory_mcp import StaticIdentityResolver, create_server
from mcp import Client

from agent_memory.capture_policy import CaptureSanitizer
from agent_memory.capture_queue import SQLiteCaptureQueue
from agent_memory.capture_sink import QueuedCaptureSink
from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryScope
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory_sdk import MCPMemoryClient


def test_mcp_capture_is_opt_in_and_uses_trusted_identity(tmp_path):
    async def scenario():
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        scope = MemoryScope(tenant_id="tenant", user_id="user", session_id="session")
        identity = StaticIdentityResolver(MCPRequestContext(scope=scope, actor="trusted"))
        queue = SQLiteCaptureQueue(tmp_path / "capture.db", sanitizer=CaptureSanitizer())
        sink = QueuedCaptureSink(queue)
        event = LifecycleEvent(
            scope=scope,
            event_id="mcp-event",
            event_type=LifecycleEventType.TOOL_COMPLETED,
            origin=LifecycleOrigin.TOOL,
            occurred_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            run_id="run",
            content="Tool completed.",
            payload={"password": "private-secret-12345678", "result": "done"},
        ).to_dict()
        event["actor"] = "spoofed"
        async with MCPMemoryClient(create_server(provider, identity, capture_sink=sink)) as client:
            receipt = await client.capture(event)
            assert receipt["status"] == "pending"
            assert (await client.capture(event))["duplicate"]
            assert (await client.try_capture({**event, "scope": {"tenant_id": "other"}}))["status"] == "skipped"
        lease = await queue.claim(scope=scope)
        assert lease.event.actor == "trusted"
        assert lease.event.payload["password"] == "[REDACTED]"
        async with Client(create_server(provider, identity)) as client:
            tools = await client.list_tools()
            assert all(tool.name != "memory_capture" for tool in tools.tools)
            existing = await client.call_tool("memory_capabilities", {})
            assert existing.is_error is False

    asyncio.run(scenario())
