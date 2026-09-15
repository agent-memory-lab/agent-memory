from __future__ import annotations

from typing import Any

from mcp.server import MCPServer
from mcp.server.mcpserver import Context

from agent_memory.capture_api import submit_capture
from agent_memory.capture_sink import CaptureError, CaptureSink
from agent_memory.lifecycle import LifecycleEventError
from agent_memory.mcp import MCPMemoryTools, MCPToolError
from agent_memory.ports import MemoryProvider
from agent_memory.serialization import to_jsonable

from .identity import IdentityResolver


def create_server(
    provider: MemoryProvider,
    identity_resolver: IdentityResolver,
    *,
    name: str = "Agent Memory",
    capture_sink: CaptureSink | None = None,
) -> MCPServer:
    """Create an MCP v2 server while retaining one core business contract."""
    server = MCPServer(name)
    tools = MCPMemoryTools(provider)

    async def call(ctx: Context, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        identity = await identity_resolver.resolve(ctx)
        try:
            return await tools.call_tool(name, arguments, identity)
        except MCPToolError as error:
            raise RuntimeError(error.to_transport()) from None

    @server.tool()
    async def memory_ingest(
        ctx: Context,
        event_type: str,
        content: str,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        source_uri: str | None = None,
    ) -> dict[str, Any]:
        """Append an immutable event and derive trusted memory."""
        return await call(
            ctx,
            "memory_ingest",
            {
                "event_type": event_type,
                "content": content,
                "metadata": metadata or {},
                "idempotency_key": idempotency_key,
                "source_uri": source_uri,
            },
        )

    @server.tool()
    async def memory_retrieve(
        ctx: Context,
        text: str,
        limit: int = 8,
        token_budget: int = 1200,
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        """Retrieve a state-first, citation-backed memory bundle."""
        arguments: dict[str, Any] = {"text": text, "limit": limit, "token_budget": token_budget}
        if channels is not None:
            arguments["channels"] = channels
        return await call(ctx, "memory_retrieve", arguments)

    @server.tool()
    async def memory_get_state(ctx: Context) -> dict[str, Any]:
        """Return active Current State claims."""
        return await call(ctx, "memory_get_state", {})

    @server.tool()
    async def memory_propose(
        ctx: Context,
        key: str,
        value: Any,
        text: str,
        source_event_ids: list[str],
        expected_version: int,
        scope_level: str = "session",
        confidence: float = 1.0,
        importance: float = 0.5,
    ) -> dict[str, Any]:
        """Propose an evidence-backed optimistic Current State update."""
        return await call(
            ctx,
            "memory_propose",
            {
                "key": key,
                "value": value,
                "text": text,
                "source_event_ids": source_event_ids,
                "expected_version": expected_version,
                "scope_level": scope_level,
                "confidence": confidence,
                "importance": importance,
            },
        )

    @server.tool()
    async def memory_forget(
        ctx: Context,
        memory_ids: list[str] | None = None,
        all_in_scope: bool = False,
        mode: str = "archive",
    ) -> dict[str, Any]:
        """Archive memory or perform an identity-authorized legal erase."""
        return await call(
            ctx,
            "memory_forget",
            {
                "memory_ids": memory_ids or [],
                "all_in_scope": all_in_scope,
                "mode": mode,
            },
        )

    @server.tool()
    async def memory_capabilities(ctx: Context) -> dict[str, Any]:
        """Return protocol, schema, provider, and capability information."""
        return await call(ctx, "memory_capabilities", {})

    @server.resource("memory://state/{view}")
    async def current_state_resource(view: str, ctx: Context) -> dict[str, Any]:
        """Current State as a host-loadable MCP resource."""
        if view != "current":
            raise ValueError("state resource view must be 'current'")
        return await call(ctx, "memory_get_state", {})

    @server.resource("memory://capabilities/{view}")
    async def capabilities_resource(view: str, ctx: Context) -> dict[str, Any]:
        """Provider capability manifest as an MCP resource."""
        if view != "manifest":
            raise ValueError("capability resource view must be 'manifest'")
        return await call(ctx, "memory_capabilities", {})

    if capture_sink is not None:
        @server.tool()
        async def memory_capture(ctx: Context, event: dict[str, Any]) -> dict[str, Any]:
            """Capture a lifecycle event under the authenticated request scope."""

            identity = await identity_resolver.resolve(ctx)
            try:
                submission = await submit_capture(
                    event,
                    sink=capture_sink,
                    scope=identity.scope,
                    actor=identity.actor,
                )
                return to_jsonable(submission)
            except (CaptureError, LifecycleEventError) as error:
                code = error.code if isinstance(error, CaptureError) else "capture_invalid_event"
                raise RuntimeError(
                    MCPToolError(str(error), code=code, field=error.field).to_transport()
                ) from None
            except Exception:
                raise RuntimeError(
                    MCPToolError("capture storage failed", code="capture_storage_failed").to_transport()
                ) from None

    return server
