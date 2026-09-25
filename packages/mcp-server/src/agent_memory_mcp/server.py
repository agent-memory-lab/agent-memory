from __future__ import annotations

from typing import Any

from mcp.server import MCPServer
from mcp.server.mcpserver import Context

from agent_memory.capture_api import submit_capture
from agent_memory.capture_sink import CaptureError, CaptureSink
from agent_memory.deletion_audit import DeletionAuditService
from agent_memory.lifecycle import LifecycleEventError
from agent_memory.memory_doctor import MemoryDoctorProvider
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
    doctor: MemoryDoctorProvider | None = None,
    deletion_auditor: DeletionAuditService | None = None,
    ontology=None,
) -> MCPServer:
    """Create an MCP v2 server while retaining one core business contract."""
    server = MCPServer(name)
    tools = MCPMemoryTools(
        provider,
        doctor=doctor,
        deletion_auditor=deletion_auditor,
        ontology=ontology,
    )

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

    if doctor is not None:
        @server.tool()
        async def memory_doctor(ctx: Context) -> dict[str, Any]:
            """Run bounded, read-only diagnostics for the authenticated scope."""
            return await call(ctx, "memory_doctor", {})

        @server.tool()
        async def memory_repair_plan(ctx: Context) -> dict[str, Any]:
            """Return approval-gated repair recommendations without applying them."""
            return await call(ctx, "memory_repair_plan", {})

    if deletion_auditor is not None:
        @server.tool()
        async def memory_deletion_audit(ctx: Context, limit: int = 100) -> dict[str, Any]:
            """Return signed deletion receipts for the authenticated scope."""
            return await call(ctx, "memory_deletion_audit", {"limit": limit})

    if ontology is not None:
        @server.tool()
        async def memory_ontology_status(ctx: Context) -> dict[str, Any]:
            """Read the authenticated scope's ontology activation and versions."""
            return await call(ctx, "memory_ontology_status", {})

        @server.tool()
        async def memory_ontology_search(ctx: Context, text: str, limit: int = 8) -> dict[str, Any]:
            """Search evidence-backed assertions in the active ontology version."""
            return await call(ctx, "memory_ontology_search", dict(text=text, limit=limit))

        @server.tool()
        async def memory_ontology_assertions(ctx: Context, ids: list[str]) -> dict[str, Any]:
            """Read active assertions by exact ID under authenticated scope."""
            return await call(ctx, "memory_ontology_assertions", dict(ids=ids))

        @server.tool()
        async def memory_ontology_graph(ctx: Context, start_entity: str,
            target_entity: str | None = None, max_depth: int = 2, max_nodes: int = 32,
            max_edges: int = 64, token_budget: int = 1200,
            predicates: list[str] | None = None, direction: str = "outgoing") -> dict[str, Any]:
            """Run a bounded, evidence-backed relation traversal."""
            args = dict(start_entity=start_entity, max_depth=max_depth, max_nodes=max_nodes,
                max_edges=max_edges, token_budget=token_budget, direction=direction)
            if target_entity is not None:
                args["target_entity"] = target_entity
            if predicates is not None:
                args["predicates"] = predicates
            return await call(ctx, "memory_ontology_graph", args)

        if ontology.switch_policy is not None:
            @server.tool()
            async def memory_ontology_switch(ctx: Context, version: str,
                expected_generation: int, reason: str, action: str = "activate") -> dict[str, Any]:
                """Request a version switch through the configured host policy."""
                return await call(ctx, "memory_ontology_switch", dict(version=version,
                    expected_generation=expected_generation, reason=reason, action=action))

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
