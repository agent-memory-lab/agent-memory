from __future__ import annotations

from typing import Any

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.tools import Tool

from agent_memory.capture.api import submit_capture
from agent_memory.capture.sink import CaptureError, CaptureSink
from agent_memory.lifecycle import LifecycleEventError
from agent_memory.mcp import MCPMemoryTools, MCPToolError
from agent_memory.operations.deletion_audit import DeletionAuditService
from agent_memory.operations.doctor import MemoryDoctorProvider
from agent_memory.operations.retention import RetentionError
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
    recovery_tools=None,
    durable_capture=None,
    derived=None,
    questions=None,
) -> MCPServer:
    """Create an MCP v2 server while retaining one core business contract."""
    if recovery_tools is not None and (
        capture_sink is not None
        or durable_capture is not None
        or derived is not None
        or questions is not None
        or recovery_tools.memory.provider is not provider
    ):
        raise ValueError("recovery requires its own provider and capture gate")
    tools = MCPMemoryTools(
        provider,
        doctor=doctor,
        deletion_auditor=deletion_auditor,
        ontology=ontology,
        derived=derived,
        questions=questions,
    )

    contract = {spec["name"]: spec for spec in tools.list_tools()}
    registered_tools: list[Tool] = []

    class ContractTool(Tool):
        async def run(self, arguments, context, convert_result=False):
            # Validate raw JSON before MCP's JSON-in-string pre-parsing and
            # Pydantic coercion can change the meaning of a feedback/write request.
            try:
                tools.validate_tool_arguments(self.name, arguments)
            except MCPToolError as error:
                raise ToolError(error.to_transport()) from None
            return await super().run(arguments, context, convert_result=convert_result)

    def register_tool():
        def register(function):
            tool = ContractTool.from_function(function)
            # MCP normally ignores unknown function arguments. Fail closed instead,
            # so an unsupported temporal filter can never become a current query.
            tool.fn_metadata.arg_model.model_config["extra"] = "forbid"
            tool.fn_metadata.arg_model.model_rebuild(force=True)
            tool.parameters = tool.fn_metadata.arg_model.model_json_schema(by_alias=True)
            if tool.name in contract:
                spec = contract[tool.name]
                properties = set(spec["inputSchema"]["properties"])
                wrapper_properties = set(tool.parameters["properties"])
                # The wrapper keeps these fields to return the core's explicit
                # unsupported_capability error even when they are not advertised.
                hidden_temporal = (
                    {"valid_at", "known_at"}
                    if tool.name == "memory_retrieve" and not properties & {"valid_at", "known_at"}
                    else set()
                )
                if (
                    wrapper_properties != properties | hidden_temporal
                    or not set(tool.parameters.get("required", ()))
                    <= set(spec["inputSchema"]["required"])
                ):
                    raise ValueError(f"MCP wrapper does not match the core contract: {tool.name}")
                tool.parameters = spec["inputSchema"]
                tool.description = spec["description"]
            registered_tools.append(tool)
            return function

        return register

    async def call(ctx: Context, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        identity = await identity_resolver.resolve(ctx)
        try:
            if recovery_tools is not None:
                return await recovery_tools.legacy(tools, name, arguments, identity)
            return await tools.call_tool(name, arguments, identity)
        except MCPToolError as error:
            raise ToolError(error.to_transport()) from None

    @register_tool()
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

    @register_tool()
    async def memory_retrieve(
        ctx: Context,
        text: str,
        limit: int = 8,
        token_budget: int = 1200,
        channels: list[str] | None = None,
        valid_at: str | None = None,
        known_at: str | None = None,
    ) -> dict[str, Any]:
        """Retrieve a state-first, citation-backed memory bundle."""
        arguments: dict[str, Any] = {"text": text, "limit": limit, "token_budget": token_budget}
        if channels is not None:
            arguments["channels"] = channels
        if valid_at is not None:
            arguments["valid_at"] = valid_at
        if known_at is not None:
            arguments["known_at"] = known_at
        return await call(ctx, "memory_retrieve", arguments)

    @register_tool()
    async def memory_get_state(ctx: Context) -> dict[str, Any]:
        """Return active Current State claims."""
        return await call(ctx, "memory_get_state", {})

    @register_tool()
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

    @register_tool()
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

    @register_tool()
    async def memory_capabilities(ctx: Context) -> dict[str, Any]:
        """Return protocol, schema, provider, and capability information."""
        return await call(ctx, "memory_capabilities", {})

    if "memory_block_read" in contract:
        @register_tool()
        async def memory_block_read(ctx: Context, block_id: str) -> dict[str, Any]:
            return await call(ctx, "memory_block_read", {"block_id": block_id})

        @register_tool()
        async def memory_block_write(
            ctx: Context,
            title: str,
            content: str,
            event_ids: list[str],
            block_id: str | None = None,
            channel: str = "semantic",
            scope_level: str = "session",
            token_budget: int = 256,
            expected_version: int = 0,
            status: str = "active",
            metadata: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return await call(ctx, "memory_block_write", {
                "title": title, "content": content, "event_ids": event_ids,
                "block_id": block_id, "channel": channel, "scope_level": scope_level,
                "token_budget": token_budget, "expected_version": expected_version,
                "status": status, "metadata": metadata or {},
            })

        @register_tool()
        async def memory_block_search(
            ctx: Context, text: str, limit: int = 8, channels: list[str] | None = None,
        ) -> dict[str, Any]:
            arguments: dict[str, Any] = {"text": text, "limit": limit}
            if channels is not None:
                arguments["channels"] = channels
            return await call(ctx, "memory_block_search", arguments)

        @register_tool()
        async def memory_block_forget(
            ctx: Context, block_id: str, mode: str = "archive",
        ) -> dict[str, Any]:
            return await call(ctx, "memory_block_forget", {"block_id": block_id, "mode": mode})

    if "memory_record_decision" in contract:
        @register_tool()
        async def memory_record_decision(
            ctx: Context,
            action: str,
            record_id: str | None = None,
            memory_ids: list[str] | None = None,
            procedure_ids: list[str] | None = None,
            run_id: str | None = None,
            bundle_id: str | None = None,
            memory_usage: str = "unknown",
            policy_version: str = "trusted-default",
            context_hash: str = "",
            idempotency_key: str | None = None,
            corrects_id: str | None = None,
        ) -> dict[str, Any]:
            return await call(ctx, "memory_record_decision", {
                "action": action, "record_id": record_id, "memory_ids": memory_ids or [],
                "procedure_ids": procedure_ids or [], "run_id": run_id, "bundle_id": bundle_id,
                "memory_usage": memory_usage, "policy_version": policy_version,
                "context_hash": context_hash, "idempotency_key": idempotency_key,
                "corrects_id": corrects_id,
            })

        @register_tool()
        async def memory_record_outcome(
            ctx: Context,
            decision_id: str,
            outcome: str,
            success: bool | None,
            record_id: str | None = None,
            score: float | None = None,
            metrics: dict[str, Any] | None = None,
            run_id: str | None = None,
            termination_reason: str | None = None,
            outcome_status: str | None = None,
            idempotency_key: str | None = None,
            corrects_id: str | None = None,
        ) -> dict[str, Any]:
            return await call(ctx, "memory_record_outcome", {
                "decision_id": decision_id, "outcome": outcome, "success": success,
                "record_id": record_id, "score": score, "metrics": metrics or {},
                "run_id": run_id, "termination_reason": termination_reason,
                "outcome_status": outcome_status, "idempotency_key": idempotency_key,
                "corrects_id": corrects_id,
            })

        @register_tool()
        async def memory_record_evaluation(
            ctx: Context,
            outcome_id: str,
            evaluator_id: str,
            evaluator_version: str,
            rubric_id: str,
            rubric_version: str,
            metrics: dict[str, Any],
            evidence_digest: str,
            record_id: str | None = None,
            idempotency_key: str | None = None,
            corrects_id: str | None = None,
        ) -> dict[str, Any]:
            return await call(ctx, "memory_record_evaluation", {
                "outcome_id": outcome_id, "evaluator_id": evaluator_id,
                "evaluator_version": evaluator_version, "rubric_id": rubric_id,
                "rubric_version": rubric_version, "metrics": metrics,
                "evidence_digest": evidence_digest, "record_id": record_id,
                "idempotency_key": idempotency_key, "corrects_id": corrects_id,
            })

        @register_tool()
        async def memory_record_reward(
            ctx: Context,
            outcome_id: str,
            value: float,
            formula_version: str,
            record_id: str | None = None,
            evaluation_id: str | None = None,
            reward_definition_id: str = "legacy",
            components: dict[str, Any] | None = None,
            idempotency_key: str | None = None,
            corrects_id: str | None = None,
        ) -> dict[str, Any]:
            return await call(ctx, "memory_record_reward", {
                "outcome_id": outcome_id, "value": value, "formula_version": formula_version,
                "record_id": record_id, "evaluation_id": evaluation_id,
                "reward_definition_id": reward_definition_id, "components": components or {},
                "idempotency_key": idempotency_key, "corrects_id": corrects_id,
            })

        @register_tool()
        async def memory_feedback_status(
            ctx: Context, record_id: str, record_type: str,
        ) -> dict[str, Any]:
            return await call(ctx, "memory_feedback_status", {
                "record_id": record_id, "record_type": record_type,
            })

    if doctor is not None:
        @register_tool()
        async def memory_doctor(ctx: Context) -> dict[str, Any]:
            """Run bounded, read-only diagnostics for the authenticated scope."""
            return await call(ctx, "memory_doctor", {})

        @register_tool()
        async def memory_repair_plan(ctx: Context) -> dict[str, Any]:
            """Return approval-gated repair recommendations without applying them."""
            return await call(ctx, "memory_repair_plan", {})

    if deletion_auditor is not None:
        @register_tool()
        async def memory_deletion_audit(ctx: Context, limit: int = 100) -> dict[str, Any]:
            """Return signed deletion receipts for the authenticated scope."""
            return await call(ctx, "memory_deletion_audit", {"limit": limit})

    if ontology is not None:
        @register_tool()
        async def memory_ontology_status(ctx: Context) -> dict[str, Any]:
            """Read the authenticated scope's ontology activation and versions."""
            return await call(ctx, "memory_ontology_status", {})

        @register_tool()
        async def memory_ontology_search(ctx: Context, text: str, limit: int = 8) -> dict[str, Any]:
            """Search evidence-backed assertions in the active ontology version."""
            return await call(ctx, "memory_ontology_search", dict(text=text, limit=limit))

        @register_tool()
        async def memory_ontology_assertions(ctx: Context, ids: list[str]) -> dict[str, Any]:
            """Read active assertions by exact ID under authenticated scope."""
            return await call(ctx, "memory_ontology_assertions", dict(ids=ids))

        @register_tool()
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
            @register_tool()
            async def memory_ontology_switch(ctx: Context, version: str,
                expected_generation: int, reason: str, action: str = "activate") -> dict[str, Any]:
                """Request a version switch through the configured host policy."""
                return await call(ctx, "memory_ontology_switch", dict(version=version,
                    expected_generation=expected_generation, reason=reason, action=action))

    if derived is not None:

        @register_tool()
        async def memory_derived(
            ctx: Context, operation: str, payload: dict[str, Any]
        ) -> dict[str, Any]:
            """Read current guarded Observations; registration and grants belong to the host."""
            return await call(ctx, "memory_derived", {"operation": operation, "payload": payload})

    if questions is not None:

        @register_tool()
        async def memory_question(
            ctx: Context, operation: str, payload: dict[str, Any]
        ) -> dict[str, Any]:
            """Read host-registered project answers or request bounded shared full refresh."""
            return await call(ctx, "memory_question", {"operation": operation, "payload": payload})

    if durable_capture is not None:
        @register_tool()
        async def memory_durable(
            ctx: Context, operation: str, payload: dict[str, Any]
        ) -> dict[str, Any]:
            """Append/revise sources or query status/cursors with a host-issued producer session."""
            identity = await identity_resolver.resolve(ctx)
            try:
                return await durable_capture.call(operation, payload, identity)
            except RetentionError as error:
                raise ToolError(MCPToolError(error.code, code=error.code).to_transport()) from None

    if capture_sink is not None:
        @register_tool()
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
                raise ToolError(
                    MCPToolError(str(error), code=code, field=error.field).to_transport()
                ) from None
            except Exception:
                raise ToolError(
                    MCPToolError(
                        "capture storage failed", code="capture_storage_failed"
                    ).to_transport()
                ) from None

    if recovery_tools is not None:
        @register_tool()
        async def memory_recovery(ctx: Context, operation: str,
                                  payload: dict[str, Any] | None = None) -> dict[str, Any]:
            """Authorized recovery: capture, receipt, save, load, compress,
            validate_compression, load_compression, enqueue, process_one, retry,
            complete, expire, cleanup, stats, runs, run_status, queue_status,
            cancel, record_compression_feedback, compression_feedback,
            history, restore, feedback_report,
            forget, or resume_deletion.
            Scope is identity-derived. Context replacement remains host-owned.
            """
            identity = await identity_resolver.resolve(ctx)
            try:
                return await recovery_tools.call(operation, payload or {}, identity)
            except MCPToolError as error:
                raise ToolError(error.to_transport()) from None

    missing = set(contract) - {tool.name for tool in registered_tools}
    if missing:
        raise ValueError(f"MCP wrappers are missing core tools: {sorted(missing)}")
    server = MCPServer(name, tools=registered_tools)

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

    return server
