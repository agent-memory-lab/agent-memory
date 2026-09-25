from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from typing import Any

from .domain import (
    ArtifactStatus,
    DecisionRecord,
    EvaluationRecord,
    ForgetMode,
    ForgetRequest,
    MemoryBlock,
    MemoryChannel,
    MemoryEvent,
    MemoryProposal,
    MemoryQuery,
    MemoryScope,
    MemoryUsage,
    OutcomeEvent,
    OutcomeStatus,
    RewardSignal,
    ScopeLevel,
)
from .deletion_audit import DeletionAuditError, DeletionAuditService
from .memory_doctor import MemoryDoctorProvider, build_memory_repair_plan
from .ports import MemoryProvider
from .plugins import PluginError, PluginErrorCode
from .serialization import to_jsonable

MCP_ERROR_PREFIX = "agent-memory-error:"


class MCPToolError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "invalid_request",
        field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.field = field

    @classmethod
    def from_plugin_error(cls, error: PluginError) -> MCPToolError:
        message = str(error)
        if error.code is PluginErrorCode.PLUGIN_LOAD_FAILED:
            message = "memory plugin operation failed"
        return cls(message, code=error.code.value, field=error.field)

    def to_dict(self) -> dict[str, str]:
        payload = {"code": self.code, "message": str(self)}
        if self.field is not None:
            payload["field"] = self.field
        return payload

    def to_transport(self) -> str:
        return MCP_ERROR_PREFIX + json.dumps(
            {"error": self.to_dict()},
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )


def decode_mcp_error(value: str) -> dict[str, str] | None:
    marker = value.find(MCP_ERROR_PREFIX)
    if marker < 0:
        return None
    encoded = value[marker + len(MCP_ERROR_PREFIX) :]
    try:
        decoded, _ = json.JSONDecoder().raw_decode(encoded)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(decoded, Mapping):
        return None
    payload = decoded.get("error")
    if not isinstance(payload, Mapping):
        return None
    code = payload.get("code")
    message = payload.get("message")
    field = payload.get("field")
    if not isinstance(code, str) or not isinstance(message, str):
        return None
    if field is not None and not isinstance(field, str):
        return None
    result = {"code": code, "message": message}
    if field is not None:
        result["field"] = field
    return result


@dataclass(frozen=True, slots=True)
class MCPRequestContext:
    """Trusted identity-derived context; tool arguments cannot override this scope."""

    scope: MemoryScope
    actor: str = "agent"
    can_erase: bool = False


class MCPMemoryTools:
    def __init__(
        self,
        provider: MemoryProvider,
        *,
        doctor: MemoryDoctorProvider | None = None,
        deletion_auditor: DeletionAuditService | None = None,
        ontology=None,
    ) -> None:
        self._provider = provider
        self._doctor = doctor
        self._deletion_auditor = deletion_auditor
        self._ontology = ontology

    def list_tools(self) -> tuple[dict[str, Any], ...]:
        scope_note = "Scope is derived from the authenticated request and is not an argument."
        tools = (
            self._tool(
                "memory_ingest",
                "Append an immutable event and derive trusted memory. " + scope_note,
                {
                    "event_type": {"type": "string"},
                    "content": {"type": "string"},
                    "metadata": {"type": "object"},
                    "idempotency_key": {"type": "string"},
                    "source_uri": {"type": "string"},
                },
                ("event_type", "content"),
            ),
            self._tool(
                "memory_retrieve",
                "Retrieve a state-first, citation-backed memory bundle. " + scope_note,
                {
                    "text": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "token_budget": {"type": "integer", "minimum": 64},
                    "channels": {
                        "type": "array",
                        "items": {"enum": [channel.value for channel in MemoryChannel]},
                    },
                },
                ("text",),
            ),
            self._tool(
                "memory_get_state",
                "Return active Current State claims. " + scope_note,
                {},
                (),
            ),
            self._tool(
                "memory_block_read",
                "Read one visible memory block by id. " + scope_note,
                {"block_id": {"type": "string"}},
                ("block_id",),
            ),
            self._tool(
                "memory_block_write",
                "Create or optimistically update an evidence-backed memory block. " + scope_note,
                {
                    "block_id": {"type": "string"},
                    "title": {"type": "string"},
                    "content": {"type": "string"},
                    "event_ids": {"type": "array", "items": {"type": "string"}},
                    "channel": {"enum": [channel.value for channel in MemoryChannel]},
                    "scope_level": {"enum": [level.value for level in ScopeLevel]},
                    "token_budget": {"type": "integer", "minimum": 16, "maximum": 4096},
                    "expected_version": {"type": "integer", "minimum": 0},
                    "status": {"enum": [status.value for status in ArtifactStatus]},
                    "metadata": {"type": "object"},
                },
                ("title", "content", "event_ids"),
            ),
            self._tool(
                "memory_block_search",
                "Search active memory blocks without loading unrelated memory. " + scope_note,
                {
                    "text": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    "channels": {
                        "type": "array",
                        "items": {"enum": [channel.value for channel in MemoryChannel]},
                    },
                },
                ("text",),
            ),
            self._tool(
                "memory_block_forget",
                "Archive one memory block or perform an authorized legal erase. " + scope_note,
                {
                    "block_id": {"type": "string"},
                    "mode": {"enum": [mode.value for mode in ForgetMode]},
                },
                ("block_id",),
            ),
            self._tool(
                "memory_propose",
                "Propose an evidence-backed optimistic memory update. " + scope_note,
                {
                    "key": {"type": "string"},
                    "value": {},
                    "text": {"type": "string"},
                    "source_event_ids": {"type": "array", "items": {"type": "string"}},
                    "expected_version": {"type": "integer", "minimum": 0},
                    "scope_level": {"enum": [level.value for level in ScopeLevel]},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "importance": {"type": "number", "minimum": 0, "maximum": 1},
                },
                ("key", "value", "text", "source_event_ids", "expected_version"),
            ),
            self._tool(
                "memory_forget",
                "Archive memory or perform an authorized legal erase. " + scope_note,
                {
                    "memory_ids": {"type": "array", "items": {"type": "string"}},
                    "all_in_scope": {"type": "boolean"},
                    "mode": {"enum": [mode.value for mode in ForgetMode]},
                },
                (),
            ),
            self._tool(
                "memory_record_decision",
                "Record a host decision and confirmed memory usage. " + scope_note,
                {
                    "record_id": {"type": "string"},
                    "action": {"type": "string"},
                    "memory_ids": {"type": "array", "items": {"type": "string"}},
                    "procedure_ids": {"type": "array", "items": {"type": "string"}},
                    "run_id": {"type": "string"},
                    "bundle_id": {"type": "string"},
                    "memory_usage": {"enum": [value.value for value in MemoryUsage]},
                    "policy_version": {"type": "string"},
                    "context_hash": {"type": "string"},
                    "idempotency_key": {"type": "string"},
                    "corrects_id": {"type": "string"},
                },
                ("action",),
            ),
            self._tool(
                "memory_record_outcome",
                "Record an outcome linked to a host decision. " + scope_note,
                {
                    "record_id": {"type": "string"},
                    "decision_id": {"type": "string"},
                    "outcome": {"type": "string"},
                    "success": {"type": ["boolean", "null"]},
                    "score": {"type": "number", "minimum": 0, "maximum": 1},
                    "metrics": {"type": "object"},
                    "run_id": {"type": "string"},
                    "termination_reason": {"type": "string"},
                    "outcome_status": {"enum": [value.value for value in OutcomeStatus]},
                    "idempotency_key": {"type": "string"},
                    "corrects_id": {"type": "string"},
                },
                ("decision_id", "outcome", "success"),
            ),
            self._tool(
                "memory_record_evaluation",
                "Record a versioned host evaluation for an outcome. " + scope_note,
                {
                    "record_id": {"type": "string"},
                    "outcome_id": {"type": "string"},
                    "evaluator_id": {"type": "string"},
                    "evaluator_version": {"type": "string"},
                    "rubric_id": {"type": "string"},
                    "rubric_version": {"type": "string"},
                    "metrics": {"type": "object"},
                    "evidence_digest": {"type": "string"},
                    "idempotency_key": {"type": "string"},
                    "corrects_id": {"type": "string"},
                },
                (
                    "outcome_id",
                    "evaluator_id",
                    "evaluator_version",
                    "rubric_id",
                    "rubric_version",
                    "metrics",
                    "evidence_digest",
                ),
            ),
            self._tool(
                "memory_record_reward",
                "Record a versioned reward derived by the host. " + scope_note,
                {
                    "record_id": {"type": "string"},
                    "outcome_id": {"type": "string"},
                    "evaluation_id": {"type": "string"},
                    "value": {"type": "number"},
                    "formula_version": {"type": "string"},
                    "reward_definition_id": {"type": "string"},
                    "components": {"type": "object"},
                    "idempotency_key": {"type": "string"},
                    "corrects_id": {"type": "string"},
                },
                ("outcome_id", "value", "formula_version"),
            ),
            self._tool(
                "memory_feedback_status",
                "Read one feedback receipt without exposing another scope. " + scope_note,
                {
                    "record_id": {"type": "string"},
                    "record_type": {
                        "enum": ["retrieval", "decision", "outcome", "evaluation", "reward"]
                    },
                },
                ("record_id", "record_type"),
            ),
            self._tool(
                "memory_capabilities",
                "Return protocol, schema, provider, and capability information.",
                {},
                (),
            ),
        )
        if self._doctor is not None:
            tools += (
                self._tool(
                    "memory_doctor",
                    "Run bounded, read-only diagnostics in the authenticated request scope.",
                    {},
                    (),
                ),
                self._tool(
                    "memory_repair_plan",
                    "Generate approval-gated repair recommendations without applying changes.",
                    {},
                    (),
                ),
            )
        if self._deletion_auditor is not None:
            tools += (
                self._tool(
                    "memory_deletion_audit",
                    "Return privacy-safe deletion receipts for the authenticated scope.",
                    {"limit": {"type": "integer", "minimum": 1, "maximum": 1000}},
                    (),
                ),
            )
        if self._ontology is not None:
            tools += self._ontology.tools()
        capabilities = self._provider.manifest().capabilities
        enabled = list(tools)
        if not capabilities.memory_blocks:
            enabled = [
                tool for tool in enabled if not tool["name"].startswith("memory_block_")
            ]
        if not (capabilities.decision_lineage and capabilities.outcome_feedback):
            enabled = [
                tool
                for tool in enabled
                if not tool["name"].startswith("memory_record_")
                and tool["name"] != "memory_feedback_status"
            ]
        return tuple(enabled)

    async def call_tool(
        self,
        name: str,
        arguments: Mapping[str, Any],
        context: MCPRequestContext,
    ) -> dict[str, Any]:
        try:
            if name.startswith("memory_ontology_"):
                if self._ontology is None:
                    raise MCPToolError("ontology is not configured", code="ontology_unavailable")
                return await self._ontology.call(name, dict(arguments), context)
            return await self._call_tool(name, arguments, context)
        except MCPToolError:
            raise
        except PluginError as error:
            raise MCPToolError.from_plugin_error(error) from error
        except DeletionAuditError as error:
            raise MCPToolError(str(error), code=error.code) from error

    async def _call_tool(
        self,
        name: str,
        arguments: Mapping[str, Any],
        context: MCPRequestContext,
    ) -> dict[str, Any]:
        if (
            name.startswith("memory_record_") or name == "memory_feedback_status"
        ) and not (
            self._provider.manifest().capabilities.decision_lineage
            and self._provider.manifest().capabilities.outcome_feedback
        ):
            raise MCPToolError("the selected memory provider does not support feedback records")
        if (
            name.startswith("memory_block_")
            and not self._provider.manifest().capabilities.memory_blocks
        ):
            raise MCPToolError("the selected memory provider does not support memory blocks")
        if name == "memory_ingest":
            result = await self._provider.ingest_event(
                MemoryEvent(
                    scope=context.scope,
                    event_type=self._string(arguments, "event_type"),
                    content=self._string(arguments, "content"),
                    metadata=self._mapping(arguments.get("metadata", {}), "metadata"),
                    idempotency_key=self._optional_string(arguments, "idempotency_key"),
                    source_uri=self._optional_string(arguments, "source_uri"),
                    actor=context.actor,
                )
            )
            return to_jsonable(result)

        if name == "memory_retrieve":
            raw_channels = arguments.get("channels")
            channels = (
                tuple(MemoryChannel(str(channel)) for channel in raw_channels)
                if isinstance(raw_channels, list)
                else MemoryQuery.__dataclass_fields__["channels"].default
            )
            result = await self._provider.retrieve(
                MemoryQuery(
                    scope=context.scope,
                    text=self._string(arguments, "text"),
                    limit=int(arguments.get("limit", 8)),
                    token_budget=int(arguments.get("token_budget", 1200)),
                    channels=channels,
                )
            )
            return to_jsonable(result)

        if name == "memory_get_state":
            return {"current_state": to_jsonable(await self._provider.get_state(context.scope))}

        if name == "memory_block_read":
            block = await self._provider.read_block(
                context.scope, self._string(arguments, "block_id")
            )
            return {"block": to_jsonable(block) if block else None}

        if name == "memory_block_write":
            event_ids = self._string_list(arguments.get("event_ids"), "event_ids")
            scope_level = ScopeLevel(str(arguments.get("scope_level", ScopeLevel.SESSION)))
            block_values: dict[str, Any] = {
                "scope": context.scope.project(scope_level),
                "title": self._string(arguments, "title"),
                "content": self._string(arguments, "content"),
                "event_ids": event_ids,
                "channel": MemoryChannel(
                    str(arguments.get("channel", MemoryChannel.SEMANTIC))
                ),
                "token_budget": int(arguments.get("token_budget", 256)),
                "status": ArtifactStatus(str(arguments.get("status", ArtifactStatus.ACTIVE))),
                "metadata": self._mapping(arguments.get("metadata", {}), "metadata"),
            }
            block_id = self._optional_string(arguments, "block_id")
            if block_id:
                block_values["id"] = block_id
            block = await self._provider.write_block(
                MemoryBlock(**block_values),
                expected_version=int(arguments.get("expected_version", 0)),
            )
            return {"block": to_jsonable(block)}

        if name == "memory_block_search":
            raw_channels = arguments.get("channels")
            channels = (
                tuple(MemoryChannel(str(channel)) for channel in raw_channels)
                if isinstance(raw_channels, list)
                else ()
            )
            blocks = await self._provider.search_blocks(
                context.scope,
                self._string(arguments, "text"),
                channels,
                int(arguments.get("limit", 8)),
            )
            return {"blocks": to_jsonable(blocks)}

        if name == "memory_block_forget":
            mode = ForgetMode(str(arguments.get("mode", ForgetMode.ARCHIVE)))
            if mode == ForgetMode.ERASE and not context.can_erase:
                raise MCPToolError("legal erase requires an authorized request context")
            request = ForgetRequest(
                scope=context.scope,
                memory_ids=(self._string(arguments, "block_id"),),
                mode=mode,
            )
            if self._deletion_auditor is not None:
                receipt = await self._deletion_auditor.forget(
                    self._provider, request, actor=context.actor
                )
                return self._audited_forget_payload(receipt)
            result = await self._provider.forget(request)
            return to_jsonable(result)

        if name == "memory_propose":
            source_event_ids = arguments.get("source_event_ids")
            if not isinstance(source_event_ids, list) or not all(
                isinstance(item, str) for item in source_event_ids
            ):
                raise MCPToolError("source_event_ids must be an array of strings")
            result = await self._provider.propose(
                MemoryProposal(
                    scope=context.scope,
                    key=self._string(arguments, "key"),
                    value=arguments.get("value"),
                    text=self._string(arguments, "text"),
                    source_event_ids=tuple(source_event_ids),
                    expected_version=int(arguments["expected_version"]),
                    scope_level=ScopeLevel(str(arguments.get("scope_level", ScopeLevel.SESSION))),
                    confidence=float(arguments.get("confidence", 1.0)),
                    importance=float(arguments.get("importance", 0.5)),
                    actor=context.actor,
                )
            )
            return to_jsonable(result)

        if name == "memory_forget":
            mode = ForgetMode(str(arguments.get("mode", ForgetMode.ARCHIVE)))
            if mode == ForgetMode.ERASE and not context.can_erase:
                raise MCPToolError("legal erase requires an authorized request context")
            raw_ids = arguments.get("memory_ids", [])
            if not isinstance(raw_ids, list) or not all(isinstance(item, str) for item in raw_ids):
                raise MCPToolError("memory_ids must be an array of strings")
            request = ForgetRequest(
                scope=context.scope,
                memory_ids=tuple(raw_ids),
                all_in_scope=bool(arguments.get("all_in_scope", False)),
                mode=mode,
            )
            if self._deletion_auditor is not None:
                receipt = await self._deletion_auditor.forget(
                    self._provider, request, actor=context.actor
                )
                return self._audited_forget_payload(receipt)
            result = await self._provider.forget(request)
            return to_jsonable(result)

        if name == "memory_record_decision":
            values: dict[str, Any] = {
                "scope": context.scope,
                "action": self._string(arguments, "action"),
                "memory_ids": tuple(arguments.get("memory_ids", ())),
                "procedure_ids": tuple(arguments.get("procedure_ids", ())),
                "run_id": self._optional_string(arguments, "run_id"),
                "bundle_id": self._optional_string(arguments, "bundle_id"),
                "memory_usage": MemoryUsage(
                    str(arguments.get("memory_usage", MemoryUsage.UNKNOWN))
                ),
                "policy_version": str(arguments.get("policy_version", "trusted-default")),
                "context_hash": str(arguments.get("context_hash", "")),
                "idempotency_key": self._optional_string(arguments, "idempotency_key"),
                "corrects_id": self._optional_string(arguments, "corrects_id"),
            }
            record_id = self._optional_string(arguments, "record_id")
            if record_id:
                values["id"] = record_id
            record = DecisionRecord(**values)
            persisted_id = await self._provider.record_decision(record)
            return {"record_id": persisted_id}

        if name == "memory_record_outcome":
            success = arguments.get("success")
            if success is not None and not isinstance(success, bool):
                raise MCPToolError("success must be a boolean or null")
            values = {
                "scope": context.scope,
                "decision_id": self._string(arguments, "decision_id"),
                "outcome": self._string(arguments, "outcome"),
                "success": success,
                "score": arguments.get("score"),
                "metrics": self._mapping(arguments.get("metrics", {}), "metrics"),
                "run_id": self._optional_string(arguments, "run_id"),
                "termination_reason": self._optional_string(arguments, "termination_reason"),
                "outcome_status": (
                    OutcomeStatus(str(arguments["outcome_status"]))
                    if arguments.get("outcome_status") is not None
                    else None
                ),
                "idempotency_key": self._optional_string(arguments, "idempotency_key"),
                "corrects_id": self._optional_string(arguments, "corrects_id"),
            }
            record_id = self._optional_string(arguments, "record_id")
            if record_id:
                values["id"] = record_id
            record = OutcomeEvent(**values)
            persisted_id = await self._provider.record_outcome(record)
            return {"record_id": persisted_id}

        if name == "memory_record_evaluation":
            values = {
                "scope": context.scope,
                "outcome_id": self._string(arguments, "outcome_id"),
                "evaluator_id": self._string(arguments, "evaluator_id"),
                "evaluator_version": self._string(arguments, "evaluator_version"),
                "rubric_id": self._string(arguments, "rubric_id"),
                "rubric_version": self._string(arguments, "rubric_version"),
                "metrics": self._mapping(arguments.get("metrics"), "metrics"),
                "evidence_digest": self._string(arguments, "evidence_digest"),
                "idempotency_key": self._optional_string(arguments, "idempotency_key"),
                "corrects_id": self._optional_string(arguments, "corrects_id"),
            }
            record_id = self._optional_string(arguments, "record_id")
            if record_id:
                values["id"] = record_id
            record = EvaluationRecord(**values)
            persisted_id = await self._provider.record_evaluation(record)
            return {"record_id": persisted_id}

        if name == "memory_record_reward":
            values = {
                "scope": context.scope,
                "outcome_id": self._string(arguments, "outcome_id"),
                "value": float(arguments["value"]),
                "formula_version": self._string(arguments, "formula_version"),
                "evaluation_id": self._optional_string(arguments, "evaluation_id"),
                "reward_definition_id": str(
                    arguments.get("reward_definition_id", "legacy")
                ),
                "components": self._mapping(arguments.get("components", {}), "components"),
                "idempotency_key": self._optional_string(arguments, "idempotency_key"),
                "corrects_id": self._optional_string(arguments, "corrects_id"),
            }
            record_id = self._optional_string(arguments, "record_id")
            if record_id:
                values["id"] = record_id
            record = RewardSignal(**values)
            persisted_id = await self._provider.record_reward(record)
            return {"record_id": persisted_id}

        if name == "memory_feedback_status":
            receipt = await self._provider.feedback_status(
                context.scope,
                self._string(arguments, "record_id"),
                self._string(arguments, "record_type"),
            )
            return {"receipt": to_jsonable(receipt) if receipt else None}

        if name == "memory_capabilities":
            return to_jsonable(self._provider.manifest())

        if name in {"memory_doctor", "memory_repair_plan"}:
            if self._doctor is None:
                raise MCPToolError(
                    "memory diagnostics are not configured",
                    code="diagnostics_unavailable",
                )
            report = await self._doctor.inspect(context.scope)
            if name == "memory_doctor":
                return {"report": to_jsonable(report)}
            return {
                "report": to_jsonable(report),
                "repair_plan": to_jsonable(build_memory_repair_plan(report)),
            }

        if name == "memory_deletion_audit":
            if self._deletion_auditor is None:
                raise MCPToolError(
                    "deletion audit is not configured",
                    code="audit_unavailable",
                )
            limit = int(arguments.get("limit", 100))
            if not 1 <= limit <= 1000:
                raise MCPToolError("limit must be between 1 and 1000", field="limit")
            report = await self._deletion_auditor.report(context.scope, limit=limit)
            return {"report": to_jsonable(report)}

        raise MCPToolError(f"unknown memory tool: {name}")

    @staticmethod
    def _audited_forget_payload(receipt: Any) -> dict[str, Any]:
        return {
            "affected_events": receipt.affected_events,
            "affected_claims": receipt.affected_claims,
            "affected_artifacts": receipt.affected_artifacts,
            "mode": receipt.mode.value,
            "audit": to_jsonable(receipt),
        }

    @staticmethod
    def _tool(
        name: str,
        description: str,
        properties: dict[str, Any],
        required: tuple[str, ...],
    ) -> dict[str, Any]:
        return {
            "name": name,
            "description": description,
            "inputSchema": {
                "type": "object",
                "properties": properties,
                "required": list(required),
                "additionalProperties": False,
            },
        }

    @staticmethod
    def _string(arguments: Mapping[str, Any], key: str) -> str:
        value = arguments.get(key)
        if not isinstance(value, str) or not value.strip():
            raise MCPToolError(f"{key} must be a non-empty string")
        return value

    @staticmethod
    def _optional_string(arguments: Mapping[str, Any], key: str) -> str | None:
        value = arguments.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise MCPToolError(f"{key} must be a string")
        return value

    @staticmethod
    def _mapping(value: Any, key: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise MCPToolError(f"{key} must be an object")
        return value

    @staticmethod
    def _string_list(value: Any, key: str) -> tuple[str, ...]:
        if not isinstance(value, list) or not value or not all(
            isinstance(item, str) and item.strip() for item in value
        ):
            raise MCPToolError(f"{key} must be a non-empty array of strings")
        return tuple(value)
