from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .domain import (
    ArtifactStatus,
    ForgetMode,
    ForgetRequest,
    MemoryBlock,
    MemoryChannel,
    MemoryEvent,
    MemoryProposal,
    MemoryQuery,
    MemoryScope,
    ScopeLevel,
)
from .ports import MemoryProvider
from .serialization import to_jsonable


class MCPToolError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class MCPRequestContext:
    """Trusted identity-derived context; tool arguments cannot override this scope."""

    scope: MemoryScope
    actor: str = "agent"
    can_erase: bool = False


class MCPMemoryTools:
    def __init__(self, provider: MemoryProvider) -> None:
        self._provider = provider

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
                "memory_capabilities",
                "Return protocol, schema, provider, and capability information.",
                {},
                (),
            ),
        )
        if self._provider.manifest().capabilities.memory_blocks:
            return tools
        return tuple(tool for tool in tools if not tool["name"].startswith("memory_block_"))

    async def call_tool(
        self,
        name: str,
        arguments: Mapping[str, Any],
        context: MCPRequestContext,
    ) -> dict[str, Any]:
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
            result = await self._provider.forget(
                ForgetRequest(
                    scope=context.scope,
                    memory_ids=(self._string(arguments, "block_id"),),
                    mode=mode,
                )
            )
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
            result = await self._provider.forget(
                ForgetRequest(
                    scope=context.scope,
                    memory_ids=tuple(raw_ids),
                    all_in_scope=bool(arguments.get("all_in_scope", False)),
                    mode=mode,
                )
            )
            return to_jsonable(result)

        if name == "memory_capabilities":
            return to_jsonable(self._provider.manifest())

        raise MCPToolError(f"unknown memory tool: {name}")

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
