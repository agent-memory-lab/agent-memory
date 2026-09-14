from __future__ import annotations

from collections.abc import Mapping
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, Self

from agent_memory.mcp import MCPMemoryTools, MCPRequestContext, MCPToolError, decode_mcp_error
from agent_memory.ports import MemoryProvider

if TYPE_CHECKING:
    from mcp import Client


class MemoryClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "memory_client_error",
        field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.field = field

    def to_dict(self) -> dict[str, str]:
        payload = {"code": self.code, "message": str(self)}
        if self.field is not None:
            payload["field"] = self.field
        return payload


class MemoryClient(Protocol):
    async def ingest(self, event_type: str, content: str, **options: Any) -> dict[str, Any]: ...
    async def retrieve(self, text: str, **options: Any) -> dict[str, Any]: ...
    async def get_state(self) -> dict[str, Any]: ...
    async def propose(self, **proposal: Any) -> dict[str, Any]: ...
    async def forget(self, **request: Any) -> dict[str, Any]: ...
    async def read_block(self, block_id: str) -> dict[str, Any]: ...
    async def write_block(self, **block: Any) -> dict[str, Any]: ...
    async def search_blocks(self, text: str, **options: Any) -> dict[str, Any]: ...
    async def forget_block(self, block_id: str, **options: Any) -> dict[str, Any]: ...
    async def capabilities(self) -> dict[str, Any]: ...
    async def record_decision(self, action: str, **options: Any) -> dict[str, Any]: ...
    async def record_outcome(
        self, decision_id: str, outcome: str, success: bool, **options: Any
    ) -> dict[str, Any]: ...
    async def record_evaluation(self, outcome_id: str, **evaluation: Any) -> dict[str, Any]: ...
    async def record_reward(
        self, outcome_id: str, value: float, formula_version: str, **options: Any
    ) -> dict[str, Any]: ...
    async def feedback_status(self, record_id: str, record_type: str) -> dict[str, Any]: ...


class _Operations:
    async def _call(self, name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    async def ingest(
        self,
        event_type: str,
        content: str,
        *,
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        source_uri: str | None = None,
    ) -> dict[str, Any]:
        return await self._call(
            "memory_ingest",
            {
                "event_type": event_type,
                "content": content,
                "metadata": dict(metadata or {}),
                "idempotency_key": idempotency_key,
                "source_uri": source_uri,
            },
        )

    async def retrieve(
        self,
        text: str,
        *,
        limit: int = 8,
        token_budget: int = 1200,
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        arguments: dict[str, Any] = {"text": text, "limit": limit, "token_budget": token_budget}
        if channels is not None:
            arguments["channels"] = channels
        return await self._call("memory_retrieve", arguments)

    async def get_state(self) -> dict[str, Any]:
        return await self._call("memory_get_state", {})

    async def record_decision(self, action: str, **options: Any) -> dict[str, Any]:
        return await self._call("memory_record_decision", {"action": action, **options})

    async def record_outcome(
        self,
        decision_id: str,
        outcome: str,
        success: bool,
        **options: Any,
    ) -> dict[str, Any]:
        return await self._call(
            "memory_record_outcome",
            {
                "decision_id": decision_id,
                "outcome": outcome,
                "success": success,
                **options,
            },
        )

    async def record_evaluation(
        self, outcome_id: str, **evaluation: Any
    ) -> dict[str, Any]:
        return await self._call(
            "memory_record_evaluation", {"outcome_id": outcome_id, **evaluation}
        )

    async def record_reward(
        self,
        outcome_id: str,
        value: float,
        formula_version: str,
        **options: Any,
    ) -> dict[str, Any]:
        return await self._call(
            "memory_record_reward",
            {
                "outcome_id": outcome_id,
                "value": value,
                "formula_version": formula_version,
                **options,
            },
        )

    async def feedback_status(self, record_id: str, record_type: str) -> dict[str, Any]:
        return await self._call(
            "memory_feedback_status",
            {"record_id": record_id, "record_type": record_type},
        )

    async def propose(
        self,
        *,
        key: str,
        value: Any,
        text: str,
        source_event_ids: list[str],
        expected_version: int,
        scope_level: str = "session",
        confidence: float = 1.0,
        importance: float = 0.5,
    ) -> dict[str, Any]:
        return await self._call(
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

    async def forget(
        self,
        *,
        memory_ids: list[str] | None = None,
        all_in_scope: bool = False,
        mode: str = "archive",
    ) -> dict[str, Any]:
        return await self._call(
            "memory_forget",
            {
                "memory_ids": memory_ids or [],
                "all_in_scope": all_in_scope,
                "mode": mode,
            },
        )

    async def read_block(self, block_id: str) -> dict[str, Any]:
        return await self._call("memory_block_read", {"block_id": block_id})

    async def write_block(
        self,
        *,
        title: str,
        content: str,
        event_ids: list[str],
        block_id: str | None = None,
        channel: str = "semantic",
        scope_level: str = "session",
        token_budget: int = 256,
        expected_version: int = 0,
        status: str = "active",
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "title": title,
            "content": content,
            "event_ids": event_ids,
            "channel": channel,
            "scope_level": scope_level,
            "token_budget": token_budget,
            "expected_version": expected_version,
            "status": status,
            "metadata": dict(metadata or {}),
        }
        if block_id is not None:
            arguments["block_id"] = block_id
        return await self._call("memory_block_write", arguments)

    async def search_blocks(
        self,
        text: str,
        *,
        limit: int = 8,
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        arguments: dict[str, Any] = {"text": text, "limit": limit}
        if channels is not None:
            arguments["channels"] = channels
        return await self._call("memory_block_search", arguments)

    async def forget_block(self, block_id: str, *, mode: str = "archive") -> dict[str, Any]:
        return await self._call(
            "memory_block_forget",
            {"block_id": block_id, "mode": mode},
        )

    async def capabilities(self) -> dict[str, Any]:
        return await self._call("memory_capabilities", {})


class EmbeddedMemoryClient(_Operations):
    """Run the exact MCP contract in-process without a protocol hop."""

    def __init__(self, provider: MemoryProvider, context: MCPRequestContext) -> None:
        self._provider = provider
        self._tools = MCPMemoryTools(provider)
        self._context = context

    async def initialize(self) -> None:
        await self._provider.initialize()

    async def _call(self, name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        try:
            return await self._tools.call_tool(name, arguments, self._context)
        except MCPToolError as error:
            raise MemoryClientError(
                str(error),
                code=error.code,
                field=error.field,
            ) from error


class MCPMemoryClient(_Operations):
    """High-level client backed by the official MCP v2 Client."""

    def __init__(self, source: Any) -> None:
        try:
            from mcp import Client
        except ImportError as error:
            raise MemoryClientError(
                "MCPMemoryClient requires the optional 'mcp' dependency; "
                "install agent-memory-sdk[mcp]"
            ) from error
        self._client: Client = Client(source)

    @classmethod
    def from_http(cls, url: str, *, http_client: Any | None = None) -> MCPMemoryClient:
        try:
            from mcp.client.streamable_http import streamable_http_client
        except ImportError as error:
            raise MemoryClientError(
                "MCPMemoryClient requires the optional 'mcp' dependency; "
                "install agent-memory-sdk[mcp]"
            ) from error
        if http_client is None:
            return cls(url)
        return cls(streamable_http_client(url, http_client=http_client))

    @classmethod
    def from_stdio(
        cls,
        command: str,
        *,
        args: list[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> MCPMemoryClient:
        try:
            from mcp import StdioServerParameters
        except ImportError as error:
            raise MemoryClientError(
                "MCPMemoryClient requires the optional 'mcp' dependency; "
                "install agent-memory-sdk[mcp]"
            ) from error
        return cls(
            StdioServerParameters(
                command=command,
                args=args or [],
                env=dict(env) if env is not None else None,
            )
        )

    async def __aenter__(self) -> Self:
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._client.__aexit__(exc_type, exc, traceback)

    async def _call(self, name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        result = await self._client.call_tool(name, dict(arguments))
        if result.is_error:
            messages = [
                str(getattr(block, "text", ""))
                for block in result.content
                if getattr(block, "text", None)
            ]
            for message in messages:
                payload = decode_mcp_error(message)
                if payload is not None:
                    raise MemoryClientError(
                        payload["message"],
                        code=payload["code"],
                        field=payload.get("field"),
                    )
            raise MemoryClientError("; ".join(messages) or f"{name} failed")
        if not isinstance(result.structured_content, dict):
            raise MemoryClientError(f"{name} returned no structured content")
        return result.structured_content
