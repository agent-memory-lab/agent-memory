from __future__ import annotations

from collections.abc import Mapping
from types import TracebackType
from typing import Any, Protocol, Self

from mcp import Client, StdioServerParameters
from mcp.client.streamable_http import streamable_http_client

from agent_memory.mcp import MCPMemoryTools, MCPRequestContext
from agent_memory.ports import MemoryProvider


class MemoryClientError(RuntimeError):
    pass


class MemoryClient(Protocol):
    async def ingest(self, event_type: str, content: str, **options: Any) -> dict[str, Any]: ...
    async def retrieve(self, text: str, **options: Any) -> dict[str, Any]: ...
    async def get_state(self) -> dict[str, Any]: ...
    async def propose(self, **proposal: Any) -> dict[str, Any]: ...
    async def forget(self, **request: Any) -> dict[str, Any]: ...
    async def capabilities(self) -> dict[str, Any]: ...


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
        return await self._tools.call_tool(name, arguments, self._context)


class MCPMemoryClient(_Operations):
    """High-level client backed by the official MCP v2 Client."""

    def __init__(self, source: Any) -> None:
        self._client = Client(source)

    @classmethod
    def from_http(cls, url: str, *, http_client: Any | None = None) -> MCPMemoryClient:
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
            raise MemoryClientError("; ".join(messages) or f"{name} failed")
        if not isinstance(result.structured_content, dict):
            raise MemoryClientError(f"{name} returned no structured content")
        return result.structured_content
