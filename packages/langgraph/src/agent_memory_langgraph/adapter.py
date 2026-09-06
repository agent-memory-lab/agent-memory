from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from agent_memory.adapters import AgentLifecycleContext, AgentMemoryAdapter
from agent_memory.domain import MemoryScope
from agent_memory.ports import MemoryProvider
from agent_memory.serialization import to_jsonable


class ScopeResolver(Protocol):
    def __call__(self, config: Mapping[str, Any]) -> MemoryScope: ...


def scope_from_config(config: Mapping[str, Any]) -> MemoryScope:
    """Resolve scope only from host-controlled LangGraph configuration."""
    configurable = config.get("configurable", {})
    if not isinstance(configurable, Mapping):
        raise ValueError("configurable must be a mapping")
    tenant_id = configurable.get("memory_tenant_id")
    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise ValueError("configurable.memory_tenant_id is required")

    def optional(name: str) -> str | None:
        value = configurable.get(name)
        return str(value) if value is not None else None

    return MemoryScope(
        tenant_id=tenant_id,
        namespace=str(configurable.get("memory_namespace", "default")),
        user_id=optional("memory_user_id"),
        agent_id=optional("memory_agent_id"),
        workspace_id=optional("memory_workspace_id"),
        session_id=optional("memory_session_id") or optional("thread_id"),
    )


@dataclass(frozen=True, slots=True)
class LangGraphKeys:
    messages: str = "messages"
    bundle: str = "memory"
    tool_event: str = "memory_tool_event"
    response: str = "memory_response"
    receipt: str = "memory_receipt"
    turn_id: str = "memory_turn_id"


class LangGraphMemoryAdapter:
    """Node-compatible hooks with no dependency on LangGraph internal classes."""

    def __init__(
        self,
        provider: MemoryProvider,
        *,
        scope_resolver: ScopeResolver = scope_from_config,
        keys: LangGraphKeys | None = None,
        token_budget: int = 1200,
        limit: int = 8,
    ) -> None:
        self._adapter = AgentMemoryAdapter(provider)
        self._scope_resolver = scope_resolver
        self._keys = keys or LangGraphKeys()
        self._token_budget = token_budget
        self._limit = limit

    async def initialize(self) -> None:
        await self._adapter.initialize()

    def _context(
        self, state: Mapping[str, Any], config: Mapping[str, Any]
    ) -> AgentLifecycleContext:
        configurable = config.get("configurable", {})
        if not isinstance(configurable, Mapping):
            configurable = {}
        run_id = str(config.get("run_id") or configurable.get("thread_id") or "langgraph")
        turn_id = str(state.get(self._keys.turn_id) or len(state.get(self._keys.messages, ())))
        return AgentLifecycleContext(
            scope=self._scope_resolver(config),
            run_id=run_id,
            turn_id=turn_id,
            actor=str(configurable.get("memory_actor", "agent")),
        )

    def _prompt(self, state: Mapping[str, Any]) -> str:
        messages = state.get(self._keys.messages, ())
        if not messages:
            return ""
        last = messages[-1]
        if isinstance(last, Mapping):
            return str(last.get("content", ""))
        return str(getattr(last, "content", last))

    async def before_model(
        self,
        state: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> dict[str, Any]:
        bundle = await self._adapter.before_model(
            self._context(state, config),
            self._prompt(state),
            limit=self._limit,
            token_budget=self._token_budget,
        )
        return {self._keys.bundle: to_jsonable(bundle)}

    async def after_tool(
        self,
        state: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> dict[str, Any]:
        event = state.get(self._keys.tool_event)
        if not isinstance(event, Mapping):
            raise ValueError(f"state.{self._keys.tool_event} must be a mapping")
        receipt = await self._adapter.after_tool(
            self._context(state, config),
            tool_call_id=str(event["tool_call_id"]),
            tool_name=str(event["tool_name"]),
            arguments=event.get("arguments", {}),
            result=event.get("result"),
            success=bool(event.get("success", True)),
            claims=event.get("claims", ()),
        )
        return {self._keys.receipt: to_jsonable(receipt)}

    async def after_model(
        self,
        state: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> dict[str, Any]:
        response = state.get(self._keys.response)
        claims: Any = ()
        if isinstance(response, Mapping):
            claims = response.get("claims", ())
            response = response.get("content", "")
        receipt = await self._adapter.after_model(
            self._context(state, config),
            response=str(response or ""),
            claims=claims,
        )
        return {self._keys.receipt: to_jsonable(receipt)}
