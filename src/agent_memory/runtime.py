from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from inspect import isawaitable
from json import dumps
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from .domain import (
    Claim,
    ForgetMode,
    ForgetRequest,
    ForgetResult,
    IngestResult,
    MemoryBlock,
    MemoryBundle,
    MemoryChannel,
    MemoryEvent,
    MemoryQuery,
    MemoryScope,
)
from .plugins import PluginRegistry
from .ports import ClaimExtractor, MemoryPolicy, MemoryProvider, Reranker


@dataclass(frozen=True, slots=True)
class MemoryLimits:
    max_event_characters: int = 32_000
    max_metadata_bytes: int = 64_000
    max_claims_per_event: int = 32
    max_recall_items: int = 8
    max_context_tokens: int = 1_200
    max_state_claims: int = 64

    def __post_init__(self) -> None:
        values = (
            self.max_event_characters,
            self.max_metadata_bytes,
            self.max_claims_per_event,
            self.max_recall_items,
            self.max_context_tokens,
            self.max_state_claims,
        )
        if any(value < 1 for value in values):
            raise ValueError("all memory limits must be positive")
        if self.max_context_tokens < 64:
            raise ValueError("max_context_tokens must be at least 64")
        if self.max_recall_items > 100:
            raise ValueError("max_recall_items cannot exceed the protocol limit of 100")


class AgentMemory:
    """Zero-config bounded facade over a lazily selected MemoryProvider."""

    __slots__ = ("_provider", "scope", "limits", "_initialized")

    def __init__(
        self,
        provider: MemoryProvider,
        scope: MemoryScope,
        *,
        limits: MemoryLimits | None = None,
    ) -> None:
        self._provider = provider
        self.scope = scope
        self.limits = limits or MemoryLimits()
        self._initialized = False

    @classmethod
    def local(
        cls,
        database_path: str | Path = "agent-memory.db",
        *,
        scope: MemoryScope | None = None,
        limits: MemoryLimits | None = None,
        extractor: ClaimExtractor | None = None,
        policy: MemoryPolicy | None = None,
        reranker: Reranker | None = None,
    ) -> AgentMemory:
        return cls(
            PluginRegistry().create_provider(
                "sqlite",
                database_path=database_path,
                extractor=extractor,
                policy=policy,
                reranker=reranker,
            ),
            scope or MemoryScope(tenant_id="local", session_id="default"),
            limits=limits,
        )

    @classmethod
    def from_plugin(
        cls,
        name: str,
        *,
        scope: MemoryScope,
        limits: MemoryLimits | None = None,
        **config: Any,
    ) -> AgentMemory:
        return cls(PluginRegistry().create_provider(name, **config), scope, limits=limits)

    @property
    def provider(self) -> MemoryProvider:
        return self._provider

    async def initialize(self) -> Self:
        if not self._initialized:
            await self._provider.initialize()
            self._initialized = True
        return self

    async def __aenter__(self) -> Self:
        return await self.initialize()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        close = getattr(self._provider, "close", None)
        if callable(close):
            result = close()
            if isawaitable(result):
                await result

    async def remember(
        self,
        content: str,
        *,
        event_type: str = "agent.memory",
        claims: Sequence[Mapping[str, Any]] = (),
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        source_uri: str | None = None,
        actor: str = "agent",
    ) -> IngestResult:
        self._require_initialized()
        if len(content) > self.limits.max_event_characters:
            raise ValueError("event exceeds max_event_characters")
        if len(claims) > self.limits.max_claims_per_event:
            raise ValueError("event exceeds max_claims_per_event")
        combined = dict(metadata or {})
        if claims:
            combined["claims"] = [dict(claim) for claim in claims]
        metadata_size = len(dumps(combined, ensure_ascii=False, default=str).encode("utf-8"))
        if metadata_size > self.limits.max_metadata_bytes:
            raise ValueError("event metadata exceeds max_metadata_bytes")
        return await self._provider.ingest_event(
            MemoryEvent(
                scope=self.scope,
                event_type=event_type,
                content=content,
                metadata=combined,
                idempotency_key=idempotency_key,
                source_uri=source_uri,
                actor=actor,
            )
        )

    async def recall(
        self,
        text: str,
        *,
        limit: int | None = None,
        token_budget: int | None = None,
    ) -> MemoryBundle:
        self._require_initialized()
        bounded_limit = min(limit or self.limits.max_recall_items, self.limits.max_recall_items)
        bounded_tokens = min(
            token_budget or self.limits.max_context_tokens,
            self.limits.max_context_tokens,
        )
        return await self._provider.retrieve(
            MemoryQuery(
                scope=self.scope,
                text=text,
                limit=max(1, bounded_limit),
                token_budget=max(64, bounded_tokens),
            )
        )

    async def state(self) -> tuple[Claim, ...]:
        self._require_initialized()
        claims = await self._provider.get_state(self.scope)
        return tuple(claims[: self.limits.max_state_claims])

    async def write_block(self, block: MemoryBlock, *, expected_version: int = 0) -> MemoryBlock:
        self._require_initialized()
        if block.scope != self.scope:
            raise ValueError("memory block scope must match the AgentMemory scope")
        return await self._provider.write_block(block, expected_version)

    async def read_block(self, block_id: str) -> MemoryBlock | None:
        self._require_initialized()
        return await self._provider.read_block(self.scope, block_id)

    async def search_blocks(
        self,
        text: str,
        *,
        channels: Sequence[MemoryChannel] = (),
        limit: int | None = None,
    ) -> tuple[MemoryBlock, ...]:
        self._require_initialized()
        bounded_limit = min(limit or self.limits.max_recall_items, self.limits.max_recall_items)
        return await self._provider.search_blocks(
            self.scope,
            text,
            channels,
            max(1, bounded_limit),
        )

    async def forget(
        self,
        *,
        memory_ids: Sequence[str] = (),
        all_in_scope: bool = False,
        erase: bool = False,
    ) -> ForgetResult:
        self._require_initialized()
        return await self._provider.forget(
            ForgetRequest(
                scope=self.scope,
                memory_ids=tuple(memory_ids),
                all_in_scope=all_in_scope,
                mode=ForgetMode.ERASE if erase else ForgetMode.ARCHIVE,
            )
        )

    async def forget_block(self, block_id: str, *, erase: bool = False) -> ForgetResult:
        self._require_initialized()
        return await self._provider.forget_block(
            self.scope,
            block_id,
            ForgetMode.ERASE if erase else ForgetMode.ARCHIVE,
        )

    def _require_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("use 'async with AgentMemory.local()' or await initialize() first")
