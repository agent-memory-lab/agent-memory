from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from inspect import isawaitable
from json import dumps
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from .domain import (
    Claim,
    DecisionRecord,
    EvaluationRecord,
    FeedbackPage,
    FeedbackReceipt,
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
    MemoryUsage,
    OutcomeEvent,
    OutcomeStatus,
    RewardSignal,
)
from .plugins import PluginRegistry
from .plugin_loader import LoadedPlugin
from .plugin_protocol import RetrievalCandidate
from .plugins import PluginKind
from .governed_recall import RecallPipeline
from .ports import ClaimExtractor, EmbeddingProvider, MemoryPolicy, MemoryProvider, Reranker


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

    __slots__ = ("_provider", "_recall_pipeline", "scope", "limits", "_initialized")

    def __init__(
        self,
        provider: MemoryProvider,
        scope: MemoryScope,
        *,
        limits: MemoryLimits | None = None,
        recall_pipeline: RecallPipeline | None = None,
    ) -> None:
        self._provider = provider
        self._recall_pipeline = recall_pipeline
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
        embedding_provider: EmbeddingProvider | None = None,
        trusted_evaluator_ids: Collection[str] | None = None,
        recall_pipeline: RecallPipeline | None = None,
    ) -> AgentMemory:
        return cls(
            PluginRegistry().create_provider(
                "sqlite",
                database_path=database_path,
                extractor=extractor,
                policy=policy,
                reranker=reranker,
                embedding_provider=embedding_provider,
                trusted_evaluator_ids=trusted_evaluator_ids,
            ),
            scope or MemoryScope(tenant_id="local", session_id="default"),
            limits=limits,
            recall_pipeline=recall_pipeline,
        )

    @classmethod
    def from_plugin(
        cls,
        name: str,
        *,
        scope: MemoryScope,
        limits: MemoryLimits | None = None,
        recall_pipeline: RecallPipeline | None = None,
        **config: Any,
    ) -> AgentMemory:
        return cls(
            PluginRegistry().create_provider(name, **config),
            scope,
            limits=limits,
            recall_pipeline=recall_pipeline,
        )

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
        query = MemoryQuery(
            scope=self.scope,
            text=text,
            limit=max(1, bounded_limit),
            token_budget=max(64, bounded_tokens),
        )
        if self._recall_pipeline is None:
            return await self._provider.retrieve(query)
        current_state = await self._provider.get_state(self.scope)
        return await self._recall_pipeline.retrieve(
            query,
            tuple(current_state[: self.limits.max_state_claims]),
        )

    async def retrieve_candidates(
        self,
        text: str,
        plugin: LoadedPlugin,
        *,
        limit: int | None = None,
    ) -> tuple[RetrievalCandidate, ...]:
        """Call an explicitly loaded retriever without changing default recall."""
        self._require_initialized()
        if not isinstance(plugin, LoadedPlugin) or plugin.manifest.kind is not PluginKind.RETRIEVER:
            raise ValueError("plugin must be a loaded retriever")
        if plugin.context.scope != self.scope:
            raise ValueError("retriever scope must match the AgentMemory scope")
        if limit is not None and (type(limit) is not int or limit < 1):
            raise ValueError("limit must be a positive integer")
        limits = plugin.context.resource_limits
        bounded_limit = min(
            limit if limit is not None else self.limits.max_recall_items,
            self.limits.max_recall_items,
            limits.max_candidates,
        )
        query = MemoryQuery(
            scope=self.scope,
            text=text,
            limit=bounded_limit,
            token_budget=self.limits.max_context_tokens,
        )
        candidates = await asyncio.wait_for(
            plugin.instance.retrieve(query, plugin.context),
            timeout=limits.timeout_ms / 1_000,
        )
        if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
            raise ValueError("retriever must return a sequence of candidates")
        if len(candidates) > bounded_limit or any(
            not isinstance(candidate, RetrievalCandidate) for candidate in candidates
        ):
            raise ValueError("retriever returned invalid or excessive candidates")
        return tuple(candidates)

    async def state(self) -> tuple[Claim, ...]:
        self._require_initialized()
        claims = await self._provider.get_state(self.scope)
        return tuple(claims[: self.limits.max_state_claims])

    async def record_decision(
        self,
        action: str,
        *,
        memory_ids: Sequence[str] = (),
        procedure_ids: Sequence[str] = (),
        run_id: str | None = None,
        bundle_id: str | None = None,
        memory_usage: MemoryUsage = MemoryUsage.UNKNOWN,
        policy_version: str = "trusted-default",
        context_hash: str = "",
        idempotency_key: str | None = None,
        corrects_id: str | None = None,
        record_id: str | None = None,
    ) -> DecisionRecord:
        self._require_initialized()
        values: dict[str, Any] = {
            "scope": self.scope,
            "action": action,
            "memory_ids": tuple(memory_ids),
            "procedure_ids": tuple(procedure_ids),
            "run_id": run_id,
            "bundle_id": bundle_id,
            "memory_usage": memory_usage,
            "policy_version": policy_version,
            "context_hash": context_hash,
            "idempotency_key": idempotency_key,
            "corrects_id": corrects_id,
        }
        if record_id is not None:
            values["id"] = record_id
        record = DecisionRecord(**values)
        persisted_id = await self._provider.record_decision(record)
        return record if persisted_id == record.id else replace(record, id=persisted_id)

    async def record_outcome(
        self,
        decision_id: str,
        outcome: str,
        success: bool | None,
        *,
        score: float | None = None,
        metrics: Mapping[str, float] | None = None,
        run_id: str | None = None,
        termination_reason: str | None = None,
        outcome_status: OutcomeStatus | None = None,
        idempotency_key: str | None = None,
        corrects_id: str | None = None,
        record_id: str | None = None,
        expires_at: datetime | None = None,
    ) -> OutcomeEvent:
        self._require_initialized()
        values: dict[str, Any] = {
            "scope": self.scope,
            "decision_id": decision_id,
            "outcome": outcome,
            "success": success,
            "score": score,
            "metrics": metrics or {},
            "run_id": run_id,
            "termination_reason": termination_reason,
            "outcome_status": outcome_status,
            "idempotency_key": idempotency_key,
            "corrects_id": corrects_id,
            "expires_at": expires_at,
        }
        if record_id is not None:
            values["id"] = record_id
        record = OutcomeEvent(**values)
        persisted_id = await self._provider.record_outcome(record)
        return record if persisted_id == record.id else replace(record, id=persisted_id)

    async def record_evaluation(
        self,
        outcome_id: str,
        *,
        evaluator_id: str,
        evaluator_version: str,
        rubric_id: str,
        rubric_version: str,
        metrics: Mapping[str, float],
        evidence_digest: str,
        idempotency_key: str | None = None,
        corrects_id: str | None = None,
        record_id: str | None = None,
        expires_at: datetime | None = None,
    ) -> EvaluationRecord:
        self._require_initialized()
        values: dict[str, Any] = {
            "scope": self.scope,
            "outcome_id": outcome_id,
            "evaluator_id": evaluator_id,
            "evaluator_version": evaluator_version,
            "rubric_id": rubric_id,
            "rubric_version": rubric_version,
            "metrics": metrics,
            "evidence_digest": evidence_digest,
            "idempotency_key": idempotency_key,
            "corrects_id": corrects_id,
            "expires_at": expires_at,
        }
        if record_id is not None:
            values["id"] = record_id
        record = EvaluationRecord(**values)
        persisted_id = await self._provider.record_evaluation(record)
        return record if persisted_id == record.id else replace(record, id=persisted_id)

    async def record_reward(
        self,
        outcome_id: str,
        value: float,
        formula_version: str,
        *,
        evaluation_id: str | None = None,
        reward_definition_id: str = "legacy",
        components: Mapping[str, float] | None = None,
        idempotency_key: str | None = None,
        corrects_id: str | None = None,
        record_id: str | None = None,
        expires_at: datetime | None = None,
    ) -> RewardSignal:
        self._require_initialized()
        values: dict[str, Any] = {
            "scope": self.scope,
            "outcome_id": outcome_id,
            "value": value,
            "formula_version": formula_version,
            "evaluation_id": evaluation_id,
            "reward_definition_id": reward_definition_id,
            "components": components or {},
            "idempotency_key": idempotency_key,
            "corrects_id": corrects_id,
            "expires_at": expires_at,
        }
        if record_id is not None:
            values["id"] = record_id
        record = RewardSignal(**values)
        persisted_id = await self._provider.record_reward(record)
        return record if persisted_id == record.id else replace(record, id=persisted_id)

    async def feedback_status(
        self, record_id: str, record_type: str
    ) -> FeedbackReceipt | None:
        self._require_initialized()
        return await self._provider.feedback_status(self.scope, record_id, record_type)

    async def feedback_history(
        self, record_type: str, *, limit: int = 50, cursor: str | None = None
    ) -> FeedbackPage:
        self._require_initialized()
        return await self._provider.feedback_history(
            self.scope, record_type, limit=limit, cursor=cursor
        )

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
