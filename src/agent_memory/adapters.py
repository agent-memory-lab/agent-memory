from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any, Mapping, Sequence

from .domain import (
    ArtifactStatus,
    DecisionRecord,
    Episode,
    IngestResult,
    MemoryBundle,
    MemoryEvent,
    MemoryQuery,
    MemoryScope,
    OutcomeEvent,
    Provenance,
    RewardSignal,
)
from .ports import MemoryProvider


@dataclass(frozen=True, slots=True)
class AgentLifecycleContext:
    scope: MemoryScope
    run_id: str
    turn_id: str
    actor: str = "agent"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def idempotency_key(self, hook: str, operation_id: str = "default") -> str:
        return f"{self.run_id}:{self.turn_id}:{hook}:{operation_id}"


class AgentMemoryAdapter:
    """Generic lifecycle hooks usable by LangGraph, AutoGen, custom agents, or SDKs."""

    def __init__(self, provider: MemoryProvider) -> None:
        self._provider = provider

    async def initialize(self) -> None:
        await self._provider.initialize()

    async def before_model(
        self,
        context: AgentLifecycleContext,
        prompt: str,
        *,
        limit: int = 8,
        token_budget: int = 1200,
    ) -> MemoryBundle:
        return await self._provider.retrieve(
            MemoryQuery(
                scope=context.scope,
                text=prompt,
                limit=limit,
                token_budget=token_budget,
            )
        )

    async def after_tool(
        self,
        context: AgentLifecycleContext,
        *,
        tool_call_id: str,
        tool_name: str,
        arguments: Mapping[str, Any],
        result: Any,
        success: bool,
        claims: Sequence[Mapping[str, Any]] = (),
    ) -> IngestResult:
        return await self._provider.ingest_event(
            MemoryEvent(
                scope=context.scope,
                event_type="agent.tool.completed" if success else "agent.tool.failed",
                content=f"Tool {tool_name} {'succeeded' if success else 'failed'}.",
                idempotency_key=context.idempotency_key("after_tool", tool_call_id),
                actor=context.actor,
                metadata={
                    **context.metadata,
                    "tool_call_id": tool_call_id,
                    "tool_name": tool_name,
                    "arguments": dict(arguments),
                    "result": result,
                    "success": success,
                    "claims": list(claims),
                },
            )
        )

    async def after_model(
        self,
        context: AgentLifecycleContext,
        *,
        response: str,
        claims: Sequence[Mapping[str, Any]] = (),
    ) -> IngestResult:
        return await self._provider.ingest_event(
            MemoryEvent(
                scope=context.scope,
                event_type="agent.model.completed",
                content=response,
                idempotency_key=context.idempotency_key("after_model"),
                actor=context.actor,
                metadata={**context.metadata, "claims": list(claims)},
            )
        )

    async def record_decision(
        self,
        context: AgentLifecycleContext,
        bundle: MemoryBundle,
        action: str,
        *,
        procedure_ids: tuple[str, ...] = (),
        policy_version: str = "trusted-default",
    ) -> DecisionRecord:
        memory_ids = tuple(citation.memory_id for citation in bundle.citations)
        digest = sha256(f"{memory_ids}:{procedure_ids}:{policy_version}".encode()).hexdigest()
        decision = DecisionRecord(
            scope=context.scope,
            action=action,
            memory_ids=memory_ids,
            procedure_ids=procedure_ids,
            policy_version=policy_version,
            context_hash=digest,
        )
        await self._provider.record_decision(decision)
        return decision

    async def record_outcome(
        self,
        context: AgentLifecycleContext,
        decision: DecisionRecord,
        *,
        outcome: str,
        success: bool,
        score: float | None = None,
        metrics: Mapping[str, float] | None = None,
    ) -> OutcomeEvent:
        record = OutcomeEvent(
            scope=context.scope,
            decision_id=decision.id,
            outcome=outcome,
            success=success,
            score=score,
            metrics=metrics or {},
        )
        await self._provider.record_outcome(record)
        return record

    async def record_reward(
        self,
        context: AgentLifecycleContext,
        outcome: OutcomeEvent,
        *,
        value: float,
        formula_version: str,
        components: Mapping[str, float] | None = None,
    ) -> RewardSignal:
        reward = RewardSignal(
            scope=context.scope,
            outcome_id=outcome.id,
            value=value,
            formula_version=formula_version,
            components=components or {},
        )
        await self._provider.record_reward(reward)
        return reward

    async def session_end(
        self,
        context: AgentLifecycleContext,
        *,
        observation: str,
        action: str,
        outcome: str,
        lesson: str,
        source_event_ids: tuple[str, ...],
        quality: float,
    ) -> Episode:
        episode = Episode(
            scope=context.scope,
            observation=observation,
            action=action,
            outcome=outcome,
            lesson=lesson,
            quality=quality,
            status=ArtifactStatus.CANDIDATE,
            provenance=Provenance(
                source_event_ids=source_event_ids,
                extractor="AgentMemoryAdapter.session_end",
                provider=self._provider.manifest().name,
            ),
        )
        await self._provider.record_episode(episode)
        return episode
