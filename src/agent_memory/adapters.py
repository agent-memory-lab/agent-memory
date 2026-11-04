from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any

from .capture.policy import DefaultCaptureRedactor
from .domain import (
    ArtifactStatus,
    DecisionRecord,
    Episode,
    IngestResult,
    MemoryBundle,
    MemoryEvent,
    MemoryQuery,
    MemoryScope,
    MemoryUsage,
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

    def __init__(
        self,
        provider: MemoryProvider,
        *,
        tool_result_projector: Callable[[str, Any], str | None] | None = None,
        tool_result_max_bytes: int = 4096,
    ) -> None:
        """Configure host-approved tool evidence; raw results stay metadata-only.

        A projector must select and sanitize text that the host allows retrieval
        to expose. Built-in redaction is defense in depth, not a privacy policy.
        A zero byte budget disables evidence projection, including explicit text.
        """
        if tool_result_projector is not None and not callable(tool_result_projector):
            raise TypeError("tool_result_projector must be callable")
        if type(tool_result_max_bytes) is not int or not (
            tool_result_max_bytes == 0 or 32 <= tool_result_max_bytes <= 65_536
        ):
            raise ValueError("tool_result_max_bytes must be 0 or between 32 and 65536")
        self._provider = provider
        self._tool_result_projector = tool_result_projector
        self._tool_result_max_bytes = tool_result_max_bytes

    def _tool_evidence(self, tool_name: str, result: Any, evidence_text: str | None) -> str:
        if evidence_text is not None and not isinstance(evidence_text, str):
            raise TypeError("evidence_text must be a string or None")
        if self._tool_result_max_bytes == 0:
            return ""
        if evidence_text is None and self._tool_result_projector is not None:
            evidence_text = self._tool_result_projector(tool_name, result)
            if evidence_text is not None and not isinstance(evidence_text, str):
                raise TypeError("tool_result_projector must return a string or None")
        if not evidence_text or not evidence_text.strip():
            return ""
        # Redact the full selected text before truncating: cutting a credential
        # first could hide its recognizable prefix/suffix from the redactor.
        safe = DefaultCaptureRedactor().redact(evidence_text)
        encoded = safe.encode("utf-8")
        if len(encoded) > self._tool_result_max_bytes:
            marker = "\n[truncated]"
            budget = self._tool_result_max_bytes - len(marker.encode("utf-8"))
            safe = encoded[:budget].decode("utf-8", errors="ignore") + marker
        return safe

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

    async def after_user(
        self,
        context: AgentLifecycleContext,
        *,
        content: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> IngestResult:
        return await self._provider.ingest_event(
            MemoryEvent(
                scope=context.scope,
                event_type="user.message",
                content=content,
                idempotency_key=context.idempotency_key("after_user"),
                actor="user",
                metadata={**context.metadata, **dict(metadata or {})},
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
        evidence_text: str | None = None,
    ) -> IngestResult:
        """Record a tool event and optional host-approved, searchable evidence.

        ``evidence_text`` overrides the configured projector; an empty string
        suppresses it for this call. Neither results nor arguments are projected
        automatically. Evidence remains tool output, not an accepted user fact.
        """
        content = f"Tool {tool_name} {'succeeded' if success else 'failed'}."
        evidence = self._tool_evidence(tool_name, result, evidence_text)
        if evidence:
            content += f"\nResult evidence:\n{evidence}"
        return await self._provider.ingest_event(
            MemoryEvent(
                scope=context.scope,
                event_type="agent.tool.completed" if success else "agent.tool.failed",
                content=content,
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
        self._require_feedback()
        memory_ids = tuple(citation.memory_id for citation in bundle.citations)
        digest = sha256(f"{memory_ids}:{procedure_ids}:{policy_version}".encode()).hexdigest()
        decision = DecisionRecord(
            scope=context.scope,
            action=action,
            memory_ids=memory_ids,
            procedure_ids=procedure_ids,
            policy_version=policy_version,
            context_hash=digest,
            bundle_id=bundle.bundle_id,
            memory_usage=(MemoryUsage.CONFIRMED if memory_ids else MemoryUsage.NONE),
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
        self._require_feedback()
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
        self._require_feedback()
        reward = RewardSignal(
            scope=context.scope,
            outcome_id=outcome.id,
            value=value,
            formula_version=formula_version,
            components=components or {},
        )
        await self._provider.record_reward(reward)
        return reward

    def _require_feedback(self) -> None:
        capabilities = self._provider.manifest().capabilities
        if not (capabilities.decision_lineage and capabilities.outcome_feedback):
            raise NotImplementedError(
                "the selected memory provider does not support lifecycle feedback"
            )

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
