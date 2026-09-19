"""End-to-end opt-in governance through the existing AgentMemory.recall API."""

import asyncio
from datetime import datetime, timezone

import pytest

from agent_memory import AgentMemory, MemoryScope
from agent_memory.candidate_fusion import FusedCandidate
from agent_memory.candidate_guard import GovernedCandidate
from agent_memory.domain import MemoryChannel, MemoryItem, MemoryKind
from agent_memory.governed_recall import GovernedRecallPipeline
from agent_memory.plugin_loader import LoadedPlugin, PluginCandidateReference
from agent_memory.plugin_protocol import (
    PluginContext,
    PluginHealth,
    PluginHealthStatus,
    RetrievalCandidate,
)
from agent_memory.plugins import (
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)
from agent_memory.scoped_lexical_retrieval import ScopeIsolationError


NOW = datetime(2026, 9, 19, tzinfo=timezone.utc)


class FixedRetriever:
    def __init__(self, value: RetrievalCandidate) -> None:
        self.value = value

    async def retrieve(self, query, context):
        return (self.value,)


class TrustedResolver:
    async def resolve(self, scope, candidates):
        return tuple(GovernedCandidate(scope, candidate) for candidate in candidates)


class InjectingResolver:
    async def resolve(self, scope, candidates):
        injected = FusedCandidate(
            item=MemoryItem("injected", MemoryKind.EVENT, "not retrieved", 1.0, NOW),
            score=1.0,
            source_event_ids=("event-injected",),
            method_ranks=(("semantic", 1),),
            retrievers=("malicious",),
        )
        return (*tuple(GovernedCandidate(scope, value) for value in candidates), GovernedCandidate(scope, injected))


def loaded_retriever(scope: MemoryScope) -> LoadedPlugin:
    candidate = RetrievalCandidate(
        item=MemoryItem("plugin-memory", MemoryKind.EPISODE, "governed result", 0.8, NOW),
        channel=MemoryChannel.EPISODIC,
        rank=1,
        source_event_ids=("event-1",),
        retriever="integration",
        retrieval_method="semantic",
    )
    manifest = PluginManifest(
        name="integration-retriever",
        version="0.1.0",
        kind=PluginKind.RETRIEVER,
        capabilities=("semantic.search",),
        requires={"core": ">=0.1,<1.0"},
        config_schema={"type": "object"},
        resource_limits=PluginResourceLimits(max_candidates=8),
        failure_mode=PluginFailureMode.FALLBACK,
    )
    context = PluginContext(
        scope=scope,
        resource_limits=PluginResourceLimits(max_candidates=8),
        request_id="request-1",
    )
    return LoadedPlugin(
        PluginCandidateReference(
            "integration-retriever",
            PluginKind.RETRIEVER,
            "test",
            "test:FixedRetriever",
            None,
        ),
        manifest,
        FixedRetriever(candidate),
        context,
        PluginHealth(PluginHealthStatus.READY),
    )


def test_existing_recall_uses_configured_governed_pipeline(tmp_path) -> None:
    async def scenario() -> None:
        scope = MemoryScope("tenant-a", session_id="session-a")
        pipeline = GovernedRecallPipeline((loaded_retriever(scope),), TrustedResolver())
        async with AgentMemory.local(
            tmp_path / "memory.db",
            scope=scope,
            recall_pipeline=pipeline,
        ) as memory:
            bundle = await memory.recall("governed")

        assert [value.id for value in bundle.episodes] == ["plugin-memory"]
        assert bundle.citations[0].source_event_ids == ("event-1",)
        assert bundle.retrieval_metadata["policy_version"] == "governed-recall-v1"
        assert bundle.retrieval_metadata["degraded"] is False
        assert pipeline.last_trace is not None
        assert pipeline.last_trace.packing.included_count == 1

    asyncio.run(scenario())


def test_governance_cannot_inject_a_candidate_into_recall(tmp_path) -> None:
    async def scenario() -> None:
        scope = MemoryScope("tenant-a", session_id="session-a")
        pipeline = GovernedRecallPipeline((loaded_retriever(scope),), InjectingResolver())
        async with AgentMemory.local(
            tmp_path / "memory.db",
            scope=scope,
            recall_pipeline=pipeline,
        ) as memory:
            with pytest.raises(ScopeIsolationError, match="injected"):
                await memory.recall("governed")

    asyncio.run(scenario())
