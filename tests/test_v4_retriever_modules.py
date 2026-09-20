"""Acceptance tests for semantic, temporal, entity, and parallel retrieval modules."""

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from agent_memory.candidate_fusion import CandidateFusionResult
from agent_memory.domain import MemoryChannel, MemoryItem, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.entity_retriever import (
    EntityReference,
    EntityRetrieverPlugin,
    ScopedEntityMatch,
)
from agent_memory.parallel_retrieval import ParallelRetrieverOrchestrator
from agent_memory.plugin_loader import LoadedPlugin, PluginCandidateReference
from agent_memory.plugin_protocol import PluginContext, PluginHealth, PluginHealthStatus, RetrievalCandidate
from agent_memory.plugins import (
    PluginError,
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)
from agent_memory.semantic_retriever import (
    ScopedSemanticMatch,
    SemanticIndexDescriptor,
    SemanticIndexState,
    SemanticRetrieverPlugin,
    SemanticVectorSpec,
    VectorNormalization,
)
from agent_memory.temporal_retriever import ScopedTemporalMatch, TemporalRetrieverPlugin

NOW = datetime(2026, 9, 19, tzinfo=timezone.utc)
SCOPE = MemoryScope("tenant-a", session_id="session-a")


class FixedClock:
    def now(self):
        return NOW


def item(item_id="memory-1"):
    return MemoryItem(item_id, MemoryKind.EVENT, "Acme migration", 0.5, NOW)


def context(scope=SCOPE, *, timeout_ms=1000, max_candidates=8, max_batch_size=16):
    return PluginContext(
        scope=scope,
        resource_limits=PluginResourceLimits(
            timeout_ms=timeout_ms,
            max_candidates=max_candidates,
            max_batch_size=max_batch_size,
            max_concurrency=2,
        ),
        request_id="request-1",
        clock=FixedClock(),
    )


def query(scope=SCOPE):
    return MemoryQuery(scope=scope, text="Acme migration", limit=8, token_budget=256)


class Embedder:
    def __init__(self, spec):
        self.spec = spec

    def vector_spec(self):
        return self.spec

    async def embed_query(self, text):
        return (3.0, 4.0)


class SemanticStore:
    def __init__(self, descriptor, matches):
        self.descriptor = descriptor
        self.matches = matches
        self.vector = None

    async def describe(self):
        return self.descriptor

    async def search(self, vector, scope, *, limit):
        self.vector = tuple(vector)
        return self.matches


def test_semantic_contract_normalizes_and_versions_candidates():
    async def scenario():
        spec = SemanticVectorSpec("provider", "model-v1", 2, VectorNormalization.L2, "v1")
        descriptor = SemanticIndexDescriptor(spec, "index-v1", SemanticIndexState.READY)
        store = SemanticStore(
            descriptor,
            (ScopedSemanticMatch(SCOPE, item(), MemoryChannel.SEMANTIC, ("event-1",), 0.9, "index-v1"),),
        )
        plugin = SemanticRetrieverPlugin(Embedder(spec), store)
        plugin_context = context()
        await plugin.initialize(plugin_context)
        candidates = await plugin.retrieve(query(), plugin_context)
        assert store.vector == pytest.approx((0.6, 0.8))
        assert candidates[0].retrieval_method == "semantic"
        assert candidates[0].metadata["index_version"] == "index-v1"

        store.descriptor = replace(descriptor, state=SemanticIndexState.REBUILDING)
        with pytest.raises(PluginError, match="rebuilding"):
            await plugin.retrieve(query(), plugin_context)

    asyncio.run(scenario())


class TemporalStore:
    def __init__(self, matches):
        self.matches = matches

    async def search(self, text, scope, window, *, limit):
        return self.matches


def test_temporal_contract_enforces_valid_and_recorded_time():
    async def scenario():
        visible = ScopedTemporalMatch(
            SCOPE, item(), MemoryChannel.SEMANTIC, ("event-1",),
            NOW - timedelta(days=2), None, NOW - timedelta(days=1), 0.8,
        )
        plugin = TemporalRetrieverPlugin(TemporalStore((visible,)))
        plugin_context = context()
        await plugin.initialize(plugin_context)
        candidates = await plugin.retrieve(query(), plugin_context)
        assert candidates[0].retrieval_method == "temporal"

        future = replace(visible, recorded_at=NOW + timedelta(days=1))
        plugin._index.matches = (future,)
        with pytest.raises(PluginError, match="outside"):
            await plugin.retrieve(query(), plugin_context)

    asyncio.run(scenario())


class Resolver:
    async def resolve(self, text, scope):
        return (EntityReference("organization", "Acme"),)


class EntityStore:
    def __init__(self, matches):
        self.matches = matches

    async def search(self, entities, scope, *, limit):
        return self.matches


def test_entity_contract_rejects_scope_and_entity_expansion():
    async def scenario():
        entity = EntityReference("organization", "Acme")
        matched = ScopedEntityMatch(
            SCOPE, item(), MemoryChannel.SEMANTIC, ("event-1",), (entity,), 0.7
        )
        store = EntityStore((matched,))
        plugin = EntityRetrieverPlugin(Resolver(), store)
        plugin_context = context()
        await plugin.initialize(plugin_context)
        candidates = await plugin.retrieve(query(), plugin_context)
        assert candidates[0].retrieval_method == "entity"

        store.matches = (replace(matched, scope=MemoryScope("tenant-b")),)
        with pytest.raises(PluginError, match="another scope"):
            await plugin.retrieve(query(), plugin_context)
        store.matches = (replace(matched, entities=(EntityReference("organization", "Other"),)),)
        with pytest.raises(PluginError, match="expanded"):
            await plugin.retrieve(query(), plugin_context)

    asyncio.run(scenario())


class CandidatePlugin:
    def __init__(self, values=(), *, delay=0, error=None):
        self.values = values
        self.delay = delay
        self.error = error

    async def retrieve(self, query, plugin_context):
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error:
            raise self.error
        return self.values


def loaded(name, method, plugin, *, failure_mode=PluginFailureMode.FALLBACK, timeout_ms=1000):
    descriptor = PluginManifest(
        name=name,
        version="0.1.0",
        kind=PluginKind.RETRIEVER,
        capabilities=(f"{method}.search",),
        requires={"core": ">=0.1,<1.0"},
        config_schema={"type": "object"},
        resource_limits=PluginResourceLimits(timeout_ms=timeout_ms),
        failure_mode=failure_mode,
    )
    return LoadedPlugin(
        PluginCandidateReference(name, PluginKind.RETRIEVER, "test", name, None),
        descriptor,
        plugin,
        context(timeout_ms=timeout_ms),
        PluginHealth(PluginHealthStatus.READY),
    )


def candidate(method, memory_id="memory-1"):
    return RetrievalCandidate(
        item=item(memory_id),
        channel=MemoryChannel.SEMANTIC,
        rank=1,
        source_event_ids=("event-1",),
        retriever=method,
        retrieval_method=method,
    )


def test_parallel_retrieval_degrades_fallback_and_fuses_deterministically():
    async def scenario():
        plugins = (
            loaded("semantic", "semantic", CandidatePlugin((candidate("semantic"),))),
            loaded("lexical", "lexical", CandidatePlugin((candidate("lexical"),))),
            loaded("slow", "temporal", CandidatePlugin(delay=0.05), timeout_ms=1),
        )
        result = await ParallelRetrieverOrchestrator().retrieve(query(), plugins)
        assert isinstance(result.fusion, CandidateFusionResult)
        assert len(result.fusion.candidates) == 1
        assert set(result.fusion.candidates[0].method_ranks) == {
            ("lexical", 1), ("semantic", 1)
        }
        assert result.degraded is True
        assert [trace.name for trace in result.traces] == ["lexical", "semantic", "slow"]
        assert next(trace for trace in result.traces if trace.name == "slow").status == "failed"

    asyncio.run(scenario())


def test_parallel_retrieval_honors_fail_closed():
    async def scenario():
        required = loaded(
            "required",
            "semantic",
            CandidatePlugin(error=RuntimeError("offline")),
            failure_mode=PluginFailureMode.FAIL_CLOSED,
        )
        with pytest.raises(PluginError, match="required retriever"):
            await ParallelRetrieverOrchestrator().retrieve(query(), (required,))

    asyncio.run(scenario())
