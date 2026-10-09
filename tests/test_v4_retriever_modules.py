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


@pytest.mark.parametrize("limit", [3, 20])
def test_parallel_preserves_every_query_field_when_bounding_limit(limit):
    async def scenario():
        class RecordingPlugin(CandidatePlugin):
            async def retrieve(self, query, plugin_context):
                self.received = query
                return ()

        plugin = RecordingPlugin()
        entry = loaded("recording", "temporal", plugin)
        entry = replace(
            entry,
            manifest=replace(entry.manifest, capabilities=("temporal.bitemporal",)),
        )
        original = MemoryQuery(
            scope=SCOPE,
            text="Acme migration",
            limit=limit,
            token_budget=384,
            include_current_state=False,
            channels=(MemoryChannel.PROCEDURAL,),
            request_id="caller-request",
            run_id="caller-run",
            policy_version="caller-policy",
            trace_enabled=False,
            valid_at=NOW - timedelta(days=10),
            known_at=NOW - timedelta(days=5),
        )
        await ParallelRetrieverOrchestrator().retrieve(original, (entry,))
        assert plugin.received == replace(original, limit=min(limit, 8))
        assert original.limit == limit

    asyncio.run(scenario())


@pytest.mark.parametrize("valid_days,known_days", [(None, None), (10, None), (None, 5), (10, 5)])
def test_default_temporal_window_preserves_each_explicit_axis(valid_days, known_days):
    from agent_memory.retrieval.temporal import CurrentTemporalWindow

    valid_at = NOW - timedelta(days=valid_days) if valid_days is not None else None
    known_at = NOW - timedelta(days=known_days) if known_days is not None else None
    window = CurrentTemporalWindow().resolve(
        replace(query(), valid_at=valid_at, known_at=known_at), context()
    )
    assert window.valid_at == (valid_at or NOW)
    assert window.recorded_before == (known_at or NOW)


def test_parallel_temporal_retriever_returns_the_historical_version():
    async def scenario():
        historical = ScopedTemporalMatch(
            SCOPE, item("historical"), MemoryChannel.SEMANTIC, ("event-old",),
            NOW - timedelta(days=20), NOW - timedelta(days=5),
            NOW - timedelta(days=15), 0.8,
        )
        plugin = TemporalRetrieverPlugin(TemporalStore((historical,)))
        entry = loaded("temporal-bitemporal", "temporal", plugin)
        entry = replace(entry, manifest=plugin.plugin_manifest())
        await plugin.initialize(entry.context)
        result = await ParallelRetrieverOrchestrator().retrieve(
            replace(query(), valid_at=NOW - timedelta(days=10), known_at=NOW - timedelta(days=7)),
            (entry,),
        )
        assert [value.item.id for value in result.fusion.candidates] == ["historical"]
        assert not result.degraded

    asyncio.run(scenario())


def test_explicit_temporal_query_cannot_be_overridden_by_a_custom_resolver():
    from agent_memory.retrieval.temporal import TemporalWindow

    async def scenario():
        class CurrentOnlyResolver:
            def resolve(self, query, context):
                return TemporalWindow(NOW, NOW)

        plugin = TemporalRetrieverPlugin(TemporalStore(()), CurrentOnlyResolver())
        plugin_context = context()
        await plugin.initialize(plugin_context)
        with pytest.raises(PluginError, match="explicit"):
            await plugin.retrieve(
                replace(query(), valid_at=NOW - timedelta(days=10)), plugin_context
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("axis", ["valid_at", "known_at"])
@pytest.mark.parametrize("failure_mode", [PluginFailureMode.FALLBACK, PluginFailureMode.FAIL_CLOSED])
def test_parallel_history_requires_a_bitemporal_capability(axis, failure_mode):
    async def scenario():
        class CurrentOnlyPlugin(CandidatePlugin):
            called = False

            async def retrieve(self, query, plugin_context):
                self.called = True
                return (candidate("semantic"),)

        plugin = CurrentOnlyPlugin()
        entry = loaded("current-only", "semantic", plugin, failure_mode=failure_mode)
        historical = replace(query(), **{axis: NOW - timedelta(days=1)})
        if failure_mode is PluginFailureMode.FAIL_CLOSED:
            with pytest.raises(PluginError, match="required retriever"):
                await ParallelRetrieverOrchestrator().retrieve(historical, (entry,))
        else:
            result = await ParallelRetrieverOrchestrator().retrieve(historical, (entry,))
            assert not result.fusion.candidates
            assert result.degraded
            assert result.traces[0].status == "failed"
        assert not plugin.called

    asyncio.run(scenario())


@pytest.mark.parametrize("channels", [(), (MemoryChannel.PROCEDURAL,)])
def test_parallel_filters_unrequested_channels_before_fusion(channels):
    async def scenario():
        entry = loaded("semantic", "semantic", CandidatePlugin((candidate("semantic"),)))
        result = await ParallelRetrieverOrchestrator().retrieve(
            replace(query(), channels=channels), (entry,)
        )
        assert not result.fusion.candidates
        assert result.traces[0].candidate_count == 1
        assert result.fusion.trace.input_count == 0

    asyncio.run(scenario())


def built_in_plugin(name):
    """Provide valid current evidence without imposing constraints in the test stores."""
    if name == "semantic":
        spec = SemanticVectorSpec("provider", "model-v1", 2, VectorNormalization.L2, "v1")
        return SemanticRetrieverPlugin(
            Embedder(spec),
            SemanticStore(
                SemanticIndexDescriptor(spec, "index-v1", SemanticIndexState.READY),
                (ScopedSemanticMatch(SCOPE, item(), MemoryChannel.SEMANTIC, ("event-1",), 0.9, "index-v1"),),
            ),
        )
    if name == "entity":
        return EntityRetrieverPlugin(
            Resolver(), EntityStore((ScopedEntityMatch(
                SCOPE, item(), MemoryChannel.SEMANTIC, ("event-1",),
                (EntityReference("organization", "Acme"),), 0.7,
            ),)),
        )
    if name == "temporal":
        return TemporalRetrieverPlugin(TemporalStore((ScopedTemporalMatch(
            SCOPE, item(), MemoryChannel.SEMANTIC, ("event-1",),
            NOW - timedelta(days=2), None, NOW - timedelta(days=1), 0.8,
        ),)))
    from agent_memory.retrieval.lexical import EvidenceItem
    from agent_memory.retrieval.retriever_plugin import ScopedLexicalRetrieverPlugin
    from agent_memory.retrieval.scoped_lexical import ScopedEvidenceItem

    class Source:
        def load(self, scope, *, limit):
            return (ScopedEvidenceItem(
                SCOPE, EvidenceItem(item(), MemoryChannel.SEMANTIC, ("event-1",))
            ),)

    return ScopedLexicalRetrieverPlugin(Source())


@pytest.mark.parametrize("name", ["semantic", "entity", "lexical"])
@pytest.mark.parametrize("axis", ["valid_at", "known_at"])
def test_current_only_plugins_reject_explicit_temporal_queries(name, axis):
    async def scenario():
        plugin = built_in_plugin(name)
        plugin_context = context()
        await plugin.initialize(plugin_context)
        with pytest.raises(PluginError, match="historical"):
            await plugin.retrieve(
                replace(query(), **{axis: NOW - timedelta(days=1)}), plugin_context
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("name", ["semantic", "entity", "lexical", "temporal"])
@pytest.mark.parametrize("channels", [(), (MemoryChannel.PROCEDURAL,)])
def test_builtin_plugins_honor_channel_constraints_on_direct_calls(name, channels):
    async def scenario():
        plugin = built_in_plugin(name)
        plugin_context = context()
        await plugin.initialize(plugin_context)
        assert await plugin.retrieve(replace(query(), channels=channels), plugin_context) == ()
        # An excluded call must not mutate or suppress a later ordinary query.
        assert len(await plugin.retrieve(query(), plugin_context)) == 1

    asyncio.run(scenario())


def test_scoped_lexical_filters_channels_before_spending_result_budget():
    from agent_memory.retrieval.lexical import EvidenceItem
    from agent_memory.retrieval.retriever_plugin import ScopedLexicalRetrieverPlugin
    from agent_memory.retrieval.scoped_lexical import ScopedEvidenceItem

    class Source:
        def load(self, scope, *, limit):
            return (
                ScopedEvidenceItem(SCOPE, EvidenceItem(
                    item("excluded"), MemoryChannel.SEMANTIC, ("excluded-event",)
                )),
                ScopedEvidenceItem(SCOPE, EvidenceItem(
                    replace(item("eligible"), text="Acme"), MemoryChannel.EPISODIC, ("eligible-event",)
                )),
            )

    async def scenario():
        plugin = ScopedLexicalRetrieverPlugin(Source())
        plugin_context = context()
        await plugin.initialize(plugin_context)
        values = await plugin.retrieve(
            replace(query(), limit=1, channels=(MemoryChannel.EPISODIC,)), plugin_context
        )
        assert [value.item.id for value in values] == ["eligible"]
        assert values[0].rank == 1

    asyncio.run(scenario())


def test_channel_exclusions_do_not_bypass_global_raw_candidate_budget():
    async def scenario():
        entry = loaded("semantic", "semantic", CandidatePlugin((
            candidate("semantic", "one"), candidate("semantic", "two")
        )))
        with pytest.raises(PluginError, match="global candidate budget"):
            await ParallelRetrieverOrchestrator(max_candidates=1, max_results=1).retrieve(
                replace(query(), channels=()), (entry,)
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("name", ["semantic", "entity", "lexical", "temporal"])
def test_channel_exclusions_do_not_mask_foreign_scope_evidence(name):
    from agent_memory.retrieval.scoped_lexical import ScopeIsolationError

    async def scenario():
        plugin = built_in_plugin(name)
        plugin_context = context()
        await plugin.initialize(plugin_context)
        foreign_scope = MemoryScope("foreign-tenant")
        if name == "lexical":
            records = plugin._source.load(SCOPE, limit=8)

            class ForeignSource:
                def load(self, scope, *, limit):
                    return tuple(replace(value, scope=foreign_scope) for value in records)

            plugin._source = ForeignSource()
        else:
            plugin._index.matches = tuple(
                replace(value, scope=foreign_scope) for value in plugin._index.matches
            )
        expected_error = ScopeIsolationError if name == "lexical" else PluginError
        with pytest.raises(expected_error, match="scope"):
            await plugin.retrieve(replace(query(), channels=()), plugin_context)

    asyncio.run(scenario())


@pytest.mark.parametrize("name", ["semantic", "entity", "temporal"])
def test_current_index_channel_filter_can_underfill_a_bounded_pool(name):
    """These upstream index APIs cannot apply channels before their own limit."""
    async def scenario():
        plugin = built_in_plugin(name)
        excluded = plugin._index.matches[0]
        eligible = replace(
            excluded,
            item=replace(excluded.item, id="eligible"),
            channel=MemoryChannel.EPISODIC,
            score=0.1,
        )
        plugin._index.matches = (excluded, eligible)
        requested_limits = []

        async def strict_search(*args, limit):
            requested_limits.append(limit)
            return plugin._index.matches[:limit]

        plugin._index.search = strict_search
        plugin_context = context(max_candidates=2)
        await plugin.initialize(plugin_context)
        episodic_query = replace(query(), limit=1, channels=(MemoryChannel.EPISODIC,))
        # The empty result describes only this bounded pool, never all evidence.
        assert await plugin.retrieve(episodic_query, plugin_context) == ()
        values = await plugin.retrieve(replace(episodic_query, limit=2), plugin_context)
        assert [value.item.id for value in values] == ["eligible"]
        assert all(value.channel is MemoryChannel.EPISODIC for value in values)
        assert values[0].rank == 1
        # No hidden refill or request beyond the configured candidate budget.
        assert requested_limits == [1, 2]

    asyncio.run(scenario())
