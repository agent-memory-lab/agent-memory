"""Candidate budgets are spent after authorization and feasible packing."""
import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest

from agent_memory.domain import MemoryChannel, MemoryItem, MemoryKind, MemoryQuery
from agent_memory.retrieval.bundle import BundleBudget
from agent_memory.retrieval.diversity import DiversityBudget
from agent_memory.retrieval.governed import GovernedRecallPipeline
from agent_memory.retrieval.guard import GovernedCandidate
from agent_memory.extensions.protocol import RetrievalCandidate
from agent_memory.extensions.registry import PluginResourceLimits
from test_governed_recall_integration import NOW, TrustedResolver, loaded_retriever
from agent_memory import MemoryScope

SCOPE = MemoryScope('headroom', session_id='session')


def candidate(index, *, text='evidence', source=None):
    return RetrievalCandidate(
        MemoryItem(f'item-{index:02d}', MemoryKind.EVENT, text, 1.0, NOW),
        MemoryChannel.SEMANTIC, index + 1, (source or f'source-{index}',),
        'headroom', retrieval_method='lexical',
    )


class Pool:
    def __init__(self, items):
        self.items = items
        self.limits = []

    async def retrieve(self, query, context):
        self.limits.append(query.limit)
        return self.items[:query.limit]


def pipeline(items, resolver=None, **kwargs):
    loaded = loaded_retriever(SCOPE)
    pool = Pool(items)
    loaded = replace(loaded, instance=pool, context=replace(
        loaded.context, resource_limits=PluginResourceLimits(max_candidates=32)))
    return GovernedRecallPipeline((loaded,), resolver or TrustedResolver(), **kwargs), pool


def test_authorization_filters_do_not_starve_final_budget():
    class RejectFirst:
        async def resolve(self, scope, candidates):
            return tuple(GovernedCandidate(scope, c, deleted=c.item.id < 'item-08') for c in candidates)

    async def scenario():
        p, pool = pipeline(tuple(candidate(i) for i in range(12)), RejectFirst())
        bundle = await p.retrieve(MemoryQuery(SCOPE, 'evidence', limit=4), ())
        assert [c.id for c in bundle.relevant_memories] == [f'item-{i:02d}' for i in range(8,12)]
        assert pool.limits == [16]
        assert p.last_trace.guard.input_count == 12
        assert not any(c.id < 'item-08' for c in bundle.relevant_memories)
    asyncio.run(scenario())


def test_source_quota_headroom_and_oversize_backfill():
    async def scenario():
        items = (candidate(0, text='x'*4000, source='shared'),
                 *(candidate(i, source='shared') for i in range(1,9)),
                 candidate(9), candidate(10))
        p, _ = pipeline(items,
            diversity_budget=DiversityBudget(max_items=3, max_per_source_event=1),
            bundle_budget=BundleBudget(max_items=3, max_characters=100, max_tokens=64))
        bundle = await p.retrieve(MemoryQuery(SCOPE, 'evidence', limit=3), ())
        assert [c.id for c in bundle.relevant_memories] == ['item-01','item-09','item-10']
        assert bundle.retrieval_metadata['dropped_character_budget'] == 1
        assert p.last_trace.diversity.dropped_source_quota == 7
    asyncio.run(scenario())


def test_direct_governed_historical_query_never_silently_uses_now():
    async def scenario():
        p, pool = pipeline((candidate(0),))
        with pytest.raises(NotImplementedError, match='historical'):
            await p.retrieve(MemoryQuery(SCOPE, 'evidence', valid_at=NOW-timedelta(days=1)), ())
        assert pool.limits == []
    asyncio.run(scenario())


def test_packing_preserves_request_and_query_policy_metadata():
    async def scenario():
        p, _ = pipeline((candidate(0),))
        query = MemoryQuery(SCOPE, 'evidence', request_id='request-specific',
                            run_id='run-specific', policy_version='query-policy')
        bundle = await p.retrieve(query, ())
        assert bundle.request_id == query.request_id
        assert bundle.retrieval_metadata['query_policy_version'] == query.policy_version
        assert bundle.retrieval_metadata['run_id'] == query.run_id
        assert bundle.retrieval_metadata['coverage'] == 'partial'
        assert bundle.retrieval_metadata['world_negative'] is False
    asyncio.run(scenario())


def test_included_state_can_be_disabled_without_exposing_it():
    async def scenario():
        p, _ = pipeline((candidate(0),))
        # Deliberately invalid as a claim: excluded input must never be packed.
        bundle = await p.retrieve(MemoryQuery(SCOPE, 'evidence', include_current_state=False), (object(),))
        assert bundle.current_state == ()
        assert len(bundle.relevant_memories) == 1
    asyncio.run(scenario())


def test_large_final_budget_can_use_single_plugin_capacity():
    async def scenario():
        loaded = loaded_retriever(SCOPE)
        pool = Pool(tuple(candidate(i) for i in range(100)))
        loaded = replace(loaded, instance=pool, context=replace(
            loaded.context, resource_limits=PluginResourceLimits(max_candidates=100)))
        p = GovernedRecallPipeline((loaded,), TrustedResolver(),
            diversity_budget=DiversityBudget(max_items=100, max_per_kind={MemoryKind.EVENT: 100}),
            bundle_budget=BundleBudget(max_items=100))
        bundle = await p.retrieve(MemoryQuery(SCOPE, 'evidence', limit=100), ())
        assert len(bundle.relevant_memories) == 100
        assert pool.limits == [100]
    asyncio.run(scenario())


def test_injected_orchestrator_work_budget_is_respected():
    from agent_memory.retrieval.parallel import ParallelRetrieverOrchestrator

    async def scenario():
        p, pool = pipeline(tuple(candidate(i) for i in range(16)),
            orchestrator=ParallelRetrieverOrchestrator(max_candidates=8, max_results=8))
        bundle = await p.retrieve(MemoryQuery(SCOPE, 'evidence', limit=4), ())
        assert len(bundle.relevant_memories) == 4
        assert pool.limits == [8]
        assert p.last_trace.guard.input_count == 8
    asyncio.run(scenario())
