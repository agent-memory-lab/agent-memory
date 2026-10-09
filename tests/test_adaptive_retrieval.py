"""Deterministic host routing and unchanged authority/time/entity boundaries."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_question_runtime_v7 as question_runtime
from test_governed_recall_integration import NOW, TrustedResolver, loaded_retriever
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory.domain import MemoryItem, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.extensions.protocol import RetrievalCandidate
from agent_memory.extensions.registry import PluginFailureMode, PluginResourceLimits
from agent_memory.retrieval.adaptive import (
    AdaptiveRetriever,
    ExplicitEntityRetrieverPlugin,
    PartialRecallResult,
    QuestionAbstention,
    RegisteredQuestionResult,
    RetrievalNeeds,
)
from agent_memory.retrieval.entity import EntityReference
from agent_memory.retrieval.question_router import QuestionRouter
from agent_memory.retrieval.scoped_lexical import ScopeIsolationError

store = question_runtime.store

SCOPE = MemoryScope("adaptive", session_id="session")


class Recorder:
    def __init__(self, name, *, fail=False):
        self.name, self.fail, self.queries = name, fail, []

    async def retrieve(self, query, context):
        self.queries.append(query)
        if self.fail:
            raise RuntimeError("unavailable")
        return (
            RetrievalCandidate(
                MemoryItem(self.name, MemoryKind.EVENT, "risk staffing", 1.0, NOW),
                query.channels[0],
                1,
                (self.name,),
                self.name,
                retrieval_method="lexical",
            ),
        )


def plugin(name, capabilities, *, fail=False):
    base = loaded_retriever(SCOPE)
    return replace(
        base,
        instance=Recorder(name, fail=fail),
        manifest=replace(base.manifest, name=name, capabilities=capabilities),
        context=replace(base.context, resource_limits=PluginResourceLimits(max_candidates=64)),
    )


def test_primary_paths_skip_unrequested_entity_time_and_graph():
    async def run():
        plugins = (
            plugin("lexical", ("lexical.search",)),
            plugin("dense", ("semantic.search",)),
            plugin("entity", ("entity.search",)),
            plugin("time", ("temporal.bitemporal",)),
            plugin("graph", ("graph.search",)),
        )
        adaptive = AdaptiveRetriever(plugins, TrustedResolver(), max_candidates=64)
        query = MemoryQuery(
            SCOPE,
            "all risks on project Alice yesterday",
            limit=8,
            token_budget=64,
            include_current_state=False,
            run_id="run",
        )
        result = await adaptive.answer(query, actor="host", dedupe_key="request")
        assert isinstance(result, PartialRecallResult)
        assert result.plan.plugin_names == ("lexical", "dense")
        assert result.plan.candidate_capacity == 16
        assert result.bundle.retrieval_metadata["coverage"] == "partial"
        assert result.bundle.retrieval_metadata["world_negative"] is False
        for p in plugins[:2]:
            assert p.instance.queries == [replace(query, limit=8)]
        assert all(p.instance.queries == [] for p in plugins[2:])

    asyncio.run(run())


def test_one_bounded_wave_degrades_to_remaining_primary_without_retry():
    async def run():
        lexical = plugin("lexical", ("lexical.search",))
        dense = plugin("dense", ("semantic.search",), fail=True)
        adaptive = AdaptiveRetriever((lexical, dense), TrustedResolver())
        result = await adaptive.retrieve_partial(MemoryQuery(SCOPE, "risk"))
        assert [i.id for i in result.bundle.relevant_memories] == ["lexical"]
        assert result.bundle.retrieval_metadata["degraded"] is True
        assert len(lexical.instance.queries) == len(dense.instance.queries) == 1

    asyncio.run(run())


def test_fail_closed_primary_never_uses_failure_as_permission_to_bypass():
    from agent_memory.extensions.registry import PluginError

    async def run():
        lexical = plugin("lexical", ("lexical.search",), fail=True)
        lexical = replace(
            lexical, manifest=replace(lexical.manifest, failure_mode=PluginFailureMode.FAIL_CLOSED)
        )
        adaptive = AdaptiveRetriever((lexical,), TrustedResolver())
        with pytest.raises(PluginError):
            await adaptive.retrieve_partial(MemoryQuery(SCOPE, "risk"))
        assert len(lexical.instance.queries) == 1

    asyncio.run(run())


def test_explicit_entity_binding_keeps_exact_inputs_and_rejects_guessed_bindings():
    class Index:
        async def search(self, entities, scope, *, limit):
            self.observed = (entities, scope, limit)
            return ()

    async def run():
        exact = (EntityReference("project", "Project A"),)
        index = Index()
        entity = ExplicitEntityRetrieverPlugin(SCOPE, exact, index)
        loaded = plugin("entity", ("entity.search",))
        loaded = replace(loaded, instance=entity, manifest=entity.plugin_manifest())
        await entity.initialize(loaded.context)
        try:
            lexical = plugin("lexical", ("lexical.search",))
            adaptive = AdaptiveRetriever((lexical, loaded), TrustedResolver())
            result = await adaptive.retrieve_partial(
                MemoryQuery(SCOPE, "risk other project"), needs=RetrievalNeeds(entities=exact)
            )
            assert "entity-candidate" in result.plan.plugin_names
            assert index.observed[0] == list(exact)
            assert index.observed[1] == SCOPE
            with pytest.raises(ValueError, match="exact host-validated"):
                adaptive.plan(
                    MemoryQuery(SCOPE, "risk"),
                    RetrievalNeeds(entities=(EntityReference("project", "project a"),)),
                )
        finally:
            await entity.close()

    asyncio.run(run())


def test_temporal_need_is_explicit_and_historical_query_is_never_cleared():
    class Snapshot:
        async def retrieve(self, query):
            self.query = query
            return (
                await AdaptiveRetriever(
                    (plugin("lexical", ("lexical.search",)),), TrustedResolver()
                ).retrieve_partial(replace(query, valid_at=None, known_at=None))
            ).bundle

    async def run():
        lexical = plugin("lexical", ("lexical.search",))
        temporal = plugin("time", ("temporal.bitemporal",))
        snapshot = Snapshot()
        adaptive = AdaptiveRetriever(
            (lexical, temporal), TrustedResolver(), snapshot_provider=snapshot
        )
        query = MemoryQuery(SCOPE, "risk", valid_at=NOW - timedelta(days=2), known_at=NOW)
        result = await adaptive.retrieve_partial(query)
        assert snapshot.query is query
        assert result.plan.path == "snapshot_provider"
        assert lexical.instance.queries == temporal.instance.queries == []
        plan = adaptive.plan(
            replace(query, valid_at=None, known_at=None), RetrievalNeeds(temporal=True)
        )
        assert plan.plugin_names == ("lexical", "time")
        with pytest.raises(NotImplementedError, match="snapshot-aware"):
            await AdaptiveRetriever((lexical,), TrustedResolver()).retrieve_partial(query)

    asyncio.run(run())


def test_real_registered_service_precedes_topk_and_never_falls_through(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            await register(svc, "risks", aliases=("all risks",))
            adaptive = AdaptiveRetriever((), TrustedResolver(), question_router=QuestionRouter(svc))
            exact = await adaptive.answer(
                MemoryQuery(scope, "all risks"), actor=ACTOR, dedupe_key="one"
            )
            assert isinstance(exact, RegisteredQuestionResult)
            assert exact.response["answer_status"] == "empty"
            assert exact.response["model_calls"] == 0
            assert exact.response["availability_status"] == "valid"
            for query, actor, parameters, expected in (
                ("unknown question", ACTOR, None, "unregistered_question"),
                ("all risks", "other-reader", None, "unregistered_question"),
                ("all risks", ACTOR, {"project_id": "project-b"}, "question_parameter_conflict"),
            ):
                result = await adaptive.answer(
                    MemoryQuery(scope, query),
                    actor=actor,
                    dedupe_key="abstain",
                    parameters=parameters,
                )
                assert isinstance(result, QuestionAbstention) and result.reason == expected
            constrained = await adaptive.answer(
                MemoryQuery(scope, "all risks", valid_at=NOW), actor=ACTOR, dedupe_key="historical"
            )
            assert constrained.reason == "question_retrieval_constraints_unsupported"
            await register(svc, "status", aliases=("all risks",))
            ambiguous = await adaptive.answer(
                MemoryQuery(scope, "all risks"), actor=ACTOR, dedupe_key="ambiguous"
            )
            assert ambiguous.reason == "ambiguous_question"
            explicit = await adaptive.answer(
                MemoryQuery(scope, "project-a:risks"), actor=ACTOR, dedupe_key="explicit"
            )
            assert isinstance(explicit, RegisteredQuestionResult)
            with pytest.raises(ScopeIsolationError):
                await adaptive.answer(
                    MemoryQuery(SCOPE, "all risks"), actor=ACTOR, dedupe_key="scope"
                )

    asyncio.run(run())


def test_historical_entity_request_is_rejected_before_provider_access():
    class Snapshot:
        async def retrieve(self, query):
            raise AssertionError("unsupported combined constraints reached provider")

    async def run():
        adaptive = AdaptiveRetriever((), TrustedResolver(), snapshot_provider=Snapshot())
        with pytest.raises(ValueError, match="historical entity retrieval"):
            await adaptive.retrieve_partial(
                MemoryQuery(SCOPE, "risk", valid_at=NOW),
                needs=RetrievalNeeds(entities=(EntityReference("project", "Project A"),)),
            )

    asyncio.run(run())


def test_real_contested_registered_response_is_forwarded_verbatim(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            for identity, value in (("alice", "Alice"), ("bob", "Bob")):
                staged = await question_runtime.project.stage(
                    svc.admission, scope, identity=identity, value=value
                )
                await question_runtime.project.qualify(svc.admission, *staged)
            await register(svc, "owner")
            original_answer = svc.answer
            observed = []

            async def recording_answer(*args, **kwargs):
                response = await original_answer(*args, **kwargs)
                observed.append(response)
                return response

            svc.answer = recording_answer
            clock[0] += timedelta(microseconds=100)
            adaptive = AdaptiveRetriever((), TrustedResolver(), question_router=QuestionRouter(svc))
            result = await adaptive.answer(
                MemoryQuery(scope, "project-a:owner"), actor=ACTOR, dedupe_key="contested"
            )
            assert result.response is observed[0]
            assert result.response["answer_status"] == "contested"
            assert len(result.response["result"]["rows"][0]["fields"][0]["candidates"]) == 2

    asyncio.run(run())


def test_hard_entity_filter_never_returns_unbound_primary_candidates_or_falls_back():
    from agent_memory.domain import MemoryChannel
    from agent_memory.retrieval.entity import ScopedEntityMatch

    class Index:
        fail = False

        async def search(self, entities, scope, *, limit):
            if self.fail:
                raise RuntimeError("entity index unavailable")
            return (
                ScopedEntityMatch(
                    scope,
                    MemoryItem("bound", MemoryKind.CLAIM, "Project A risk", 1.0, NOW),
                    MemoryChannel.SEMANTIC,
                    ("source",),
                    tuple(entities),
                    1.0,
                ),
            )

    async def run():
        exact = (EntityReference("project", "Project A"),)
        index = Index()
        entity = ExplicitEntityRetrieverPlugin(SCOPE, exact, index)
        loaded = plugin("entity", ("entity.search",))
        loaded = replace(loaded, instance=entity, manifest=entity.plugin_manifest())
        await entity.initialize(loaded.context)
        try:
            lexical = plugin("unbound-lexical", ("lexical.search",))
            dense = plugin("unbound-dense", ("semantic.search",))
            adaptive = AdaptiveRetriever((lexical, dense, loaded), TrustedResolver())
            needs = RetrievalNeeds(entities=exact, hard_entity_filter=True)
            result = await adaptive.retrieve_partial(
                MemoryQuery(SCOPE, "risk"), (object(),), needs=needs
            )
            assert result.bundle.current_state == ()
            assert result.bundle.retrieval_metadata["adaptive_hard_entity_filter"] is True
            assert result.plan.plugin_names == ("entity-candidate",)
            assert result.plan.reasons == ("hard_entity_only",)
            assert [item.id for item in result.bundle.relevant_memories] == ["bound"]
            assert lexical.instance.queries == dense.instance.queries == []
            index.fail = True
            failed = await adaptive.retrieve_partial(MemoryQuery(SCOPE, "risk"), needs=needs)
            assert failed.bundle.relevant_memories == ()
            assert failed.bundle.retrieval_metadata["degraded"] is True
            assert failed.bundle.retrieval_metadata["world_negative"] is False
            assert lexical.instance.queries == dense.instance.queries == []
            with pytest.raises(ValueError, match="historical entity retrieval"):
                await adaptive.retrieve_partial(
                    MemoryQuery(SCOPE, "risk", known_at=NOW), needs=needs
                )
        finally:
            await entity.close()

    asyncio.run(run())


def test_hard_entity_constraint_requires_a_nonempty_binding_and_supported_intersection():
    with pytest.raises(ValueError, match="requires explicit entities"):
        RetrievalNeeds(hard_entity_filter=True)
    with pytest.raises(ValueError, match="intersection is unsupported"):
        RetrievalNeeds(
            entities=(EntityReference("project", "A"),), hard_entity_filter=True, temporal=True
        )
    with pytest.raises(ValueError, match="booleans"):
        RetrievalNeeds(hard_entity_filter="yes")
