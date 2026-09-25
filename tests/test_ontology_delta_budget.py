import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import json

import pytest

from agent_memory import (
    Claim, ClaimStatus, ForgetMode, ForgetRequest, LiveOntologyMemory,
    MCPRequestContext, MemoryEvent, MemoryQuery, OntologyProperty, Provenance,
    SQLiteOntologyStore, traverse_ontology,
)
from agent_memory.ontology_queries import serialize_graph_content
from agent_memory.ontology_source import database
from agent_memory.token_budget import TokenCounter
from test_ontology_api import setup_api
from test_ontology_p0 import Approval, NOW, SCHEMA, SCOPE, context, insert, setup
from test_ontology_queries import (
    SCHEMA as GRAPH_SCHEMA, SCOPE as GRAPH_SCOPE, projection,
)


@asynccontextmanager
async def source_setup(backend, tmp_path):
    if backend == "postgres":
        from test_ontology_postgres_source import configured
        async with configured(tmp_path) as resources:
            yield resources
    else:
        yield await setup(tmp_path)


def live(source, registry, path, **options):
    return LiveOntologyMemory(source, registry, SCHEMA.ontology_id, context(),
        work_directory=path, **options)


def index_rows(path):
    with database(path, readonly=True) as db:
        return {
            table: sorted((tuple(row) for row in db.execute("SELECT * FROM " + table)), key=repr)
            for table in ("ontology_assertions", "ontology_entities",
                          "ontology_assertion_sources", "ontology_entity_sources")
        }


async def same_as_full(runtime, source, registry, tmp_path):
    async with live(source, registry, tmp_path / "full") as full:
        assert await full.refresh()
        assert index_rows(runtime._active["path"]) == index_rows(full._active["path"])


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("mode", [ForgetMode.ERASE, ForgetMode.ARCHIVE])
def test_delta_insert_replace_forget_matches_full(tmp_path, backend, mode):
    async def scenario():
        async with source_setup(backend, tmp_path) as (repo, source, registry):
            first = await insert(repo, 1)
            await insert(repo, 2)
            async with live(source, registry, tmp_path / "delta", incremental=True) as runtime:
                assert await runtime.refresh()
                identity = runtime._active["index_id"]
                await insert(repo, 3)
                assert await runtime.refresh()
                assert (await runtime._active["sink"].load()).processed_claims == 1
                assert runtime._active["index_id"] != identity
                await same_as_full(runtime, source, registry, tmp_path)
                replacement = await insert(repo, 4, previous=first, subject="person:p1")
                assert await runtime.refresh()
                assert (await runtime._active["sink"].load()).processed_claims == 1
                await same_as_full(runtime, source, registry, tmp_path)
                await repo.forget(ForgetRequest(SCOPE,
                    memory_ids=replacement.provenance.source_event_ids, mode=mode))
                assert await runtime.refresh()
                assert (await runtime._active["sink"].load()).processed_claims == 0
                await same_as_full(runtime, source, registry, tmp_path)
                async with repo.unit_of_work() as uow:
                    await uow.append_event(MemoryEvent(scope=SCOPE, event_type="test", content="no claim"))
                assert await runtime.refresh()
                assert (await runtime._active["sink"].load()).processed_claims == 0
                await same_as_full(runtime, source, registry, tmp_path)
    asyncio.run(scenario())


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_delta_resumes_selection_after_restart(tmp_path, backend):
    async def scenario():
        async with source_setup(backend, tmp_path) as (repo, source, registry):
            await insert(repo, 1)
            directory = tmp_path / "delta"
            async with live(source, registry, directory, incremental=True,
                            batch_size=1, max_batches=1) as first:
                assert await first.refresh()
                await insert(repo, 2)
                await insert(repo, 3)
                assert not await first.refresh()
                identity = first._pending["index_id"]
                assert (await first._pending["sink"].load()).processed_claims == 1
            async with live(source, registry, directory, incremental=True,
                            batch_size=1, max_batches=1) as resumed:
                assert await resumed.refresh()
                assert resumed._active["index_id"] == identity
                assert (await resumed._active["sink"].load()).processed_claims == 2
                await same_as_full(resumed, source, registry, tmp_path)
    asyncio.run(scenario())


def test_delta_rebuilds_shared_component_and_schema_changes(tmp_path):
    async def scenario():
        repo, source, registry = await setup(tmp_path)
        schema = replace(SCHEMA, version="1.1.0", properties=(*SCHEMA.properties,
            OntologyProperty("knows", "Knows", "person", "person")))
        await registry.register(SCOPE, schema)
        await registry.activate(SCOPE, SCHEMA.ontology_id, schema.version,
            expected_generation=1, reason="graph", authorizer=Approval())

        async def edge(a, b):
            event = MemoryEvent(scope=SCOPE, event_type="test", content=a + b)
            claim = Claim(id=a + b, scope=SCOPE, key=a + b,
                value={"$ontology": {"subject": {"id": a, "class": "person", "label": a},
                    "predicate": "knows", "object": {"id": b, "class": "person", "label": b}}},
                text=a + " knows " + b, confidence=0.9, importance=0.5,
                status=ClaimStatus.ACTIVE, provenance=Provenance(source_event_ids=(event.id,)),
                valid_from=NOW, created_at=NOW)
            async with repo.unit_of_work() as uow:
                await uow.append_event(event)
                await uow.save_claim(claim)
            return claim

        first = await edge("a", "b")
        await edge("b", "c")
        await edge("c", "a")
        await edge("x", "y")
        async with live(source, registry, tmp_path / "delta", incremental=True) as runtime:
            assert await runtime.refresh()
            await repo.forget(ForgetRequest(SCOPE,
                memory_ids=first.provenance.source_event_ids, mode=ForgetMode.ERASE))
            assert await runtime.refresh()
            assert (await runtime._active["sink"].load()).processed_claims == 2
            await same_as_full(runtime, source, registry, tmp_path)
            next_schema = replace(schema, version="1.1.1")
            await registry.register(SCOPE, next_schema)
            await registry.activate(SCOPE, SCHEMA.ontology_id, next_schema.version,
                expected_generation=2, reason="schema change", authorizer=Approval())
            assert await runtime.refresh()
            assert (await runtime._active["sink"].load()).processed_claims == 3
            await same_as_full(runtime, source, registry, tmp_path)
    asyncio.run(scenario())


@pytest.mark.parametrize("budget", [1, 60, 100, 800, 16000])
def test_exact_graph_budget_matches_serialized_content(tmp_path, budget):
    async def scenario():
        store = SQLiteOntologyStore(tmp_path / "graph.db")
        await store.initialize()
        await store.register_schema(GRAPH_SCHEMA)
        for a, b in (("a", "b"), ("b", "c"), ("c", "a")):
            await store.upsert_projection(projection("person:" + a, "person:" + b))
        counter = TokenCounter("test-character-tokenizer-v1", len)
        options = dict(ontology_id=GRAPH_SCHEMA.ontology_id, ontology_version=GRAPH_SCHEMA.version,
            at_time=NOW, target_entity="person:c", max_depth=3,
            token_budget=budget, token_counter=counter)
        minimum = len(serialize_graph_content((), (), (), True))
        if budget < minimum:
            with pytest.raises(ValueError, match="empty graph"):
                await traverse_ontology(store, GRAPH_SCOPE, "person:a", **options)
            return
        result = await traverse_ontology(store, GRAPH_SCOPE, "person:a", **options)
        encoded = serialize_graph_content(result.nodes, result.edges, result.paths, result.truncated)
        assert result.token_estimate == len(encoded) <= budget
        assert result.token_count_kind == "exact"
        assert result.tokenizer_id == counter.identifier
        if budget == 16000:
            assert result.paths and len(result.edges) == 3
    asyncio.run(scenario())


@pytest.mark.parametrize("invalid", [True, -1, 0, 1.5, "10"])
def test_token_counter_rejects_invalid_results(invalid):
    with pytest.raises(ValueError):
        TokenCounter("invalid", lambda _: invalid).measure("payload")


def test_api_inherits_host_counter_and_rejects_override(tmp_path):
    async def scenario():
        from agent_memory.mcp import MCPToolError
        api, memory, _ = await setup_api(tmp_path)
        async with memory:
            api.token_counter = TokenCounter("host-character-v1", len)
            result = await api.call("memory_ontology_graph", {
                "start_entity": "person:a", "token_budget": 16000,
            }, MCPRequestContext(GRAPH_SCOPE))
            graph = result["graph"]
            assert graph["token_count_kind"] == "exact"
            assert graph["tokenizer_id"] == "host-character-v1"
            assert graph["token_estimate"] <= 16000
            with pytest.raises(MCPToolError):
                await api.call("memory_ontology_graph", {
                    "start_entity": "person:a", "token_counter": "untrusted",
                }, MCPRequestContext(GRAPH_SCOPE))
    asyncio.run(scenario())
