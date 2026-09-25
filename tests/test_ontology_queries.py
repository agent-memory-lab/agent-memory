import asyncio
from datetime import UTC, datetime, timedelta
import os
from uuid import uuid4

import pytest

from agent_memory import (
    Claim, ClaimStatus, MemoryScope, MemoryQuery, OntologyClass, OntologyProperty,
    OntologySchema, OntologyConflictResolution, OntologyRetrieverPlugin,
    PluginContext, PluginResourceLimits, Provenance, SQLiteOntologyStore,
    project_claim_to_ontology, traverse_ontology,
)
from agent_memory.candidate_fusion import fuse_candidates
from agent_memory.ontology_plugin import OntologyCandidateGovernance


NOW = datetime(2026, 9, 23, tzinfo=UTC)
SCOPE = MemoryScope("query-test", user_id="alice")
SCHEMA = OntologySchema(
    "query.test", "1.0.0", (OntologyClass("person", "Person"),),
    (OntologyProperty("knows", "Knows", "person", "person"),
     OntologyProperty("name", "Name", "person", functional=True)), created_at=NOW,
)


def projection(subject, target, *, scope=SCOPE, predicate="knows"):
    return project_claim_to_ontology(Claim(
        id="claim-" + subject + target, scope=scope, key=subject + target,
        value={"$ontology": {
            "subject": {"id": subject, "class": "person", "label": subject},
            "predicate": predicate,
            "object": ({"literal": target} if predicate == "name" else
                {"id": target, "class": "person", "label": target}),
        }}, text="shared text", confidence=0.9, importance=0.5,
        status=ClaimStatus.ACTIVE, provenance=Provenance(source_event_ids=("event-" + subject + target,)),
        valid_from=NOW, created_at=NOW,
    ), SCHEMA)


@pytest.fixture(params=["sqlite", "postgres"])
def store(request, tmp_path):
    if request.param == "sqlite":
        return SQLiteOntologyStore(tmp_path / "ontology.db")
    dsn = os.environ.get("AGENT_MEMORY_ONTOLOGY_TEST_DSN")
    if not dsn:
        pytest.skip("set AGENT_MEMORY_ONTOLOGY_TEST_DSN for real PostgreSQL contract tests")
    from agent_memory_postgres import PostgresOntologyStore
    return PostgresOntologyStore(dsn, namespace="ontology_test_" + uuid4().hex)


async def prepare(store):
    await store.initialize()
    await store.register_schema(SCHEMA)


def test_exact_authorization_does_not_repeat_text_search(store):
    async def scenario():
        await prepare(store)
        for i in range(12):
            await store.upsert_projection(projection(f"person:p{i}", f"person:t{i}"))
        retriever = OntologyRetrieverPlugin(store, SCHEMA)
        ctx = PluginContext(SCOPE, PluginResourceLimits())
        await retriever.initialize(ctx)
        candidates = await retriever.retrieve(MemoryQuery(SCOPE, "person:p11"), ctx)
        assert len(candidates) == 1
        fused = fuse_candidates(candidates).candidates

        async def forbidden(*args, **kwargs):
            raise AssertionError("authorization must not search")

        store.search = forbidden
        governance = OntologyCandidateGovernance(store, SCHEMA, clock=lambda: NOW)
        assert len(await governance.resolve(SCOPE, fused)) == 1
        await store.invalidate_sources(SCOPE, candidates[0].source_event_ids)
        assert await governance.resolve(SCOPE, fused) == ()
        await retriever.close()
    asyncio.run(scenario())


def test_exact_query_scope_validity_and_identity(store):
    async def scenario():
        await prepare(store)
        value = projection("person:a", "person:b")
        await store.upsert_projection(value)
        options = dict(ontology_id=SCHEMA.ontology_id, ontology_version=SCHEMA.version)
        ids = (value.assertion.assertion_id,)
        assert len(await store.get_assertions(SCOPE, ids, at_time=NOW, **options)) == 1
        assert await store.get_assertions(MemoryScope("foreign"), ids, at_time=NOW, **options) == ()
        assert await store.get_assertions(SCOPE, ids, at_time=NOW-timedelta(seconds=1), **options) == ()
        assert await store.index_identity() == await store.index_identity()
    asyncio.run(scenario())


def test_graph_paths_cycles_and_budgets(store):
    async def scenario():
        await prepare(store)
        for a, b in (("a", "b"), ("b", "c"), ("c", "a")):
            await store.upsert_projection(projection("person:" + a, "person:" + b))
        options = dict(ontology_id=SCHEMA.ontology_id, ontology_version=SCHEMA.version, at_time=NOW)
        graph = await traverse_ontology(store, SCOPE, "person:a", target_entity="person:c", max_depth=3, **options)
        assert graph.paths and len(graph.paths[0]) == 2
        assert len(graph.edges) == 3
        small = await traverse_ontology(store, SCOPE, "person:a", max_nodes=2, max_edges=1, **options)
        assert len(small.edges) <= 1 and len(small.nodes) <= 2 and small.truncated
        tiny = await traverse_ontology(store, SCOPE, "person:a", token_budget=1, **options)
        assert tiny.edges == () and tiny.truncated
        incoming = await store.neighbors(SCOPE, ("person:c",), direction="incoming", **options)
        assert incoming[0].subject_entity_id == "person:b"
        assert await store.neighbors(SCOPE, ("person:a",), predicates=("name",), **options) == ()
    asyncio.run(scenario())


def test_graph_does_not_join_entity_ids_across_partitions(store):
    async def scenario():
        await prepare(store)
        session = MemoryScope("query-test", user_id="alice", session_id="one")
        await store.upsert_projection(projection("person:a", "person:b"))
        await store.upsert_projection(projection("person:b", "person:c", scope=session))
        graph = await traverse_ontology(store, session, "person:a", target_entity="person:c",
            ontology_id=SCHEMA.ontology_id, ontology_version=SCHEMA.version, at_time=NOW)
        assert graph.paths == ()
    asyncio.run(scenario())


def test_functional_conflict_resolution_and_evidence_invalidation(store):
    async def scenario():
        await prepare(store)
        first, second = projection("person:a", "one", predicate="name"), projection("person:a", "two", predicate="name")
        await asyncio.gather(store.upsert_projection(first), store.upsert_projection(second))
        options = dict(ontology_id=SCHEMA.ontology_id, ontology_version=SCHEMA.version)
        conflicts = await store.list_conflicts(SCOPE, **options)
        assert len(conflicts) == 2
        result = await store.resolve_conflict(OntologyConflictResolution(
            scope=SCOPE, subject_entity_id="person:a", predicate_id="name",
            winner_assertion_id=first.assertion.assertion_id,
            conflict_assertion_ids=tuple(value.assertion_id for value in conflicts),
            reason="host evidence", approved_by="host", **options,
        ))
        assert result.assertion_id == first.assertion.assertion_id
        assert len(await store.get_assertions(SCOPE, (result.assertion_id,), at_time=NOW, **options)) == 1
        await store.invalidate_sources(SCOPE, first.assertion.source_event_ids)
        assert await store.get_assertions(SCOPE, (result.assertion_id,), at_time=NOW, **options) == ()
    asyncio.run(scenario())
