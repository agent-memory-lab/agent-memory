import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from agent_memory import MemoryScope, PluginContext, PluginResourceLimits
from agent_memory.ontology_layers import LayeredOntologyMemory, _LayeredView
from agent_memory.ontology_rules import OntologyRuleEngine, RelationRule
from test_ontology_queries import NOW, SCHEMA, SCOPE, prepare, projection, store


OPTIONS = dict(ontology_id=SCHEMA.ontology_id, ontology_version=SCHEMA.version, at_time=NOW)
CHILD = replace(SCOPE, session_id="child")


class Evidence:
    available = True

    async def verify(self, scope, ids):
        return self.available


def test_layer_functional_override_and_nonfunctional_union(store):
    async def scenario():
        await prepare(store)
        parent = projection("person:a", "old", predicate="name")
        child = projection("person:a", "new", predicate="name", scope=CHILD)
        links = (projection("person:a", "person:b"), projection("person:a", "person:c", scope=CHILD))
        for value in (parent, child, *links):
            await store.upsert_projection(value)
        view = _LayeredView(CHILD, SCHEMA, ((SCOPE, store), (CHILD, store)))
        values = await view.get_assertions(CHILD, (parent.assertion.assertion_id, child.assertion.assertion_id), **OPTIONS)
        assert [a.assertion_id for a in values] == [child.assertion.assertion_id]
        edges = await view.neighbors(CHILD, ("person:a",), predicates=("knows",), **OPTIONS)
        assert {a.assertion_id for a in edges} == {p.assertion.assertion_id for p in links}
        parent_values = await view.get_assertions(SCOPE, (parent.assertion.assertion_id,), **OPTIONS)
        assert len(parent_values) == 1
        with pytest.raises(ValueError, match="ancestry"):
            await view.get_assertions(MemoryScope("foreign"), (parent.assertion.assertion_id,), **OPTIONS)
    asyncio.run(scenario())


def test_layer_nonmatching_child_hides_parent_search(store):
    async def scenario():
        await prepare(store)
        parent = projection("person:a", "old", predicate="name")
        child = projection("person:a", "new", predicate="name", scope=CHILD)
        parent = replace(parent, assertion=replace(parent.assertion, text="parentonly"))
        child = replace(child, assertion=replace(child.assertion, text="childonly"))
        await store.upsert_projection(parent)
        await store.upsert_projection(child)
        view = _LayeredView(CHILD, SCHEMA, ((SCOPE, store), (CHILD, store)))
        assert await view.search("parentonly", CHILD, **OPTIONS) == ()
    asyncio.run(scenario())


def test_layer_child_conflict_fails_closed(store):
    async def scenario():
        await prepare(store)
        parent = projection("person:a", "old", predicate="name")
        for value in (parent, projection("person:a", "one", scope=CHILD, predicate="name"),
                      projection("person:a", "two", scope=CHILD, predicate="name")):
            await store.upsert_projection(value)
        view = _LayeredView(CHILD, SCHEMA, ((SCOPE, store), (CHILD, store)))
        assert await view.get_assertions(CHILD, (parent.assertion.assertion_id,), **OPTIONS) == ()
    asyncio.run(scenario())


@pytest.mark.parametrize("scopes", [(), (SCOPE, SCOPE), (MemoryScope("foreign"),)])
def test_layer_constructor_rejects_invalid_ancestry(scopes):
    ctx = PluginContext(CHILD, PluginResourceLimits())
    layers = [SimpleNamespace(context=SimpleNamespace(scope=s), ontology_id=SCHEMA.ontology_id) for s in scopes]
    with pytest.raises(ValueError):
        LayeredOntologyMemory(ctx, None, SCHEMA.ontology_id, layers)


def test_rule_proof_revalidation_and_no_active_write(store):
    async def scenario():
        await prepare(store)
        roots = (projection("person:a", "person:b"), projection("person:b", "person:c"))
        for value in roots:
            await store.upsert_projection(value)
        evidence = Evidence()
        engine = OntologyRuleEngine(store, SCHEMA, evidence, (RelationRule("chain", "1", "knows", "knows", "knows"),))
        result = await engine.derive(SCOPE, tuple(v.assertion.assertion_id for v in roots), at_time=NOW)
        assert len(result.candidates) == 1
        candidate = result.candidates[0]
        assert (candidate.subject, candidate.object, candidate.status) == ("person:a", "person:c", "candidate")
        assert set(candidate.source_event_ids) == {e for v in roots for e in v.assertion.source_event_ids}
        assert await engine.valid(candidate, at_time=NOW)
        assert not await engine.valid(replace(candidate, confidence=0.1), at_time=NOW)
        assert len(await store.neighbors(SCOPE, ("person:a",), **OPTIONS)) == 1
        evidence.available = False
        assert not await engine.valid(candidate, at_time=NOW)
        evidence.available = True
        await store.invalidate_sources(SCOPE, roots[0].assertion.source_event_ids)
        assert not await engine.valid(candidate, at_time=NOW)
    asyncio.run(scenario())


def test_rules_reject_ancestor_premise_and_bound_results(store):
    async def scenario():
        await prepare(store)
        roots = tuple(projection("person:" + a, "person:" + b) for a, b in (("a", "b"), ("b", "c"), ("c", "d")))
        for value in roots:
            await store.upsert_projection(value)
        engine = OntologyRuleEngine(store, SCHEMA, Evidence(), (RelationRule("chain", "1", "knows", "knows", "knows"),), max_candidates=1)
        ids = tuple(v.assertion.assertion_id for v in roots)
        with pytest.raises(ValueError, match="exact scope"):
            await engine.derive(CHILD, ids, at_time=NOW)
        result = await engine.derive(SCOPE, ids, at_time=NOW)
        assert len(result.candidates) == 1 and result.truncated
        with pytest.raises(ValueError, match="timezone"):
            await engine.derive(SCOPE, ids, at_time=NOW.replace(tzinfo=None))
    asyncio.run(scenario())
