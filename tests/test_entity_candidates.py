"""Registered spelling lookup and alias lifecycle, not automatic NER tests."""

import asyncio
from dataclasses import replace

import pytest
from test_ontology_memory import _claim, _schema

from agent_memory.domain import MemoryScope
from agent_memory.ontology.model import project_claim_to_ontology
from agent_memory.ontology.store import SQLiteOntologyStore

SCOPE = MemoryScope("lookup", user_id="alice", session_id="one")
SCHEMA = _schema()


def projection(scope=SCOPE, *, identity="person:a", label="Alice", aliases=("Al",), source="s1", schema=SCHEMA):
    base = _claim(scope, source)
    raw = {"$ontology": {
        "subject": {"id": identity, "class": "person", "label": label, "aliases": aliases},
        "predicate": "display_name", "object": {"literal": label},
    }}
    return project_claim_to_ontology(replace(base, value=raw), schema)


async def lookup(store, reference, *, scope=SCOPE, version=SCHEMA.version, ontology=SCHEMA.ontology_id, **kwargs):
    return await store.find_entity_candidates(
        scope, reference, ontology_id=ontology, ontology_version=version, **kwargs,
    )


def test_exact_candidates_never_infer_an_alias_or_merge_homonyms(tmp_path):
    async def run():
        store = SQLiteOntologyStore(tmp_path / "db")
        await store.initialize()
        await store.register_schema(SCHEMA)
        await store.upsert_projection(projection())
        assert (await lookup(store, "Al")).entity_ids == ("person:a",)
        assert (await lookup(store, "Alice")).status == "candidate"
        assert (await lookup(store, "person:a")).status == "candidate"
        for unknown in ("AL", "Al ", "Ali", "A. Smith", "I", "she", "Unknown"):
            assert (await lookup(store, unknown)).status == "unknown"
        await store.upsert_projection(projection(identity="person:b", label="Alex", source="s2"))
        result = await lookup(store, "Al")
        assert (result.scope, result.ontology_id, result.ontology_version) == (
            SCOPE, SCHEMA.ontology_id, SCHEMA.version
        )
        assert result.status == "ambiguous" and result.entity_ids == ("person:a", "person:b")
        # An ID-looking alias does not trump another exact identifier.
        await store.upsert_projection(projection(identity="person:c", label="Carol", aliases=("person:a",), source="s3"))
        assert (await lookup(store, "person:a")).status == "ambiguous"
    asyncio.run(run())


@pytest.mark.parametrize("foreign", [
    replace(SCOPE, tenant_id="other"), replace(SCOPE, user_id="other"),
    replace(SCOPE, namespace="other"), replace(SCOPE, session_id="other"),
    MemoryScope("lookup"),
])
def test_exact_partition_does_not_inherit_or_leak_parent_scope(tmp_path, foreign):
    async def run():
        store = SQLiteOntologyStore(tmp_path / "db")
        await store.initialize()
        await store.register_schema(SCHEMA)
        await store.upsert_projection(projection(foreign))
        assert (await lookup(store, "Al")).status == "unknown"
        assert (await lookup(store, "Al", scope=foreign)).status == "candidate"
        assert (await lookup(store, "Al", scope=foreign, version="2")).status == "unknown"
        assert (await lookup(store, "Al", scope=foreign, ontology="other")).status == "unknown"
    asyncio.run(run())


def test_truncated_scan_is_not_false_uniqueness_or_absence(tmp_path):
    async def run():
        store = SQLiteOntologyStore(tmp_path / "db")
        await store.initialize()
        await store.register_schema(SCHEMA)
        await store.upsert_projection(projection())
        await store.upsert_projection(projection(identity="person:b", label="Bob", source="s2"))
        for spelling in ("Al", "Alice", "unknown"):
            result = await lookup(store, spelling, max_scan=1)
            assert result.status == "incomplete" and not result.complete and not result.entity_ids
        assert (await lookup(store, "Alice", max_scan=2)).status == "candidate"
    asyncio.run(run())


def test_any_source_withdrawal_clears_unattributable_aggregate_aliases(tmp_path):
    async def run():
        store = SQLiteOntologyStore(tmp_path / "db")
        await store.initialize()
        await store.register_schema(SCHEMA)
        await store.upsert_projection(projection(aliases=("Deleted alias",), source="s1"))
        await store.upsert_projection(projection(aliases=("Surviving alias",), source="s2"))
        assert (await lookup(store, "Deleted alias")).status == "candidate"
        await store.invalidate_sources(SCOPE, ("s1",))
        assert (await lookup(store, "Alice")).status == "candidate"
        assert (await lookup(store, "Deleted alias")).status == "unknown"
        assert (await lookup(store, "Surviving alias")).status == "unknown"
        await store.upsert_projection(projection(aliases=("Surviving alias",), source="s2"))
        assert (await lookup(store, "Surviving alias")).status == "candidate"
        await store.invalidate_sources(SCOPE, ("s2",))
        assert (await lookup(store, "person:a")).status == "unknown"
        await store.upsert_projection(projection(aliases=(), source="s3"))
        assert (await lookup(store, "Deleted alias")).status == "unknown"
        assert (await lookup(store, "Surviving alias")).status == "unknown"
        assert (await lookup(store, "Alice")).status == "candidate"
    asyncio.run(run())


def test_source_invalidation_is_partition_isolated(tmp_path):
    async def run():
        foreign = replace(SCOPE, user_id="bob")
        store = SQLiteOntologyStore(tmp_path / "db")
        await store.initialize()
        await store.register_schema(SCHEMA)
        await store.upsert_projection(projection())
        await store.upsert_projection(projection(foreign))
        await store.invalidate_sources(SCOPE, ("s1",))
        assert (await lookup(store, "Al")).status == "unknown"
        assert (await lookup(store, "Al", scope=foreign)).status == "candidate"
    asyncio.run(run())


@pytest.mark.parametrize("kwargs", [{"max_scan": 0}, {"max_scan": True}, {"max_scan": 4097}, {"reference": ""}, {"scope": "bad"}])
def test_lookup_validates_host_arguments(tmp_path, kwargs):
    async def run():
        store = SQLiteOntologyStore(tmp_path / "db")
        await store.initialize()
        await store.register_schema(SCHEMA)
        options = dict(reference="Al", scope=SCOPE, ontology_id=SCHEMA.ontology_id, ontology_version=SCHEMA.version)
        with pytest.raises((ValueError, TypeError)):
            await store.find_entity_candidates(**(options | kwargs))
    asyncio.run(run())


def test_conflicting_identity_and_failed_invalidation_do_not_partially_rewrite_aliases(tmp_path):
    from agent_memory.ontology.model import OntologyValidationError

    async def run():
        store = SQLiteOntologyStore(tmp_path / "db")
        await store.initialize()
        await store.register_schema(SCHEMA)
        await store.upsert_projection(projection())
        with pytest.raises(OntologyValidationError, match="identity conflicts"):
            await store.upsert_projection(projection(label="Somebody else", aliases=("New alias",)))
        assert (await lookup(store, "New alias")).status == "unknown"
        assert (await lookup(store, "Al")).entity_ids == ("person:a",)
        await store.upsert_projection(projection(identity="person:b", label="Bob"))
        with pytest.raises(OntologyValidationError, match="max_rows"):
            await store.invalidate_sources(SCOPE, ("s1",), max_rows=1)
        assert (await lookup(store, "Al")).entity_ids == ("person:a", "person:b")
    asyncio.run(run())


def test_registered_versions_with_same_identity_do_not_share_aliases(tmp_path):
    async def run():
        store = SQLiteOntologyStore(tmp_path / "db")
        await store.initialize()
        await store.register_schema(SCHEMA)
        other = _schema("2.0.0")
        await store.register_schema(other)
        await store.upsert_projection(projection())
        await store.upsert_projection(projection(schema=other, aliases=("Alicia",)))
        assert (await lookup(store, "Al")).status == "candidate"
        assert (await lookup(store, "Al", version=other.version)).status == "unknown"
        assert (await lookup(store, "Alicia")).status == "unknown"
        assert (await lookup(store, "Alicia", version=other.version)).status == "candidate"
    asyncio.run(run())
