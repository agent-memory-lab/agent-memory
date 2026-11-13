"""Alias withdrawal contracts independent of the entity-candidate lookup API."""

import asyncio
import json
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
from test_ontology_memory import _claim, _schema

from agent_memory.domain import MemoryScope
from agent_memory.ontology.model import OntologyValidationError, project_claim_to_ontology
from agent_memory.ontology.store import SQLiteOntologyStore

SCOPE = MemoryScope("aliases", user_id="alice", session_id="one")
SCHEMA = _schema()


def projection(*, source="s1", aliases=("Al",), scope=SCOPE, identity="person:a", label="Alice"):
    raw = {"$ontology": {
        "subject": {"id": identity, "class": "person", "label": label, "aliases": aliases},
        "predicate": "display_name", "object": {"literal": label},
    }}
    return project_claim_to_ontology(replace(_claim(scope, source), value=raw), SCHEMA)


def entity(path, *, scope=SCOPE, identity="person:a"):
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM ontology_entities WHERE partition_key=? AND ontology_id=? "
            "AND ontology_version=? AND entity_id=?",
            (scope.partition_key(), SCHEMA.ontology_id, SCHEMA.version, identity),
        ).fetchone()
        return dict(row)


def test_withdrawal_clears_all_aliases_until_surviving_sources_are_reprojected(tmp_path):
    async def run():
        path = tmp_path / "aliases.db"
        store = SQLiteOntologyStore(path)
        await store.initialize()
        await store.register_schema(SCHEMA)
        await store.upsert_projection(projection(aliases=("Deleted alias",)))
        await store.upsert_projection(projection(source="s2", aliases=("Surviving alias",)))
        assert set(json.loads(entity(path)["aliases_json"])) == {"Deleted alias", "Surviving alias"}
        await store.invalidate_sources(SCOPE, ("s1",))
        remaining = entity(path)
        assert json.loads(remaining["aliases_json"]) == []
        assert json.loads(remaining["source_event_ids_json"]) == ["s2"]
        assert remaining["label"] == "Alice" and remaining["archived_at"] is None
        await store.upsert_projection(projection(source="s2", aliases=("Surviving alias",)))
        assert json.loads(entity(path)["aliases_json"]) == ["Surviving alias"]
        await store.invalidate_sources(SCOPE, ("s2",))
        assert entity(path)["archived_at"] is not None
        assert json.loads(entity(path)["aliases_json"]) == []
        await store.upsert_projection(projection(source="s3", aliases=()))
        revived = entity(path)
        assert revived["archived_at"] is None and revived["label"] == "Alice"
        assert json.loads(revived["aliases_json"]) == []
        assert json.loads(revived["source_event_ids_json"]) == ["s3"]
    asyncio.run(run())


def test_alias_withdrawal_does_not_change_other_partitions(tmp_path):
    async def run():
        path = tmp_path / "aliases.db"
        store = SQLiteOntologyStore(path)
        foreign = replace(SCOPE, user_id="bob")
        await store.initialize()
        await store.register_schema(SCHEMA)
        await store.upsert_projection(projection())
        await store.upsert_projection(projection(scope=foreign))
        before = entity(path, scope=foreign)
        await store.invalidate_sources(SCOPE, ("s1",))
        assert json.loads(entity(path)["aliases_json"]) == []
        assert entity(path, scope=foreign) == before
    asyncio.run(run())


def test_conflicting_projection_and_overflow_roll_back_alias_changes(tmp_path):
    async def run():
        path = tmp_path / "aliases.db"
        store = SQLiteOntologyStore(path)
        await store.initialize()
        await store.register_schema(SCHEMA)
        await store.upsert_projection(projection())
        before = entity(path)
        with pytest.raises(OntologyValidationError, match="identity conflicts"):
            await store.upsert_projection(projection(label="Other", aliases=("Wrong alias",)))
        assert entity(path) == before
        await store.upsert_projection(projection(identity="person:b", label="Bob"))
        other_before = entity(path, identity="person:b")
        with pytest.raises(OntologyValidationError, match="max_rows"):
            await store.invalidate_sources(SCOPE, ("s1",), max_rows=1)
        assert entity(path) == before
        assert entity(path, identity="person:b") == other_before
    asyncio.run(run())
