import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_memory.ontology_memory import (
    OntologyClass, OntologyProperty, OntologySchema, SQLiteOntologyStore,
)
from agent_memory.ontology_schema import ontology_schema_digest
from agent_memory.ontology_upgrade import register_ontology_upgrade


def schemas():
    previous = OntologySchema(
        ontology_id="upgrade.test", version="1.0.0",
        classes=(OntologyClass("person", "Person"),),
        properties=(OntologyProperty("name", "Name", "person"),),
        created_at=datetime(2026, 9, 23, tzinfo=UTC),
    )
    target = replace(
        previous, version="2.0.0",
        properties=(replace(previous.properties[0], functional=True),),
    )
    return previous, target


class RecordingStore:
    def __init__(self):
        self.writes = []

    async def register_schema(self, schema):
        self.writes.append(schema)


class HostApproval:
    def __init__(self, digest, decision=True):
        self.digest = digest
        self.decision = decision
        self.requests = []

    async def authorize(self, request):
        self.requests.append(request)
        return self.decision if request.target_digest == self.digest else False


def test_breaking_upgrade_requires_approval_before_any_write():
    async def scenario():
        previous, target = schemas()
        store = RecordingStore()
        with pytest.raises(PermissionError, match="requires host authorization"):
            await register_ontology_upgrade(store, previous, target)
        assert store.writes == []
    asyncio.run(scenario())


@pytest.mark.parametrize("decision", [False, None, 1, "approved"])
def test_non_boolean_approval_cannot_authorize_registration(decision):
    async def scenario():
        previous, target = schemas()
        store = RecordingStore()
        approval = HostApproval(ontology_schema_digest(target), decision)
        with pytest.raises(PermissionError):
            await register_ontology_upgrade(store, previous, target, authorizer=approval)
        assert store.writes == []
    asyncio.run(scenario())


def test_approval_is_bound_to_exact_target():
    async def scenario():
        previous, target = schemas()
        approval = HostApproval(ontology_schema_digest(target))
        changed = replace(target, classes=(OntologyClass("person", "Changed label"),))
        store = RecordingStore()
        with pytest.raises(PermissionError):
            await register_ontology_upgrade(store, previous, changed, authorizer=approval)
        assert store.writes == []
    asyncio.run(scenario())


def test_sqlite_registration_retains_migration_work_in_receipt(tmp_path):
    async def scenario():
        previous, target = schemas()
        store = SQLiteOntologyStore(tmp_path / "ontology.db")
        await store.initialize()
        await store.register_schema(previous)
        approval = HostApproval(ontology_schema_digest(target))
        receipt = await register_ontology_upgrade(store, previous, target, authorizer=approval)
        assert receipt.status == "schema_registered"
        assert receipt.host_authorized is True
        assert receipt.request.previous_digest == ontology_schema_digest(previous)
        assert "revalidate_existing_projections" in receipt.pending_actions
        assert "rebuild_ontology_index" in receipt.pending_actions
        assert "require_host_approval" not in receipt.pending_actions
        # Re-registration succeeds; conflicting content under the same version fails.
        await store.register_schema(target)
        conflicting = replace(target, classes=(OntologyClass("person", "Other"),))
        with pytest.raises(ValueError):
            await store.register_schema(conflicting)
        await store.register_schema(previous)
    asyncio.run(scenario())


def test_invalid_version_policy_fails_before_approval():
    async def scenario():
        previous, target = schemas()
        target = replace(target, version="1.1.0")
        store = RecordingStore()
        approval = HostApproval(ontology_schema_digest(target))
        with pytest.raises(ValueError, match="major"):
            await register_ontology_upgrade(store, previous, target, authorizer=approval)
        assert approval.requests == []
        assert store.writes == []
    asyncio.run(scenario())


def test_compatible_upgrade_and_host_veto():
    async def scenario():
        previous, _ = schemas()
        target = replace(previous, version="1.0.1")
        store = RecordingStore()
        receipt = await register_ontology_upgrade(store, previous, target)
        assert receipt.host_authorized is False
        assert receipt.pending_actions == ()
        assert store.writes == [target]
        approval = HostApproval(ontology_schema_digest(target), False)
        with pytest.raises(PermissionError):
            await register_ontology_upgrade(store, previous, target, authorizer=approval)
        assert store.writes == [target]
    asyncio.run(scenario())


def test_approval_timeout_and_storage_failure_propagate():
    class SlowApproval:
        async def authorize(self, request):
            await asyncio.Event().wait()

    class BrokenStore:
        async def register_schema(self, schema):
            raise OSError("storage unavailable")

    async def scenario():
        previous, target = schemas()
        store = RecordingStore()
        with pytest.raises(TimeoutError):
            await register_ontology_upgrade(
                store, previous, target, authorizer=SlowApproval(), approval_timeout_ms=1,
            )
        assert store.writes == []
        with pytest.raises(OSError, match="storage unavailable"):
            await register_ontology_upgrade(
                BrokenStore(), previous, target,
                authorizer=HostApproval(ontology_schema_digest(target)),
            )
    asyncio.run(scenario())
