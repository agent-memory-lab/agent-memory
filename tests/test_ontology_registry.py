import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_memory import (
    MemoryScope, OntologyClass, OntologyProperty, OntologySchema,
    OntologyRegistryConflict, SQLiteOntologyRegistry,
)


SCOPE = MemoryScope("catalog-test", user_id="alice")
OTHER = MemoryScope("catalog-test", user_id="bob")


def schema(version="1.0.0"):
    return OntologySchema(
        "catalog.test", version, (OntologyClass("person", "Person"),),
        (OntologyProperty("name", "Name", "person"),),
        created_at=datetime(2026, 9, 23, tzinfo=UTC),
    )


class Approval:
    def __init__(self, decision=True):
        self.decision = decision

    async def authorize(self, request):
        return self.decision


async def prepare(path):
    registry = SQLiteOntologyRegistry(path)
    await registry.initialize()
    for version in ("1.0.0", "1.0.1", "1.0.2"):
        await registry.register(SCOPE, schema(version))
    return registry


def test_catalog_is_scoped_immutable_and_persistent(tmp_path):
    async def scenario():
        path = tmp_path / "registry.db"
        registry = await prepare(path)
        await registry.register(SCOPE, schema())
        with pytest.raises(OntologyRegistryConflict):
            await registry.register(SCOPE, replace(schema(), classes=(OntologyClass("person", "New"),)))
        assert await registry.versions(OTHER, "catalog.test") == ()
        with pytest.raises(KeyError):
            await registry.get(OTHER, "catalog.test", "1.0.0")
        restarted = SQLiteOntologyRegistry(path)
        assert await restarted.get(SCOPE, "catalog.test", "1.0.0") == schema()
        assert await restarted.versions(SCOPE, "catalog.test", limit=1) == ("1.0.0",)
        assert await restarted.versions(SCOPE, "catalog.test", after="1.0.0") == ("1.0.1", "1.0.2")
    asyncio.run(scenario())


def test_activate_rollback_and_audit_survive_restart(tmp_path):
    async def scenario():
        path = tmp_path / "registry.db"
        registry = await prepare(path)
        for generation, version in enumerate(("1.0.0", "1.0.1")):
            result = await registry.activate(
                SCOPE, "catalog.test", version, expected_generation=generation,
                reason="validated target index", authorizer=Approval(),
            )
            assert result.generation == generation + 1
        result = await registry.rollback(
            SCOPE, "catalog.test", "1.0.0", expected_generation=2,
            reason="host observed regression", authorizer=Approval(),
        )
        assert result.generation == 3
        restarted = SQLiteOntologyRegistry(path)
        assert await restarted.active_schema(SCOPE, "catalog.test") == schema()
        history = await restarted.history(SCOPE, "catalog.test")
        assert [entry.action for entry in history] == ["activate", "activate", "rollback"]
        assert history[-1].from_version == "1.0.1"
        assert history[-1].to_version == "1.0.0"
        assert await restarted.history(SCOPE, "catalog.test", after_generation=2) == (history[-1],)
        assert await restarted.active(OTHER, "catalog.test") is None
        assert await restarted.history(OTHER, "catalog.test") == ()
    asyncio.run(scenario())


@pytest.mark.parametrize("decision", [False, None, "approved", 1])
def test_denied_approval_never_mutates_active_or_audit(tmp_path, decision):
    async def scenario():
        registry = await prepare(tmp_path / "registry.db")
        with pytest.raises(PermissionError):
            await registry.activate(
                SCOPE, "catalog.test", "1.0.0", expected_generation=0,
                reason="bootstrap", authorizer=Approval(decision),
            )
        assert await registry.active(SCOPE, "catalog.test") is None
        assert await registry.history(SCOPE, "catalog.test") == ()
    asyncio.run(scenario())


def test_concurrent_approved_switches_have_one_winner(tmp_path):
    class BarrierApproval:
        def __init__(self):
            self.count = 0
            self.ready = asyncio.Event()

        async def authorize(self, request):
            self.count += 1
            if self.count == 2:
                self.ready.set()
            await self.ready.wait()
            return True

    async def scenario():
        registry = await prepare(tmp_path / "registry.db")
        approval = BarrierApproval()
        results = await asyncio.gather(*(
            registry.activate(
                SCOPE, "catalog.test", version, expected_generation=0,
                reason="parallel bootstrap", authorizer=approval,
            ) for version in ("1.0.0", "1.0.1")
        ), return_exceptions=True)
        assert sum(isinstance(result, OntologyRegistryConflict) for result in results) == 1
        assert (await registry.active(SCOPE, "catalog.test")).generation == 1
        assert len(await registry.history(SCOPE, "catalog.test")) == 1
    asyncio.run(scenario())


def test_unseen_rollback_target_and_stale_generation_are_rejected(tmp_path):
    async def scenario():
        registry = await prepare(tmp_path / "registry.db")
        await registry.activate(
            SCOPE, "catalog.test", "1.0.1", expected_generation=0,
            reason="bootstrap", authorizer=Approval(),
        )
        with pytest.raises(ValueError, match="never been active"):
            await registry.rollback(
                SCOPE, "catalog.test", "1.0.0", expected_generation=1,
                reason="unseen", authorizer=Approval(),
            )
        with pytest.raises(OntologyRegistryConflict):
            await registry.activate(
                SCOPE, "catalog.test", "1.0.2", expected_generation=0,
                reason="stale", authorizer=Approval(),
            )
        assert (await registry.active(SCOPE, "catalog.test")).version == "1.0.1"
        assert len(await registry.history(SCOPE, "catalog.test")) == 1
    asyncio.run(scenario())
