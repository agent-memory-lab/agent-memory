import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_memory import (
    AgentMemory,
    CallableOntologyEvidenceVerifier,
    Claim,
    ClaimStatus,
    ConsolidationRequest,
    MemoryScope,
    OntologyClass,
    OntologyProperty,
    OntologyRegistryConflict,
    OntologySchema,
    PluginContext,
    PluginError,
    PluginResourceLimits,
    Provenance,
    SQLiteOntologyRegistry,
    SQLiteOntologyStore,
    open_active_ontology_memory,
)


NOW = datetime(2026, 9, 23, tzinfo=UTC)
SCOPE = MemoryScope("active-runtime", user_id="alice")


class Approval:
    async def authorize(self, request):
        return True


async def evidence_exists(scope, ids):
    return scope == SCOPE and ids == ("event-name",)


def context():
    return PluginContext(SCOPE, PluginResourceLimits(max_batch_size=64))


def schema():
    return OntologySchema(
        "runtime.test", "1.0.0", (OntologyClass("person", "Person"),),
        (OntologyProperty("name", "Name", "person"),), created_at=NOW,
    )


async def registry_at(path):
    registry = SQLiteOntologyRegistry(path)
    await registry.initialize()
    await registry.register(SCOPE, schema())
    await registry.register(SCOPE, replace(schema(), version="1.0.1"))
    await registry.activate(
        SCOPE, "runtime.test", "1.0.0", expected_generation=0,
        reason="initial index ready", authorizer=Approval(),
    )
    return registry


async def switch(registry):
    await registry.activate(
        SCOPE, "runtime.test", "1.0.1", expected_generation=1,
        reason="next index ready", authorizer=Approval(),
    )


def test_active_version_is_pinned_and_plugins_close(tmp_path):
    async def scenario():
        registry = await registry_at(tmp_path / "registry.db")
        store = SQLiteOntologyStore(tmp_path / "ontology.db")
        verifier = CallableOntologyEvidenceVerifier(evidence_exists)
        async with open_active_ontology_memory(
            registry, "runtime.test", context(), store, verifier,
        ) as active:
            assert active.activation.version == "1.0.0"
            await switch(registry)
            assert active.activation.version == "1.0.0"
            assert active.extension.retriever.health.details["ontology_version"] == "1.0.0"
            async with AgentMemory.local(
                tmp_path / "memory.db", scope=SCOPE, recall_pipeline=active.recall_pipeline,
            ) as memory:
                assert (await memory.recall("name")).retrieval_metadata["degraded"] is False
        with pytest.raises(PluginError, match="not active"):
            await active.consolidate(ConsolidationRequest(scope=SCOPE))
        async with open_active_ontology_memory(
            registry, "runtime.test", context(), store, verifier,
        ) as reopened:
            assert reopened.activation.version == "1.0.1"
    asyncio.run(scenario())


def test_missing_or_foreign_activation_does_not_initialize_index(tmp_path):
    async def scenario():
        registry = await registry_at(tmp_path / "registry.db")
        path = tmp_path / "unused.db"
        foreign = replace(context(), scope=MemoryScope("other-tenant"))
        with pytest.raises(LookupError, match="no ontology version"):
            async with open_active_ontology_memory(
                registry, "runtime.test", foreign, SQLiteOntologyStore(path),
                CallableOntologyEvidenceVerifier(evidence_exists),
            ):
                pytest.fail("foreign activation must not be opened")
        assert not path.exists()
    asyncio.run(scenario())


def test_activation_change_during_loading_closes_owned_plugins(tmp_path, monkeypatch):
    from agent_memory import ontology_runtime

    loaders = []
    original = ontology_runtime.PluginLoader

    def recording_loader():
        loader = original()
        loaders.append(loader)
        return loader

    monkeypatch.setattr(ontology_runtime, "PluginLoader", recording_loader)

    async def scenario():
        registry = await registry_at(tmp_path / "registry.db")

        class SwitchingStore(SQLiteOntologyStore):
            switched = False

            async def register_schema(self, value):
                await super().register_schema(value)
                if not self.switched:
                    self.switched = True
                    await switch(registry)

        with pytest.raises(OntologyRegistryConflict, match="changed while"):
            async with open_active_ontology_memory(
                registry, "runtime.test", context(), SwitchingStore(tmp_path / "ontology.db"),
                CallableOntologyEvidenceVerifier(evidence_exists),
            ):
                pytest.fail("outdated loading result must not be yielded")
        assert loaders[0].loaded == ()
    asyncio.run(scenario())


def test_host_exception_closes_plugins(tmp_path):
    async def scenario():
        registry = await registry_at(tmp_path / "registry.db")
        with pytest.raises(ValueError, match="host failed"):
            async with open_active_ontology_memory(
                registry, "runtime.test", context(), SQLiteOntologyStore(tmp_path / "ontology.db"),
                CallableOntologyEvidenceVerifier(evidence_exists),
            ) as active:
                raise ValueError("host failed")
        with pytest.raises(PluginError, match="not active"):
            await active.consolidate(ConsolidationRequest(scope=SCOPE))
    asyncio.run(scenario())


def test_changed_catalog_document_is_rejected_before_loading(tmp_path):
    async def scenario():
        registry = await registry_at(tmp_path / "registry.db")

        class CorruptCatalog:
            async def active(self, scope, ontology_id):
                return await registry.active(scope, ontology_id)

            async def get(self, scope, ontology_id, version):
                value = await registry.get(scope, ontology_id, version)
                return replace(value, classes=(OntologyClass("person", "Altered"),))

        path = tmp_path / "unused.db"
        with pytest.raises(OntologyRegistryConflict, match="does not match"):
            async with open_active_ontology_memory(
                CorruptCatalog(), "runtime.test", context(), SQLiteOntologyStore(path),
                CallableOntologyEvidenceVerifier(evidence_exists),
            ):
                pytest.fail("changed document must not be loaded")
        assert not path.exists()
    asyncio.run(scenario())
