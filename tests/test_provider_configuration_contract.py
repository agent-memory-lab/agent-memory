"""Public entry points must execute requested policy and reject broken providers."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agent_memory import AgentMemory, MemoryScope, build_local_kernel
from agent_memory.extensions.registry import PluginError, PluginRegistry
from agent_memory.retrieval.lossless_packing import LosslessBudget

SCOPE = MemoryScope("configuration-contract", session_id="one")
BUDGET = LosslessBudget(
    lambda state, items: 10 + sum(len(x.text) for x in (*state, *items)), "test-characters"
)


@pytest.mark.parametrize("entry", ["direct", "plugin", "local"])
@pytest.mark.parametrize(
    "options, schema, version",
    [
        ({"packing_version": "v5"}, "native-evidence-packing/1", "v5"),
        ({"lossless_packing_budget": BUDGET}, "lossless-ranked-units/1", None),
    ],
)
def test_requested_packing_is_visible_in_actual_delivery(tmp_path, entry, options, schema, version):
    async def run():
        path = tmp_path / "memory.db"
        if entry == "direct":
            memory = AgentMemory(build_local_kernel(path, **options), SCOPE)
        elif entry == "plugin":
            memory = AgentMemory.from_plugin("sqlite", scope=SCOPE, database_path=path, **options)
        else:
            memory = AgentMemory.local(path, scope=SCOPE, **options)
        async with memory:
            await memory.remember("Project budget is 300 tokens.")
            bundle = await memory.recall("budget")
        contract = bundle.retrieval_metadata["packing_contract"]
        assert contract["schema"] == schema
        assert contract.get("version") == version
        if version is None:
            assert bundle.retrieval_metadata["packing_final_delivery_recounted"] is True

    asyncio.run(run())


@pytest.mark.parametrize("options", [{"packing_version": "invalid"}, {"packing_verison": "v5"}])
def test_plugin_rejects_invalid_or_unknown_configuration(tmp_path, options):
    with pytest.raises((TypeError, ValueError)):
        AgentMemory.from_plugin(
            "sqlite", scope=SCOPE, database_path=tmp_path / "memory.db", **options
        )
    assert not (tmp_path / "memory.db").exists()


@pytest.mark.parametrize("value", [None, lambda *args: None])
def test_provider_registration_rejects_incomplete_contract(value):
    registry = PluginRegistry()
    registry._builtin_providers = {
        "broken": lambda: SimpleNamespace(
            initialize=value, ingest_event=value, retrieve=value, manifest=value
        )
    }
    with pytest.raises(PluginError) as caught:
        registry.create_provider("broken")
    assert caught.value.code.value == "invalid_implementation"


def test_strict_delivery_requires_advertised_support_before_initialization(tmp_path):
    kernel = build_local_kernel(tmp_path / "memory.db")

    class LegacyProvider:
        def __getattr__(self, name):
            return getattr(kernel, name)

        def manifest(self):
            manifest = kernel.manifest()
            return replace(
                manifest, capabilities=replace(manifest.capabilities, final_delivery_guard=False)
            )

    AgentMemory(LegacyProvider(), SCOPE)  # Explicit strict mode preserves legacy construction.
    with pytest.raises(ValueError, match="required final delivery"):
        AgentMemory(LegacyProvider(), SCOPE, require_delivery_guard=True)
    assert not (tmp_path / "memory.db").exists()


def test_pipeline_also_checks_final_complete_payload_budget(tmp_path):
    from agent_memory.domain import MemoryBundle

    async def run():
        kernel = build_local_kernel(
            tmp_path / "memory.db",
            lossless_packing_budget=LosslessBudget(
                lambda state, items: 2048, "fixed-prompt-overhead"
            ),
        )

        class Pipeline:
            async def retrieve(self, query, state):
                return MemoryBundle((), (), (), (), (), 0, {}, kernel.manifest().capabilities)

        async with AgentMemory(
            kernel, SCOPE, recall_pipeline=Pipeline(), require_delivery_guard=True
        ) as memory:
            with pytest.raises(ValueError, match="final delivery payload exceeds token budget"):
                await memory.recall("budget", token_budget=64)

    asyncio.run(run())


def test_declared_guard_requires_callable_implementation(tmp_path):
    kernel = build_local_kernel(tmp_path / "memory.db")
    kernel.guard_pipeline_delivery = None
    registry = PluginRegistry()
    registry._builtin_providers = {"broken": lambda: kernel}
    with pytest.raises(PluginError, match="final_delivery_guard"):
        registry.create_provider("broken")


def test_provider_cannot_register_unimplemented_protocol_stubs(tmp_path):
    from agent_memory.ports import MemoryProvider

    manifest = build_local_kernel(tmp_path / "memory.db").manifest()

    class IncompleteProvider(MemoryProvider):
        def manifest(self):
            return manifest

    registry = PluginRegistry()
    registry._builtin_providers = {"incomplete": IncompleteProvider}
    with pytest.raises(PluginError, match="ingest_event"):
        registry.create_provider("incomplete")


def test_repository_history_stub_is_not_advertised_or_called(tmp_path, monkeypatch):
    from agent_memory.domain import MemoryQuery
    from agent_memory.ports import BitemporalMemoryRepository
    from agent_memory.sqlite import SQLiteMemoryRepository

    monkeypatch.setattr(SQLiteMemoryRepository, "claims_at", BitemporalMemoryRepository.claims_at)
    kernel = build_local_kernel(tmp_path / "memory.db")
    assert not kernel.manifest().capabilities.bitemporal_claims
    now = datetime.now(UTC)
    with pytest.raises(NotImplementedError, match="bitemporal"):
        asyncio.run(kernel.get_state_at(SCOPE, valid_at=now, known_at=now))
    with pytest.raises(NotImplementedError, match="bitemporal"):
        asyncio.run(kernel.retrieve(MemoryQuery(SCOPE, "history", valid_at=now)))
    assert not (tmp_path / "memory.db").exists()


def test_explicit_passage_reranker_requires_an_implemented_method(tmp_path):
    from agent_memory.ports import Reranker

    class EmptyReranker(Reranker):
        pass

    with pytest.raises(ValueError, match="rerank"):
        AgentMemory.from_plugin(
            "sqlite", scope=SCOPE, database_path=tmp_path / "memory.db",
            packing_version="v5", passage_reranker=EmptyReranker(),
        )
    assert not (tmp_path / "memory.db").exists()


def test_strict_delivery_rejects_runtime_downgrade(tmp_path):
    async def run():
        async with AgentMemory.local(
            tmp_path / "memory.db", scope=SCOPE, require_delivery_guard=True
        ) as memory:
            memory.provider._repository.guard_retrieval_delivery = None
            with pytest.raises(RuntimeError, match="required final delivery guard"):
                await memory.recall("budget")

    asyncio.run(run())


def test_native_kernel_strict_mode_checks_the_final_receipt(tmp_path):
    from agent_memory.domain import MemoryQuery

    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db", require_delivery_guard=True)

        async def unsupported(query, bundle):
            return replace(bundle, retrieval_metadata={"delivery_guard": {"status": "unsupported"}})

        await kernel.initialize()
        try:
            kernel._repository.guard_retrieval_delivery = unsupported
            with pytest.raises(RuntimeError, match="did not validate"):
                await kernel.retrieve(MemoryQuery(SCOPE, "budget"))
        finally:
            await kernel.close()

    asyncio.run(run())


def test_postgres_rejects_explicit_unsupported_source_context_before_connecting():
    pg = pytest.importorskip("agent_memory_postgres")
    with pytest.raises(ValueError, match="does not support adjacent source-context"):
        pg.build_postgres_kernel("unused", enable_source_context=True)


@pytest.mark.parametrize("entry", ["direct", "plugin"])
@pytest.mark.parametrize(
    "options, schema",
    [
        ({"packing_version": "v5"}, "native-evidence-packing/1"),
        ({"lossless_packing_budget": BUDGET}, "lossless-ranked-units/1"),
    ],
)
def test_postgres_factory_accepts_the_same_native_policy(entry, options, schema):
    import os
    from urllib.parse import urlsplit
    from uuid import uuid4

    dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires isolated PostgreSQL")
    assert "test" in urlsplit(dsn).path.casefold()
    pg = pytest.importorskip("agent_memory_postgres")
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql
    from psycopg.conninfo import make_conninfo

    async def run():
        namespace = "factory_" + uuid4().hex
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace)))
        scoped_dsn = make_conninfo(dsn, options="-csearch_path=" + namespace)
        try:
            if entry == "direct":
                memory = AgentMemory(
                    pg.build_postgres_kernel(scoped_dsn, enable_source_context=False, **options),
                    SCOPE,
                    require_delivery_guard=True,
                )
            else:
                memory = AgentMemory.from_plugin(
                    "postgres",
                    dsn=scoped_dsn,
                    scope=SCOPE,
                    enable_source_context=False,
                    require_delivery_guard=True,
                    **options,
                )
            async with memory:
                await memory.remember("Project budget is 300 tokens.")
                bundle = await memory.recall("budget")
            contract = bundle.retrieval_metadata["packing_contract"]
            assert contract["schema"] == schema
            if "packing_version" in options:
                assert contract["version"] == options["packing_version"]
            assert (
                bundle.retrieval_metadata["source_context_expansion"]["status"] == "disabled_config"
            )
        finally:
            with psycopg.connect(dsn, autocommit=True) as connection:
                connection.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace))
                )

    asyncio.run(run())
