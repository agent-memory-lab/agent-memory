import asyncio

import pytest

from agent_memory import AgentMemory, MCPMemoryTools, MCPRequestContext, OntologyAPI, SQLiteOntologyRegistry, SQLiteOntologyStore
from agent_memory_sdk import EmbeddedMemoryClient, MCPMemoryClient, MemoryClientError
from test_ontology_queries import SCHEMA, SCOPE, projection


async def setup_api(tmp_path):
    store = SQLiteOntologyStore(tmp_path / "ontology.db")
    await store.initialize()
    await store.register_schema(SCHEMA)
    value = projection("person:a", "person:b")
    await store.upsert_projection(value)
    registry = SQLiteOntologyRegistry(tmp_path / "registry.db")
    await registry.initialize()
    await registry.register(SCOPE, SCHEMA)

    class Approval:
        async def authorize(self, request):
            return True

    await registry.activate(SCOPE, SCHEMA.ontology_id, SCHEMA.version,
        expected_generation=0, reason="host approved", authorizer=Approval())
    api = OntologyAPI(store, registry, SCHEMA.ontology_id)
    memory = AgentMemory.local(tmp_path / "core.db", scope=SCOPE)
    await memory.initialize()
    return api, memory, value


def test_embedded_sdk_ontology_calls_and_scope_override_rejection(tmp_path):
    async def scenario():
        api, memory, value = await setup_api(tmp_path)
        client = EmbeddedMemoryClient(memory.provider, MCPRequestContext(SCOPE), ontology=api)
        assert (await client.ontology_status())["active"]["version"] == SCHEMA.version
        assert len((await client.ontology_search("shared"))["matches"]) == 1
        assert len((await client.ontology_assertions([value.assertion.assertion_id]))["assertions"]) == 1
        graph = await client.ontology_graph("person:a", target_entity="person:b")
        assert graph["graph"]["paths"]
        with pytest.raises(MemoryClientError):
            await client.ontology_graph("person:a", scope={"tenant_id": "foreign"})
        with pytest.raises(MemoryClientError):
            await client.ontology_graph("person:a", max_depth=True)
        with pytest.raises(MemoryClientError):
            await client.ontology_switch("2.0.0", expected_generation=1, reason="unauthorized")
        tools = MCPMemoryTools(memory.provider, ontology=api).list_tools()
        assert "memory_ontology_graph" in {value["name"] for value in tools}
        assert "memory_ontology_switch" not in {value["name"] for value in tools}
    asyncio.run(scenario())


def test_real_mcp_server_ontology_roundtrip(tmp_path):
    pytest.importorskip("agent_memory_mcp")
    from agent_memory_mcp import StaticIdentityResolver, create_server

    async def scenario():
        api, memory, value = await setup_api(tmp_path)
        server = create_server(memory.provider, StaticIdentityResolver(MCPRequestContext(SCOPE)), ontology=api)
        async with MCPMemoryClient(server) as client:
            assert (await client.ontology_status())["active"]["version"] == SCHEMA.version
            assert len((await client.ontology_assertions([value.assertion.assertion_id]))["assertions"]) == 1
            assert (await client.ontology_graph("person:a", max_edges=1))["graph"]["edges"]
    asyncio.run(scenario())


def test_cli_and_lazy_backend_configuration(tmp_path, monkeypatch):
    from agent_memory import OntologyStoreConfig
    pytest.importorskip("agent_memory_mcp")
    from agent_memory_mcp.cli import parser
    options = parser().parse_args(["--ontology-id", "query.test", "--ontology-backend", "postgres"])
    assert options.ontology_id == "query.test"
    assert options.ontology_backend == "postgres"
    assert isinstance(OntologyStoreConfig(database_path=str(tmp_path / "index.db")).create_store(), SQLiteOntologyStore)
    monkeypatch.delenv("MISSING_TEST_ONTOLOGY_DSN", raising=False)
    with pytest.raises(ValueError, match="missing"):
        OntologyStoreConfig(backend="postgres", dsn_env="MISSING_TEST_ONTOLOGY_DSN").create_store()


def test_sdk_switch_requires_host_authorization(tmp_path):
    from dataclasses import replace
    async def scenario():
        api, memory, _ = await setup_api(tmp_path)
        await api.catalog.register(SCOPE, replace(SCHEMA, version="1.0.1"))
        class Deny:
            async def authorize(self, request):
                return False
        api.switch_policy = Deny()
        client = EmbeddedMemoryClient(memory.provider, MCPRequestContext(SCOPE), ontology=api)
        with pytest.raises(MemoryClientError):
            await client.ontology_switch("1.0.1", expected_generation=1, reason="unapproved")
        assert (await client.ontology_status())["active"]["version"] == "1.0.0"
    asyncio.run(scenario())
