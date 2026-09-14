from __future__ import annotations

import asyncio

import pytest

from agent_memory import (
    MemoryCapabilities,
    MemoryScope,
    PluginError,
    PluginErrorCode,
    PluginManifestError,
    ProviderManifest,
)
from agent_memory.mcp import MCPRequestContext, MCPToolError, decode_mcp_error
from agent_memory_sdk import EmbeddedMemoryClient, MemoryClientError


class FailingProvider:
    async def initialize(self) -> None:
        return None

    def manifest(self) -> ProviderManifest:
        return ProviderManifest(
            name="failing-provider",
            version="0.1.0",
            protocol_version="0.1",
            schema_version=1,
            capabilities=MemoryCapabilities(),
        )

    async def ingest_event(self, event):
        raise PluginManifestError(
            "invalid plugin manifest field 'name': unsafe name",
            field="name",
        )


def run(coroutine):
    return asyncio.run(coroutine)


def test_plugin_error_has_stable_serializable_payload():
    error = PluginManifestError("invalid name", field="name")

    assert error.to_dict() == {
        "code": "invalid_manifest",
        "message": "invalid name",
        "field": "name",
    }


def test_mcp_transport_round_trip_preserves_code_message_and_field():
    source = PluginManifestError("invalid name", field="name")
    error = MCPToolError.from_plugin_error(source)

    assert decode_mcp_error(error.to_transport()) == {
        "code": "invalid_manifest",
        "message": "invalid name",
        "field": "name",
    }


def test_mcp_decoder_finds_envelope_inside_framework_message():
    error = MCPToolError("invalid request", code="invalid_request", field="query")
    wrapped = f"Error executing tool: {error.to_transport()} trailing framework text"

    assert decode_mcp_error(wrapped) == {
        "code": "invalid_request",
        "message": "invalid request",
        "field": "query",
    }


@pytest.mark.parametrize(
    "value",
    [
        "plain error",
        "agent-memory-error:not-json",
        'agent-memory-error:{"error":{"code":1,"message":"bad"}}',
        'agent-memory-error:{"error":{"code":"bad"}}',
    ],
)
def test_mcp_decoder_rejects_invalid_envelopes(value):
    assert decode_mcp_error(value) is None


def test_generic_plugin_failure_does_not_expose_internal_message():
    source = PluginError(
        "connection failed with password=secret at /private/path",
        code=PluginErrorCode.PLUGIN_LOAD_FAILED,
    )

    error = MCPToolError.from_plugin_error(source)

    assert str(error) == "memory plugin operation failed"
    assert "secret" not in error.to_transport()
    assert "/private/path" not in error.to_transport()


def test_embedded_sdk_preserves_plugin_error_contract():
    async def scenario():
        client = EmbeddedMemoryClient(
            FailingProvider(),
            MCPRequestContext(MemoryScope("tenant-a", user_id="user-a")),
        )
        await client.initialize()
        with pytest.raises(MemoryClientError) as caught:
            await client.ingest("user.message", "hello")

        assert caught.value.to_dict() == {
            "code": "invalid_manifest",
            "message": "invalid plugin manifest field 'name': unsafe name",
            "field": "name",
        }

    run(scenario())
