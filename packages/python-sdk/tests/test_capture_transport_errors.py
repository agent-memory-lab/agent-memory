"""Remote best-effort capture must not interrupt a host on transport failure."""

import asyncio

import pytest
from mcp.shared.exceptions import MCPError
from pydantic import BaseModel, ValidationError

from agent_memory_sdk import MCPMemoryClient, MemoryClientError


class _Response(BaseModel):
    value: int


def _invalid_response():
    try:
        _Response.model_validate({"value": "invalid"})
    except ValidationError as error:
        return error
    raise AssertionError("expected validation failure")


class _FailingTransport:
    def __init__(self, error):
        self.error = error

    async def call_tool(self, name, arguments):
        raise self.error


def _client(transport):
    # Keep the production capture/_call stack, replacing only network I/O.
    client = object.__new__(MCPMemoryClient)
    client._client = transport
    client._capture_timeout_seconds = 0.25
    return client


@pytest.mark.parametrize("error", [
    MCPError(-32000, "remote failure containing private detail"),
    OSError("connection closed with private detail"),
    _invalid_response(),
])
def test_capture_normalizes_transport_and_response_failures(error):
    async def scenario():
        client = _client(_FailingTransport(error))
        with pytest.raises(MemoryClientError) as caught:
            await client.capture({"event_id": "event"})
        assert caught.value.code == "capture_transport_failed"
        assert str(caught.value) == "capture transport failed"
        assert caught.value.__cause__ is error
        assert await client.try_capture({"event_id": "event"}) == {
            "status": "skipped", "reason": "capture_transport_failed",
        }
        # This is not a global rewrite of ordinary SDK error handling.
        with pytest.raises(type(error)) as ordinary:
            await client.capabilities()
        assert ordinary.value is error

    asyncio.run(scenario())


def test_capture_preserves_existing_error_codes_and_fields():
    async def scenario():
        error = MemoryClientError("invalid event", code="capture_invalid_event", field="event_id")
        client = _client(_FailingTransport(error))
        with pytest.raises(MemoryClientError) as caught:
            await client.capture({})
        assert caught.value is error
        assert caught.value.field == "event_id"
        assert await client.try_capture({}) == {
            "status": "skipped", "reason": "capture_invalid_event",
        }

    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["capture", "try_capture"])
def test_capture_propagates_caller_cancellation_and_cleans_up(method):
    async def scenario():
        entered = asyncio.Event()
        finished = asyncio.Event()

        class WaitingTransport:
            async def call_tool(self, name, arguments):
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    finished.set()

        client = _client(WaitingTransport())
        task = asyncio.create_task(getattr(client, method)({}))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()

    asyncio.run(scenario())


def test_try_capture_preserves_timeout_contract():
    async def scenario():
        class WaitingTransport:
            async def call_tool(self, name, arguments):
                await asyncio.Event().wait()

        client = _client(WaitingTransport())
        client._capture_timeout_seconds = 0.001
        assert await client.try_capture({}) == {
            "status": "skipped", "reason": "capture_timeout",
        }

    asyncio.run(scenario())
