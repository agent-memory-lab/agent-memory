"""Forget selectors must not broaden deletion through Python truthiness."""

import asyncio
import sqlite3
from contextlib import closing
from unittest.mock import AsyncMock

import pytest

from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryEvent, MemoryScope
from agent_memory.mcp import MCPMemoryTools, MCPRequestContext, MCPToolError
from agent_memory_sdk import EmbeddedMemoryClient, MemoryClientError


def snapshot(path):
    with closing(sqlite3.connect(path)) as connection:
        return list(connection.iterdump())


async def setup(path, transport, *, can_erase=True, auditor=None):
    kernel = build_local_kernel(path)
    await kernel.initialize()
    scope = MemoryScope("synthetic-tenant", session_id="synthetic-session")
    for identity in ("record-a", "record-b"):
        await kernel.ingest_event(MemoryEvent(scope, "test", identity, id=identity))
    context = MCPRequestContext(scope, can_erase=can_erase)
    if transport == "sdk":
        client = EmbeddedMemoryClient(kernel, context, deletion_auditor=auditor)
        return client.forget, MemoryClientError
    tools = MCPMemoryTools(kernel, deletion_auditor=auditor)

    async def call(**arguments):
        return await tools.call_tool("memory_forget", arguments, context)

    return call, MCPToolError


INVALID_OPTIONS = [
    *({"memory_ids": ["record-a"], "all_in_scope": value}
      for value in ("false", "true", "", 0, 1, None, [], {}, [False], {"enabled": False})),
    *({"memory_ids": value, "all_in_scope": True}
      for value in ("", "record-a", 0, False, {}, (), [1], [None], [""], [" "])),
    {"memory_ids": ["record-a"], "all_in_scope": True},
    {},
    {"memory_ids": [], "all_in_scope": False},
]


@pytest.mark.parametrize("transport", ["mcp", "sdk"])
@pytest.mark.parametrize("mode", ["archive", "erase"])
@pytest.mark.parametrize("options", INVALID_OPTIONS)
def test_invalid_forget_does_not_mutate_any_record(tmp_path, transport, mode, options):
    async def scenario():
        path = tmp_path / "disposable.db"
        auditor = AsyncMock()
        call, error = await setup(path, transport, auditor=auditor)
        before = snapshot(path)
        with pytest.raises(error) as caught:
            await call(mode=mode, **options)
        assert caught.value.code == "invalid_request"
        assert snapshot(path) == before
        auditor.forget.assert_not_called()

    asyncio.run(scenario())


@pytest.mark.parametrize("transport", ["mcp", "sdk"])
@pytest.mark.parametrize("mode", [None, False, 1, [], {}, "unknown"])
def test_invalid_mode_is_a_contract_error_without_mutation(tmp_path, transport, mode):
    async def scenario():
        path = tmp_path / "disposable.db"
        call, error = await setup(path, transport)
        before = snapshot(path)
        with pytest.raises(error) as caught:
            await call(memory_ids=["record-a"], mode=mode)
        assert caught.value.code == "invalid_request"
        assert snapshot(path) == before

    asyncio.run(scenario())


@pytest.mark.parametrize("transport", ["mcp", "sdk"])
@pytest.mark.parametrize("mode", ["archive", "erase"])
def test_explicit_id_then_true_scope_preserve_intended_selection(tmp_path, transport, mode):
    async def scenario():
        path = tmp_path / "disposable.db"
        call, _ = await setup(path, transport)
        result = await call(memory_ids=["record-a"], all_in_scope=False, mode=mode)
        assert result["affected_events"] == 1
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute(
                "SELECT id FROM events WHERE archived_at IS NULL ORDER BY id"
            ).fetchall() == [("record-b",)]
        result = await call(all_in_scope=True, mode=mode)
        assert result["affected_events"] == (2 if mode == "archive" else 1)
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute(
                "SELECT id FROM events WHERE archived_at IS NULL"
            ).fetchall() == []

    asyncio.run(scenario())


@pytest.mark.parametrize("transport", ["mcp", "sdk"])
@pytest.mark.parametrize("options", [{"memory_ids": ["record-a"]}, {"all_in_scope": True}])
def test_erase_still_requires_trusted_permission(tmp_path, transport, options):
    async def scenario():
        path = tmp_path / "disposable.db"
        call, error = await setup(path, transport, can_erase=False)
        before = snapshot(path)
        with pytest.raises(error, match="authorized request context"):
            await call(mode="erase", **options)
        assert snapshot(path) == before

    asyncio.run(scenario())
