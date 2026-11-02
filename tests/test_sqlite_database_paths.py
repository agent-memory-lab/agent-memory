"""Per-operation connections require a persistent SQLite database."""

import asyncio
import sqlite3
from pathlib import Path

import pytest

from agent_memory.runtime import AgentMemory
from agent_memory.sqlite import SQLiteMemoryRepository


@pytest.mark.parametrize("path", ["", ":memory:", Path(":memory:")])
@pytest.mark.parametrize("factory", [SQLiteMemoryRepository, AgentMemory.local])
def test_connection_local_paths_are_rejected_at_construction(path, factory):
    with pytest.raises(ValueError, match="persistent file-backed database"):
        factory(path)


@pytest.mark.parametrize("path", [
    "file:",
    "file:?cache=shared",
    "file::memory:",
    "file::memory:?cache=shared",
    "file:%3Amemory%3A?cache=shared",
    "file:ephemeral?mode=memory",
    "file:ephemeral?mode=memory&cache=shared",
    "file:ephemeral?mode=memory&cache=private",
])
def test_connection_local_uris_are_rejected_before_initialization(path, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = SQLiteMemoryRepository(path)
    with pytest.raises(ValueError, match="persistent file-backed database"):
        asyncio.run(repository.initialize())
    assert list(tmp_path.iterdir()) == []


def test_rejected_uri_connection_is_closed(monkeypatch):
    connect = sqlite3.connect
    connections = []

    def tracked_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    repository = SQLiteMemoryRepository("file::memory:?cache=shared")
    with pytest.raises(ValueError, match="persistent file-backed database"):
        repository._connect()
    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")


@pytest.mark.parametrize("path_kind", [
    "relative", "absolute", "path", "memory_filename", "whitespace", "uri",
])
def test_persistent_paths_preserve_remember_across_reopen(path_kind, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    database = tmp_path / "memory.db"
    path = {
        "relative": "memory.db",
        "absolute": str(database),
        "path": database,
        "memory_filename": "./:memory:",
        "whitespace": " ",
        "uri": database.as_uri() + "?mode=rwc",
    }[path_kind]

    async def run():
        async with AgentMemory.local(path) as memory:
            first = await memory.remember("Persistent event", idempotency_key="event")
            assert not first.duplicate
        async with AgentMemory.local(path) as reopened:
            second = await reopened.remember("Persistent event", idempotency_key="event")
            assert second.duplicate
            assert second.event_id == first.event_id

    asyncio.run(run())
