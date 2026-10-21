"""Initialization must publish one complete schema, including failure recovery."""

import asyncio
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest

from agent_memory.operations import sqlite_model_budget
from agent_memory.retrieval import sqlite_lexical_index
from agent_memory.sqlite import SQLiteMemoryRepository


@pytest.mark.parametrize("owner", [sqlite_model_budget, sqlite_lexical_index])
def test_failed_initialization_does_not_publish_partial_schema(tmp_path, monkeypatch, owner):
    database = tmp_path / "memory.db"
    repository = SQLiteMemoryRepository(database)
    schema = owner.SCHEMA
    monkeypatch.setattr(owner, "SCHEMA", "SELECT missing_initialization_function();")
    with pytest.raises(sqlite3.OperationalError, match="missing_initialization_function"):
        asyncio.run(repository.initialize())

    with closing(sqlite3.connect(database)) as observer:
        assert observer.execute("SELECT name FROM sqlite_master").fetchall() == []

    monkeypatch.setattr(owner, "SCHEMA", schema)
    asyncio.run(repository.initialize())
    with closing(sqlite3.connect(database)) as observer:
        assert observer.execute("SELECT COUNT(*) FROM memory_schema").fetchone()[0] > 0


def test_schema_scripts_hold_one_writer_lock_until_publication(tmp_path, monkeypatch):
    database = tmp_path / "memory.db"
    observed = []

    class ObservedConnection(sqlite3.Connection):
        def executescript(self, script):
            result = super().executescript(script)
            with closing(sqlite3.connect(database, timeout=0)) as observer:
                assert observer.execute("SELECT name FROM sqlite_master").fetchall() == []
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    observer.execute("BEGIN IMMEDIATE")
            observed.append(True)
            return result

    def connect(self):
        connection = sqlite3.connect(database, timeout=30, factory=ObservedConnection)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    monkeypatch.setattr(SQLiteMemoryRepository, "_connect", connect)
    asyncio.run(SQLiteMemoryRepository(database).initialize())
    assert observed and all(observed)


def test_independent_processes_initialize_one_database(tmp_path):
    database = tmp_path / "memory.db"
    script = (
        "import asyncio, sys; from agent_memory.sqlite import SQLiteMemoryRepository; "
        "asyncio.run(SQLiteMemoryRepository(sys.argv[1]).initialize())"
    )

    def initialize(_):
        return subprocess.run(
            [sys.executable, "-c", script, str(database)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=30,
            check=True,
        ).returncode

    with ThreadPoolExecutor(max_workers=4) as workers:
        assert list(workers.map(initialize, range(4))) == [0, 0, 0, 0]
    with closing(sqlite3.connect(database)) as observer:
        assert observer.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert observer.execute("SELECT COUNT(*) FROM memory_schema").fetchone()[0] > 0
