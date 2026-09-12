import asyncio
import sqlite3
from contextlib import closing

from agent_memory import SCHEMA_VERSION
from agent_memory.sqlite import SQLiteMemoryRepository


def test_existing_sqlite_feedback_table_upgrades_in_place(tmp_path) -> None:
    database = tmp_path / "legacy.db"
    with closing(sqlite3.connect(database)) as connection:
        connection.executescript(
            """
            CREATE TABLE memory_schema (
                schema_version INTEGER PRIMARY KEY,
                installed_at TEXT NOT NULL
            );
            INSERT INTO memory_schema VALUES (1, CURRENT_TIMESTAMP);
            CREATE TABLE evolution_records (
                id TEXT PRIMARY KEY,
                partition_key TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                namespace TEXT NOT NULL,
                user_id TEXT,
                agent_id TEXT,
                workspace_id TEXT,
                session_id TEXT,
                record_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                occurred_at TEXT NOT NULL
            );
            """
        )

    asyncio.run(SQLiteMemoryRepository(database).initialize())

    with closing(sqlite3.connect(database)) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(evolution_records)")
        }
        versions = {
            row[0] for row in connection.execute("SELECT schema_version FROM memory_schema")
        }

    assert SCHEMA_VERSION == 2
    assert {
        "feedback_status",
        "parent_id",
        "idempotency_key",
        "payload_hash",
        "corrects_id",
        "expires_at",
        "invalidated_at",
    } <= columns
    assert versions == {1, 2}
