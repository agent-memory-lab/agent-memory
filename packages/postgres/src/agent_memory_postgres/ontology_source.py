"""Optional PostgreSQL source for the durable local ontology coordinator."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime
import json
from pathlib import Path
import re
from uuid import uuid4

import psycopg
from psycopg import sql

from agent_memory.ontology_source import SQLiteOntologySnapshot, database


class PostgresOntologySource:
    """Track core writes transactionally and stream repeatable-read snapshots.

    Initialize the PostgreSQL core repository first. This source adds tracking
    objects only when explicitly initialized. Snapshots and ontology indexes
    remain local SQLite files owned by LiveOntologyMemory, not PG projections.
    """

    def __init__(self, dsn, *, namespace="public", timeout_ms=30000):
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must be non-empty")
        if not isinstance(namespace, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", namespace):
            raise ValueError("namespace must be a lowercase SQL identifier")
        if type(timeout_ms) is not int or not 1 <= timeout_ms <= 60000:
            raise ValueError("timeout_ms must be between 1 and 60000")
        self._dsn, self.namespace, self.timeout_ms = dsn, namespace, timeout_ms

    @property
    def storage_key(self):
        # Persistent source UUID distinguishes databases; never persist a DSN.
        return "postgres-core:" + self.namespace

    @contextmanager
    def _connection(self, *, snapshot=False):
        with psycopg.connect(self._dsn, connect_timeout=5, client_encoding="utf8") as connection:
            if snapshot:
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            connection.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(self.namespace)))
            connection.execute("SELECT set_config('statement_timeout', %s, true)", (str(self.timeout_ms),))
            connection.execute("SELECT set_config('lock_timeout', %s, true)", (str(self.timeout_ms),))
            yield connection

    async def initialize(self):
        await asyncio.to_thread(self._initialize)

    def _initialize(self):
        with self._connection() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (self.storage_key,))
            connection.execute("""CREATE TABLE IF NOT EXISTS agent_memory_ontology_source (
                id integer PRIMARY KEY CHECK(id=1), source_id text NOT NULL,
                revision bigint NOT NULL DEFAULT 0)""")
            connection.execute("INSERT INTO agent_memory_ontology_source(id,source_id) VALUES (1,%s) ON CONFLICT DO NOTHING", (str(uuid4()),))
            connection.execute(sql.SQL("""CREATE OR REPLACE FUNCTION {}.agent_memory_track_ontology()
                RETURNS trigger LANGUAGE plpgsql AS $body$
                BEGIN
                    UPDATE {}.agent_memory_ontology_source SET revision=revision+1 WHERE id=1;
                    RETURN NULL;
                END;
                $body$""").format(sql.Identifier(self.namespace), sql.Identifier(self.namespace)))
            for table in ("agent_memory_claims", "agent_memory_events"):
                connection.execute(sql.SQL("DROP TRIGGER IF EXISTS agent_memory_ontology_change ON {}.{}").format(sql.Identifier(self.namespace), sql.Identifier(table)))
                connection.execute(sql.SQL("""CREATE TRIGGER agent_memory_ontology_change
                    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {}.{}
                    FOR EACH STATEMENT EXECUTE FUNCTION {}.agent_memory_track_ontology()""").format(
                        sql.Identifier(self.namespace), sql.Identifier(table), sql.Identifier(self.namespace)))

    def _state(self):
        with self._connection(snapshot=True) as connection:
            row = connection.execute("SELECT source_id, revision FROM agent_memory_ontology_source WHERE id=1").fetchone()
            if row is None:
                raise RuntimeError("ontology source is not initialized")
            return row

    async def identity(self):
        return (await asyncio.to_thread(self._state))[0]

    async def revision(self):
        return (await asyncio.to_thread(self._state))[1]

    async def snapshot(self, scope, destination):
        return await asyncio.to_thread(self._snapshot, scope, Path(destination))

    def _snapshot(self, scope, destination):
        with destination.open("xb"):
            pass
        snapshot_id = str(uuid4())
        # Keep a single MVCC view for revision, claims and evidence. Server-side
        # cursors avoid buffering the entire scope in the client process.
        with self._connection(snapshot=True) as source, database(destination) as target:
            revision = source.execute("SELECT revision FROM agent_memory_ontology_source WHERE id=1").fetchone()[0]
            with source.cursor(name="ontology_claim_snapshot") as cursor:
                cursor.execute("SELECT * FROM agent_memory_claims WHERE partition_key=%s AND status='active' AND archived_at IS NULL ORDER BY id", (scope.partition_key(),))
                rows = cursor.fetchmany(128)
                columns = tuple(column.name for column in cursor.description)
                target.execute("CREATE TABLE claims (" + ",".join('"' + name.replace('"', '""') + '"' for name in columns) + ")")
                insert = "INSERT INTO claims VALUES (" + ",".join("?" for _ in columns) + ")"
                while rows:
                    target.executemany(insert, (
                        tuple(json.dumps(value) if name.endswith("_json") else
                              value.isoformat() if isinstance(value, datetime) else value
                              for name, value in zip(columns, row)) for row in rows))
                    rows = cursor.fetchmany(128)
            target.execute("CREATE UNIQUE INDEX snapshot_claim_id ON claims(id)")
            target.execute("CREATE TABLE evidence (id TEXT PRIMARY KEY)")
            with source.cursor(name="ontology_event_snapshot") as cursor:
                cursor.execute("SELECT id FROM agent_memory_events WHERE partition_key=%s AND archived_at IS NULL", (scope.partition_key(),))
                while rows := cursor.fetchmany(128):
                    target.executemany("INSERT INTO evidence VALUES (?)", rows)
            target.execute("CREATE TABLE snapshot_metadata (snapshot_id TEXT, partition_key TEXT, revision INTEGER)")
            target.execute("INSERT INTO snapshot_metadata VALUES (?, ?, ?)", (snapshot_id, scope.partition_key(), revision))
        return SQLiteOntologySnapshot(destination, scope, snapshot_id, revision)
