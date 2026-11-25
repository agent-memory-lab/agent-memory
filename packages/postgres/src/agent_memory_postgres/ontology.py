"""PostgreSQL OntologyStore using the shared SQL projection implementation.

Only connection/dialect handling differs from SQLite. Data is stored natively
in PostgreSQL, with a bounded transaction-scoped advisory lock per namespace.
The adapter does not require pgvector and never creates a local SQLite mirror.
"""
from __future__ import annotations

import re

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from agent_memory.ontology.sql_store import SQLOntologyStore


class _Connection:
    def __init__(self, raw):
        self.raw = raw

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.raw.commit()
        else:
            self.raw.rollback()

    def execute(self, statement, parameters=()):
        # Shared operations use qmark parameters; only DB-API binding differs.
        return self.raw.execute(statement.replace("?", "%s"), parameters)

    def executescript(self, script):
        for statement in script.split(";"):
            if statement.strip():
                self.execute(statement)

    def close(self):
        self.raw.close()


class PostgresOntologyStore(SQLOntologyStore):
    """Optional PostgreSQL projection, evidence, graph and conflict adapter.

    The shared implementation contains portable SQL operations. A dedicated
    namespace isolates installations.
    The caller owns DSN secrets; exceptions must not be exposed to untrusted
    transports. Readers share a namespace lock; mutations hold it exclusively.
    """

    def __init__(self, dsn: str, *, namespace="agent_memory_ontology", timeout_ms=5000):
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must be non-empty")
        if not isinstance(namespace, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", namespace):
            raise ValueError("namespace must be a lowercase SQL identifier")
        if type(timeout_ms) is not int or not 1 <= timeout_ms <= 60000:
            raise ValueError("timeout_ms must be between 1 and 60000")
        self._dsn, self._namespace, self._timeout_ms = dsn, namespace, timeout_ms

    def _connect(self):
        return self._connection(write=False)

    def _connect_write(self):
        return self._connection(write=True)

    def _connect_initialize(self):
        return self._connection(write=True, initialize=True)

    def _begin_write(self, connection):
        # The write connection owns its exclusive transaction lock before any read.
        pass

    def _assertion_columns(self, connection):
        return {row["name"] for row in connection.raw.execute(
            "SELECT column_name AS name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name='ontology_assertions'", (self._namespace,),
        ).fetchall()}

    def _connection(self, *, write, initialize=False):
        raw = psycopg.connect(self._dsn, row_factory=dict_row, connect_timeout=5, client_encoding="utf8")
        try:
            raw.execute("SELECT set_config('statement_timeout', %s, true)", (str(self._timeout_ms),))
            raw.execute("SELECT set_config('lock_timeout', %s, true)", (str(self._timeout_ms),))
            lock = "pg_advisory_xact_lock" if write else "pg_advisory_xact_lock_shared"
            raw.execute(sql.SQL("SELECT {}(hashtext(%s))").format(sql.Identifier(lock)),
                        (self._namespace,))
            if initialize:
                raw.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(self._namespace)))
            raw.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(self._namespace)))
            return _Connection(raw)
        except BaseException:
            raw.close()
            raise

    def _timestamp_sql(self, expression):
        return f"CAST({expression} AS TIMESTAMPTZ)"
