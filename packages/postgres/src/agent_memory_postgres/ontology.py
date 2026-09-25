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

from agent_memory.ontology_memory import SQLiteOntologyStore


class _Connection:
    def __init__(self, raw, namespace):
        self.raw, self.namespace = raw, namespace

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.raw.commit()
        else:
            self.raw.rollback()

    def execute(self, statement, parameters=()):
        if statement.strip().startswith("PRAGMA table_info("):
            return self.raw.execute(
                "SELECT column_name AS name FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name='ontology_assertions'", (self.namespace,),
            )
        translated = statement.replace("?", "%s")
        translated = re.sub(r"\bREAL\b", "DOUBLE PRECISION", translated)
        ignore = "INSERT OR IGNORE" in translated
        translated = translated.replace("INSERT OR IGNORE", "INSERT")
        translated = translated.replace(
            "MAX(ontology_assertions.confidence, excluded.confidence)",
            "GREATEST(ontology_assertions.confidence, excluded.confidence)",
        )
        if ignore:
            translated = translated.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        return self.raw.execute(translated, parameters)

    def executescript(self, script):
        for statement in script.split(";"):
            if statement.strip():
                self.execute(statement)

    def close(self):
        self.raw.close()


class PostgresOntologyStore(SQLiteOntologyStore):
    """Optional PostgreSQL projection, evidence, graph and conflict adapter.

    The inherited implementation consists of shared SQL operations; _connect
    replaces SQLite completely. A dedicated namespace isolates installations.
    The caller owns DSN secrets; exceptions must not be exposed to untrusted
    transports. Locks serialize mutations and searches within one namespace.
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
        raw = psycopg.connect(self._dsn, row_factory=dict_row, connect_timeout=5, client_encoding="utf8")
        try:
            raw.execute("SELECT set_config('statement_timeout', %s, true)", (str(self._timeout_ms),))
            raw.execute("SELECT set_config('lock_timeout', %s, true)", (str(self._timeout_ms),))
            raw.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (self._namespace,))
            raw.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self._namespace)))
            raw.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(self._namespace)))
            return _Connection(raw, self._namespace)
        except BaseException:
            raw.close()
            raise

    def _timestamp_sql(self, expression):
        return f"CAST({expression} AS TIMESTAMPTZ)"
