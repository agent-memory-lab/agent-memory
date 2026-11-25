"""SQLite connections for the shared transactional ontology operations."""

import sqlite3
from pathlib import Path

from .queries import ontology_instant
from .sql_store import (
    SQLOntologyStore,
)
from .sql_store import (
    _intervals_overlap as _intervals_overlap,  # Compatibility for the original store module.
)
from .sql_store import (
    _stored_object as _stored_object,
)
from .sql_store import (
    _visible_scope_partitions as _visible_scope_partitions,
)


class SQLiteOntologyStore(SQLOntologyStore):
    """Optional local ontology index using Python's standard library."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    def _connect(self):
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.create_function("ontology_instant", 1, ontology_instant, deterministic=True)
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _begin_write(self, connection):
        connection.execute("BEGIN IMMEDIATE")

    def _assertion_columns(self, connection):
        return {row["name"] for row in connection.execute(
            "PRAGMA table_info(ontology_assertions)"
        ).fetchall()}

    def _timestamp_sql(self, expression):
        return f"ontology_instant({expression})"
