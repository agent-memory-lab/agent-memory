"""SQLite Claim snapshots and transactional change detection for ontology jobs."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from uuid import uuid4

from .domain import MemoryScope
from .ontology_backfill import OntologyClaimPage
from .sqlite import SQLiteMemoryRepository


@contextmanager
def database(path, *, readonly=False):
    connection = sqlite3.connect(
        Path(path).resolve().as_uri() + "?mode=ro" if readonly else str(path),
        uri=readonly, timeout=5,
    )
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


class SQLiteOntologySource:
    """Opt-in revision counter updated in the same transaction as core writes.

    The counter conservatively covers all scopes. Consumers rebuild only their
    bound scope; a write in another scope may cause harmless extra work. No event
    content is copied into the change tracker. Initialize after the core store.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path).resolve()

    async def initialize(self):
        await asyncio.to_thread(self._initialize)

    def _initialize(self):
        with database(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS ontology_source_revision (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL)")
            columns = {row["name"] for row in db.execute("PRAGMA table_info(ontology_source_revision)")}
            if "source_id" not in columns:
                db.execute("ALTER TABLE ontology_source_revision ADD COLUMN source_id TEXT")
            db.execute("INSERT OR IGNORE INTO ontology_source_revision (id,revision,source_id) VALUES (1, 0, ?)", (str(uuid4()),))
            db.execute("UPDATE ontology_source_revision SET source_id=? WHERE source_id IS NULL", (str(uuid4()),))
            for table in ("claims", "events", "claim_sources"):
                for action in ("INSERT", "UPDATE", "DELETE"):
                    db.execute(
                        f"CREATE TRIGGER IF NOT EXISTS ontology_track_{table}_{action.lower()} "
                        f"AFTER {action} ON {table} BEGIN "
                        "UPDATE ontology_source_revision SET revision=revision+1 WHERE id=1; END"
                    )

    async def revision(self) -> int:
        return await asyncio.to_thread(self._revision)

    async def identity(self) -> str:
        def read():
            with database(self.path, readonly=True) as db:
                return db.execute("SELECT source_id FROM ontology_source_revision WHERE id=1").fetchone()[0]
        return await asyncio.to_thread(read)

    def _revision(self):
        with database(self.path, readonly=True) as db:
            return db.execute("SELECT revision FROM ontology_source_revision WHERE id=1").fetchone()[0]

    async def snapshot(self, scope: MemoryScope, destination: str | Path):
        return await asyncio.to_thread(self._snapshot, scope, Path(destination))

    def _snapshot(self, scope, destination):
        # Exclusive creation prevents overwriting a previous snapshot or source.
        with destination.open("xb"):
            pass
        snapshot_id = str(uuid4())
        with database(self.path, readonly=True) as source, database(destination) as target:
            source.execute("BEGIN")
            revision = source.execute("SELECT revision FROM ontology_source_revision WHERE id=1").fetchone()[0]
            query = source.execute(
                "SELECT * FROM claims WHERE partition_key=? AND status='active' AND archived_at IS NULL ORDER BY id",
                (scope.partition_key(),),
            )
            columns = tuple(item[0] for item in query.description)
            target.execute("CREATE TABLE claims (" + ",".join('"' + name + '"' for name in columns) + ")")
            insert = "INSERT INTO claims VALUES (" + ",".join("?" for _ in columns) + ")"
            while rows := query.fetchmany(128):
                target.executemany(insert, (tuple(row) for row in rows))
            target.execute("CREATE UNIQUE INDEX snapshot_claim_id ON claims(id)")
            target.execute("CREATE TABLE evidence (id TEXT PRIMARY KEY)")
            events = source.execute(
                "SELECT id FROM events WHERE partition_key=? AND archived_at IS NULL", (scope.partition_key(),),
            )
            while rows := events.fetchmany(128):
                target.executemany("INSERT INTO evidence VALUES (?)", (tuple(row) for row in rows))
            target.execute("CREATE TABLE snapshot_metadata (snapshot_id TEXT, partition_key TEXT, revision INTEGER)")
            target.execute("INSERT INTO snapshot_metadata VALUES (?, ?, ?)", (snapshot_id, scope.partition_key(), revision))
        return SQLiteOntologySnapshot(destination, scope, snapshot_id, revision)


class SQLiteOntologySnapshot:
    def __init__(self, path, scope, snapshot_id, revision):
        self.path = Path(path)
        self.scope = scope
        self.snapshot_id = snapshot_id
        self.revision = revision

    @classmethod
    async def open(cls, path: str | Path, scope: MemoryScope):
        def read():
            with database(path, readonly=True) as db:
                row = db.execute("SELECT * FROM snapshot_metadata").fetchone()
                if row is None or row["partition_key"] != scope.partition_key():
                    raise ValueError("snapshot is outside the requested scope")
                return cls(path, scope, row["snapshot_id"], row["revision"])
        return await asyncio.to_thread(read)

    async def read_page(self, scope, snapshot_id, *, cursor, limit):
        if scope != self.scope or snapshot_id != self.snapshot_id:
            raise ValueError("snapshot scope or identity mismatch")
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("snapshot page limit must be between 1 and 256")
        return await asyncio.to_thread(self._page, cursor, limit)

    def _page(self, cursor, limit):
        with database(self.path, readonly=True) as db:
            rows = db.execute("SELECT * FROM claims WHERE id>? ORDER BY id LIMIT ?", (cursor or "", limit + 1)).fetchall()
        # Keep compatibility with the core Claim codec in this SQLite adapter.
        codec = SQLiteMemoryRepository(self.path)
        claims = tuple(codec._claim_from_row(row) for row in rows[:limit])
        return OntologyClaimPage(claims, claims[-1].id if len(rows) > limit else None)

    async def verify(self, scope, source_event_ids):
        if scope != self.scope or not source_event_ids or len(source_event_ids) > 256:
            return False
        def read():
            ids = tuple(set(source_event_ids))
            with database(self.path, readonly=True) as db:
                count = db.execute(
                    "SELECT COUNT(*) FROM evidence WHERE id IN (" + ",".join("?" for _ in ids) + ")", ids,
                ).fetchone()[0]
            return count == len(ids)
        return await asyncio.to_thread(read)
