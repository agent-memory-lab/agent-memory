"""Durable, job-bound checkpoints for optional ontology backfill workers."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
import sqlite3

from .domain import MemoryScope
from .ontology_backfill import OntologyBackfillCheckpoint


class OntologyCheckpointConflict(ValueError):
    """A checkpoint would overwrite newer or incompatible job progress."""


class SQLiteOntologyCheckpointSink:
    """A durable sink and resume reader bound to one trusted backfill job.

    The host owns the database path and constructs the scope binding. Saves
    serialize using SQLite transactions, reject progress regression, and accept
    identical retries. This is not a worker lease: use one worker per immutable
    snapshot/schema job. The host must reuse the same target index when resuming;
    after losing or replacing that index, use a fresh checkpoint database/job.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        scope: MemoryScope,
        snapshot_id: str,
        schema_digest: str,
    ) -> None:
        self._binding = OntologyBackfillCheckpoint(scope, snapshot_id, schema_digest)
        self._path = Path(path)
        self._key = (scope.partition_key(), snapshot_id, schema_digest)

    @contextmanager
    def _connection(self):
        connection = sqlite3.connect(self._path, timeout=5)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize)

    def _initialize(self) -> None:
        with self._connection() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS ontology_backfill_checkpoints (
                    partition_key TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL,
                    schema_digest TEXT NOT NULL,
                    cursor TEXT,
                    processed_claims INTEGER NOT NULL CHECK(processed_claims >= 0),
                    completed INTEGER NOT NULL CHECK(completed IN (0, 1)),
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(partition_key, snapshot_id, schema_digest),
                    CHECK(completed = 0 OR cursor IS NULL),
                    CHECK(completed = 1 OR processed_claims = 0 OR cursor IS NOT NULL)
                )
            """)

    async def load(self) -> OntologyBackfillCheckpoint | None:
        """Return durable progress for this job, or None for a fresh job."""
        return await asyncio.to_thread(self._load)

    def _load(self) -> OntologyBackfillCheckpoint | None:
        with self._connection() as db:
            row = db.execute(
                "SELECT cursor, processed_claims, completed FROM ontology_backfill_checkpoints "
                "WHERE partition_key=? AND snapshot_id=? AND schema_digest=?", self._key,
            ).fetchone()
        return None if row is None else self._decode(row)

    async def save(self, checkpoint: OntologyBackfillCheckpoint) -> None:
        if not isinstance(checkpoint, OntologyBackfillCheckpoint):
            raise TypeError("checkpoint must be an OntologyBackfillCheckpoint")
        if (
            checkpoint.scope != self._binding.scope
            or checkpoint.snapshot_id != self._binding.snapshot_id
            or checkpoint.schema_digest != self._binding.schema_digest
        ):
            raise ValueError("checkpoint does not match the bound backfill job")
        await asyncio.to_thread(self._save, checkpoint)

    def _save(self, checkpoint: OntologyBackfillCheckpoint) -> None:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT cursor, processed_claims, completed FROM ontology_backfill_checkpoints "
                "WHERE partition_key=? AND snapshot_id=? AND schema_digest=?", self._key,
            ).fetchone()
            if row is not None:
                previous = self._decode(row)
                if previous == checkpoint:
                    return
                if previous.completed:
                    raise OntologyCheckpointConflict("completed job cannot be overwritten")
                if checkpoint.processed_claims < previous.processed_claims:
                    raise OntologyCheckpointConflict("checkpoint would regress processed progress")
                if checkpoint.processed_claims == previous.processed_claims and not checkpoint.completed:
                    raise OntologyCheckpointConflict("same progress has a different continuation cursor")
                if not checkpoint.completed and checkpoint.cursor == previous.cursor:
                    raise OntologyCheckpointConflict("progress advanced without a new cursor")
            db.execute(
                "INSERT INTO ontology_backfill_checkpoints VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(partition_key, snapshot_id, schema_digest) DO UPDATE SET "
                "cursor=excluded.cursor, processed_claims=excluded.processed_claims, "
                "completed=excluded.completed, updated_at=excluded.updated_at",
                (*self._key, checkpoint.cursor, checkpoint.processed_claims,
                 int(checkpoint.completed), datetime.now(UTC).isoformat()),
            )

    def _decode(self, row: sqlite3.Row) -> OntologyBackfillCheckpoint:
        return OntologyBackfillCheckpoint(
            scope=self._binding.scope,
            snapshot_id=self._binding.snapshot_id,
            schema_digest=self._binding.schema_digest,
            cursor=row["cursor"],
            processed_claims=row["processed_claims"],
            completed=bool(row["completed"]),
        )
