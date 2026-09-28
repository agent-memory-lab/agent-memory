"""Optional bounded persistence port for recovery artifacts and capture receipts."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
import json
from datetime import UTC, datetime
from pathlib import Path
import sqlite3
from typing import Protocol

from .domain import MemoryScope


class RecoveryConflict(ValueError):
    """A stale version, changed identity, or terminal deletion tombstone."""


@dataclass(frozen=True, slots=True)
class StoredRecoveryRecord:
    revision: int
    payload: dict
    source_event_ids: tuple[str, ...]


class RecoveryStore(Protocol):
    """Trusted exact-scope port; writes are compare-and-swap and failure atomic.

    Invalidated identities must never be reactivated. Reads omit tombstones.
    sources() returns active capture receipts covering the requested evidence.
    All participants sharing a store must use the same deployment limits.
    """
    async def initialize(self) -> None: ...
    async def read(self, scope, kind, identity) -> StoredRecoveryRecord | None: ...
    async def write(self, scope, kind, identity, payload, source_event_ids,
                    *, expected_revision) -> int: ...
    async def sources(self, scope, source_event_ids) -> tuple[StoredRecoveryRecord, ...]: ...
    async def forget_sources(self, request) -> int: ...
    async def ensure_run(self, scope, run_id) -> None: ...
    async def configure_run(self, scope, run_id, *, completed=False, expires_at=None) -> None: ...
    async def cleanup(self, scope, *, limit=100) -> int: ...
    async def stats(self, scope) -> dict: ...
    async def next_job(self, scope) -> tuple[str, StoredRecoveryRecord] | None: ...


def _key(kind, identity):
    if kind not in ("receipt", "state", "summary", "job"):
        raise ValueError("unknown recovery record kind")
    if not isinstance(identity, str) or not 1 <= len(identity) <= 256:
        raise ValueError("recovery identity must contain 1 to 256 characters")


def _refs(values, *, empty=False):
    if not isinstance(values, (tuple, list)) or not (0 if empty else 1) <= len(values) <= 256:
        raise ValueError("provide at most 256 source event IDs")
    if any(not isinstance(value, str) or not 1 <= len(value) <= 256 for value in values):
        raise ValueError("invalid source event ID")
    if len(set(values)) != len(values):
        raise ValueError("source event IDs must be unique")
    return tuple(values)


class SQLiteRecoveryStore:
    """No worker or resident cache. Tombstones count toward the capacity limit.

    Both archive and erase remove derivative content here. Minimal identity,
    version and source IDs remain as deletion tombstones, not usable memory.
    This is logical deletion; SQLite free pages/backups need host retention.
    """
    def __init__(self, path, *, max_records=10000, max_payload_bytes=65536):
        if str(path) == ":memory:":
            raise ValueError("recovery storage must survive process restart")
        if type(max_records) is not int or not 1 <= max_records <= 1000000:
            raise ValueError("invalid recovery record capacity")
        if type(max_payload_bytes) is not int or not 1024 <= max_payload_bytes <= 1048576:
            raise ValueError("invalid recovery payload budget")
        self.path = Path(path)
        self.max_records, self.max_payload_bytes = max_records, max_payload_bytes

    @contextmanager
    def _db(self):
        connection = sqlite3.connect(self.path, timeout=5)
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    async def initialize(self):
        def create():
            with self._db() as db:
                db.execute("""CREATE TABLE IF NOT EXISTS recovery_records_v1 (
                    scope TEXT NOT NULL, kind TEXT NOT NULL, identity TEXT NOT NULL,
                    revision INTEGER NOT NULL, payload TEXT, sources TEXT NOT NULL,
                    PRIMARY KEY(scope,kind,identity))""")
                columns = {row[1] for row in db.execute("PRAGMA table_info(recovery_records_v1)")}
                if "run_id" not in columns:
                    db.execute("ALTER TABLE recovery_records_v1 ADD COLUMN run_id TEXT")
                db.execute("UPDATE recovery_records_v1 SET run_id=json_extract(payload,'$.run_id') "
                           "WHERE run_id IS NULL AND payload IS NOT NULL")
                db.execute("""CREATE TABLE IF NOT EXISTS recovery_runs_v1 (
                    scope TEXT NOT NULL, run_id TEXT NOT NULL, state TEXT NOT NULL,
                    expires_at TEXT, PRIMARY KEY(scope,run_id))""")
                db.execute("CREATE INDEX IF NOT EXISTS recovery_run_idx "
                           "ON recovery_records_v1(scope,run_id)")
        await asyncio.to_thread(create)

    @staticmethod
    def _run_open(db, scope_key, run_id):
        row = db.execute("SELECT state,expires_at FROM recovery_runs_v1 WHERE scope=? AND run_id=?",
                         (scope_key, run_id)).fetchone()
        return row is None or (row[0] == "active" and
            (row[1] is None or row[1] > datetime.now(UTC).isoformat()))

    def _decode(self, row):
        if len(row[1].encode("utf-8")) > self.max_payload_bytes:
            raise ValueError("stored recovery payload exceeds budget")
        return StoredRecoveryRecord(row[0], json.loads(row[1]), tuple(json.loads(row[2])))

    async def read(self, scope, kind, identity):
        _key(kind, identity)
        def read():
            with self._db() as db:
                row = db.execute("""SELECT revision,payload,sources,run_id FROM recovery_records_v1
                    WHERE scope=? AND kind=? AND identity=? AND payload IS NOT NULL""",
                    (scope.partition_key(), kind, identity)).fetchone()
                if row and not self._run_open(db, scope.partition_key(), row[3]):
                    return None
                return self._decode(row) if row else None
        return await asyncio.to_thread(read)

    async def write(self, scope, kind, identity, payload, source_event_ids, *, expected_revision):
        _key(kind, identity)
        refs = _refs(source_event_ids, empty=kind in ("receipt", "job"))
        if type(expected_revision) is not int or expected_revision < 0:
            raise ValueError("expected revision must be a nonnegative integer")
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True,
                             allow_nan=False, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > self.max_payload_bytes:
            raise ValueError("recovery payload exceeds byte budget")
        key = (scope.partition_key(), kind, identity)
        run_id = payload.get("run_id")
        if run_id is not None:
            _key("state", run_id)
        def write():
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                if not self._run_open(db, scope.partition_key(), run_id):
                    raise RecoveryConflict("recovery run is completed or expired")
                old = db.execute("SELECT revision,payload FROM recovery_records_v1 "
                                 "WHERE scope=? AND kind=? AND identity=?", key).fetchone()
                if old and old[1] is None:
                    raise RecoveryConflict("deleted recovery identity cannot be reused")
                if (old[0] if old else 0) != expected_revision:
                    raise RecoveryConflict("recovery revision changed")
                if old is None and db.execute("SELECT count(*) FROM recovery_records_v1").fetchone()[0] >= self.max_records:
                    raise ValueError("recovery store capacity exceeded")
                revision = expected_revision + 1
                db.execute("""INSERT INTO recovery_records_v1
                    (scope,kind,identity,revision,payload,sources,run_id) VALUES (?,?,?,?,?,?,?)
                    ON CONFLICT(scope,kind,identity) DO UPDATE SET
                    revision=excluded.revision,payload=excluded.payload,sources=excluded.sources""",
                    (*key, revision, encoded, json.dumps(refs), run_id))
                return revision
        return await asyncio.to_thread(write)

    async def sources(self, scope, source_event_ids):
        refs = _refs(source_event_ids)
        def read():
            with self._db() as db:
                rows = db.execute("""SELECT revision,payload,sources,run_id FROM recovery_records_v1 r
                    WHERE scope=? AND kind='receipt' AND payload IS NOT NULL
                    AND EXISTS (SELECT 1 FROM json_each(r.sources) s
                        JOIN json_each(?) wanted ON s.value=wanted.value) LIMIT 257""",
                    (scope.partition_key(), json.dumps(refs))).fetchall()
                if len(rows) > 256:
                    raise ValueError("ambiguous or excessive source receipts")
                return tuple(self._decode(row) for row in rows
                             if self._run_open(db, scope.partition_key(), row[3]))
        return await asyncio.to_thread(read)

    async def forget_sources(self, request):
        if not request.all_in_scope:
            _refs(request.memory_ids)
        def write():
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                condition = "scope=? AND payload IS NOT NULL"
                args = [request.scope.partition_key()]
                if not request.all_in_scope:
                    condition += """ AND (EXISTS (SELECT 1 FROM json_each(sources) s
                        JOIN json_each(?) removed ON s.value=removed.value)
                        OR (kind='job' AND identity IN (
                            SELECT r.identity FROM recovery_records_v1 r
                            WHERE r.scope=? AND r.kind='receipt' AND EXISTS (
                                SELECT 1 FROM json_each(r.sources) s
                                JOIN json_each(?) removed ON s.value=removed.value))))"""
                    args.extend((json.dumps(request.memory_ids), request.scope.partition_key(),
                                 json.dumps(request.memory_ids)))
                return db.execute("UPDATE recovery_records_v1 SET payload=NULL, "
                                  "revision=revision+1 WHERE " + condition, args).rowcount
        return await asyncio.to_thread(write)

    async def ensure_run(self, scope, run_id):
        _key("state", run_id)
        def write():
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                key = (scope.partition_key(), run_id)
                if not self._run_open(db, *key):
                    raise RecoveryConflict("recovery run is completed or expired")
                if db.execute("SELECT 1 FROM recovery_runs_v1 WHERE scope=? AND run_id=?", key).fetchone():
                    return
                if db.execute("SELECT count(*) FROM recovery_runs_v1").fetchone()[0] >= self.max_records:
                    raise ValueError("recovery run registry capacity exceeded")
                db.execute("INSERT INTO recovery_runs_v1 VALUES (?,?,'active',NULL)", key)
        await asyncio.to_thread(write)

    async def configure_run(self, scope, run_id, *, completed=False, expires_at=None):
        _key("state", run_id)
        if type(completed) is not bool:
            raise ValueError("completed must be a boolean")
        expiry = None
        if expires_at is not None:
            if not isinstance(expires_at, datetime) or expires_at.utcoffset() is None:
                raise ValueError("expiry requires a timezone")
            expiry = expires_at.astimezone(UTC).isoformat()
        await self.ensure_run(scope, run_id)
        def write():
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                if not self._run_open(db, scope.partition_key(), run_id):
                    raise RecoveryConflict("run is no longer active")
                if completed:
                    db.execute("UPDATE recovery_runs_v1 SET state='completed' WHERE scope=? AND run_id=?",
                               (scope.partition_key(), run_id))
                else:
                    db.execute("UPDATE recovery_runs_v1 SET expires_at=? WHERE scope=? AND run_id=?",
                               (expiry, scope.partition_key(), run_id))
        await asyncio.to_thread(write)

    async def cleanup(self, scope, *, limit=100):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("cleanup limit must be between 1 and 1000")
        def write():
            with self._db() as db:
                return db.execute("""DELETE FROM recovery_records_v1 WHERE rowid IN (
                    SELECT r.rowid FROM recovery_records_v1 r JOIN recovery_runs_v1 lifecycle
                    ON lifecycle.scope=r.scope AND lifecycle.run_id=r.run_id
                    WHERE r.scope=? AND (lifecycle.state!='active' OR lifecycle.expires_at<=?)
                    ORDER BY r.rowid LIMIT ?)""",
                    (scope.partition_key(), datetime.now(UTC).isoformat(), limit)).rowcount
        return await asyncio.to_thread(write)

    async def stats(self, scope):
        def read():
            with self._db() as db:
                row = db.execute("""SELECT count(*),coalesce(sum(payload IS NOT NULL),0),
                    coalesce(sum(length(CAST(payload AS BLOB))),0)
                    FROM recovery_records_v1 WHERE scope=?""", (scope.partition_key(),)).fetchone()
                runs = db.execute("""SELECT count(*),coalesce(sum(state='active' AND
                    (expires_at IS NULL OR expires_at>?)),0) FROM recovery_runs_v1 WHERE scope=?""",
                    (datetime.now(UTC).isoformat(), scope.partition_key())).fetchone()
                return dict(records=row[0], retained_payloads=row[1], payload_bytes=row[2],
                            run_fences=runs[0], active_runs=runs[1],
                            record_capacity=self.max_records, run_capacity=self.max_records)
        return await asyncio.to_thread(read)

    async def next_job(self, scope):
        def read():
            with self._db() as db:
                row = db.execute("""SELECT r.identity,r.revision,r.payload,r.sources FROM recovery_records_v1 r
                    WHERE r.scope=? AND r.kind='job' AND r.payload IS NOT NULL
                    AND json_extract(r.payload,'$.status') IN ('queued','processing')
                    AND NOT EXISTS (SELECT 1 FROM recovery_runs_v1 lifecycle
                        WHERE lifecycle.scope=r.scope AND lifecycle.run_id=r.run_id
                        AND (lifecycle.state!='active' OR lifecycle.expires_at<=?))
                    ORDER BY r.rowid LIMIT 1""",
                    (scope.partition_key(), datetime.now(UTC).isoformat())).fetchone()
                return (row[0], self._decode(row[1:])) if row else None
        return await asyncio.to_thread(read)
