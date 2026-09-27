"""Optional bounded persistence port for recovery artifacts and capture receipts."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
import json
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


def _key(kind, identity):
    if kind not in ("receipt", "state", "summary"):
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
        await asyncio.to_thread(create)

    def _decode(self, row):
        if len(row[1].encode("utf-8")) > self.max_payload_bytes:
            raise ValueError("stored recovery payload exceeds budget")
        return StoredRecoveryRecord(row[0], json.loads(row[1]), tuple(json.loads(row[2])))

    async def read(self, scope, kind, identity):
        _key(kind, identity)
        def read():
            with self._db() as db:
                row = db.execute("""SELECT revision,payload,sources FROM recovery_records_v1
                    WHERE scope=? AND kind=? AND identity=? AND payload IS NOT NULL""",
                    (scope.partition_key(), kind, identity)).fetchone()
                return self._decode(row) if row else None
        return await asyncio.to_thread(read)

    async def write(self, scope, kind, identity, payload, source_event_ids, *, expected_revision):
        _key(kind, identity)
        refs = _refs(source_event_ids, empty=kind == "receipt")
        if type(expected_revision) is not int or expected_revision < 0:
            raise ValueError("expected revision must be a nonnegative integer")
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True,
                             allow_nan=False, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > self.max_payload_bytes:
            raise ValueError("recovery payload exceeds byte budget")
        key = (scope.partition_key(), kind, identity)
        def write():
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                old = db.execute("SELECT revision,payload FROM recovery_records_v1 "
                                 "WHERE scope=? AND kind=? AND identity=?", key).fetchone()
                if old and old[1] is None:
                    raise RecoveryConflict("deleted recovery identity cannot be reused")
                if (old[0] if old else 0) != expected_revision:
                    raise RecoveryConflict("recovery revision changed")
                if old is None and db.execute("SELECT count(*) FROM recovery_records_v1").fetchone()[0] >= self.max_records:
                    raise ValueError("recovery store capacity exceeded")
                revision = expected_revision + 1
                db.execute("""INSERT INTO recovery_records_v1 VALUES (?,?,?,?,?,?)
                    ON CONFLICT(scope,kind,identity) DO UPDATE SET
                    revision=excluded.revision,payload=excluded.payload,sources=excluded.sources""",
                    (*key, revision, encoded, json.dumps(refs)))
                return revision
        return await asyncio.to_thread(write)

    async def sources(self, scope, source_event_ids):
        refs = _refs(source_event_ids)
        def read():
            with self._db() as db:
                rows = db.execute("""SELECT revision,payload,sources FROM recovery_records_v1 r
                    WHERE scope=? AND kind='receipt' AND payload IS NOT NULL
                    AND EXISTS (SELECT 1 FROM json_each(r.sources) s
                        JOIN json_each(?) wanted ON s.value=wanted.value) LIMIT 257""",
                    (scope.partition_key(), json.dumps(refs))).fetchall()
                if len(rows) > 256:
                    raise ValueError("ambiguous or excessive source receipts")
                return tuple(self._decode(row) for row in rows)
        return await asyncio.to_thread(read)

    async def forget_sources(self, request):
        if not request.all_in_scope:
            _refs(request.memory_ids)
        def write():
            with self._db() as db:
                condition = "scope=? AND payload IS NOT NULL"
                args = [request.scope.partition_key()]
                if not request.all_in_scope:
                    condition += """ AND EXISTS (SELECT 1 FROM json_each(sources) s
                        JOIN json_each(?) removed ON s.value=removed.value)"""
                    args.append(json.dumps(request.memory_ids))
                return db.execute("UPDATE recovery_records_v1 SET payload=NULL, "
                                  "revision=revision+1 WHERE " + condition, args).rowcount
        return await asyncio.to_thread(write)
