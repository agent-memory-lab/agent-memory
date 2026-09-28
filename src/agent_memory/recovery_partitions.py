"""Opt-in recovery-store rotation with permanent monotonic generation fences."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sqlite3

from .recovery_store import RecoveryConflict, SQLiteRecoveryStore


class PartitionedRecoveryStore:
    """Bind one exact scope to a catalog and host-authorized generations.

    Task and transport-event IDs must start with the active prefix. The single
    catalog generation replaces per-task tombstones after a partition retires.
    Recovery operations are fenced by a catalog transaction across processes;
    whole provider ingestion still requires UnifiedMemory's exclusive owner.
    """
    def __init__(self, directory, scope, *, authorizer, max_records=10000,
                 max_payload_bytes=65536, max_retained_partitions=8):
        if not callable(getattr(authorizer, "authorize", None)):
            raise TypeError("partition rotation requires a host authorizer")
        if type(max_retained_partitions) is not int or not 1 <= max_retained_partitions <= 128:
            raise ValueError("invalid retained partition limit")
        self.directory, self.scope = Path(directory).resolve(), scope
        self.authorizer = authorizer
        self.max_records, self.max_payload_bytes = max_records, max_payload_bytes
        self.max_retained_partitions = max_retained_partitions
        self._lock = asyncio.Lock()

    @staticmethod
    def _prefix(generation):
        return f"p{generation:016x}:"

    def _store(self, generation):
        path = self.directory / f"recovery-{generation:016x}.db"
        if path.is_symlink():
            raise ValueError("partition files must not be symbolic links")
        return SQLiteRecoveryStore(path, max_records=self.max_records,
                                   max_payload_bytes=self.max_payload_bytes)

    async def initialize(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        def create():
            path = self.directory / "catalog.db"
            if path.is_symlink():
                raise ValueError("catalog must not be a symbolic link")
            db = sqlite3.connect(path, timeout=5)
            try:
                with db:
                    db.execute("CREATE TABLE IF NOT EXISTS recovery_partition_head_v1 "
                               "(id INTEGER PRIMARY KEY CHECK(id=1),scope TEXT,generation INTEGER,retired_through INTEGER)")
                    db.execute("INSERT OR IGNORE INTO recovery_partition_head_v1 VALUES (1,?,1,0)",
                               (self.scope.partition_key(),))
            finally:
                db.close()
        await asyncio.to_thread(create)
        async with self._active(self.scope) as (_, generation, _):
            await self._store(generation).initialize()

    @asynccontextmanager
    async def _active(self, scope):
        if scope != self.scope:
            raise ValueError("partition scope mismatch")
        async with self._lock:
            def connect():
                db = sqlite3.connect(self.directory / "catalog.db", timeout=5, check_same_thread=False)
                try:
                    db.execute("BEGIN IMMEDIATE")
                    row = db.execute("SELECT scope,generation,retired_through FROM recovery_partition_head_v1 WHERE id=1").fetchone()
                    if row is None or row[0] != scope.partition_key():
                        raise ValueError("partition catalog belongs to a different scope")
                    return db, row[1], row[2]
                except BaseException:
                    db.close()
                    raise
            # Connection creation is short bounded SQLite I/O. Keeping it in
            # this thread ensures cancellation cannot orphan a returned handle.
            db, generation, retired = connect()
            try:
                yield db, generation, retired
                db.commit()
            except BaseException:
                db.rollback()
                raise
            finally:
                db.close()

    def _require_prefix(self, generation, identity):
        if not isinstance(identity, str) or not identity.startswith(self._prefix(generation)):
            raise RecoveryConflict("identity belongs to an inactive recovery partition")

    async def partition_info(self):
        async with self._active(self.scope) as (_, generation, retired):
            return dict(generation=generation, required_prefix=self._prefix(generation),
                        retired_through=retired, retained_partitions=generation-retired)

    async def _delegate(self, scope, method, *args, **kwargs):
        async with self._active(scope) as (_, generation, _):
            return await getattr(self._store(generation), method)(scope, *args, **kwargs)

    async def ensure_run(self, scope, run_id):
        async with self._active(scope) as (_, generation, _):
            self._require_prefix(generation, run_id)
            return await self._store(generation).ensure_run(scope, run_id)

    async def write(self, scope, kind, identity, payload, source_event_ids, *, expected_revision):
        async with self._active(scope) as (_, generation, _):
            self._require_prefix(generation, payload.get("run_id"))
            if kind in ("receipt", "job", "state"):
                self._require_prefix(generation, identity)
            store = self._store(generation)
            await store.ensure_run(scope, payload["run_id"])
            return await store.write(scope, kind, identity, payload, source_event_ids,
                                     expected_revision=expected_revision)

    async def read(self, scope, kind, identity):
        return await self._delegate(scope, "read", kind, identity)

    async def sources(self, scope, source_event_ids):
        return await self._delegate(scope, "sources", source_event_ids)

    async def next_job(self, scope):
        return await self._delegate(scope, "next_job")

    async def cancel_job(self, scope, event_id):
        return await self._delegate(scope, "cancel_job", event_id)

    async def configure_run(self, scope, run_id, *, completed=False, expires_at=None):
        async with self._active(scope) as (_, generation, _):
            self._require_prefix(generation, run_id)
            return await self._store(generation).configure_run(scope, run_id,
                completed=completed, expires_at=expires_at)

    async def cleanup(self, scope, *, limit=100):
        return await self._delegate(scope, "cleanup", limit=limit)

    async def stats(self, scope):
        return await self._delegate(scope, "stats")

    async def list_runs(self, scope, *, limit=20, after=None):
        return await self._delegate(scope, "list_runs", limit=limit, after=after)

    async def run_status(self, scope, run_id):
        return await self._delegate(scope, "run_status", run_id)

    async def forget_sources(self, request):
        # Closed retained partitions also contain sensitive derivative data.
        async with self._active(request.scope) as (_, generation, retired):
            count = 0
            for number in range(retired + 1, generation + 1):
                count += await self._store(number).forget_sources(request)
            return count

    async def rotate(self, *, expected_generation, approval):
        if type(expected_generation) is not int or not 1 <= expected_generation < 2**63-1:
            raise ValueError("invalid partition generation")
        if await self.authorizer.authorize(self.scope, "rotate", expected_generation, approval) is not True:
            raise PermissionError("host did not approve partition rotation")
        async with self._active(self.scope) as (db, generation, retired):
            if generation != expected_generation:
                raise RecoveryConflict("active partition changed")
            if generation-retired >= self.max_retained_partitions:
                raise ValueError("retire the oldest closed partition before rotating")
            if (await self._store(generation).stats(self.scope))["active_runs"]:
                raise RecoveryConflict("close or expire every run before rotation")
            await self._store(generation + 1).initialize()
            db.execute("UPDATE recovery_partition_head_v1 SET generation=? WHERE id=1", (generation + 1,))
            return dict(generation=generation + 1, required_prefix=self._prefix(generation + 1))

    async def retire(self, generation, *, approval):
        if type(generation) is not int or generation < 1:
            raise ValueError("invalid retired generation")
        if await self.authorizer.authorize(self.scope, "retire", generation, approval) is not True:
            raise PermissionError("host did not approve partition retirement")
        async with self._active(self.scope) as (db, active, retired):
            if generation <= retired:
                return dict(retired_through=retired)
            if generation >= active or generation != retired + 1:
                raise RecoveryConflict("only the oldest inactive partition can retire")
            # The generation fence is committed before physical cleanup. A
            # deletion failure retains only unreachable files, never old access.
            db.execute("UPDATE recovery_partition_head_v1 SET retired_through=? WHERE id=1", (generation,))
        path = self._store(generation).path
        path.unlink(missing_ok=True)
        return dict(retired_through=generation)
