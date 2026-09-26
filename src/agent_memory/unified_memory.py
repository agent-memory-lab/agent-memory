"""Host-owned, exact-scope raw capture, retrieval and resumable deletion."""
import asyncio
from contextlib import contextmanager
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3

from .capture_api import submit_capture
from .capture_policy import CaptureSanitizer
from .capture_sink import DirectCaptureSink
from .composition import build_local_kernel
from .domain import ForgetMode, ForgetRequest, MemoryQuery
from .providers import GeneratedTrajectoryClaimExtractor


class PendingMemoryDeletion(RuntimeError):
    pass


class _ExactExtractor:
    def __init__(self, generator):
        self.extractor = GeneratedTrajectoryClaimExtractor(generator, fail_open=False)

    async def extract(self, event):
        drafts = await self.extractor.extract(event)
        if any(event.scope.project(d.scope_level) != event.scope for d in drafts):
            raise ValueError("unified capture requires exact-scope generated claims")
        return drafts


class SQLiteDeletionJournal:
    """One pending operation per scope, with no source text in the journal."""
    def __init__(self, path):
        self.path = Path(path)
        if str(path) == ":memory:":
            raise ValueError("deletion journal must survive process restart")

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=5)
        try:
            with db:
                yield db
        finally:
            db.close()

    async def initialize(self):
        def create():
            with self._db() as db:
                db.execute("""CREATE TABLE IF NOT EXISTS unified_deletion_v1 (
                    scope TEXT PRIMARY KEY, request TEXT NOT NULL)""")
        await asyncio.to_thread(create)

    async def pending(self, scope):
        def read():
            with self._db() as db:
                row = db.execute("SELECT request FROM unified_deletion_v1 WHERE scope=?",
                                 (scope.partition_key(),)).fetchone()
                return json.loads(row[0]) if row else None
        return await asyncio.to_thread(read)

    async def begin(self, scope, document):
        payload = json.dumps(document, sort_keys=True)
        def write():
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT request FROM unified_deletion_v1 WHERE scope=?",
                                 (scope.partition_key(),)).fetchone()
                if row and row[0] != payload:
                    raise PendingMemoryDeletion("resume the existing deletion first")
                db.execute("INSERT OR IGNORE INTO unified_deletion_v1 VALUES (?,?)",
                           (scope.partition_key(), payload))
        await asyncio.to_thread(write)

    async def complete(self, scope, document):
        payload = json.dumps(document, sort_keys=True)
        def write():
            with self._db() as db:
                changed = db.execute("DELETE FROM unified_deletion_v1 WHERE scope=? AND request=?",
                                     (scope.partition_key(), payload)).rowcount
                if changed != 1:
                    raise PendingMemoryDeletion("deletion journal changed")
        await asyncio.to_thread(write)


class ManagedOntologyDeletionTarget:
    """Rebuild a configured live index after core deletion; false means pending."""
    def __init__(self, runtime):
        self.runtime = runtime

    async def forget_sources(self, request):
        if request.scope != self.runtime.context.scope:
            raise ValueError("ontology deletion target scope mismatch")
        if not await self.runtime.refresh():
            raise PendingMemoryDeletion("ontology rebuild incomplete; resume deletion")


class UnifiedMemory:
    """One host-owned instance per scope; callers must not bypass this gate.

    Targets expose async forget_sources(ForgetRequest), must be idempotent,
    and must raise on incomplete cleanup. Hosts own all injected resources.
    This is a recovery protocol, not a distributed transaction or writer fence.
    """
    def __init__(self, provider, scope, *, journal, targets=None, sanitizer=None):
        self.provider, self.scope, self.journal = provider, scope, journal
        self.targets = dict(targets or {})
        if len(self.targets) > 16 or any(not isinstance(k, str) or not 1 <= len(k) <= 128 for k in self.targets):
            raise ValueError("configure at most 16 named deletion targets")
        if any(not callable(getattr(v, "forget_sources", None)) for v in self.targets.values()):
            raise TypeError("every deletion target must implement forget_sources")
        self.sink = DirectCaptureSink(provider, sanitizer or CaptureSanitizer())
        self._lock = asyncio.Lock()

    @classmethod
    def local(cls, path, scope, *, generator=None, targets=None, journal_path=None):
        kernel = build_local_kernel(path, extractor=_ExactExtractor(generator) if generator else None)
        return cls(kernel, scope, journal=SQLiteDeletionJournal(journal_path or str(path) + ".deletions.db"), targets=targets)

    async def initialize(self):
        await self.journal.initialize()
        await self.provider.initialize()

    async def close(self):
        await self.provider.close()

    async def _ready(self):
        if await self.journal.pending(self.scope):
            raise PendingMemoryDeletion("memory unavailable until deletion is resumed")

    async def capture(self, *, event_id, role, content, run_id, occurred_at=None):
        if role not in ("user", "assistant", "tool"):
            raise ValueError("role must be user, assistant or tool")
        at = occurred_at or datetime.now(UTC)
        envelope = dict(schema_version=1, event_id=event_id,
            event_type="tool.completed" if role == "tool" else "message.received",
            origin={"user": "user", "assistant": "model", "tool": "tool"}[role],
            occurred_at=at.isoformat(), run_id=run_id, content=content, payload={})
        async with self._lock:
            await self._ready()
            result = await submit_capture(envelope, sink=self.sink, scope=self.scope, actor="host-adapter")
            await self._ready()
            return result

    async def recall(self, text, *, token_budget=1024):
        async with self._lock:
            await self._ready()
            result = await self.provider.retrieve(MemoryQuery(self.scope, text, token_budget=token_budget))
            await self._ready()
            return result

    async def forget_sources(self, source_event_ids=(), *, all_in_scope=False, erase=True):
        ids = tuple(source_event_ids)
        if len(ids) > 256 or any(not isinstance(i, str) or not 1 <= len(i) <= 256 for i in ids):
            raise ValueError("at most 256 source event IDs of 1 to 256 characters are allowed")
        request = ForgetRequest(self.scope, ids, all_in_scope, ForgetMode.ERASE if erase else ForgetMode.ARCHIVE)
        document = dict(ids=sorted(set(ids)), all_in_scope=all_in_scope, mode=request.mode.value,
                        targets=sorted(self.targets))
        async with self._lock:
            await self.journal.begin(self.scope, document)
            return await self._delete(document)

    async def resume_deletion(self):
        async with self._lock:
            document = await self.journal.pending(self.scope)
            return None if document is None else await self._delete(document)

    async def _delete(self, document):
        if document["targets"] != sorted(self.targets):
            raise PendingMemoryDeletion("restore the original deletion target configuration")
        request = ForgetRequest(self.scope, tuple(document["ids"]), document["all_in_scope"], ForgetMode(document["mode"]))
        # Repeat all steps after a crash. No participant may assume exactly-once.
        result = await self.provider.forget(request)
        for name in document["targets"]:
            await self.targets[name].forget_sources(request)
        await self.journal.complete(self.scope, document)
        return result  # Counts describe this attempt, not cumulative retry work.
