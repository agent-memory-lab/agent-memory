"""Host-owned, exact-scope raw capture, retrieval and resumable deletion."""
import asyncio
from contextlib import contextmanager
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3

from .capture.api import submit_capture
from .capture.policy import CaptureSanitizer
from .capture.sink import DirectCaptureSink
from .composition import build_local_kernel
from .domain import ForgetMode, ForgetRequest, MemoryQuery
from .providers import GeneratedTrajectoryClaimExtractor
from .context.operations import RecoveryOperations


class PendingMemoryDeletion(RuntimeError):
    pass


class _ExactExtractor:
    def __init__(self, generator):
        self.extractor = GeneratedTrajectoryClaimExtractor(generator, fail_open=False)

    async def extract(self, event):
        drafts = await self.extractor.extract(event)
        if any(event.scope.project(d.scope_level) != event.scope for d in drafts):
            raise ValueError("unified capture requires exact-scope generated claims")
        from .context.recovery import _extracted_event
        _extracted_event.set(event.id)
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


class UnifiedMemory(RecoveryOperations):
    """One host-owned instance per scope; callers must not bypass this gate.

    Targets expose async forget_sources(ForgetRequest), must be idempotent,
    and must raise on incomplete cleanup. Hosts own all injected resources.
    This is a recovery protocol, not a distributed transaction or writer fence.
    """
    def __init__(self, provider, scope, *, journal, targets=None, sanitizer=None,
                 recovery=None, compressor=None, compression_validator=None,
                 token_counter=None, strategy_registry=None):
        self.provider, self.scope, self.journal = provider, scope, journal
        self.targets = dict(targets or {})
        self._recovery, self._compression = recovery, None
        if recovery is not None:
            if recovery.scope != scope or "__recovery_v1" in self.targets:
                raise ValueError("recovery scope mismatch or reserved deletion target")
            self.targets["__recovery_v1"] = recovery
            from .context.compression import CompressionCoordinator
            self._compression = CompressionCoordinator(recovery, compressor=compressor,
                validator=compression_validator, counter=token_counter, strategy_registry=strategy_registry)
        elif any(value is not None for value in (compressor, compression_validator, token_counter, strategy_registry)):
            raise ValueError("compression requires recovery persistence")
        if len(self.targets) > 16 or any(not isinstance(k, str) or not 1 <= len(k) <= 128 for k in self.targets):
            raise ValueError("configure at most 16 named deletion targets")
        if any(not callable(getattr(v, "forget_sources", None)) for v in self.targets.values()):
            raise TypeError("every deletion target must implement forget_sources")
        self.sink = DirectCaptureSink(provider, sanitizer or CaptureSanitizer())
        if recovery is not None:
            from .context.recovery import RecoveryCaptureSink
            self.sink = RecoveryCaptureSink(provider, self.sink.sanitizer, recovery)
        self._lock = asyncio.Lock()

    @classmethod
    def local(cls, path, scope, *, generator=None, targets=None, journal_path=None,
              recovery_path=None, retrieval_probe=None, compressor=None,
              compression_validator=None, token_counter=None, recovery_store=None, strategy_registry=None):
        kernel = build_local_kernel(path, extractor=_ExactExtractor(generator) if generator else None)
        recovery = None
        if recovery_path is not None and recovery_store is not None:
            raise ValueError("choose recovery_path or recovery_store, not both")
        if recovery_path is not None or recovery_store is not None:
            if str(path) == ":memory:":
                raise ValueError("recovery requires persistent source evidence")
            from .context.recovery import RecoveryMemory, RepositoryEvidenceVerifier
            from .context.store import SQLiteRecoveryStore
            from .sqlite import SQLiteMemoryRepository
            from .context.operations import LocalRetrievalReadinessProbe
            evidence_repository = SQLiteMemoryRepository(path)
            evidence = RepositoryEvidenceVerifier(evidence_repository)
            recovery = RecoveryMemory(scope, recovery_store if recovery_store is not None else SQLiteRecoveryStore(recovery_path),
                evidence, retrieval_probe=retrieval_probe if retrieval_probe is not None else
                LocalRetrievalReadinessProbe(evidence_repository, kernel, evidence))
        elif retrieval_probe is not None:
            raise ValueError("retrieval probe requires recovery persistence")
        return cls(kernel, scope, journal=SQLiteDeletionJournal(journal_path or str(path) + ".deletions.db"),
                   targets=targets, recovery=recovery, compressor=compressor,
                   compression_validator=compression_validator, token_counter=token_counter,
                   strategy_registry=strategy_registry)

    async def initialize(self):
        await self.journal.initialize()
        await self.provider.initialize()
        if self._recovery is not None:
            await self._recovery.initialize()
            if self._compression.strategy_registry is not None:
                await self._compression.strategy_registry.initialize()

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

    async def recall(self, text, *, token_budget=1024, limit=8, include_current_state=True):
        async with self._lock:
            await self._ready()
            result = await self.provider.retrieve(MemoryQuery(
                self.scope, text, limit=limit, token_budget=token_budget,
                include_current_state=include_current_state,
            ))
            await self._ready()
            return result

    def _require_recovery(self):
        if self._recovery is None:
            raise RuntimeError("enable recovery_path or inject RecoveryMemory first")

    async def capture_with_receipt(self, **event):
        self._require_recovery()
        result = await self.capture(**event)
        receipt = await self.capture_receipt(result.event_id)
        if receipt is None:
            raise PendingMemoryDeletion("capture evidence is no longer available")
        return receipt

    async def capture_receipt(self, event_id):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            result = await self._recovery.receipt(event_id)
            await self._ready()
            return result

    async def save_recovery(self, state, *, expected_version=0):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            result = await self._recovery.save(state, expected_version=expected_version)
            await self._ready()
            return result

    async def load_recovery(self, run_id):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            result = await self._recovery.load(run_id)
            await self._ready()
            return result

    async def propose_compression(self, plan):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            result = await self._compression.propose(plan)
            await self._ready()
            return result

    async def load_compression(self, summary_id):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            result = await self._compression.load(summary_id)
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
