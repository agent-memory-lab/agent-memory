"""Real SQLite thread scheduling controls for cancellation and deletion fences."""
import asyncio
from contextlib import closing, contextmanager
from dataclasses import asdict, replace
import inspect
import sqlite3
import threading

import pytest

from agent_memory.context.compression import CompressionPlan, ContextSegment
from agent_memory.context.compression_feedback import CompressionFeedback
from agent_memory.context.recovery import RecoveryState
from agent_memory.context.store import RecoveryConflict, SQLiteRecoveryStore
from agent_memory.domain import ForgetRequest, MemoryScope
from agent_memory.unified_memory import PendingMemoryDeletion, UnifiedMemory


SCOPE = MemoryScope("recovery-erasure", session_id="s")
BODY = "Synthetic recovery ORCHID-824"


class WriteGate:
    """Pause the real synchronous operation before BEGIN, not its asyncio await."""
    def __init__(self, store, monkeypatch, *, kind=None, journal=False):
        self.entered, self.release, self.done = threading.Event(), threading.Event(), threading.Event()
        self.outcomes = []
        original = store._db
        @contextmanager
        def gated():
            frame = inspect.currentframe()
            selected = False
            while frame:
                expected = ("SQLiteDeletionJournal.begin.<locals>.write" if journal
                            else "SQLiteRecoveryStore.write.<locals>.write")
                if frame.f_code.co_qualname == expected:
                    selected = journal or frame.f_locals.get("key", (None, None))[1] == kind
                    break
                frame = frame.f_back
            del frame
            if selected:
                self.entered.set()
                if not self.release.wait(10):
                    raise TimeoutError("test scheduling gate timed out")
            try:
                with original() as db:
                    yield db
                if selected:
                    self.outcomes.append("committed")
            except BaseException as error:
                if selected:
                    self.outcomes.append(type(error).__name__)
                raise
            finally:
                if selected:
                    self.done.set()
        monkeypatch.setattr(store, "_db", gated)


async def prepared(tmp_path):
    memory = UnifiedMemory.local(tmp_path / "core.db", SCOPE,
                                 recovery_path=tmp_path / "recovery.db")
    await memory.initialize()
    receipt = await memory.capture(event_id="source", role="user", content=BODY, run_id="run")
    state = RecoveryState("run", 1, BODY, (receipt.provider_event_id,))
    return memory, state


@pytest.mark.parametrize("kind", ["new-state", "updated-state", "summary", "feedback"])
@pytest.mark.parametrize("cancel_mode", ["none", "once", "twice", "timeout"])
@pytest.mark.parametrize("all_in_scope", [False, True])
def test_cancelled_offload_holds_owner_gate_until_deletion_can_finish(
    tmp_path, monkeypatch, kind, cancel_mode, all_in_scope,
):
    async def run():
        memory, state = await prepared(tmp_path)
        gate = None
        writer = deletion = None
        timer = asyncio.timeout(None)
        try:
            if kind != "new-state":
                await memory.save_recovery(state)
            plan = CompressionPlan("run", 1, (ContextSegment(state.source_event_ids[0], BODY),))
            if kind == "feedback":
                summary = await memory.propose_compression(plan)
                assert summary.accepted
            storage_kind = "state" if "state" in kind else kind
            gate = WriteGate(memory._recovery.store, monkeypatch, kind=storage_kind)
            async def write():
                async with timer:
                    if kind == "new-state":
                        return await memory.save_recovery(state)
                    if kind == "updated-state":
                        return await memory.save_recovery(replace(state, version=2), expected_version=1)
                    if kind == "summary":
                        return await memory.propose_compression(plan)
                    return await memory.record_compression_feedback(CompressionFeedback(
                        "feedback", summary.summary_id, "host", "succeeded", 1000, 500,
                        "tokens", "counter",
                    ))
            writer = asyncio.create_task(write())
            assert await asyncio.to_thread(gate.entered.wait, 5)
            if cancel_mode == "timeout":
                timer.reschedule(asyncio.get_running_loop().time())
            elif cancel_mode != "none":
                writer.cancel()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            if cancel_mode == "twice":
                writer.cancel()
                await asyncio.sleep(0)
            deletion = asyncio.create_task(memory.forget_sources(
                () if all_in_scope else state.source_event_ids, all_in_scope=all_in_scope,
            ))
            await asyncio.sleep(0)
            assert not writer.done(), "cancellation must wait for the real SQLite operation"
            assert memory._lock.locked()
            assert not deletion.done(), "deletion must not overtake the in-flight write"
            gate.release.set()
            if cancel_mode == "none":
                await writer
            else:
                with pytest.raises(TimeoutError if cancel_mode == "timeout" else asyncio.CancelledError):
                    await writer
            await asyncio.wait_for(deletion, 5)
            assert gate.done.is_set() and gate.outcomes == ["committed"]
            assert await memory.journal.pending(SCOPE) is None
            with closing(sqlite3.connect(tmp_path / "recovery.db")) as db:
                assert db.execute("SELECT count(*) FROM recovery_records_v1 WHERE payload IS NOT NULL").fetchone()[0] == 0
            assert await memory.load_recovery("run") is None
            await memory.close()
            restarted = UnifiedMemory.local(tmp_path / "core.db", SCOPE,
                                             recovery_path=tmp_path / "recovery.db")
            await restarted.initialize()
            try:
                assert await restarted.resume_deletion() is None
                assert await restarted.load_recovery("run") is None
            finally:
                await restarted.close()
        finally:
            if gate:
                gate.release.set()
            for task in (writer, deletion):
                if task is not None and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            await memory.close()
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["state", "summary", "feedback", "state_history"])
@pytest.mark.parametrize("all_in_scope", [False, True])
def test_new_identity_commit_rechecks_deleted_receipt(tmp_path, monkeypatch, kind, all_in_scope):
    async def run():
        memory, state = await prepared(tmp_path)
        store = memory._recovery.store
        gate = WriteGate(store, monkeypatch, kind=kind)
        writer = None
        try:
            writer = asyncio.create_task(store.write(SCOPE, kind, "new-identity", asdict(state),
                state.source_event_ids, expected_revision=0))
            assert await asyncio.to_thread(gate.entered.wait, 5)
            # Isolate the store's commit guard from UnifiedMemory's owner gate.
            await store.forget_sources(ForgetRequest(SCOPE,
                () if all_in_scope else state.source_event_ids, all_in_scope))
            gate.release.set()
            with pytest.raises(RecoveryConflict, match="live capture receipts"):
                await writer
            assert gate.outcomes == ["RecoveryConflict"]
            with closing(sqlite3.connect(tmp_path / "recovery.db")) as db:
                assert db.execute("SELECT count(*) FROM recovery_records_v1 WHERE identity='new-identity'").fetchone()[0] == 0
        finally:
            gate.release.set()
            if writer is not None and not writer.done():
                await asyncio.gather(writer, return_exceptions=True)
            await memory.close()
    asyncio.run(run())


def test_commit_receipt_guard_requires_same_run_and_scope(tmp_path):
    async def run():
        memory, state = await prepared(tmp_path)
        try:
            store = memory._recovery.store
            for scope, payload in ((replace(SCOPE, session_id="other"), asdict(state)),
                                   (SCOPE, asdict(replace(state, run_id="other-run")))):
                with pytest.raises(RecoveryConflict, match="live capture receipts"):
                    await store.write(scope, "state", "new", payload, state.source_event_ids,
                                      expected_revision=0)
            await memory.save_recovery(state)
            assert await memory.load_recovery("run") == state
        finally:
            await memory.close()
    asyncio.run(run())


def test_repeated_cancelled_journal_begin_remains_resumable(tmp_path, monkeypatch):
    async def run():
        memory, state = await prepared(tmp_path)
        gate = WriteGate(memory.journal, monkeypatch, journal=True)
        deletion = capture = None
        try:
            deletion = asyncio.create_task(memory.forget_sources(all_in_scope=True))
            assert await asyncio.to_thread(gate.entered.wait, 5)
            deletion.cancel()
            await asyncio.sleep(0)
            deletion.cancel()
            await asyncio.sleep(0)
            capture = asyncio.create_task(memory.capture(
                event_id="late", role="user", content="late sentinel", run_id="late-run"))
            await asyncio.sleep(0)
            assert not deletion.done() and not capture.done()
            gate.release.set()
            with pytest.raises(asyncio.CancelledError):
                await deletion
            with pytest.raises(PendingMemoryDeletion):
                await capture
            assert await memory.journal.pending(SCOPE) is not None
            await memory.resume_deletion()
            assert await memory.journal.pending(SCOPE) is None
            with closing(sqlite3.connect(tmp_path / "recovery.db")) as db:
                assert db.execute("SELECT count(*) FROM recovery_records_v1 WHERE payload IS NOT NULL").fetchone()[0] == 0
        finally:
            gate.release.set()
            for task in (deletion, capture):
                if task and not task.done():
                    await asyncio.gather(task, return_exceptions=True)
            await memory.close()
    asyncio.run(run())
