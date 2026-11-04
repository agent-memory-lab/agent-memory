"""SQLite async calls keep connection ownership and cleanup off the caller loop."""

import asyncio
import inspect
import sqlite3
import threading
import time
from contextlib import closing
from contextvars import ContextVar

import pytest

from agent_memory.domain import ForgetMode, ForgetRequest, MemoryEvent, MemoryScope
from agent_memory.operations import sqlite_consolidation_outbox
from agent_memory.retrieval import sqlite_lexical_index
from agent_memory.sqlite import SQLiteMemoryRepository, SQLiteMemoryUnitOfWork


SCOPE = MemoryScope("async-tenant", session_id="session")


async def ticks_for(seconds=0.12):
    started, ticks = time.monotonic(), 0
    while time.monotonic() - started < seconds:
        ticks += 1
        await asyncio.sleep(0.005)
    return ticks


async def wait_for_thread(event):
    assert await asyncio.to_thread(event.wait, 5), "worker did not reach the expected boundary"


def count_events(path):
    with closing(sqlite3.connect(path)) as connection:
        return connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]


def install_observer(repository, monkeypatch, *, block_at=None, fail_at=None):
    """Instrument actual connections, not a fake async SQLite implementation."""
    entered, resume = threading.Event(), threading.Event()
    calls = []

    class ObservedConnection(sqlite3.Connection):
        def record(self, phase):
            calls.append((phase, threading.get_ident()))
            if phase == block_at:
                entered.set()
                assert resume.wait(5), f"{phase} was not released"
            if phase == fail_at:
                raise sqlite3.OperationalError(f"injected {phase} failure")

        def execute(self, sql, parameters=()):
            self.record("begin" if sql == "BEGIN IMMEDIATE" else "execute")
            return super().execute(sql, parameters)

        def commit(self):
            self.record("commit")
            return super().commit()

        def rollback(self):
            self.record("rollback")
            return super().rollback()

        def close(self):
            self.record("close")
            return super().close()

    def connect():
        connection = sqlite3.connect(repository._path, factory=ObservedConnection)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        calls.append(("connect", threading.get_ident()))
        return connection

    monkeypatch.setattr(repository, "_connect", connect)
    return entered, resume, calls


def test_real_writer_lock_wait_does_not_block_event_loop(tmp_path):
    async def scenario():
        path = tmp_path / "contention.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        acquired, release = threading.Event(), threading.Event()

        def external_writer():
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("BEGIN IMMEDIATE")
                acquired.set()
                assert release.wait(5)
                connection.rollback()

        holder = asyncio.create_task(asyncio.to_thread(external_writer))
        await wait_for_thread(acquired)

        async def write():
            async with repository.unit_of_work() as uow:
                await uow.append_event(MemoryEvent(SCOPE, "note", "after lock"))

        blocked = asyncio.create_task(write())
        try:
            assert await ticks_for() >= 8
            assert not blocked.done()
        finally:
            release.set()
        await holder
        await blocked
        assert count_events(path) == 1

    asyncio.run(scenario())


def test_all_public_async_uow_methods_dispatch_to_transaction_thread():
    operations = [method for name, method in vars(SQLiteMemoryUnitOfWork).items()
                  if not name.startswith("_") and name != "run_sync"
                  and inspect.iscoroutinefunction(method)]
    assert len(operations) > 90
    assert all(hasattr(method, "__wrapped__") for method in operations)


def test_thread_ownership_nested_hooks_raw_compatibility_and_context(tmp_path, monkeypatch):
    async def scenario():
        path = tmp_path / "owner.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        _, _, calls = install_observer(repository, monkeypatch)
        caller = threading.get_ident()
        value = ContextVar("transaction-request", default="missing")
        value.set("request")
        async with repository.unit_of_work() as uow:
            await uow.append_event(MemoryEvent(SCOPE, "note", "thread ownership", id="source"))
            # This operation calls async hooks and other dispatched UoW methods.
            await uow.save_admission_record(
                SCOPE, "record", "source", "slot", {"action": "L0_ONLY"}, 0,
            )
            assert await uow.run_sync(lambda _: value.get()) == "request"
            value.set("updated")
            assert await uow.run_sync(lambda _: value.get()) == "updated"
            connection = uow.connection
            assert not isinstance(connection, sqlite3.Connection)
            assert connection.row_factory is sqlite3.Row
            assert connection.in_transaction
            cursor = connection.execute("SELECT id FROM events")
            assert connection and cursor
            assert [row["id"] for row in cursor] == ["source"]
            cursor.close()
            cursor = await uow.run_sync(lambda db: db.execute("SELECT 7"))
            assert not isinstance(cursor, sqlite3.Cursor)
            assert cursor.fetchone()[0] == 7
            cursor.close()
            nested = await uow.run_sync(lambda db: {"handles": [db.cursor(), db]})
            assert not isinstance(nested["handles"][0], sqlite3.Cursor)
            assert not isinstance(nested["handles"][1], sqlite3.Connection)
            nested["handles"][0].close()
            # Functions registered via the compatibility API execute on its owner.
            connection.create_function("thread_id", 0, threading.get_ident)
            assert connection.execute("SELECT thread_id()").fetchone()[0] != caller
            assert any("CREATE TABLE" in line for line in connection.iterdump())
            assert connection.execute("SELECT 1").connection.in_transaction
            execute = await uow.run_sync(lambda db: db.execute)
            assert execute("SELECT 9").fetchone()[0] == 9

            def lazy(connection):
                yield connection.execute("SELECT thread_id()").fetchone()[0]

            values = await uow.run_sync(lazy)
            assert next(values) != caller
            with pytest.raises(StopIteration):
                next(values)

            def factory(cursor, row):
                yield (threading.get_ident(), row[0])

            connection.row_factory = factory
            assert connection.row_factory is factory
            row = connection.execute("SELECT 42").fetchone()
            assert next(row) == (calls[0][1], 42)
            with pytest.raises(StopIteration):
                next(row)
            connection.row_factory = sqlite3.Row
        assert {thread for _, thread in calls} == {calls[0][1]}
        assert caller != calls[0][1]
        assert {phase for phase, _ in calls} >= {"connect", "begin", "execute", "commit", "close"}
        with pytest.raises(RuntimeError, match="closed"):
            connection.execute("SELECT 1")

    asyncio.run(scenario())


@pytest.mark.parametrize("swallow", [False, True])
def test_cancelled_statement_drains_then_rolls_back_even_if_caught(tmp_path, swallow):
    async def scenario():
        path = tmp_path / "cancel-operation.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        entered, resume = threading.Event(), threading.Event()

        def statement(connection):
            connection.execute("UPDATE events SET content='changed before cancellation'")
            entered.set()
            assert resume.wait(5)

        async def transaction():
            async with repository.unit_of_work() as uow:
                await uow.append_event(MemoryEvent(SCOPE, "note", "never committed"))
                try:
                    await uow.run_sync(statement)
                except asyncio.CancelledError:
                    if not swallow:
                        raise

        task = asyncio.create_task(transaction())
        await wait_for_thread(entered)
        task.cancel()
        try:
            assert await ticks_for() >= 8
            assert not task.done()
            assert repository._write_lock.locked()
            task.cancel()  # Repeated cancellation must not abandon cleanup.
            await asyncio.sleep(0)
            assert count_events(path) == 0
        finally:
            resume.set()
        with pytest.raises(RuntimeError if swallow else asyncio.CancelledError,
                           match="cannot commit" if swallow else None):
            await task
        assert not repository._write_lock.locked()
        assert count_events(path) == 0
        async with repository.unit_of_work() as uow:
            await uow.append_event(MemoryEvent(SCOPE, "note", "next writer succeeds"))
        assert count_events(path) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ["begin", "commit", "rollback", "close"])
def test_cancelled_lifecycle_settles_before_releasing_writer(tmp_path, monkeypatch, phase):
    async def scenario():
        path = tmp_path / (phase + ".db")
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        entered, resume, calls = install_observer(repository, monkeypatch, block_at=phase)

        async def transaction():
            async with repository.unit_of_work() as uow:
                await uow.append_event(MemoryEvent(SCOPE, "note", "atomic"))
                if phase == "rollback":
                    raise ValueError("rollback requested")

        task = asyncio.create_task(transaction())
        await wait_for_thread(entered)
        task.cancel()
        try:
            assert await ticks_for() >= 8
            assert not task.done()
            assert repository._write_lock.locked()
            task.cancel()
            await asyncio.sleep(0)
        finally:
            resume.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not repository._write_lock.locked()
        assert any(action == "close" for action, _ in calls)
        # COMMIT cannot be undone once it is entered. Close cancellation likewise
        # preserves a commit that already completed; all other phases roll back.
        assert count_events(path) == (1 if phase in {"commit", "close"} else 0)
        assert len({thread for _, thread in calls}) == 1

    asyncio.run(scenario())


def test_cancelled_index_sync_rolls_back_before_commit(tmp_path, monkeypatch):
    async def scenario():
        path = tmp_path / "cancel-index.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        entered, resume = threading.Event(), threading.Event()
        original = sqlite_lexical_index.synchronize

        def blocked(connection):
            original(connection)
            entered.set()
            assert resume.wait(5)

        monkeypatch.setattr(sqlite_lexical_index, "synchronize", blocked)

        async def transaction():
            async with repository.unit_of_work() as uow:
                await uow.append_event(MemoryEvent(SCOPE, "note", "indexed atomically"))

        task = asyncio.create_task(transaction())
        await wait_for_thread(entered)
        task.cancel()
        await asyncio.sleep(0)
        resume.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert count_events(path) == 0
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute("SELECT COUNT(*) FROM lexical_chunks").fetchone()[0] == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ["begin", "commit"])
def test_failed_lifecycle_closes_and_releases_transaction(tmp_path, monkeypatch, phase):
    async def scenario():
        path = tmp_path / "failed.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        _, _, calls = install_observer(repository, monkeypatch, fail_at=phase)
        with pytest.raises(sqlite3.OperationalError, match=f"injected {phase}"):
            async with repository.unit_of_work() as uow:
                await uow.append_event(MemoryEvent(SCOPE, "note", "rollback on failure"))
        assert not repository._write_lock.locked()
        assert count_events(path) == 0
        assert any(action == "close" for action, _ in calls)

    asyncio.run(scenario())


def test_independent_repositories_serialize_writers_without_dirty_reads(tmp_path):
    async def scenario():
        path = tmp_path / "isolation.db"
        first, second = SQLiteMemoryRepository(path), SQLiteMemoryRepository(path)
        await first.initialize()
        entered, resume = asyncio.Event(), asyncio.Event()

        async def first_writer():
            async with first.unit_of_work() as uow:
                await uow.append_event(MemoryEvent(SCOPE, "note", "first", id="first"))
                entered.set()
                await resume.wait()

        async def second_writer():
            async with second.unit_of_work() as uow:
                assert await uow.get_source_event(SCOPE, "first") is not None
                await uow.append_event(MemoryEvent(SCOPE, "note", "second", id="second"))

        one = asyncio.create_task(first_writer())
        await entered.wait()
        two = asyncio.create_task(second_writer())
        try:
            assert await ticks_for() >= 8
            assert not two.done()
            # WAL readers proceed while the first writer holds its transaction.
            assert await asyncio.to_thread(count_events, path) == 0
            assert await second.current_claims(SCOPE) == ()
        finally:
            resume.set()
        await asyncio.gather(one, two)
        assert count_events(path) == 2
        async with first.unit_of_work() as uow:
            assert await uow.get_source_event(SCOPE, "second") is not None

    asyncio.run(scenario())


def test_outbox_helpers_and_forget_run_on_transaction_worker(tmp_path, monkeypatch):
    async def scenario():
        path = tmp_path / "outbox.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        caller, observed = threading.get_ident(), []
        original = sqlite_consolidation_outbox.status

        def status(connection, scope):
            observed.append(threading.get_ident())
            time.sleep(0.12)
            return original(connection, scope)

        monkeypatch.setattr(sqlite_consolidation_outbox, "status", status)
        task = asyncio.create_task(repository.consolidation_outbox_status(SCOPE))
        assert await ticks_for() >= 8
        await task
        assert observed and all(thread != caller for thread in observed)
        async with repository.unit_of_work() as uow:
            await uow.append_event(MemoryEvent(SCOPE, "note", "forget", id="source"))
        entered, resume = threading.Event(), threading.Event()
        forget = repository._forget_on_connection

        def blocked(connection, request):
            result = forget(connection, request)
            entered.set()
            assert resume.wait(5)
            return result

        monkeypatch.setattr(repository, "_forget_on_connection", blocked)
        task = asyncio.create_task(repository.forget(ForgetRequest(
            SCOPE, memory_ids=("source",), mode=ForgetMode.ERASE,
        )))
        await wait_for_thread(entered)
        task.cancel()
        try:
            assert await ticks_for() >= 8
            assert repository._write_lock.locked()
        finally:
            resume.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert count_events(path) == 1
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute("SELECT COUNT(*) FROM retention_purges").fetchone()[0] == 0

    asyncio.run(scenario())


def test_worker_raised_cancellation_is_not_swallowed_or_spun(tmp_path):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "worker-cancel.db")
        await repository.initialize()

        def cancel(connection):
            raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            async with repository.unit_of_work() as uow:
                await uow.run_sync(cancel)
        assert not repository._write_lock.locked()

    asyncio.run(scenario())


def test_cancelled_repository_lock_wait_does_not_release_another_transaction(tmp_path):
    async def scenario():
        path = tmp_path / "local-lock.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        second = repository.unit_of_work()
        async with repository.unit_of_work() as first:
            await first.append_event(MemoryEvent(SCOPE, "note", "first"))
            waiting = asyncio.create_task(second.__aenter__())
            await asyncio.sleep(0)
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            assert repository._write_lock.locked()
            assert count_events(path) == 0
        assert count_events(path) == 1
        async with second:
            assert await second.run_sync(lambda db: db.execute("SELECT 1").fetchone()[0]) == 1

    asyncio.run(scenario())


def test_parallel_operations_and_repeated_transactions_remain_atomic(tmp_path):
    async def scenario():
        path = tmp_path / "parallel.db"
        repositories = [SQLiteMemoryRepository(path), SQLiteMemoryRepository(path)]
        await repositories[0].initialize()

        async def transaction(index):
            async with repositories[index % 2].unit_of_work() as uow:
                await asyncio.gather(*(uow.append_event(MemoryEvent(
                    SCOPE, "note", f"transaction-{index}-{item}", id=f"event-{index}-{item}",
                )) for item in range(3)))
                assert await uow.run_sync(lambda db: db.execute(
                    "SELECT COUNT(*) FROM events WHERE id LIKE ?", (f"event-{index}-%",)
                ).fetchone()[0]) == 3
                if index % 3 == 0:
                    raise ValueError("discard this whole transaction")

        results = await asyncio.gather(*(transaction(index) for index in range(12)),
                                       return_exceptions=True)
        assert sum(isinstance(value, ValueError) for value in results) == 4
        assert all(value is None or isinstance(value, ValueError) for value in results)
        assert count_events(path) == 24
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

    asyncio.run(scenario())


def test_cancelled_real_begin_lock_wait_drains_and_releases_connection(tmp_path):
    async def scenario():
        path = tmp_path / "cancel-contention.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        acquired, release = threading.Event(), threading.Event()

        def hold():
            with closing(sqlite3.connect(path)) as db:
                db.execute("BEGIN IMMEDIATE")
                acquired.set()
                assert release.wait(5)
                db.rollback()

        holder = asyncio.create_task(asyncio.to_thread(hold))
        await wait_for_thread(acquired)
        uow = repository.unit_of_work()
        task = asyncio.create_task(uow.__aenter__())
        await asyncio.sleep(0.02)
        task.cancel()
        try:
            assert await ticks_for() >= 8
            assert not task.done()
            assert repository._write_lock.locked()
        finally:
            release.set()
        await holder
        with pytest.raises(asyncio.CancelledError):
            await task
        assert uow._connection is None
        assert not repository._write_lock.locked()
        async with repository.unit_of_work() as next_uow:
            await next_uow.append_event(MemoryEvent(SCOPE, "note", "writer after cancellation"))
        assert count_events(path) == 1

    asyncio.run(scenario())


def test_run_sync_rejects_async_callbacks_and_handles_stop_iteration(tmp_path):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "callback-types.db")
        await repository.initialize()

        async def wrong(connection):
            return 1

        async with repository.unit_of_work() as uow:
            with pytest.raises(TypeError, match="synchronous callback"):
                await uow.run_sync(wrong)
            with pytest.raises(RuntimeError, match="StopIteration"):
                await uow.run_sync(lambda connection: next(iter(())))
            assert await uow.run_sync(lambda connection: 1) == 1

    asyncio.run(scenario())
