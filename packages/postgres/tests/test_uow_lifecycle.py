"""Fault injection against the UoW and the real psycopg pool context protocol.

No live PostgreSQL server is used: connection/BEGIN failures are controlled.
"""

import asyncio
from collections import Counter
from types import SimpleNamespace

import pytest
from psycopg_pool import AsyncConnectionPool

from agent_memory_postgres.repository import PostgresMemoryUnitOfWork


class FakeTransaction:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        self.pool.started.set()
        if self.pool.mode == "entry-error":
            raise RuntimeError("BEGIN failed")
        if self.pool.mode == "entry-cancel":
            await asyncio.Event().wait()
        return self

    async def __aexit__(self, *args):
        self.pool.transaction_exits.append(args)
        if self.pool.mode == "exit-error":
            raise RuntimeError("transaction exit failed")


class FakeConnection:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.pool.connection_exits.append(args)
        self.pool.cleanup_started.set()
        await self.pool.allow_cleanup.wait()
        if self.pool.mode == "connection-exit-error":
            raise RuntimeError("connection exit failed")

    def transaction(self):
        if self.pool.mode == "construction-error":
            raise RuntimeError("transaction construction failed")
        return FakeTransaction(self.pool)


class FakePool:
    # Exercise the installed pool's real context and its finally: putconn.
    connection = AsyncConnectionPool.connection
    _USAGE_MS = AsyncConnectionPool._USAGE_MS

    def __init__(self, mode):
        self.mode = mode
        self.borrowed = self.returned = 0
        self.connection_exits = []
        self.transaction_exits = []
        self._stats = Counter()
        self.started = asyncio.Event()
        self.cleanup_started = asyncio.Event()
        self.allow_cleanup = asyncio.Event()
        self.allow_cleanup.set()

    async def getconn(self, timeout=None):
        if self.mode == "pool-error":
            raise RuntimeError("acquisition failed")
        self.borrowed += 1
        return FakeConnection(self)

    async def putconn(self, connection):
        self.returned += 1


def assert_released(uow, pool, *, entered):
    assert pool.returned == pool.borrowed == 1
    assert len(pool.connection_exits) == 1
    assert len(pool.transaction_exits) == int(entered)
    assert uow.connection is None
    assert uow._exit_stack is None


@pytest.mark.parametrize("mode", ["construction-error", "entry-error", "entry-cancel"])
def test_entry_failure_returns_connection_without_gc(mode):
    async def scenario():
        pool = FakePool(mode)
        uow = PostgresMemoryUnitOfWork(SimpleNamespace(pool=pool))

        async def enter():
            async with uow:
                pytest.fail("failed entry must not reach the body")

        task = asyncio.create_task(enter())
        if mode == "entry-cancel":
            await asyncio.wait_for(pool.started.wait(), 2)
            task.cancel()
        with pytest.raises(asyncio.CancelledError if mode == "entry-cancel" else RuntimeError):
            await task
        # Keep UoW and task references alive: finalizers must not be necessary.
        assert_released(uow, pool, entered=False)

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["entry-error", "entry-cancel"])
def test_repeated_cancellation_waits_for_entry_cleanup(mode):
    async def scenario():
        pool = FakePool(mode)
        pool.allow_cleanup.clear()
        uow = PostgresMemoryUnitOfWork(SimpleNamespace(pool=pool))
        task = asyncio.create_task(uow.__aenter__())
        if mode == "entry-cancel":
            await asyncio.wait_for(pool.started.wait(), 2)
            task.cancel()
        await asyncio.wait_for(pool.cleanup_started.wait(), 2)
        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            assert pool.returned == 0
        pool.allow_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert_released(uow, pool, entered=False)

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["success", "body-error", "exit-error", "connection-exit-error"])
def test_successful_entry_unwinds_once_and_clears_references(mode):
    async def scenario():
        pool = FakePool(mode)
        uow = PostgresMemoryUnitOfWork(SimpleNamespace(pool=pool))

        async def use():
            async with uow as entered:
                assert entered is uow and uow.connection is not None
                if mode == "body-error":
                    raise RuntimeError("body failed")

        if mode == "success":
            await use()
        else:
            with pytest.raises(RuntimeError):
                await use()
        assert_released(uow, pool, entered=True)
        if mode == "exit-error":
            # Connection context sees the transaction failure, not a clean exit.
            assert pool.connection_exits[0][0] is RuntimeError

    asyncio.run(scenario())


def test_pool_entry_failure_clears_unentered_context():
    async def scenario():
        pool = FakePool("pool-error")
        uow = PostgresMemoryUnitOfWork(SimpleNamespace(pool=pool))
        with pytest.raises(RuntimeError, match="acquisition failed"):
            await uow.__aenter__()
        assert pool.borrowed == pool.returned == 0
        assert uow.connection is None and uow._exit_stack is None
        assert not pool.connection_exits and not pool.transaction_exits

    asyncio.run(scenario())


def test_repeated_cancellation_waits_for_normal_exit_pool_return():
    async def scenario():
        pool = FakePool("success")
        pool.allow_cleanup.clear()
        uow = PostgresMemoryUnitOfWork(SimpleNamespace(pool=pool))

        async def use():
            async with uow:
                pass

        task = asyncio.create_task(use())
        await asyncio.wait_for(pool.cleanup_started.wait(), 2)
        try:
            for _ in range(3):
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
        finally:
            pool.allow_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert_released(uow, pool, entered=True)

    asyncio.run(scenario())


@pytest.mark.parametrize("phase,failure", [
    ("entry", "connection"), ("entry", "pool"),
    ("body", "connection"), ("body", "pool"), ("body", "rollback"),
])
@pytest.mark.parametrize("repeat_cancel", [False, True])
def test_original_cancellation_wins_after_cleanup_failure(phase, failure, repeat_cancel):
    async def scenario():
        pool = FakePool("entry-cancel" if phase == "entry" else "success")
        pool.allow_cleanup.clear()
        uow = PostgresMemoryUnitOfWork(SimpleNamespace(pool=pool))
        body_started = asyncio.Event()
        return_finished = asyncio.Event()
        return_attempts = []

        async def putconn(connection):
            return_attempts.append(connection)
            try:
                if failure == "pool":
                    raise RuntimeError("pool return failed")
                pool.returned += 1
            finally:
                return_finished.set()

        pool.putconn = putconn

        async def use():
            async with uow:
                body_started.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(use())
        ready = pool.started if phase == "entry" else body_started
        await asyncio.wait_for(ready.wait(), 2)
        if failure == "rollback":
            pool.mode = "exit-error"
        task.cancel("original cancellation")
        await asyncio.wait_for(pool.cleanup_started.wait(), 2)
        try:
            if failure == "connection":
                pool.mode = "connection-exit-error"
            if repeat_cancel:
                for _ in range(3):
                    task.cancel("later cancellation")
                    await asyncio.sleep(0)
                    assert not task.done()
            assert not return_finished.is_set()
        finally:
            pool.allow_cleanup.set()
        with pytest.raises(asyncio.CancelledError, match="original cancellation") as cancelled:
            await task
        assert task.cancelled()
        # Keep the cleanup failure inspectable without replacing cancellation.
        assert isinstance(cancelled.value.__cause__, RuntimeError)
        # Cancellation is reported only after the owned cleanup attempt settles.
        # A failed putconn is not evidence that the pool accepted the connection.
        assert return_finished.is_set() and len(return_attempts) == 1
        assert pool.returned == (0 if failure == "pool" else 1)
        assert len(pool.connection_exits) == 1
        assert len(pool.transaction_exits) == (0 if phase == "entry" else 1)
        assert uow.connection is None and uow._exit_stack is None

    asyncio.run(scenario())
