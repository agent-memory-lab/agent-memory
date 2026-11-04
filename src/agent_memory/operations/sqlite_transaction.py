"""Thread-owned SQLite transactions with cancellation-safe async dispatch.

The compatibility handles deliberately have a *synchronous* API. Async callers
should use UoW methods or run_sync instead of issuing SQL through those handles.
"""

from __future__ import annotations

import asyncio
import inspect
import sqlite3
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from functools import wraps


async def settle(future, *, cancelled):
    """Do not abandon a database operation, even after repeated cancellation.

    Python cannot cancel a SQLite call already running in another thread. Keep
    ownership until it finishes; cleanup must never race that call. Prefer the
    original cancellation to a secondary worker error, but retrieve both.
    """
    cancellation = None
    while True:
        try:
            result = await asyncio.shield(future)
            break
        except asyncio.CancelledError as exc:
            cancellation = exc
            cancelled()
            if future.done():
                # A worker can itself raise CancelledError. Retrieve a completed
                # outcome instead of repeatedly shielding an already-done future.
                try:
                    future.result()
                except BaseException:
                    pass
                raise cancellation
        except BaseException:
            if cancellation is not None:
                raise cancellation from None
            raise
    if cancellation is not None:
        raise cancellation
    return result


class SQLiteTransactionWorker:
    """One executor thread and one private event loop for a transaction's life."""

    def __init__(self):
        self.cancelled = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="memory-sqlite")
        self._thread = None
        self._runner = None
        self._closed = False

    def owns_thread(self):
        return not self._closed and self._thread == threading.get_ident()

    def _invoke(self, callback):
        self._thread = threading.get_ident()
        return callback()

    def _submit(self, callback):
        if self._closed:
            raise RuntimeError("SQLite transaction worker is closed")
        context = copy_context()
        return self._executor.submit(context.run, self._invoke, callback)

    async def run(self, callback):
        if self.owns_thread():
            return callback()

        def invoke():
            try:
                return callback()
            except StopIteration as exc:
                # asyncio futures cannot carry StopIteration as an exception.
                raise RuntimeError("SQLite callback raised StopIteration") from exc

        future = asyncio.wrap_future(self._submit(invoke))
        return await settle(future, cancelled=self.cancelled.set)

    async def run_async(self, callback):
        if self.owns_thread():
            return await callback()

        def invoke():
            if self._runner is None:
                self._runner = asyncio.Runner()
            return self._runner.run(callback(), context=copy_context())

        return await self.run(invoke)

    def blocking(self, callback):
        """Legacy maintenance access: serialized, thread-safe, but blocking."""
        if self.owns_thread():
            return callback()
        return self._submit(callback).result()

    async def close(self):
        def close_runner():
            if self._runner is not None:
                self._runner.close()
                self._runner = None

        try:
            await self.run(close_runner)
        finally:
            self._closed = True
            # All submitted work has settled. No join on the caller's event loop.
            self._executor.shutdown(wait=False)

    def export(self, value):
        """Do not leak thread-bound SQLite handles to an async caller."""
        if isinstance(value, (sqlite3.Connection, sqlite3.Cursor, sqlite3.Blob, Iterator)):
            return _BlockingSQLiteHandle(self, value)
        if isinstance(value, list):
            return [self.export(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self.export(item) for item in value)
        if isinstance(value, dict):
            return {self.export(key): self.export(item) for key, item in value.items()}
        if isinstance(value, (set, frozenset)):
            return type(value)(self.export(item) for item in value)
        if isinstance(getattr(value, "__self__", None),
                      (sqlite3.Connection, sqlite3.Cursor, sqlite3.Blob)):
            def method(*args, **kwargs):
                return self.export(self.blocking(lambda: value(
                    *(self.unwrap(item) for item in args),
                    **{key: self.unwrap(item) for key, item in kwargs.items()},
                )))
            return method
        return value

    def unwrap(self, value):
        if isinstance(value, _BlockingSQLiteHandle):
            if value._worker is not self:
                raise ValueError("SQLite handles belong to different transactions")
            return value._value
        return value


class _BlockingSQLiteHandle:
    """Compatibility proxy; every attribute, method and cursor step is owned.

    This is not a sqlite3.Connection/Cursor subclass. Native extensions requiring
    an actual connection must be invoked through UoW.run_sync on its owner thread.
    """

    def __init__(self, worker, value):
        object.__setattr__(self, "_worker", worker)
        object.__setattr__(self, "_value", value)

    def __getattr__(self, name):
        worker = self._worker

        def attribute():
            value = getattr(self._value, name)
            return getattr(value, "__self__", None) is self._value, value

        is_method, value = worker.blocking(attribute)
        if not is_method:
            return worker.export(value)

        def method(*args, **kwargs):
            return worker.export(worker.blocking(lambda: getattr(self._value, name)(
                *(worker.unwrap(value) for value in args),
                **{key: worker.unwrap(value) for key, value in kwargs.items()},
            )))

        return method

    def __setattr__(self, name, value):
        self._worker.blocking(lambda: setattr(self._value, name, value))

    def __iter__(self):
        return self

    def __next__(self):
        # StopIteration must not cross an asyncio.Future. This path is sync-only.
        return self._worker.export(self._worker.blocking(lambda: next(self._value)))

    def __bool__(self):
        return self._worker.blocking(lambda: bool(self._value))

    def __len__(self):
        return self._worker.blocking(lambda: len(self._value))

    def __getitem__(self, key):
        return self._worker.export(self._worker.blocking(lambda: self._value[key]))

    def __setitem__(self, key, value):
        self._worker.blocking(lambda: self._value.__setitem__(key, value))

    def __enter__(self):
        return self._worker.export(self._worker.blocking(self._value.__enter__))

    def __exit__(self, *args):
        return self._worker.blocking(lambda: self._value.__exit__(*args))


def transaction_methods(cls):
    """Dispatch every public async UoW operation, including newly added ones.

    Internal nested awaits run directly on the same worker and private loop.
    Lifecycle and run_sync own their dispatch explicitly.
    """
    for name, method in tuple(vars(cls).items()):
        if name.startswith("_") or name == "run_sync" or not inspect.iscoroutinefunction(method):
            continue

        def wrap(method):
            @wraps(method)
            async def dispatched(self, *args, **kwargs):
                worker = self._require_worker()
                if worker.owns_thread():
                    return await method(self, *args, **kwargs)

                async def invoke():
                    return worker.export(await method(self, *args, **kwargs))

                return await worker.run_async(invoke)

            return dispatched

        setattr(cls, name, wrap(method))
    return cls
