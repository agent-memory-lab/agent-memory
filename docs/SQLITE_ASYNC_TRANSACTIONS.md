# SQLite async transaction isolation

`SQLiteMemoryRepository.unit_of_work()` owns one SQLite connection, one worker
thread and one private async runner until the transaction is fully closed.
Connection creation, `BEGIN IMMEDIATE`, every public async UoW operation, nested
storage hooks, lexical-index synchronization, commit, rollback and close all run
on that thread. SQLite's `check_same_thread` protection stays enabled.

The repository's consolidation-outbox methods and `forget()` use this same
boundary. Existing repository reads retain their worker-thread connections and
snapshot transactions, including bounded state reads. WAL readers can continue
while a writer holds an uncommitted transaction. Writers remain serialized by
SQLite across repository instances/processes and by the repository's async lock
within one instance. This is event-loop isolation, not parallel SQLite writers
or a claim of bounded database latency. Each public UoW operation crosses a
thread boundary, so workloads with many tiny operations can take longer even
though they no longer block the caller's event loop.

## Cancellation and atomicity

- Cancelling a statement or transaction entry does not abandon its running
  SQLite call. The await drains that call, then rolls back and closes before
  releasing the writer lock. Repeated cancellation cannot skip cleanup.
- A cancelled UoW operation marks the transaction rollback-only. If application
  code catches `CancelledError` inside the context, further async operations are
  rejected. Normal context exit rolls back and raises `RuntimeError` rather than
  silently committing the cancelled operation.
- Cancellation observed during lexical-index synchronization is checked again before
  `COMMIT`, so source data and index changes roll back together.
- Once SQLite `COMMIT` has started, cancellation cannot undo it. The task waits
  for commit and close to settle, then propagates cancellation. The write may
  already be committed, including when cancellation arrives during close.
  Callers should use the existing idempotency keys/read-back checks when retrying
  a cancelled write; cancellation alone is not proof of rollback.
- Cleanup errors still attempt rollback and close. A cancelled in-flight call
  may take up to SQLite's busy timeout (normally 30 seconds), or however long a
  host callback takes, before the cancellation can finish. The caller's event
  loop remains available during that wait.

These guarantees concern UoW transactions and repository writes routed through
one. Existing independent read/initialization worker calls are not interruptible
SQLite statements; their connection contexts still own their cleanup.

## Host maintenance and compatibility

Use `run_sync` for custom SQL or a synchronous SQLite helper. The callable runs
on the transaction thread and receives the real connection:

```python
async with repository.unit_of_work() as uow:
    rows = await uow.run_sync(
        lambda connection: connection.execute(
            "SELECT id FROM events WHERE partition_key = ?", (scope.partition_key(),)
        ).fetchall()
    )
```

The callback must be synchronous and must not retain the connection, start
background work on it, or manage the outer transaction's commit/rollback/close.
Return materialized data. Connections/cursors, bound SQLite methods and lazy
iterators returned directly or in ordinary list/tuple/dict/set containers are
wrapped instead of escaping as raw thread-bound handles. Iterator stepping is
also synchronous; consume it inside the callback when async behavior matters.
Native extensions requiring a real `sqlite3.Connection` must run inside the callback.

For existing maintenance callers, `uow.connection` provides synchronous
connection/cursor access through a serialized compatibility proxy. `execute`,
`executemany`, cursor iteration/fetching and connection attributes still route
to the owner thread. This compatibility access **blocks its calling thread**;
it is not an async API. The proxy is not a `sqlite3.Connection` or
`sqlite3.Cursor` instance. Code relying on those concrete types should migrate
to `run_sync`. Handles must not outlive the UoW or be exchanged between different
transaction workers. Await every operation before leaving its UoW context.

No database schema, public async method signature, retention limit or persisted
format changes. The concrete type of the synchronous `connection` escape hatch
is the compatibility change. PostgreSQL behavior is unchanged.

Purge-stream maintenance still performs synchronous binary file I/O and retains
its offline, potentially long transaction contract. This change does not make
file/network streams asynchronous, bound complete replay latency, or add an
online replay gate. See [purge stream limits](PURGE_RESTORE_STREAMS.md).

## Validation

`tests/test_sqlite_async_isolation.py` uses real SQLite transactions and a
separate lock-holding connection, verifies event-loop timer progress, checks
thread ownership through nested hooks and compatibility handles, and covers
cancellation during entry/statements/index synchronization/commit/rollback/close,
rollback-only behavior, lifecycle failures, independent writers and WAL readers.
Existing lexical, bounded-state, outbox, maintenance and purge-restore suites
exercise the unchanged storage contracts.
