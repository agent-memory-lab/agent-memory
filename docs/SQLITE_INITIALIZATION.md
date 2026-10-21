# Atomic SQLite initialization

Concurrent local providers could interleave schema scripts and migrations. A CI
feedback-idempotency test exposed `database schema has changed` during startup.
The two providers' separate asyncio locks do not serialize different instances
or processes, and a legacy `executescript()` can release a transaction early.

Initialization now acquires `BEGIN IMMEDIATE` before schema creation, then uses
Python's explicit transaction-control mode so scripts retain that writer lock.
The repository connection context commits the complete schema/index/migrations
or rolls them all back. The scheduler and lexical initializer retain their
caller's active transaction rather than starting or committing a nested one.
WAL configuration still happens before the schema transaction.

SQLite owns the lock, so it also covers independent processes and path aliases
of the same database. Normal SQLite busy timeout and errors still apply; there
is no blanket retry, sidecar lock file, API retry or swallowed migration error.
Existing databases and schema versions remain compatible, and ordinary read/write
connections keep their previous transaction behavior. This uses the repository's
existing Python 3.13 minimum.

Regression checks cover uncommitted-schema visibility from another connection,
writer exclusion across every script, rollback of early/late failures, successful
reinitialization after rollback, and four independently started Python processes.
The existing concurrent feedback test still verifies one persisted decision and
one idempotent identity. Dedicated CI runs these on Linux, macOS and Windows.

The underlying API contract is documented in Python's
[sqlite3 transaction control](https://docs.python.org/3.13/library/sqlite3.html#transaction-control)
and [executescript](https://docs.python.org/3.13/library/sqlite3.html#sqlite3.Cursor.executescript)
reference. These are storage/concurrency checks, not model-quality evidence.
