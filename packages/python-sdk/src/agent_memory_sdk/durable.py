"""Host-side SQLite pending submissions. Persist before attempting transport."""

import json
import sqlite3
from contextlib import contextmanager
from hashlib import sha256

from agent_memory.serialization import to_jsonable

from .durable_dispositions import settle
from .durable_purge import identity_hash, purge_session, synchronize


class DurableOutbox:
    def __init__(self, path, session, *, sync_purges=False, settle_purges=False):
        if type(sync_purges) is not bool:
            raise ValueError("sync_purges must be boolean")
        if type(settle_purges) is not bool or (settle_purges and not sync_purges):
            raise ValueError("settle_purges requires purge synchronization")
        self.sync_purges, self.settle_purges = sync_purges, settle_purges
        self.path = str(path)
        self.session = to_jsonable(session)
        self.session_key = sha256(json.dumps(self.session, sort_keys=True).encode()).hexdigest()
        with self._connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS durable_sessions (
                    session_key TEXT PRIMARY KEY, revoked INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS durable_purged_ids (event_sha256 TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS durable_pending (
                    session_key TEXT NOT NULL, sequence INTEGER NOT NULL, event_id TEXT NOT NULL,
                    event_json TEXT NOT NULL, content_sha256 TEXT NOT NULL,
                    acknowledged INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(session_key,sequence), UNIQUE(session_key,event_id)
                );
            """)
            conn.execute("BEGIN IMMEDIATE")
            session_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(durable_sessions)")
            }
            for name, declaration in (
                ("purge_cursor", "INTEGER NOT NULL DEFAULT 0"),
                ("scope_key", "TEXT"),
                ("epoch", "INTEGER"),
            ):
                if name not in session_columns:
                    conn.execute(f"ALTER TABLE durable_sessions ADD COLUMN {name} {declaration}")
            columns = {row[1] for row in conn.execute("PRAGMA table_info(durable_pending)")}
            for name in ("purged", "cancel_confirmed"):
                if name not in columns:
                    conn.execute(
                        f"ALTER TABLE durable_pending ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0"
                    )
            if "operation" not in columns:
                conn.execute(
                    "ALTER TABLE durable_pending "
                    "ADD COLUMN operation TEXT NOT NULL DEFAULT 'append'"
                )
            if "revision_json" not in columns:
                conn.execute("ALTER TABLE durable_pending ADD COLUMN revision_json TEXT")
            conn.execute(
                "INSERT OR IGNORE INTO durable_sessions(session_key) VALUES (?)",
                (self.session_key,),
            )
            conn.execute(
                "UPDATE durable_sessions SET epoch=? WHERE session_key=?",
                (self.session["epoch"], self.session_key),
            )

    def _check_live(self, conn):
        if conn.execute(
            "SELECT revoked FROM durable_sessions WHERE session_key=?", (self.session_key,)
        ).fetchone()[0]:
            raise ValueError("pending session was purged")

    @contextmanager
    def _connection(self):
        conn = sqlite3.connect(self.path, timeout=30)
        try:
            conn.execute("PRAGMA secure_delete=ON")
            with conn:
                yield conn
        finally:
            conn.close()

    def append(self, sanitized_event):
        """Accept an already sanitized lifecycle envelope from a trusted host."""
        return self._append(sanitized_event, "append", None)

    def append_revision(self, sanitized_event, *, base_event_id, expected_revision):
        if (
            not isinstance(base_event_id, str)
            or not 1 <= len(base_event_id) <= 256
            or type(expected_revision) is not int
            or expected_revision < 1
        ):
            raise ValueError("invalid source revision")
        return self._append(
            sanitized_event,
            "revise",
            {"base_event_id": base_event_id, "expected_revision": expected_revision},
        )

    def _append(self, sanitized_event, operation, revision):
        revision_json = json.dumps(revision, sort_keys=True) if revision else None
        encoded = json.dumps(sanitized_event, sort_keys=True, ensure_ascii=False, allow_nan=False)
        if len(encoded.encode()) > 128_000:
            raise ValueError("pending submission exceeds size limit")
        event_id = sanitized_event["event_id"]
        digest = sha256(encoded.encode()).hexdigest()
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._check_live(conn)
            if conn.execute(
                "SELECT 1 FROM durable_purged_ids WHERE event_sha256=?", (identity_hash(event_id),)
            ).fetchone():
                raise ValueError("source identity was purged")
            found = conn.execute(
                "SELECT sequence,content_sha256,operation,revision_json FROM durable_pending "
                "WHERE session_key=? AND event_id=?",
                (self.session_key, event_id),
            ).fetchone()
            if found:
                if found[1:] != (digest, operation, revision_json):
                    raise ValueError("pending source identity reused with different content")
                return found[0]
            count = conn.execute(
                "SELECT count(*) FROM durable_pending WHERE session_key=? AND acknowledged=0",
                (self.session_key,),
            ).fetchone()[0]
            if count >= 1000:
                raise ValueError("pending submission capacity exceeded")
            seq = conn.execute(
                "SELECT coalesce(max(sequence),0)+1 FROM durable_pending WHERE session_key=?",
                (self.session_key,),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO durable_pending "
                "(session_key,sequence,event_id,event_json,content_sha256,"
                "acknowledged,operation,revision_json) "
                "VALUES (?,?,?,?,?,0,?,?)",
                (self.session_key, seq, event_id, encoded, digest, operation, revision_json),
            )
            return seq

    async def synchronize_purges(self, client, *, max_pages=32):
        return await synchronize(self, client, max_pages=max_pages)

    async def flush_one(self, client):
        if self.sync_purges:
            await self.synchronize_purges(client)
        if self.settle_purges:
            await settle(self, client)
        with self._connection() as conn:
            self._check_live(conn)
            row = conn.execute(
                "SELECT sequence,event_json,content_sha256,operation,revision_json "
                "FROM durable_pending "
                "WHERE session_key=? AND acknowledged=0 ORDER BY sequence LIMIT 1",
                (self.session_key,),
            ).fetchone()
        if row is None:
            return None
        revision = json.loads(row[4]) if row[4] else None
        if row[3] == "revise":
            response = await client.durable_revise(
                json.loads(row[1]), self.session, row[0], **revision
            )
        else:
            response = await client.durable_append(json.loads(row[1]), self.session, row[0])
        if (
            response.get("operation", "append") != row[3]
            or response.get("revision") != revision
            or response.get("producer_id") != self.session["producer_id"]
            or response.get("epoch") != self.session["epoch"]
            or response.get("event_sha256") != row[2]
            or response.get("sequence") != row[0]
        ) or response.get("receipt", {}).get("status") not in {
            "queued",
            "running",
            "retry_wait",
            "completed",
            "dead",
            "superseded",
        }:
            raise ValueError("invalid durable acknowledgment")
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._check_live(conn)
            event_id = json.loads(row[1])["event_id"]
            if conn.execute(
                "SELECT 1 FROM durable_purged_ids WHERE event_sha256=?", (identity_hash(event_id),)
            ).fetchone():
                raise ValueError("pending submission purged during delivery")
            # Remove the body after acknowledgment; keep identity/sequence to avoid reuse.
            conn.execute(
                "UPDATE durable_pending SET acknowledged=1,event_json='null' "
                "WHERE session_key=? AND sequence=?",
                (self.session_key, row[0]),
            )
        return response

    def purge(self):
        """Erase this session's pending bodies and block their identity across this file."""
        purge_session(self)
