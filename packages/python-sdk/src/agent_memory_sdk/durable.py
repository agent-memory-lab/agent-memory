"""Host-side SQLite pending submissions. Persist before attempting transport."""

import json
import sqlite3
from contextlib import contextmanager
from hashlib import sha256

from agent_memory.serialization import to_jsonable


class DurableOutbox:
    def __init__(self, path, session):
        self.path = str(path)
        self.session = to_jsonable(session)
        self.session_key = sha256(json.dumps(self.session, sort_keys=True).encode()).hexdigest()
        with self._connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS durable_sessions (
                    session_key TEXT PRIMARY KEY, revoked INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS durable_pending (
                    session_key TEXT NOT NULL, sequence INTEGER NOT NULL, event_id TEXT NOT NULL,
                    event_json TEXT NOT NULL, content_sha256 TEXT NOT NULL,
                    acknowledged INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(session_key,sequence), UNIQUE(session_key,event_id)
                );
            """)
            conn.execute(
                "INSERT OR IGNORE INTO durable_sessions(session_key) VALUES (?)",
                (self.session_key,),
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
            with conn:
                yield conn
        finally:
            conn.close()

    def append(self, sanitized_event):
        """Accept an already sanitized lifecycle envelope from a trusted host."""
        encoded = json.dumps(sanitized_event, sort_keys=True, ensure_ascii=False, allow_nan=False)
        if len(encoded.encode()) > 128_000:
            raise ValueError("pending submission exceeds size limit")
        event_id = sanitized_event["event_id"]
        digest = sha256(encoded.encode()).hexdigest()
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._check_live(conn)
            found = conn.execute(
                "SELECT sequence,content_sha256 FROM durable_pending "
                "WHERE session_key=? AND event_id=?",
                (self.session_key, event_id),
            ).fetchone()
            if found:
                if found[1] != digest:
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
                "INSERT INTO durable_pending VALUES (?,?,?,?,?,0)",
                (self.session_key, seq, event_id, encoded, digest),
            )
            return seq

    async def flush_one(self, client):
        with self._connection() as conn:
            self._check_live(conn)
            row = conn.execute(
                "SELECT sequence,event_json,content_sha256 FROM durable_pending "
                "WHERE session_key=? AND acknowledged=0 ORDER BY sequence LIMIT 1",
                (self.session_key,),
            ).fetchone()
        if row is None:
            return None
        response = await client.durable_append(json.loads(row[1]), self.session, row[0])
        if (
            response.get("producer_id") != self.session["producer_id"]
            or response.get("epoch") != self.session["epoch"]
            or response.get("event_sha256") != row[2]
            or response.get("sequence") != row[0]
        ) or response.get("receipt", {}).get("status") not in {
            "queued",
            "running",
            "retry_wait",
            "completed",
            "dead",
        }:
            raise ValueError("invalid durable acknowledgment")
        with self._connection() as conn:
            # Remove the body after acknowledgment; keep identity/sequence to avoid reuse.
            conn.execute(
                "UPDATE durable_pending SET acknowledged=1,event_json='null' "
                "WHERE session_key=? AND sequence=?",
                (self.session_key, row[0]),
            )
        return response

    def purge(self):
        """Host deletion hook: erase pending bodies, retain sequence tombstones."""
        with self._connection() as conn:
            conn.execute(
                "UPDATE durable_sessions SET revoked=1 WHERE session_key=?", (self.session_key,)
            )
            conn.execute(
                "UPDATE durable_pending SET acknowledged=1,event_json='null' WHERE session_key=?",
                (self.session_key,),
            )
