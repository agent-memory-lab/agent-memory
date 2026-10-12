"""Durable, protocol-frozen task accounting for a resumable local experiment.

Only the main worker calls recover, after ensuring that no previous worker is
still running. SQLite atomically claims tasks; abandoned attempts consume the
same finite attempt budget as failed attempts. Unknown judgments remain unknown
and every planned task stays in the accuracy denominator.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from threading import RLock


def _canonical(value):
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError) as error:
        raise ValueError("ledger documents must be finite JSON data") from error


def _now():
    return datetime.now(UTC).isoformat()


def _labels(values, name):
    values = tuple(values)
    if not values or len(values) > 100_000:
        raise ValueError(f"{name} must be a bounded nonempty sequence")
    if any(not isinstance(value, str) or not 1 <= len(value) <= 128 for value in values):
        raise ValueError(f"{name} must contain bounded string labels")
    if len(set(values)) != len(values):
        raise ValueError(f"{name} must contain unique labels")
    return values


class RunLedger:
    """Single-worker run state with SQL summaries and lazy result iteration."""

    def __init__(self, path, manifest, task_ids, arms, max_attempts=2):
        if not isinstance(manifest, dict) or not manifest:
            raise ValueError("manifest must be a nonempty complete protocol document")
        if (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)
            or not 1 <= max_attempts <= 10
        ):
            raise ValueError("max_attempts must be an integer between 1 and 10")
        self._task_ids = _labels(task_ids, "task_ids")
        self._arms = _labels(arms, "arms")
        if len(self.task_ids) * len(self.arms) > 100_000:
            raise ValueError("planned task set exceeds ledger capacity")
        self._manifest = json.loads(_canonical(manifest))
        self.protocol_hash = sha256(_canonical(self.manifest).encode("utf-8")).hexdigest()
        self._max_attempts = max_attempts
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            self.path, timeout=5, isolation_level=None, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        configuration = _canonical(
            {
                "schema_version": 1,
                "manifest": self.manifest,
                "task_ids": self.task_ids,
                "arms": self.arms,
                "max_attempts": max_attempts,
            }
        )
        try:
            with self._transaction() as connection:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS run_metadata "
                    "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                connection.execute("""CREATE TABLE IF NOT EXISTS tasks (
                    question_id TEXT NOT NULL, arm TEXT NOT NULL, ordinal INTEGER NOT NULL UNIQUE,
                    status TEXT NOT NULL
                        CHECK(status IN ('pending','running','completed','failed')),
                    attempt INTEGER NOT NULL DEFAULT 0,
                    judge_label INTEGER CHECK(judge_label IN (0,1)),
                    error_type TEXT, payload_json TEXT, updated_at TEXT NOT NULL,
                    PRIMARY KEY (question_id, arm))""")
                connection.execute("""CREATE TABLE IF NOT EXISTS attempts (
                    question_id TEXT NOT NULL, arm TEXT NOT NULL, attempt INTEGER NOT NULL,
                    status TEXT NOT NULL
                        CHECK(status IN ('running','completed','failed','abandoned')),
                    error_type TEXT, payload_json TEXT, started_at TEXT NOT NULL, finished_at TEXT,
                    PRIMARY KEY (question_id, arm, attempt),
                    FOREIGN KEY(question_id, arm) REFERENCES tasks(question_id, arm))""")
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS tasks_pending_order ON tasks(status,ordinal)"
                )
                stored = connection.execute(
                    "SELECT value FROM run_metadata WHERE key='configuration'"
                ).fetchone()
                if stored is None:
                    if connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]:
                        raise ValueError("existing task data has no protocol manifest")
                    connection.executemany(
                        "INSERT INTO run_metadata(key,value) VALUES (?,?)",
                        (("configuration", configuration), ("protocol_hash", self.protocol_hash)),
                    )
                    created = _now()
                    connection.executemany(
                        "INSERT INTO tasks(question_id,arm,ordinal,status,updated_at) "
                        "VALUES (?,?,?,'pending',?)",
                        (
                            (question_id, arm, question_index * len(self.arms) + arm_index, created)
                            for question_index, question_id in enumerate(self.task_ids)
                            for arm_index, arm in enumerate(self.arms)
                        ),
                    )
                elif stored["value"] != configuration:
                    raise ValueError(
                        "run protocol, model, dependencies, settings, or planned task set changed"
                    )
                else:
                    stored_hash = connection.execute(
                        "SELECT value FROM run_metadata WHERE key='protocol_hash'"
                    ).fetchone()
                    if stored_hash is None or stored_hash["value"] != self.protocol_hash:
                        raise ValueError("run manifest hash mismatch")
                    if connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] != len(
                        self.task_ids
                    ) * len(self.arms):
                        raise ValueError("stored planned task set is incomplete")
                    expected = (
                        (question_id, arm, question_index * len(self.arms) + arm_index)
                        for question_index, question_id in enumerate(self.task_ids)
                        for arm_index, arm in enumerate(self.arms)
                    )
                    actual = connection.execute(
                        "SELECT question_id,arm,ordinal FROM tasks ORDER BY ordinal"
                    )
                    if any(tuple(row) != task for row, task in zip(actual, expected, strict=True)):
                        raise ValueError("stored planned task identities or order changed")
        except Exception:
            self._connection.close()
            self._closed = True
            raise

    @contextmanager
    def _transaction(self):
        with self._lock:
            if self._closed:
                raise RuntimeError("run ledger is closed")
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise

    @property
    def task_ids(self):
        return self._task_ids

    @property
    def arms(self):
        return self._arms

    @property
    def manifest(self):
        return deepcopy(self._manifest)

    @property
    def max_attempts(self):
        return self._max_attempts

    def next_task(self):
        """Atomically claim the next pending task, including finite retries."""
        with self._transaction() as connection:
            connection.execute(
                """UPDATE tasks SET status='failed',
                   error_type='AttemptBudgetExhausted', updated_at=?
                   WHERE status='pending' AND attempt>=?""",
                (_now(), self.max_attempts),
            )
            row = connection.execute(
                "SELECT * FROM tasks WHERE status='pending' ORDER BY ordinal LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            attempt = row["attempt"] + 1
            started = _now()
            connection.execute(
                """UPDATE tasks SET status='running', attempt=?, judge_label=NULL, error_type=NULL,
                   updated_at=? WHERE question_id=? AND arm=?""",
                (attempt, started, row["question_id"], row["arm"]),
            )
            connection.execute(
                """INSERT INTO attempts(question_id,arm,attempt,status,started_at)
                   VALUES (?,?,?,'running',?)""",
                (row["question_id"], row["arm"], attempt, started),
            )
            return {"question_id": row["question_id"], "arm": row["arm"], "attempt": attempt}

    @staticmethod
    def _payload(payload):
        if payload is None:
            return None
        if not isinstance(payload, dict):
            raise ValueError("result payload must be a JSON object")
        return _canonical(payload)

    def _finish(self, question_id, arm, payload, *, error_type=None):
        encoded = self._payload(payload)
        label = payload.get("judge_label") if payload is not None else None
        if label is not None and not isinstance(label, bool):
            raise ValueError("judge_label must be true, false, or null")
        if error_type is not None and (
            not isinstance(error_type, str)
            or not 1 <= len(error_type) <= 128
            or not error_type.isidentifier()
        ):
            raise ValueError(
                "error_type must be a bounded class name, not a provider error message"
            )
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE question_id=? AND arm=?", (question_id, arm)
            ).fetchone()
            if row is None or row["status"] != "running":
                raise ValueError("only a claimed running task can be completed or failed")
            successful = error_type is None
            attempt_status = "completed" if successful else "failed"
            task_status = (
                "completed"
                if successful
                else "pending"
                if row["attempt"] < self.max_attempts
                else "failed"
            )
            finished = _now()
            connection.execute(
                """UPDATE attempts SET status=?,error_type=?,payload_json=?,finished_at=?
                   WHERE question_id=? AND arm=? AND attempt=?""",
                (attempt_status, error_type, encoded, finished, question_id, arm, row["attempt"]),
            )
            connection.execute(
                """UPDATE tasks SET status=?,judge_label=?,error_type=?,payload_json=?,updated_at=?
                   WHERE question_id=? AND arm=?""",
                (
                    task_status,
                    int(label) if successful and label is not None else None,
                    error_type,
                    encoded,
                    finished,
                    question_id,
                    arm,
                ),
            )

    def complete(self, question_id, arm, payload):
        if payload is None:
            raise ValueError("completed task requires a result payload")
        self._finish(question_id, arm, payload)

    def fail(self, question_id, arm, error_type, payload=None):
        if error_type is None:
            raise ValueError("failed task requires an error class")
        self._finish(question_id, arm, payload, error_type=error_type)

    def recover(self):
        """Recover interrupted attempts; caller must first stop the prior worker.

        Recovered tasks become pending. next_task then terminalizes any task
        whose abandoned attempts have exhausted the frozen attempt budget.
        """
        with self._transaction() as connection:
            recovered = connection.execute(
                "SELECT COUNT(*) FROM tasks WHERE status='running'"
            ).fetchone()[0]
            finished = _now()
            connection.execute(
                """UPDATE attempts SET status='abandoned',
                   error_type='AbandonedAttempt',finished_at=?
                   WHERE status='running'""",
                (finished,),
            )
            connection.execute(
                """UPDATE tasks SET status='pending',judge_label=NULL,
                   error_type='AbandonedAttempt',updated_at=?
                   WHERE status='running'""",
                (finished,),
            )
            return recovered

    def summary(self):
        """Full planned denominators, including unknown judgments and failures."""
        with self._lock:
            if self._closed:
                raise RuntimeError("run ledger is closed")
            rows = self._connection.execute("""SELECT arm, COUNT(*) AS planned,
                SUM(status='completed') AS completed, SUM(status='failed') AS failed,
                SUM(status='pending') AS pending, SUM(status='running') AS running,
                SUM(attempt>0) AS started, SUM(attempt) AS attempts,
                SUM(status='completed' AND judge_label IS NOT NULL) AS scored,
                SUM(status='completed' AND judge_label=1) AS correct
                FROM tasks GROUP BY arm""").fetchall()
            by_arm = {row["arm"]: dict(row) for row in rows}
            attempt_rows = self._connection.execute("""SELECT arm,
                SUM(status='abandoned') AS abandoned_attempts,
                SUM(error_type='IncompleteModelAccounting') AS incomplete_accounting_attempts
                FROM attempts GROUP BY arm""").fetchall()
            attempt_counts = {row["arm"]: row for row in attempt_rows}
            for arm, metrics in by_arm.items():
                metrics.pop("arm")
                counts = attempt_counts.get(arm)
                for name in ("abandoned_attempts", "incomplete_accounting_attempts"):
                    metrics[name] = (counts[name] or 0) if counts is not None else 0
                metrics["correct"] = metrics["correct"] or 0
                metrics["accuracy_all_planned"] = metrics["correct"] / metrics["planned"]
                metrics["unscored_completed"] = metrics["completed"] - metrics["scored"]
            totals = {
                name: sum(metrics[name] for metrics in by_arm.values())
                for name in (
                    "planned",
                    "completed",
                    "scored",
                    "correct",
                    "failed",
                    "pending",
                    "running",
                    "started",
                    "attempts",
                    "unscored_completed",
                    "abandoned_attempts",
                    "incomplete_accounting_attempts",
                )
            }
            totals["accuracy_all_planned"] = totals["correct"] / totals["planned"]
            return {
                **totals,
                "protocol_hash": self.protocol_hash,
                "max_attempts": self.max_attempts,
                "by_arm": [{"name": arm, **by_arm[arm]} for arm in self.arms],
            }

    def results(self, arm):
        """Yield final completed/failed task envelopes one row at a time.

        Each envelope has payload, the original JSON object, plus authoritative
        task state. Retry costs remain available in attempt_history.
        """
        if arm not in self.arms:
            raise ValueError("unknown experiment arm")
        last_ordinal = -1
        while True:
            with self._lock:
                if self._closed:
                    raise RuntimeError("run ledger is closed")
                row = self._connection.execute(
                    """SELECT * FROM tasks WHERE arm=? AND ordinal>?
                       AND status IN ('completed','failed')
                       ORDER BY ordinal LIMIT 1""",
                    (arm, last_ordinal),
                ).fetchone()
            if row is None:
                return
            last_ordinal = row["ordinal"]
            yield {
                "question_id": row["question_id"],
                "arm": row["arm"],
                "status": row["status"],
                "attempt": row["attempt"],
                "error_type": row["error_type"],
                "judge_label": bool(row["judge_label"]) if row["judge_label"] is not None else None,
                "payload": json.loads(row["payload_json"])
                if row["payload_json"] is not None
                else None,
            }

    def attempt_history(self, question_id, arm):
        with self._lock:
            if self._closed:
                raise RuntimeError("run ledger is closed")
            rows = self._connection.execute(
                "SELECT * FROM attempts WHERE question_id=? AND arm=? ORDER BY attempt",
                (question_id, arm),
            ).fetchall()
            return [
                {
                    **{key: row[key] for key in row.keys() if key != "payload_json"},
                    "payload": json.loads(row["payload_json"])
                    if row["payload_json"] is not None
                    else None,
                }
                for row in rows
            ]

    def __enter__(self):
        return self

    def close(self):
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def __exit__(self, *_args):
        self.close()
