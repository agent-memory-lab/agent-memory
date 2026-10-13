"""Forget cancellation SQL contract and opt-in tests against an existing test DB."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import pytest
from agent_memory_postgres.jobs import (
    ConsolidationJob,
    ConsolidationWorker,
    JobStatus,
    PostgresConsolidationQueue,
)
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from test_live_contract import _test_dsn

from agent_memory import ForgetRequest, MemoryScope


class Cursor:
    def __init__(self, rowcount=0, row=None):
        self.rowcount = rowcount
        self.row = row

    async def fetchone(self):
        return self.row


class RecordingPool:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    @asynccontextmanager
    async def connection(self):
        yield self

    async def execute(self, query, parameters):
        self.calls.append((" ".join(query.split()), parameters))
        return self.results.pop(0)


SCOPE = MemoryScope("tenant", namespace="notes", user_id="user", session_id="session")


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_cancel_sql_binds_exact_scope_filters_active_jobs_and_retains_identity(all_in_scope):
    async def scenario():
        pool = RecordingPool(Cursor(2))
        queue = PostgresConsolidationQueue(pool)
        # IDs containing SQL syntax remain data, never part of the SQL statement.
        ids = ("source'); DROP TABLE anything; --", "claim")
        request = ForgetRequest(SCOPE, ids, all_in_scope)
        assert await queue.cancel_forget(request) == 2
        query, parameters = pool.calls[0]
        assert "partition_key = %s" in query
        assert "status IN ('pending', 'running')" in query
        assert "status = 'dead'" in query
        assert "leased_by = NULL, lease_expires_at = NULL" in query
        assert "payload_json = '{}'::jsonb" in query
        assert "last_error = 'cancelled_by_forget'" in query
        assignment = query.split(" SET ", 1)[1].split(" WHERE ", 1)[0]
        assert "job_key =" not in assignment and "id =" not in assignment
        assert ids[0] not in query
        if all_in_scope:
            assert parameters == (SCOPE.partition_key(),)
            assert "jsonb_array_elements_text" not in query
        else:
            assert parameters == (SCOPE.partition_key(), list(ids), list(ids))
            assert "payload_json->>'event_id' = ANY(%s)" in query
            assert "jsonb_typeof(payload_json->'claim_ids') = 'array'" in query
            assert "ELSE '[]'::jsonb END" in query
            assert "ids.value = ANY(%s)" in query

    asyncio.run(scenario())


def job():
    return ConsolidationJob(
        "job",
        "event:source:consolidate",
        SCOPE,
        "memory.consolidate",
        {"event_id": "source", "claim_ids": ["claim"]},
        JobStatus.RUNNING,
        1,
        5,
        datetime.now(UTC),
        leased_by="worker",
    )


@pytest.mark.parametrize("operation", ["complete", "fail"])
@pytest.mark.parametrize("forgotten", [False, True])
def test_only_explicit_forget_cancellation_is_a_terminal_callback_noop(operation, forgotten):
    async def scenario():
        pool = RecordingPool(Cursor(0), Cursor(row={"exists": 1} if forgotten else None))
        queue = PostgresConsolidationQueue(pool)
        args = (job(), "worker")
        if operation == "fail":
            args += (RuntimeError("handler failed"),)
        if forgotten:
            await getattr(queue, operation)(*args)
        else:
            with pytest.raises(RuntimeError, match="job lease was lost"):
                await getattr(queue, operation)(*args)
        assert len(pool.calls) == 2
        query, parameters = pool.calls[-1]
        assert query.startswith("SELECT 1 FROM agent_memory_consolidation_jobs")
        assert "status = 'dead' AND last_error = 'cancelled_by_forget'" in query
        assert parameters == ("job",)
        # The no-op never issues an UPDATE that could resurrect or complete the job.
        assert pool.results == []

    asyncio.run(scenario())


@pytest.mark.parametrize("handler_fails", [False, True])
def test_worker_survives_job_cancelled_while_its_handler_was_running(handler_fails):
    async def scenario():
        pool = RecordingPool(Cursor(0), Cursor(row={"exists": 1}))
        queue = PostgresConsolidationQueue(pool)

        async def claim(worker_id, *, lease_seconds):
            return job()

        async def handler(current):
            assert current.id == "job"
            if handler_fails:
                raise ValueError("already forgotten")

        queue.claim = claim
        worker = ConsolidationWorker(queue, {"memory.consolidate": handler}, worker_id="worker")
        assert await worker.run_once()
        assert len(pool.calls) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("target", ["event", "claim", "all"])
def test_live_cancel_is_exact_idempotent_scrubs_payload_and_prevents_resurrection(target):
    async def scenario():
        dsn = _test_dsn()
        schema = "job_forget_" + uuid4().hex
        async with await AsyncConnection.connect(dsn, autocommit=True) as admin:
            await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            parsed = urlsplit(dsn)
            query = dict(parse_qsl(parsed.query))
            query["options"] = "-csearch_path=" + schema
            scoped_dsn = urlunsplit(parsed._replace(query=urlencode(query)))
            pool = AsyncConnectionPool(
                scoped_dsn,
                min_size=1,
                max_size=1,
                open=False,
                kwargs={"row_factory": dict_row},
            )
            await pool.open()
            try:
                async with pool.connection() as connection:
                    migration = Path(__file__).parents[1] / "migrations" / "001_core.sql"
                    await connection.execute(migration.read_text(encoding="utf-8"), prepare=False)
                queue = PostgresConsolidationQueue(pool)
                payload = {"event_id": "source", "claim_ids": ["claim"], "private": "secret"}
                matched = await queue.enqueue(
                    SCOPE, job_key="matched", job_type="test", payload=payload
                )
                sibling = await queue.enqueue(
                    replace(SCOPE, session_id="sibling"),
                    job_key="sibling",
                    job_type="test",
                    payload=payload,
                )
                unrelated = await queue.enqueue(
                    SCOPE,
                    job_key="unrelated",
                    job_type="test",
                    payload={"event_id": "other", "claim_ids": None, "private": "secret"},
                )
                terminal = await queue.enqueue(
                    SCOPE,
                    job_key="terminal",
                    job_type="test",
                    payload=payload,
                )
                async with pool.connection() as connection:
                    await connection.execute(
                        "UPDATE agent_memory_consolidation_jobs SET status = 'running', "
                        "leased_by = 'worker', lease_expires_at = now() + interval '1 minute' "
                        "WHERE id = %s",
                        (matched,),
                    )
                    await connection.execute(
                        "UPDATE agent_memory_consolidation_jobs SET status = 'completed' "
                        "WHERE id = %s",
                        (terminal,),
                    )
                request = ForgetRequest(
                    SCOPE,
                    () if target == "all" else ("source" if target == "event" else "claim",),
                    all_in_scope=target == "all",
                )
                assert await queue.cancel_forget(request) == (2 if target == "all" else 1)
                assert await queue.cancel_forget(request) == 0
                assert (
                    await queue.enqueue(
                        SCOPE,
                        job_key="matched",
                        job_type="test",
                        payload=payload,
                    )
                    == matched
                )
                stale = replace(job(), id=matched, job_key="matched", job_type="test")
                await queue.complete(stale, "worker")
                await queue.fail(stale, "worker", RuntimeError("late handler result"))
                async with pool.connection() as connection:
                    cursor = await connection.execute(
                        "SELECT * FROM agent_memory_consolidation_jobs ORDER BY job_key"
                    )
                    rows = {row["id"]: row for row in await cursor.fetchall()}
                cancelled = rows[matched]
                assert cancelled["status"] == "dead"
                assert cancelled["payload_json"] == {}
                assert cancelled["job_key"] == "matched"
                assert cancelled["leased_by"] is None and cancelled["lease_expires_at"] is None
                assert cancelled["last_error"] == "cancelled_by_forget"
                assert "secret" not in json.dumps(cancelled["payload_json"])
                assert (
                    rows[sibling]["status"] == "pending"
                    and rows[sibling]["payload_json"] == payload
                )
                assert (
                    rows[terminal]["status"] == "completed"
                    and rows[terminal]["payload_json"] == payload
                )
                assert rows[unrelated]["status"] == ("dead" if target == "all" else "pending")
            finally:
                await pool.close()
                await admin.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    asyncio.run(scenario())
