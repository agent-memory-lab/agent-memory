"""A poison intent must not starve other tenants, including in long batches."""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import pytest

from agent_memory import MemoryEvent, MemoryScope
from agent_memory.domain import IngestResult
from agent_memory.operations.consolidation_outbox import (
    drain_consolidation_outbox,
    retry_delay_seconds,
)
from agent_memory.sqlite import SQLiteMemoryRepository


@asynccontextmanager
async def isolated_repository(backend, tmp_path):
    if backend == "sqlite":
        from agent_memory.operations import sqlite_consolidation_outbox as storage

        def factory():
            return SQLiteMemoryRepository(tmp_path / "outbox.db")
        yield factory, storage
    else:
        from agent_memory_postgres import build_postgres_kernel
        from agent_memory_postgres import consolidation_outbox as storage
        from psycopg import AsyncConnection, sql

        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN")
        if not dsn:
            pytest.skip("set AGENT_MEMORY_TEST_POSTGRES_DSN for live PostgreSQL")
        schema = "outbox_retry_" + uuid4().hex
        async with await AsyncConnection.connect(dsn, autocommit=True) as admin:
            await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            parsed = urlsplit(dsn)
            query = dict(parse_qsl(parsed.query))
            query["options"] = "-csearch_path=" + schema
            target = urlunsplit(parsed._replace(query=urlencode(query)))
            try:
                yield lambda: build_postgres_kernel(
                    target, min_pool_size=1, max_pool_size=1
                )._repository, storage
            finally:
                await admin.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )


async def close(repository):
    if hasattr(repository, "close"):
        await repository.close()


async def add(repository, name):
    event = MemoryEvent(MemoryScope(name), "event", "source", id=name)
    async with repository.unit_of_work() as uow:
        await uow.append_event(event)
        await uow.save_consolidation_intent(event, IngestResult(event.id, (), (), False, ()))
    return event


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("limit", [1, 64])
def test_bad_intent_does_not_block_other_tenants_or_repeat_in_long_batch(
    backend, tmp_path, monkeypatch, limit,
):
    async def scenario():
        async with isolated_repository(backend, tmp_path) as (factory, storage):
            clock = [datetime(2026, 10, 12, tzinfo=UTC)]
            monkeypatch.setattr(storage, "utc_now", lambda: clock[0])
            repository = factory()
            try:
                await repository.initialize()
                await add(repository, "bad")
                clock[0] += timedelta(seconds=1)
                await add(repository, "good")
                attempts = []

                class Scheduler:
                    async def enqueue_event(self, event, result):
                        attempts.append(event.id)
                        if event.id == "bad":
                            raise RuntimeError("poison")
                        # Exceeds the backoff but stays inside the delivery lease.
                        clock[0] += timedelta(seconds=10)

                result = await drain_consolidation_outbox(repository, Scheduler(), limit=limit)
                if limit == 1:
                    assert result == dict(claimed=1, delivered=0, failed=1, skipped=0)
                    # Even after the poison task is due again, older untried work
                    # gets the next batch's slot instead of starving indefinitely.
                    clock[0] += timedelta(seconds=2)
                    result = await drain_consolidation_outbox(repository, Scheduler(), limit=1)
                    assert result == dict(claimed=1, delivered=1, failed=0, skipped=0)
                else:
                    assert result == dict(claimed=2, delivered=1, failed=1, skipped=0)
                assert attempts == ["bad", "good"]
                assert (await repository.consolidation_outbox_status())["pending"] == 1
            finally:
                await close(repository)

    asyncio.run(scenario())


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_backoff_survives_restart_and_manual_retry_remains_fenced(backend, tmp_path, monkeypatch):
    async def scenario():
        async with isolated_repository(backend, tmp_path) as (factory, storage):
            clock = [datetime(2026, 10, 12, tzinfo=UTC)]
            monkeypatch.setattr(storage, "utc_now", lambda: clock[0])
            repository = factory()
            await repository.initialize()
            source = await add(repository, "bad")
            first = await repository.claim_consolidation_intent()
            assert first.attempts == 1
            assert await repository.fail_consolidation_intent(first, "RuntimeError")
            await close(repository)
            repository = factory()
            try:
                await repository.initialize()
                assert await repository.claim_consolidation_intent() is None
                assert await repository.claim_consolidation_intent(scope=source.scope) is None
                # A caller choosing this exact event can retry immediately.
                second = await repository.claim_consolidation_intent(event_id=source.id)
                assert second.attempts == 2 and first.token != second.token
                assert not await repository.fail_consolidation_intent(first, "stale")
                assert not await repository.complete_consolidation_intent(first)
                assert await repository.fail_consolidation_intent(second, "RuntimeError")
                clock[0] += timedelta(seconds=1)
                assert await repository.claim_consolidation_intent() is None
                clock[0] += timedelta(seconds=1)
                third = await repository.claim_consolidation_intent()
                assert third.attempts == 3
                assert await repository.complete_consolidation_intent(third)
            finally:
                await close(repository)

    asyncio.run(scenario())


def test_backoff_grows_to_bounded_five_minutes():
    assert [retry_delay_seconds(n) for n in (1, 2, 3, 9, 10, 1000000)] == [1, 2, 4, 256, 300, 300]
