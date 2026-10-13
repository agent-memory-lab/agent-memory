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


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("limit", [1, 64])
def test_failed_cancellation_does_not_starve_other_tenants(backend, tmp_path, monkeypatch, limit):
    from agent_memory import ForgetMode, ForgetRequest
    from agent_memory.operations.consolidation_outbox import drain_consolidation_cancellations

    async def scenario():
        async with isolated_repository(backend, tmp_path) as (factory, storage):
            clock = [datetime(2026, 10, 12, tzinfo=UTC)]
            monkeypatch.setattr(storage, "utc_now", lambda: clock[0])
            repository = factory()
            await repository.initialize()
            attempts = []
            try:
                for name in ("bad", "good"):
                    event = MemoryEvent(MemoryScope(name), "event", "source", id=name)
                    async with repository.unit_of_work() as uow:
                        await uow.append_event(event)
                        await uow.save_consolidation_intent(
                            event, IngestResult(name, ("claim-" + name,), (), False, ())
                        )
                    await repository.forget(ForgetRequest(
                        event.scope, ("claim-" + name,), mode=ForgetMode.ERASE,
                    ))
                    clock[0] += timedelta(seconds=1)

                class Scheduler:
                    async def cancel_forget(self, request):
                        identity = request.memory_ids[0]
                        attempts.append(identity)
                        if identity == "bad":
                            raise RuntimeError("poison cancellation")

                result = await drain_consolidation_cancellations(
                    repository, Scheduler(), limit=limit,
                )
                assert result["failed"] == 1
                if limit == 1:
                    # Even when the failed marker is due again, untried work is
                    # older than its next eligible time and gets the next slot.
                    clock[0] += timedelta(seconds=2)
                    result = await drain_consolidation_cancellations(
                        repository, Scheduler(), limit=1,
                    )
                assert result["cancelled"] == 1
                assert attempts == ["bad", "good"]
                await close(repository)
                repository = factory()
                await repository.initialize()
                clock[0] += timedelta(seconds=2)
                remaining = await repository.pending_consolidation_cancellations()
                assert len(remaining) == 1 and remaining[0].event_id == "bad"
                assert remaining[0].attempts == 1
            finally:
                await close(repository)
    asyncio.run(scenario())


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_cancellation_failure_backoff_is_durable_and_generation_fenced(
    backend, tmp_path, monkeypatch,
):
    from agent_memory import ForgetMode, ForgetRequest

    async def scenario():
        async with isolated_repository(backend, tmp_path) as (factory, storage):
            clock = [datetime(2026, 10, 12, tzinfo=UTC)]
            monkeypatch.setattr(storage, "utc_now", lambda: clock[0])
            repository = factory()
            await repository.initialize()
            event = MemoryEvent(MemoryScope("scope"), "event", "source", id="source")
            try:
                async with repository.unit_of_work() as uow:
                    await uow.append_event(event)
                    await uow.save_consolidation_intent(
                        event, IngestResult(event.id, ("claim",), (), False, ()),
                    )
                await repository.forget(ForgetRequest(
                    event.scope, ("claim",), mode=ForgetMode.ERASE,
                ))
                original = (await repository.pending_consolidation_cancellations())[0]
                assert await repository.fail_consolidation_cancellation(original)
                # Concurrent reports of the same attempt cannot increase/delay it twice.
                assert not await repository.fail_consolidation_cancellation(original)
                await close(repository)
                repository = factory()
                await repository.initialize()
                assert await repository.pending_consolidation_cancellations() == ()
                clock[0] += timedelta(seconds=1)
                failed = (await repository.pending_consolidation_cancellations())[0]
                assert failed.attempts == 1
                newer = await repository.request_consolidation_cancellation(event.scope, event.id)
                assert newer.generation > failed.generation and newer.attempts == 0
                assert not await repository.fail_consolidation_cancellation(failed)
                assert not await repository.ack_consolidation_cancellation(failed)
                assert await repository.pending_consolidation_cancellations() == (newer,)
                assert await repository.ack_consolidation_cancellation(newer)
                assert await repository.pending_consolidation_cancellations() == ()
            finally:
                await close(repository)
    asyncio.run(scenario())
