"""Atomic durable source reception on SQLite and real PostgreSQL."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as admission_tests

from agent_memory.domain import ForgetMode, ForgetRequest, MemoryQuery
from agent_memory.operations.retention import DurableReceiver, RetentionError
from agent_memory.serialization import to_jsonable

store = admission_tests.store
CONFIG = "a" * 64


def source(scope, identity="event-one", **changes):
    return replace(
        admission_tests.source(
            scope, "Alice lives in Hangzhou", identity=identity, idempotency=identity
        ),
        **changes,
    )


async def ticket(receiver, event, request_id="request-one", **changes):
    return await receiver.issue_ticket(
        event, request_id=request_id, producer_id="host-one", configuration_sha256=CONFIG, **changes
    )


async def submit(receiver, event, issued, **changes):
    values = dict(ticket=issued, producer_id="host-one", configuration_sha256=CONFIG)
    return await receiver.submit(event, **{**values, **changes})


def test_acceptance_is_durable_idempotent_and_never_an_admitted_fact(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            event = source(scope, metadata={"claims": [{"key": "city", "value": "Hangzhou"}]})
            issued = await ticket(receiver, event)
            # Issuing a ticket stores no source text or processing request.
            async with engine.repository.unit_of_work() as uow:
                assert await uow.find_event_by_idempotency(scope, event.idempotency_key) is None
                assert await uow.retention_get(scope, "request", issued.request_id) is None
                assert event.content not in str(
                    await uow.retention_get(scope, "ticket", issued.request_id)
                )
            first = await submit(receiver, event, issued)
            assert first.status == "queued" and not first.duplicate
            assert to_jsonable(first)["schema"] == "durable-receive/1"
            # Another receiver represents a restarted host; retry after TTL must
            # recover the original committed receipt without recreating source.
            restarted = DurableReceiver(engine.repository, clock=lambda: clock[0])
            clock[0] += timedelta(hours=1)
            retry = await submit(restarted, replace(event, ingested_at=clock[0]), issued)
            assert replace(retry, duplicate=False) == first
            assert retry.duplicate
            assert await restarted.status(scope, issued.request_id) == first
            async with engine.repository.unit_of_work() as uow:
                saved = await uow.find_event_by_idempotency(scope, event.idempotency_key)
                assert saved.content == event.content and saved.id == event.id
                assert saved.metadata["_retention"]["original_event_type"] == event.event_type
                assert await uow.retention_count(scope, "request") == 1
            assert await kernel._repository.current_claims(scope) == ()
            result = await kernel.retrieve(MemoryQuery(scope, "Hangzhou", token_budget=2048))
            assert not result.current_state
            assert all(item.id != event.id for item in result.relevant_memories)

    asyncio.run(run())


@pytest.mark.parametrize("after_insert", [False, True])
def test_receive_transaction_rolls_back_source_and_request_together(
    store, monkeypatch, after_insert
):
    async def run():
        async with store() as (engine, _, scope, clock):
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            event = source(scope)
            issued = await ticket(receiver, event)
            unit_type = type(engine.repository.unit_of_work())
            original = unit_type.retention_insert

            async def crash(self, scope, kind, request_id, payload):
                if kind == "request":
                    if after_insert:
                        await original(self, scope, kind, request_id, payload)
                    raise asyncio.CancelledError("crash before transaction commit")
                return await original(self, scope, kind, request_id, payload)

            with monkeypatch.context() as patch:
                patch.setattr(unit_type, "retention_insert", crash)
                with pytest.raises(asyncio.CancelledError):
                    await submit(receiver, event, issued)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.find_event_by_idempotency(scope, event.idempotency_key) is None
                assert await uow.retention_count(scope, "request") == 0
            assert (await submit(receiver, event, issued)).status == "queued"

    asyncio.run(run())


@pytest.mark.parametrize("all_in_scope", [True, False])
@pytest.mark.parametrize("already_received", [True, False])
@pytest.mark.parametrize("mode", [ForgetMode.ERASE, ForgetMode.ARCHIVE])
def test_deletion_fences_tickets_and_pending_sources(store, all_in_scope, already_received, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            event = source(scope)
            issued = await ticket(receiver, event)
            if already_received:
                await submit(receiver, event, issued)
            await kernel.forget(
                ForgetRequest(
                    scope,
                    memory_ids=() if all_in_scope else (event.id,),
                    all_in_scope=all_in_scope,
                    mode=mode,
                )
            )
            with pytest.raises(RetentionError, match="ticket_revoked"):
                await submit(receiver, event, issued)
            with pytest.raises(RetentionError, match="ticket_revoked"):
                await ticket(receiver, event)
            # An old outbox cannot just use a new request identity for the same source.
            with pytest.raises(RetentionError, match="already_reserved"):
                await ticket(receiver, event, "new-request-for-old-source")
            status = await receiver.status(scope, issued.request_id)
            assert (status.status == "cancelled") if already_received else status is None
            fresh = source(scope, "new-authorized-event")
            new = await ticket(receiver, fresh, "new-authorized-request")
            assert new.epoch == issued.epoch + int(all_in_scope)
            assert (await submit(receiver, fresh, new)).status == "queued"

    asyncio.run(run())


def test_concurrent_retransmissions_commit_once_and_preserve_first_receipt(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            event = source(scope)
            issued = await asyncio.gather(*(ticket(receiver, event) for _ in range(4)))
            assert len(set(issued)) == 1
            results = await asyncio.gather(*(submit(receiver, event, issued[0]) for _ in range(4)))
            assert sum(not result.duplicate for result in results) == 1
            assert len({replace(result, duplicate=False) for result in results}) == 1

    asyncio.run(run())


def test_capacity_expiration_and_changed_input_cannot_leak_a_partial_source(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            receiver = DurableReceiver(
                engine.repository, max_pending=1, max_tickets=3, clock=lambda: clock[0]
            )
            one, two, three = (source(scope, name) for name in ("one", "two", "three"))
            issued = await ticket(receiver, one)
            for changed in (replace(one, content="different"), replace(one, actor="other")):
                with pytest.raises(RetentionError, match="input_conflict"):
                    await submit(receiver, changed, issued)
            with pytest.raises(RetentionError, match="input_conflict"):
                await submit(receiver, one, issued, configuration_sha256="b" * 64)
            await submit(receiver, one, issued)
            second = await ticket(receiver, two, "two")
            with pytest.raises(RetentionError, match="pending_capacity"):
                await submit(receiver, two, second)
            third = await ticket(receiver, three, "three")
            with pytest.raises(RetentionError, match="ticket_capacity"):
                await ticket(receiver, source(scope, "four"), "four")
            clock[0] += timedelta(minutes=10)
            with pytest.raises(RetentionError, match="ticket_expired"):
                await submit(receiver, three, third)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.find_event_by_idempotency(scope, "two") is None
                assert await uow.find_event_by_idempotency(scope, "three") is None

    asyncio.run(run())


def test_ticket_and_status_cannot_cross_scope_or_be_forged(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            event = source(scope)
            issued = await ticket(receiver, event)
            other = replace(scope, tenant_id="another-tenant")
            assert await receiver.status(other, issued.request_id) is None
            with pytest.raises(RetentionError, match="invalid_ticket"):
                await submit(receiver, replace(event, scope=other), issued)
            with pytest.raises(RetentionError, match="invalid_ticket"):
                await submit(receiver, event, replace(issued, token="forged"))
            with pytest.raises(RetentionError, match="independent_evidence"):
                await ticket(receiver, replace(event, event_type="agent.memory.context"))
            with pytest.raises(RetentionError, match="must_be_json"):
                await ticket(receiver, replace(event, metadata={"value": float("nan")}))
            with pytest.raises(RetentionError, match="reserved"):
                await ticket(
                    receiver,
                    replace(
                        event,
                        metadata={
                            "atom_extraction": {
                                "candidates": [],
                                "processing_state": "completed",
                            }
                        },
                    ),
                )

    asyncio.run(run())


def test_another_connection_recovers_committed_request_after_migration_replay(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            receiver = DurableReceiver(repo, clock=lambda: clock[0])
            event = source(scope)
            issued = await ticket(receiver, event)
            first = await submit(receiver, event, issued)
            other = (
                type(repo).from_dsn(repo.pool.conninfo)
                if hasattr(repo, "pool")
                else type(repo)(repo._path)
            )
            await other.initialize()
            try:
                recovered = DurableReceiver(other, clock=lambda: clock[0])
                assert await recovered.status(scope, issued.request_id) == first
                assert (await submit(recovered, event, issued)).duplicate
            finally:
                close = getattr(other, "close", None)
                if close is not None:
                    await close()

    asyncio.run(run())


def test_concurrent_capacity_and_erasure_keep_a_consistent_boundary(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            receiver = DurableReceiver(engine.repository, max_pending=1, clock=lambda: clock[0])
            events = (source(scope, "a"), source(scope, "b"))
            tickets = [await ticket(receiver, event, event.id) for event in events]
            results = await asyncio.gather(
                *(
                    submit(receiver, event, issued)
                    for event, issued in zip(events, tickets, strict=True)
                ),
                return_exceptions=True,
            )
            assert sum(isinstance(result, RetentionError) for result in results) == 1
            assert all(
                result.code == "pending_capacity"
                for result in results
                if isinstance(result, RetentionError)
            )
            # Race a committed retry with erase. Whichever obtains the transaction
            # lock first, the final source and its processing responsibility are gone.
            winner = next(
                i for i, result in enumerate(results) if not isinstance(result, Exception)
            )
            outcomes = await asyncio.gather(
                submit(receiver, events[winner], tickets[winner]),
                kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE)),
                return_exceptions=True,
            )
            assert not isinstance(outcomes[1], Exception)
            assert (await receiver.status(scope, tickets[winner].request_id)).status == "cancelled"
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.events_exist(scope, (events[winner].id,))

    asyncio.run(run())
