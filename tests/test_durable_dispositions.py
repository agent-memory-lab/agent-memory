"""Explicit producer sequence cancellation and body-free offline recovery."""

import asyncio
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
import test_atom_admission as base
from test_durable_purge import envelope, source_id

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.lifecycle import LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.retention import DurableReceiver, RetentionError

store = base.store


async def service(engine, kernel, scope, clock, *, enabled=True, max_gap=2):
    sdk = pytest.importorskip("agent_memory_sdk")
    producer = DurableProducer(
        DurableReceiver(engine.repository, clock=lambda: clock[0]), max_gap=max_gap
    )
    session = await producer.open(
        scope,
        producer_id="device",
        actor="alice",
        configuration_sha256="a" * 64,
        sync_purges=True,
        sequence_dispositions=enabled,
    )
    api = DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER)
    client = sdk.EmbeddedMemoryClient(
        kernel, MCPRequestContext(scope, actor="alice"), durable_capture=api
    )
    return producer, session, client, api


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_cancelled_hole_allows_bounded_stream_to_continue_without_fake_reception(
    store, tmp_path, transport
):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, embedded, api = await service(engine, kernel, scope, clock)
            path = tmp_path / "pending.db"
            outbox = sdk.DurableOutbox(path, session, sync_purges=True, settle_purges=True)
            outbox.append(envelope(scope, "old", clock))
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id(scope, "old"),), mode=ForgetMode.ERASE)
            )

            async def exercise(client):
                contracts = await client.durable_contracts(session)
                assert contracts["cursor_schema"] == "producer-disposition/1"
                assert contracts["sequence_dispositions"] and contracts["staged_readiness"]
                for i in range(2, 9):
                    outbox.append(envelope(scope, f"fresh-{i}", clock))
                    response = await outbox.flush_one(client)
                    assert "acked_through" not in response
                    assert response["cursor"]["settled_through"] == i
                    assert response["cursor"]["received_through"] == 0
                cursor = await client.durable_cursor(session)
                assert cursor["received_count"] == 7 and cursor["cancelled_count"] == 1
                assert cursor["settled_after_gap"] == []
                with closing(sqlite3.connect(path)) as connection:
                    assert connection.execute(
                        "SELECT purged,cancel_confirmed,event_json FROM durable_pending "
                        "WHERE sequence=1"
                    ).fetchone() == (1, 1, "null")
                async with engine.repository.unit_of_work() as uow:
                    assert await uow.retention_count(scope, "request") == 7
                    assert await uow.get_source_event(scope, source_id(scope, "old")) is None

            if transport == "embedded":
                await exercise(embedded)
            else:
                mcp = pytest.importorskip("agent_memory_mcp")
                server = mcp.create_server(
                    kernel,
                    mcp.StaticIdentityResolver(MCPRequestContext(scope, actor="alice")),
                    durable_capture=api,
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())


def test_cancel_is_proven_identity_bound_idempotent_and_never_reclassifies_received(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _, _ = await service(engine, kernel, scope, clock)
            received = replace(base.source(scope), actor="alice")
            await producer.append(received, session, sequence=2, actor="alice")
            with pytest.raises(RetentionError, match="unproven"):
                await producer.cancel_sequence(
                    scope, session, sequence=1, source_event_id="never-erased", actor="alice"
                )
            await kernel.forget(
                ForgetRequest(
                    scope, memory_ids=("offline-id", received.id, "other-id"), mode=ForgetMode.ERASE
                )
            )
            with pytest.raises(RetentionError, match="purge_required"):
                await producer.cancel_sequence(
                    scope, session, sequence=1, source_event_id="offline-id", actor="alice"
                )
            page = await producer.purge_sync(scope, session, actor="alice")
            await producer.purge_ack(scope, session, through=page["through"], actor="alice")
            cancel = await producer.cancel_sequence(
                scope, session, sequence=1, source_event_id="offline-id", actor="alice"
            )
            assert cancel["disposition"] == "cancelled" and cancel["cursor"]["settled_through"] == 2
            assert cancel["cursor"]["received_through"] == 0
            assert (
                await producer.cancel_sequence(
                    scope, session, sequence=1, source_event_id="offline-id", actor="alice"
                )
                == cancel
            )
            already = await producer.cancel_sequence(
                scope, session, sequence=2, source_event_id=received.id, actor="alice"
            )
            assert already["disposition"] == "received" and already["cursor"]["received_count"] == 1
            assert already["cursor"]["cancelled_count"] == 1
            with pytest.raises(RetentionError, match="input_conflict"):
                await producer.cancel_sequence(
                    scope, session, sequence=1, source_event_id="other-id", actor="alice"
                )
            with pytest.raises(RetentionError, match="sequence_cancelled"):
                await producer.append(
                    replace(base.source(scope), actor="alice"), session, sequence=1, actor="alice"
                )
            with pytest.raises(RetentionError, match="invalid_producer"):
                await producer.cancel_sequence(
                    scope,
                    replace(session, token="wrong"),
                    sequence=3,
                    source_event_id="offline-id",
                    actor="alice",
                )
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            with pytest.raises(RetentionError, match="revoked"):
                await producer.cancel_sequence(
                    scope, session, sequence=3, source_event_id="offline-id", actor="alice"
                )

    asyncio.run(run())


def test_lost_cancel_ack_restarts_without_resending_erased_body(store, tmp_path):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _ = await service(engine, kernel, scope, clock)
            path = tmp_path / "pending.db"
            outbox = sdk.DurableOutbox(path, session, sync_purges=True, settle_purges=True)
            outbox.append(envelope(scope, "old", clock))
            outbox.append(envelope(scope, "new", clock))
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id(scope, "old"),), mode=ForgetMode.ERASE)
            )

            class LoseAck:
                def __getattr__(self, name):
                    return getattr(client, name)

                async def durable_cancel_sequence(self, *args):
                    await client.durable_cancel_sequence(*args)
                    raise ConnectionError("cancel acknowledgment lost")

            with pytest.raises(ConnectionError):
                await outbox.flush_one(LoseAck())
            with closing(sqlite3.connect(path)) as connection:
                assert connection.execute(
                    "SELECT cancel_confirmed,event_json FROM durable_pending WHERE sequence=1"
                ).fetchone() == (0, "null")
            restarted = sdk.DurableOutbox(path, session, sync_purges=True, settle_purges=True)
            assert (await restarted.flush_one(client))["cursor"]["settled_through"] == 2
            cursor = await client.durable_cursor(session)
            assert cursor["received_count"] == cursor["cancelled_count"] == 1

    asyncio.run(run())


def test_cancellation_and_disposition_cursor_share_one_transaction(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _, _ = await service(engine, kernel, scope, clock)
            await kernel.forget(ForgetRequest(scope, memory_ids=("old",), mode=ForgetMode.ERASE))
            await producer.purge_ack(scope, session, through=1, actor="alice")
            cls = type(engine.repository.unit_of_work())
            original = cls.producer_put

            async def fail(*args):
                await original(*args)
                raise RuntimeError("cursor commit failed")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "producer_put", fail)
                with pytest.raises(RuntimeError, match="commit failed"):
                    await producer.cancel_sequence(
                        scope, session, sequence=1, source_event_id="old", actor="alice"
                    )
            assert (await producer.cursor(scope, session, actor="alice"))["settled_through"] == 0
            async with engine.repository.unit_of_work() as uow:
                assert await uow.delivery_count(scope, "sequence") == 0
            assert (
                await producer.cancel_sequence(
                    scope, session, sequence=1, source_event_id="old", actor="alice"
                )
            )["cursor"]["cancelled_count"] == 1

    asyncio.run(run())


def test_legacy_mode_does_not_change_cursor_contract_or_allow_cancellation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _, _ = await service(engine, kernel, scope, clock, enabled=False)
            assert await producer.cursor(scope, session, actor="alice") == {
                "acked_through": 0,
                "received_after_gap": [],
            }
            with pytest.raises(RetentionError, match="unsupported"):
                await producer.cancel_sequence(
                    scope, session, sequence=1, source_event_id="old", actor="alice"
                )
            with pytest.raises(RetentionError, match="configuration_conflict"):
                await producer.open(
                    scope,
                    producer_id="device",
                    actor="alice",
                    configuration_sha256="a" * 64,
                    sync_purges=True,
                    sequence_dispositions=True,
                )

    asyncio.run(run())


def test_opt_in_receive_and_disposition_ledger_roll_back_together(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _, _ = await service(engine, kernel, scope, clock)
            cls = type(engine.repository.unit_of_work())
            original = cls.delivery_insert

            async def fail(*args):
                await original(*args)
                raise RuntimeError("delivery ledger failed")

            event = replace(base.source(scope), actor="alice")
            with monkeypatch.context() as patch:
                patch.setattr(cls, "delivery_insert", fail)
                with pytest.raises(RuntimeError, match="ledger failed"):
                    await producer.append(event, session, sequence=1, actor="alice")
            async with engine.repository.unit_of_work() as uow:
                assert await uow.delivery_count(scope, "sequence") == 0
                assert await uow.retention_count(scope, "request") == 0
                assert await uow.get_source_event(scope, event.id) is None
            assert (await producer.cursor(scope, session, actor="alice"))["received_count"] == 0
            result = await producer.append(event, session, sequence=1, actor="alice")
            assert result["cursor"]["received_through"] == result["cursor"]["settled_through"] == 1

    asyncio.run(run())


def test_cancellation_and_receive_compete_without_lost_dispositions(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _, _ = await service(engine, kernel, scope, clock)
            await kernel.forget(
                ForgetRequest(scope, memory_ids=("offline",), mode=ForgetMode.ERASE)
            )
            await producer.purge_ack(scope, session, through=1, actor="alice")
            results = await asyncio.gather(
                producer.cancel_sequence(
                    scope, session, sequence=1, source_event_id="offline", actor="alice"
                ),
                producer.append(
                    replace(base.source(scope), actor="alice"), session, sequence=2, actor="alice"
                ),
            )
            assert len(results) == 2
            cursor = await producer.cursor(scope, session, actor="alice")
            assert cursor["settled_through"] == 2 and cursor["received_through"] == 0
            assert cursor["received_count"] == cursor["cancelled_count"] == 1

    asyncio.run(run())


def test_outbox_rejects_missing_negotiation_and_malformed_cancel_ack_before_delivery(
    store, tmp_path
):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _ = await service(engine, kernel, scope, clock)
            path = tmp_path / "pending.db"
            outbox = sdk.DurableOutbox(path, session, sync_purges=True, settle_purges=True)
            outbox.append(envelope(scope, "old", clock))
            outbox.append(envelope(scope, "fresh", clock))
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id(scope, "old"),), mode=ForgetMode.ERASE)
            )

            class Invalid:
                negotiation = True

                def __getattr__(self, name):
                    return getattr(client, name)

                async def durable_contracts(self, *args):
                    result = await client.durable_contracts(*args)
                    result["sequence_dispositions"] = self.negotiation
                    return result

                async def durable_cancel_sequence(self, *args):
                    result = await client.durable_cancel_sequence(*args)
                    result["sequence"] += 1
                    return result

                async def durable_append(self, *args):
                    pytest.fail("no source transport after malformed control acknowledgment")

            invalid = Invalid()
            invalid.negotiation = False
            with pytest.raises(ValueError, match="not negotiated"):
                await outbox.flush_one(invalid)
            invalid.negotiation = True
            with pytest.raises(ValueError, match="cancellation acknowledgment"):
                await outbox.flush_one(invalid)
            restarted = sdk.DurableOutbox(path, session, sync_purges=True, settle_purges=True)
            assert (await restarted.flush_one(client))["cursor"]["settled_through"] == 2

    asyncio.run(run())
