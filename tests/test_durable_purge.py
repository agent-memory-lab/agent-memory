"""Offline deletion synchronization and producer competition on both real backends."""

import asyncio
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from hashlib import sha256

import pytest
import test_atom_admission as base

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.retention import DurableReceiver, RetentionError

store = base.store
CONFIG = "a" * 64


def envelope(scope, identity, clock):
    return LifecycleEvent(
        scope,
        identity,
        LifecycleEventType.MESSAGE_RECEIVED,
        LifecycleOrigin.USER,
        clock[0],
        "run",
        content="private-offline-marker " + identity,
        actor="alice",
    ).to_dict()


def source_id(scope, identity):
    return (
        "source:"
        + sha256(
            json.dumps([scope.partition_key(), identity], separators=(",", ":")).encode()
        ).hexdigest()
    )


async def client_for(engine, kernel, scope, clock, name="device", *, sync=True):
    sdk = pytest.importorskip("agent_memory_sdk")
    producer = DurableProducer(DurableReceiver(engine.repository, clock=lambda: clock[0]))
    session = await producer.open(
        scope,
        producer_id=name,
        actor="alice",
        configuration_sha256=CONFIG,
        sync_purges=sync,
    )
    api = DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER)
    client = sdk.EmbeddedMemoryClient(
        kernel, MCPRequestContext(scope, actor="alice"), durable_capture=api
    )
    return producer, session, client


@pytest.mark.parametrize("mode", [ForgetMode.ERASE, ForgetMode.ARCHIVE])
def test_offline_object_purge_precedes_body_delivery_and_retains_unrelated_input(
    store, tmp_path, mode
):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, client = await client_for(engine, kernel, scope, clock)
            path = tmp_path / "outbox.db"
            outbox = sdk.DurableOutbox(path, session, sync_purges=True)
            old, fresh = envelope(scope, "old", clock), envelope(scope, "fresh", clock)
            outbox.append(old)
            outbox.append(fresh)
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id(scope, "old"),), mode=mode)
            )
            delivered = []

            class Observe:
                def __getattr__(self, name):
                    return getattr(client, name)

                async def durable_append(self, event, *args):
                    delivered.append(event["event_id"])
                    return await client.durable_append(event, *args)

            result = await outbox.flush_one(Observe())
            assert delivered == ["fresh"] and result["sequence"] == 2
            # Purged sequence 1 remains a real gap, never an acknowledgment of reception.
            assert result["acked_through"] == 0 and result["received_after_gap"] == [2]
            with closing(sqlite3.connect(path)) as connection:
                assert {
                    r[0] for r in connection.execute("SELECT event_json FROM durable_pending")
                } == {"null"}
                assert (
                    connection.execute("SELECT purge_cursor FROM durable_sessions").fetchone()[0]
                    == 1
                )
            assert b"private-offline-marker" not in path.read_bytes()
            with pytest.raises(ValueError, match="purged"):
                outbox.append(old)
            _, new_session, _ = await client_for(engine, kernel, scope, clock, "new-device")
            with pytest.raises(ValueError, match="purged"):
                sdk.DurableOutbox(path, new_session, sync_purges=True).append(old)
            with pytest.raises(RetentionError, match="source_identity_erased"):
                await producer.append(
                    replace(base.source(scope, identity=source_id(scope, "old")), actor="alice"),
                    session,
                    sequence=1,
                    actor="alice",
                )

    asyncio.run(run())


def test_scope_purge_reconnect_erases_all_old_pending_before_attempting_send(store, tmp_path):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, client = await client_for(engine, kernel, scope, clock)
            path = tmp_path / "outbox.db"
            outbox = sdk.DurableOutbox(path, session, sync_purges=True)
            old = envelope(scope, "offline-never-submitted", clock)
            outbox.append(old)
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))

            class NeverSend:
                def __getattr__(self, name):
                    return getattr(client, name)

                async def durable_append(self, *args):
                    pytest.fail("revoked outbox must not dispatch a source body")

            with pytest.raises(ValueError, match="purged"):
                await sdk.DurableOutbox(path, session, sync_purges=True).flush_one(NeverSend())
            with closing(sqlite3.connect(path)) as connection:
                row = connection.execute(
                    "SELECT revoked,purge_cursor FROM durable_sessions"
                ).fetchone()
                assert row == (1, 1)
                assert (
                    connection.execute("SELECT event_json FROM durable_pending").fetchone()[0]
                    == "null"
                )
            with pytest.raises(RetentionError, match="revoked"):
                await producer.cursor(scope, session, actor="alice")
            _, fresh_session, fresh_client = await client_for(
                engine, kernel, scope, clock, "explicit-new-device"
            )
            fresh_outbox = sdk.DurableOutbox(path, fresh_session, sync_purges=True)
            with pytest.raises(ValueError, match="purged"):
                fresh_outbox.append(old)
            fresh_outbox.append(envelope(scope, "explicit-new-capture", clock))
            assert (await fresh_outbox.flush_one(fresh_client))["receipt"]["epoch"] == 1

    asyncio.run(run())


def test_lost_purge_ack_restarts_at_committed_local_cursor_without_source_replay(store, tmp_path):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client = await client_for(engine, kernel, scope, clock)
            path = tmp_path / "outbox.db"
            outbox = sdk.DurableOutbox(path, session, sync_purges=True)
            outbox.append(envelope(scope, "old", clock))
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id(scope, "old"),), mode=ForgetMode.ERASE)
            )

            class LostAck:
                def __getattr__(self, name):
                    return getattr(client, name)

                async def durable_purge_ack(self, *args, **kwargs):
                    await client.durable_purge_ack(*args, **kwargs)
                    raise ConnectionError("purge acknowledgment lost")

            with pytest.raises(ConnectionError):
                await outbox.flush_one(LostAck())
            restarted = sdk.DurableOutbox(path, session, sync_purges=True)
            assert await restarted.flush_one(client) is None
            response = await restarted.synchronize_purges(client)
            assert response["after"] == response["through"] == 1

    asyncio.run(run())


def test_paged_purge_has_no_gaps_is_scope_bound_and_never_exposes_bodies(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _ = await client_for(engine, kernel, scope, clock)
            ids = tuple("source:" + str(i) for i in range(270))
            await kernel.forget(ForgetRequest(scope, memory_ids=ids, mode=ForgetMode.ERASE))
            after, entries = 0, []
            while True:
                page = await producer.purge_sync(scope, session, actor="alice", after=after)
                entries.extend(page["entries"])
                after = page["through"]
                if page["closed"]:
                    break
            assert len(entries) == 270 and {e["source_event_id"] for e in entries} == set(ids)
            assert [e["cursor"] for e in entries] == list(range(1, 271))
            with pytest.raises(RetentionError, match="history_unavailable"):
                await producer.purge_sync(scope, session, actor="alice", after=271)
            with pytest.raises(RetentionError, match="invalid_producer"):
                await producer.purge_sync(replace(scope, tenant_id="other"), session, actor="alice")
            with pytest.raises(RetentionError, match="invalid_producer"):
                await producer.purge_sync(scope, replace(session, token="wrong"), actor="alice")
            with pytest.raises(RetentionError, match="invalid_purge_cursor"):
                await producer.purge_ack(scope, session, actor="alice", through=271)
            assert (await producer.purge_ack(scope, session, actor="alice", through=270))["closed"]
            assert (await producer.purge_ack(scope, session, actor="alice", through=10))[
                "purged_through"
            ] == 270

    asyncio.run(run())


def test_strict_producer_blocks_append_until_current_deletion_page_is_acknowledged(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _ = await client_for(engine, kernel, scope, clock)
            await kernel.forget(
                ForgetRequest(scope, memory_ids=("other-object",), mode=ForgetMode.ERASE)
            )
            event = replace(base.source(scope), actor="alice")
            with pytest.raises(RetentionError, match="purge_required"):
                await producer.append(event, session, sequence=1, actor="alice")
            page = await producer.purge_sync(scope, session, actor="alice")
            await producer.purge_ack(scope, session, actor="alice", through=page["through"])
            assert (await producer.append(event, session, sequence=1, actor="alice"))["receipt"][
                "status"
            ] == "queued"

    asyncio.run(run())


def test_concurrent_producers_keep_independent_order_and_duplicate_receipts(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            repo = engine.repository
            other = (
                type(repo).from_dsn(repo.pool.conninfo)
                if hasattr(repo, "pool")
                else type(repo)(repo._path)
            )
            await other.initialize()
            try:
                producers = [
                    DurableProducer(DurableReceiver(r, clock=lambda: clock[0]))
                    for r in (repo, other)
                ]
                sessions = await asyncio.gather(
                    *(
                        producers[i % 2].open(
                            scope,
                            producer_id=f"device-{i}",
                            actor="alice",
                            configuration_sha256=CONFIG,
                        )
                        for i in range(6)
                    )
                )
                events = {
                    (i, n): replace(base.source(scope, identity=f"source-{i}-{n}"), actor="alice")
                    for i in range(6)
                    for n in (1, 2, 3)
                }
                for n in (3, 1, 2):
                    await asyncio.gather(
                        *(
                            producers[i % 2].append(
                                events[i, n], sessions[i], sequence=n, actor="alice"
                            )
                            for i in range(6)
                        )
                    )
                for i, session in enumerate(sessions):
                    assert await producers[i % 2].cursor(scope, session, actor="alice") == {
                        "acked_through": 3,
                        "received_after_gap": [],
                    }
                repeats = await asyncio.gather(
                    *(
                        producers[i % 2].append(
                            events[i, 2], sessions[i], sequence=2, actor="alice"
                        )
                        for i in range(6)
                        for _ in range(2)
                    )
                )
                assert all(r["receipt"]["duplicate"] for r in repeats)
                async with repo.unit_of_work() as uow:
                    assert await uow.retention_count(scope, "request") == 18
            finally:
                if callable(getattr(other, "close", None)):
                    await other.close()

    asyncio.run(run())


def test_purge_journal_rolls_back_with_source_delete_failure(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _ = await client_for(engine, kernel, scope, clock)
            event = replace(base.source(scope), actor="alice")
            await producer.append(event, session, sequence=1, actor="alice")
            if hasattr(engine.repository, "pool"):
                import agent_memory_postgres.admission as owner

                name = "forget_records"
            else:
                owner, name = engine.repository, "_forget_admission_records"
            original = getattr(owner, name)
            if hasattr(engine.repository, "pool"):

                async def fail(*args):
                    await original(*args)
                    raise RuntimeError("delete failed")
            else:

                def fail(*args):
                    original(*args)
                    raise RuntimeError("delete failed")

            with monkeypatch.context() as patch:
                patch.setattr(owner, name, fail)
                with pytest.raises(RuntimeError, match="delete failed"):
                    await kernel.forget(
                        ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE)
                    )
            page = await producer.purge_sync(scope, session, actor="alice")
            assert page["head"] == 0 and not page["scope_revoked"]
            assert (await producer.cursor(scope, session, actor="alice"))["acked_through"] == 1

    asyncio.run(run())


def test_mcp_outbox_cleans_deletion_before_transport(store, tmp_path):
    sdk = pytest.importorskip("agent_memory_sdk")
    mcp = pytest.importorskip("agent_memory_mcp")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _ = await client_for(engine, kernel, scope, clock)
            outbox = sdk.DurableOutbox(tmp_path / "mcp.db", session, sync_purges=True)
            outbox.append(envelope(scope, "old", clock))
            outbox.append(envelope(scope, "new", clock))
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id(scope, "old"),), mode=ForgetMode.ERASE)
            )
            server = mcp.create_server(
                kernel,
                mcp.StaticIdentityResolver(MCPRequestContext(scope, actor="alice")),
                durable_capture=DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER),
            )
            async with sdk.MCPMemoryClient(server) as client:
                assert (await outbox.flush_one(client))["sequence"] == 2
                assert await outbox.flush_one(client) is None

    asyncio.run(run())


def test_local_purge_failure_rolls_back_body_tombstone_and_cursor(store, tmp_path, monkeypatch):
    sdk = pytest.importorskip("agent_memory_sdk")
    import agent_memory_sdk.durable_purge as owner

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client = await client_for(engine, kernel, scope, clock)
            path = tmp_path / "local.db"
            outbox = sdk.DurableOutbox(path, session, sync_purges=True)
            outbox.append(envelope(scope, "old", clock))
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id(scope, "old"),), mode=ForgetMode.ERASE)
            )
            original = owner.purge_rows

            def fail(*args):
                original(*args)
                raise RuntimeError("local cleanup failed")

            with monkeypatch.context() as patch:
                patch.setattr(owner, "purge_rows", fail)
                with pytest.raises(RuntimeError, match="cleanup failed"):
                    await outbox.flush_one(client)
            with closing(sqlite3.connect(path)) as connection:
                assert connection.execute(
                    "SELECT purge_cursor,scope_key FROM durable_sessions"
                ).fetchone() == (0, None)
                assert (
                    connection.execute("SELECT count(*) FROM durable_purged_ids").fetchone()[0] == 0
                )
                assert (
                    "private-offline-marker"
                    in connection.execute("SELECT event_json FROM durable_pending").fetchone()[0]
                )
            assert (
                await sdk.DurableOutbox(path, session, sync_purges=True).flush_one(client) is None
            )

    asyncio.run(run())


def test_purge_during_transport_does_not_accept_old_ack_or_restore_local_body(store, tmp_path):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client = await client_for(engine, kernel, scope, clock)
            path = tmp_path / "race.db"
            outbox = sdk.DurableOutbox(path, session, sync_purges=True)
            outbox.append(envelope(scope, "old", clock))

            class DeleteDuringDelivery:
                def __getattr__(self, name):
                    return getattr(client, name)

                async def durable_append(self, *args):
                    result = await client.durable_append(*args)
                    await kernel.forget(
                        ForgetRequest(
                            scope, memory_ids=(source_id(scope, "old"),), mode=ForgetMode.ERASE
                        )
                    )
                    await sdk.DurableOutbox(path, session, sync_purges=True).synchronize_purges(
                        client
                    )
                    return result

            with pytest.raises(ValueError, match="purged during delivery"):
                await outbox.flush_one(DeleteDuringDelivery())
            assert await outbox.flush_one(client) is None
            assert b"private-offline-marker" not in path.read_bytes()

    asyncio.run(run())


def test_missing_purge_journal_or_regressed_head_blocks_sync(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, _ = await client_for(engine, kernel, scope, clock)
            await kernel.forget(ForgetRequest(scope, memory_ids=("erased",), mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                owner = type(uow)

            async def missing(*args):
                return ()

            with monkeypatch.context() as patch:
                patch.setattr(owner, "purge_page", missing)
                with pytest.raises(RetentionError, match="history_unavailable"):
                    await producer.purge_sync(scope, session, actor="alice")
            await producer.purge_ack(scope, session, actor="alice", through=1)

            async def regressed(*args):
                return 0

            with monkeypatch.context() as patch:
                patch.setattr(owner, "purge_head", regressed)
                for call in (
                    producer.purge_sync(scope, session, actor="alice"),
                    producer.purge_ack(scope, session, actor="alice", through=0),
                    producer.append(
                        replace(base.source(scope), actor="alice"),
                        session,
                        sequence=1,
                        actor="alice",
                    ),
                ):
                    with pytest.raises(RetentionError, match="history_unavailable"):
                        await call

    asyncio.run(run())


@pytest.mark.parametrize("fault", ["gap", "scope", "binding", "ack"])
def test_malformed_control_response_never_dispatches_body(store, tmp_path, fault):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client = await client_for(engine, kernel, scope, clock)
            outbox = sdk.DurableOutbox(tmp_path / "invalid.db", session, sync_purges=True)
            outbox.append(envelope(scope, "pending", clock))
            await outbox.synchronize_purges(client)
            await kernel.forget(ForgetRequest(scope, memory_ids=("other",), mode=ForgetMode.ERASE))

            class Malformed:
                async def durable_purge_sync(self, *args, **kwargs):
                    page = await client.durable_purge_sync(*args, **kwargs)
                    if fault == "gap":
                        page["entries"][0]["cursor"] += 1
                    elif fault == "scope":
                        page["scope_key"] = "wrong-scope"
                    elif fault == "binding":
                        page["producer_id"] = "wrong-producer"
                    return page

                async def durable_purge_ack(self, *args, **kwargs):
                    ack = await client.durable_purge_ack(*args, **kwargs)
                    if fault == "ack":
                        ack["head"] = -1
                    return ack

                async def durable_append(self, *args):
                    pytest.fail("invalid cleanup response must block body transport")

            with pytest.raises(ValueError):
                await outbox.flush_one(Malformed())
            assert (await outbox.flush_one(client))["sequence"] == 1

    asyncio.run(run())


def test_new_epoch_sync_also_cleans_bound_old_sessions_in_same_scope(store, tmp_path):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, old_session, old_client = await client_for(engine, kernel, scope, clock)
            path = tmp_path / "shared.db"
            old = sdk.DurableOutbox(path, old_session, sync_purges=True)
            old.append(envelope(scope, "old", clock))
            await old.synchronize_purges(old_client)  # Bind this authenticated local scope.
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            _, fresh_session, fresh_client = await client_for(engine, kernel, scope, clock, "new")
            fresh = sdk.DurableOutbox(path, fresh_session, sync_purges=True)
            fresh.append(envelope(scope, "fresh", clock))
            assert (await fresh.flush_one(fresh_client))["sequence"] == 1
            with pytest.raises(ValueError, match="purged"):
                old.append(envelope(scope, "old-again", clock))
            with closing(sqlite3.connect(path)) as connection:
                assert connection.execute(
                    "SELECT event_json FROM durable_pending WHERE event_id='old'"
                ).fetchone() == ("null",)

    asyncio.run(run())
