import asyncio
import sqlite3
from contextlib import closing

import pytest
import test_atom_admission as base
from test_reprocessing import Adapter, processing, seed

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.retention import DurableReceiver, RetentionError

store = base.store


def test_revision_invalidates_current_support_and_preserves_history(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            old, receiver, service, _ = await seed(engine, scope, clock)
            clock[0] = base.at(10)
            new = base.source(scope, "Alice lives in Shanghai", day=10)
            configuration, queue, _, runner = processing(
                engine,
                scope,
                clock,
                Adapter(("Shanghai",), supported=("Shanghai",), version="rev2"),
            )
            options = dict(
                base_event_id=old.id,
                expected_revision=1,
                request_id="revision",
                producer_id="host",
                configuration_sha256=configuration,
            )
            receipt = await receiver.revise(new, **options)
            assert receipt.status == "queued"
            assert not (await engine.state(scope, valid_at=clock[0], known_at=clock[0]))[0]
            assert (await engine.state(scope, valid_at=base.at(1), known_at=base.at(9)))[0][
                0
            ].value == "Hangzhou"
            assert (await receiver.revise(new, **options)).duplicate
            with pytest.raises(RetentionError, match="document_head_changed"):
                await receiver.revise(base.source(scope), **{**options, "request_id": "stale"})
            with pytest.raises(RetentionError, match="source_revision_changed"):
                await service.snapshot(scope, old.id)
            # Revision withdrawal and extraction publish are separate system-time commits.
            clock[0] = base.at(11)
            assert await runner.run_once()
            assert (await queue.status("revision"))["status"] == "completed"
            assert (await engine.state(scope, valid_at=clock[0], known_at=clock[0]))[0][
                0
            ].value == "Shanghai"
            async with engine.repository.unit_of_work() as uow:
                first = await uow.get_source_event(scope, old.id)
                second = await uow.get_source_event(scope, new.id)
                assert first.content == old.content
                assert second.metadata["_retention"]["parent_event_id"] == old.id
                assert second.metadata["_retention"]["document_id"] == old.id
                rows = await uow.list_admission_records(scope)
                assert {r["payload"]["source_family"] for r in rows} == {old.id}

    asyncio.run(run())


def test_revision_cas_failure_rolls_back_source_withdrawal_and_request(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            old, receiver, service, head = await seed(engine, scope, clock)
            configuration, _, _, _ = processing(engine, scope, clock, Adapter())
            new = base.source(scope)
            cls = type(engine.repository.unit_of_work())
            original = cls.retention_head_put

            async def fail_after_cas(self, *args):
                result = await original(self, *args)
                if args[1] == "document":
                    raise RuntimeError("after document CAS")
                return result

            with monkeypatch.context() as patch:
                patch.setattr(cls, "retention_head_put", fail_after_cas)
                with pytest.raises(RuntimeError, match="document CAS"):
                    await receiver.revise(
                        new,
                        base_event_id=old.id,
                        expected_revision=1,
                        request_id="rev",
                        producer_id="host",
                        configuration_sha256=configuration,
                    )
            assert await service.snapshot(scope, old.id) == head
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, new.id) is None
                assert await uow.retention_get(scope, "request", "rev") is None
            assert (await engine.state(scope, valid_at=clock[0], known_at=clock[0]))[0]

    asyncio.run(run())


def test_revision_of_pending_source_fences_old_worker_and_erasure_fences_revision(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0], max_pending=1)
            configuration, queue, handler, runner = processing(engine, scope, clock, Adapter())
            old = base.source(scope)
            ticket = await receiver.issue_ticket(
                old, request_id="a-old", producer_id="host", configuration_sha256=configuration
            )
            await receiver.submit(
                old, ticket=ticket, producer_id="host", configuration_sha256=configuration
            )
            lease = await queue.claim("old-worker", lease_seconds=60)
            new = base.source(scope)
            await receiver.revise(
                new,
                base_event_id=old.id,
                expected_revision=1,
                request_id="b-revised",
                producer_id="host",
                configuration_sha256=configuration,
            )
            assert (await queue.status("a-old"))["status"] == "superseded"
            with pytest.raises(Exception, match="lease"):
                await handler(lease.task, lambda value: queue.checkpoint(lease, value))
            assert await runner.run_once()
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            with pytest.raises(RetentionError, match="source_unavailable"):
                await receiver.revise(
                    base.source(scope),
                    base_event_id=new.id,
                    expected_revision=2,
                    request_id="after",
                    producer_id="host",
                    configuration_sha256=configuration,
                )

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_revision_outbox_replays_lost_confirmation_on_shared_transport(store, tmp_path, transport):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            producer = DurableProducer(receiver)
            session = await producer.open(
                scope, producer_id="host", actor="alice", configuration_sha256="a" * 64
            )
            api = DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER)
            context = MCPRequestContext(scope, actor="alice")
            one = LifecycleEvent(
                scope=scope,
                event_id="v1",
                event_type=LifecycleEventType.MESSAGE_RECEIVED,
                origin=LifecycleOrigin.USER,
                occurred_at=clock[0],
                run_id="r",
                content="I live in Hangzhou",
            ).to_dict()
            two = {**one, "event_id": "v2", "content": "I live in Shanghai"}
            outbox = sdk.DurableOutbox(tmp_path / "pending.db", session)

            async def exercise(client):
                original = await client.durable_append(one, session, 1)
                base_id = original["receipt"]["source_event_id"]
                # The local outbox first learns sequence 1 using the already-confirmed submission.
                outbox.append(one)
                await outbox.flush_one(client)
                assert outbox.append_revision(two, base_event_id=base_id, expected_revision=1) == 2

                class LoseAck:
                    async def durable_revise(self, *args, **kwargs):
                        await client.durable_revise(*args, **kwargs)
                        raise ConnectionError("lost")

                with pytest.raises(ConnectionError):
                    await outbox.flush_one(LoseAck())
                restarted = sdk.DurableOutbox(tmp_path / "pending.db", session)
                receipt = await restarted.flush_one(client)
                assert receipt["receipt"]["duplicate"] and receipt["acked_through"] == 2
                assert not (await client.durable_status(session, 1))["source_current"]
                with pytest.raises(sdk.MemoryClientError):
                    await client.durable_append(two, session, 2)
                assert (
                    restarted.append_revision(two, base_event_id=base_id, expected_revision=1) == 2
                )
                with pytest.raises(ValueError):
                    restarted.append(two)
                with closing(sqlite3.connect(tmp_path / "pending.db")) as conn:
                    assert (
                        conn.execute(
                            "SELECT count(*) FROM durable_pending WHERE event_json!='null'"
                        ).fetchone()[0]
                        == 0
                    )

            if transport == "embedded":
                await exercise(sdk.EmbeddedMemoryClient(kernel, context, durable_capture=api))
            else:
                mcp = pytest.importorskip("agent_memory_mcp")
                async with sdk.MCPMemoryClient(
                    mcp.create_server(
                        kernel, mcp.StaticIdentityResolver(context), durable_capture=api
                    )
                ) as client:
                    await exercise(client)

    asyncio.run(run())


def test_revision_ack_cursor_failure_rolls_back_entire_receive(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            producer = DurableProducer(receiver)
            session = await producer.open(
                scope, producer_id="host", actor="agent", configuration_sha256="a" * 64
            )
            old, new = base.source(scope), base.source(scope)
            await producer.append(old, session, sequence=1, actor="agent")
            cls = type(engine.repository.unit_of_work())
            original = cls.producer_put

            async def fail_ack(self, *args):
                await original(self, *args)
                raise RuntimeError("ack cursor commit failure")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "producer_put", fail_ack)
                with pytest.raises(RuntimeError, match="ack cursor"):
                    await producer.revise(
                        new,
                        session,
                        sequence=2,
                        actor="agent",
                        base_event_id=old.id,
                        expected_revision=1,
                    )
            assert (await producer.cursor(scope, session, actor="agent"))["acked_through"] == 1
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, new.id) is None
                head = await uow.retention_head_get(scope, "document", old.id)
                assert head["payload"]["event_id"] == old.id and head["generation"] == 1
                assert (
                    await uow.retention_get(
                        scope, "request", producer._request_key(scope, session, 2)
                    )
                    is None
                )
            result = await producer.revise(
                new, session, sequence=2, actor="agent", base_event_id=old.id, expected_revision=1
            )
            assert result["acked_through"] == 2

    asyncio.run(run())


def test_pending_outbox_schema_migration_preserves_unsent_body(tmp_path):
    import json
    from hashlib import sha256

    sdk = pytest.importorskip("agent_memory_sdk")
    session = {"producer_id": "host", "epoch": 0}
    session_key = sha256(json.dumps(session, sort_keys=True).encode()).hexdigest()
    event = {"event_id": "legacy", "content": "not sent yet"}
    encoded = json.dumps(event, sort_keys=True, ensure_ascii=False, allow_nan=False)
    digest = sha256(encoded.encode()).hexdigest()
    path = tmp_path / "legacy.db"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(
            "CREATE TABLE durable_pending(session_key TEXT, sequence INTEGER, event_id TEXT, "
            "event_json TEXT, content_sha256 TEXT, acknowledged INTEGER DEFAULT 0, "
            "PRIMARY KEY(session_key,sequence), UNIQUE(session_key,event_id))"
        )
        conn.execute(
            "INSERT INTO durable_pending VALUES(?,?,?,?,?,0)",
            (session_key, 1, "legacy", encoded, digest),
        )
        conn.commit()
    outbox = sdk.DurableOutbox(path, session)
    assert outbox.append(event) == 1

    class Client:
        async def durable_append(self, received, received_session, sequence):
            assert received == event and received_session == session and sequence == 1
            return {
                "producer_id": "host",
                "epoch": 0,
                "event_sha256": digest,
                "sequence": 1,
                "receipt": {"status": "queued"},
            }

    assert asyncio.run(outbox.flush_one(Client()))["sequence"] == 1
    assert (
        outbox.append_revision({"event_id": "next"}, base_event_id="legacy", expected_revision=1)
        == 2
    )
