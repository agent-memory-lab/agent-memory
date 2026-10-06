import asyncio
import sqlite3
from contextlib import closing

import pytest
import test_atom_admission as base
from test_durable_execution import Generator

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.retention import DurableReceiver
from agent_memory.operations.worker_runtime import BoundedWorker

store = base.store


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_lost_ack_restart_and_same_contract(store, tmp_path, transport):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            generator = Generator()
            pipeline = AtomExtractionPipeline(generator, generator)
            configuration = processing_configuration_sha256(pipeline, base.POLICY, base.SELF)
            producer = DurableProducer(DurableReceiver(engine.repository, clock=lambda: clock[0]))
            session = await producer.open(
                scope, producer_id="host", actor="alice", configuration_sha256=configuration
            )
            api = DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER)
            context = MCPRequestContext(scope, actor="alice")
            embedded = sdk.EmbeddedMemoryClient(kernel, context, durable_capture=api)
            event = LifecycleEvent(
                scope=scope,
                event_id="host-event",
                event_type=LifecycleEventType.MESSAGE_RECEIVED,
                origin=LifecycleOrigin.MODEL,
                occurred_at=clock[0],
                run_id="run",
                content="Alice lives in Hangzhou",
                actor="spoofed",
            ).to_dict()
            path = tmp_path / "outbox.db"
            outbox = sdk.DurableOutbox(path, session)
            assert outbox.append(event) == 1

            async def exercise(client):
                class LoseAck:
                    async def durable_append(self, *args):
                        await client.durable_append(*args)
                        raise ConnectionError("confirmation lost")

                with pytest.raises(ConnectionError):
                    await outbox.flush_one(LoseAck())
                restarted = sdk.DurableOutbox(path, session)
                receipt = await restarted.flush_one(client)
                assert receipt["receipt"]["duplicate"]
                assert receipt["acked_through"] == 1
                assert await restarted.flush_one(client) is None
                assert restarted.append(event) == 1
                assert (await client.durable_cursor(session))["acked_through"] == 1
                with closing(sqlite3.connect(path)) as conn:
                    assert (
                        conn.execute("SELECT event_json FROM durable_pending").fetchone()[0]
                        == "null"
                    )
                status = await client.durable_status(session, 1)
                assert status["source_persisted"] and not status["l1_decided"]
                queue = ExtractionQueue(
                    engine.repository, scope, configuration, clock=lambda: clock[0]
                )
                handler = DurableAtomHandler(
                    queue, pipeline, base.POLICY, base.SELF, local_only=True
                )
                assert await BoundedWorker(
                    queue, {"memory.extract": handler}, worker_id="worker"
                ).run_once()
                status = await client.durable_status(session, 1)
                assert status["l1_decided"] and status["result"]["claim_ids"]
                assert (await engine.state(scope, valid_at=clock[0], known_at=clock[0]))[0]
                restarted.purge()
                with pytest.raises(ValueError, match="purged"):
                    restarted.append({**event, "event_id": "another"})
                with pytest.raises(sdk.MemoryClientError):
                    await client.durable_append(
                        {**event, "payload": {"_agent_memory_capture": {}}}, session, 2
                    )

            if transport == "embedded":
                await exercise(embedded)
            else:
                mcp = pytest.importorskip("agent_memory_mcp")
                server = mcp.create_server(
                    kernel, mcp.StaticIdentityResolver(context), durable_capture=api
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)
            async with engine.repository.unit_of_work() as uow:
                saved = await uow.find_event_by_idempotency(scope, "lifecycle:v1:host-event")
                assert saved.actor == "alice" and saved.metadata["lifecycle"]["origin"] == "user"

    asyncio.run(run())
