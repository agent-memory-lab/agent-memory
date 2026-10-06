import asyncio
from dataclasses import replace

import pytest
import test_atom_admission as base

from agent_memory.capture.producer import DurableProducer
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.operations.retention import DurableReceiver, RetentionError

store = base.store


def test_gap_retry_identity_and_epoch(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer = DurableProducer(
                DurableReceiver(engine.repository, clock=lambda: clock[0]), max_gap=4
            )
            session = await producer.open(
                scope, producer_id="host", actor="alice", configuration_sha256="a" * 64
            )
            one = replace(base.source(scope), actor="alice")
            two = replace(base.source(scope), actor="alice")
            receipt = await producer.append(two, session, sequence=2, actor="alice")
            assert receipt["acked_through"] == 0 and receipt["received_after_gap"] == [2]
            receipt = await producer.append(one, session, sequence=1, actor="alice")
            assert receipt["acked_through"] == 2 and receipt["received_after_gap"] == []
            assert (await producer.append(two, session, sequence=2, actor="alice"))["receipt"][
                "duplicate"
            ]
            with pytest.raises(RetentionError, match="conflict"):
                await producer.append(
                    replace(two, content="changed"), session, sequence=2, actor="alice"
                )
            with pytest.raises(RetentionError, match="gap_limit"):
                await producer.append(
                    replace(base.source(scope), actor="alice"), session, sequence=8, actor=one.actor
                )
            with pytest.raises(RetentionError, match="invalid_producer"):
                await producer.cursor(scope, replace(session, token="bad"), actor="alice")
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            with pytest.raises(RetentionError, match="revoked"):
                await producer.append(one, session, sequence=1, actor="alice")

    asyncio.run(run())


def test_cursor_and_source_roll_back_together(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            producer = DurableProducer(DurableReceiver(engine.repository, clock=lambda: clock[0]))
            session = await producer.open(
                scope, producer_id="host", actor="alice", configuration_sha256="a" * 64
            )
            event = replace(base.source(scope), actor="alice")
            cls = type(engine.repository.unit_of_work())
            original = cls.producer_put

            async def crash(self, *args):
                await original(self, *args)
                raise RuntimeError("lost commit")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "producer_put", crash)
                with pytest.raises(RuntimeError):
                    await producer.append(event, session, sequence=1, actor="alice")
            assert (await producer.cursor(scope, session, actor="alice"))["acked_through"] == 0
            receipt = await producer.append(event, session, sequence=1, actor="alice")
            assert receipt["receipt"]["duplicate"] is False

    asyncio.run(run())
