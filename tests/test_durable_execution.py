import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as admission_tests

from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import AtomReview, ForgetMode, ForgetRequest
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.retention import DurableReceiver
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.operations.worker_tasks import WorkerQueueError

store = admission_tests.store


class Generator:
    version = "test-generator-1"
    calls = 0

    async def generate_atoms(self, event):
        self.calls += 1
        return [
            {
                "subject_id": "alice",
                "predicate": "city",
                "value": "Hangzhou",
                "kind": "fact",
                "modality": "asserted",
                "source_quote": event.content,
            }
        ]

    async def review_atoms(self, event, candidates):
        return [AtomReview(0, "supported", "durable", ("test_review",))]


async def setup(engine, scope, clock):
    generator = Generator()
    pipeline = AtomExtractionPipeline(generator, generator)
    config = processing_configuration_sha256(pipeline, admission_tests.POLICY, admission_tests.SELF)
    receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
    source = admission_tests.source(scope)
    source = replace(source, idempotency_key=source.id)
    ticket = await receiver.issue_ticket(
        source, request_id="one", producer_id="host", configuration_sha256=config
    )
    await receiver.submit(source, ticket=ticket, producer_id="host", configuration_sha256=config)
    queue = ExtractionQueue(
        engine.repository, scope, config, clock=lambda: clock[0], retry_seconds=0
    )
    handler = DurableAtomHandler(
        queue, pipeline, admission_tests.POLICY, admission_tests.SELF, local_only=True
    )
    return source, generator, receiver, queue, handler


def test_worker_publishes_once_without_replacing_l0(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            source, generator, receiver, queue, handler = await setup(engine, scope, clock)
            async with engine.repository.unit_of_work() as uow:
                original = await uow.find_event_by_idempotency(scope, source.id)
            runner = BoundedWorker(queue, {"memory.extract": handler}, worker_id="one")
            assert await runner.run_once()
            assert not await runner.run_once()
            status = await queue.status("one")
            assert status["l1_decided"] and status["result"]["claim_ids"]
            assert (await receiver.status(scope, "one")).status == "completed"
            assert generator.calls == 1
            async with engine.repository.unit_of_work() as uow:
                assert await uow.find_event_by_idempotency(scope, source.id) == original
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source.id,), mode=ForgetMode.ERASE)
            )
            assert (await queue.status("one"))["status"] == "cancelled"
            assert not (await engine.state(scope, valid_at=clock[0], known_at=clock[0]))[0]

    asyncio.run(run())


def test_stage_is_reused_after_publication_rollback_and_stale_lease_cannot_commit(
    store, monkeypatch
):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, generator, _, queue, handler = await setup(engine, scope, clock)
            first = await queue.claim("old", lease_seconds=5)
            cls = type(engine.repository.unit_of_work())
            update = cls.retention_update

            async def crash(self, scope, request_id, row):
                await update(self, scope, request_id, row)
                if row["status"] == "completed":
                    raise RuntimeError("transaction B interrupted")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "retention_update", crash)
                with pytest.raises(RuntimeError, match="interrupted"):
                    await handler(first.task, lambda value: queue.checkpoint(first, value))
            assert not await engine.repository.admission_records(scope)
            clock[0] += timedelta(seconds=6)
            second = await queue.claim("new", lease_seconds=60)
            with pytest.raises(WorkerQueueError, match="lease"):
                await handler(first.task, lambda value: queue.checkpoint(first, value))
            await handler(second.task, lambda value: queue.checkpoint(second, value))
            await queue.complete(second)
            assert generator.calls == 1
            assert (await queue.status("one"))["attempts"] == 2

    asyncio.run(run())


def test_erasure_after_generation_prevents_checkpoint_and_publication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            source, _, _, queue, handler = await setup(engine, scope, clock)
            lease = await queue.claim("worker", lease_seconds=60)

            async def erase_then_checkpoint(value):
                await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
                await queue.checkpoint(lease, value)

            with pytest.raises(WorkerQueueError):
                await handler(lease.task, erase_then_checkpoint)
            assert not await engine.repository.admission_records(scope)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.retention_get(scope, "request", "one")
                assert "prepared" not in row and "result" not in row

    asyncio.run(run())


def test_workers_share_claim_and_failed_attempts_reach_dead(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            _, generator, _, queue, handler = await setup(engine, scope, clock)
            leases = await asyncio.gather(
                queue.claim("a", lease_seconds=60), queue.claim("b", lease_seconds=60)
            )
            assert sum(lease is not None for lease in leases) == 1
            lease = next(item for item in leases if item is not None)
            await queue.fail(lease, RuntimeError("credential must not be saved"))

            async def fail(event):
                raise RuntimeError("credential must not be saved")

            generator.generate_atoms = fail
            runner = BoundedWorker(queue, {"memory.extract": handler}, worker_id="retry")
            assert await runner.run_once()
            assert await runner.run_once()
            assert not await runner.run_once()
            status = await queue.status("one")
            assert status["status"] == "dead" and not status["l1_decided"]
            assert status["attempts"] == 3
            assert "credential" not in repr(status)

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["empty", "pending", "configuration_changed"])
def test_processing_outcomes_remain_distinct(store, mode):
    async def run():
        async with store() as (engine, _, scope, clock):
            _, generator, _, queue, handler = await setup(engine, scope, clock)
            if mode == "empty":

                async def empty(event):
                    return []

                generator.generate_atoms = empty
            elif mode == "pending":

                async def pending(event, candidates):
                    return [AtomReview(0, "uncertain", "durable", ("review_required",))]

                generator.review_atoms = pending
            else:
                generator.version = "changed"
            runner = BoundedWorker(queue, {"memory.extract": handler}, worker_id="worker")
            assert await runner.run_once()
            status = await queue.status("one")
            if mode == "configuration_changed":
                assert not status["l1_decided"] and generator.calls == 0
            else:
                assert status["l1_decided"] and not status["result"]["claim_ids"]
                assert bool(status["result"]["pending_ids"]) == (mode == "pending")
                assert status["result"]["extraction"]["processing_state"] == "completed"

    asyncio.run(run())
