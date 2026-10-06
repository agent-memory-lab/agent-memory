"""R02 interpretation replacement contracts on SQLite and real PostgreSQL."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base

from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import AtomReview, ForgetMode, ForgetRequest
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.reprocessing import ReprocessingService
from agent_memory.operations.retention import DurableReceiver, RetentionError
from agent_memory.operations.worker_runtime import BoundedWorker

store = base.store


class Adapter:
    def __init__(
        self, values=("Hangzhou",), *, supported=("Hangzhou",), verdict=None, version="initial"
    ):
        self.values, self.supported, self.verdict, self.version = (
            values,
            supported,
            verdict,
            version,
        )
        self.generations = self.reviews = 0

    async def generate_atoms(self, event):
        self.generations += 1
        return [
            dict(
                subject_id="alice",
                predicate="city",
                value=value,
                kind="fact",
                modality="asserted",
                source_quote=event.content,
            )
            for value in self.values
        ]

    async def review_atoms(self, event, candidates):
        self.reviews += 1
        if self.verdict == "incomplete":
            return []
        return [
            AtomReview(
                i,
                self.verdict or ("supported" if c.draft.value in self.supported else "unsupported"),
                "durable",
                ("fixture_review",),
            )
            for i, c in enumerate(candidates)
        ]


def processing(engine, scope, clock, adapter):
    pipeline = AtomExtractionPipeline(adapter, adapter)
    configuration = processing_configuration_sha256(pipeline, base.POLICY, base.SELF)
    queue = ExtractionQueue(
        engine.repository, scope, configuration, clock=lambda: clock[0], retry_seconds=0
    )
    handler = DurableAtomHandler(queue, pipeline, base.POLICY, base.SELF, local_only=True)
    runner = BoundedWorker(queue, {"memory.extract": handler}, worker_id="test")
    return configuration, queue, handler, runner


async def seed(engine, scope, clock, request_id="original"):
    source = base.source(scope)
    configuration, queue, _, runner = processing(engine, scope, clock, Adapter())
    receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
    ticket = await receiver.issue_ticket(
        source, request_id=request_id, producer_id="host", configuration_sha256=configuration
    )
    await receiver.submit(
        source, ticket=ticket, producer_id="host", configuration_sha256=configuration
    )
    assert await runner.run_once()
    assert (await queue.status(request_id))["status"] == "completed"
    service = ReprocessingService(receiver, producer_id="host", actor=source.actor)
    head = await service.snapshot(scope, source.id)
    return source, receiver, service, head


async def submit(
    service,
    scope,
    source,
    configuration,
    *,
    request_id="reprocess",
    generation=1,
    mode="replace_interpretation",
    allow_pending=False,
):
    return await service.submit(
        scope,
        source_event_id=source.id,
        request_id=request_id,
        mode=mode,
        configuration_sha256=configuration,
        expected_head_generation=generation,
        allow_pending=allow_pending,
    )


def test_zero_new_candidates_retain_old_support_without_new_evidence(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, _, service, before = await seed(engine, scope, clock)
            clock[0] = base.at(20)
            adapter = Adapter((), version="zero")
            configuration, queue, _, runner = processing(engine, scope, clock, adapter)
            await submit(service, scope, source, configuration)
            assert await runner.run_once()
            result = (await queue.status("reprocess"))["result"]
            assert result["interpretation"]["generation"] == 2
            assert result["candidate_ids"] == before["active_candidate_ids"]
            assert result["interpretation"]["new_candidate_ids"] == []
            assert adapter.generations == adapter.reviews == 1
            row = await engine.repository.admission_record(scope, before["active_candidate_ids"][0])
            assert row["payload"]["source_event_ids"] == [source.id]
            assert len(row["payload"]["evidence"]) == 1
            assert (await submit(service, scope, source, configuration)).duplicate

    asyncio.run(run())


def test_replacement_preserves_source_and_old_known_at(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, _, service, old = await seed(engine, scope, clock)
            async with engine.repository.unit_of_work() as uow:
                original = await uow.get_source_event(scope, source.id)
            clock[0] = base.at(20)
            adapter = Adapter(("Shanghai",), supported=("Shanghai",), version="corrected")
            configuration, queue, _, runner = processing(engine, scope, clock, adapter)
            await submit(service, scope, source, configuration)
            assert await runner.run_once()
            status = await queue.status("reprocess")
            assert status["status"] == "completed", status
            assert (await engine.state(scope, valid_at=base.at(1), known_at=base.at(19)))[0][
                0
            ].value == "Hangzhou"
            assert (await engine.state(scope, valid_at=base.at(1), known_at=base.at(20)))[0][
                0
            ].value == "Shanghai"
            assert (
                await engine.repository.admission_record(scope, old["active_candidate_ids"][0])
            )["payload"]["action"] == "WITHDRAWN"
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, source.id) == original
            assert adapter.reviews == 2

    asyncio.run(run())


@pytest.mark.parametrize("verdict", ["uncertain", "incomplete"])
def test_unknown_or_incomplete_review_cannot_activate(store, verdict):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, _, service, old = await seed(engine, scope, clock)
            configuration, queue, _, runner = processing(
                engine, scope, clock, Adapter((), verdict=verdict, version=verdict)
            )
            await submit(service, scope, source, configuration)
            assert await runner.run_once()
            assert (await queue.status("reprocess"))["status"] == "needs_resolution"
            assert not await runner.run_once()
            assert await service.snapshot(scope, source.id) == old
            assert (await engine.state(scope, valid_at=clock[0], known_at=clock[0]))[0][
                0
            ].value == "Hangzhou"

    asyncio.run(run())


def test_explicit_unknown_qualification_can_activate_pending(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, _, service, _ = await seed(engine, scope, clock)
            clock[0] = base.at(20)
            configuration, queue, _, runner = processing(
                engine, scope, clock, Adapter((), verdict="uncertain", version="unknown")
            )
            await submit(service, scope, source, configuration, allow_pending=True)
            assert await runner.run_once()
            result = (await queue.status("reprocess"))["result"]
            assert result["interpretation"]["activation_state"] == "activated_with_pending"
            assert result["pending_ids"] and not result["claim_ids"]
            assert not (await engine.state(scope, valid_at=base.at(1), known_at=clock[0]))[0]
            assert (await engine.state(scope, valid_at=base.at(1), known_at=base.at(19)))[0]

    asyncio.run(run())


def test_withdrawing_one_source_keeps_independent_source(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, _, service, _ = await seed(engine, scope, clock)
            independent, _, _, independent_head = await seed(engine, scope, clock, "independent")
            clock[0] = base.at(20)
            configuration, queue, _, runner = processing(
                engine, scope, clock, Adapter((), supported=(), version="withdraw")
            )
            await submit(service, scope, source, configuration)
            assert await runner.run_once()
            assert (await queue.status("reprocess"))["status"] == "completed"
            claims, _ = await engine.state(scope, valid_at=base.at(1), known_at=clock[0])
            assert claims[0].value == "Hangzhou" and claims[0].provenance.source_event_ids == (
                independent.id,
            )
            assert (await service.snapshot(scope, independent.id))[
                "active_candidate_ids"
            ] == independent_head["active_candidate_ids"]

    asyncio.run(run())


def test_additive_and_replace_share_cas_and_request_never_rebases(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, _, service, _ = await seed(engine, scope, clock)
            configuration, queue, _, runner = processing(
                engine, scope, clock, Adapter((), version="new")
            )
            await submit(service, scope, source, configuration, request_id="a", mode="additive")
            await submit(service, scope, source, configuration, request_id="b")
            assert await runner.run_once()
            assert await runner.run_once()
            assert (await queue.status("a"))["status"] == "completed"
            status = await queue.status("b")
            assert (
                status["status"] == "conflict"
                and status["last_error_code"] == "interpretation_head_changed"
            )
            assert (await service.snapshot(scope, source.id))["generation"] == 2
            with pytest.raises(RetentionError, match="request_input_conflict"):
                await submit(service, scope, source, configuration, request_id="b", generation=2)
            with pytest.raises(RetentionError, match="partial_coverage_unsupported"):
                await service.submit(
                    scope,
                    source_event_id=source.id,
                    request_id="partial",
                    mode="replace_interpretation",
                    configuration_sha256=configuration,
                    expected_head_generation=2,
                    coverage={"start": 0, "end": 1},
                )

    asyncio.run(run())


def test_replacement_transaction_rollback_reuses_review_stage(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, _, service, before = await seed(engine, scope, clock)
            adapter = Adapter(("Shanghai",), supported=("Shanghai",), version="retry")
            configuration, queue, handler, _ = processing(engine, scope, clock, adapter)
            await submit(service, scope, source, configuration)
            first = await queue.claim("one", lease_seconds=5)
            cls = type(engine.repository.unit_of_work())
            original = cls.retention_update

            async def fail_commit(self, *args):
                await original(self, *args)
                if args[-1]["status"] == "completed":
                    raise RuntimeError("rollback")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "retention_update", fail_commit)
                with pytest.raises(RuntimeError, match="rollback"):
                    await handler(first.task, lambda value: queue.checkpoint(first, value))
            assert await service.snapshot(scope, source.id) == before
            assert len(await engine.repository.admission_records(scope)) == 1
            clock[0] += timedelta(seconds=6)
            lease = await queue.claim("two", lease_seconds=60)
            await handler(lease.task, lambda value: queue.checkpoint(lease, value))
            await queue.complete(lease)
            assert adapter.generations == 1 and adapter.reviews == 2

    asyncio.run(run())


def test_erasure_during_reprocessing_prevents_activation_and_scrubs_stage(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            source, _, service, _ = await seed(engine, scope, clock)
            configuration, queue, handler, _ = processing(
                engine, scope, clock, Adapter((), version="erase")
            )
            await submit(service, scope, source, configuration)
            lease = await queue.claim("one", lease_seconds=60)

            async def erase(value):
                await queue.checkpoint(lease, value)
                await kernel.forget(
                    ForgetRequest(scope, memory_ids=(source.id,), mode=ForgetMode.ERASE)
                )

            with pytest.raises(Exception, match="lease"):
                await handler(lease.task, erase)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.retention_get(scope, "request", "reprocess")
                assert "prepared" not in row and "result" not in row
            with pytest.raises(RetentionError, match="source_unavailable"):
                await submit(service, scope, source, configuration, request_id="after-erase")

    asyncio.run(run())


def test_changed_contribution_after_checkpoint_prevents_activation(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, _, service, old = await seed(engine, scope, clock)
            configuration, queue, handler, _ = processing(
                engine, scope, clock, Adapter((), version="guard")
            )
            await submit(service, scope, source, configuration)
            lease = await queue.claim("worker", lease_seconds=60)

            async def concurrent_change(value):
                await queue.checkpoint(lease, value)
                async with engine.repository.unit_of_work() as uow:
                    await uow.lock_admission_scope(scope)
                    row = await uow.get_admission_record(scope, old["active_candidate_ids"][0])
                    row["payload"]["reasons"] = ["host_review_changed"]
                    await uow.save_admission_record(
                        scope,
                        row["id"],
                        row["event_id"],
                        row["slot_key"],
                        row["payload"],
                        row["version"],
                    )

            with pytest.raises(
                RetentionError, match="interpretation_contribution_changed"
            ) as failure:
                await handler(lease.task, concurrent_change)
            await queue.fail(lease, failure.value)
            assert (await queue.status("reprocess"))["status"] == "conflict"
            assert await service.snapshot(scope, source.id) == old

    asyncio.run(run())


def test_request_identity_and_owner_cannot_be_reused(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, receiver, service, _ = await seed(engine, scope, clock)
            configuration, _, _, _ = processing(engine, scope, clock, Adapter((), version="owner"))
            await submit(service, scope, source, configuration)
            with pytest.raises(RetentionError, match="request_identity_reserved"):
                await receiver.issue_ticket(
                    base.source(scope),
                    request_id="reprocess",
                    producer_id="host",
                    configuration_sha256=configuration,
                )
            await receiver.issue_ticket(
                base.source(scope),
                request_id="ticket",
                producer_id="host",
                configuration_sha256=configuration,
            )
            with pytest.raises(RetentionError, match="request_identity_reserved"):
                await submit(service, scope, source, configuration, request_id="ticket")
            for producer, actor in [("other", source.actor), ("host", "other")]:
                stranger = ReprocessingService(receiver, producer_id=producer, actor=actor)
                with pytest.raises(RetentionError, match="source_owner_mismatch"):
                    await stranger.snapshot(scope, source.id)
            with pytest.raises(RetentionError, match="source_unavailable"):
                await service.snapshot(replace(scope, tenant_id="other"), source.id)

    asyncio.run(run())


def test_additive_deduplicates_and_carries_pending_union_into_next_review(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            source, _, service, original = await seed(engine, scope, clock)
            adapter = Adapter(
                ("Hangzhou", "Shanghai"), supported=("Hangzhou", "Shanghai"), version="add"
            )
            configuration, queue, _, runner = processing(engine, scope, clock, adapter)
            await submit(service, scope, source, configuration, mode="additive")
            clock[0] = base.at(20)
            assert await runner.run_once()
            status = await queue.status("reprocess")
            assert status["status"] == "completed", status
            head = await service.snapshot(scope, source.id)
            assert len(head["active_candidate_ids"]) == 2
            assert set(original["active_candidate_ids"]) < set(head["active_candidate_ids"])
            assert status["result"]["pending_ids"]
            # The union must include the contested addition, even though it isn't a Claim.
            configuration, queue, _, runner = processing(
                engine, scope, clock, Adapter((), supported=("Hangzhou",), version="union")
            )
            await submit(service, scope, source, configuration, generation=2, request_id="union")
            clock[0] = base.at(21)
            assert await runner.run_once()
            status = await queue.status("union")
            assert status["status"] == "completed", status
            assert len(status["result"]["interpretation"]["old_review"]) == 2
            assert (await service.snapshot(scope, source.id))["active_candidate_ids"] == original[
                "active_candidate_ids"
            ]
            assert (await engine.state(scope, valid_at=base.at(1), known_at=clock[0]))[0][
                0
            ].value == "Hangzhou"

    asyncio.run(run())
