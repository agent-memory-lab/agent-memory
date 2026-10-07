"""Atomic locator outbox, finite coverage, real visibility and recovery on both databases."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_durable_execution import Generator
from test_durable_purge import envelope, source_id

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import AtomReview, ForgetMode, ForgetRequest
from agent_memory.lifecycle import LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.indexing import CandidateIndexChannel, CandidateIndexQueue
from agent_memory.operations.retention import DurableReceiver, RetentionError
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.operations.worker_tasks import WorkerQueueError

store = base.store


async def service(
    engine,
    kernel,
    scope,
    clock,
    *,
    generator=None,
    publication_policy=None,
    policy=base.POLICY,
    authority=base.SELF,
    max_candidates=32,
):
    sdk = pytest.importorskip("agent_memory_sdk")
    channel = CandidateIndexChannel("local")
    generator = generator or Generator()
    pipeline = AtomExtractionPipeline(generator, generator, max_candidates=max_candidates)
    config = processing_configuration_sha256(
        pipeline, policy, authority, index_channel=channel, publication_policy=publication_policy
    )
    producer = DurableProducer(
        DurableReceiver(engine.repository, clock=lambda: clock[0]), index_channel=channel
    )
    session = await producer.open(
        scope, producer_id="device", actor="alice", configuration_sha256=config
    )
    api = DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER)
    client = sdk.EmbeddedMemoryClient(
        kernel, MCPRequestContext(scope, actor="alice"), durable_capture=api
    )
    extract = ExtractionQueue(engine.repository, scope, config, clock=lambda: clock[0])
    handler = DurableAtomHandler(
        extract,
        pipeline,
        policy,
        authority,
        local_only=True,
        index_channel=channel,
        publication_policy=publication_policy,
    )
    worker = BoundedWorker(extract, {"memory.extract": handler}, worker_id="extract")
    queue = CandidateIndexQueue(engine.repository, scope, channel, clock=lambda: clock[0])
    indexer = BoundedWorker(queue, {"memory.index": queue.apply}, worker_id="index")
    return producer, session, client, api, generator, worker, queue, indexer


async def status(client, session, target):
    return await client.durable_readiness(session, target["target_id"], stage="index_visible")


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_exact_publication_coverage_and_continuous_prefix_differ(store, transport):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, embedded, api, _, worker, queue, _ = await service(
                engine, kernel, scope, clock
            )

            async def exercise(client):
                contracts = await client.durable_contracts(session)
                assert "index_visible" in contracts["supported_stages"]
                for seq in (1, 2):
                    await client.durable_append(envelope(scope, str(seq), clock), session, seq)
                target = await client.durable_freeze_target(session, [1, 2])
                before = await status(client, session, target)
                assert before["state"] == "processing" and not before["publication_manifest_closed"]
                assert await worker.run_once() and await worker.run_once()
                assert (await status(client, session, target))["state"] == "processing"
                one = await queue.claim("one", lease_seconds=5)
                two = await queue.claim("two", lease_seconds=5)
                await queue.apply(two.task, None)
                await queue.complete(two)
                partial = await status(client, session, target)
                assert (
                    partial["state"] == "processing" and partial["continuous_visible_through"] == 0
                )
                assert len(partial["applied_publication_ids"]) == 1
                assert not partial["covered_publication_ids"]
                # Even a finite later token waits for the fixed preceding coverage gap.
                only_two = await client.durable_freeze_target(session, [2])
                assert (await status(client, session, only_two))["state"] == "processing"
                await queue.apply(one.task, None)
                await queue.complete(one)
                reached = await status(client, session, target)
                assert reached["state"] == "reached" and reached["continuous_visible_through"] == 2
                assert not reached["uncovered_publication_ids"]
                async with engine.repository.unit_of_work() as uow:
                    records = await uow.list_admission_records(scope)
                found = await queue.lookup(records[0]["slot_key"])
                assert {d["candidate_id"] for d in found} == {r["id"] for r in records}
                assert all(
                    set(d) == {"candidate_id", "event_id", "slot_key", "record_version"}
                    for d in found
                )
                await client.durable_append(envelope(scope, "later", clock), session, 3)
                assert (await status(client, session, target))["state"] == "reached"
                assert await client.durable_freeze_target(session, [1, 2]) == target

            if transport == "embedded":
                await exercise(embedded)
            else:
                sdk = pytest.importorskip("agent_memory_sdk")
                mcp = pytest.importorskip("agent_memory_mcp")
                server = mcp.create_server(
                    kernel,
                    mcp.StaticIdentityResolver(MCPRequestContext(scope, actor="alice")),
                    durable_capture=api,
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())


def test_outbox_rolls_back_with_l1_publication_and_retry_reuses_stage(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, generator, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            cls = type(engine.repository.unit_of_work())
            original = cls.index_job_put

            async def fault(self, scope, row):
                await original(self, scope, row)
                raise RuntimeError("after outbox write")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "index_job_put", fault)
                assert await worker.run_once()
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.index_jobs(scope, queue.channel.key, session.epoch)
                assert not await uow.list_admission_records(scope)
            assert not (await status(client, session, target))["publication_manifest_closed"]
            clock[0] += timedelta(seconds=3)
            assert await worker.run_once() and generator.calls == 1
            assert await indexer.run_once()
            assert (await status(client, session, target))["state"] == "reached"
            assert not await indexer.run_once()

    asyncio.run(run())


def test_index_documents_and_completion_proof_commit_together(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once()
            cls = type(engine.repository.unit_of_work())
            original = cls.index_job_put

            async def fault(self, scope, row):
                await original(self, scope, row)
                if row["status"] == "completed":
                    raise RuntimeError("after document and proof writes")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "index_job_put", fault)
                assert await indexer.run_once()
            async with engine.repository.unit_of_work() as uow:
                record = (await uow.list_admission_records(scope))[0]
                assert await uow.index_document_get(scope, queue.channel.key, record["id"]) is None
                jobs = await uow.index_jobs(scope, queue.channel.key, session.epoch)
                assert jobs[0]["status"] == "retry_wait" and "proof" not in jobs[0]
            assert (await status(client, session, target))["state"] == "processing"
            clock[0] += timedelta(seconds=3)
            # A new queue object recovers the persistent pending task.
            recovered = CandidateIndexQueue(
                engine.repository, scope, queue.channel, clock=lambda: clock[0]
            )
            runner = BoundedWorker(
                recovered, {"memory.index": recovered.apply}, worker_id="recovered"
            )
            assert await runner.run_once()
            assert (await status(client, session, target))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("when", ["leased", "completed", "scope"])
def test_deletion_clears_locator_and_fences_old_index_lease(store, when):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker, queue, _ = await service(engine, kernel, scope, clock)
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once()
            lease = await queue.claim("old", lease_seconds=5)
            async with engine.repository.unit_of_work() as uow:
                record = (await uow.list_admission_records(scope))[0]
            if when != "leased":
                await queue.apply(lease.task, None)
            await kernel.forget(
                ForgetRequest(
                    scope,
                    memory_ids=() if when == "scope" else (source_id(scope, "one"),),
                    all_in_scope=when == "scope",
                    mode=ForgetMode.ERASE,
                )
            )
            assert not await queue.lookup(record["slot_key"])
            with pytest.raises(WorkerQueueError, match="source or lease"):
                await queue.apply(lease.task, None)
            async with engine.repository.unit_of_work() as uow:
                jobs = await uow.index_jobs(scope, queue.channel.key, session.epoch)
                assert jobs[0]["status"] == "cancelled" and "proof" not in jobs[0]
                assert await uow.index_document_get(scope, queue.channel.key, record["id"]) is None
            if when == "scope":
                with pytest.raises(
                    pytest.importorskip("agent_memory_sdk").MemoryClientError,
                    match="producer_revoked",
                ):
                    await status(client, session, target)
            else:
                assert (await status(client, session, target))["state"] == "blocked"

    asyncio.run(run())


def test_expired_index_lease_cannot_overwrite_recovered_publication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker, queue, _ = await service(engine, kernel, scope, clock)
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once()
            old = await queue.claim("old", lease_seconds=5)
            assert await queue.claim("competitor", lease_seconds=5) is None
            clock[0] += timedelta(seconds=6)
            new = await queue.claim("new", lease_seconds=5)
            assert new.token != old.token
            with pytest.raises(WorkerQueueError):
                await queue.apply(old.task, None)
            await queue.apply(new.task, None)
            await queue.complete(new)
            await queue.complete(new)  # Lost completion acknowledgment is safe.
            assert (await status(client, session, target))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("corruption", ["status_only", "document_missing", "wrong_token"])
def test_completed_flag_alone_cannot_prove_real_visibility(store, corruption):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once()
            if corruption != "status_only":
                assert await indexer.run_once()
            async with engine.repository.unit_of_work() as uow:
                await producer_lock(uow, scope)
                job = (await uow.index_jobs(scope, queue.channel.key, session.epoch))[0]
                if corruption == "status_only":
                    job["status"] = "completed"
                    await uow.index_job_put(scope, job)
                elif corruption == "document_missing":
                    await uow.index_document_put(
                        scope, queue.channel.key, job["dispositions"][0]["candidate_id"], None
                    )
                else:
                    job["token"]["configuration_sha256"] = "a" * 64
                    await uow.index_job_put(scope, job)
            after = await status(client, session, target)
            assert after["state"] == "blocked" and not after["index_visible"]
            assert after["continuous_visible_through"] == 0

    asyncio.run(run())


async def producer_lock(uow, scope):
    await DurableReceiver._check_support(uow, scope)


@pytest.mark.parametrize("outcome", ["empty", "pending", "rejected"])
def test_closed_no_claims_distinguished_from_open_and_unconfigured_index(store, outcome):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, client, _, generator, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            if outcome == "empty":

                async def empty(event):
                    return []

                generator.generate_atoms = empty
            else:

                async def review(event, candidates):
                    return [
                        AtomReview(
                            0,
                            "uncertain" if outcome == "pending" else "unsupported",
                            "durable",
                            ("review",),
                        )
                    ]

                generator.review_atoms = review
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert (await status(client, session, target))["state"] == "processing"
            assert await worker.run_once()
            if outcome != "empty":
                assert (await status(client, session, target))["state"] == "processing"
                assert await indexer.run_once()
            else:
                assert not await indexer.run_once()
            result = await status(client, session, target)
            assert result["state"] == "reached" and result["no_indexable_outputs"]
            assert result["no_outputs"] == (outcome == "empty")
            # Target identity binds its selected channel; another channel cannot reuse receipts.
            alternate = DurableProducer(
                producer.receiver, index_channel=CandidateIndexChannel("other")
            )
            different = await alternate.freeze_target(scope, session, sequences=[1], actor="alice")
            assert different["target_id"] != target["target_id"]
            assert (
                await alternate.readiness(
                    scope,
                    session,
                    target_id=different["target_id"],
                    stage="index_visible",
                    actor="alice",
                )
            )["state"] == "unsupported"

    asyncio.run(run())


def test_terminal_index_failure_does_not_advance_visibility(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once()

            async def fail(*args):
                raise RuntimeError("storage write failed")

            cls = type(engine.repository.unit_of_work())
            monkeypatch.setattr(cls, "index_document_put", fail)
            for _ in range(3):
                assert await indexer.run_once()
                clock[0] += timedelta(seconds=3)
            result = await status(client, session, target)
            assert result["state"] == "failed" and result["continuous_visible_through"] == 0

    asyncio.run(run())


def test_current_authority_guards_stale_locator_and_new_index_channel_changes_config(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, generator, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            assert await worker.run_once() and await indexer.run_once()
            async with engine.repository.unit_of_work() as uow:
                record = (await uow.list_admission_records(scope))[0]
            assert await queue.lookup(record["slot_key"])
            # Authority may change outside this locator; stale identities are filtered.
            await engine.retract(
                scope,
                record["id"],
                event=base.source(scope, "Alice left Hangzhou", day=2),
                authority=base.SELF,
                policy=base.POLICY,
                expected_version=record["version"],
                valid_to=base.at(2),
                source_quote="Alice left Hangzhou",
            )
            assert not await queue.lookup(record["slot_key"])
            other = replace(queue.channel, name="other")
            pipeline = AtomExtractionPipeline(generator, generator)
            assert (
                processing_configuration_sha256(
                    pipeline, base.POLICY, base.SELF, index_channel=other
                )
                != session.configuration_sha256
            )
            with pytest.raises(RetentionError, match="processing_configuration_changed"):
                DurableAtomHandler(
                    ExtractionQueue(engine.repository, scope, session.configuration_sha256),
                    pipeline,
                    base.POLICY,
                    base.SELF,
                    local_only=True,
                )

    asyncio.run(run())


@pytest.mark.parametrize("phase", ["index_before_commit", "index_after_commit"])
def test_real_index_process_kill_keeps_documents_and_proof_atomic(store, tmp_path, phase):
    from test_durable_process_recovery import kill_at_boundary

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once()
            await kill_at_boundary(engine, scope, clock, tmp_path, phase)
            committed = phase == "index_after_commit"
            result = await status(client, session, target)
            assert result["state"] == ("reached" if committed else "processing")
            async with engine.repository.unit_of_work() as uow:
                record = (await uow.list_admission_records(scope))[0]
                assert (
                    bool(await uow.index_document_get(scope, queue.channel.key, record["id"]))
                    == committed
                )
            if committed:
                assert not await indexer.run_once()
            else:
                assert not await indexer.run_once()  # Original lease still owns the task.
                clock[0] += timedelta(seconds=6)
                assert await indexer.run_once()
            assert (await status(client, session, target))["state"] == "reached"
            async with engine.repository.unit_of_work() as uow:
                jobs = await uow.index_jobs(scope, queue.channel.key, session.epoch)
                assert len(jobs) == 1 and jobs[0]["status"] == "completed"

    asyncio.run(run())


def test_old_outbox_job_uses_current_record_after_new_interpretation(store):
    from agent_memory.operations.reprocessing import ReprocessingService

    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, client, _, _, worker, queue, _ = await service(
                engine, kernel, scope, clock
            )
            received = await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once()
            old = await queue.claim("old", lease_seconds=60)
            reprocess = ReprocessingService(producer.receiver, producer_id="device", actor="alice")
            source = received["receipt"]["source_event_id"]
            head = await reprocess.snapshot(scope, source)
            await reprocess.submit(
                scope,
                source_event_id=source,
                request_id="again",
                mode="additive",
                configuration_sha256=session.configuration_sha256,
                expected_head_generation=head["generation"],
            )
            assert await worker.run_once()
            new = await queue.claim("new", lease_seconds=60)
            async with engine.repository.unit_of_work() as uow:
                record = (await uow.list_admission_records(scope))[0]
            await engine.retract(
                scope,
                record["id"],
                event=base.source(scope, "Alice left Hangzhou", day=2),
                authority=base.SELF,
                policy=base.POLICY,
                expected_version=record["version"],
                valid_to=base.at(2),
                source_quote="Alice left Hangzhou",
            )
            await queue.apply(new.task, None)
            async with engine.repository.unit_of_work() as uow:
                records = await uow.list_admission_records(scope)
                assert len(records) == 1 and records[0]["version"] > 1
            first = await queue.lookup(records[0]["slot_key"])
            await queue.apply(old.task, None)
            assert await queue.lookup(records[0]["slot_key"]) == first
            reached = await status(client, session, target)
            assert reached["state"] == "reached" and reached["continuous_visible_through"] == 2
            assert len(reached["publication_commit_tokens"]) == 1
            assert not reached["status_version"][0]["interpretation_current"]

    asyncio.run(run())


def test_locator_scope_channel_and_lookup_capacity_are_explicit(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            for seq in (1, 2):
                await client.durable_append(envelope(scope, str(seq), clock), session, seq)
                assert await worker.run_once() and await indexer.run_once()
            async with engine.repository.unit_of_work() as uow:
                slot = (await uow.list_admission_records(scope))[0]["slot_key"]
            with pytest.raises(RetentionError, match="index_lookup_capacity"):
                await queue.lookup(slot, limit=1)
            other = CandidateIndexQueue(
                engine.repository, replace(scope, user_id="mallory"), queue.channel
            )
            assert not await other.lookup(slot)
            channel = CandidateIndexQueue(engine.repository, scope, CandidateIndexChannel("other"))
            assert not await channel.lookup(slot)

    asyncio.run(run())


def test_index_backpressure_keeps_l1_and_closed_manifest_atomic(store):
    """Seed a bounded pending backlog, then exercise actual publication backpressure."""
    from copy import deepcopy

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, generator, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            assert await worker.run_once()
            async with engine.repository.unit_of_work() as uow:
                await producer_lock(uow, scope)
                seed = (await uow.index_jobs(scope, queue.channel.key, session.epoch))[0]
                for sequence in range(2, 129):
                    row = deepcopy(seed)
                    row["sequence"] = sequence
                    row["request_id"] = "seed:" + str(sequence)
                    row["token"]["id"] = "seed-token:" + str(sequence)
                    row["token"]["generation"] = row["request_id"]
                    await uow.index_job_put(scope, row)
            await client.durable_append(envelope(scope, "two", clock), session, 2)
            target = await client.durable_freeze_target(session, [2])
            assert await worker.run_once()
            result = await status(client, session, target)
            assert result["state"] == "processing" and not result["publication_manifest_closed"]
            assert len(await engine.repository.admission_records(scope)) == 1
            assert await indexer.run_once()  # Drain the first genuine publication.
            clock[0] += timedelta(seconds=3)
            assert await worker.run_once() and generator.calls == 2
            assert len(await engine.repository.admission_records(scope)) == 2
            async with engine.repository.unit_of_work() as uow:
                jobs = await uow.index_jobs(scope, queue.channel.key, session.epoch)
                assert len(jobs) == 129 and sum(j["status"] == "pending" for j in jobs) == 128
            assert (await status(client, session, target))["publication_manifest_closed"]

    asyncio.run(run())


@pytest.mark.parametrize("identity_kind", ["candidate", "claim", "source"])
def test_direct_and_cascaded_deletion_erases_index_metadata_and_proofs(store, identity_kind):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            for seq in (1, 2):
                await client.durable_append(envelope(scope, str(seq), clock), session, seq)
                assert await worker.run_once() and await indexer.run_once()
            async with engine.repository.unit_of_work() as uow:
                records = await uow.list_admission_records(scope)
                original = records[0]
                jobs = await uow.index_jobs(scope, queue.channel.key, session.epoch)
            assert len(await queue.lookup(original["slot_key"])) == 2
            identity = {
                "candidate": original["id"],
                "claim": original["payload"]["claim_id"],
                "source": original["event_id"],
            }[identity_kind]
            await kernel.forget(ForgetRequest(scope, memory_ids=(identity,), mode=ForgetMode.ERASE))
            assert not await queue.lookup(original["slot_key"])
            assert not await indexer.run_once()
            async with engine.repository.unit_of_work() as uow:
                after = await uow.index_jobs(scope, queue.channel.key, session.epoch)
                assert len(after) == len(jobs) == 2
                assert all(
                    j["status"] == "cancelled" and "proof" not in j and "applied" not in j
                    for j in after
                )
                for record in records:
                    assert (
                        await uow.index_document_get(scope, queue.channel.key, record["id"]) is None
                    )

    asyncio.run(run())


def test_cascaded_locator_cleanup_rolls_back_with_source_erasure(store, monkeypatch):
    import importlib

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker, queue, indexer = await service(
                engine, kernel, scope, clock
            )
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once() and await indexer.run_once()
            async with engine.repository.unit_of_work() as uow:
                record = (await uow.list_admission_records(scope))[0]
            postgres = hasattr(engine.repository, "pool")
            owner = importlib.import_module(
                "agent_memory_postgres.index"
                if postgres
                else "agent_memory.operations.sqlite_index"
            )
            original = owner.invalidate_records

            async def async_fault(*args):
                await original(*args)
                raise RuntimeError("after cascaded index erase")

            def sync_fault(*args):
                original(*args)
                raise RuntimeError("after cascaded index erase")

            with monkeypatch.context() as patch:
                patch.setattr(owner, "invalidate_records", async_fault if postgres else sync_fault)
                with pytest.raises(RuntimeError, match="after cascaded index erase"):
                    await kernel.forget(
                        ForgetRequest(
                            scope, memory_ids=(record["event_id"],), mode=ForgetMode.ERASE
                        )
                    )
            assert await queue.lookup(record["slot_key"])
            assert (await status(client, session, target))["state"] == "reached"
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, record["event_id"]) is not None
                current = await uow.get_admission_record(scope, record["id"])
                assert current["version"] == record["version"] and not current["payload"].get(
                    "deleted"
                )

    asyncio.run(run())
