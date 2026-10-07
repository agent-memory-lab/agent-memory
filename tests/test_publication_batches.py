"""A full request covers every committed batch, both databases and client transports."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_durable_indexing import service, status
from test_durable_purge import envelope

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.domain import AtomReview, ForgetMode, ForgetRequest, PredicateSpec
from agent_memory.operations.extraction_worker import processing_configuration_sha256
from agent_memory.operations.index_recovery import CandidateIndexRecovery
from agent_memory.operations.publication_batches import PublicationPolicy, pack
from agent_memory.operations.publication_manifest import digest, valid_manifest
from agent_memory.operations.reprocessing import ReprocessingService
from agent_memory.operations.retention import RetentionError
from agent_memory.operations.worker_tasks import WorkerQueueError

store = base.store


class Generator:
    version = "batch-fixture/1"
    calls = 0

    def __init__(self, values=(("city", "Hangzhou"), ("locale", "zh-CN")), verdict="supported"):
        self.values, self.verdict = values, verdict

    async def generate_atoms(self, event):
        self.calls += 1
        return [
            dict(
                subject_id="alice",
                predicate=predicate,
                value=value,
                kind="fact",
                modality="asserted",
                source_quote=event.content,
            )
            for predicate, value in self.values
        ]

    async def review_atoms(self, event, candidates):
        return [
            AtomReview(i, self.verdict, "durable", ("fixture",)) for i, _ in enumerate(candidates)
        ]


class ReplacementGenerator(Generator):
    version = "replacement/1"

    def __init__(self):
        super().__init__((("city", "Suzhou"), ("locale", "en-US")))

    async def review_atoms(self, event, candidates):
        return [
            AtomReview(
                i,
                "supported" if (c.draft.predicate, c.draft.value) in self.values else "unsupported",
                "durable",
                ("fixture",),
            )
            for i, c in enumerate(candidates)
        ]


async def setup(engine, kernel, scope, clock, **kwargs):
    services = await service(
        engine,
        kernel,
        scope,
        clock,
        generator=kwargs.pop("generator", Generator()),
        publication_policy=PublicationPolicy(1),
        **kwargs,
    )
    producer, session, client, _, _, _, _, _ = services
    appended = await client.durable_append(envelope(scope, "batches", clock), session, 1)
    target = await client.durable_freeze_target(session, [1])
    request_id = appended["receipt"]["request_id"]
    return services, target, request_id


async def row_for(engine, scope, request_id):
    async with engine.repository.unit_of_work() as uow:
        return await uow.retention_get(scope, "request", request_id)


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_every_token_and_continuous_prefix_must_be_covered(store, transport):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, target, request_id = await setup(engine, kernel, scope, clock)
            _, session, embedded, api, generator, worker, queue, _ = services

            async def exercise(client):
                assert (
                    "publication-manifest/2"
                    in (await client.durable_contracts(session))["publication_manifest_schemas"]
                )
                assert await worker.run_once()
                row = await row_for(engine, scope, request_id)
                manifest = row["publication_manifest"]
                assert (
                    valid_manifest(scope, row) and manifest["closed"] and manifest["version"] == 3
                )
                assert len(manifest["publication_commit_tokens"]) == 2
                assert len(row["result"]["claim_ids"]) == 2 and generator.calls == 1
                first = await queue.claim("one", lease_seconds=60)
                second = await queue.claim("two", lease_seconds=60)
                await queue.apply(second.task, None)
                pending = await status(client, session, target)
                assert pending["state"] == "processing" and not pending["covered_publication_ids"]
                assert len(pending["applied_publication_ids"]) == 1
                await queue.apply(first.task, None)
                ready = await status(client, session, target)
                assert ready["state"] == "reached" and len(ready["covered_publication_ids"]) == 2
                async with engine.repository.unit_of_work() as uow:
                    jobs = await uow.index_jobs(scope, queue.channel.key, 0)
                assert [len(j["dispositions"]) for j in jobs] == [1, 1]
                assert not await worker.run_once()

            if transport == "embedded":
                await exercise(embedded)
            else:
                mcp = pytest.importorskip("agent_memory_mcp")
                sdk = pytest.importorskip("agent_memory_sdk")

                from agent_memory.mcp import MCPRequestContext

                server = mcp.create_server(
                    kernel,
                    mcp.StaticIdentityResolver(MCPRequestContext(scope, actor="alice")),
                    durable_capture=api,
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["batch", "close", "outbox"])
def test_partial_commit_resume_never_regenerates_or_republishes(store, monkeypatch, boundary):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, target, request_id = await setup(engine, kernel, scope, clock)
            producer, session, client, _, generator, worker, queue, indexer = services
            cls = type(engine.repository.unit_of_work())
            original = cls.index_job_put if boundary == "outbox" else cls.retention_update

            async def fault(uow, *args):
                await original(uow, *args)
                row = args[-1]
                if (
                    boundary == "outbox"
                    and row["sequence"] == 2
                    or boundary == "batch"
                    and len(row.get("publication_manifest", {}).get("publications", [])) == 2
                    or boundary == "close"
                    and row.get("status") == "completed"
                ):
                    raise RuntimeError("commit interrupted")

            with monkeypatch.context() as patch:
                patch.setattr(
                    cls, "index_job_put" if boundary == "outbox" else "retention_update", fault
                )
                assert await worker.run_once()
            before = await row_for(engine, scope, request_id)
            count = 2 if boundary == "close" else 1
            assert len(before["publication_manifest"]["publications"]) == count
            assert valid_manifest(scope, before) and not before["publication_manifest"]["closed"]
            assert (await status(client, session, target))["state"] == "processing"
            records = await engine.repository.admission_records(scope)
            assert len(records) == count
            # Committed batches can be indexed while the request is still open.
            assert await indexer.run_once()
            assert (await status(client, session, target))["state"] == "processing"
            reprocessor = ReprocessingService(
                producer.receiver, producer_id="device", actor="alice"
            )
            with pytest.raises(RetentionError, match="initial_interpretation_not_ready"):
                await reprocessor.snapshot(scope, before["event_id"])
            saved = deepcopy(before["publication_manifest"]["publications"])
            clock[0] += timedelta(seconds=3)
            assert await worker.run_once()
            after = await row_for(engine, scope, request_id)
            assert after["publication_manifest"]["publications"][:count] == saved
            assert generator.calls == 1 and after["status"] == "completed"
            assert len(await engine.repository.admission_records(scope)) == 2
            while await indexer.run_once():
                pass
            assert (await status(client, session, target))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize(
    "values,verdict,tokens,claims",
    [
        ((), "supported", 0, 0),
        ((("city", "Hangzhou"), ("city", "Suzhou"), ("locale", "zh-CN")), "supported", 2, 1),
        ((("city", "Hangzhou"), ("locale", "zh-CN")), "uncertain", 2, 0),
        ((("city", "Hangzhou"), ("locale", "zh-CN")), "unsupported", 2, 0),
        ((("city", "Hangzhou"), ("city", "Hangzhou")), "supported", 1, 1),
    ],
)
def test_slot_grouping_and_empty_pending_rejected_dispositions(
    store, values, verdict, tokens, claims
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, target, request_id = await setup(
                engine, kernel, scope, clock, generator=Generator(values, verdict)
            )
            _, session, client, _, _, worker, _, indexer = services
            assert await worker.run_once()
            row = await row_for(engine, scope, request_id)
            assert row["status"] == "completed" and valid_manifest(scope, row)
            manifest = row["publication_manifest"]
            assert len(manifest["publication_commit_tokens"]) == tokens
            assert len(row["result"]["claim_ids"]) == claims
            assert manifest["no_outputs"] == (tokens == 0)
            assert manifest["no_indexable_outputs"] == (claims == 0)
            if len(values) == 3:
                assert manifest["plan"] == [[0, 1], [2]]
                assert [d["action"] for d in row["result"]["decisions"]] == [
                    "CONTESTED",
                    "CONTESTED",
                    "ACCEPT",
                ]
            while await indexer.run_once():
                pass
            assert (await status(client, session, target))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("field", ["token", "dispositions", "result", "plan", "version", "policy"])
def test_corrupt_proof_blocks_readiness_and_index_application(store, field):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, target, request_id = await setup(engine, kernel, scope, clock)
            _, session, client, _, _, worker, queue, _ = services
            assert await worker.run_once()
            lease = await queue.claim("index", lease_seconds=60)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.retention_get(scope, "request", request_id)
                m = row["publication_manifest"]
                if field == "token":
                    m["publications"][0]["token"]["id"] = "forged"
                elif field == "dispositions":
                    m["publications"][0]["dispositions"][0]["action"] = "ACCEPT_OTHER"
                elif field == "result":
                    row["result"]["claim_ids"] = []
                    m["no_indexable_outputs"] = True
                elif field == "version":
                    m["version"] = True
                else:
                    if field == "plan":
                        m["plan"] = [[1], [0]]
                    else:
                        m["policy"]["batch_size"] = 2
                    m["plan_sha256"] = digest(
                        {k: m[k] for k in ("policy", "plan", "prepared_sha256", "mode")}
                    )
                assert not valid_manifest(scope, row)
                await uow.retention_update(scope, request_id, row)
            assert (await status(client, session, target))["state"] == "blocked"
            with pytest.raises(RetentionError, match="index_publication_conflict"):
                await queue.apply(lease.task, None)

    asyncio.run(run())


def test_configuration_and_policy_are_explicit_bounded_and_legacy_hash_stays_identical():
    assert pack([(0, "one"), (1, "two"), (2, "one")], 1) == [[0, 2], [1]]
    for value in (True, 0, 65, "2", None):
        with pytest.raises(ValueError):
            PublicationPolicy(value)
    from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline

    pipeline = AtomExtractionPipeline(Generator(), Generator())
    legacy = processing_configuration_sha256(pipeline, base.POLICY, base.SELF)
    assert legacy == processing_configuration_sha256(
        pipeline, base.POLICY, base.SELF, publication_policy=None
    )
    assert legacy != processing_configuration_sha256(
        pipeline, base.POLICY, base.SELF, publication_policy=PublicationPolicy()
    )


async def interrupt_after_first(engine, scope, worker, monkeypatch):
    cls = type(engine.repository.unit_of_work())
    original = cls.retention_update

    async def fault(uow, *args):
        await original(uow, *args)
        row = args[-1]
        if (
            row.get("status") == "running"
            and len(row.get("publication_manifest", {}).get("publications", [])) == 2
        ):
            raise RuntimeError("second batch rollback")

    with monkeypatch.context() as patch:
        patch.setattr(cls, "retention_update", fault)
        assert await worker.run_once()


@pytest.mark.parametrize("open_request", [False, True])
def test_repair_and_rollover_bind_each_actual_batch(store, monkeypatch, open_request):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, target, request_id = await setup(engine, kernel, scope, clock)
            _, session, client, _, _, worker, queue, indexer = services
            if open_request:
                await interrupt_after_first(engine, scope, worker, monkeypatch)
            else:
                assert await worker.run_once()
            row = await row_for(engine, scope, request_id)
            tokens = row["publication_manifest"]["publication_commit_tokens"]
            async with engine.repository.unit_of_work() as uow:
                job = await uow.index_job_get(scope, queue.channel.key, 0, tokens[0]["id"])
                job.update(status="dead", dispositions=[])
                await uow.index_job_put(scope, job)
            recovery = CandidateIndexRecovery(
                engine.repository, scope, queue.channel, actor="operator", clock=lambda: clock[0]
            )
            inspection = await recovery.inspect(tokens[0]["id"])
            await recovery.repair(
                tokens[0]["id"],
                recovery_id="repair",
                expected_job_sha256=inspection["job_sha256"],
                stream=inspection["stream"],
                reason="operator",
            )
            async with engine.repository.unit_of_work() as uow:
                job = await uow.index_job_get(scope, queue.channel.key, 0, tokens[0]["id"])
                assert len(job["dispositions"]) == 1 and len(job["applied"]) == 1
            rollover = await recovery.rollover(
                recovery_id="roll", expected_generation=0, reason="operator"
            )
            assert rollover["baseline_count"] == len(tokens)
            # Old finite target remains bound to its original stream.
            assert (await status(client, session, target))["state"] != "reached"
            if open_request:
                clock[0] += timedelta(seconds=3)
                assert await worker.run_once()
                assert await indexer.run_once()
            fresh = await client.durable_freeze_target(session, [1])
            assert (await status(client, session, fresh))["state"] == "reached"
            async with engine.repository.unit_of_work() as uow:
                jobs = await uow.index_jobs(scope, await queue.active_key(uow), 0)
                assert len(jobs) == 2 and [len(j["dispositions"]) for j in jobs] == [1, 1]

    asyncio.run(run())


@pytest.mark.parametrize("action", ["source", "scope", "revision"])
def test_erasure_or_revision_after_partial_commit_fences_remainder(store, monkeypatch, action):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, target, request_id = await setup(engine, kernel, scope, clock)
            _, session, client, _, generator, worker, queue, indexer = services
            await interrupt_after_first(engine, scope, worker, monkeypatch)
            row = await row_for(engine, scope, request_id)
            old = await queue.claim("old", lease_seconds=60)
            if action == "revision":
                new = envelope(scope, "revision", clock)
                await client.durable_revise(
                    new, session, 2, base_event_id=row["event_id"], expected_revision=1
                )
            else:
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        memory_ids=(row["event_id"],),
                        all_in_scope=action == "scope",
                        mode=ForgetMode.ERASE,
                    )
                )
            with pytest.raises(WorkerQueueError):
                await queue.apply(old.task, None)
            if action == "scope":
                from agent_memory_sdk import MemoryClientError

                with pytest.raises(MemoryClientError, match="producer_revoked"):
                    await status(client, session, target)
            else:
                assert (await status(client, session, target))["state"] == "blocked"
            obsolete = await row_for(engine, scope, request_id)
            assert not any(k in obsolete for k in ("prepared", "result", "publication_manifest"))
            if action != "revision":
                assert not await worker.run_once() and not await indexer.run_once()
                assert not await engine.repository.admission_records(scope)
            else:
                records = await engine.repository.admission_records(scope)
                assert all(r["payload"]["action"] == "WITHDRAWN" for r in records)
            assert generator.calls == 1

    asyncio.run(run())


def test_dead_partial_request_requires_explicit_cas_resume(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, _, request_id = await setup(engine, kernel, scope, clock)
            _, _, _, _, generator, worker, _, _ = services
            extract = worker._queue
            extract.max_attempts = 1
            await interrupt_after_first(engine, scope, worker, monkeypatch)
            row = await row_for(engine, scope, request_id)
            assert row["status"] == "dead" and not await worker.run_once()
            with pytest.raises(RetentionError, match="publication_manifest_changed"):
                await extract.resume(
                    request_id, expected_manifest_version=0, actor="operator", reason="retry"
                )
            await extract.resume(
                request_id, expected_manifest_version=1, actor="operator", reason="retry"
            )
            with pytest.raises(RetentionError, match="publication_resume_unavailable"):
                await extract.resume(
                    request_id, expected_manifest_version=1, actor="operator", reason="retry"
                )
            assert await worker.run_once()
            final = await row_for(engine, scope, request_id)
            assert final["status"] == "completed" and generator.calls == 1
            assert (
                final["publication_manifest"]["publications"][0]
                == row["publication_manifest"]["publications"][0]
            )
            assert final["publication_manifest"]["resumptions"][0]["actor"] == "operator"

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["additive", "replace_interpretation"])
def test_reprocessing_keeps_whole_activation_atomic_with_multiple_proofs(store, monkeypatch, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, _, request_id = await setup(engine, kernel, scope, clock)
            producer, _, _, _, _, worker, queue, indexer = services
            assert await worker.run_once()
            initial = await row_for(engine, scope, request_id)
            while await indexer.run_once():
                pass
            reprocessor = ReprocessingService(
                producer.receiver, producer_id="device", actor="alice"
            )
            head = await reprocessor.snapshot(scope, initial["event_id"])
            clock[0] += timedelta(days=1)

            generator = (
                ReplacementGenerator() if mode == "replace_interpretation" else Generator(values=())
            )
            handler = worker._handlers["memory.extract"]
            from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline

            pipeline = AtomExtractionPipeline(generator, generator)
            config = processing_configuration_sha256(
                pipeline,
                base.POLICY,
                base.SELF,
                index_channel=queue.channel,
                publication_policy=PublicationPolicy(1),
            )
            await reprocessor.submit(
                scope,
                source_event_id=initial["event_id"],
                request_id="reprocess",
                mode=mode,
                configuration_sha256=config,
                expected_head_generation=head["generation"],
            )
            from agent_memory.operations.extraction_worker import (
                DurableAtomHandler,
                ExtractionQueue,
            )
            from agent_memory.operations.worker_runtime import BoundedWorker

            extract = ExtractionQueue(
                engine.repository, scope, config, clock=lambda: clock[0], retry_seconds=0
            )
            handler = DurableAtomHandler(
                extract,
                pipeline,
                base.POLICY,
                base.SELF,
                local_only=True,
                index_channel=queue.channel,
                publication_policy=PublicationPolicy(1),
            )
            runner = BoundedWorker(extract, {"memory.extract": handler}, worker_id="reprocess")
            cls = type(engine.repository.unit_of_work())
            original = cls.index_job_put

            async def fault(uow, *args):
                await original(uow, *args)
                if args[-1]["sequence"] == 4:
                    raise RuntimeError("second proof rollback")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "index_job_put", fault)
                assert await runner.run_once()
            assert await reprocessor.snapshot(scope, initial["event_id"]) == head
            assert len(await engine.repository.admission_records(scope)) == 2
            failed = await row_for(engine, scope, "reprocess")
            assert failed["publication_manifest"]["schema"] == "publication-manifest/1"
            assert not failed["publication_manifest"]["closed"]
            assert await runner.run_once()
            result = await row_for(engine, scope, "reprocess")
            assert result["status"] == "completed" and valid_manifest(scope, result)
            assert result["publication_manifest"]["mode"] == "atomic_activation"
            assert len(result["publication_manifest"]["publication_commit_tokens"]) == 2
            assert generator.calls == 1
            assert len(result["result"]["claim_ids"]) == 2
            if mode == "replace_interpretation":
                assert [
                    len(p["dispositions"]) for p in result["publication_manifest"]["publications"]
                ] == [2, 2]
                claims, _ = await engine.state(scope, valid_at=clock[0], known_at=clock[0])
                assert {c.value for c in claims} == {"Suzhou", "en-US"}

    asyncio.run(run())


def test_maximum_64_drafts_publish_64_finite_batches(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            predicates = [f"field_{i}" for i in range(64)]
            policy = AdmissionPolicy([PredicateSpec(p) for p in predicates])
            authority = replace(base.SELF, predicates=tuple(predicates))
            services, target, request_id = await setup(
                engine,
                kernel,
                scope,
                clock,
                policy=policy,
                authority=authority,
                max_candidates=64,
                generator=Generator([(p, "value") for p in predicates]),
            )
            _, session, client, _, _, worker, _, indexer = services
            assert await worker.run_once()
            row = await row_for(engine, scope, request_id)
            assert row["status"] == "completed" and valid_manifest(scope, row)
            assert len(row["publication_manifest"]["publication_commit_tokens"]) == 64
            assert row["publication_manifest"]["version"] == 65
            for _ in range(64):
                assert await indexer.run_once()
            assert not await indexer.run_once()
            assert (await status(client, session, target))["continuous_visible_through"] == 64

    asyncio.run(run())


@pytest.mark.parametrize(
    "phase",
    [
        "publication_batch_before_commit",
        "publication_batch_after_commit",
        "publication_close_before_commit",
        "publication_close_after_commit",
    ],
)
def test_real_process_kill_at_batch_and_closure_boundaries(store, tmp_path, phase):
    async def run():
        from test_durable_process_recovery import kill_at_boundary

        async with store() as (engine, kernel, scope, clock):
            services, target, request_id = await setup(engine, kernel, scope, clock)
            _, session, client, _, generator, worker, queue, indexer = services
            await kill_at_boundary(engine, scope, clock, tmp_path, phase)
            row = await row_for(engine, scope, request_id)
            expected = 0 if phase.endswith("batch_before_commit") else 1 if "batch" in phase else 2
            assert len(row["publication_manifest"].get("publications", [])) == expected
            assert len(await engine.repository.admission_records(scope)) == expected
            assert valid_manifest(scope, row)
            assert row["publication_manifest"]["closed"] == phase.endswith("close_after_commit")
            saved = deepcopy(row["publication_manifest"].get("publications", []))
            clock[0] += timedelta(seconds=6)
            if phase.endswith("close_after_commit"):
                assert not await worker.run_once()
            else:
                assert await worker.run_once()
            final = await row_for(engine, scope, request_id)
            assert final["status"] == "completed" and valid_manifest(scope, final)
            assert final["publication_manifest"]["publications"][:expected] == saved
            assert (
                generator.calls == 0 and (tmp_path / "generation.log").read_text() == "generate\n"
            )
            assert len(await engine.repository.admission_records(scope)) == 2
            while await indexer.run_once():
                pass
            assert (await status(client, session, target))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize(
    "phase", ["publication_activation_before_commit", "publication_activation_after_commit"]
)
def test_real_process_replacement_never_commits_half_an_activation(store, tmp_path, phase):
    async def run():
        from test_durable_process_recovery import kill_at_boundary

        from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
        from agent_memory.operations.extraction_worker import DurableAtomHandler, ExtractionQueue
        from agent_memory.operations.worker_runtime import BoundedWorker

        async with store() as (engine, kernel, scope, clock):
            services, _, request_id = await setup(engine, kernel, scope, clock)
            producer, session, client, _, _, worker, queue, indexer = services
            assert await worker.run_once()
            while await indexer.run_once():
                pass
            row = await row_for(engine, scope, request_id)
            operator = ReprocessingService(producer.receiver, producer_id="device", actor="alice")
            head = await operator.snapshot(scope, row["event_id"])
            generator = ReplacementGenerator()
            pipeline = AtomExtractionPipeline(generator, generator)
            config = processing_configuration_sha256(
                pipeline,
                base.POLICY,
                base.SELF,
                index_channel=queue.channel,
                publication_policy=PublicationPolicy(1),
            )
            await operator.submit(
                scope,
                source_event_id=row["event_id"],
                request_id="reprocess",
                mode="replace_interpretation",
                configuration_sha256=config,
                expected_head_generation=head["generation"],
            )
            target = await client.durable_freeze_reprocessing_target(session, ["reprocess"])
            await kill_at_boundary(engine, scope, clock, tmp_path, phase)
            after = await row_for(engine, scope, "reprocess")
            committed = phase.endswith("after_commit")
            assert after["publication_manifest"]["closed"] == committed
            assert len(await engine.repository.admission_records(scope)) == (4 if committed else 2)
            if not committed:
                assert await operator.snapshot(scope, row["event_id"]) == head
            clock[0] += timedelta(seconds=6)
            extract = ExtractionQueue(engine.repository, scope, config, clock=lambda: clock[0])
            handler = DurableAtomHandler(
                extract,
                pipeline,
                base.POLICY,
                base.SELF,
                local_only=True,
                index_channel=queue.channel,
                publication_policy=PublicationPolicy(1),
            )
            runner = BoundedWorker(extract, {"memory.extract": handler}, worker_id="retry")
            assert await runner.run_once() == (not committed)
            assert generator.calls == 0
            final = await row_for(engine, scope, "reprocess")
            assert (
                valid_manifest(scope, final)
                and len(final["publication_manifest"]["publication_commit_tokens"]) == 2
            )
            while await indexer.run_once():
                pass
            assert (await status(client, session, target))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_old_backup_of_open_request_cannot_resume_deleted_batches(
    store, monkeypatch, tmp_path, all_in_scope
):
    async def run():
        from test_purge_restore import backup_copy, replay, restorer

        from agent_memory.operations.extraction_worker import ExtractionQueue

        async with store() as (engine, kernel, scope, clock):
            services, _, request_id = await setup(engine, kernel, scope, clock)
            _, _, _, _, _, worker, queue, _ = services
            await interrupt_after_first(engine, scope, worker, monkeypatch)
            row = await row_for(engine, scope, request_id)
            clock[0] += timedelta(seconds=3)
            old_lease = await worker._queue.claim("old", lease_seconds=60)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        memory_ids=(row["event_id"],),
                        all_in_scope=all_in_scope,
                        mode=ForgetMode.ERASE,
                    )
                )
                journal = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), journal)
                clone_queue = ExtractionQueue(
                    backup, scope, row["configuration_sha256"], clock=lambda: clock[0]
                )
                assert (await clone_queue.status(request_id))["status"] == "cancelled"
                with pytest.raises(WorkerQueueError):
                    await clone_queue.checkpoint(
                        old_lease,
                        {"prepared": row["prepared"], "input_manifest": row["input_manifest"]},
                    )
                with pytest.raises(RetentionError):
                    await clone_queue.resume(
                        request_id, expected_manifest_version=1, actor="operator", reason="retry"
                    )
                async with backup.unit_of_work() as uow:
                    clean = await uow.retention_get(scope, "request", request_id)
                    assert not any(
                        k in clean for k in ("prepared", "result", "publication_manifest")
                    )
                    assert not await uow.list_admission_records(scope)
                    assert not await uow.get_source_event(scope, row["event_id"])
                    jobs = await uow.index_jobs(scope, queue.channel.key, 0)
                    assert all(j["status"] == "cancelled" and "applied" not in j for j in jobs)

    asyncio.run(run())


@pytest.mark.parametrize("change", ["lease", "head", "configuration", "prepared"])
def test_partial_request_rechecks_execution_and_semantic_fences(store, monkeypatch, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, _, request_id = await setup(engine, kernel, scope, clock)
            _, _, _, _, generator, worker, _, _ = services
            await interrupt_after_first(engine, scope, worker, monkeypatch)
            before = await row_for(engine, scope, request_id)
            saved = deepcopy(before["publication_manifest"]["publications"])
            clock[0] += timedelta(seconds=3)
            lease = await worker._queue.claim("retry", lease_seconds=60)
            handler = worker._handlers["memory.extract"]
            if change == "configuration":
                generator.version = "changed"
                expected = "processing_configuration_changed"
            elif change == "lease":
                lease = replace(
                    lease, task=replace(lease.task, payload={"fence": before["lease_token"]})
                )
                expected = "lease"
            else:
                async with engine.repository.unit_of_work() as uow:
                    if change == "head":
                        head = await uow.retention_head_get(
                            scope, "interpretation", before["event_id"]
                        )
                        await uow.retention_head_put(
                            scope,
                            "interpretation",
                            before["event_id"],
                            head["payload"],
                            head["generation"],
                        )
                        expected = "interpretation_head_changed"
                    else:
                        row = await uow.retention_get(scope, "request", request_id)
                        row["prepared"]["drafts"][1]["value"] = "changed"
                        await uow.retention_update(scope, request_id, row)
                        expected = "publication_manifest_invalid"
            with pytest.raises((RetentionError, WorkerQueueError), match=expected):
                await handler(lease.task, lambda value: worker._queue.checkpoint(lease, value))
            after = await row_for(engine, scope, request_id)
            assert after["publication_manifest"]["publications"] == saved
            assert not after["publication_manifest"]["closed"]
            assert (
                len(await engine.repository.admission_records(scope)) == 1 and generator.calls == 1
            )

    asyncio.run(run())


@pytest.mark.parametrize("limit", ["resume", "outbox", "manifest"])
def test_bounded_capacity_failure_does_not_publish_or_close_extra_work(store, monkeypatch, limit):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            services, _, request_id = await setup(engine, kernel, scope, clock)
            _, _, _, _, _, worker, queue, _ = services
            extract = worker._queue
            if limit == "outbox":
                cls = type(engine.repository.unit_of_work())
                original = cls.index_jobs

                async def full(uow, *args):
                    jobs = await original(uow, *args)
                    return (*jobs, *({"status": "pending"} for _ in range(128)))

                with monkeypatch.context() as patch:
                    patch.setattr(cls, "index_jobs", full)
                    assert await worker.run_once()
                row = await row_for(engine, scope, request_id)
                assert row["status"] == "retry_wait"
                assert row["last_error_code"] == "index_outbox_capacity"
                assert not row["publication_manifest"]["publication_commit_tokens"]
                assert not await engine.repository.admission_records(scope)
                clock[0] += timedelta(seconds=3)
                assert await worker.run_once()
                assert (await row_for(engine, scope, request_id))["status"] == "completed"
            else:
                extract.max_attempts = 1
                await interrupt_after_first(engine, scope, worker, monkeypatch)
                row = await row_for(engine, scope, request_id)
                if limit == "resume":
                    audit = {
                        "actor": "operator",
                        "reason": "retry",
                        "attempts": 1,
                        "manifest_version": 1,
                        "recorded_at": clock[0].isoformat(),
                    }
                    row["publication_manifest"]["resumptions"] = [audit] * 32
                    assert valid_manifest(scope, row)
                    async with engine.repository.unit_of_work() as uow:
                        await uow.retention_update(scope, request_id, row)
                    with pytest.raises(RetentionError, match="publication_resume_capacity"):
                        await extract.resume(
                            request_id,
                            expected_manifest_version=1,
                            actor="operator",
                            reason="retry",
                        )
                else:
                    row["publication_manifest"]["unexpected_data"] = "x" * 320_000
                    assert not valid_manifest(scope, row)
                assert len(await engine.repository.admission_records(scope)) == 1
                assert not row["publication_manifest"]["closed"]

    asyncio.run(run())


def test_unindexed_host_can_publish_batches_without_claiming_index_readiness(store):
    async def run():
        from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
        from agent_memory.operations.extraction_worker import DurableAtomHandler, ExtractionQueue
        from agent_memory.operations.retention import DurableReceiver
        from agent_memory.operations.worker_runtime import BoundedWorker

        async with store() as (engine, _, scope, clock):
            generator, publication = Generator(), PublicationPolicy(1)
            pipeline = AtomExtractionPipeline(generator, generator)
            config = processing_configuration_sha256(
                pipeline, base.POLICY, base.SELF, publication_policy=publication
            )
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            source = base.source(scope, idempotency="unindexed")
            ticket = await receiver.issue_ticket(
                source, request_id="unindexed", producer_id="host", configuration_sha256=config
            )
            await receiver.submit(
                source, ticket=ticket, producer_id="host", configuration_sha256=config
            )
            queue = ExtractionQueue(engine.repository, scope, config, clock=lambda: clock[0])
            handler = DurableAtomHandler(
                queue,
                pipeline,
                base.POLICY,
                base.SELF,
                local_only=True,
                publication_policy=publication,
            )
            assert await BoundedWorker(
                queue, {"memory.extract": handler}, worker_id="host"
            ).run_once()
            status = await queue.status("unindexed")
            assert status["l1_decided"] and status["index_visible"] == "unsupported"
            row = await row_for(engine, scope, "unindexed")
            assert (
                valid_manifest(scope, row)
                and len(row["publication_manifest"]["publication_commit_tokens"]) == 2
            )
            assert "index_channel" not in row["publication_manifest"]

    asyncio.run(run())
