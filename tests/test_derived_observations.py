"""Real SQLite/PostgreSQL Observation lifecycle; bounded, targeted stage 12 contracts."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_durable_indexing import service as capture_service
from test_durable_purge import envelope, source_id
from test_publication_batches import Generator

from agent_memory.derived import DerivedError, FacetDefinition, ObservationService, ProcessingGrant
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.facet_refresh import FacetRefreshQueue
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.operations.worker_tasks import WorkerQueueError

store = base.store


async def setup(engine, kernel, scope, clock, *, inputs=1, readers=("alice",)):
    derived = ObservationService(engine.repository, scope, base.POLICY, clock=lambda: clock[0])
    definition = FacetDefinition("language", "alice", readers=readers)
    await derived.register(definition)
    queue = FacetRefreshQueue(derived)
    capture = await capture_service(engine, kernel, scope, clock, generator=Generator())
    _, session, client, _, _, worker, _, _ = capture
    for i in range(1, inputs + 1):
        await client.durable_append(envelope(scope, str(i), clock), session, i)
        assert await worker.run_once()
        clock[0] += timedelta(seconds=1)
        await derived.grant(ProcessingGrant(source_id(scope, str(i)), readers))
    return derived, queue, capture


async def build(queue):
    receipt = await queue.request("language", dedupe_key="first")
    lease = await queue.claim("derived", lease_seconds=60)
    await queue.service.apply(lease.task)
    await queue.complete(lease)
    status = await queue.status(receipt["target_id"], actor="alice")
    assert status["complete"] and status["outcome"] == "applied", status
    return receipt


def test_full_snapshot_edges_and_finite_target(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, capture = await setup(engine, kernel, scope, clock, inputs=2)
            receipt = await build(queue)
            view = await derived.read("language", actor="alice")
            assert view["state"] == "ready" and view["body"]["blocks"][0]["value"] == "zh-CN"
            async with engine.repository.unit_of_work() as uow:
                revision = await uow.derived_get(scope, "revision", view["revision_id"])
                assert len(revision["manifest"]["sources"]) == 2
                assert len(revision["manifest"]["atoms"]) == 2
                for i in (1, 2):
                    assert view["revision_id"] in await uow.derived_reverse(
                        scope, "source:" + source_id(scope, str(i))
                    )
            assert (await queue.request("language", dedupe_key="first")) == receipt
            _, session, client, _, _, worker, _, _ = capture
            await client.durable_append(envelope(scope, "3", clock), session, 3)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            assert (await derived.read("language", actor="alice"))["state"] == "stale"
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_empty_first_subscription_then_member_and_last_delete(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, capture = await setup(engine, kernel, scope, clock, inputs=0)
            first = await build(queue)
            assert (await queue.status(first["target_id"], actor="alice"))["no_outputs"]
            assert (await derived.read("language", actor="alice"))["state"] == "empty"
            _, session, client, _, _, worker, _, _ = capture
            await client.durable_append(envelope(scope, "1", clock), session, 1)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            await derived.grant(ProcessingGrant(source_id(scope, "1"), ("alice",)))
            assert await BoundedWorker(
                queue, {"memory.facet_refresh": derived.apply}, worker_id="next"
            ).run_once()
            assert (await derived.read("language", actor="alice"))["state"] == "ready"
            await kernel.forget(
                ForgetRequest(scope, (source_id(scope, "1"),), mode=ForgetMode.ERASE)
            )
            assert (await derived.read("language", actor="alice"))["body"] is None
            last = await queue.request("language", dedupe_key="last")
            assert await BoundedWorker(
                queue, {"memory.facet_refresh": derived.apply}, worker_id="delete"
            ).run_once()
            status = await queue.status(last["target_id"], actor="alice")
            assert status["complete"] and status["no_outputs"]
            async with engine.repository.unit_of_work() as uow:
                revisions = await uow.derived_records(scope, "revision")
                assert all("body" not in r["payload"] for r in revisions)

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["missing", "private", "revoked", "expired"])
def test_all_processing_inputs_require_host_grants_before_bodies(store, problem, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock, inputs=2)
            key = source_id(scope, "1")
            grant = ProcessingGrant(
                key,
                ("bob",) if problem == "private" else ("alice",),
                revoked=problem == "revoked",
                expires_at=clock[0] if problem == "expired" else None,
            )
            async with engine.repository.unit_of_work() as uow:
                await uow.derived_put(
                    scope,
                    "grant",
                    key,
                    None if problem == "missing" else dict(**grant.payload(), version=2),
                )
            lease = await queue.claim("test", lease_seconds=5)
            cls = type(engine.repository.unit_of_work())
            original = cls.get_source_event
            calls = []

            async def tracked(self, *args):
                calls.append(args)
                return await original(self, *args)

            monkeypatch.setattr(cls, "get_source_event", tracked)
            with pytest.raises(DerivedError, match="derived_processing"):
                await derived.snapshot(lease.task)
            assert not calls
            assert (await derived.read("language", actor="alice"))["body"] is None

    asyncio.run(run())


def test_cas_stale_lease_successor_and_output_validation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            first = await queue.request("language", dedupe_key="first")
            old = await queue.claim("old", lease_seconds=5)
            assert await queue.claim("parallel", lease_seconds=5) is None
            snapshot = await derived.snapshot(old.task)
            prepared = derived.prepare(snapshot)
            corrupt = dict(prepared, no_outputs=True)
            with pytest.raises(DerivedError, match="derived_output_invalid"):
                await derived.publish(old.task, snapshot, corrupt)
            clock[0] += timedelta(seconds=6)
            current = await queue.claim("new", lease_seconds=5)
            with pytest.raises(WorkerQueueError, match="stale"):
                await derived.publish(old.task, snapshot, prepared)
            await derived.apply(current.task)
            await queue.complete(current)
            assert (await queue.status(first["target_id"], actor="alice"))["complete"]
            with pytest.raises(WorkerQueueError):
                await queue.complete(old)
            await derived.grant(
                ProcessingGrant(source_id(scope, "1"), ("alice",), revoked=True), expected_version=1
            )
            assert (await derived.read("language", actor="alice"))["body"] is None

    asyncio.run(run())


def test_scope_and_transport_readonly_history_and_final_guard(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory_sdk import EmbeddedMemoryClient, MemoryClientError

            derived, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            client = EmbeddedMemoryClient(
                kernel, MCPRequestContext(scope, actor="alice"), derived=derived
            )
            assert (await client.derived_capabilities())["readonly"]
            assert len((await client.derived_context("language"))["observations"]) == 1
            with pytest.raises(MemoryClientError, match="derived_history_unsupported"):
                await client.derived_read("language", known_at=clock[0].isoformat())
            with pytest.raises(DerivedError, match="unsupported_derived_operation"):
                await derived.call("grant", {}, MCPRequestContext(scope, actor="alice"))
            with pytest.raises(DerivedError, match="derived_scope_mismatch"):
                await derived.call(
                    "read",
                    {"facet_id": "language"},
                    MCPRequestContext(replace(scope, user_id="bob")),
                )
            with pytest.raises(DerivedError, match="derived_read_denied"):
                await derived.read("language", actor="bob")
            await derived.grant(
                ProcessingGrant(source_id(scope, "1"), ("alice",), revoked=True), expected_version=1
            )
            assert (await client.derived_context("language"))["observations"] == []

    asyncio.run(run())


def test_mutation_during_preparation_leaves_successor_and_fixed_old_target(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, capture = await setup(engine, kernel, scope, clock)
            old_target = await queue.request("language", dedupe_key="old")
            lease = await queue.claim("one", lease_seconds=60)
            snapshot = await derived.snapshot(lease.task)
            _, session, client, _, _, worker, _, _ = capture
            await client.durable_append(envelope(scope, "2", clock), session, 2)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            await derived.grant(ProcessingGrant(source_id(scope, "2"), ("alice",)))
            successor = await queue.request("language", dedupe_key="next")
            assert successor["unit"] != old_target["unit"]
            assert lease.task.payload["unit"] == old_target["unit"]
            with pytest.raises(DerivedError, match="derived_snapshot_changed"):
                await derived.publish(lease.task, snapshot, derived.prepare(snapshot))
            await queue.fail(lease, DerivedError("derived_snapshot_changed"))
            current = await queue.claim("two", lease_seconds=60)
            await derived.apply(current.task)
            await queue.complete(current)
            assert not (await queue.status(old_target["target_id"], actor="alice"))["complete"]
            assert (await queue.status(successor["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


@pytest.mark.parametrize("transition", ["future", "expiry", "grant_expiry"])
def test_time_coverage_without_any_new_write(store, transition):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            boundary = clock[0] + timedelta(seconds=10)
            if transition == "grant_expiry":
                await derived.grant(
                    ProcessingGrant(source_id(scope, "1"), ("alice",), expires_at=boundary),
                    expected_version=1,
                )
            else:
                async with engine.repository.unit_of_work() as uow:
                    rows = await uow.list_admission_records(scope)
                    row = next(r for r in rows if r["payload"]["draft"]["predicate"] == "locale")
                    payload = row["payload"]
                    field = "valid_from" if transition == "future" else "valid_to"
                    payload[field] = payload["claim"][field] = boundary.isoformat()
                    await uow.save_admission_record(
                        scope, row["id"], row["event_id"], row["slot_key"], payload, row["version"]
                    )
                clock[0] += timedelta(seconds=1)
            await build(queue)
            before = await derived.read("language", actor="alice")
            assert before["state"] == ("empty" if transition == "future" else "ready")
            clock[0] = boundary
            after = await derived.read("language", actor="alice")
            assert after["state"] == "stale" and after["body"] is None
            receipt = await queue.request("language", dedupe_key="boundary")
            lease = await queue.claim("boundary", lease_seconds=60)
            if transition == "grant_expiry":
                with pytest.raises(DerivedError, match="derived_processing_grant_expired"):
                    await derived.apply(lease.task)
                assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            else:
                await derived.apply(lease.task)
                assert (await derived.read("language", actor="alice"))["state"] == (
                    "ready" if transition == "future" else "empty"
                )

    asyncio.run(run())


@pytest.mark.parametrize("qualification", ["conditions", "exceptions", "negated"])
def test_qualifiers_never_dropped_to_publish_unconditional_fact(store, qualification):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.list_admission_records(scope)
                row = next(r for r in rows if r["payload"]["draft"]["predicate"] == "locale")
                payload = row["payload"]
                payload["draft"][qualification] = (
                    True if qualification == "negated" else [{"fact": "conditional"}]
                )
                await uow.save_admission_record(
                    scope, row["id"], row["event_id"], row["slot_key"], payload, row["version"]
                )
            clock[0] += timedelta(seconds=1)
            lease = await queue.claim("qualified", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_qualification_unsupported"):
                await derived.apply(lease.task)
            assert (await derived.read("language", actor="alice"))["body"] is None

    asyncio.run(run())


def test_initial_partial_publication_cannot_publish_until_final_closure(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from test_publication_batches import interrupt_after_first

            from agent_memory.operations.publication_batches import PublicationPolicy

            derived = ObservationService(
                engine.repository, scope, base.POLICY, clock=lambda: clock[0]
            )
            await derived.register(FacetDefinition("language", "alice"))
            generator = Generator((("locale", "zh-CN"), ("city", "Hangzhou")))
            _, session, client, _, _, worker, _, _ = await capture_service(
                engine,
                kernel,
                scope,
                clock,
                generator=generator,
                publication_policy=PublicationPolicy(1),
            )
            await client.durable_append(envelope(scope, "1", clock), session, 1)
            await interrupt_after_first(engine, scope, worker, monkeypatch)
            clock[0] += timedelta(seconds=1)
            await derived.grant(ProcessingGrant(source_id(scope, "1"), ("alice",)))
            queue = FacetRefreshQueue(derived)
            target = await queue.request("language", dedupe_key="partial")
            lease = await queue.claim("partial", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_interpretation_incomplete"):
                await derived.snapshot(lease.task)
            await queue.fail(lease, DerivedError("derived_interpretation_incomplete"))
            clock[0] += timedelta(seconds=65)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            closed = await queue.request("language", dedupe_key="closed")
            assert closed["unit"] != target["unit"]
            current = await queue.claim("closed", lease_seconds=60)
            await derived.apply(current.task)
            assert (await derived.read("language", actor="alice"))["state"] == "ready"

    asyncio.run(run())


def test_verified_noop_definition_change_and_failed_publication_rollback(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            old = await derived.read("language", actor="alice")
            fresh = await queue.request("language", dedupe_key="force", force=True)
            lease = await queue.claim("force", lease_seconds=60)
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_put

            async def fail(self, scope, kind, key, row):
                await original(self, scope, kind, key, row)
                if kind == "job" and row["status"] == "completed":
                    raise RuntimeError("publication rollback")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_put", fail)
                with pytest.raises(RuntimeError, match="publication rollback"):
                    await derived.apply(lease.task)
            assert not (await queue.status(fresh["target_id"], actor="alice"))["complete"]
            result = await derived.apply(lease.task)
            assert result["outcome"] == "noop" and result["revision_id"] != old["revision_id"]
            assert (await queue.status(fresh["target_id"], actor="alice"))["complete"]
            updated = replace(FacetDefinition("language", "alice"), version="2")
            await derived.register(updated, expected_generation=1)
            assert (await derived.read("language", actor="alice"))["state"] == "stale"
            assert (await queue.status(fresh["target_id"], actor="alice"))["complete"]
            lease = await queue.claim("definition", lease_seconds=60)
            assert lease.task.payload["unit"]["definition_generation"] == 2
            await derived.apply(lease.task)

    asyncio.run(run())


def test_cas_query_coverage_tampering_is_not_empty_success(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            lease = await queue.claim("tamper", lease_seconds=60)
            snapshot = await derived.snapshot(lease.task)
            snapshot["records"] = []
            snapshot["sources"] = {}
            snapshot["grants"] = {}
            snapshot["manifest"]["atoms"] = {}
            snapshot["manifest"]["sources"] = {}
            with pytest.raises(DerivedError, match="derived_query_coverage_incomplete"):
                await derived.publish(lease.task, snapshot, derived.prepare(snapshot))

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["object", "scope"])
def test_actual_backup_purge_replay_scrubs_derived_history(store, tmp_path, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from test_purge_restore import backup_copy, replay, restorer

            derived, queue, _ = await setup(engine, kernel, scope, clock, inputs=2)
            await build(queue)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                clone = ObservationService(backup, scope, base.POLICY, clock=lambda: clock[0])
                assert (await clone.read("language", actor="alice"))["state"] == "ready"
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        (source_id(scope, "1"),) if mode == "object" else (),
                        all_in_scope=mode == "scope",
                        mode=ForgetMode.ERASE,
                    )
                )
                snapshot = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), snapshot)
                if mode == "scope":
                    with pytest.raises(DerivedError, match="derived_definition_unavailable"):
                        await clone.read("language", actor="alice")
                else:
                    assert (await clone.read("language", actor="alice"))["body"] is None
                async with backup.unit_of_work() as uow:
                    assert all(
                        "body" not in r["payload"] and "manifest" not in r["payload"]
                        for r in await uow.derived_records(scope, "revision")
                    )
                    assert not await uow.derived_reverse(scope, "source:" + source_id(scope, "1"))

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_real_sigkill_atomic_observation_publication(store, tmp_path, boundary):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from test_durable_process_recovery import kill_at_boundary

            derived, queue, _ = await setup(engine, kernel, scope, clock)
            receipt = await queue.request("language", dedupe_key="crash")
            await kill_at_boundary(engine, scope, clock, tmp_path, "derived_" + boundary)
            status = await queue.status(receipt["target_id"], actor="alice")
            assert status["complete"] == (boundary == "after_commit")
            async with engine.repository.unit_of_work() as uow:
                revisions = await uow.derived_records(scope, "revision")
                assert len(revisions) == (1 if boundary == "after_commit" else 0)
            if boundary == "before_commit":
                clock[0] += timedelta(seconds=6)
                lease = await queue.claim("recovery", lease_seconds=60)
                await derived.apply(lease.task)
                await queue.complete(lease)
                assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            else:
                assert await queue.claim("recovery", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "revision")) == 1

    asyncio.run(run())


def test_reprocess_rebuild_and_source_revision_invalidate(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from test_publication_batches import ReplacementGenerator

            from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
            from agent_memory.operations.extraction_worker import (
                DurableAtomHandler,
                ExtractionQueue,
                processing_configuration_sha256,
            )
            from agent_memory.operations.indexing import CandidateIndexChannel
            from agent_memory.operations.reprocessing import ReprocessingService
            from agent_memory.operations.retention import DurableReceiver

            derived, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            source = source_id(scope, "1")
            generator = ReplacementGenerator()
            pipeline = AtomExtractionPipeline(generator, generator)
            channel = CandidateIndexChannel("local")
            config = processing_configuration_sha256(
                pipeline, base.POLICY, base.SELF, index_channel=channel
            )
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            reprocess = ReprocessingService(receiver, producer_id="device", actor="alice")
            old = await reprocess.snapshot(scope, source)
            await reprocess.submit(
                scope,
                source_event_id=source,
                request_id="replace",
                mode="replace_interpretation",
                configuration_sha256=config,
                expected_head_generation=old["generation"],
            )
            extraction = ExtractionQueue(engine.repository, scope, config, clock=lambda: clock[0])
            handler = DurableAtomHandler(
                extraction, pipeline, base.POLICY, base.SELF, local_only=True, index_channel=channel
            )
            assert await BoundedWorker(
                extraction, {"memory.extract": handler}, worker_id="replace"
            ).run_once()
            clock[0] += timedelta(seconds=1)
            assert (await derived.read("language", actor="alice"))["state"] == "stale"
            lease = await queue.claim("rebuild", lease_seconds=60)
            await derived.apply(lease.task)
            assert (await derived.read("language", actor="alice"))["body"]["blocks"][0][
                "value"
            ] == "en-US"
            new = replace(
                base.source(scope, "New language source", identity="new-source"), actor="alice"
            )
            await receiver.revise(
                new,
                base_event_id=source,
                expected_revision=1,
                request_id="revision",
                producer_id="device",
                configuration_sha256=config,
            )
            assert (await derived.read("language", actor="alice"))["body"] is None

    asyncio.run(run())


def test_unrelated_slot_does_not_rebuild_language_and_dependency_types(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            before = await derived.read("language", actor="alice")
            async with engine.repository.unit_of_work() as uow:
                row = next(
                    r
                    for r in await uow.list_admission_records(scope)
                    if r["payload"]["draft"]["predicate"] == "city"
                )
                await uow.save_admission_record(
                    scope,
                    row["id"],
                    row["event_id"],
                    row["slot_key"],
                    row["payload"],
                    row["version"],
                )
            assert (await derived.read("language", actor="alice"))["revision_id"] == before[
                "revision_id"
            ]
            assert await queue.claim("unrelated", lease_seconds=60) is None
            with pytest.raises(DerivedError, match="unsupported_derived_facet"):
                FacetDefinition("persona", "alice", predicates=("city",), facet="persona")
            with pytest.raises(DerivedError, match="derived_subject_scope_mismatch"):
                await derived.register(FacetDefinition("bob", "bob"))

    asyncio.run(run())


def test_snapshot_budget_never_marks_empty_completion_and_backpressure_keeps_dirty(
    store, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_candidates

            async def overflow(self, *args):
                rows = await original(self, *args)
                return tuple(rows[0] for _ in range(65))

            lease = await queue.claim("budget", lease_seconds=60)
            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_candidates", overflow)
                with pytest.raises(DerivedError, match="derived_snapshot_capacity"):
                    await derived.snapshot(lease.task)
            await derived.apply(lease.task)
            second = FacetDefinition("language2", "alice")
            await derived.register(second)
            limited = FacetRefreshQueue(derived, max_active=1)
            first = await limited.request("language", dedupe_key="force", force=True)
            with pytest.raises(DerivedError, match="derived_refresh_backpressure"):
                await limited.request("language2", dedupe_key="second")
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, "definition", "language2"))["dirty"]
            current = await limited.claim("first", lease_seconds=60)
            await derived.apply(current.task)
            next_lease = await limited.claim("second", lease_seconds=60)
            assert next_lease.task.payload["unit"]["facet_id"] == "language2"
            await derived.apply(next_lease.task)
            assert (await limited.status(first["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_readonly_transports_and_final_delivery_recheck(store, transport, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            import agent_memory_mcp as mcp
            import agent_memory_sdk as sdk

            derived, queue, _ = await setup(engine, kernel, scope, clock)
            receipt = await build(queue)
            context = MCPRequestContext(scope, actor="alice")
            original = derived.read
            calls = []

            async def intervening(*args, **kwargs):
                result = await original(*args, **kwargs)
                calls.append(result)
                if len(calls) == 1:
                    await derived.grant(
                        ProcessingGrant(source_id(scope, "1"), ("alice",), revoked=True),
                        expected_version=1,
                    )
                return result

            monkeypatch.setattr(derived, "read", intervening)

            async def exercise(client):
                assert (await client.derived_capabilities())["historical"] is False
                assert (await client.derived_status(receipt["target_id"]))["complete"]
                result = await client.derived_context("language")
                assert not result["observations"] and result["state"] == "invalid"
                with pytest.raises(sdk.MemoryClientError, match="derived_history_unsupported"):
                    await client.derived_read("language", valid_at=clock[0].isoformat())

            if transport == "embedded":
                await exercise(sdk.EmbeddedMemoryClient(kernel, context, derived=derived))
            else:
                server = mcp.create_server(
                    kernel, mcp.StaticIdentityResolver(context), derived=derived
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())


def test_independent_termination_input_permission_and_empty_rebuild(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            rows = await engine.repository.admission_records(scope)
            row = next(r for r in rows if r["payload"]["draft"]["predicate"] == "locale")
            boundary = clock[0] + timedelta(seconds=10)
            event = replace(
                base.source(scope, "Alice stopped preferring Chinese", identity="termination"),
                actor="alice",
            )
            await engine.retract(
                scope,
                row["id"],
                event=event,
                authority=base.SELF,
                policy=base.POLICY,
                expected_version=row["version"],
                valid_to=boundary,
                source_quote=event.content,
            )
            clock[0] += timedelta(seconds=1)
            await derived.grant(ProcessingGrant(event.id, ("alice",)))
            lease = await queue.claim("termination", lease_seconds=60)
            snapshot = await derived.snapshot(lease.task)
            assert snapshot["manifest"]["sources"][event.id]["interpretation"] == {"context": True}
            await derived.publish(lease.task, snapshot, derived.prepare(snapshot))
            assert (await derived.read("language", actor="alice"))["body"]["blocks"][0][
                "valid_to"
            ] == boundary.isoformat()
            clock[0] = boundary
            assert (await derived.read("language", actor="alice"))["state"] == "stale"
            lease = await queue.claim("ended", lease_seconds=60)
            result = await derived.apply(lease.task)
            assert (
                result["no_outputs"]
                and (await derived.read("language", actor="alice"))["state"] == "empty"
            )
            await kernel.forget(ForgetRequest(scope, (event.id,), mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                assert all(
                    "manifest" not in r["payload"]
                    for r in await uow.derived_records(scope, "revision")
                )

    asyncio.run(run())


def test_counterexample_query_membership_rebuild_preserves_conflict(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, capture = await setup(engine, kernel, scope, clock)
            await build(queue)
            # Source 2 is unverified, so it is a counterexample instead of replacing source 1.
            _, session, client, _, generator, worker, _, _ = capture
            generator.values = (("locale", "en-US"),)
            generator.verdict = "uncertain"
            await client.durable_append(envelope(scope, "2", clock), session, 2)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            await derived.grant(ProcessingGrant(source_id(scope, "2"), ("alice",)))
            assert (await derived.read("language", actor="alice"))["state"] == "invalid"
            lease = await queue.claim("counterexample", lease_seconds=60)
            await derived.apply(lease.task)
            view = await derived.read("language", actor="alice")
            # NEEDS_VERIFICATION is not promoted; accepted L1 remains the factual basis.
            assert view["state"] == "ready"
            assert len(view["body"]["blocks"]) == 1
            assert view["body"]["blocks"][0]["value"] == "zh-CN"
            async with engine.repository.unit_of_work() as uow:
                revision = await uow.derived_get(scope, "revision", view["revision_id"])
                assert len(revision["manifest"]["atoms"]) == 2

    asyncio.run(run())


def test_existing_store_upgrade_twice_backfills_query_headers(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, _, _ = await setup(engine, kernel, scope, clock)
            pg = hasattr(engine.repository, "pool")
            tables = ["derived_dependencies", "derived_atom_headers", "derived_entries"]
            async with engine.repository.unit_of_work() as uow:
                for table in tables:
                    if pg:
                        await uow.connection.execute("DROP TABLE agent_memory_" + table)
                    else:
                        uow.connection.execute("DROP TABLE " + table)
            await engine.repository.initialize()
            await engine.repository.initialize()
            await derived.register(FacetDefinition("language", "alice"))
            await derived.grant(ProcessingGrant(source_id(scope, "1"), ("alice",)))
            queue = FacetRefreshQueue(derived)
            await build(queue)
            assert (await derived.read("language", actor="alice"))["state"] == "ready"

    asyncio.run(run())


def test_backend_capability_missing_fails_before_snapshot_and_publication():
    from contextlib import asynccontextmanager

    class MissingBackend:
        @asynccontextmanager
        async def unit_of_work(self):
            yield object()

    async def run():
        service = ObservationService(
            MissingBackend(), base.MemoryScope("test", user_id="alice"), base.POLICY
        )
        with pytest.raises(DerivedError, match="derived_backend_unsupported"):
            await service.register(FacetDefinition("language", "alice"))

    asyncio.run(run())


def test_bounded_retries_and_age_leave_failed_targets_incomplete(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = FacetRefreshQueue(derived, max_attempts=2, max_age_seconds=20)
            receipt = await queue.request("language", dedupe_key="failure")
            for _ in range(2):
                lease = await queue.claim("fail", lease_seconds=5)
                await queue.fail(lease, RuntimeError("private text never goes into diagnostics"))
                clock[0] += timedelta(seconds=5)
            status = await queue.status(receipt["target_id"], actor="alice")
            assert status["state"] == "dead" and not status["complete"]
            second = await queue.request("language", dedupe_key="age", force=True)
            lease = await queue.claim("age", lease_seconds=60)
            clock[0] += timedelta(seconds=21)
            with pytest.raises(WorkerQueueError, match="stale"):
                await derived.snapshot(lease.task)
            assert not (await queue.status(second["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_actual_contested_slot_does_not_return_old_fact_as_current(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, capture = await setup(engine, kernel, scope, clock)
            await build(queue)
            _, session, client, _, generator, worker, _, _ = capture
            generator.values = (("locale", "en-US"),)
            original = generator.generate_atoms

            async def same_boundary(event):
                return [
                    {**row, "valid_from": base.at(1).isoformat()} for row in await original(event)
                ]

            generator.generate_atoms = same_boundary
            await client.durable_append(envelope(scope, "2", clock), session, 2)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            await derived.grant(ProcessingGrant(source_id(scope, "2"), ("alice",)))
            lease = await queue.claim("conflict", lease_seconds=60)
            await derived.apply(lease.task)
            view = await derived.read("language", actor="alice")
            assert view["state"] == "ready" and view["body"]["blocks"][0]["kind"] == "conflict"
            assert all(block["kind"] != "source_fact" for block in view["body"]["blocks"])

    asyncio.run(run())


def test_invalid_completion_token_cannot_attest_coverage(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, queue, _ = await setup(engine, kernel, scope, clock)
            receipt = await build(queue)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(scope, "job", receipt["unit_id"])
                row["commit_token"] = "forged"
                await uow.derived_put(scope, "job", receipt["unit_id"], row)
            assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


@pytest.mark.parametrize("parent", ["observation:self", "atom:unknown", "source:other-scope"])
def test_unknown_self_and_derived_parents_explicitly_rejected(parent):
    from agent_memory.derived.model import validate_edges

    with pytest.raises(DerivedError, match="derived_parent_unsupported"):
        validate_edges(
            [("processing", parent)],
            atom_ids=["candidate"],
            source_event_ids=["source"],
            slot_ids=["slot"],
        )


def test_real_query_limit_reports_incomplete_at_sixty_five_rows(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from copy import deepcopy

            derived, queue, _ = await setup(engine, kernel, scope, clock)
            async with engine.repository.unit_of_work() as uow:
                row = next(
                    r
                    for r in await uow.list_admission_records(scope)
                    if r["payload"]["draft"]["predicate"] == "locale"
                )
                for i in range(64):
                    await uow.save_admission_record(
                        scope,
                        "capacity-" + str(i),
                        row["event_id"],
                        row["slot_key"],
                        deepcopy(row["payload"]),
                        0,
                    )
                assert len(await uow.derived_candidates(scope, [row["slot_key"]])) == 65
            clock[0] += timedelta(seconds=1)
            receipt = await queue.request("language", dedupe_key="overflow")
            lease = await queue.claim("capacity", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_snapshot_capacity"):
                await derived.snapshot(lease.task)
            assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_publish_delete_competition_on_independent_connections(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            lease = await queue.claim("publish", lease_seconds=60)
            pg = hasattr(engine.repository, "pool")
            if pg:
                from agent_memory_postgres.repository import PostgresMemoryRepository

                other = PostgresMemoryRepository.from_dsn(
                    engine.repository.pool.conninfo, max_size=2
                )
            else:
                from agent_memory.sqlite import SQLiteMemoryRepository

                other = SQLiteMemoryRepository(engine.repository._path)
            await other.initialize()
            entered, release, deleting = asyncio.Event(), asyncio.Event(), asyncio.Event()
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_put

            async def paused(self, *args):
                await original(self, *args)
                if self._repository is engine.repository and args[1] == "revision":
                    entered.set()
                    await release.wait()

            async def delete():
                deleting.set()
                return await other.forget(
                    ForgetRequest(scope, (source_id(scope, "1"),), mode=ForgetMode.ERASE)
                )

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "derived_put", paused)
                    publication = asyncio.create_task(derived.apply(lease.task))
                    await asyncio.wait_for(entered.wait(), timeout=10)
                    deletion = asyncio.create_task(delete())
                    await asyncio.wait_for(deleting.wait(), timeout=10)
                    await asyncio.sleep(0.03)
                    assert not deletion.done()
                    release.set()
                    await asyncio.wait_for(publication, timeout=10)
                    await asyncio.wait_for(deletion, timeout=10)
                assert (await derived.read("language", actor="alice"))["body"] is None
                async with other.unit_of_work() as uow:
                    assert all(
                        "body" not in r["payload"]
                        for r in await uow.derived_records(scope, "revision")
                    )
            finally:
                release.set()
                if pg:
                    await other.close()

    asyncio.run(run())


def test_scope_epoch_allows_fresh_target_without_reusing_old_receipt(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock, inputs=0)
            old = await build(queue)
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            await derived.register(FacetDefinition("language", "alice"), expected_generation=1)
            new = await queue.request("language", dedupe_key="first")
            assert old["target_id"] != new["target_id"]
            with pytest.raises(DerivedError, match="derived_target_unavailable"):
                await queue.status(old["target_id"], actor="alice")
            lease = await queue.claim("new", lease_seconds=60)
            await derived.apply(lease.task)
            assert (await queue.status(new["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_multisource_delete_reconciles_conservative_l1_tombstones_for_rebuild(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock, inputs=2)
            await build(queue)
            await kernel.forget(
                ForgetRequest(scope, (source_id(scope, "1"),), mode=ForgetMode.ERASE)
            )
            clock[0] += timedelta(seconds=1)
            lease = await queue.claim("after-delete", lease_seconds=60)
            result = await derived.apply(lease.task)
            assert result["outcome"] == "applied"
            view = await derived.read("language", actor="alice")
            assert view["state"] in {"empty", "ready"}
            assert view["state"] == "empty" or view["body"]["blocks"][0]["value"] == "zh-CN"

    asyncio.run(run())


def test_manifest_shape_and_oversized_serialization_cannot_publish(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            lease = await queue.claim("manifest", lease_seconds=60)
            snapshot = await derived.snapshot(lease.task)
            snapshot["manifest"]["unknown_input"] = "hidden"
            with pytest.raises(DerivedError, match="derived_manifest_invalid"):
                await derived.publish(lease.task, snapshot, derived.prepare(snapshot))
            del snapshot["manifest"]["unknown_input"]
            snapshot["manifest"]["authorization"]["oversized"] = "语言" * 50000
            with pytest.raises(DerivedError, match="derived_manifest_capacity"):
                await derived.publish(lease.task, snapshot, derived.prepare(snapshot))

    asyncio.run(run())


def test_corrupt_body_is_not_delivered_even_when_versions_are_current(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            view = await derived.read("language", actor="alice")
            async with engine.repository.unit_of_work() as uow:
                revision = await uow.derived_get(scope, "revision", view["revision_id"])
                revision["body"]["blocks"][0]["value"] = "injected"
                await uow.derived_put(scope, "revision", view["revision_id"], revision)
            after = await derived.read("language", actor="alice")
            assert after["state"] == "invalid" and after["body"] is None

    asyncio.run(run())


def test_facet_revision_capacity_keeps_current_body_blocked(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            async with engine.repository.unit_of_work() as uow:
                for i in range(127):
                    await uow.derived_put(
                        scope,
                        "revision",
                        "old-" + str(i),
                        {"id": "old-" + str(i), "facet_id": "language", "state": "erased"},
                    )
            receipt = await queue.request("language", dedupe_key="capacity", force=True)
            lease = await queue.claim("capacity", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_revision_capacity"):
                await derived.apply(lease.task)
            assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            assert (await derived.read("language", actor="alice"))["body"] is None

    asyncio.run(run())


def test_first_empty_snapshot_race_cannot_publish_after_member_arrives(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, capture = await setup(engine, kernel, scope, clock, inputs=0)
            lease = await queue.claim("empty", lease_seconds=60)
            snapshot = await derived.snapshot(lease.task)
            assert derived.prepare(snapshot)["no_outputs"]
            _, session, client, _, _, worker, _, _ = capture
            await client.durable_append(envelope(scope, "1", clock), session, 1)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            await derived.grant(ProcessingGrant(source_id(scope, "1"), ("alice",)))
            with pytest.raises(DerivedError, match="derived_snapshot_changed"):
                await derived.publish(lease.task, snapshot, derived.prepare(snapshot))
            await queue.fail(lease, DerivedError("derived_snapshot_changed"))
            next_lease = await queue.claim("member", lease_seconds=60)
            await derived.apply(next_lease.task)
            assert (await derived.read("language", actor="alice"))["state"] == "ready"

    asyncio.run(run())


def test_cross_scope_and_unknown_source_grants_rejected_by_real_storage(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            foreign = base.source(replace(scope, user_id="bob"), identity="foreign")
            async with engine.repository.unit_of_work() as uow:
                await uow.append_event(foreign)
            for key in (foreign.id, "unknown-source"):
                with pytest.raises(DerivedError, match="derived_grant_source_missing"):
                    await derived.grant(ProcessingGrant(key, ("alice",)))

    asyncio.run(run())


def test_actual_permission_intersection_cannot_be_relabelled_by_preparation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            derived, queue, _ = await setup(engine, kernel, scope, clock)
            lease = await queue.claim("permission", lease_seconds=60)
            snapshot = await derived.snapshot(lease.task)
            snapshot["manifest"]["authorization"]["sensitivity"] = "public"
            with pytest.raises(DerivedError, match="derived_manifest_invalid"):
                await derived.publish(lease.task, snapshot, derived.prepare(snapshot))

    asyncio.run(run())
