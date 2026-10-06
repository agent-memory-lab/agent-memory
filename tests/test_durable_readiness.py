"""Finite readiness, explicit manifest closure and bounded SDK/MCP waits."""

import asyncio
from dataclasses import replace

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
from agent_memory.operations.retention import DurableReceiver, RetentionError
from agent_memory.operations.worker_runtime import BoundedWorker

store = base.store


async def service(engine, kernel, scope, clock):
    sdk = pytest.importorskip("agent_memory_sdk")
    generator = Generator()
    pipeline = AtomExtractionPipeline(generator, generator)
    configuration = processing_configuration_sha256(pipeline, base.POLICY, base.SELF)
    producer = DurableProducer(DurableReceiver(engine.repository, clock=lambda: clock[0]))
    session = await producer.open(
        scope, producer_id="device", actor="alice", configuration_sha256=configuration
    )
    api = DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER)
    client = sdk.EmbeddedMemoryClient(
        kernel, MCPRequestContext(scope, actor="alice"), durable_capture=api
    )
    queue = ExtractionQueue(engine.repository, scope, configuration, clock=lambda: clock[0])
    handler = DurableAtomHandler(queue, pipeline, base.POLICY, base.SELF, local_only=True)
    worker = BoundedWorker(queue, {"memory.extract": handler}, worker_id="worker")
    return producer, session, client, api, generator, worker


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_finite_target_waits_for_all_publications_and_does_not_follow_new_inputs(store, transport):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, embedded, api, _, worker = await service(engine, kernel, scope, clock)

            async def exercise(client):
                await client.durable_append(envelope(scope, "one", clock), session, 1)
                await client.durable_append(envelope(scope, "two", clock), session, 2)
                target = await client.durable_freeze_target(session, [2, 1])
                target_id = target["target_id"]
                assert await client.durable_freeze_target(session, [1, 2]) == target
                initial = await client.durable_readiness(session, target_id)
                assert (
                    initial["state"] == "processing" and not initial["publication_manifest_closed"]
                )
                assert initial["publication_commit_tokens"] == [] and initial["no_outputs"] is None
                assert {t["kind"] for t in initial["capture_commit_tokens"]} == {"capture"}
                assert (
                    await client.durable_wait_until(
                        session, target_id, stage="source_persisted", timeout=0
                    )
                )["state"] == "reached"
                timeout = await client.durable_wait_until(session, target_id, timeout=0)
                assert timeout["state"] == "timed_out" and timeout["last_state"] == "processing"
                # Deadline stops waiting; it has not cancelled either processing request.
                assert await worker.run_once()
                partial = await client.durable_readiness(session, target_id)
                assert (
                    partial["state"] == "processing"
                    and len(partial["publication_commit_tokens"]) == 1
                )
                assert len(partial["uncovered_request_ids"]) == 1
                assert await worker.run_once()
                await client.durable_append(envelope(scope, "later", clock), session, 3)
                reached = await client.durable_wait_until(session, target_id, timeout=0)
                assert reached["state"] == "reached" and reached["publication_manifest_closed"]
                assert len(reached["publication_commit_tokens"]) == 2
                assert {t["kind"] for t in reached["publication_commit_tokens"]} == {"publication"}
                assert not reached["no_outputs"] and not reached["no_indexable_outputs"]
                assert not (await client.durable_status(session, 3))["l1_decided"]
                for stage in ("index_visible", "observation_covered", "view_covered"):
                    assert (
                        await client.durable_wait_until(session, target_id, stage=stage, timeout=0)
                    )["state"] == "unsupported"
                assert await client.durable_freeze_target(session, [1, 2]) == target

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


@pytest.mark.parametrize("outcome", ["empty", "pending", "rejected"])
def test_closed_zero_outputs_and_all_pending_are_distinguished_from_open_empty(store, outcome):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, generator, worker = await service(engine, kernel, scope, clock)
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
            before = await client.durable_readiness(session, target["target_id"])
            assert before["state"] == "processing" and before["no_outputs"] is None
            assert await worker.run_once()
            after = await client.durable_readiness(session, target["target_id"])
            assert after["state"] == "reached" and after["publication_manifest_closed"]
            assert after["no_indexable_outputs"]
            assert after["no_outputs"] == (outcome == "empty")
            assert len(after["publication_commit_tokens"]) == int(outcome != "empty")
            if outcome == "pending":
                assert after["disposition_counts"] == {"PENDING_VERIFICATION": 1}
            if outcome == "rejected":
                assert after["disposition_counts"] == {"REJECT": 1}
            assert (
                await client.durable_readiness(session, target["target_id"], stage="index_visible")
            )["state"] == "unsupported"

    asyncio.run(run())


def test_manifest_closure_rolls_back_with_failed_publication_and_recovers_saved_stage(
    store, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, client, _, generator, worker = await service(
                engine, kernel, scope, clock
            )
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            cls = type(engine.repository.unit_of_work())
            original = cls.retention_update

            async def fail(self, scope, identity, row):
                await original(self, scope, identity, row)
                if row["status"] == "completed":
                    assert row["publication_manifest"]["closed"]
                    raise RuntimeError("publication commit failed")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "retention_update", fail)
                assert await worker.run_once()
            failed = await client.durable_readiness(session, target["target_id"])
            assert failed["state"] == "processing" and not failed["publication_manifest_closed"]
            assert not failed[
                "publication_commit_tokens"
            ] and not await engine.repository.admission_records(scope)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.retention_get(
                    scope, "request", producer._request_key(scope, session, 1)
                )
                row["next_attempt_at"] = clock[0].isoformat()
                await uow.retention_update(scope, row["request_id"], row)
            assert await worker.run_once()
            assert (await client.durable_readiness(session, target["target_id"]))[
                "state"
            ] == "reached"
            assert generator.calls == 1

    asyncio.run(run())


def test_target_scope_producer_identity_and_current_erasure_are_rechecked(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, client, _, _, worker = await service(engine, kernel, scope, clock)
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            target_id = target["target_id"]
            for sequences in ([], [True], [1, 1], [0], [2]):
                with pytest.raises(RetentionError):
                    await producer.freeze_target(scope, session, sequences=sequences, actor="alice")
            with pytest.raises(RetentionError, match="invalid_producer"):
                await producer.readiness(
                    replace(scope, tenant_id="other"),
                    session,
                    target_id=target_id,
                    stage="l1_decided",
                    actor="alice",
                )
            with pytest.raises(RetentionError, match="invalid_readiness_target"):
                await producer.readiness(
                    scope, session, target_id="forged", stage="l1_decided", actor="alice"
                )
            other = await producer.open(
                scope,
                producer_id="other-device",
                actor="alice",
                configuration_sha256=session.configuration_sha256,
            )
            with pytest.raises(RetentionError, match="invalid_readiness_target"):
                await producer.readiness(
                    scope, other, target_id=target_id, stage="l1_decided", actor="alice"
                )
            assert await worker.run_once()
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id(scope, "one"),), mode=ForgetMode.ERASE)
            )
            deleted = await client.durable_readiness(session, target_id)
            assert deleted["state"] == "blocked" and deleted["reason"] == "source_unavailable"
            assert (
                "publication_manifests" not in deleted
                and "publication_commit_tokens" not in deleted
            )
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            with pytest.raises(RetentionError, match="revoked"):
                await producer.readiness(
                    scope, session, target_id=target_id, stage="l1_decided", actor="alice"
                )

    asyncio.run(run())


def test_missing_or_inconsistent_manifest_never_reports_success(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, client, _, _, worker = await service(engine, kernel, scope, clock)
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once()
            async with engine.repository.unit_of_work() as uow:
                row = await uow.retention_get(
                    scope, "request", producer._request_key(scope, session, 1)
                )
                row["publication_manifest"]["publication_commit_tokens"] = []
                await uow.retention_update(scope, row["request_id"], row)
            status = await client.durable_readiness(session, target["target_id"])
            assert (
                status["state"] == "blocked" and status["reason"] == "readiness_history_unavailable"
            )
            async with engine.repository.unit_of_work() as uow:
                row.pop("publication_manifest")
                await uow.retention_update(scope, row["request_id"], row)
            with pytest.raises(RetentionError, match="history_unavailable"):
                await producer.freeze_target(scope, session, sequences=[1], actor="alice")

    asyncio.run(run())


def test_wait_deadline_keeps_target_fixed_and_background_work_running(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, _, worker = await service(engine, kernel, scope, clock)
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert (
                await client.durable_wait_until(
                    session, target["target_id"], timeout=0.02, poll_interval=0.01
                )
            )["state"] == "timed_out"
            assert await worker.run_once()
            assert (await client.durable_wait_until(session, target["target_id"], timeout=1))[
                "state"
            ] == "reached"
            from agent_memory_sdk.durable_readiness import wait_until

            class SlowStatus:
                async def durable_readiness(self, *args, **kwargs):
                    await asyncio.sleep(1)
                    pytest.fail("deadline must stop waiting for a stalled status response")

            stalled = await wait_until(SlowStatus(), session, target["target_id"], timeout=0.01)
            assert stalled["state"] == "timed_out" and stalled["last_state"] is None
            assert (await client.durable_readiness(session, target["target_id"]))[
                "state"
            ] == "reached"
            for timeout in (-1, 31, True, float("nan")):
                with pytest.raises(ValueError):
                    await client.durable_wait_until(session, target["target_id"], timeout=timeout)

    asyncio.run(run())


def test_terminal_processing_failure_is_not_l1_ready(store):
    from datetime import timedelta

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, session, client, _, generator, worker = await service(engine, kernel, scope, clock)
            await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            generator.version = "configuration-changed"
            for _ in range(3):
                assert await worker.run_once()
                clock[0] += timedelta(seconds=3)
            status = await client.durable_wait_until(session, target["target_id"], timeout=0)
            assert status["state"] == "failed" and not status["publication_manifest_closed"]
            assert not status["publication_commit_tokens"]
            assert status["status_version"][0]["attempts"] == 3
            assert generator.calls == 0

    asyncio.run(run())


def test_new_processing_generation_does_not_expand_old_capture_target(store):
    from agent_memory.operations.reprocessing import ReprocessingService

    async def run():
        async with store() as (engine, kernel, scope, clock):
            producer, session, client, _, _, worker = await service(engine, kernel, scope, clock)
            received = await client.durable_append(envelope(scope, "one", clock), session, 1)
            target = await client.durable_freeze_target(session, [1])
            assert await worker.run_once()
            before = await client.durable_readiness(session, target["target_id"])
            assert (
                before["state"] == "reached"
                and before["status_version"][0]["interpretation_current"]
            )
            reprocessing = ReprocessingService(
                producer.receiver, producer_id="device", actor="alice"
            )
            snapshot = await reprocessing.snapshot(scope, received["receipt"]["source_event_id"])
            await reprocessing.submit(
                scope,
                source_event_id=received["receipt"]["source_event_id"],
                request_id="new-generation",
                mode="additive",
                configuration_sha256=session.configuration_sha256,
                expected_head_generation=snapshot["generation"],
            )
            assert await worker.run_once()
            after = await client.durable_readiness(session, target["target_id"])
            assert (
                after["state"] == "reached"
                and not after["status_version"][0]["interpretation_current"]
            )
            assert after["publication_commit_tokens"] == before["publication_commit_tokens"]
            assert after["publication_manifests"] == before["publication_manifests"]
            async with engine.repository.unit_of_work() as uow:
                current = await uow.retention_get(scope, "request", "new-generation")
                assert current["publication_manifest"]["closed"]
                assert current["capture_commit_token"]["generation"] == "new-generation"

    asyncio.run(run())
