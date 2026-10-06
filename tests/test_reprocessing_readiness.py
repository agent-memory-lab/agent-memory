"""Explicit, owner-bound reprocessing targets through embedded SDK and MCP."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest
import test_atom_admission as base
from test_durable_purge import envelope
from test_reprocessing import Adapter

from agent_memory.capture.durable_api import DurableCaptureAPI
from agent_memory.capture.producer import DurableProducer
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.lifecycle import LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.extraction_worker import (
    DurableAtomHandler,
    ExtractionQueue,
    processing_configuration_sha256,
)
from agent_memory.operations.indexing import CandidateIndexChannel, CandidateIndexQueue
from agent_memory.operations.reprocessing import ReprocessingService, contract_fingerprint
from agent_memory.operations.retention import DurableReceiver, RetentionError
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.serialization import to_jsonable

store = base.store


def processing(repository, scope, clock, adapter, channel):
    pipeline = AtomExtractionPipeline(adapter, adapter)
    config = processing_configuration_sha256(
        pipeline, base.POLICY, base.SELF, index_channel=channel
    )
    queue = ExtractionQueue(repository, scope, config, clock=lambda: clock[0])
    handler = DurableAtomHandler(
        queue, pipeline, base.POLICY, base.SELF, local_only=True, index_channel=channel
    )
    worker = BoundedWorker(queue, {"memory.extract": handler}, worker_id="processor")
    return SimpleNamespace(config=config, queue=queue, worker=worker, adapter=adapter)


async def seed(engine, kernel, scope, clock, *, count=1, indexed=True, empty=False):
    sdk = pytest.importorskip("agent_memory_sdk")
    channel = CandidateIndexChannel("local") if indexed else None
    initial = processing(
        engine.repository, scope, clock, Adapter(() if empty else ("Hangzhou",)), channel
    )
    producer = DurableProducer(
        DurableReceiver(engine.repository, clock=lambda: clock[0]), index_channel=channel
    )
    session = await producer.open(
        scope, producer_id="device", actor="alice", configuration_sha256=initial.config
    )
    api = DurableCaptureAPI(producer, trusted_origin=LifecycleOrigin.USER)
    client = sdk.EmbeddedMemoryClient(
        kernel, MCPRequestContext(scope, actor="alice"), durable_capture=api
    )
    index = (
        CandidateIndexQueue(engine.repository, scope, channel, clock=lambda: clock[0])
        if channel
        else None
    )
    indexer = (
        BoundedWorker(index, {"memory.index": index.apply}, worker_id="indexer") if index else None
    )
    sources = []
    for sequence in range(1, count + 1):
        response = await client.durable_append(
            envelope(scope, str(sequence), clock), session, sequence
        )
        sources.append(response["receipt"]["source_event_id"])
        assert await initial.worker.run_once()
        if indexer and not empty:
            assert await indexer.run_once()
    return SimpleNamespace(
        producer=producer,
        session=session,
        api=api,
        client=client,
        sources=sources,
        channel=channel,
        index=index,
        indexer=indexer,
        initial=initial,
        reprocess=ReprocessingService(producer.receiver, producer_id="device", actor="alice"),
    )


async def submit(
    h,
    engine,
    scope,
    clock,
    request_id,
    *,
    source=0,
    adapter=None,
    mode="replace_interpretation",
    allow_pending=False,
):
    work = processing(engine.repository, scope, clock, adapter or Adapter(version="new"), h.channel)
    head = await h.reprocess.snapshot(scope, h.sources[source])
    receipt = await h.reprocess.submit(
        scope,
        source_event_id=h.sources[source],
        request_id=request_id,
        mode=mode,
        configuration_sha256=work.config,
        expected_head_generation=head["generation"],
        allow_pending=allow_pending,
    )
    work.receipt = receipt
    return work


@pytest.mark.parametrize("indexed", [False, True])
@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_fixed_multi_request_target_waits_for_reprocessing_and_selected_index(
    store, indexed, transport
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock, count=2, indexed=indexed)
            capture = await h.client.durable_freeze_target(h.session, [1, 2])
            one = await submit(h, engine, scope, clock, "alpha")
            await submit(h, engine, scope, clock, "zeta", source=1)
            assert one.config != h.session.configuration_sha256

            async def exercise(client):
                contract = await client.durable_contracts(h.session)
                assert contract["reprocessing_targets"]
                assert "durable-target/2" in contract["target_schemas"]
                target = await client.durable_freeze_reprocessing_target(
                    h.session, ["zeta", "alpha"]
                )
                assert (
                    target["schema"] == "durable-target/2"
                    and target["target_kind"] == "reprocessing"
                )
                assert [m["request_id"] for m in target["members"]] == ["alpha", "zeta"]
                assert all("sequence" not in m for m in target["members"])
                assert (
                    await client.durable_freeze_reprocessing_target(h.session, ["alpha", "zeta"])
                    == target
                )
                persisted = await client.durable_readiness(
                    h.session, target["target_id"], stage="source_persisted"
                )
                assert (
                    persisted["state"] == "reached" and not persisted["publication_manifest_closed"]
                )
                assert "capture_commit_tokens" not in persisted
                assert {t["kind"] for t in persisted["processing_commit_tokens"]} == {"processing"}
                timed = await client.durable_wait_until(h.session, target["target_id"], timeout=0)
                assert timed["state"] == "timed_out" and timed["last_state"] == "processing"
                assert one.adapter.generations == 0
                assert (await one.queue.status("alpha"))["status"] == "queued"
                assert await one.worker.run_once()
                partial = await client.durable_readiness(h.session, target["target_id"])
                assert (
                    partial["state"] == "processing"
                    and len(partial["publication_commit_tokens"]) == 1
                )
                assert await one.worker.run_once()
                decided = await client.durable_readiness(h.session, target["target_id"])
                assert (
                    decided["state"] == "reached" and len(decided["publication_commit_tokens"]) == 2
                )
                assert decided["processing_commit_tokens"] == persisted["processing_commit_tokens"]
                assert {
                    t["configuration_sha256"] for t in decided["publication_commit_tokens"]
                } == {one.config}
                if indexed:
                    assert (
                        await client.durable_readiness(
                            h.session, target["target_id"], stage="index_visible"
                        )
                    )["state"] == "processing"
                    assert await h.indexer.run_once()
                    assert (
                        await client.durable_readiness(
                            h.session, target["target_id"], stage="index_visible"
                        )
                    )["state"] == "processing"
                    assert await h.indexer.run_once()
                    visible = await client.durable_wait_until(
                        h.session, target["target_id"], stage="index_visible", timeout=0
                    )
                    assert visible["state"] == "reached" and visible["target_visible_through"] == 4
                else:
                    assert (
                        await client.durable_readiness(
                            h.session, target["target_id"], stage="index_visible"
                        )
                    )["state"] == "unsupported"
                later = await submit(
                    h, engine, scope, clock, "later", adapter=Adapter(version="later")
                )
                assert await later.worker.run_once()
                after = await client.durable_readiness(h.session, target["target_id"])
                assert (
                    after["state"] == "reached"
                    and after["publication_manifests"] == decided["publication_manifests"]
                )
                assert after["publication_commit_tokens"] == decided["publication_commit_tokens"]
                assert not after["status_version"][0]["interpretation_current"]
                if indexed:
                    waiting_for_later = await client.durable_readiness(
                        h.session, target["target_id"], stage="index_visible"
                    )
                    assert waiting_for_later["state"] == "blocked"
                    assert await h.indexer.run_once()
                    after_index = await client.durable_readiness(
                        h.session, target["target_id"], stage="index_visible"
                    )
                    assert (
                        after_index["state"] == "reached"
                        and after_index["target_visible_through"] == 4
                    )
                assert await client.durable_freeze_target(h.session, [1, 2]) == capture
                assert (await client.durable_cursor(h.session))["acked_through"] == 2
                for source in h.sources:
                    async with engine.repository.unit_of_work() as uow:
                        assert await uow.get_source_event(scope, source) is not None

            if transport == "embedded":
                await exercise(h.client)
            else:
                sdk = pytest.importorskip("agent_memory_sdk")
                mcp = pytest.importorskip("agent_memory_mcp")
                server = mcp.create_server(
                    kernel,
                    mcp.StaticIdentityResolver(MCPRequestContext(scope, actor="alice")),
                    durable_capture=h.api,
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())


def test_target_freezing_rejects_unknown_capture_and_foreign_request_ownership(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock)
            await submit(h, engine, scope, clock, "again")
            other = await h.producer.open(
                scope,
                producer_id="other",
                actor="alice",
                configuration_sha256=h.session.configuration_sha256,
            )
            for identities, session in [
                (["missing"], h.session),
                (["again"], other),
                ([h.producer._request_key(scope, h.session, 1)], h.session),
            ]:
                with pytest.raises(RetentionError, match="invalid_reprocessing_target"):
                    await h.producer.freeze_reprocessing_target(
                        scope, session, request_ids=identities, actor="alice"
                    )
            for identities in (
                [],
                ["again", "again"],
                [None],
                [True],
                [""],
                [" "],
                "again",
                ["x"] * 129,
            ):
                with pytest.raises(RetentionError, match="invalid_readiness_target"):
                    await h.producer.freeze_reprocessing_target(
                        scope, h.session, request_ids=identities, actor="alice"
                    )
            payload = {"session": to_jsonable(h.session)}
            payload["request_ids"] = ["again"]
            for context in (
                MCPRequestContext(replace(scope, user_id="mallory"), actor="alice"),
                MCPRequestContext(scope, actor="mallory"),
            ):
                with pytest.raises(RetentionError, match="invalid_producer"):
                    await h.api.call("freeze_reprocessing_target", payload, context)
            with pytest.raises(RetentionError, match="invalid_producer"):
                await h.producer.freeze_reprocessing_target(
                    scope, replace(h.session, token="wrong"), request_ids=["again"], actor="alice"
                )
            async with engine.repository.unit_of_work() as uow:
                assert await uow.delivery_count(scope, "target") == 0

    asyncio.run(run())


@pytest.mark.parametrize(
    "corruption",
    [
        "missing_processing",
        "processing_coordinate",
        "fingerprint",
        "contract",
        "config",
        "source_identity",
        "missing_manifest",
        "manifest",
    ],
)
def test_corrupted_request_binding_blocks_frozen_target_without_exposing_manifests(
    store, corruption
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock, count=2)
            await submit(h, engine, scope, clock, "again")
            target = await h.client.durable_freeze_reprocessing_target(h.session, ["again"])
            async with engine.repository.unit_of_work() as uow:
                await DurableReceiver._check_support(uow, scope)
                row = await uow.retention_get(scope, "request", "again")
                if corruption == "missing_processing":
                    row.pop("processing_commit_token")
                elif corruption == "processing_coordinate":
                    row["processing_commit_token"]["configuration_sha256"] = "a" * 64
                elif corruption == "fingerprint":
                    row["reprocessing_fingerprint"] = "a" * 64
                elif corruption == "contract":
                    row["reprocessing"]["mode"] = "additive"
                elif corruption == "config":
                    row["configuration_sha256"] = "a" * 64
                elif corruption == "source_identity":
                    row["event_id"] = row["reprocessing"]["source_event_id"] = h.sources[1]
                    row["reprocessing_fingerprint"] = contract_fingerprint(row["reprocessing"])
                elif corruption == "missing_manifest":
                    row.pop("publication_manifest")
                else:
                    row["publication_manifest"]["closed"] = True
                await uow.retention_update(scope, "again", row)
            state = await h.client.durable_readiness(h.session, target["target_id"])
            assert (
                state["state"] == "blocked" and state["reason"] == "readiness_history_unavailable"
            )
            assert "publication_manifests" not in state and "processing_commit_tokens" not in state
            if corruption != "source_identity":
                with pytest.raises(RetentionError, match="readiness_history_unavailable"):
                    await h.producer.freeze_reprocessing_target(
                        scope, h.session, request_ids=["again"], actor="alice"
                    )

    asyncio.run(run())


def test_target_insert_rolls_back_and_concurrent_freezes_share_identity(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock)
            await submit(h, engine, scope, clock, "again")
            cls = type(engine.repository.unit_of_work())
            original = cls.delivery_insert

            async def fail(self, *args):
                await original(self, *args)
                raise RuntimeError("after target insertion")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "delivery_insert", fail)
                with pytest.raises(RuntimeError, match="after target insertion"):
                    await h.client.durable_freeze_reprocessing_target(h.session, ["again"])
            async with engine.repository.unit_of_work() as uow:
                assert await uow.delivery_count(scope, "target") == 0
                assert (await uow.retention_get(scope, "request", "again"))["status"] == "queued"
            targets = await asyncio.gather(
                *(
                    h.client.durable_freeze_reprocessing_target(h.session, ["again"])
                    for _ in range(4)
                )
            )
            assert all(t == targets[0] for t in targets)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.delivery_count(scope, "target") == 1

    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["empty", "pending", "needs_resolution", "conflict", "dead"])
def test_reprocessing_outcomes_do_not_claim_new_facts_or_hide_failures(store, outcome):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock, empty=outcome == "empty")
            adapter = Adapter(
                () if outcome == "empty" else ("Hangzhou",),
                verdict="uncertain" if outcome in {"pending", "needs_resolution"} else None,
                version="next",
            )
            work = await submit(
                h,
                engine,
                scope,
                clock,
                "again",
                adapter=adapter,
                allow_pending=outcome == "pending",
            )
            if outcome == "conflict":
                await submit(h, engine, scope, clock, "before", adapter=adapter)
                # "again" sorts first; force its competitor's publication through the lease API.
                first = await work.queue.claim("first", lease_seconds=60)
                second = await work.queue.claim("second", lease_seconds=60)
                handler = DurableAtomHandler(
                    work.queue,
                    AtomExtractionPipeline(adapter, adapter),
                    base.POLICY,
                    base.SELF,
                    local_only=True,
                    index_channel=h.channel,
                )
                await handler(second.task, lambda value: work.queue.checkpoint(second, value))
                await work.queue.fail(first, RetentionError("interpretation_head_changed"))
            target = await h.client.durable_freeze_reprocessing_target(h.session, ["again"])
            if outcome == "dead":
                adapter.version = "changed"
                for _ in range(3):
                    assert await work.worker.run_once()
                    clock[0] += timedelta(seconds=3)
            elif outcome != "conflict":
                assert await work.worker.run_once()
            decided = await h.client.durable_readiness(h.session, target["target_id"])
            if outcome in {"conflict", "dead"}:
                assert decided["state"] == "failed" and not decided["publication_manifest_closed"]
            elif outcome == "needs_resolution":
                assert decided["state"] == "blocked" and not decided["publication_manifest_closed"]
            else:
                assert decided["state"] == "reached" and decided["no_indexable_outputs"]
                assert decided["no_outputs"] == (outcome == "empty")
                if outcome == "pending":
                    assert "PENDING_VERIFICATION" in decided["disposition_counts"]
                    assert await h.indexer.run_once()
                assert (
                    await h.client.durable_readiness(
                        h.session, target["target_id"], stage="index_visible"
                    )
                )["state"] == "reached"
            assert (
                await h.client.durable_readiness(
                    h.session, target["target_id"], stage="source_persisted"
                )
            )["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("change", ["source", "scope", "revision"])
def test_each_status_rechecks_deletion_epoch_and_source_revision(store, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock)
            work = await submit(h, engine, scope, clock, "again")
            target = await h.client.durable_freeze_reprocessing_target(h.session, ["again"])
            assert await work.worker.run_once() and await h.indexer.run_once()
            if change == "revision":
                await h.client.durable_revise(
                    envelope(scope, "revision", clock),
                    h.session,
                    2,
                    base_event_id=h.sources[0],
                    expected_revision=1,
                )
            else:
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        memory_ids=() if change == "scope" else (h.sources[0],),
                        all_in_scope=change == "scope",
                        mode=ForgetMode.ERASE,
                    )
                )
            if change == "scope":
                with pytest.raises(
                    pytest.importorskip("agent_memory_sdk").MemoryClientError,
                    match="producer_revoked",
                ):
                    await h.client.durable_readiness(h.session, target["target_id"])
            else:
                state = await h.client.durable_readiness(h.session, target["target_id"])
                assert state["state"] == "blocked"
                assert state["reason"] == (
                    "source_revision_changed" if change == "revision" else "source_unavailable"
                )
                assert (
                    "publication_manifests" not in state and "processing_commit_tokens" not in state
                )

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["empty", "member", "schema", "scope"])
def test_persisted_target_identity_cannot_be_changed_to_an_empty_or_different_target(
    store, monkeypatch, mutation
):
    from copy import deepcopy

    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock)
            await submit(h, engine, scope, clock, "again")
            target = await h.client.durable_freeze_reprocessing_target(h.session, ["again"])
            cls = type(engine.repository.unit_of_work())
            original = cls.delivery_get

            async def tamper(self, read_scope, kind, identity):
                value = await original(self, read_scope, kind, identity)
                if kind != "target" or identity != target["target_id"]:
                    return value
                value = deepcopy(value)
                if mutation == "empty":
                    value["members"] = []
                elif mutation == "member":
                    value["members"][0]["request_id"] = "other-request"
                elif mutation == "schema":
                    value["schema"] = "durable-target/99"
                else:
                    value["scope_key"] = "other-scope"
                return value

            monkeypatch.setattr(cls, "delivery_get", tamper)
            with pytest.raises(
                pytest.importorskip("agent_memory_sdk").MemoryClientError,
                match="invalid_readiness_target",
            ):
                await h.client.durable_readiness(h.session, target["target_id"])

    asyncio.run(run())


def test_pending_index_update_does_not_repair_a_corrupt_old_completion_proof(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock)
            work = await submit(h, engine, scope, clock, "again")
            target = await h.client.durable_freeze_reprocessing_target(h.session, ["again"])
            assert await work.worker.run_once()
            before = await h.client.durable_readiness(
                h.session, target["target_id"], stage="index_visible"
            )
            assert before["state"] == "processing"
            async with engine.repository.unit_of_work() as uow:
                await DurableReceiver._check_support(uow, scope)
                old = (await uow.index_jobs(scope, h.channel.key, h.session.epoch))[0]
                old["proof"] = "corrupt"
                await uow.index_job_put(scope, old)
            after = await h.client.durable_readiness(
                h.session, target["target_id"], stage="index_visible"
            )
            assert after["state"] == "blocked" and after["reason"] == "index_proof_unavailable"
            assert after["continuous_visible_through"] == 0
            assert await h.indexer.run_once()
            assert (
                await h.client.durable_readiness(
                    h.session, target["target_id"], stage="index_visible"
                )
            )["state"] == "blocked"

    asyncio.run(run())


def test_target_capacity_is_shared_and_existing_target_remains_idempotent(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock)
            first = await h.client.durable_freeze_target(h.session, [1])
            await submit(h, engine, scope, clock, "again")
            cls = type(engine.repository.unit_of_work())
            original = cls.delivery_count

            async def full(self, read_scope, kind):
                return 1000 if kind == "target" else await original(self, read_scope, kind)

            with monkeypatch.context() as patch:
                patch.setattr(cls, "delivery_count", full)
                assert await h.client.durable_freeze_target(h.session, [1]) == first
                with pytest.raises(
                    pytest.importorskip("agent_memory_sdk").MemoryClientError,
                    match="readiness_target_capacity",
                ):
                    await h.client.durable_freeze_reprocessing_target(h.session, ["again"])
            async with engine.repository.unit_of_work() as uow:
                assert await uow.delivery_count(scope, "target") == 1

    asyncio.run(run())


def test_transport_does_not_submit_or_authorize_reprocessing_work(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed(engine, kernel, scope, clock)
            payload = {
                "session": to_jsonable(h.session),
                "request_ids": ["not-submitted"],
                "scope": {"tenant_id": "foreign"},
                "actor": "spoofed",
            }
            with pytest.raises(RetentionError, match="invalid_reprocessing_target"):
                await h.api.call(
                    "freeze_reprocessing_target", payload, MCPRequestContext(scope, actor="alice")
                )
            with pytest.raises(RetentionError, match="unsupported_durable_operation"):
                await h.api.call("reprocess", payload, MCPRequestContext(scope, actor="alice"))
            for malformed in (
                {},
                {"session": to_jsonable(h.session)},
                {"session": to_jsonable(h.session), "request_ids": None},
            ):
                with pytest.raises(RetentionError):
                    await h.api.call(
                        "freeze_reprocessing_target",
                        malformed,
                        MCPRequestContext(scope, actor="alice"),
                    )
            async with engine.repository.unit_of_work() as uow:
                assert await uow.retention_get(scope, "request", "not-submitted") is None
                assert await uow.delivery_count(scope, "target") == 0
            disabled = pytest.importorskip("agent_memory_sdk").EmbeddedMemoryClient(
                kernel, MCPRequestContext(scope, actor="alice")
            )
            with pytest.raises(
                pytest.importorskip("agent_memory_sdk").MemoryClientError,
                match="durable capture is not enabled",
            ):
                await disabled.durable_freeze_reprocessing_target(h.session, ["not-submitted"])

    asyncio.run(run())
