"""Explicit repair and new stream baselines preserve source and finite target boundaries."""

import asyncio
from dataclasses import replace

import pytest
import test_atom_admission as base
from test_durable_indexing import service, status
from test_durable_purge import envelope, source_id

from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.index_recovery import (
    CandidateIndexRecovery,
    active_stream,
    physical_channel,
)
from agent_memory.operations.indexing import digest, verified
from agent_memory.operations.retention import DurableReceiver, RetentionError
from agent_memory.operations.worker_tasks import WorkerQueueError

store = base.store


async def seed(engine, kernel, scope, clock, *, count=2, indexed=True):
    h = await service(engine, kernel, scope, clock)
    producer, session, client, api, generator, worker, queue, indexer = h
    for sequence in range(1, count + 1):
        await client.durable_append(envelope(scope, str(sequence), clock), session, sequence)
        assert await worker.run_once()
        if indexed:
            assert await indexer.run_once()
    recovery = CandidateIndexRecovery(
        engine.repository, scope, queue.channel, actor="operator", clock=lambda: clock[0]
    )
    return h, recovery


async def jobs(engine, scope, queue):
    async with engine.repository.unit_of_work() as uow:
        stream = await active_stream(uow, scope, queue.channel)
        return await uow.index_jobs(scope, physical_channel(queue.channel, stream), stream["epoch"])


async def repair(recovery, publication_id, *, key="repair"):
    snap = await recovery.inspect(publication_id)
    options = dict(
        recovery_id=key,
        expected_job_sha256=snap["job_sha256"],
        stream=snap["stream"],
        reason="operator-repair",
    )
    return await recovery.repair(publication_id, **options), options


@pytest.mark.parametrize("damage", ["dead", "proof", "locator", "cancelled", "dispositions"])
def test_explicit_repair_reexecutes_actual_projection_without_skipping_position(store, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock)
            _, session, client, _, _, _, queue, _ = h
            target = await client.durable_freeze_target(session, [2])
            before = (await jobs(engine, scope, queue))[0]
            publication = before["token"]["id"]
            async with engine.repository.unit_of_work() as uow:
                await DurableReceiver._check_support(uow, scope)
                row = await uow.index_job_get(scope, queue.channel.key, session.epoch, publication)
                if damage in {"dead", "cancelled"}:
                    row["status"] = damage
                elif damage == "proof":
                    row["proof"] = "corrupt"
                elif damage == "locator":
                    await uow.index_document_put(
                        scope, queue.channel.key, row["dispositions"][0]["candidate_id"], None
                    )
                else:
                    row["dispositions"] = []
                await uow.index_job_put(scope, row)
            assert (await status(client, session, target))["state"] != "reached"
            receipt, options = await repair(recovery, publication)
            assert receipt["sequence"] == 1 and receipt["stream"]["generation"] == 0
            assert await recovery.repair(publication, **options) == receipt
            with pytest.raises(RetentionError, match="index_recovery_idempotency_conflict"):
                await recovery.repair(publication, **{**options, "reason": "changed"})
            visible = await status(client, session, target)
            assert visible["state"] == "reached" and visible["continuous_visible_through"] == 2
            after = (await jobs(engine, scope, queue))[0]
            assert after["token"] == before["token"] and after["sequence"] == before["sequence"]
            assert after["repair_id"] == receipt["id"] and digest(after) == receipt["after_sha256"]
            async with engine.repository.unit_of_work() as uow:
                assert await verified(uow, scope, after)
            with pytest.raises(RetentionError, match="index_repair_not_needed"):
                await repair(recovery, publication, key="unneeded")

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["active", "cas", "sequence", "binding", "source", "revision"])
def test_repair_does_not_bypass_fencing_or_authority(store, boundary):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(
                engine, kernel, scope, clock, count=1, indexed=boundary != "active"
            )
            producer, session, client, _, _, _, queue, _ = h
            row = (await jobs(engine, scope, queue))[0]
            publication = row["token"]["id"]
            snap = await recovery.inspect(publication)
            if boundary == "active":
                expected = "index_repair_active_job"
            elif boundary == "source":
                await kernel.forget(
                    ForgetRequest(scope, memory_ids=(source_id(scope, "1"),), mode=ForgetMode.ERASE)
                )
                expected = "index_publication_conflict"
            elif boundary == "revision":
                await client.durable_revise(
                    envelope(scope, "revision", clock),
                    session,
                    2,
                    base_event_id=source_id(scope, "1"),
                    expected_revision=1,
                )
                expected = "source_revision_changed"
            else:
                async with engine.repository.unit_of_work() as uow:
                    await DurableReceiver._check_support(uow, scope)
                    changed = await uow.index_job_get(
                        scope, queue.channel.key, session.epoch, publication
                    )
                    if boundary == "cas":
                        changed["status"] = "dead"
                    elif boundary == "sequence":
                        changed["sequence"] = 99
                    else:
                        changed["token"] = {**changed["token"], "generation": "unknown"}
                    await uow.index_job_put(scope, changed)
                expected = {
                    "cas": "index_repair_head_changed",
                    "sequence": "index_sequence_conflict",
                    "binding": "index_publication_conflict",
                }[boundary]
                if boundary != "cas":
                    snap = await recovery.inspect(publication)
            with pytest.raises(RetentionError, match=expected):
                await recovery.repair(
                    publication,
                    recovery_id="unsafe",
                    stream=snap["stream"],
                    expected_job_sha256=snap["job_sha256"],
                    reason="operator",
                )
            async with engine.repository.unit_of_work() as uow:
                assert (
                    await uow.index_recovery_count(
                        scope, queue.channel.key, session.epoch, "repair"
                    )
                    == 0
                )

    asyncio.run(run())


@pytest.mark.parametrize("broken", ["erased", "revision", "missing_job", "bad_proof"])
def test_rollover_rebuilds_from_authority_and_old_targets_keep_original_stream(store, broken):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock)
            _, session, client, _, _, worker, queue, indexer = h
            old_target = await client.durable_freeze_target(session, [2])
            original_jobs = await jobs(engine, scope, queue)
            if broken == "erased":
                await kernel.forget(
                    ForgetRequest(scope, memory_ids=(source_id(scope, "1"),), mode=ForgetMode.ERASE)
                )
            elif broken == "revision":
                await client.durable_revise(
                    envelope(scope, "revision", clock),
                    session,
                    3,
                    base_event_id=source_id(scope, "1"),
                    expected_revision=1,
                )
                assert await worker.run_once()
            else:
                async with engine.repository.unit_of_work() as uow:
                    await DurableReceiver._check_support(uow, scope)
                    if broken == "bad_proof":
                        row = original_jobs[0]
                        row["proof"] = "corrupt"
                        await uow.index_job_put(scope, row)
                    elif hasattr(engine.repository, "pool"):
                        await uow.connection.execute(
                            "DELETE FROM agent_memory_index_jobs WHERE token_id=%s",
                            (original_jobs[0]["token"]["id"],),
                        )
                    else:
                        uow.connection.execute(
                            "DELETE FROM index_jobs WHERE token_id=?",
                            (original_jobs[0]["token"]["id"],),
                        )
            assert (await status(client, session, old_target))["state"] != "reached"
            receipt = await recovery.rollover(
                recovery_id="new-space", expected_generation=0, reason="unrecoverable-prefix"
            )
            assert (
                receipt["stream"]["generation"] == 1 and receipt["stream"]["epoch"] == session.epoch
            )
            assert (
                await recovery.rollover(
                    recovery_id="new-space", expected_generation=0, reason="unrecoverable-prefix"
                )
                == receipt
            )
            fresh = await client.durable_freeze_target(session, [2])
            assert (
                fresh["target_id"] != old_target["target_id"]
                and fresh["index_stream"] == receipt["stream"]
            )
            assert (await status(client, session, fresh))["state"] == "reached"
            old = await status(client, session, old_target)
            assert old["state"] == "blocked" and old["reason"] == "index_stream_retired"
            assert old["index_stream"]["generation"] == 0 and not old["index_stream_current"]
            assert (
                old["publication_commit_tokens"]
                == (await status(client, session, fresh))["publication_commit_tokens"]
            )
            # New publications keep the extraction config and capture epoch.
            next_sequence = 4 if broken == "revision" else 3
            await client.durable_append(envelope(scope, "later", clock), session, next_sequence)
            assert await worker.run_once() and await indexer.run_once()
            later = await client.durable_freeze_target(session, [next_sequence])
            assert later["index_stream"] == receipt["stream"]
            assert (await status(client, session, later))["state"] == "reached"
            assert (await client.durable_cursor(session))["acked_through"] == next_sequence
            async with engine.repository.unit_of_work() as uow:
                saved = await uow.index_jobs(scope, queue.channel.key, session.epoch)
                assert saved[0]["token"] in [j["token"] for j in original_jobs]
                if broken == "erased":
                    assert saved[0]["status"] == "cancelled" and "applied" not in saved[0]
                if broken == "bad_proof":
                    assert saved[0]["proof"] == "corrupt"

    asyncio.run(run())


def test_rollover_fences_old_lease_and_open_target_without_drifting_publication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock, count=1, indexed=False)
            _, session, client, _, _, worker, queue, indexer = h
            old = await client.durable_freeze_target(session, [1])
            lease = await queue.claim("old", lease_seconds=60)
            await client.durable_append(envelope(scope, "pending", clock), session, 2)
            open_old = await client.durable_freeze_target(session, [2])
            receipt = await recovery.rollover(
                recovery_id="switch", expected_generation=0, reason="operator"
            )
            for action in [
                lambda: queue.apply(lease.task, None),
                lambda: queue.complete(lease),
                lambda: queue.fail(lease, RuntimeError()),
            ]:
                with pytest.raises(WorkerQueueError) as error:
                    await action()
                assert error.value.code == "stale_lease"
            assert (await status(client, session, old))["state"] == "blocked"
            fresh = await client.durable_freeze_target(session, [1])
            assert (await status(client, session, fresh))["state"] == "reached"
            assert await worker.run_once() and await indexer.run_once()
            assert (await status(client, session, open_old))["reason"] == "index_stream_retired"
            open_new = await client.durable_freeze_target(session, [2])
            assert open_new["index_stream"] == receipt["stream"]
            assert (await status(client, session, open_new))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["repair", "rollover"])
@pytest.mark.parametrize("failure", ["document", "ledger", "activation"])
def test_recovery_writes_and_activation_rollback_atomically(store, monkeypatch, operation, failure):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock, count=1)
            _, session, client, _, _, _, queue, _ = h
            target = await client.durable_freeze_target(session, [1])
            pub = (await jobs(engine, scope, queue))[0]["token"]["id"]
            async with engine.repository.unit_of_work() as uow:
                await DurableReceiver._check_support(uow, scope)
                row = await uow.index_job_get(scope, queue.channel.key, session.epoch, pub)
                row["status"] = "dead"
                await uow.index_document_put(
                    scope, queue.channel.key, row["dispositions"][0]["candidate_id"], None
                )
                await uow.index_job_put(scope, row)
            snap = await recovery.inspect(pub)
            cls = type(engine.repository.unit_of_work())
            name = {
                "document": "index_document_put",
                "ledger": "index_job_put",
                "activation": "index_recovery_put",
            }[failure]
            original = getattr(cls, name)

            async def broken(self, *args):
                await original(self, *args)
                if failure != "activation" or operation == "repair" or args[3] == "head":
                    raise RuntimeError("recovery interrupted")

            with monkeypatch.context() as patch:
                patch.setattr(cls, name, broken)
                with pytest.raises(RuntimeError):
                    if operation == "repair":
                        await recovery.repair(
                            pub,
                            recovery_id="repair",
                            expected_job_sha256=snap["job_sha256"],
                            stream=snap["stream"],
                            reason="operator",
                        )
                    else:
                        await recovery.rollover(
                            recovery_id="switch", expected_generation=0, reason="operator"
                        )
            assert (await status(client, session, target))["state"] == "failed"
            async with engine.repository.unit_of_work() as uow:
                assert (await active_stream(uow, scope, queue.channel))["generation"] == 0
                assert (
                    await uow.index_recovery_count(
                        scope, queue.channel.key, session.epoch, "repair"
                    )
                    == 0
                )
                assert (
                    await uow.index_recovery_count(
                        scope, queue.channel.key, session.epoch, "stream"
                    )
                    == 0
                )
            if operation == "repair":
                await repair(recovery, pub)
                assert (await status(client, session, target))["state"] == "reached"
            else:
                await recovery.rollover(
                    recovery_id="switch", expected_generation=0, reason="operator"
                )
                fresh = await client.durable_freeze_target(session, [1])
                assert (await status(client, session, fresh))["state"] == "reached"

    asyncio.run(run())


def test_concurrent_rollovers_compare_head_and_duplicate_recovery_identity(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock, count=1)
            results = await asyncio.gather(
                *[
                    recovery.rollover(recovery_id="same", expected_generation=0, reason="operator")
                    for _ in range(4)
                ]
            )
            assert all(r == results[0] for r in results)
            with pytest.raises(RetentionError, match="index_stream_head_changed"):
                await recovery.rollover(
                    recovery_id="other", expected_generation=0, reason="operator"
                )
            with pytest.raises(RetentionError, match="index_recovery_idempotency_conflict"):
                await recovery.rollover(
                    recovery_id="same", expected_generation=0, reason="different"
                )
            next_stream = await recovery.rollover(
                recovery_id="next", expected_generation=1, reason="operator"
            )
            assert next_stream["stream"]["generation"] == 2
            assert results[0]["stream"]["generation"] == 1
            # An old recovery receipt remains historical rather than selecting the moving head.
            assert (
                await recovery.rollover(
                    recovery_id="same", expected_generation=0, reason="operator"
                )
                == results[0]
            )

    asyncio.run(run())


def test_operator_recovery_is_not_a_model_dispatch_or_scope_bypass(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock, count=1)
            _, session, _, api, _, _, queue, _ = h
            for op in ["index_repair", "index_rollover"]:
                with pytest.raises(RetentionError, match="unsupported_durable_operation"):
                    await api.call(
                        op,
                        {
                            "session": {
                                "producer_id": session.producer_id,
                                "epoch": session.epoch,
                                "token": session.token,
                                "configuration_sha256": session.configuration_sha256,
                            }
                        },
                        MCPRequestContext(scope, actor="alice"),
                    )
            wrong = CandidateIndexRecovery(
                engine.repository, replace(scope, user_id="other"), queue.channel, actor="operator"
            )
            pub = (await jobs(engine, scope, queue))[0]["token"]["id"]
            with pytest.raises(RetentionError, match="index_publication_missing"):
                await wrong.inspect(pub)

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["repair", "rollover"])
@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_real_recovery_process_kill_preserves_projection_proof_and_head(
    store, tmp_path, operation, boundary
):
    from test_durable_process_recovery import kill_at_boundary

    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock, count=1)
            _, session, client, _, _, _, queue, _ = h
            target = await client.durable_freeze_target(session, [1])
            row = (await jobs(engine, scope, queue))[0]
            publication = row["token"]["id"]
            async with engine.repository.unit_of_work() as uow:
                await DurableReceiver._check_support(uow, scope)
                row["status"] = "dead"
                await uow.index_document_put(
                    scope, queue.channel.key, row["dispositions"][0]["candidate_id"], None
                )
                await uow.index_job_put(scope, row)
            await kill_at_boundary(
                engine, scope, clock, tmp_path, f"recovery_{operation}_{boundary}"
            )
            committed = boundary == "after_commit"
            async with engine.repository.unit_of_work() as uow:
                stream = await active_stream(uow, scope, queue.channel)
                assert stream["generation"] == int(operation == "rollover" and committed)
                assert await uow.index_recovery_count(
                    scope, queue.channel.key, session.epoch, operation
                ) == int(committed)
            if not committed:
                assert (await status(client, session, target))["state"] == "failed"
                if operation == "repair":
                    await repair(recovery, publication, key="crash")
                else:
                    await recovery.rollover(
                        recovery_id="crash", expected_generation=0, reason="operator"
                    )
            fresh = await client.durable_freeze_target(session, [1])
            assert (await status(client, session, fresh))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("damage", ["source", "scope", "head", "stream"])
def test_rollover_readiness_and_locator_recheck_deletion_and_stream_integrity(store, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock, count=1)
            _, session, client, _, _, _, queue, _ = h
            receipt = await recovery.rollover(
                recovery_id="new", expected_generation=0, reason="operator"
            )
            fresh = await client.durable_freeze_target(session, [1])
            row = (await jobs(engine, scope, queue))[0]
            async with engine.repository.unit_of_work() as uow:
                candidate = await uow.get_admission_record(
                    scope, row["dispositions"][0]["candidate_id"]
                )
            assert len(await queue.lookup(candidate["slot_key"])) == 1
            if damage in {"source", "scope"}:
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        memory_ids=(source_id(scope, "1"),),
                        all_in_scope=damage == "scope",
                        mode=ForgetMode.ERASE,
                    )
                )
                if damage == "source":
                    assert (await status(client, session, fresh))["state"] == "blocked"
                else:
                    with pytest.raises(Exception, match="producer_revoked"):
                        await status(client, session, fresh)
                assert await queue.lookup(candidate["slot_key"]) == ()
                async with engine.repository.unit_of_work() as uow:
                    for key in [
                        queue.channel.key,
                        physical_channel(queue.channel, receipt["stream"]),
                    ]:
                        old = await uow.index_job_get(scope, key, session.epoch, row["token"]["id"])
                        assert old["status"] == "cancelled" and "applied" not in old
                return
            async with engine.repository.unit_of_work() as uow:
                await DurableReceiver._check_support(uow, scope)
                if damage == "head":
                    kind, identity, payload = "head", "head", {"stream": {"generation": 99}}
                else:
                    kind, identity, payload = (
                        "stream",
                        receipt["stream"]["id"],
                        {"status": "active", "stream": {}},
                    )
                await uow.index_recovery_put(
                    scope, queue.channel.key, session.epoch, kind, identity, payload
                )
            result = await status(client, session, fresh)
            assert (
                result["state"] == "blocked"
                and result["reason"] == "index_stream_history_unavailable"
            )
            with pytest.raises(RetentionError, match="index_stream_history_unavailable"):
                await queue.lookup(candidate["slot_key"])

    asyncio.run(run())


def test_completed_legacy_target_and_empty_baseline_remain_honest(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock, count=1)
            _, session, client, _, _, _, queue, _ = h
            old = await client.durable_freeze_target(session, [1])
            await recovery.rollover(recovery_id="first", expected_generation=0, reason="operator")
            historical = await status(client, session, old)
            assert historical["state"] == "reached" and not historical["index_stream_current"]
            assert old["index_channel"] == queue.channel.payload() and "index_stream" not in old
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source_id(scope, "1"),), mode=ForgetMode.ERASE)
            )
            empty = await recovery.rollover(
                recovery_id="empty", expected_generation=1, reason="operator"
            )
            assert empty["baseline_count"] == 0 and (await jobs(engine, scope, queue)) == ()
            assert (await status(client, session, old))["state"] == "blocked"

    asyncio.run(run())


@pytest.mark.parametrize("limit", ["repairs", "baseline", "generation"])
def test_recovery_capacity_does_not_leave_partial_activation(store, monkeypatch, limit):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, recovery = await seed(engine, kernel, scope, clock, count=1)
            _, session, client, _, _, _, queue, _ = h
            cls = type(engine.repository.unit_of_work())
            if limit == "repairs":
                row = (await jobs(engine, scope, queue))[0]
                async with engine.repository.unit_of_work() as uow:
                    row["status"] = "dead"
                    await uow.index_job_put(scope, row)
                original = cls.index_recovery_count

                async def full(self, scope, channel, epoch, kind):
                    return (
                        1000
                        if kind == "repair"
                        else await original(self, scope, channel, epoch, kind)
                    )

                with monkeypatch.context() as patch:
                    patch.setattr(cls, "index_recovery_count", full)
                    with pytest.raises(RetentionError, match="index_repair_capacity"):
                        await repair(recovery, row["token"]["id"])
            elif limit == "baseline":
                original = cls.retention_requests

                async def overflow(self, scope):
                    rows = await original(self, scope)
                    return rows * 257

                with monkeypatch.context() as patch:
                    patch.setattr(cls, "retention_requests", overflow)
                    with pytest.raises(RetentionError, match="index_rollover_capacity"):
                        await recovery.rollover(
                            recovery_id="full", expected_generation=0, reason="operator"
                        )
            else:
                with pytest.raises(RetentionError, match="invalid_index_stream_generation"):
                    await recovery.rollover(
                        recovery_id="full", expected_generation=32, reason="operator"
                    )
            async with engine.repository.unit_of_work() as uow:
                assert (await active_stream(uow, scope, queue.channel))["generation"] == 0
                assert (
                    await uow.index_recovery_count(
                        scope, queue.channel.key, session.epoch, "stream"
                    )
                    == 0
                )

    asyncio.run(run())


def test_reprocessing_target_keeps_processing_tokens_and_explicit_new_stream_binding(store):
    from test_reprocessing_readiness import seed as seed_processing
    from test_reprocessing_readiness import submit as submit_processing

    async def run():
        async with store() as (engine, kernel, scope, clock):
            h = await seed_processing(engine, kernel, scope, clock)
            work = await submit_processing(h, engine, scope, clock, "new-interpretation")
            old = await h.client.durable_freeze_reprocessing_target(
                h.session, ["new-interpretation"]
            )
            recovery = CandidateIndexRecovery(
                engine.repository, scope, h.channel, actor="operator", clock=lambda: clock[0]
            )
            receipt = await recovery.rollover(
                recovery_id="switch", expected_generation=0, reason="operator"
            )
            assert await work.worker.run_once()
            fresh = await h.client.durable_freeze_reprocessing_target(
                h.session, ["new-interpretation"]
            )
            assert (
                fresh["schema"] == "durable-target/2" and fresh["index_stream"] == receipt["stream"]
            )
            old_l1 = await h.client.durable_readiness(h.session, old["target_id"])
            new_l1 = await h.client.durable_readiness(h.session, fresh["target_id"])
            assert old_l1["processing_commit_tokens"] == new_l1["processing_commit_tokens"]
            assert (await status(h.client, h.session, old))["reason"] == "index_stream_retired"
            assert (await status(h.client, h.session, fresh))["state"] == "processing"
            assert await h.indexer.run_once()
            assert (await status(h.client, h.session, fresh))["state"] == "reached"

    asyncio.run(run())


def test_separate_repository_rollover_fences_owner_lease_and_preserves_new_head(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            h, _ = await seed(engine, kernel, scope, clock, count=1, indexed=False)
            _, session, client, _, _, _, queue, _ = h
            repo = engine.repository
            peer = (
                type(repo).from_dsn(repo.pool.conninfo)
                if hasattr(repo, "pool")
                else type(repo)(repo._path)
            )
            await peer.initialize()
            try:
                lease = await queue.claim("old-connection", lease_seconds=60)
                recovery = CandidateIndexRecovery(
                    peer, scope, queue.channel, actor="operator", clock=lambda: clock[0]
                )
                switched = await recovery.rollover(
                    recovery_id="peer", expected_generation=0, reason="operator"
                )
                with pytest.raises(WorkerQueueError) as error:
                    await queue.apply(lease.task, None)
                assert error.value.code == "stale_lease"
                fresh = await client.durable_freeze_target(session, [1])
                assert fresh["index_stream"] == switched["stream"]
                assert (await status(client, session, fresh))["state"] == "reached"
                await peer.initialize()
                async with repo.unit_of_work() as uow:
                    assert await active_stream(uow, scope, queue.channel) == switched["stream"]
            finally:
                if hasattr(peer, "close"):
                    await peer.close()

    asyncio.run(run())
