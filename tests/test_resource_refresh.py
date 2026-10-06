"""Finite refresh receipts, resource exclusivity and transactional progress on both SQL backends."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_durable_purge import envelope

from agent_memory.capture.producer import DurableProducer
from agent_memory.domain import ForgetMode, ForgetRequest, MemoryBlock, Provenance
from agent_memory.lifecycle import LifecycleEvent
from agent_memory.operations.resource_refresh import RefreshDeferred, ResourceRefreshQueue
from agent_memory.operations.retention import DurableReceiver, RetentionError
from agent_memory.operations.worker_runtime import BoundedWorker
from agent_memory.operations.worker_tasks import WorkerQueueError

store = base.store
CONFIG = "a" * 64
DEFINITION = "b" * 64


async def setup(engine, scope, clock, **options):
    receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
    producer = DurableProducer(receiver)
    session = await producer.open(
        scope, producer_id="device", actor="alice", configuration_sha256=CONFIG
    )
    sources = []
    for sequence in range(1, 5):
        result = await producer.append(
            LifecycleEvent.from_dict(
                envelope(scope, str(sequence), clock), trusted_scope=scope
            ).to_memory_event(),
            session,
            sequence=sequence,
            actor="alice",
        )
        sources.append(result["receipt"]["source_event_id"])
    queue = ResourceRefreshQueue(engine.repository, scope, clock=lambda: clock[0], **options)
    return queue, sources, producer, session


async def submit(queue, key, sources, *, resource="view:city", definition=DEFINITION):
    return await queue.submit(
        dedupe_key=key, serialization_key=resource, definition_sha256=definition, units=sources
    )


async def write_output(uow, task):
    # A real SQL output in the same UoW, used to prove rollback/commit ownership.
    await uow.delivery_insert(
        task.scope,
        "target",
        f"refresh-output:{task.id}:{task.payload['generation']}",
        {"units": task.payload["claimed_through"]},
    )
    return task.payload["claimed_through"]


def test_running_new_work_coalesces_and_finite_receipts_do_not_drift(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            first = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("worker-a", lease_seconds=5)
            assert lease.task.payload["claimed_through"] == ["one"]
            later = await submit(q, "second", {"two": [sources[1]], "one": [sources[0]]})
            assert first["resource_id"] == later["resource_id"]
            assert await submit(q, "first", {"one": [sources[0]]}) == first
            assert await q.claim("worker-b", lease_seconds=5) is None
            await q.commit(lease.task, write_output)
            await q.complete(lease)
            assert (await q.status(first["request_id"]))["state"] == "reached"
            partial = await q.status(later["request_id"])
            assert partial["completed_units"] == ["one"] and partial["remaining_units"] == ["two"]
            next_lease = await q.claim("worker-b", lease_seconds=5)
            assert next_lease.task.payload["claimed_through"] == ["two"]
            assert next_lease.task.payload["generation"] == 2
            # Acknowledging the previous commit cannot mutate the active successor.
            await q.fail(lease, RuntimeError("lost acknowledgement"))
            await q.complete(lease)
            await q.commit(next_lease.task, write_output)
            await q.complete(next_lease)
            assert (await q.status(later["request_id"]))["state"] == "reached"
            done = await q.status(first["request_id"])
            assert done["completed_units"] == ["one"] and len(done["commit_tokens"]) == 1
            assert done["commit_tokens"][0]["generation"] == 1
            assert await q.claim("worker-c", lease_seconds=5) is None

    asyncio.run(run())


def test_concurrent_submissions_and_claims_preserve_resource_exclusion(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            peer = ResourceRefreshQueue(engine.repository, scope, clock=lambda: clock[0])
            receipts = await asyncio.gather(
                *[
                    submit(queue, str(i), {str(i): [sources[i]]})
                    for i, queue in enumerate([q, peer, q, peer])
                ]
            )
            assert len({r["resource_id"] for r in receipts}) == 1
            leases = await asyncio.gather(
                q.claim("a", lease_seconds=5), peer.claim("b", lease_seconds=5)
            )
            assert sum(lease is not None for lease in leases) == 1
            lease = next(lease for lease in leases if lease)
            assert lease.task.payload["claimed_through"] == ["0", "1", "2", "3"]
            # Another resource is available despite this live resource lease.
            independent = await submit(
                peer, "other", {"independent": [sources[3]]}, resource="view:other"
            )
            other = await peer.claim("c", lease_seconds=5)
            assert other.task.id == independent["resource_id"]
            await q.commit(lease.task, write_output)
            await peer.commit(other.task, write_output)
            for r in receipts:
                assert (await q.status(r["request_id"]))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["range", "output", "ledger", "expiry"])
def test_output_and_coverage_rollback_together(store, monkeypatch, mutation):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            receipt = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            cls = type(engine.repository.unit_of_work())
            original = cls.refresh_put

            async def broken(self, s, kind, identity, payload):
                await original(self, s, kind, identity, payload)
                if kind == "resource" and payload.get("completed_through"):
                    raise RuntimeError("after ledger write")

            async def write(uow, task):
                result = await write_output(uow, task)
                if mutation == "range":
                    return ["unexpected"]
                if mutation == "output":
                    raise RuntimeError("after output write")
                if mutation == "expiry":
                    clock[0] += timedelta(seconds=6)
                return result

            with monkeypatch.context() as patch:
                if mutation == "ledger":
                    patch.setattr(cls, "refresh_put", broken)
                with pytest.raises((RetentionError, RuntimeError)):
                    await q.commit(lease.task, write)
            status = await q.status(receipt["request_id"])
            assert status["state"] == "processing" and status["completed_units"] == []
            async with engine.repository.unit_of_work() as uow:
                assert await uow.delivery_count(scope, "target") == 0
            if mutation == "expiry":
                lease = await q.claim("recovered", lease_seconds=5)
            await q.commit(lease.task, write_output)
            # Lost response: duplicate commit is side-effect-free.
            await q.commit(lease.task, write_output)
            await q.complete(lease)
            assert (await q.status(receipt["request_id"]))["state"] == "reached"
            async with engine.repository.unit_of_work() as uow:
                assert await uow.delivery_count(scope, "target") == 1

    asyncio.run(run())


def test_expired_lease_fences_writes_and_retries_do_not_expand_claim(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            await submit(q, "one", {"one": [sources[0]]})
            old = await q.claim("a", lease_seconds=5)
            await q.checkpoint(old, {"prepared": "one"})
            await submit(q, "two", {"two": [sources[1]]})
            clock[0] += timedelta(seconds=6)
            newer = await q.claim("b", lease_seconds=5)
            assert newer.task.payload["generation"] == 2
            assert newer.task.payload["claimed_through"] == ["one"]
            assert newer.task.checkpoint == {"prepared": "one"}
            for action in [
                lambda: q.commit(old.task, write_output),
                lambda: q.checkpoint(old, {}),
                lambda: q.complete(old),
                lambda: q.fail(old, RuntimeError()),
                lambda: q.heartbeat(old),
            ]:
                with pytest.raises(WorkerQueueError, match="stale"):
                    await action()
            await q.commit(newer.task, write_output)
            successor = await q.claim("c", lease_seconds=5)
            assert successor.task.payload["claimed_through"] == ["two"]
            assert successor.task.attempts == 1 and successor.task.checkpoint == {}

    asyncio.run(run())


def test_worker_needs_real_commit_and_new_work_is_scheduled_after_handler(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            first = await submit(q, "first", {"one": [sources[0]]})

            async def omitted(task, checkpoint):
                await checkpoint({"prepared": True})

            worker = BoundedWorker(q, {"memory.refresh": omitted}, worker_id="a")
            assert (await worker.run_batch()).failed == 1
            assert (await q.status(first["request_id"]))["state"] == "processing"
            clock[0] += timedelta(seconds=2)
            later = []

            async def handler(task, checkpoint):
                if task.payload["claimed_through"] == ["one"]:
                    later.append(await submit(q, "later", {"two": [sources[1]]}))
                await q.commit(task, write_output)

            worker = BoundedWorker(q, {"memory.refresh": handler}, worker_id="b")
            assert await worker.run_once()
            assert (await q.status(first["request_id"]))["state"] == "reached"
            assert (await q.status(later[0]["request_id"]))["state"] == "processing"
            assert await worker.run_once()
            assert (await q.status(later[0]["request_id"]))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("stop", ["cancel", "erase", "scope", "revise"])
def test_stop_and_source_changes_fence_active_work(store, stop):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            q, sources, producer, session = await setup(engine, scope, clock)
            first = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            if stop == "cancel":
                await q.cancel(first["resource_id"])
            elif stop == "revise":
                await producer.revise(
                    LifecycleEvent.from_dict(
                        envelope(scope, "changed", clock), trusted_scope=scope
                    ).to_memory_event(),
                    session,
                    sequence=5,
                    base_event_id=sources[0],
                    expected_revision=1,
                    actor="alice",
                )
            else:
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        memory_ids=(sources[0],),
                        all_in_scope=stop == "scope",
                        mode=ForgetMode.ERASE,
                    )
                )
            with pytest.raises(WorkerQueueError, match="stale"):
                await q.commit(lease.task, write_output)
            status = await q.status(first["request_id"])
            assert status["state"] == "blocked"
            assert await q.claim("b", lease_seconds=5) is None
            async with engine.repository.unit_of_work() as uow:
                assert await uow.delivery_count(scope, "target") == 0
                if stop in {"erase", "scope"}:
                    record = await uow.refresh_get(scope, "resource", first["resource_id"])
                    assert record["units"] == record["commits"] == record["checkpoint"] == {}
                    assert "lease_token" not in record

    asyncio.run(run())


def test_cancel_preserves_committed_units_but_blocks_remaining_receipt(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            first = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            later = await submit(q, "later", {"two": [sources[1]], "one": [sources[0]]})
            await q.commit(lease.task, write_output)
            next_lease = await q.claim("b", lease_seconds=5)
            await q.cancel(first["resource_id"])
            assert (await q.status(first["request_id"]))["state"] == "reached"
            status = await q.status(later["request_id"])
            assert status["state"] == "blocked" and status["completed_units"] == ["one"]
            assert status["remaining_units"] == ["two"]
            with pytest.raises(WorkerQueueError):
                await q.commit(next_lease.task, write_output)
            with pytest.raises(RetentionError, match="refresh_resource_stopped"):
                await submit(q, "new", {"three": [sources[2]]})

    asyncio.run(run())


def test_deferral_does_not_consume_attempts_or_advance_progress(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock, max_attempts=1)
            receipt = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            await q.checkpoint(lease, {"prepared": True})
            await q.heartbeat(lease, lease_seconds=10)
            assert (await q.status(receipt["request_id"]))["last_committed_progress_at"] is None
            await q.fail(lease, RefreshDeferred(clock[0] + timedelta(seconds=10)))
            assert await q.claim("b", lease_seconds=5) is None
            clock[0] += timedelta(seconds=10)
            next_lease = await q.claim("b", lease_seconds=5)
            assert next_lease.task.attempts == 1 and next_lease.task.checkpoint == {
                "prepared": True
            }
            await q.commit(next_lease.task, write_output)
            assert (await q.status(receipt["request_id"]))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("limit", ["age", "progress", "attempts"])
def test_limits_stop_without_false_coverage(store, limit):
    async def run():
        async with store() as (engine, _, scope, clock):
            options = (
                {"max_attempts": 1}
                if limit == "attempts"
                else {"max_age_seconds" if limit == "age" else "no_progress_seconds": 5}
            )
            q, sources, _, _ = await setup(engine, scope, clock, **options)
            receipt = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            if limit == "attempts":
                await q.fail(lease, RuntimeError("failure"))
            else:
                await q.heartbeat(lease, lease_seconds=60)
                clock[0] += timedelta(seconds=5)
                with pytest.raises(WorkerQueueError):
                    await q.commit(lease.task, write_output)
                # Live leases can no longer be extended when the independent deadline expires.
                with pytest.raises(WorkerQueueError):
                    await q.heartbeat(lease)
                # The wall/progress limit fences a live heartbeat lease immediately.
                assert (await q.status(receipt["request_id"]))["state"] == "failed"
                assert await q.claim("b", lease_seconds=5) is None
            status = await q.status(receipt["request_id"])
            assert status["state"] == "failed" and status["completed_units"] == []

    asyncio.run(run())


def test_input_identity_capacity_and_scope_boundaries(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock, max_active=1)
            first = await submit(q, "first", {"one": [sources[0]]})
            with pytest.raises(RetentionError, match="refresh_idempotency_conflict"):
                await submit(q, "first", {"two": [sources[1]]})
            with pytest.raises(RetentionError, match="refresh_unit_conflict"):
                await submit(q, "second", {"one": [sources[1]]})
            with pytest.raises(RetentionError, match="refresh_definition_conflict"):
                await submit(q, "second", {"one": [sources[0]]}, definition="c" * 64)
            with pytest.raises(RetentionError, match="refresh_active_capacity"):
                await submit(q, "second", {"two": [sources[1]]}, resource="other")
            # Coalescing at capacity does not drop the new receipt.
            await submit(q, "second", {"two": [sources[1]]})
            for invalid in [
                {},
                [],
                {"x": []},
                {"x": [sources[0], sources[0]]},
                {str(i): [sources[0]] for i in range(129)},
            ]:
                with pytest.raises(RetentionError):
                    await submit(q, "invalid", invalid)
            with pytest.raises(RetentionError, match="source_unavailable"):
                await submit(q, "unknown", {"unknown": ["unknown"]})
            wrong = ResourceRefreshQueue(
                engine.repository, replace(scope, user_id="other"), clock=lambda: clock[0]
            )
            with pytest.raises(RetentionError, match="invalid_refresh_request"):
                await wrong.status(first["request_id"])
            lease = await q.claim("a", lease_seconds=5)
            with pytest.raises(WorkerQueueError):
                await wrong.commit(lease.task, write_output)

    asyncio.run(run())


@pytest.mark.parametrize("damage", ["proof", "receipt"])
def test_corrupt_committed_history_is_blocked(store, damage):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            receipt = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            await q.commit(lease.task, write_output)
            async with engine.repository.unit_of_work() as uow:
                await DurableReceiver._check_support(uow, scope)
                if damage == "proof":
                    row = await uow.refresh_get(scope, "resource", receipt["resource_id"])
                    row["commits"][row["unit_commits"]["one"]]["id"] = "damaged"
                    await uow.refresh_put(scope, "resource", receipt["resource_id"], row)
                else:
                    row = await uow.refresh_get(scope, "request", receipt["request_id"])
                    row["units"] = {}
                    await uow.refresh_put(scope, "request", receipt["request_id"], row)
            status = await q.status(receipt["request_id"])
            assert (
                status["state"] == "blocked" and status["reason"] == "refresh_history_unavailable"
            )
            assert "commit_tokens" not in status

    asyncio.run(run())


@pytest.mark.parametrize("phase", ["refresh_before_commit", "refresh_after_commit"])
def test_real_process_kill_keeps_output_coverage_and_successor_atomic(store, tmp_path, phase):
    from test_durable_process_recovery import kill_at_boundary

    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            first = await submit(q, "first", {"one": [sources[0]]})
            await kill_at_boundary(engine, scope, clock, tmp_path, phase)
            committed = phase == "refresh_after_commit"
            later = await submit(q, "later", {"two": [sources[1]]})
            status = await q.status(first["request_id"])
            assert status["state"] == ("reached" if committed else "processing")
            async with engine.repository.unit_of_work() as uow:
                assert (
                    await uow.delivery_get(scope, "target", "refresh-crash-output") is not None
                ) == committed
            if not committed:
                assert await q.claim("a", lease_seconds=5) is None
                clock[0] += timedelta(seconds=6)
                recovered = await q.claim("a", lease_seconds=5)
                assert recovered.task.payload["claimed_through"] == ["one"]
                await q.commit(recovered.task, write_output)
            successor = await q.claim("b", lease_seconds=5)
            assert successor.task.payload["claimed_through"] == ["two"]
            await q.commit(successor.task, write_output)
            assert (await q.status(first["request_id"]))["state"] == "reached"
            assert (await q.status(later["request_id"]))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("after", ["resource", "request"])
def test_submit_rollback_does_not_lose_later_work_or_leave_orphan_receipt(
    store, monkeypatch, after
):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            first = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            cls = type(engine.repository.unit_of_work())
            original = cls.refresh_put

            async def broken(self, scope, kind, identity, payload):
                await original(self, scope, kind, identity, payload)
                if kind == after:
                    raise RuntimeError("submission interrupted")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "refresh_put", broken)
                with pytest.raises(RuntimeError):
                    await submit(q, "later", {"two": [sources[1]]})
            async with engine.repository.unit_of_work() as uow:
                row = await uow.refresh_get(scope, "resource", first["resource_id"])
                assert row["requested_through"] == ["one"]
                assert len(await uow.refresh_records(scope, "request")) == 1
            await q.commit(lease.task, write_output)
            later = await submit(q, "later", {"two": [sources[1]]})
            successor = await q.claim("b", lease_seconds=5)
            assert successor.task.payload["claimed_through"] == ["two"]
            await q.commit(successor.task, write_output)
            assert (await q.status(later["request_id"]))["state"] == "reached"

    asyncio.run(run())


def test_separate_connections_and_reopened_queue_share_exclusion_and_receipts(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            repo = engine.repository
            peer_repo = (
                type(repo).from_dsn(repo.pool.conninfo)
                if hasattr(repo, "pool")
                else type(repo)(repo._path)
            )
            await peer_repo.initialize()
            try:
                peer = ResourceRefreshQueue(peer_repo, scope, clock=lambda: clock[0])
                first = await submit(q, "first", {"one": [sources[0]]})
                lease = await q.claim("a", lease_seconds=5)
                assert await peer.claim("b", lease_seconds=5) is None
                later = await submit(peer, "later", {"two": [sources[1]]})
                await q.commit(lease.task, write_output)
                assert (await peer.status(first["request_id"]))["state"] == "reached"
                successor = await peer.claim("b", lease_seconds=5)
                assert successor.task.payload["claimed_through"] == ["two"]
                await peer.commit(successor.task, write_output)
                assert (await q.status(later["request_id"]))["state"] == "reached"
                # Schema initialization is idempotent over persisted queue state.
                await peer_repo.initialize()
                reopened = ResourceRefreshQueue(peer_repo, scope, clock=lambda: clock[0])
                assert (await reopened.status(first["request_id"]))["state"] == "reached"
                assert await reopened.claim("c", lease_seconds=5) is None
            finally:
                if hasattr(peer_repo, "close"):
                    await peer_repo.close()

    asyncio.run(run())


def test_deletion_refresh_cleanup_failure_rolls_back_source_and_fence(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            receipt = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            if hasattr(engine.repository, "pool"):
                from agent_memory_postgres import refresh as storage

                original = storage.put

                async def broken(*args):
                    await original(*args)
                    raise RuntimeError("cleanup interrupted")
            else:
                from agent_memory.operations import sqlite_refresh as storage

                original = storage.put

                def broken(*args):
                    original(*args)
                    raise RuntimeError("cleanup interrupted")

            with monkeypatch.context() as patch:
                patch.setattr(storage, "put", broken)
                with pytest.raises(RuntimeError):
                    await kernel.forget(
                        ForgetRequest(scope, memory_ids=(sources[0],), mode=ForgetMode.ERASE)
                    )
            assert (await q.status(receipt["request_id"]))["state"] == "processing"
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, sources[0]) is not None
            await q.commit(lease.task, write_output)
            assert (await q.status(receipt["request_id"]))["state"] == "reached"

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["resource", "request"])
def test_history_capacity_rejects_new_work_but_preserves_existing_receipts(
    store, monkeypatch, kind
):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            first = await submit(q, "first", {"one": [sources[0]]})
            cls = type(engine.repository.unit_of_work())
            original = cls.refresh_records

            async def full(self, s, read_kind):
                return (
                    [{}] * (1000 if kind == "resource" else 4096)
                    if read_kind == kind
                    else await original(self, s, read_kind)
                )

            with monkeypatch.context() as patch:
                patch.setattr(cls, "refresh_records", full)
                assert await submit(q, "first", {"one": [sources[0]]}) == first
                with pytest.raises(RetentionError, match=f"refresh_{kind}_capacity"):
                    await submit(q, "later", {"two": [sources[1]]}, resource="new")
            assert (await q.status(first["request_id"]))["state"] == "processing"

    asyncio.run(run())


def test_actual_block_output_keeps_cas_and_source_erasure_contract(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            receipt = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            block = MemoryBlock(
                scope,
                "Inventory",
                "One processed source",
                (sources[0],),
                id="inventory",
                provenance=Provenance(source_event_ids=(sources[0],)),
            )

            async def write(uow, task):
                await uow.save_block(block, expected_version=0)
                return task.payload["claimed_through"]

            await q.commit(lease.task, write)
            stored = await engine.repository.read_block(scope, "inventory")
            assert stored.version == 1 and stored.event_ids == (sources[0],)
            later = await submit(q, "later", {"two": [sources[1]]})
            next_lease = await q.claim("b", lease_seconds=5)
            # Serialization does not replace output CAS: a stale write rolls back coverage.
            with pytest.raises(RuntimeError):
                await q.commit(next_lease.task, write)
            assert (await q.status(later["request_id"]))["completed_units"] == []
            assert (await q.status(receipt["request_id"]))["state"] == "reached"
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(sources[0],), mode=ForgetMode.ERASE)
            )
            assert await engine.repository.read_block(scope, "inventory") is None
            assert (await q.status(receipt["request_id"]))["state"] == "blocked"
            with pytest.raises(WorkerQueueError):
                await q.commit(next_lease.task, write)

    asyncio.run(run())


def test_claim_identity_cannot_be_rebound_and_invalid_deferral_uses_failure_budget(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            receipt = await submit(q, "first", {"one": [sources[0]]})
            lease = await q.claim("a", lease_seconds=5)
            for changes in [
                {"generation": 100},
                {"fence": "wrong"},
                {"claimed_through": []},
                {"units": {"one": [sources[1]]}},
                {"definition_sha256": "c" * 64},
            ]:
                bad = replace(lease.task, payload={**lease.task.payload, **changes})
                with pytest.raises(WorkerQueueError):
                    await q.commit(bad, write_output)
            await q.fail(lease, RefreshDeferred(clock[0] - timedelta(seconds=1)))
            async with engine.repository.unit_of_work() as uow:
                row = await uow.refresh_get(scope, "resource", receipt["resource_id"])
                assert row["attempts"] == 1 and row["status"] == "retry_wait"
            clock[0] += timedelta(seconds=2)
            retried = await q.claim("b", lease_seconds=5)
            assert retried.task.attempts == 2
            await q.commit(retried.task, write_output)

    asyncio.run(run())


def test_additive_schema_upgrade_preserves_existing_sources_and_is_repeatable(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock)
            repo = engine.repository
            # Emulate the previous schema in this isolated fixture database.
            async with repo.unit_of_work() as uow:
                if hasattr(repo, "pool"):
                    await uow.connection.execute("DROP TABLE agent_memory_resource_refresh")
                else:
                    uow.connection.execute("DROP TABLE resource_refresh")
            await repo.initialize()
            await repo.initialize()
            receipt = await submit(q, "first", {"one": [sources[0]]})
            async with repo.unit_of_work() as uow:
                for source in sources:
                    assert await uow.get_source_event(scope, source) is not None
            lease = await q.claim("a", lease_seconds=5)
            await q.commit(lease.task, write_output)
            assert (await q.status(receipt["request_id"]))["state"] == "reached"

    asyncio.run(run())


def test_worker_reports_deferral_separately_from_failure(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            q, sources, _, _ = await setup(engine, scope, clock, max_attempts=1)
            receipt = await submit(q, "first", {"one": [sources[0]]})

            async def deferred(task, checkpoint):
                raise RefreshDeferred(clock[0] + timedelta(seconds=10), "quota")

            worker = BoundedWorker(q, {"memory.refresh": deferred}, worker_id="a")
            result = await worker.run_batch()
            assert result.failed == result.completed == 0 and result.deferred == 1
            assert (await q.status(receipt["request_id"]))["resource_status"] == "deferred"
            clock[0] += timedelta(seconds=10)
            lease = await q.claim("b", lease_seconds=5)
            assert lease.task.attempts == 1
            await q.commit(lease.task, write_output)

    asyncio.run(run())
