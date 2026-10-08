"""Clock health, graceful drain, and explicitly measured policy decisions."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import test_atom_admission as base
from test_derived_observations import setup
from test_refresh_demand import scheduler

from agent_memory.derived import DerivedError
from agent_memory.operations.refresh_host import RefreshHost
from agent_memory.operations.refresh_policy import RefreshPolicy, temperature_decision
from agent_memory.operations.worker_tasks import WorkerLimits

store = base.store


def test_policy_due_hysteresis_residency_unknown_cost_and_strict_inputs():
    now = datetime(2026, 10, 1, tzinfo=UTC)
    policy = RefreshPolicy(
        debounce_seconds=10,
        max_wait_seconds=15,
        mode="on_demand",
        hotness_window_seconds=10,
        min_residence_seconds=20,
        minimum_samples=4,
        promote_reuses=3,
        demote_reuses=1,
    )
    assert policy.due(now, now + timedelta(seconds=8)) == now + timedelta(seconds=15)
    assert policy.due(now, now, boundary=now + timedelta(seconds=2)) == now + timedelta(seconds=2)
    assert policy.due(now, now, deadline=now + timedelta(seconds=1)) == now + timedelta(seconds=1)
    stats = dict(
        window_started_at=now.isoformat(),
        changed_at=now.isoformat(),
        authorized_reads=4,
        valid_reuses=3,
        measured_net_work_saved=None,
    )
    later = now + timedelta(seconds=25)
    assert temperature_decision(policy, stats, now=later, budget_available=True) == "on_demand"
    stats["measured_net_work_saved"] = 2
    assert temperature_decision(policy, stats, now=later, budget_available=False) == "on_demand"
    assert (
        temperature_decision(policy, stats, now=now + timedelta(seconds=15), budget_available=True)
        == "on_demand"
    )
    assert temperature_decision(policy, stats, now=later, budget_available=True) == "on_change"
    hot = RefreshPolicy.from_payload({**policy.payload(), "mode": "on_change"})
    stats["valid_reuses"] = 2
    assert temperature_decision(hot, stats, now=later, budget_available=True) == "on_change"
    stats["valid_reuses"] = 1
    assert temperature_decision(hot, stats, now=later, budget_available=True) == "on_demand"
    for invalid in ({"max_wait_seconds": 0}, {"max_attempts": True}, {"mode": "unlimited"}):
        with pytest.raises(DerivedError):
            RefreshPolicy(**invalid)


def test_host_missing_notifications_graceful_stop_and_clock_health(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            receipt = await queue.request("language", dedupe_key="one", actor="alice")
            mono = [0.0]
            host = RefreshHost(
                queue, worker_id="host", poll_seconds=0.01, monotonic=lambda: mono[0]
            )
            started, release = asyncio.Event(), asyncio.Event()
            original = queue.apply

            async def slow(task, checkpoint):
                started.set()
                await release.wait()
                return await original(task, checkpoint)

            queue.apply = slow
            task = asyncio.create_task(host.run())
            await asyncio.wait_for(started.wait(), timeout=3)
            host.stop()
            assert not task.done()  # Stop means drain current publication, no new claim.
            release.set()
            await asyncio.wait_for(task, timeout=3)
            assert host.health.state == "stopped"
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            # Fresh host after a dropped notification reads the durable due index.
            queue.apply = original
            await queue.request("language", dedupe_key="two", actor="alice")
            await queue.initialize()
            fresh = RefreshHost(queue, worker_id="fresh", monotonic=lambda: mono[0])
            assert (await fresh.run_once()).completed == 1
            clock[0] -= timedelta(seconds=10)
            mono[0] += 1
            assert await fresh.run_once() is None
            assert not fresh.health.clock_healthy
            assert fresh.health.reason == "refresh_clock_discontinuity"

    asyncio.run(run())


def test_host_renews_long_work_without_progress_and_claims_only_free_slots(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            heartbeats = []
            original_heartbeat, original_apply = queue.heartbeat, queue.apply

            async def heartbeat(lease, *, lease_seconds):
                heartbeats.append(lease.task.id)
                await original_heartbeat(lease, lease_seconds=lease_seconds)

            async def slow(task, checkpoint):
                await asyncio.sleep(0.05)
                return await original_apply(task, checkpoint)

            queue.heartbeat, queue.apply = heartbeat, slow
            host = RefreshHost(
                queue,
                worker_id="alive",
                heartbeat_seconds=0.01,
                limits=WorkerLimits(max_batch_size=8, max_concurrency=1, lease_seconds=5),
            )
            result = await host.run_once()
            assert result.claimed == result.completed == 1 and len(heartbeats) >= 2

    asyncio.run(run())


def test_recreated_host_uses_durable_clock_bound_before_claim_and_body_access(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=1)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            lease = await queue.claim("before", lease_seconds=60)
            clock[0] += timedelta(seconds=10)
            await queue.heartbeat(lease, lease_seconds=60)
            clock[0] -= timedelta(seconds=5)
            restarted = scheduler(service, clock)
            host = RefreshHost(restarted, worker_id="after-restart")
            assert await host.run_once() is None
            assert not host.health.clock_healthy
            assert host.health.reason == "refresh_clock_discontinuity"
            cls = type(engine.repository.unit_of_work())
            original = cls.get_source_event

            async def forbidden(*args, **kwargs):
                pytest.fail("source body loaded after durable clock rollback")

            monkeypatch.setattr(cls, "get_source_event", forbidden)
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                await service.snapshot(lease.task)
            monkeypatch.setattr(cls, "get_source_event", original)
            clock[0] += timedelta(seconds=5)
            await restarted.apply(lease.task)
            await restarted.complete(lease)

    asyncio.run(run())


def test_heartbeat_after_atomic_publication_does_not_cancel_successful_work(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            committed, renewed = asyncio.Event(), asyncio.Event()
            original_apply, original_heartbeat = queue.apply, queue.heartbeat

            async def apply(task, checkpoint):
                result = await original_apply(task, checkpoint)
                committed.set()
                await renewed.wait()
                return result

            async def heartbeat(lease, *, lease_seconds):
                await committed.wait()
                await original_heartbeat(lease, lease_seconds=lease_seconds)
                renewed.set()

            queue.apply, queue.heartbeat = apply, heartbeat
            host = RefreshHost(queue, worker_id="commit-race", heartbeat_seconds=0.01)
            result = await asyncio.wait_for(host.run_once(), timeout=3)
            assert renewed.is_set()
            assert result.completed == 1 and result.cancelled == 0
            assert (await service.read("language", actor="alice"))["state"] == "empty"

    asyncio.run(run())


def test_host_prefers_completed_work_when_renewal_also_finishes(store, monkeypatch):
    async def run():
        from agent_memory.operations.facet_refresh import stale

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            committed = asyncio.Event()
            original_apply = queue.apply

            async def apply(task, checkpoint):
                result = await original_apply(task, checkpoint)
                committed.set()
                return result

            async def heartbeat(lease, *, lease_seconds):
                await committed.wait()
                raise stale()

            async def both_finished(tasks, *, return_when):
                # Deterministically exercise the legal wait result with both
                # publication and renewal done in one scheduling turn.
                await asyncio.gather(*tasks, return_exceptions=True)
                return set(tasks), set()

            queue.apply, queue.heartbeat = apply, heartbeat
            monkeypatch.setattr(asyncio, "wait", both_finished)
            host = RefreshHost(queue, worker_id="both-finished", heartbeat_seconds=0.01)
            result = await asyncio.wait_for(host.run_once(), timeout=3)
            assert result.completed == 1 and result.cancelled == 0

    asyncio.run(run())


def test_failed_heartbeat_cancels_work_that_has_not_published(store):
    async def run():
        from agent_memory.operations.facet_refresh import stale

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            started, cancelled = asyncio.Event(), asyncio.Event()

            async def apply(task, checkpoint):
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

            async def heartbeat(lease, *, lease_seconds):
                await started.wait()
                raise stale()

            queue.apply, queue.heartbeat = apply, heartbeat
            host = RefreshHost(queue, worker_id="early-failure", heartbeat_seconds=0.01)
            result = await asyncio.wait_for(host.run_once(), timeout=3)
            assert cancelled.is_set() and result.completed == 0
            assert (await service.read("language", actor="alice"))["body"] is None
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "refresh_publication")

    asyncio.run(run())
