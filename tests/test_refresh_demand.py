"""Durable exact-set coalescing on SQLite and real PostgreSQL."""

import asyncio
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_observations import setup
from test_durable_purge import envelope

from agent_memory.derived import DerivedError
from agent_memory.operations.refresh_demand import (
    ObservationRefreshProcessor,
    RefreshDemandQueue,
)
from agent_memory.operations.refresh_policy import RefreshPolicy

store = base.store


def scheduler(service, clock, **kwargs):
    return RefreshDemandQueue(
        (ObservationRefreshProcessor(service),), clock=lambda: clock[0], **kwargs
    )


async def finish(queue, lease):
    await queue.apply(lease.task)
    await queue.complete(lease)


def test_fixed_coverage_receipt_coalesces_and_cannot_fake_publication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, legacy, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            one = await queue.request("language", dedupe_key="one", actor="alice")
            two = await queue.request("language", dedupe_key="two", actor="alice")
            lease = await queue.claim("one", lease_seconds=60)
            assert lease is not None
            assert await queue.claim("two", lease_seconds=60) is None
            assert await legacy.claim("legacy", lease_seconds=60) is None
            with pytest.raises(DerivedError, match="refresh_publication_unverified"):
                await queue.complete(lease)
            await finish(queue, lease)
            for receipt in (one, two):
                status = await queue.status(receipt["target_id"], actor="alice")
                assert status["complete"] and status["state"] == "completed"
            assert await queue.claim("idle", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(scope, "definition", "language")
                assert row["dirty"] is False
                assert len(await uow.derived_records(scope, "refresh_execution")) == 1

    asyncio.run(run())


def test_new_dirty_keeps_fixed_receipt_and_successor_legacy_remains_exact(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, legacy, capture = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            old_exact = await legacy.request("language", dedupe_key="old-exact")
            receipt = await queue.request("language", dedupe_key="coverage", actor="alice")
            lease = await queue.claim("one", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            _, session, client, _, _, worker, _, _ = capture
            await client.durable_append(envelope(scope, "1", clock), session, 1)
            assert await worker.run_once()
            # Full failed publication leaves all exact responsibility outstanding.
            with pytest.raises(DerivedError) as error:
                await service.publish(lease.task, snapshot, service.prepare(snapshot))
            await queue.fail(lease, error.value)
            assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            clock[0] += timedelta(seconds=3)
            # A grant is a safety compatibility change; use the no-input withdrawal
            # path so this test isolates ordinary input frontier change.
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                from agent_memory.operations.refresh_demand import record_dirty

                await record_dirty(uow, scope, definition, at=clock[0], reason="extra")
            newer = await queue.claim("two", lease_seconds=60)
            assert newer is not None and newer.task.id != lease.task.id
            assert receipt["target"]["required_frontier"] != {
                "mode": "continuous",
                "positions": {"max": 999},
                "units": [],
            }
            # Source lacking processing grant cannot complete, including via fail.
            with pytest.raises(DerivedError):
                await queue.apply(newer.task)
            assert not (await legacy.status(old_exact["target_id"], actor="alice"))["complete"]
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, "definition", "language"))["dirty"]

    asyncio.run(run())


def test_cold_dirty_waits_for_request_and_hot_debounce_never_resets_first(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            row = await queue.configure("language", RefreshPolicy(mode="on_demand"))
            assert row["status"] == "dirty" and row["due_at"] is None
            assert await queue.claim("cold", lease_seconds=60) is None
            receipt = await queue.request("language", dedupe_key="request", actor="alice")
            await finish(queue, await queue.claim("requested", lease_seconds=60))
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            policy = RefreshPolicy(debounce_seconds=10, max_wait_seconds=12)
            first = await queue.configure("language", policy)
            for _ in range(3):
                clock[0] += timedelta(seconds=3)
                async with engine.repository.unit_of_work() as uow:
                    definition = await uow.derived_get(scope, "definition", "language")
                    from agent_memory.operations.refresh_demand import record_dirty

                    row = await record_dirty(uow, scope, definition, at=clock[0])
                    assert row["first_dirty_at"] == first["first_dirty_at"]
                assert await queue.claim("early", lease_seconds=60) is None
            clock[0] += timedelta(seconds=3)
            assert await queue.claim("due", lease_seconds=60) is not None

    asyncio.run(run())


def test_compatible_full_successor_finishes_finite_coverage_but_not_exact(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived.service import mark_slot_changed

            service, legacy, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            exact = await legacy.request("language", dedupe_key="exact")
            receipt = await queue.request("language", dedupe_key="coverage", actor="alice")
            lease = await queue.claim("first", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                await mark_slot_changed(uow, scope, definition["slots"][0], at=clock[0])
            with pytest.raises(DerivedError) as error:
                await service.publish(lease.task, snapshot, service.prepare(snapshot))
            await queue.fail(lease, error.value)
            clock[0] += timedelta(seconds=3)
            await finish(queue, await queue.claim("successor", lease_seconds=60))
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            assert not (await legacy.status(exact["target_id"], actor="alice"))["complete"]
            async with engine.repository.unit_of_work() as uow:
                assert not (await uow.derived_get(scope, "definition", "language"))["dirty"]

    asyncio.run(run())


def test_unchanged_census_never_consumes_new_unclaimed_dirty(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.operations.refresh_demand import record_dirty

            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            first = await queue.request("language", dedupe_key="first", actor="alice")
            lease = await queue.claim("first", lease_seconds=60)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                await record_dirty(uow, scope, definition, at=clock[0], reason="new_dirty")
            second = await queue.request("language", dedupe_key="second", actor="alice")
            await finish(queue, lease)
            assert (await queue.status(first["target_id"], actor="alice"))["complete"]
            assert not (await queue.status(second["target_id"], actor="alice"))["complete"]
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, "definition", "language"))["dirty"]
            await finish(queue, await queue.claim("second", lease_seconds=60))
            assert (await queue.status(second["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_shared_quota_fair_rotation_and_unsupported_routes(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from dataclasses import replace

            from agent_memory.derived import FacetDefinition, ObservationService
            from agent_memory.operations.refresh_policy import RefreshLimits

            a = ObservationService(
                engine.repository,
                replace(scope, tenant_id="a"),
                base.POLICY,
                clock=lambda: clock[0],
            )
            b = ObservationService(
                engine.repository,
                replace(scope, tenant_id="b"),
                base.POLICY,
                clock=lambda: clock[0],
            )
            for service in (a, b):
                for key in ("first", "second"):
                    await service.register(FacetDefinition(key, "alice"))
            pa, pb = ObservationRefreshProcessor(a), ObservationRefreshProcessor(b)
            limits = RefreshLimits(
                global_running=1,
                tenant_running=1,
                global_pending=2,
                tenant_pending=2,
                instance_pending=1,
            )
            queue = RefreshDemandQueue((pa, pb), limits=limits, clock=lambda: clock[0])
            for processor in (pa, pb):
                for key in ("first", "second"):
                    await queue.configure(key, RefreshPolicy(), processor_key=processor.key)
            async with engine.repository.unit_of_work() as uow:
                usage = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(), tenant_id="a", instance_key="irrelevant"
                )
                assert usage["global_pending"] == 2
            first = await queue.claim("one", lease_seconds=60)
            assert first.task.scope.tenant_id in {"a", "b"}
            assert await queue.claim("two", lease_seconds=60) is None
            await finish(queue, first)
            clock[0] += timedelta(seconds=3)
            # B's deferred demand enters the same admission path when capacity opens.
            second = await queue.claim("two", lease_seconds=60)
            assert second.task.scope.tenant_id != first.task.scope.tenant_id
            await finish(queue, second)
            only_b = RefreshDemandQueue((pb,), limits=limits, clock=lambda: clock[0])
            third = await only_b.claim("only-b", lease_seconds=60)
            assert third is None or third.task.scope.tenant_id == "b"
            built_a = next(
                lease for lease in (first, second) if lease.task.scope.tenant_id == "a"
            ).task.payload["unit"]["facet_id"]
            unserved_a = ({"first", "second"} - {built_a}).pop()
            assert (await a.read(unserved_a, actor="alice"))["state"] != "ready"

    asyncio.run(run())


def test_hot_child_temporarily_pulls_cold_parent_under_shared_budget(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived import FacetDefinition

            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            await service.register(
                FacetDefinition(
                    "child",
                    "alice",
                    template_version="locale-parents/1",
                    parent_facets=("language",),
                )
            )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(mode="on_demand"))
            await queue.configure("child", RefreshPolicy())
            receipt = await queue.request("child", dedupe_key="child", actor="alice")
            assert await queue.claim("child-blocked", lease_seconds=60) is None
            parent = await queue.claim("parent", lease_seconds=60)
            assert parent.task.payload["unit"]["facet_id"] == "language"
            await finish(queue, parent)
            clock[0] += timedelta(seconds=3)
            child = await queue.claim("child", lease_seconds=60)
            assert child.task.payload["unit"]["facet_id"] == "child"
            await finish(queue, child)
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            async with engine.repository.unit_of_work() as uow:
                config = await uow.derived_get(scope, "refresh_policy", "language")
                assert config["policy"]["mode"] == "on_demand"
                demands = await uow.derived_records(scope, "refresh_demand")
                parent_row = next(
                    r["payload"] for r in demands if r["payload"]["facet_id"] == "language"
                )
                assert not parent_row["explicit"] and parent_row["status"] == "idle"

    asyncio.run(run())


def test_heartbeat_does_not_extend_progress_and_expired_fence_cannot_publish(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.operations.worker_tasks import WorkerQueueError

            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(max_no_progress_seconds=12))
            lease = await queue.claim("first", lease_seconds=5)
            snapshot = await service.snapshot(lease.task)
            clock[0] += timedelta(seconds=4)
            await queue.heartbeat(lease, lease_seconds=5)
            async with engine.repository.unit_of_work() as uow:
                demand = (await uow.derived_records(scope, "refresh_demand"))[0]["payload"]
                assert demand["progress_at"] == (clock[0] - timedelta(seconds=4)).isoformat()
            clock[0] += timedelta(seconds=6)
            newer = await queue.claim("recovered", lease_seconds=5)
            assert newer is not None and newer.token != lease.token
            with pytest.raises(WorkerQueueError, match="stale"):
                await service.publish(lease.task, snapshot, service.prepare(snapshot))
            clock[0] += timedelta(seconds=2)
            assert newer.task.lease_expires_at == clock[0]
            with pytest.raises(WorkerQueueError, match="stale"):
                await queue.heartbeat(newer, lease_seconds=5)

    asyncio.run(run())


def test_guarded_hotness_counts_are_bounded_and_erased_receipts_unavailable(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.domain import ForgetMode, ForgetRequest

            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(hotness_window_seconds=5))
            receipt = await queue.request("language", dedupe_key="one", actor="alice")
            await finish(queue, await queue.claim("one", lease_seconds=60))
            for _ in range(3):
                await service.read("language", actor="alice")
            with pytest.raises(DerivedError):
                await service.read("language", actor="bob")
            async with engine.repository.unit_of_work() as uow:
                config = await uow.derived_get(scope, "refresh_policy", "language")
                assert config["stats"]["authorized_reads"] == config["stats"]["valid_reuses"] == 3
            clock[0] += timedelta(seconds=6)
            await service.read("language", actor="alice")
            async with engine.repository.unit_of_work() as uow:
                config = await uow.derived_get(scope, "refresh_policy", "language")
                assert config["previous_window"]["authorized_reads"] == 3
                assert config["stats"]["authorized_reads"] == 1
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            with pytest.raises(DerivedError, match="derived_target_unavailable"):
                await queue.status(receipt["target_id"], actor="alice")
            assert await queue.claim("erased", lease_seconds=60) is None

    asyncio.run(run())


def test_scheduled_timer_and_cold_boundary_preserve_eligibility_without_scan(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(mode="scheduled", schedule_seconds=5))
            assert await queue.claim("before", lease_seconds=60) is None
            clock[0] += timedelta(seconds=5)
            await finish(queue, await queue.claim("due", lease_seconds=60))
            async with engine.repository.unit_of_work() as uow:
                first_head = await uow.derived_get(scope, "head", "language")
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_records

            async def no_definition_census(self, requested_scope, kind):
                assert kind != "definition", "durable due path scanned all definitions"
                return await original(self, requested_scope, kind)

            monkeypatch.setattr(cls, "derived_records", no_definition_census)
            clock[0] += timedelta(seconds=5)
            await finish(queue, await queue.claim("timer", lease_seconds=60))
            async with engine.repository.unit_of_work() as uow:
                next_head = await uow.derived_get(scope, "head", "language")
                assert next_head["unit"] != first_head["unit"]
            # A cold semantic timer only invalidates, never creates a compute lease.
            await queue.configure("language", RefreshPolicy(mode="on_demand"))
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                definition["next_transition_at"] = (clock[0] + timedelta(seconds=2)).isoformat()
                await uow.derived_put(scope, "definition", "language", definition)
                from agent_memory.operations.refresh_demand import record_dirty

                await record_dirty(uow, scope, definition, at=clock[0])
            clock[0] += timedelta(seconds=3)
            assert await queue.claim("cold", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(scope, "definition", "language")
                assert row["dirty"] and row["next_transition_at"] is None

    asyncio.run(run())


def test_failure_budget_partial_output_deadline_and_controlled_recovery(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(max_attempts=1))
            receipt = await queue.request(
                "language", dedupe_key="finite", actor="alice", deadline=clock[0]
            )
            lease = await queue.claim("first", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            invalid = service.prepare(snapshot)
            invalid["manifest_sha256"] = "0" * 64
            with pytest.raises(DerivedError, match="derived_output_invalid") as error:
                await service.publish(lease.task, snapshot, invalid)
            await queue.fail(lease, error.value)
            clock[0] += timedelta(seconds=3)
            status = await queue.status(receipt["target_id"], actor="alice")
            assert (
                not status["complete"] and status["state"] == "dead" and status["deadline_missed"]
            )
            assert await queue.claim("dead", lease_seconds=60) is None
            await queue.request("language", dedupe_key="recover", actor="alice")
            await finish(queue, await queue.claim("recovery", lease_seconds=60))
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_receipt_is_immutable_after_completion_and_worker_contract_fails_closed(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.operations.refresh_demand import supported

            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            receipt = await queue.request("language", dedupe_key="one", actor="alice")
            await finish(queue, await queue.claim("one", lease_seconds=60))
            assert await queue.request("language", dedupe_key="one", actor="alice") == receipt
            assert receipt["target"]["required_frontier"]["mode"] == "exact_units"
            assert receipt["target"]["required_frontier"]["positions"] == {}
            async with engine.repository.unit_of_work() as uow:

                class Partial:
                    def __getattr__(self, key):
                        return None if key == "refresh_scheduler_expired" else getattr(uow, key)

                assert not supported(Partial())

    asyncio.run(run())


def test_aging_progresses_old_low_priority_ahead_of_new_high_priority_burst(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived import FacetDefinition

            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(priority=0, aging_seconds=1))
            clock[0] += timedelta(seconds=15)
            for index in range(8):
                facet_id = f"high-{index}"
                await service.register(FacetDefinition(facet_id, "alice"))
                await queue.configure(facet_id, RefreshPolicy(priority=10, aging_seconds=1))
            # Recreate the coordinator: age and ordering come from durable SQL.
            restarted = scheduler(service, clock)
            lease = await restarted.claim("aged", lease_seconds=60)
            assert lease.task.payload["unit"]["facet_id"] == "language"
            await finish(restarted, lease)
            assert (await service.read("language", actor="alice"))["state"] == "empty"

    asyncio.run(run())
