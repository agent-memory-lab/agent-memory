import asyncio
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_observations import setup
from test_durable_purge import source_id
from test_refresh_demand import finish, scheduler

from agent_memory.derived import DerivedError, ProcessingGrant
from agent_memory.operations.refresh_policy import RefreshPolicy

store = base.store


def test_rollback_cannot_revive_expired_managed_head(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=1)
            start = clock[0]
            await service.grant(
                ProcessingGrant(
                    source_id(scope, "1"), ("alice",), expires_at=start + timedelta(seconds=10)
                ),
                expected_version=1,
            )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            await finish(queue, await queue.claim("build", lease_seconds=60))
            clock[0] = start + timedelta(seconds=20)
            await queue.initialize()
            assert (await service.read("language", actor="alice"))["state"] == "stale"
            clock[0] = start + timedelta(seconds=2)
            try:
                result = await service.read("language", actor="alice")
            except DerivedError as error:
                assert error.code == "refresh_clock_discontinuity"
            else:
                assert result["state"] not in {"ready", "empty"}, result

    asyncio.run(run())


@pytest.mark.parametrize("provider_delay", [0.0, 0.15], ids=["normal", "slow-provider"])
def test_restarting_stopped_host_catches_up_after_initial_clock_rollback(
    store, monkeypatch, provider_delay
):
    async def run():
        from agent_memory.operations.refresh_host import RefreshHost

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            receipt = await queue.request("language", dedupe_key="pending", actor="alice")
            high = clock[0] + timedelta(seconds=2)
            clock[0] = high
            await queue.initialize()
            cls = type(engine.repository.unit_of_work())
            original_due = cls.refresh_scheduler_due
            due_calls = []

            async def delayed_due(uow, **kwargs):
                due_calls.append(clock[0])
                # A healthy provider can legitimately exceed the former 100 ms
                # completion assumption. Keep real claim/publication behavior.
                if provider_delay:
                    await asyncio.sleep(provider_delay)
                return await original_due(uow, **kwargs)

            monkeypatch.setattr(cls, "refresh_scheduler_due", delayed_due)
            host = RefreshHost(queue, worker_id="host", poll_seconds=0.01)
            host.stop()
            clock[0] = high - timedelta(seconds=1)
            running = asyncio.create_task(host.run())
            phase, status = "initial rollback", None
            try:
                # Observe the rejected initialization before advancing the fake
                # wall clock. A fixed sleep could skip this coverage on a busy CI.
                async with asyncio.timeout(5):
                    while host.health.state != "degraded":
                        if running.done():
                            await running  # Surface unexpected host exceptions immediately.
                            pytest.fail("refresh host exited before observing rollback")
                        await asyncio.sleep(0.01)
                assert not host.health.clock_healthy
                assert host.health.reason == "refresh_clock_discontinuity"
                assert queue.stopping
                assert due_calls == []  # No claim may reach the provider during rollback.
                status = await queue.status(receipt["target_id"], actor="alice")
                assert not status["complete"]

                # Completion depends on several real provider transactions, not
                # host health or a fixed wall-time latency. Keep the durable clock
                # exactly at its high-water mark while the existing host catches up.
                phase = "durable completion after catch-up"
                clock[0] = high
                async with asyncio.timeout(5):
                    while True:
                        if running.done():
                            await running
                            pytest.fail("refresh host exited before completing receipt")
                        status = await queue.status(receipt["target_id"], actor="alice")
                        if status["complete"]:
                            break
                        await asyncio.sleep(0.01)
                assert status["state"] == "completed"
                assert status["covered"] == sorted(receipt["target"]["required_frontier"]["units"])
                assert due_calls and all(at == high for at in due_calls)
                assert host.health.state == "running" and host.health.clock_healthy
                assert not queue.stopping
            except TimeoutError:
                pytest.fail(
                    f"timed out waiting for {phase}: status={status!r}, "
                    f"health={host.health!r}, stopping={queue.stopping}"
                )
            finally:
                host.stop()
                await asyncio.wait_for(running, 5)
            assert host.health.state == "stopped" and queue.stopping

    asyncio.run(run())


def test_clock_rollback_during_current_read_cannot_revive_expired_body(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=1)
            start = clock[0]
            await service.grant(
                ProcessingGrant(
                    source_id(scope, "1"), ("alice",), expires_at=start + timedelta(seconds=10)
                ),
                expected_version=1,
            )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            await finish(queue, await queue.claim("build", lease_seconds=60))
            clock[0] = start + timedelta(seconds=20)
            await queue.initialize()
            original = service._unit

            async def rollback_after_metadata(uow, definition):
                unit = await original(uow, definition)
                clock[0] = start + timedelta(seconds=2)
                return unit

            monkeypatch.setattr(service, "_unit", rollback_after_metadata)
            result = await service.read("language", actor="alice")
            assert result["state"] == "invalid" and result["body"] is None
            assert result["reason"] == "refresh_clock_discontinuity"

    asyncio.run(run())


def test_snapshot_cannot_use_lower_clock_after_initial_guard(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=1)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            lease = await queue.claim("snapshot", lease_seconds=60)
            clock[0] += timedelta(seconds=20)
            await queue.initialize()
            original = service._check_unit

            async def rollback_after_metadata(uow, unit):
                definition = await original(uow, unit)
                clock[0] -= timedelta(seconds=18)
                return definition

            async def forbidden(*args, **kwargs):
                pytest.fail("source body loaded after the managed clock guard was undercut")

            monkeypatch.setattr(service, "_check_unit", rollback_after_metadata)
            monkeypatch.setattr(
                type(engine.repository.unit_of_work()), "get_source_event", forbidden
            )
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                await service.snapshot(lease.task)

    asyncio.run(run())


def test_forward_clock_at_read_delivery_removes_expired_body(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=1)
            start = clock[0]
            await service.grant(
                ProcessingGrant(
                    source_id(scope, "1"), ("alice",), expires_at=start + timedelta(seconds=10)
                ),
                expected_version=1,
            )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            await finish(queue, await queue.claim("build", lease_seconds=60))
            cls = type(engine.repository.unit_of_work())
            original = cls.get_admission_record

            async def advance_after_body(uow, *args, **kwargs):
                row = await original(uow, *args, **kwargs)
                clock[0] = start + timedelta(seconds=10)
                return row

            monkeypatch.setattr(cls, "get_admission_record", advance_after_body)
            result = await service.read("language", actor="alice")
            assert result["state"] == "stale" and result["body"] is None
            assert result["reason"] == "time_coverage_expired"
            clock[0] = start + timedelta(seconds=1)
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                await service.read("language", actor="alice")

    asyncio.run(run())


def test_failed_authority_read_does_not_roll_back_observed_expiry(store):
    async def run():
        import pytest
        from test_derived_controls import configured

        async with store() as (engine, kernel, scope, clock):
            service, _, _, authority, _, _ = await configured(engine, kernel, scope, clock)
            start = clock[0]
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            await finish(queue, await queue.claim("build", lease_seconds=60))
            clock[0] = authority.expires_at
            with pytest.raises(DerivedError, match="derived_authority_expired"):
                await service.read("language", actor="alice")
            clock[0] = start + timedelta(seconds=1)
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                await service.read("language", actor="alice")

    asyncio.run(run())


def test_unmanaged_page_of_managed_parent_rechecks_clock_after_materialization(store, monkeypatch):
    async def run():
        import pytest
        from test_derived_pages import page
        from test_derived_parents import publish

        import agent_memory.derived.pages as pages

        async with store() as (engine, kernel, scope, clock):
            service, legacy, _ = await setup(engine, kernel, scope, clock, inputs=1)
            start = clock[0]
            await service.grant(
                ProcessingGrant(
                    source_id(scope, "1"), ("alice",), expires_at=start + timedelta(seconds=10)
                ),
                expected_version=1,
            )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            await finish(queue, await queue.claim("parent", lease_seconds=60))
            await service.register_page(page(scope))
            await publish(service, legacy, "language-page")
            original = pages.materialized_body

            def advance_after_materialization(*args):
                body = original(*args)
                clock[0] = start + timedelta(seconds=10)
                return body

            monkeypatch.setattr(pages, "materialized_body", advance_after_materialization)
            result = await service.pages.read("language-page", actor="alice")
            assert result["state"] == "stale" and result["body"] is None
            assert result["reason"] == "time_coverage_expired"
            clock[0] = start + timedelta(seconds=1)
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                await service.pages.read("language-page", actor="alice")

    asyncio.run(run())


def test_snapshot_rechecks_each_source_authorization_before_loading_body(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=2)
            start = clock[0]
            for key in ("1", "2"):
                await service.grant(
                    ProcessingGrant(
                        source_id(scope, key), ("alice",), expires_at=start + timedelta(seconds=10)
                    ),
                    expected_version=1,
                )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            lease = await queue.claim("snapshot", lease_seconds=60)
            cls = type(engine.repository.unit_of_work())
            original, loaded = cls.get_source_event, []

            async def expire_after_first_source(uow, scope, key):
                loaded.append(key)
                assert len(loaded) == 1, "second source loaded after its permission expired"
                source = await original(uow, scope, key)
                clock[0] = start + timedelta(seconds=10)
                return source

            monkeypatch.setattr(cls, "get_source_event", expire_after_first_source)
            with pytest.raises(DerivedError, match="derived_processing_grant_expired"):
                await service.snapshot(lease.task)
            assert len(loaded) == 1

    asyncio.run(run())


@pytest.mark.parametrize("seconds", [10, 60])
def test_managed_commit_rechecks_time_and_lease_before_publication(store, monkeypatch, seconds):
    async def run():
        import agent_memory.operations.refresh_demand as demand
        from agent_memory.operations.worker_tasks import WorkerQueueError

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=1)
            start = clock[0]
            await service.grant(
                ProcessingGrant(
                    source_id(scope, "1"), ("alice",), expires_at=start + timedelta(seconds=10)
                ),
                expected_version=1,
            )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            receipt = await queue.request("language", dedupe_key="commit", actor="alice")
            lease = await queue.claim("build", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            original = demand.publish_coverage
            error = DerivedError if seconds == 10 else WorkerQueueError
            reason = "derived_time_coverage_expired" if seconds == 10 else "stale"

            async def advance_at_commit(*args, _seconds=seconds, **kwargs):
                clock[0] = start + timedelta(seconds=_seconds)
                return await original(*args, **kwargs)

            monkeypatch.setattr(demand, "publish_coverage", advance_at_commit)
            with pytest.raises(error, match=reason):
                await service.publish(lease.task, snapshot, service.prepare(snapshot))
            async with engine.repository.unit_of_work() as uow:
                for kind in ("refresh_publication", "revision", "head"):
                    assert not await uow.derived_records(scope, kind)
                row = (await uow.derived_records(scope, "refresh_demand"))[0]["payload"]
                assert row["active_execution"] == lease.task.payload["refresh_execution"]
                assert set(row["requested"]) == set(receipt["target"]["required_frontier"]["units"])

    asyncio.run(run())


@pytest.mark.parametrize("phase", ["snapshot", "publish"])
def test_direct_failed_attempt_keeps_observed_expiry_floor(store, phase):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=1)
            start = clock[0]
            await service.grant(
                ProcessingGrant(
                    source_id(scope, "1"), ("alice",), expires_at=start + timedelta(seconds=10)
                ),
                expected_version=1,
            )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            lease = await queue.claim("build", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)

            async def invoke():
                if phase == "snapshot":
                    return await service.snapshot(lease.task)
                return await service.publish(lease.task, snapshot, service.prepare(snapshot))

            clock[0] = start + timedelta(seconds=20)
            with pytest.raises(DerivedError):
                await invoke()
            clock[0] = start + timedelta(seconds=2)
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                await invoke()

    asyncio.run(run())


@pytest.mark.parametrize("phase", ["snapshot", "publish"])
def test_direct_midflight_expiry_failure_keeps_clock_floor(store, phase, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=1)
            start = clock[0]
            await service.grant(
                ProcessingGrant(
                    source_id(scope, "1"), ("alice",), expires_at=start + timedelta(seconds=10)
                ),
                expected_version=1,
            )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            lease = await queue.claim("build", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            original = service._candidates

            async def expire_during_io(uow, definition):
                result = await original(uow, definition)
                clock[0] = start + timedelta(seconds=20)
                return result

            monkeypatch.setattr(service, "_candidates", expire_during_io)
            with pytest.raises(DerivedError):
                if phase == "snapshot":
                    await service.snapshot(lease.task)
                else:
                    await service.publish(lease.task, snapshot, service.prepare(snapshot))
            monkeypatch.setattr(service, "_candidates", original)
            clock[0] = start + timedelta(seconds=2)
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                if phase == "snapshot":
                    await service.snapshot(lease.task)
                else:
                    await service.publish(lease.task, snapshot, service.prepare(snapshot))

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["heartbeat", "fail"])
def test_rejected_lease_operation_persists_observed_clock(store, operation):
    async def run():
        from agent_memory.operations.worker_tasks import WorkerQueueError

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            start = clock[0]
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            lease = await queue.claim("build", lease_seconds=5)
            snapshot = await service.snapshot(lease.task)
            clock[0] = start + timedelta(seconds=6)
            with pytest.raises(WorkerQueueError):
                if operation == "heartbeat":
                    await queue.heartbeat(lease, lease_seconds=5)
                else:
                    await queue.fail(lease, RuntimeError("failure"))
            clock[0] = start + timedelta(seconds=2)
            with pytest.raises(DerivedError, match="refresh_clock_discontinuity"):
                await service.publish(lease.task, snapshot, service.prepare(snapshot))

    asyncio.run(run())
