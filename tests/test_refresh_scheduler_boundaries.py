import asyncio
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_observations import setup
from test_refresh_demand import finish, scheduler

from agent_memory.operations.refresh_demand import record_dirty
from agent_memory.operations.refresh_policy import RefreshPolicy

store = base.store


def test_receipt_deadline_is_individual(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            early = await queue.request(
                "language",
                dedupe_key="early",
                actor="alice",
                deadline=clock[0] + timedelta(seconds=1),
            )
            late = await queue.request(
                "language",
                dedupe_key="late",
                actor="alice",
                deadline=clock[0] + timedelta(seconds=100),
            )
            no_deadline = await queue.request("language", dedupe_key="none", actor="alice")
            clock[0] += timedelta(seconds=2)
            statuses = [
                await queue.status(r["target_id"], actor="alice")
                for r in (early, late, no_deadline)
            ]
            assert [s["deadline_missed"] for s in statuses] == [True, False, False], statuses

    asyncio.run(run())


def test_max_age_recovery_can_publish(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(max_age_seconds=6))
            old = await queue.request("language", dedupe_key="old", actor="alice")
            assert await queue.claim("first", lease_seconds=5) is not None
            clock[0] += timedelta(seconds=7)
            assert await queue.claim("expired", lease_seconds=5) is None
            assert (await queue.status(old["target_id"], actor="alice"))["state"] == "dead"
            await queue.request("language", dedupe_key="revive", actor="alice")
            lease = await queue.claim("revived", lease_seconds=5)
            assert lease is not None
            await finish(queue, lease)
            assert (await queue.status(old["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_successor_owns_its_age(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(max_age_seconds=10))
            first = await queue.claim("first", lease_seconds=60)
            clock[0] += timedelta(seconds=9)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                await record_dirty(uow, scope, definition, at=clock[0], reason="new_dirty")
            second = await queue.request("language", dedupe_key="second", actor="alice")
            await finish(queue, first)
            clock[0] += timedelta(seconds=2)
            lease = await queue.claim("successor", lease_seconds=60)
            assert lease is not None, await queue.status(second["target_id"], actor="alice")

    asyncio.run(run())


def test_expired_authority_stably_defers(store):
    async def run():
        from test_derived_controls import configured

        async with store() as (engine, kernel, scope, clock):
            service, _, _, authority, _, _ = await configured(
                engine, kernel, scope, clock, inputs=0
            )
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(max_no_progress_seconds=7200))
            clock[0] = authority.expires_at
            assert await queue.claim("expired", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.derived_records(scope, "refresh_demand")
                assert rows[0]["payload"]["status"] == "deferred"
                assert rows[0]["payload"]["reason"] == "derived_authority_expired"

    asyncio.run(run())


def test_missing_parent_proof_can_be_refreshed(store):
    async def run():
        from agent_memory.derived import FacetDefinition

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(mode="on_demand"))
            await queue.request("language", dedupe_key="parent", actor="alice")
            await finish(queue, await queue.claim("build-parent", lease_seconds=60))
            async with engine.repository.unit_of_work() as uow:
                head = await uow.derived_get(scope, "head", "language")
                await uow.derived_put(scope, "revision_header", head["audit_revision_id"], None)
            await service.register(
                FacetDefinition(
                    "child",
                    "alice",
                    template_version="locale-parents/1",
                    parent_facets=("language",),
                )
            )
            await queue.configure("child", RefreshPolicy())
            assert await queue.claim("wait-child", lease_seconds=60) is None
            lease = await queue.claim("rebuild-parent", lease_seconds=60)
            assert lease is not None
            assert lease.task.payload["unit"]["facet_id"] == "language"

    asyncio.run(run())


def test_cold_idle_time_is_not_execution_age(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(mode="on_demand", max_age_seconds=10))
            clock[0] += timedelta(seconds=11)
            receipt = await queue.request("language", dedupe_key="first-read", actor="alice")
            lease = await queue.claim("reader", lease_seconds=60)
            assert lease is not None, await queue.status(receipt["target_id"], actor="alice")
            await finish(queue, lease)

    asyncio.run(run())


def test_deferred_boundaries_do_not_starve_ready_work(store):
    async def run():
        from dataclasses import replace

        from agent_memory.derived import FacetDefinition, ObservationService
        from agent_memory.operations.refresh_demand import (
            ObservationRefreshProcessor,
            RefreshDemandQueue,
        )

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            other = ObservationService(
                engine.repository,
                replace(scope, session_id="healthy"),
                base.POLICY,
                clock=lambda: clock[0],
            )
            await other.register(FacetDefinition("healthy", "alice"))
            ps, po = ObservationRefreshProcessor(service), ObservationRefreshProcessor(other)
            queue = RefreshDemandQueue((ps, po), clock=lambda: clock[0])
            ids = ["language"] + [f"blocked-{n:03}" for n in range(127)]
            for facet in ids:
                if facet != "language":
                    await service.register(FacetDefinition(facet, "alice"))
                row = await queue.configure(
                    facet, RefreshPolicy(retry_seconds=3600), processor_key=ps.key
                )
                async with engine.repository.unit_of_work() as uow:
                    row["next_transition_at"] = clock[0].isoformat()
                    await queue._defer(uow, ps, row, "derived_authority_expired", clock[0])
            clock[0] += timedelta(seconds=1)
            await queue.configure("healthy", RefreshPolicy(), processor_key=po.key)
            lease = await queue.claim("ready", lease_seconds=60)
            assert lease is not None
            assert lease.task.payload["unit"]["facet_id"] == "healthy"

    asyncio.run(run())


def test_repeated_child_wait_does_not_create_new_parent_obligations(store):
    async def run():
        from agent_memory.derived import FacetDefinition

        async with store() as (engine, kernel, scope, clock):
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
            assert await queue.claim("child-blocked", lease_seconds=60) is None
            parent = await queue.claim("parent", lease_seconds=60)
            assert parent is not None
            for _ in range(4):
                clock[0] += timedelta(seconds=3)
                assert await queue.claim("child-wait", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.derived_records(scope, "refresh_demand")
                parentrow = next(
                    r["payload"] for r in rows if r["payload"]["facet_id"] == "language"
                )
                assert len(parentrow["requested"]) == 1, parentrow["requested"]

    asyncio.run(run())


def test_authority_deferral_still_has_total_age_bound(store):
    async def run():
        from test_derived_controls import configured

        async with store() as (engine, kernel, scope, clock):
            service, _, _, authority, _, _ = await configured(
                engine, kernel, scope, clock, inputs=0
            )
            queue = scheduler(service, clock)
            await queue.configure(
                "language", RefreshPolicy(max_age_seconds=86400, max_no_progress_seconds=86400)
            )
            clock[0] = authority.expires_at
            assert await queue.claim("blocked", lease_seconds=60) is None
            clock[0] += timedelta(days=2)
            assert await queue.claim("aged", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.derived_records(scope, "refresh_demand")
                assert rows[0]["payload"]["status"] == "dead", rows[0]["payload"]["status"]

    asyncio.run(run())


def test_running_policy_pinned_and_next_revision_gets_new_execution(store):
    async def run():
        from agent_memory.derived import DerivedError

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            start = clock[0]
            first_policy = RefreshPolicy(max_age_seconds=10, revision="first")
            await queue.configure("language", first_policy)
            first = await queue.claim("first", lease_seconds=60)
            assert first.task.lease_expires_at == start + timedelta(seconds=10)
            clock[0] += timedelta(seconds=1)
            await queue.configure("language", RefreshPolicy(max_age_seconds=60, revision="second"))
            await queue.heartbeat(first, lease_seconds=60)
            async with engine.repository.unit_of_work() as uow:
                execution = await uow.derived_get(
                    scope, "refresh_execution", first.task.payload["refresh_execution"]
                )
                assert execution["policy"] == first_policy.payload()
                assert execution["lease_until"] == (start + timedelta(seconds=10)).isoformat()
                with pytest.raises(DerivedError, match="refresh_execution_immutable"):
                    await uow.derived_put(
                        scope,
                        "refresh_execution",
                        execution["id"],
                        {**execution, "policy": RefreshPolicy().payload()},
                    )
            await queue.fail(first, DerivedError("temporary_test_failure"))
            clock[0] += timedelta(seconds=3)
            second = await queue.claim("second", lease_seconds=60)
            assert (
                second.task.payload["refresh_execution"] != first.task.payload["refresh_execution"]
            )
            await finish(queue, second)

    asyncio.run(run())


def test_cold_finite_request_does_not_chase_unrequested_new_dirty(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(mode="on_demand"))
            receipt = await queue.request("language", dedupe_key="finite", actor="alice")
            lease = await queue.claim("reader", lease_seconds=60)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                await record_dirty(uow, scope, definition, at=clock[0], reason="new_change")
            await finish(queue, lease)
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            assert await queue.claim("no_unbounded_chase", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                row = (await uow.derived_records(scope, "refresh_demand"))[0]["payload"]
                assert row["requested"] and row["status"] == "dirty" and row["active_since"] is None

    asyncio.run(run())


def test_legacy_active_adoption_requires_drain_before_shared_quota(store):
    async def run():
        from agent_memory.derived import DerivedError, FacetDefinition
        from agent_memory.operations.refresh_policy import RefreshLimits

        async with store() as (engine, kernel, scope, clock):
            service, legacy, _ = await setup(engine, kernel, scope, clock, inputs=0)
            old = await legacy.claim("legacy", lease_seconds=60)
            queue = scheduler(
                service, clock, limits=RefreshLimits(global_running=1, tenant_running=1)
            )
            with pytest.raises(DerivedError, match="refresh_legacy_lease_active"):
                await queue.configure("language", RefreshPolicy())
            async with engine.repository.unit_of_work() as uow:
                assert not (await uow.derived_get(scope, "definition", "language")).get(
                    "refresh_managed"
                )
                assert await uow.derived_get(scope, "refresh_policy", "language") is None
            await service.apply(old.task)
            await legacy.complete(old)
            await queue.configure("language", RefreshPolicy())
            await service.register(FacetDefinition("second", "alice"))
            await queue.configure("second", RefreshPolicy())
            assert await queue.claim("one", lease_seconds=60) is not None
            assert await queue.claim("two", lease_seconds=60) is None

    asyncio.run(run())


@pytest.mark.parametrize("request_before_configure", [True, False])
@pytest.mark.parametrize("force", [False, True])
def test_legacy_exact_requests_join_managed_cold_admission(store, request_before_configure, force):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, legacy, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            if request_before_configure:
                receipt = await legacy.request("language", dedupe_key="exact", force=force)
            await queue.configure("language", RefreshPolicy(mode="on_demand"))
            if not request_before_configure:
                receipt = await legacy.request("language", dedupe_key="exact", force=force)
            assert "target" not in receipt and receipt["unit"]["schema"].startswith(
                "facet-refresh-unit/"
            )
            assert await legacy.claim("legacy", lease_seconds=60) is None
            lease = await queue.claim("managed", lease_seconds=60)
            assert lease is not None and lease.task.payload["unit"] == receipt["unit"]
            await finish(queue, lease)
            assert (await legacy.status(receipt["target_id"], actor="alice"))["complete"]
            assert await legacy.request("language", dedupe_key="exact", force=force) == receipt
            assert await queue.claim("no_extra", lease_seconds=60) is None

    asyncio.run(run())


def test_cold_parent_auto_adoption_cannot_bypass_legacy_lease_guard(store):
    async def run():
        from agent_memory.derived import FacetDefinition

        async with store() as (engine, kernel, scope, clock):
            service, legacy, _ = await setup(engine, kernel, scope, clock, inputs=0)
            old = await legacy.claim("legacy-parent", lease_seconds=60)
            assert old is not None
            await service.register(
                FacetDefinition(
                    "child",
                    "alice",
                    template_version="locale-parents/1",
                    parent_facets=("language",),
                )
            )
            queue = scheduler(service, clock)
            await queue.configure("child", RefreshPolicy())
            assert await queue.claim("managed-child", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                parent = await uow.derived_get(scope, "definition", "language")
                assert not parent.get("refresh_managed"), (
                    "legacy-running parent was silently adopted without reservation"
                )

    asyncio.run(run())


def test_misconfigured_failure_handler_cannot_expand_pending_quota(store):
    async def run():
        from agent_memory.derived import DerivedError
        from agent_memory.operations.refresh_policy import RefreshLimits

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            lease = await queue.claim("owner", lease_seconds=60)
            wrong = scheduler(service, clock, limits=RefreshLimits(global_pending=8192))
            with pytest.raises(DerivedError, match="refresh_scheduler_config_mismatch"):
                await wrong.fail(lease, DerivedError("retry_me"))
            # The whole attempted failure transition and release rolled back.
            await finish(queue, lease)

    asyncio.run(run())


def test_obsolete_pending_exact_jobs_cannot_permanently_block_managed_work(store):
    async def run():
        from agent_memory.derived.service import mark_slot_changed

        async with store() as (engine, kernel, scope, clock):
            service, legacy, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(mode="on_demand"))
            for n in range(128):
                await legacy.request("language", dedupe_key=f"exact-{n}", force=True)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                await mark_slot_changed(uow, scope, definition["slots"][0], at=clock[0])
            lease = await queue.claim("managed", lease_seconds=60)
            assert lease is not None, "all old exact jobs are obsolete but still consume max_active"

    asyncio.run(run())


@pytest.mark.parametrize("publish_after_deadline", [False, True])
def test_completion_preserves_actual_deadline_result(store, publish_after_deadline, monkeypatch):
    async def run():
        import agent_memory.operations.refresh_demand as demand

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy())
            start = clock[0]
            receipt = await queue.request(
                "language", dedupe_key="deadline", actor="alice",
                deadline=start + timedelta(seconds=1),
            )
            lease = await queue.claim("build", lease_seconds=60)
            original = demand.publish_coverage

            async def delayed_commit(*args, **kwargs):
                if publish_after_deadline:
                    clock[0] = start + timedelta(seconds=2)
                return await original(*args, **kwargs)

            monkeypatch.setattr(demand, "publish_coverage", delayed_commit)
            await finish(queue, lease)
            clock[0] = start + timedelta(seconds=3)
            status = await queue.status(receipt["target_id"], actor="alice")
            assert status["complete"]
            assert status["deadline_missed"] is publish_after_deadline
            async with engine.repository.unit_of_work() as uow:
                publications = await uow.derived_records(scope, "refresh_publication")
                publication = publications[0]["payload"]
                assert publication["published_at"] == (
                    start + timedelta(seconds=2 if publish_after_deadline else 0)
                ).isoformat()

    asyncio.run(run())


@pytest.mark.parametrize("exact", [False, True])
def test_new_request_recovers_exhausted_demand_after_intervening_dirty_write(store, exact):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, legacy, _ = await setup(engine, kernel, scope, clock, inputs=0)
            queue = scheduler(service, clock)
            await queue.configure("language", RefreshPolicy(max_attempts=1))
            original = await queue.request("language", dedupe_key="original", actor="alice")
            lease = await queue.claim("failure", lease_seconds=60)
            await queue.fail(lease, RuntimeError("bad compute"))
            assert (await queue.status(original["target_id"], actor="alice"))["state"] == "dead"
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                await record_dirty(uow, scope, definition, at=clock[0])
            # Merely writing more data never grants an unbounded retry budget.
            assert await queue.claim("not-recovered", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                await record_dirty(uow, scope, definition, at=clock[0])
            if exact:
                receipt = await legacy.request("language", dedupe_key="revive", force=True)
            else:
                receipt = await queue.request("language", dedupe_key="revive", actor="alice")
            recovered = await queue.claim("recovered", lease_seconds=60)
            assert recovered is not None
            await finish(queue, recovered)
            assert (await queue.status(original["target_id"], actor="alice"))["complete"]
            status_queue = legacy if exact else queue
            assert (await status_queue.status(receipt["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_pending_quota_cannot_starve_small_tenant_under_continuous_large_tenant_input(store):
    async def run():
        from dataclasses import replace

        from agent_memory.derived import FacetDefinition, ObservationService
        from agent_memory.operations.refresh_demand import (
            ObservationRefreshProcessor,
            RefreshDemandQueue,
        )
        from agent_memory.operations.refresh_policy import RefreshLimits

        async with store() as (engine, _, scope, clock):
            large, small = (
                ObservationService(
                    engine.repository, replace(scope, tenant_id=tenant), base.POLICY,
                    clock=lambda: clock[0],
                ) for tenant in ("large", "small")
            )
            for service in (large, small):
                await service.register(FacetDefinition("question", "alice"))
            pa, pb = ObservationRefreshProcessor(large), ObservationRefreshProcessor(small)
            limits = RefreshLimits(
                global_pending=1, tenant_pending=1, instance_pending=1,
                global_running=1, tenant_running=1,
            )
            queue = RefreshDemandQueue((pa, pb), clock=lambda: clock[0], limits=limits)
            await queue.configure("question", RefreshPolicy(), processor_key=pa.key)
            await queue.configure("question", RefreshPolicy(), processor_key=pb.key)
            served = []
            for attempt in range(5):
                lease = await queue.claim(f"worker-{attempt}", lease_seconds=60)
                assert lease is not None
                served.append(lease.task.scope.tenant_id)
                async with engine.repository.unit_of_work() as uow:
                    usage = await uow.refresh_scheduler_usage(
                        now=clock[0].isoformat(), tenant_id="large", instance_key="unused"
                    )
                    assert usage["global_pending"] <= 1 and usage["global_running"] == 1
                await finish(queue, lease)
                clock[0] += timedelta(seconds=3)
                async with engine.repository.unit_of_work() as uow:
                    definition = await uow.derived_get(large.scope, "definition", "question")
                    await record_dirty(uow, large.scope, definition, at=clock[0])
            assert "small" in served[:2], served

    asyncio.run(run())
