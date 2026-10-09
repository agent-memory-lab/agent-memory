"""Actual RefreshHost page maintenance through the shared durable queue."""

import asyncio
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_purge_restore import erase
from test_question_page_validation_v7 import setup
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.operations.refresh_host import RefreshHost
from agent_memory.operations.refresh_policy import RefreshLimits, RefreshPolicy
from agent_memory.operations.worker_tasks import WorkerLimits, WorkerQueueError

store = project.store


def host(service, worker="page-host", concurrency=1):
    return RefreshHost(
        service.queue,
        worker_id=worker,
        limits=WorkerLimits(max_concurrency=concurrency),
        clock_tolerance_seconds=300,
    )


async def records(service, kind):
    async with service.repository.unit_of_work() as uow:
        return [r["payload"] for r in await uow.derived_records(service.scope, kind)]


async def noop_parent(service, clock):
    await service.grant(
        ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
    )
    answer = await fresh(service, clock, dedupe="new-parent")
    clock[0] += timedelta(seconds=2)
    return answer


def test_host_automatically_validates_published_pages_in_bounded_batches(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _ = await setup(engine, scope, clock, count=3)
            before = {
                str(i): await service.pages.read("page:" + str(i), actor=ACTOR) for i in range(3)
            }
            parent = await noop_parent(service, clock)
            runner = host(service)
            for expected in (2, 1, 0):
                result = await runner.run_once()
                assert result.claimed == result.completed == 1, [
                    (r["facet_id"], r["status"], r.get("reason"), r.get("runnable_at"))
                    for r in await records(service, "refresh_demand")
                ]
                pending = await records(service, "question_page_validation")
                assert sum(r["state"] == "validation_pending" for r in pending) == expected
            assert (await runner.run_once()).idle
            for i in range(3):
                page = await service.pages.read("page:" + str(i), actor=ACTOR)
                assert page["revision_id"] == before[str(i)]["revision_id"]
                assert page["certificate_revision_id"] != before[str(i)]["certificate_revision_id"]
                assert page["blocks"][0]["body"]["answer"] == parent
            page_executions = [
                r
                for r in await records(service, "refresh_execution")
                if r["unit"].get("schema") == "question-page-refresh-unit/1"
            ]
            assert len(page_executions) == 3
            assert all(r["status"] == "completed" for r in page_executions)
            async with service.repository.unit_of_work() as uow:
                for execution in page_executions:
                    assert await service.page_processor.verify_coverage(uow, execution)

    asyncio.run(run())


def test_host_demands_cold_parent_without_permanently_heating_its_policy(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _ = await setup(engine, scope, clock)
            new = await project.stage(service.admission, scope, identity="new", value="Bob")
            await project.qualify(service.admission, *new)
            clock[0] += timedelta(microseconds=100)
            runner = host(service)
            # Child defers in shared queue; same poll may claim the newly-demanded
            # parent if it was among the original indexed hints.
            for _ in range(4):
                await runner.run_once()
                clock[0] += timedelta(seconds=2)
            page = await service.pages.read("page:0", actor=ACTOR)
            assert page["blocks"][0]["body"]["answer"]["answer_status"] == "contested"
            policies = await records(service, "refresh_policy")
            assert (
                next(r for r in policies if not r["facet_id"].startswith("question-page:"))[
                    "policy"
                ]["mode"]
                == "on_demand"
            )
            assert (await runner.run_once()).idle

    asyncio.run(run())


def test_restart_recovers_expired_page_lease_and_rejects_stale_publication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _ = await setup(engine, scope, clock)
            await noop_parent(service, clock)
            lease = await service.queue.claim("crashed-host", lease_seconds=5)
            assert lease.task.payload["unit"]["schema"] == "question-page-refresh-unit/1"
            snapshot = await service.page_processor.snapshot(lease.task)
            clock[0] += timedelta(seconds=6)
            # Exact route identity includes context; rebuild with the original context.
            from agent_memory.derived.question_service import QuestionService

            restarted = QuestionService(service.admission, service.context)
            result = await host(restarted).run_once()
            assert result.completed == 1
            with pytest.raises(WorkerQueueError, match="stale"):
                await service.page_processor.publish(
                    lease.task, snapshot, service.page_processor.prepare(snapshot)
                )
            assert (await restarted.pages.read("page:0", actor=ACTOR))[
                "availability_status"
            ] == "valid"
            assert (await host(restarted, "again").run_once()).idle

    asyncio.run(run())


def test_concurrent_hosts_share_global_page_running_budget(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = runtime(
                engine, scope, clock, limits=RefreshLimits(global_running=1, tenant_running=1)
            )
            item = await project.stage(service.admission, scope)
            await project.qualify(service.admission, *item)
            await register(service)
            await fresh(service, clock)
            for i in range(2):
                await service.pages.register(
                    "page:" + str(i), ("project-a:owner",), readers=(ACTOR,)
                )
                await service.pages.publish("page:" + str(i), actor=ACTOR)
            await noop_parent(service, clock)
            entered, release = asyncio.Event(), asyncio.Event()
            original = service.pages.maintenance.snapshot

            async def blocked(task):
                snapshot = await original(task)
                entered.set()
                await release.wait()
                return snapshot

            service.pages.maintenance.snapshot = blocked
            first = asyncio.create_task(host(service, "first").run_once())
            await entered.wait()
            from agent_memory.derived.question_service import QuestionService

            competing = QuestionService(
                service.admission, service.context, limits=service.queue.limits
            )
            second = await host(competing, "second").run_once()
            assert second.idle
            release.set()
            assert (await first).completed == 1
            clock[0] += timedelta(seconds=2)
            assert (await host(service, "last").run_once()).completed == 1

    asyncio.run(run())


def test_page_revalidation_cannot_publish_after_parent_changes_or_erase(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _ = await setup(engine, scope, clock)
            await noop_parent(service, clock)
            lease = await service.queue.claim("stale-parent", lease_seconds=30)
            snapshot = await service.page_processor.snapshot(lease.task)
            new = await project.stage(service.admission, scope, identity="new", value="Bob")
            await project.qualify(service.admission, *new)
            with pytest.raises(DerivedError):
                await service.page_processor.publish(
                    lease.task, snapshot, service.page_processor.prepare(snapshot)
                )
            await erase(kernel, scope, "source")
            with pytest.raises((DerivedError, WorkerQueueError)):
                await service.page_processor.publish(
                    lease.task, snapshot, service.page_processor.prepare(snapshot)
                )
            assert (await host(service).run_once()).idle
            async with service.repository.unit_of_work() as uow:
                await service._open(uow)
                await uow.refresh_scheduler_lock(scope)
                usage = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(),
                    tenant_id=scope.tenant_id,
                    instance_key=service.pages.key("page:0"),
                )
                assert usage["global_running"] == 0
            with pytest.raises(DerivedError):
                await service.pages.read("page:0", actor=ACTOR)

    asyncio.run(run())


def test_restart_does_not_reset_page_aging_budget(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _ = await setup(engine, scope, clock)
            await service.queue.configure(
                service.pages.key("page:0"),
                RefreshPolicy(max_age_seconds=3, max_no_progress_seconds=3),
                processor_key=service.page_processor.key,
            )
            await noop_parent(service, clock)
            clock[0] += timedelta(seconds=4)
            from agent_memory.derived.question_service import QuestionService

            restarted = QuestionService(service.admission, service.context)
            assert (await host(restarted).run_once()).idle
            page_demands = [
                r
                for r in await records(service, "refresh_demand")
                if r["facet_id"] == service.pages.key("page:0")
            ]
            assert page_demands[-1]["status"] == "dead"
            assert page_demands[-1]["reason"] == "refresh_budget_exhausted"
            assert (await host(restarted, "again").run_once()).idle
            with pytest.raises(DerivedError):
                await restarted.pages.read("page:0", actor=ACTOR)

    asyncio.run(run())


@pytest.mark.parametrize("pending", [False, True])
def test_host_bounded_startup_backfills_pre_scheduler_pages(store, monkeypatch, pending):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived.question_page_refresh import QuestionPageRefresh
            from agent_memory.derived.question_pages import ProjectQuestionPages, sealed

            async def legacy(*args, **kwargs):
                pass

            with monkeypatch.context() as old:
                old.setattr(QuestionPageRefresh, "published", legacy)
                old.setattr(ProjectQuestionPages, "parent_published", legacy)
                service, _ = await setup(engine, scope, clock)
                before = await service.pages.read("page:0", actor=ACTOR)
                if pending:
                    await noop_parent(service, clock)
                    async with service.repository.unit_of_work() as uow:
                        await service._open(uow)
                        key = service.pages.key("page:0")
                        await uow.derived_put(
                            scope,
                            "question_page_validation",
                            key,
                            sealed(
                                dict(
                                    schema="question-page-validation/1",
                                    instance_id=key,
                                    state="validation_pending",
                                    attempts=0,
                                    targets={},
                                )
                            ),
                        )
            result = await host(service).run_once()
            assert result.completed == int(pending)
            page = await service.pages.read("page:0", actor=ACTOR)
            assert page["revision_id"] == before["revision_id"]
            assert (await host(service, "restart").run_once()).idle
            definition = next(
                r
                for r in await records(service, "definition")
                if r["facet_id"] == service.pages.key("page:0")
            )
            assert definition["next_transition_at"] == page["valid_until"]

    asyncio.run(run())


@pytest.mark.parametrize("phase", ["before_publication_commit", "after_publication_commit"])
def test_page_host_sigkill_publication_is_atomic_and_restart_is_idempotent(
    store, tmp_path, monkeypatch, phase
):
    import signal
    from pathlib import Path

    import test_refresh_scheduler_process as processes

    if not hasattr(signal, "SIGKILL"):
        pytest.skip("requires real SIGKILL")
    monkeypatch.setattr(
        processes, "CHILD", Path(__file__).parent / "fixtures/question_page_crash_child.py"
    )

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _ = await setup(engine, scope, clock)
            before = await records(service, "question_page_head")
            await noop_parent(service, clock)
            publications = await records(service, "refresh_publication")
            async with processes.child(
                engine.repository, scope, clock, tmp_path, phase, context=service.context.payload()
            ) as process:
                await processes.event(process, "claimed")
                await processes.release(process)
                await processes.event(process, "boundary")
                await processes.kill(process)
            committed = phase == "after_publication_commit"
            assert len(await records(service, "refresh_publication")) == len(publications) + int(
                committed
            )
            if not committed:
                assert await records(service, "question_page_head") == before
            clock[0] += timedelta(seconds=6)
            recovered = await processes.drain(
                engine.repository, scope, clock, tmp_path, context=service.context.payload()
            )
            assert len(recovered) == int(not committed)
            assert (await service.pages.read("page:0", actor=ACTOR))[
                "availability_status"
            ] == "valid"
            assert len(await records(service, "refresh_publication")) == len(publications) + 1
            assert (
                await processes.drain(
                    engine.repository, scope, clock, tmp_path, context=service.context.payload()
                )
                == []
            )

    asyncio.run(run())


def test_published_page_refreshes_when_cold_parent_time_boundary_arrives(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = runtime(engine, scope, clock)
            future = clock[0] + timedelta(seconds=10)
            item = await project.stage(service.admission, scope, valid_from=future)
            await project.qualify(service.admission, *item)
            await register(service)
            first = await fresh(service, clock)
            assert first["answer_status"] == "unknown"
            await service.pages.register("time-page", ("project-a:owner",), readers=(ACTOR,))
            await service.pages.publish("time-page", actor=ACTOR)
            clock[0] = future + timedelta(seconds=1)
            for i in range(5):
                await host(service, worker="time-" + str(i)).run_once()
                clock[0] += timedelta(seconds=2)
            page = await service.pages.read("time-page", actor=ACTOR)
            assert page["blocks"][0]["body"]["answer"]["answer_status"] == "resolved"

    asyncio.run(run())


def test_page_final_guard_cannot_cross_lease_after_publish_coverage(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _ = await setup(engine, scope, clock)
            await noop_parent(service, clock)
            lease = await service.queue.claim("lease-test", lease_seconds=5)
            assert lease.task.payload["unit"]["schema"] == "question-page-refresh-unit/1"
            snapshot = await service.page_processor.snapshot(lease.task)
            complete, guard = service.pages.maintenance.complete, service.pages._guard
            completed = [False]

            async def wrapped_complete(*args, **kwargs):
                result = await complete(*args, **kwargs)
                completed[0] = True
                return result

            async def late_guard(*args, **kwargs):
                result = await guard(*args, **kwargs)
                if completed[0]:
                    clock[0] += timedelta(seconds=6)
                return result

            service.pages.maintenance.complete = wrapped_complete
            service.pages._guard = late_guard
            with pytest.raises(WorkerQueueError, match="stale"):
                await service.page_processor.publish(
                    lease.task, snapshot, service.page_processor.prepare(snapshot)
                )

    asyncio.run(run())


def test_completed_page_work_uses_existing_bounded_garbage_collection(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived.question_gc import QuestionRetentionPolicy

            service, _ = await setup(engine, scope, clock)
            await service.queue.configure(
                service.pages.key("page:0"),
                RefreshPolicy(max_age_seconds=60),
                processor_key=service.page_processor.key,
            )
            for version in range(1, 4):
                await service.grant(
                    ProcessingGrant("source", (ACTOR,), ("project_questions",)),
                    expected_version=version,
                )
                await fresh(service, clock, dedupe="parent:" + str(version))
                clock[0] += timedelta(seconds=2)
                assert (await host(service).run_once()).completed == 1
            page = await service.pages.read("page:0", actor=ACTOR)
            executions = {
                r["id"]
                for r in await records(service, "refresh_execution")
                if r["adapter_key"] == service.page_processor.key
            }
            assert len(executions) == 3
            clock[0] += timedelta(seconds=61)
            result = await service.collect_garbage(QuestionRetentionPolicy("page-work-retention"))
            deleted = {key for kind, key in result["deleted"] if kind == "refresh_execution"}
            assert executions <= deleted
            assert await service.pages.read("page:0", actor=ACTOR) == page

    asyncio.run(run())


def test_page_no_metadata_await_can_cross_parent_boundary_after_final_guard(store):
    from agent_memory.derived.model import ProcessingGrant

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = runtime(engine, scope, clock)
            boundary = clock[0] + timedelta(seconds=10)
            item = await project.stage(service.admission, scope, valid_from=boundary)
            await project.qualify(service.admission, *item)
            await register(service)
            await fresh(service, clock)
            await service.pages.register("boundary-page", ("project-a:owner",), readers=(ACTOR,))
            await service.pages.publish("boundary-page", actor=ACTOR)
            await service.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            await fresh(service, clock, dedupe="boundary-new-proof")
            lease = await service.queue.claim("boundary-test", lease_seconds=30)
            assert lease.task.payload["unit"]["schema"] == "question-page-refresh-unit/1"
            snapshot = await service.page_processor.snapshot(lease.task)
            complete, guard, original = (
                service.pages.maintenance.complete,
                service.pages._guard,
                engine.repository.unit_of_work,
            )
            completed, guarded, injected = [False], [False], [False]

            async def wrapped_complete(*args, **kwargs):
                result = await complete(*args, **kwargs)
                completed[0] = True
                return result

            async def wrapped_guard(*args, **kwargs):
                result = await guard(*args, **kwargs)
                if completed[0]:
                    guarded[0] = True
                return result

            class DelayFinalRead:
                def __init__(self):
                    self.inner = original()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_get(self, scope, kind, key):
                    result = await self.uow.derived_get(scope, kind, key)
                    if guarded[0] and kind == "job":
                        clock[0] = boundary + timedelta(seconds=1)
                        injected[0] = True
                    return result

            service.pages.maintenance.complete = wrapped_complete
            service.pages._guard = wrapped_guard
            engine.repository.unit_of_work = DelayFinalRead
            try:
                try:
                    await service.page_processor.publish(
                        lease.task, snapshot, service.page_processor.prepare(snapshot)
                    )
                except (DerivedError, WorkerQueueError):
                    assert injected[0]
                else:
                    assert not injected[0], (
                        "Committed expired parent proof after final authority guard"
                    )
            finally:
                engine.repository.unit_of_work = original

    asyncio.run(run())


def test_targeted_direct_answer_reclaims_unrelated_expired_global_reservation(store):
    from agent_memory.operations.refresh_policy import RefreshLimits

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service = runtime(
                engine, scope, clock, limits=RefreshLimits(global_running=1, tenant_running=1)
            )
            await register(service, question="owner")
            await register(service, question="status")
            await service.request("project-a:owner", actor=ACTOR, dedupe_key="old-crashed")
            crashed = await service.queue.claim("crashed-direct", lease_seconds=5)
            assert crashed
            clock[0] += timedelta(seconds=6)
            answer = await service.answer("project-a:status", actor=ACTOR, dedupe_key="new-target")
            assert answer["availability_status"] == "valid"
            async with engine.repository.unit_of_work() as uow:
                old = await uow.derived_get(scope, "job", crashed.task.id)
                assert old["status"] != "completed", (
                    "Recovery must not spend direct work budget on unrelated compute"
                )

    asyncio.run(run())
