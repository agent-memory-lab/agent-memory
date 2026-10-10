"""Cross-feature checks for shared relation publication and immutable history."""

import asyncio
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
import test_relation_questions_v7 as relation
from test_purge_restore import backup_copy, erase, replay, restorer
from test_question_runtime_v7 import ACTOR, register

from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.derived.model import DerivedError
from agent_memory.derived.question_history import KINDS
from agent_memory.derived.question_service import QuestionService
from agent_memory.operations.worker_tasks import WorkerQueueError

store = project.store


def service(engine, scope, clock):
    base = relation.service(engine, scope, clock)
    return QuestionService(base.admission, base.context, history_points=True)


async def captured(engine, scope, clock, *, valid_to=None):
    svc = service(engine, scope, clock)
    await relation.seed(svc, valid_to=valid_to)
    questions = ("risks", "owner", "status")
    for question in questions:
        await register(svc, question)
    clock[0] += timedelta(microseconds=100)
    leases = []
    for question in questions:
        receipt = await svc.request("project-a:" + question, actor=ACTOR, dedupe_key=question)
        lease = await svc.queue.claim(
            "history-batch", lease_seconds=30, target_id=receipt["target_id"]
        )
        assert lease is not None
        leases.append(lease)
    tasks = [lease.task for lease in leases]
    snapshots = await svc.snapshot_many(tasks)
    return svc, leases, tasks, snapshots, [svc.prepare(snapshot) for snapshot in snapshots]


async def unpublished(repository, scope):
    async with repository.unit_of_work() as uow:
        for kind in (
            *KINDS, "question_head", "question_content", "question_certificate",
            "refresh_publication",
        ):
            assert not await uow.derived_records(scope, kind)
        for kind in ("job", "refresh_execution"):
            assert all(row["payload"]["status"] == "running"
                       for row in await uow.derived_records(scope, kind))


def test_shared_relation_publication_captures_complete_original_history(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases, tasks, snapshots, prepared = await captured(engine, scope, clock)
            await svc.publish_many(tasks, snapshots, prepared)
            for lease in leases:
                await svc.queue.complete(lease)
            known = clock[0]
            result = await svc.read("project-a:risks", actor=ACTOR)
            old = await svc.read(
                "project-a:risks", actor=ACTOR, known_at=known, valid_at=known
            )
            assert relation.rule(result)["conclusions"][0]["matches"] is True
            assert old["result"] == result["result"]
            sources = {"edge", "launch", "due"}
            assert set(relation.rule(old)["conclusions"][0]["source_event_ids"]) == sources
            async with engine.repository.unit_of_work() as uow:
                headers = await uow.derived_records(scope, KINDS[0])
                assert len(headers) == 3
                assert all(set(row["payload"]["sources"]) == sources for row in headers)
    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["history", "final_guard", "last_lease_read"])
@pytest.mark.parametrize("expiry_kind", ["lease", "coverage"])
def test_shared_relation_history_rolls_back_at_final_time_boundary(
    store, monkeypatch, boundary, expiry_kind
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            coverage = clock[0] + timedelta(seconds=10) if expiry_kind == "coverage" else None
            svc, leases, tasks, snapshots, prepared = await captured(
                engine, scope, clock, valid_to=coverage
            )
            deadline = coverage or tasks[0].lease_expires_at
            owner = svc.history if boundary == "history" else svc
            method = {
                "history": "capture", "final_guard": "_guard_batch",
                "last_lease_read": "_batch_lease_deadlines",
            }[boundary]
            original = getattr(owner, method)
            calls = []

            async def late(*args, **kwargs):
                result = await original(*args, **kwargs)
                calls.append(True)
                if boundary != "history" or len(calls) == len(tasks):
                    clock[0] = deadline
                return result

            monkeypatch.setattr(owner, method, late)
            with pytest.raises((DerivedError, WorkerQueueError), match="expired|stale"):
                await svc.publish_many(tasks, snapshots, prepared)
            await unpublished(engine.repository, scope)
    asyncio.run(run())


def test_relation_history_old_backup_erasure_and_restart(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, leases, tasks, snapshots, prepared = await captured(engine, scope, clock)
            await svc.publish_many(tasks, snapshots, prepared)
            for lease in leases:
                await svc.queue.complete(lease)
            known = clock[0]
            await svc.pages.register("relation-overview", ["project-a:risks"], readers=(ACTOR,))
            await svc.pages.publish("relation-overview", actor=ACTOR)
            await svc.history.capture("relation-overview", actor=ACTOR, kind="page")
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, "due")
                journal = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), journal)
                for repository in (engine.repository, backup):
                    restarted = service(AdmissionEngine(repository), scope, clock)
                    async with repository.unit_of_work() as uow:
                        for kind in KINDS:
                            rows = await uow.derived_records(scope, kind)
                            assert rows and all(row["payload"] == {"state": "erased"}
                                                for row in rows)
                    with pytest.raises(DerivedError, match="erased|unavailable"):
                        await restarted.read(
                            "project-a:risks", actor=ACTOR, known_at=known, valid_at=known
                        )
                    with pytest.raises(DerivedError, match="erased|unavailable"):
                        await restarted.history.read(
                            "relation-overview", actor=ACTOR, kind="page",
                            known_at=known, valid_at=known,
                        )
    asyncio.run(run())
