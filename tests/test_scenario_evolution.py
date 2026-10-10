"""L2 enrollment includes the first build and native evidence changes."""

import asyncio
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory.derived.evolution import ScenarioEvolution
from agent_memory.derived.model import DerivedError
from agent_memory.operations.refresh_host import RefreshHost
from agent_memory.operations.worker_tasks import WorkerLimits

store = project.store


def test_scene_public_queue_receipt_is_finite_and_preserves_existing_quota_identity(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            scenes = ScenarioEvolution(svc)
            registration = await scenes.register(
                "finite-context", ("project-a:owner",), readers=(ACTOR,)
            )
            receipt = await scenes.queue.request(
                registration["instance_id"],
                dedupe_key="scene-target",
                actor=ACTOR,
                processor_key=svc.page_processor.key,
            )
            assert receipt["target"]["instance_id"].startswith("question-instance:")
            async with engine.repository.unit_of_work() as uow:
                binding = await uow.derived_get(
                    scope, "refresh_policy", registration["instance_id"]
                )
            assert binding["instance_key"] == registration["instance_id"]
            assert (
                await scenes.queue.request(
                    registration["instance_id"],
                    dedupe_key="scene-target",
                    actor=ACTOR,
                    processor_key=svc.page_processor.key,
                )
                == receipt
            )
            with pytest.raises(DerivedError, match="read_denied"):
                await scenes.queue.status(
                    receipt["target_id"], actor="mallory", processor_key=svc.page_processor.key
                )
            host = RefreshHost(
                scenes.queue,
                worker_id="scene-receipt",
                limits=WorkerLimits(max_concurrency=1),
                clock_tolerance_seconds=300,
            )
            for _ in range(6):
                await host.run_once()
                clock[0] += timedelta(seconds=2)
            status = await scenes.queue.status(
                receipt["target_id"], actor=ACTOR, processor_key=svc.page_processor.key
            )
            assert status["complete"]
            assert (await scenes.read("finite-context", actor=ACTOR))["blocks"]

    asyncio.run(run())


def test_registered_scene_builds_from_cold_parents_and_evolves_without_manual_publication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            scenes = ScenarioEvolution(svc)
            await scenes.register("project-context", ("project-a:owner",), readers=(ACTOR,))
            host = RefreshHost(
                scenes.queue,
                worker_id="scene-host",
                limits=WorkerLimits(max_concurrency=1),
                clock_tolerance_seconds=300,
            )
            for _ in range(5):
                await host.run_once()
                clock[0] += timedelta(seconds=2)
            first = await scenes.read("project-context", actor=ACTOR)
            assert first["blocks"][0]["body"]["answer"]["answer_status"] == "resolved"
            candidate = await project.stage(svc.admission, scope, identity="bob", value="Bob")
            await project.qualify(svc.admission, *candidate)
            with pytest.raises(DerivedError, match="stale|unavailable"):
                await scenes.read("project-context", actor=ACTOR)
            for _ in range(5):
                await host.run_once()
                clock[0] += timedelta(seconds=2)
            later = await scenes.read("project-context", actor=ACTOR)
            assert later["blocks"][0]["body"]["answer"]["answer_status"] == "contested"
            assert later["revision_id"] != first["revision_id"]
            async with svc.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "question_page_content", first["revision_id"])
                assert await uow.derived_get(scope, "question_page_content", later["revision_id"])
                jobs = await uow.derived_records(scope, "job")
                assert any(
                    j["payload"]["unit"].get("schema") == "question-page-refresh-unit/1"
                    for j in jobs
                )

    asyncio.run(run())


def test_scene_discovery_retains_first_cold_build_across_restart_and_does_not_invent_ready_output(
    store,
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            await register(svc)
            scenes = ScenarioEvolution(svc)
            registration = await scenes.register(
                "empty-context", ("project-a:owner",), readers=(ACTOR,)
            )
            pending = await scenes.discover_changes()
            assert registration["instance_id"] in pending
            with pytest.raises(DerivedError, match="unavailable"):
                await scenes.read("empty-context", actor=ACTOR)
            from agent_memory.derived.question_service import QuestionService

            resumed = ScenarioEvolution(QuestionService(svc.admission, svc.context))
            assert registration["instance_id"] in await resumed.discover_changes()
            host = RefreshHost(
                resumed.queue,
                worker_id="resumed-empty-scene",
                limits=WorkerLimits(max_concurrency=1),
                clock_tolerance_seconds=300,
            )
            for _ in range(5):
                await host.run_once()
                clock[0] += timedelta(seconds=2)
            current = await resumed.read("empty-context", actor=ACTOR)
            assert current["blocks"][0]["body"]["answer"]["answer_status"] == "unknown"
            assert not await resumed.discover_changes()

    asyncio.run(run())
