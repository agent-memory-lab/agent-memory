import asyncio
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_purge_restore import erase
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.derived.question_history import KINDS

store = project.store


def test_published_points_survive_business_changes_but_enforce_current_grants(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_points=True)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            first = await fresh(svc, clock)
            known = clock[0]
            answer = await svc.read("project-a:owner", actor=ACTOR, known_at=known, valid_at=known)
            assert answer["result"] == first["result"]
            assert answer["historical"]["mode"] == "published_point"
            new = await project.stage(svc.admission, scope, identity="later", value="Bob")
            await project.qualify(svc.admission, *new)
            await fresh(svc, clock, dedupe="later")
            assert (await svc.read("project-a:owner", actor=ACTOR, known_at=known, valid_at=known))[
                "result"
            ] == first["result"]
            with pytest.raises(DerivedError, match="point_unavailable"):
                await svc.read(
                    "project-a:owner",
                    actor=ACTOR,
                    known_at=known + timedelta(microseconds=1),
                    valid_at=known,
                )
            with pytest.raises(DerivedError, match="time_coverage"):
                await svc.read(
                    "project-a:owner",
                    actor=ACTOR,
                    known_at=known,
                    valid_at=known - timedelta(seconds=1),
                )
            await svc.grant(
                ProcessingGrant(item[0].id, (ACTOR,), (svc.admission.purpose,), revoked=True),
                expected_version=1,
            )
            with pytest.raises(DerivedError, match="processing_denied"):
                await svc.read("project-a:owner", actor=ACTOR, known_at=known, valid_at=known)

    asyncio.run(run())


def test_empty_point_and_page_capture_are_bounded_and_erased(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_points=True)
            await register(svc)
            await fresh(svc, clock)
            empty_known = clock[0]
            assert (
                await svc.read(
                    "project-a:owner", actor=ACTOR, known_at=empty_known, valid_at=empty_known
                )
            )["answer_status"] == "unknown"
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await fresh(svc, clock, dedupe="nonempty")
            await svc.pages.register("overview", ["project-a:owner"], readers=(ACTOR,))
            page = await svc.pages.publish("overview", actor=ACTOR)
            key = await svc.history.capture("overview", actor=ACTOR, kind="page")
            known = clock[0]
            old = await svc.history.read(
                "overview", actor=ACTOR, kind="page", known_at=known, valid_at=known
            )
            assert old["blocks"] == page["blocks"]
            await erase(kernel, scope, item[0].id)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, KINDS[0], key) == {"state": "erased"}
                assert await uow.derived_get(scope, KINDS[1], key) == {"state": "erased"}

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_historical_delivery_never_substitutes_current_question_or_page(store, transport):
    from test_question_transport_v7 import client_for

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_points=True)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(svc)
            first = await fresh(svc, clock)
            known = clock[0]
            # An explicit capture after the worker's completion is idempotent.
            await svc.history.capture("project-a:owner", actor=ACTOR)
            await svc.pages.register("overview", ["project-a:owner"], readers=(ACTOR,))
            page = await svc.pages.publish("overview", actor=ACTOR)
            await svc.history.capture("overview", actor=ACTOR, kind="page")
            new = await project.stage(svc.admission, scope, identity="later", value="Bob")
            await project.qualify(svc.admission, *new)
            await fresh(svc, clock, dedupe="newer")
            async with client_for(kernel, scope, svc, transport) as client:
                caps = await client.question_capabilities()
                assert caps["historical"] and caps["historical_mode"] == "published_point"
                historical = await client.question_read(
                    "project-a:owner", valid_at=known.isoformat(), known_at=known.isoformat()
                )
                assert historical["result"] == first["result"] and historical["historical"]
                old_page = await client.question_page_read(
                    "overview", valid_at=known.isoformat(), known_at=known.isoformat()
                )
                assert old_page["blocks"] == page["blocks"] and old_page["historical"]

    asyncio.run(run())
