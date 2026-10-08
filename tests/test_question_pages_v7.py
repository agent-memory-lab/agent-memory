"""Full current L2 project pages preserve every actual QuestionView input."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_purge_restore import backup_copy, erase, replay, restorer
from test_question_runtime_v7 import ACTOR, fresh, register, runtime
from test_question_transport_v7 import client_for

from agent_memory.derived.model import DerivedError, ProcessingGrant, digest
from agent_memory.derived.question_pages import PAGE_KINDS
from agent_memory.derived.question_service import QuestionService

store = project.store


async def setup(engine, scope, clock, *, templates=("owner", "status", "commitments", "risks")):
    svc = runtime(engine, scope, clock)
    item = await project.stage(svc.admission, scope)
    await project.qualify(svc.admission, *item)
    for name in templates:
        await register(svc, name)
    await svc.pages.register("project-a-overview", ["project-a:" + q for q in templates],
                             readers=(ACTOR,))
    return svc, item


async def build(svc, clock, *, templates=("owner", "status", "commitments", "risks")):
    for name in templates:
        await fresh(svc, clock, name, dedupe=name)
    return await svc.pages.publish("project-a-overview", actor=ACTOR)


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_full_project_page_and_transport_preserve_parent_answers_and_lineage(store, transport):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock)
            with pytest.raises(DerivedError, match="parent_unavailable"):
                await svc.pages.publish("project-a-overview", actor=ACTOR)
            answer = await build(svc, clock)
            assert len(answer["blocks"]) == 4 and answer["model_calls"] == 0
            assert answer["rebuild"] == "full"
            assert len(answer["generation_manifest"]["inputs"]) == 8
            for block in answer["blocks"]:
                question = block["body"]["answer"]
                assert await svc.read(question["question_id"], actor=ACTOR) == question
                assert question["generation_manifest"]["inputs"]
                assert question["processing_references"]
            again = await svc.pages.publish("project-a-overview", actor=ACTOR)
            assert again == answer
            async with client_for(kernel, scope, svc, transport) as client:
                assert (await client.question_page_read("project-a-overview")) == answer
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "question_page_content")) == 1
                assert len(await uow.derived_records(scope, "question_page_block")) == 4

    asyncio.run(run())


def test_parent_change_invalidates_whole_page_and_full_rebuild_keeps_stable_block_ids(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock)
            first = await build(svc, clock)
            new = await project.stage(svc.admission, scope, identity="second", value="Bob")
            await project.qualify(svc.admission, *new)
            with pytest.raises(DerivedError, match="stale"):
                await svc.pages.read("project-a-overview", actor=ACTOR)
            for name in ("owner", "status", "commitments", "risks"):
                await fresh(svc, clock, name, dedupe="second:" + name)
            with pytest.raises(DerivedError, match="stale"):
                await svc.pages.read("project-a-overview", actor=ACTOR)
            second = await svc.pages.publish("project-a-overview", actor=ACTOR)
            assert first["revision_id"] != second["revision_id"]
            assert ([b["block_id"] for b in first["blocks"]]
                    == [b["block_id"] for b in second["blocks"]])
            assert second["blocks"][0]["body"]["answer"]["answer_status"] == "contested"
            # Replacement never relabels previous processing inputs as the new generation.
            async with engine.repository.unit_of_work() as uow:
                old = await uow.derived_get(scope, "question_page_content", first["revision_id"])
                internal = old["generation_manifest"]
                public = first["generation_manifest"]
                assert public["schema"] == "question-page-generation-references/1"
                assert internal["inputs"] == public["inputs"]
                assert public["parents"] == {
                    key: {"sha256": digest(value)} for key, value in internal["parents"].items()
                }

    asyncio.run(run())


@pytest.mark.parametrize("all_in_scope", [False, True])
@pytest.mark.parametrize("published", [False, True])
def test_page_erase_and_real_backup_replay_scrub_even_unbuilt_metadata(
    store, tmp_path, all_in_scope, published,
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, item = await setup(engine, scope, clock, templates=("owner",))
            if published:
                await build(svc, clock, templates=("owner",))
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, item[0].id, all_in_scope=all_in_scope)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        for kind in PAGE_KINDS:
                            rows = await uow.derived_records(scope, kind)
                            assert "private-project-marker" not in json.dumps(rows)
                            assert "project-a-overview" not in json.dumps(rows)
                            assert all(r["payload"]["state"] == "erased" for r in rows)
            with pytest.raises(DerivedError, match="page_unavailable"):
                await svc.pages.read("project-a-overview", actor=ACTOR)

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["capacity", "partial_write"])
def test_full_materialized_budget_and_failed_commit_publish_nothing(store, failure):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            await fresh(svc, clock)
            if failure == "capacity":
                await svc.pages.register("project-a-overview", ("project-a:owner",),
                                         readers=(ACTOR,), expected_generation=1,
                                         max_output_bytes=1024)
            original = engine.repository.unit_of_work

            class FailingUow:
                def __init__(self):
                    self.inner = original()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_put(self, scope, kind, key, value):
                    if kind == "question_page_certificate":
                        raise DerivedError("injected_page_failure")
                    return await self.uow.derived_put(scope, kind, key, value)

            if failure == "partial_write":
                engine.repository.unit_of_work = FailingUow
            try:
                with pytest.raises(DerivedError, match="output_capacity|injected_page_failure"):
                    await svc.pages.publish("project-a-overview", actor=ACTOR)
            finally:
                engine.repository.unit_of_work = original
            async with engine.repository.unit_of_work() as uow:
                for kind in PAGE_KINDS[1:]:
                    assert not await uow.derived_records(scope, kind)

    asyncio.run(run())


def test_page_permissions_guard_metadata_before_any_stored_body_and_final_expiry(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            await build(svc, clock, templates=("owner",))
            original = engine.repository.unit_of_work
            bodies, mode = [], ["deny"]

            class ObservingUow:
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
                    value = await self.uow.derived_get(scope, kind, key)
                    if kind in {"question_page_content", "question_page_block"}:
                        bodies.append(kind)
                        if mode[0] == "expire":
                            clock[0] = svc.context.expires_at
                    return value

            await svc.grant(ProcessingGrant("source", (ACTOR,), revoked=True), expected_version=1)
            engine.repository.unit_of_work = ObservingUow
            try:
                with pytest.raises(DerivedError, match="processing_denied"):
                    await svc.pages.read("project-a-overview", actor=ACTOR)
                assert not bodies
            finally:
                engine.repository.unit_of_work = original
            await svc.grant(ProcessingGrant("source", (ACTOR,), ("project_questions",)),
                            expected_version=2)
            await fresh(svc, clock, dedupe="new-permission")
            await svc.pages.publish("project-a-overview", actor=ACTOR)
            mode[0] = "expire"
            engine.repository.unit_of_work = ObservingUow
            try:
                with pytest.raises(DerivedError, match="expired"):
                    await svc.pages.read("project-a-overview", actor=ACTOR)
                assert bodies
            finally:
                engine.repository.unit_of_work = original

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["read", "publish"])
@pytest.mark.parametrize("change", ["membership", "clock_rollback"])
def test_page_final_parent_await_rechecks_host_registration_and_clock(store, operation, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            await build(svc, clock, templates=("owner",))
            clock[0] += timedelta(seconds=10)
            if operation == "publish":
                await svc.pages.register("project-a-overview", ("project-a:owner",),
                                         readers=(ACTOR,), expected_generation=1)
            async with engine.repository.unit_of_work() as uow:
                before = {kind: await uow.derived_records(scope, kind) for kind in PAGE_KINDS[1:]}
            original = engine.repository.unit_of_work
            state = {"final_guard": False, "changed": False}

            class ChangingUow:
                def __init__(self):
                    self.inner = original()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_put(self, scope, kind, key, value):
                    result = await self.uow.derived_put(scope, kind, key, value)
                    if operation == "publish" and kind == "question_page_head":
                        state["final_guard"] = True
                    return result

                async def derived_get(self, scope, kind, key):
                    value = await self.uow.derived_get(scope, kind, key)
                    if operation == "read" and kind == "question_page_block":
                        state["final_guard"] = True
                    if state["final_guard"] and not state["changed"] and kind == "barrier":
                        if change == "membership":
                            svc.admission.memberships["a"] = replace(
                                svc.admission.memberships["a"], registry_revision="registry/2"
                            )
                        else:
                            # Still inside the content's valid interval, but earlier
                            # than the request's already-observed durable clock.
                            clock[0] -= timedelta(seconds=1)
                        state["changed"] = True
                    return value

            engine.repository.unit_of_work = ChangingUow
            try:
                expected = ("registration_changed|page_stale" if change == "membership"
                            else "clock_discontinuity")
                with pytest.raises(DerivedError, match=expected):
                    await getattr(svc.pages, operation)("project-a-overview", actor=ACTOR)
                assert state["changed"]
            finally:
                engine.repository.unit_of_work = original
            if operation == "publish":
                async with engine.repository.unit_of_work() as uow:
                    after = {
                        kind: await uow.derived_records(scope, kind) for kind in PAGE_KINDS[1:]
                    }
                    assert after == before

    asyncio.run(run())


def test_failed_page_publication_preserves_observed_expiry_across_restart(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            await build(svc, clock, templates=("owner",))
            original = engine.repository.unit_of_work
            previous_time = clock[0]

            class ExpiringUow:
                def __init__(self):
                    self.inner = original()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_put(self, scope, kind, key, value):
                    result = await self.uow.derived_put(scope, kind, key, value)
                    if kind == "question_page_head":
                        clock[0] = svc.context.expires_at
                    return result

            engine.repository.unit_of_work = ExpiringUow
            try:
                with pytest.raises(DerivedError, match="expired"):
                    await svc.pages.publish("project-a-overview", actor=ACTOR)
            finally:
                engine.repository.unit_of_work = original
            clock[0] = previous_time + timedelta(seconds=1)
            restarted = QuestionService(project.service(engine, scope, clock), svc.context)
            with pytest.raises(DerivedError, match="clock_discontinuity"):
                await restarted.pages.read("project-a-overview", actor=ACTOR)

    asyncio.run(run())


def test_page_public_lineage_does_not_disclose_moved_foreign_candidate_metadata(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(
                svc.admission, scope, identity="foreign-private-source-marker",
                membership="promise-a", subject="promise-1", predicate="commitment.action",
                value="foreign-private-action-marker",
            )
            await project.qualify(svc.admission, *item)
            await svc.admission.replace_membership(
                item[2], expected_version=4, membership_id="promise-b"
            )
            await project.grant(engine.repository, scope, item[0].id, revoked=True)
            await register(svc, "commitments")
            await svc.pages.register("project-a-overview", ("project-a:commitments",),
                                     readers=(ACTOR,))
            answer = await fresh(svc, clock, "commitments")
            assert answer["answer_status"] == "empty"
            page = await svc.pages.publish("project-a-overview", actor=ACTOR)
            encoded = json.dumps(page)
            for private in (
                "project-b", "foreign-private-source-marker", "foreign-private-action-marker"
            ):
                assert private not in encoded
            manifest = page["generation_manifest"]
            assert manifest["schema"] == "question-page-generation-references/1"
            assert manifest["inputs"]
            async with engine.repository.unit_of_work() as uow:
                content = await uow.derived_get(scope, "question_page_content", page["revision_id"])
                original = content["generation_manifest"]
                assert "project-b" in json.dumps(original)
                assert original["inputs"] == manifest["inputs"]
                assert manifest["parents"] == {
                    key: {"sha256": digest(value)} for key, value in original["parents"].items()
                }

    asyncio.run(run())
