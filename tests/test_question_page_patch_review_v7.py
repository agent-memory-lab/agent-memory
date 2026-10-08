"""Independent Q7-23 review regressions for exact processed page inputs."""

import asyncio
import json

import pytest
import test_project_admission_v7 as project
from test_purge_restore import backup_copy, erase, replay, restorer
from test_question_page_patches_v7 import PAGE, patch, rows
from test_question_pages_v7 import build, setup
from test_question_runtime_v7 import ACTOR, fresh

from agent_memory.derived.model import DerivedError, ProcessingGrant, digest
from agent_memory.derived.question_page_patches import AppendQuestionBlock, RemoveQuestionBlock
from agent_memory.derived.question_pages import PAGE_KINDS

store = project.store


def test_patch_preserves_consumed_previous_page_validation_generation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            first = await build(svc, clock, templates=("owner",))
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            await fresh(svc, clock, dedupe="review:middle")
            middle = await svc.pages.publish(PAGE, actor=ACTOR)
            assert middle["rebuild"] == "proof_reuse"
            assert middle["revision_id"] == first["revision_id"]
            async with engine.repository.unit_of_work() as uow:
                previous_certificate = await uow.derived_get(
                    scope, "question_page_certificate", middle["certificate_revision_id"]
                )
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=2
            )
            await fresh(svc, clock, dedupe="review:latest")
            patched = await patch(
                svc, middle, [AppendQuestionBlock("review:extra", "project-a:owner")]
            )
            async with engine.repository.unit_of_work() as uow:
                content = await uow.derived_get(
                    scope, "question_page_content", patched["revision_id"]
                )
            processed = previous_certificate["validation_manifest"]
            recorded = content["generation_manifest"]
            assert {digest(ref) for ref in processed["inputs"]} <= {
                digest(ref) for ref in recorded["inputs"]
            }
            assert {digest(parent) for parent in processed["parents"].values()} <= {
                digest(parent) for parent in recorded["parents"].values()
            }

    asyncio.run(run())


def test_full_refresh_preserving_typed_layout_preserves_its_processing_inputs(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            first = await build(svc, clock, templates=("owner",))
            selected = await patch(
                svc, first, [AppendQuestionBlock("review:selected-layout", "project-a:owner")]
            )
            new = await project.stage(svc.admission, scope, identity="new-owner", value="Bob")
            await project.qualify(svc.admission, *new)
            await fresh(svc, clock, dedupe="review:changed-answer")
            rebuilt = await svc.pages.publish(PAGE, actor=ACTOR)
            assert rebuilt["rebuild"] == "full"
            assert [b["block_id"] for b in rebuilt["blocks"]] == [
                b["block_id"] for b in selected["blocks"]
            ]
            assert {digest(ref) for ref in selected["generation_manifest"]["inputs"]} <= {
                digest(ref) for ref in rebuilt["generation_manifest"]["inputs"]
            }

    asyncio.run(run())


def test_operation_plan_is_snapshotted_before_the_first_await(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            first = await build(svc, clock, templates=("owner",))
            operation = AppendQuestionBlock("review:original", "project-a:owner")
            operations = [operation]
            original = svc._clock_barrier
            injected = []

            async def mutate_plan(**options):
                if not injected:
                    injected.append(True)
                    operations.clear()
                    object.__setattr__(operation, "block_id", "review:mutated")
                    object.__setattr__(operation, "question_id", "unregistered")
                return await original(**options)

            svc._clock_barrier = mutate_plan
            try:
                result = await patch(svc, first, operations)
            finally:
                svc._clock_barrier = original
            assert injected
            assert result["blocks"][-1]["block_id"] == "review:original"
            assert result["blocks"][-1]["body"]["answer"]["question_id"] == "project-a:owner"

    asyncio.run(run())


def test_removed_private_block_remains_guarded_and_erased_from_real_backup(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            templates = ("owner", "commitments")
            svc, _ = await setup(engine, scope, clock, templates=templates)
            private = await project.stage(
                svc.admission,
                scope,
                identity="review:removed-private-source",
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="review:removed-private-body",
            )
            await project.qualify(svc.admission, *private)
            first = await build(svc, clock, templates=templates)
            removed = first["blocks"][1]
            assert "review:removed-private-body" in json.dumps(removed)
            selected = await patch(
                svc,
                first,
                [RemoveQuestionBlock(removed["block_id"], removed["revision_id"])],
            )
            assert len(selected["blocks"]) == 1
            await svc.admission.replace_membership(
                private[2], expected_version=4, membership_id="promise-b"
            )
            await svc.grant(
                ProcessingGrant(
                    private[0].id, (ACTOR,), ("project_questions",), revoked=True
                ),
                expected_version=1,
            )
            for name in templates:
                await fresh(svc, clock, name, dedupe="review:public:" + name)
            before = await rows(engine, scope)
            with pytest.raises(DerivedError, match="processing_denied"):
                await patch(
                    svc, selected, [AppendQuestionBlock("review:public", "project-a:owner")]
                )
            assert await rows(engine, scope) == before
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, private[0].id)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        for kind in PAGE_KINDS:
                            records = await uow.derived_records(scope, kind)
                            assert all(row["payload"].get("state") == "erased" for row in records)
                            assert "review:removed-private-body" not in json.dumps(records)
                            assert PAGE not in json.dumps(records)

    asyncio.run(run())


@pytest.mark.parametrize(
    "first_body", ["question_page_content", "question_page_certificate", "question_page_block"]
)
@pytest.mark.parametrize("change", ["grant_expiry", "context", "host"])
def test_patch_rechecks_controls_after_each_old_body_before_loading_another(
    store, first_body, change
):
    from dataclasses import replace
    from datetime import timedelta

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            expiry = clock[0] + timedelta(seconds=10)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",), expires_at=expiry),
                expected_version=1,
            )
            first = await build(svc, clock, templates=("owner",))
            before = await rows(engine, scope)
            original = engine.repository.unit_of_work
            fired, later_bodies = [], []

            class ChangingBody:
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
                    if fired and kind in {
                        "question_content",
                        "question_certificate",
                        "question_page_content",
                        "question_page_certificate",
                        "question_page_block",
                    }:
                        later_bodies.append(kind)
                    value = await self.uow.derived_get(scope, kind, key)
                    if kind == first_body and not fired:
                        fired.append(True)
                        if change == "grant_expiry":
                            clock[0] = expiry
                        elif change == "context":
                            svc.context = replace(svc.context, revision="review:changed-context")
                        else:
                            svc.admission.memberships["a"] = replace(
                                project.MEMBERSHIPS[0], registry_revision="review:changed-host"
                            )
                    return value

            engine.repository.unit_of_work = ChangingBody
            try:
                with pytest.raises(DerivedError):
                    await patch(svc, first, [AppendQuestionBlock("review:new", "project-a:owner")])
                assert fired
                assert not later_bodies, "A later body loaded after patch controls changed"
            finally:
                engine.repository.unit_of_work = original
            assert await rows(engine, scope) == before

    asyncio.run(run())
