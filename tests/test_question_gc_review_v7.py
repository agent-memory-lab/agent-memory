"""Independent retention counterexamples: lineage, collisions and atomic rollback."""

import asyncio
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_purge_restore import backup_copy, erase, replay, restorer
from test_question_page_patches_v7 import PAGE, patch
from test_question_page_patches_v7 import rows as page_rows
from test_question_pages_v7 import build, setup
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory.derived.model import ProcessingGrant
from agent_memory.derived.question_gc import QuestionRetentionPolicy
from agent_memory.derived.question_page_patches import RemoveQuestionBlock
from agent_memory.derived.question_pages import PAGE_KINDS
from agent_memory.operations.refresh_policy import RefreshPolicy

store = project.store


async def background(svc, clock):
    clock[0] += timedelta(microseconds=100)
    lease = await svc.queue.claim("gc-independent-review", lease_seconds=5)
    assert lease
    snapshot = await svc.snapshot(lease.task)
    await svc.publish(lease.task, snapshot, svc.prepare(snapshot))
    await svc.queue.complete(lease)
    return await svc.read("project-a:owner", actor=ACTOR)


async def obsolete_generation(engine, scope, clock):
    svc = runtime(engine, scope, clock)
    item = await project.stage(svc.admission, scope)
    await project.qualify(svc.admission, *item)
    await register(svc, refresh_policy=RefreshPolicy(mode="on_change", max_age_seconds=60))
    await background(svc, clock)
    clock[0] += timedelta(seconds=6)
    await svc.grant(
        ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1,
    )
    old = await background(svc, clock)
    clock[0] += timedelta(seconds=6)
    await svc.grant(
        ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=2,
    )
    current = await background(svc, clock)
    clock[0] += timedelta(seconds=61)
    assert old["certificate_revision_id"] != current["certificate_revision_id"]
    return svc, old, current


async def census(repo, scope):
    async with repo.unit_of_work() as uow:
        return await uow.derived_gc_snapshot(
            scope, max_records=32768, max_edges=131072, max_bytes=67108864
        )


def test_collision_with_root_identity_preserves_shared_dependency_owner(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, old, current = await obsolete_generation(engine, scope, clock)
            old_id = old["certificate_revision_id"]
            async with engine.repository.unit_of_work() as uow:
                await uow.lock_admission_scope(scope)
                # IDs are only unique within kind, while typed edge owners are
                # untyped. A root need not redundantly embed its own storage key.
                await uow.derived_put(scope, "barrier", old_id, {"epoch": 0})
                before_edges = (await uow.derived_gc_snapshot(
                    scope, max_records=32768, max_edges=131072, max_bytes=67108864
                ))["edges"]
            result = await svc.collect_garbage(QuestionRetentionPolicy("review:collision"))
            assert ("question_certificate", old_id) not in map(tuple, result["deleted"])
            after = await census(engine.repository, scope)
            assert [e for e in after["edges"] if e["revision_id"] == old_id] == [
                e for e in before_edges if e["revision_id"] == old_id
            ]
            assert (await svc.read("project-a:owner", actor=ACTOR))["content_revision_id"] == (
                current["content_revision_id"]
            )

    asyncio.run(run())


@pytest.mark.parametrize("fail_after", [1, 3])
def test_partial_gc_delete_failure_rolls_back_records_and_owned_edges(store, fail_after):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, old, _ = await obsolete_generation(engine, scope, clock)
            before = await census(engine.repository, scope)
            original = engine.repository.unit_of_work
            deleted = []

            class FailingDelete:
                def __init__(self):
                    self.inner = original()

                async def __aenter__(self):
                    self.uow = await self.inner.__aenter__()
                    return self

                async def __aexit__(self, *args):
                    return await self.inner.__aexit__(*args)

                def __getattr__(self, name):
                    return getattr(self.uow, name)

                async def derived_gc_delete(self, scope, kind, key):
                    await self.uow.derived_gc_delete(scope, kind, key)
                    deleted.append((kind, key))
                    if len(deleted) == fail_after:
                        raise RuntimeError("independent mid-delete fault")

            engine.repository.unit_of_work = FailingDelete
            try:
                with pytest.raises(RuntimeError, match="mid-delete fault"):
                    await svc.collect_garbage(QuestionRetentionPolicy("review:atomic"))
            finally:
                engine.repository.unit_of_work = original
            assert len(deleted) == fail_after
            assert await census(engine.repository, scope) == before
            result = await svc.collect_garbage(QuestionRetentionPolicy("review:atomic"))
            assert ("question_certificate", old["certificate_revision_id"]) in map(
                tuple, result["deleted"]
            )

    asyncio.run(run())


def test_removed_block_generation_survives_gc_then_erases_from_real_backup(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner", "commitments"))
            private = await project.stage(
                svc.admission, scope, identity="review:removed-block-source",
                membership="promise-a", subject="promise-1", predicate="commitment.action",
                value="review:retained-processing-private-body",
            )
            await project.qualify(svc.admission, *private)
            first = await build(svc, clock, templates=("owner", "commitments"))
            removed = first["blocks"][1]
            selected = await patch(
                svc, first, [RemoveQuestionBlock(removed["block_id"], removed["revision_id"])],
            )
            before = await page_rows(engine, scope)
            clock[0] += timedelta(seconds=31)
            await svc.collect_garbage(QuestionRetentionPolicy("review:page-generation"))
            assert await page_rows(engine, scope) == before
            assert await svc.pages.read(PAGE, actor=ACTOR) == selected
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, private[0].id)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        for kind in PAGE_KINDS:
                            entries = await uow.derived_records(scope, kind)
                            assert all(
                                row["payload"].get("state") == "erased" for row in entries
                            )

    asyncio.run(run())


@pytest.mark.parametrize(
    "root_kind", [None, "model_flight", "model_authorization", "model_cache_header"]
)
def test_model_roots_preserve_consumed_question_certificate_without_model_calls(store, root_kind):
    from test_question_models_v7 import configured_question

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock)
            first = await svc.read("project-a:owner", actor=ACTOR)
            await svc.queue.configure(
                first["instance_id"], RefreshPolicy(mode="on_change", max_age_seconds=60)
            )
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=2,
            )
            middle = await background(svc, clock)
            sealed = await models.authority.prepare_question("project-a:owner", actor=ACTOR)
            assert not port.calls
            if root_kind == "model_flight":
                assert await models.answers._claim(sealed)
            elif root_kind == "model_authorization":
                async with engine.repository.unit_of_work() as uow:
                    await models.authority.record(
                        uow, sealed, "dispatch", payload_sha256=sealed.payload_sha256,
                        call_id="review:record-only-no-model-call",
                    )
            elif root_kind == "model_cache_header":
                # Use the real sealed generation/parents, without inference. A
                # cache metadata owner is independent of current-head liveness.
                import json
                async with engine.repository.unit_of_work() as uow:
                    await uow.lock_admission_scope(scope)
                    await uow.derived_put(scope, root_kind, sealed.key, {
                        "generation_manifest": json.loads(sealed.manifest_json),
                        "parents": models.authority.parents(sealed),
                    })
            if root_kind:
                async with engine.repository.unit_of_work() as uow:
                    roots_before = await uow.derived_records(scope, root_kind)
            clock[0] += timedelta(seconds=6)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=3,
            )
            await background(svc, clock)
            clock[0] += timedelta(seconds=61)
            report = await svc.collect_garbage(QuestionRetentionPolicy("review:model-pins"))
            middle_key = ("question_certificate", middle["certificate_revision_id"])
            assert (middle_key in map(tuple, report["deleted"])) == (root_kind is None)
            if root_kind:
                async with engine.repository.unit_of_work() as uow:
                    assert await uow.derived_records(scope, root_kind) == roots_before
                    assert await uow.derived_get(scope, *middle_key)
            assert not port.calls and not await ledger.snapshot()

    asyncio.run(run())


def test_real_unreceipted_delta_chain_keeps_processing_ancestry_and_backpressure(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            await register(
                svc, refresh_policy=RefreshPolicy(mode="on_change", max_age_seconds=60)
            )
            revisions = [await background(svc, clock)]
            for index in range(3):
                clock[0] += timedelta(seconds=6)
                item = await project.stage(
                    svc.admission, scope, identity=f"review:delta-source:{index}",
                    value=f"Owner {index}",
                )
                await project.qualify(svc.admission, *item)
                revisions.append(await background(svc, clock))
            assert all(row["compute_mode"] == "delta" for row in revisions[1:])
            clock[0] += timedelta(seconds=61)
            before = await census(engine.repository, scope)
            report = await svc.collect_garbage(QuestionRetentionPolicy("review:delta-lineage"))
            assert report["deleted"] == []
            assert report["reason"] == "question_gc_references_retained"
            assert report["retained"]["question_content"] == len(revisions)
            assert await census(engine.repository, scope) == before
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "coverage_request")
                assert not await uow.derived_records(scope, "request")
            assert (await svc.read("project-a:owner", actor=ACTOR))["content_revision_id"] == (
                revisions[-1]["content_revision_id"]
            )

    asyncio.run(run())
