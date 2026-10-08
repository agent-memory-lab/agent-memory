"""Q7-23: typed host patches, immutable mixed lineage and real-provider atomicity."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_purge_restore import backup_copy, erase, replay, restorer
from test_question_pages_v7 import build, setup
from test_question_runtime_v7 import ACTOR, fresh
from test_question_transport_v7 import client_for

from agent_memory.derived.model import DerivedError, ProcessingGrant, digest
from agent_memory.derived.question_page_patches import (
    AppendQuestionBlock,
    InsertQuestionBlock,
    RemoveQuestionBlock,
    ReplaceQuestionBlock,
)
from agent_memory.derived.question_pages import PAGE_KINDS

store = project.store
PAGE = "project-a-overview"


async def patch(svc, page, operations, **kwargs):
    return await svc.pages.patch(
        PAGE,
        operations,
        actor=ACTOR,
        expected_revision_id=page["revision_id"],
        expected_certificate_revision_id=page["certificate_revision_id"],
        **kwargs,
    )


async def rows(engine, scope):
    async with engine.repository.unit_of_work() as uow:
        return {kind: await uow.derived_records(scope, kind) for kind in PAGE_KINDS}


def test_all_four_operations_preserve_exact_unchanged_block_revisions_and_read_surfaces(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock)
            first = await build(svc, clock)
            owner, status, commitment, risk = first["blocks"]
            before = await rows(engine, scope)
            changed = await patch(
                svc,
                first,
                [ReplaceQuestionBlock(owner["block_id"], owner["revision_id"], "project-a:status")],
            )
            assert changed["rebuild"] == "typed_patch"
            assert changed["blocks"][0]["block_id"] == owner["block_id"]
            assert changed["blocks"][0]["revision_id"] != owner["revision_id"]
            assert changed["blocks"][1:] == first["blocks"][1:]
            inserted = await patch(
                svc,
                changed,
                [
                    AppendQuestionBlock("extra-owner", "project-a:owner"),
                    InsertQuestionBlock(
                        "extra-risk", "project-a:risks", status["block_id"], status["revision_id"]
                    ),
                    RemoveQuestionBlock(commitment["block_id"], commitment["revision_id"]),
                ],
            )
            assert [b["block_id"] for b in inserted["blocks"]] == [
                owner["block_id"],
                "extra-risk",
                status["block_id"],
                risk["block_id"],
                "extra-owner",
            ]
            after = await rows(engine, scope)
            for item in before["question_page_block"]:
                assert item in after["question_page_block"]
            for block in inserted["blocks"]:
                answer = block["body"]["answer"]
                assert answer == await svc.read(answer["question_id"], actor=ACTOR)
            for transport in ("embedded", "mcp"):
                async with client_for(kernel, scope, svc, transport) as client:
                    assert await client.question_page_read(PAGE) == inserted
            # A parent proof refresh preserves the host-selected layout.
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            for name in ("owner", "status", "commitments", "risks"):
                await fresh(svc, clock, name, dedupe="noop:" + name)
            result = await svc.pages.validate_pending(actor=ACTOR)
            assert result[0]["rebuild"] == "proof_reuse"
            checked = await svc.pages.read(PAGE, actor=ACTOR)
            assert checked["revision_id"] == inserted["revision_id"]
            assert [b["revision_id"] for b in checked["blocks"]] == [
                b["revision_id"] for b in inserted["blocks"]
            ]

    asyncio.run(run())


@pytest.mark.parametrize(
    "failure",
    [
        "page",
        "certificate",
        "block",
        "anchor",
        "duplicate",
        "missing",
        "parent",
        "empty",
        "freeform",
        "overflow",
        "multi",
    ],
)
def test_precise_conflicts_and_failed_multi_operation_publish_nothing(store, failure):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            first = await build(svc, clock, templates=("owner",))
            block = first["blocks"][0]
            expected = dict(first)
            operation = AppendQuestionBlock("new", "project-a:owner")
            error = {
                "page": "page_revision_conflict",
                "certificate": "page_certificate_conflict",
                "block": "block_revision_conflict",
                "anchor": "block_revision_conflict",
                "duplicate": "block_id_conflict",
                "missing": "block_missing",
                "parent": "parent_unregistered",
                "empty": "block_capacity",
                "freeform": "invalid_question_page_patch",
                "overflow": "block_capacity",
                "multi": "block_revision_conflict",
            }[failure]
            if failure == "page":
                expected["revision_id"] = "stale"
            elif failure == "certificate":
                expected["certificate_revision_id"] = "stale"
            elif failure in {"block", "multi"}:
                operation = RemoveQuestionBlock(block["block_id"], "stale")
            elif failure == "anchor":
                operation = InsertQuestionBlock(
                    "new", "project-a:owner", block["block_id"], "stale"
                )
            elif failure == "duplicate":
                operation = AppendQuestionBlock(block["block_id"], "project-a:owner")
            elif failure == "missing":
                operation = RemoveQuestionBlock("absent", "stale")
            elif failure == "parent":
                operation = AppendQuestionBlock("new", "unregistered")
            elif failure == "empty":
                operation = RemoveQuestionBlock(block["block_id"], block["revision_id"])
            elif failure == "freeform":
                operation = {"op": "append", "body": "untrusted prose"}
            operations = [operation]
            if failure == "overflow":
                operations = [
                    AppendQuestionBlock("new:" + str(i), "project-a:owner") for i in range(16)
                ]
            if failure == "multi":
                operations.insert(0, AppendQuestionBlock("new", "project-a:owner"))
            before = await rows(engine, scope)
            with pytest.raises(DerivedError, match=error):
                await patch(svc, expected, operations)
            assert await rows(engine, scope) == before
            assert await svc.pages.read(PAGE, actor=ACTOR) == first

    asyncio.run(run())


def test_retained_stale_answers_fail_closed_instead_of_silently_replacing_other_blocks(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            first = await build(svc, clock, templates=("owner",))
            new = await project.stage(svc.admission, scope, identity="bob", value="Bob")
            await project.qualify(svc.admission, *new)
            await fresh(svc, clock, dedupe="changed")
            before = await rows(engine, scope)
            with pytest.raises(DerivedError, match="retained_block_stale"):
                await patch(svc, first, [AppendQuestionBlock("new", "project-a:owner")])
            assert await rows(engine, scope) == before
            block = first["blocks"][0]
            updated = await patch(
                svc,
                first,
                [ReplaceQuestionBlock(block["block_id"], block["revision_id"], "project-a:owner")],
            )
            assert updated["blocks"][0]["body"]["answer"]["answer_status"] == "contested"

    asyncio.run(run())


def test_same_parent_generations_remain_distinct_and_original_private_lineage_guards_prebody(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner", "status"))
            private = await project.stage(
                svc.admission,
                scope,
                identity="private-historical-input",
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="private-lineage-secret",
            )
            await project.qualify(svc.admission, *private)
            first = await build(svc, clock, templates=("owner", "status"))
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            for question in ("owner", "status"):
                await fresh(svc, clock, question, dedupe="proof:" + question)
            owner = first["blocks"][0]
            changed = await patch(
                svc,
                first,
                [ReplaceQuestionBlock(owner["block_id"], owner["revision_id"], "project-a:owner")],
            )
            assert changed["blocks"][1]["revision_id"] == first["blocks"][1]["revision_id"]
            async with engine.repository.unit_of_work() as uow:
                content = await uow.derived_get(
                    scope, "question_page_content", changed["revision_id"]
                )
                parents = content["generation_manifest"]["parents"]
                assert len(parents) == 4
                assert len({p["head"]["instance_id"] for p in parents.values()}) == 2
                assert set(parents) == {
                    "question-page-parent-generation:" + digest(p) for p in parents.values()
                }
            assert "private-historical-input" not in json.dumps(changed["generation_manifest"])
            assert "private-lineage-secret" not in json.dumps(changed["generation_manifest"])
            await svc.admission.replace_membership(
                private[2], expected_version=4, membership_id="promise-b"
            )
            await svc.grant(
                ProcessingGrant(
                    "private-historical-input", (ACTOR,), ("project_questions",), revoked=True
                ),
                expected_version=1,
            )
            for question in ("owner", "status"):
                await fresh(svc, clock, question, dedupe="public:" + question)
            original = engine.repository.unit_of_work
            bodies = []

            class Observing:
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
                    if kind in {"question_page_content", "question_page_block"}:
                        bodies.append(kind)
                    return await self.uow.derived_get(scope, kind, key)

            engine.repository.unit_of_work = Observing
            try:
                with pytest.raises(DerivedError, match="stale|processing_denied"):
                    await svc.pages.read(PAGE, actor=ACTOR)
                with pytest.raises(DerivedError, match="processing_denied"):
                    await patch(
                        svc,
                        changed,
                        [
                            RemoveQuestionBlock(
                                changed["blocks"][0]["block_id"],
                                changed["blocks"][0]["revision_id"],
                            )
                        ],
                    )
                assert not bodies
            finally:
                engine.repository.unit_of_work = original

    asyncio.run(run())


@pytest.mark.parametrize(
    "mutation",
    [
        "page_body",
        "parent_body",
        "published_body",
        "source_grant",
        "authority",
        "clock_rollback",
        "expiry",
        "partial_write",
    ],
)
def test_await_races_reject_atomically_and_keep_old_publication(store, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            first = await build(svc, clock, templates=("owner",))
            before = await rows(engine, scope)
            clock[0] += timedelta(seconds=5)
            original = engine.repository.unit_of_work
            injected = []

            class Racing:
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
                    if (
                        not injected
                        and kind == "question_page_block"
                        and mutation in {"page_body", "parent_body"}
                    ):
                        injected.append(True)
                        if mutation == "page_body":
                            corrupt = dict(value, body={"injected": "untrusted"})
                            await self.uow.derived_put(scope, kind, key, corrupt)
                        else:
                            answer = first["blocks"][0]["body"]["answer"]
                            key = answer["content_revision_id"]
                            body = await self.uow.derived_get(scope, "question_content", key)
                            await self.uow.derived_put(
                                scope, "question_content", key, dict(body, injected="untrusted")
                            )
                    return value

                async def derived_put(self, scope, kind, key, value):
                    result = await self.uow.derived_put(scope, kind, key, value)
                    if not injected and kind == "question_page_head":
                        injected.append(True)
                        if mutation == "published_body":
                            stored = await self.uow.derived_records(scope, "question_page_block")
                            changed = next(
                                row for row in stored if row["payload"]["block_id"] == "new"
                            )
                            await self.uow.derived_put(
                                scope,
                                "question_page_block",
                                changed["identity"],
                                dict(changed["payload"], body={"corrupt": True}),
                            )
                        elif mutation == "source_grant":
                            grant = await self.uow.derived_get(scope, "grant", "source")
                            await self.uow.derived_put(
                                scope, "grant", "source", dict(grant, revoked=True)
                            )
                        elif mutation == "authority":
                            svc.admission.authorities[project.AUTHORITY.source_id] = replace(
                                project.AUTHORITY, subjects=("another-project",)
                            )
                        elif mutation == "clock_rollback":
                            clock[0] -= timedelta(seconds=1)
                        elif mutation == "expiry":
                            clock[0] = svc.context.expires_at
                        elif mutation == "partial_write":
                            raise DerivedError("injected_patch_failure")
                    return result

            engine.repository.unit_of_work = Racing
            try:
                with pytest.raises(DerivedError):
                    await patch(svc, first, [AppendQuestionBlock("new", "project-a:owner")])
                assert injected
            finally:
                engine.repository.unit_of_work = original
            assert await rows(engine, scope) == before

    asyncio.run(run())


def test_real_materialized_response_capacity_rolls_back_with_references_and_metadata(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            first = await build(svc, clock, templates=("owner",))
            size = len(json.dumps(first, ensure_ascii=False, separators=(",", ":")).encode())
            await svc.pages.register(
                PAGE,
                ("project-a:owner",),
                readers=(ACTOR,),
                expected_generation=1,
                max_output_bytes=size + 256,
            )
            first = await svc.pages.publish(PAGE, actor=ACTOR)
            before = await rows(engine, scope)
            with pytest.raises(DerivedError, match="question_output_capacity"):
                await patch(svc, first, [AppendQuestionBlock("large", "project-a:owner")])
            assert await rows(engine, scope) == before
            assert await svc.pages.read(PAGE, actor=ACTOR) == first

    asyncio.run(run())


def test_actual_serialized_concurrent_patch_cas_has_one_winner(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            first = await build(svc, clock, templates=("owner",))
            results = await asyncio.gather(
                patch(svc, first, [AppendQuestionBlock("one", "project-a:owner")]),
                patch(svc, first, [AppendQuestionBlock("two", "project-a:owner")]),
                return_exceptions=True,
            )
            assert sum(isinstance(r, dict) for r in results) == 1
            loser = next(r for r in results if isinstance(r, Exception))
            assert (
                isinstance(loser, DerivedError) and loser.code == "question_page_revision_conflict"
            )
            after = await svc.pages.read(PAGE, actor=ACTOR)
            assert len(after["blocks"]) == 2
            all_rows = await rows(engine, scope)
            assert len(all_rows["question_page_content"]) == 2
            assert len(all_rows["question_page_block"]) == 2

    asyncio.run(run())


@pytest.mark.parametrize("target", ["source", "old_block", "new_block", "page"])
def test_patch_erasure_and_actual_backup_replay_include_all_original_revisions(
    store, tmp_path, target
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, item = await setup(engine, scope, clock, templates=("owner", "status"))
            first = await build(svc, clock, templates=("owner", "status"))
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            for name in ("owner", "status"):
                await fresh(svc, clock, name, dedupe="noop:" + name)
            block = first["blocks"][0]
            second = await patch(
                svc,
                first,
                [ReplaceQuestionBlock(block["block_id"], block["revision_id"], "project-a:owner")],
            )
            identity = dict(
                source=item[0].id,
                old_block=block["revision_id"],
                new_block=second["blocks"][0]["revision_id"],
                page=first["revision_id"],
            )[target]
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, identity)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        for kind in PAGE_KINDS:
                            records = await uow.derived_records(scope, kind)
                            assert all(row["payload"].get("state") == "erased" for row in records)
                            assert "private-project-marker" not in json.dumps(records)
                            assert PAGE not in json.dumps(records)
            with pytest.raises(DerivedError, match="page_unavailable"):
                await svc.pages.read(PAGE, actor=ACTOR)

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_typed_patch_capability_is_additive_but_transport_mutations_stay_closed(store, transport):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner",))
            async with client_for(kernel, scope, svc, transport) as client:
                caps = await client.question_capabilities()
                assert caps["page_patch"]["authority"] == "trusted_host_only"
                assert caps["page_patch"]["operations"] == ["append", "insert", "replace", "remove"]
                assert caps["page_patch"]["free_form"] is False
                for operation in ("patch", "page_patch", "page_publish", "page_register"):
                    with pytest.raises(sdk.MemoryClientError):
                        await client._call(
                            "memory_question", {"operation": operation, "payload": {}}
                        )

    asyncio.run(run())


def test_replace_only_changed_business_answer_keeps_other_current_block_exact(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, templates=("owner", "status"))
            transition = clock[0] + timedelta(hours=1)
            future = await project.stage(
                svc.admission, scope, identity="future-owner", value="Bob", valid_from=transition
            )
            await project.qualify(svc.admission, *future)
            first = await build(svc, clock, templates=("owner", "status"))
            assert first["blocks"][0]["body"]["answer"]["answer_status"] == "resolved"
            clock[0] = transition + timedelta(seconds=1)
            with pytest.raises(DerivedError):
                await svc.pages.read(PAGE, actor=ACTOR)
            owner = await fresh(svc, clock, "owner", dedupe="time:owner")
            status = await fresh(svc, clock, "status", dedupe="time:status")
            assert owner["answer_status"] == "contested"
            assert (
                status["content_revision_id"]
                == first["blocks"][1]["body"]["answer"]["content_revision_id"]
            )
            before = await rows(engine, scope)
            old_owner = first["blocks"][0]
            updated = await patch(
                svc,
                first,
                [
                    ReplaceQuestionBlock(
                        old_owner["block_id"], old_owner["revision_id"], "project-a:owner"
                    )
                ],
            )
            assert updated["blocks"][0]["body"]["answer"] == owner
            assert updated["blocks"][1]["body"]["answer"] == status
            assert updated["blocks"][1]["revision_id"] == first["blocks"][1]["revision_id"]
            after = await rows(engine, scope)
            old_status = next(
                row
                for row in before["question_page_block"]
                if row["identity"] == first["blocks"][1]["revision_id"]
            )
            assert old_status in after["question_page_block"]

    asyncio.run(run())
