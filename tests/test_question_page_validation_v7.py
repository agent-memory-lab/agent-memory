"""Bounded source-sensitive downstream validation never stops at value equality."""

import asyncio

import pytest
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.derived.model import DerivedError, ProcessingGrant

store = project.store


async def setup(engine, scope, clock, count=1):
    svc = runtime(engine, scope, clock)
    item = await project.stage(svc.admission, scope)
    await project.qualify(svc.admission, *item)
    await register(svc)
    await fresh(svc, clock)
    for i in range(count):
        await svc.pages.register("page:" + str(i), ("project-a:owner",), readers=(ACTOR,))
        await svc.pages.publish("page:" + str(i), actor=ACTOR)
    return svc, item


def test_noop_parent_keeps_immutable_page_generation_but_requires_bounded_certificate_work(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _ = await setup(engine, scope, clock, count=2)
            first = {key: await svc.pages.read(key, actor=ACTOR) for key in ("page:0", "page:1")}
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            parent = await fresh(svc, clock, dedupe="noop")
            assert parent["compute_mode"] == "proof_reuse"
            for key in first:
                with pytest.raises(DerivedError, match="stale"):
                    await svc.pages.read(key, actor=ACTOR)
            one = await svc.pages.validate_pending(actor=ACTOR, max_pages=1)
            assert len(one) == 1 and one[0]["state"] == "valid"
            assert one[0]["rebuild"] == "proof_reuse"
            key = one[0]["page_id"]
            next_page = await svc.pages.read(key, actor=ACTOR)
            assert first[key]["revision_id"] == next_page["revision_id"]
            assert first[key]["generation_manifest"] == next_page["generation_manifest"]
            assert first[key]["certificate_revision_id"] != next_page["certificate_revision_id"]
            assert next_page["blocks"][0]["body"]["answer"] == parent
            async with engine.repository.unit_of_work() as uow:
                pending = await uow.derived_records(scope, "question_page_validation")
                assert sum(r["payload"]["state"] == "validation_pending" for r in pending) == 1
                content = await uow.derived_get(
                    scope, "question_page_content", next_page["revision_id"]
                )
                cert = await uow.derived_get(
                    scope, "question_page_certificate", next_page["certificate_revision_id"]
                )
                assert content["generation_manifest"] == cert["generation_manifest"]
                assert cert["validation_manifest"] != cert["generation_manifest"]
            assert len(await svc.pages.validate_pending(actor=ACTOR, max_pages=1)) == 1
            assert await svc.pages.validate_pending(actor=ACTOR, max_pages=1) == []

    asyncio.run(run())


def test_source_sensitive_page_rebuilds_same_business_value_new_evidence(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, item = await setup(engine, scope, clock)
            first = await svc.pages.read("page:0", actor=ACTOR)
            before = await svc.read("project-a:owner", actor=ACTOR)
            new = await project.stage(svc.admission, scope, identity="new", value="Alice")
            await project.qualify(svc.admission, *new)
            await svc.admission.withdraw(
                item[2], expected_version=4, review_id="withdraw", reasons=("superseded",)
            )
            after = await fresh(svc, clock, dedupe="new-evidence")
            assert before["digests"]["value"] == after["digests"]["value"]
            assert before["digests"]["structure"] != after["digests"]["structure"]
            result = await svc.pages.validate_pending(actor=ACTOR)
            assert result == [{"page_id": "page:0", "state": "valid", "rebuild": "full"}]
            second = await svc.pages.read("page:0", actor=ACTOR)
            assert second["revision_id"] != first["revision_id"]
            assert second["blocks"][0]["body"]["answer"]["citations"] == after["citations"]
            async with engine.repository.unit_of_work() as uow:
                old = await uow.derived_get(scope, "question_page_content", first["revision_id"])
                assert (
                    old["generation_manifest"]["inputs"] == first["generation_manifest"]["inputs"]
                )

    asyncio.run(run())


def test_revoked_page_original_certificate_blocks_prebody_even_after_parent_new_full(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            public = await project.stage(svc.admission, scope, identity="public")
            private = await project.stage(
                svc.admission,
                scope,
                identity="private",
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="secret",
            )
            for item in (public, private):
                await project.qualify(svc.admission, *item)
            await register(svc)
            await fresh(svc, clock)
            await svc.pages.register("page", ("project-a:owner",), readers=(ACTOR,))
            first = await svc.pages.publish("page", actor=ACTOR)
            await svc.admission.replace_membership(
                private[2], expected_version=4, membership_id="promise-b"
            )
            await svc.grant(
                ProcessingGrant("private", (ACTOR,), ("project_questions",), revoked=True),
                expected_version=1,
            )
            await svc.request("project-a:owner", actor=ACTOR, dedupe_key="new-full")
            lease = await svc.queue.claim("worker", lease_seconds=30)
            await svc.queue.apply(lease.task)
            await svc.queue.complete(lease)
            with pytest.raises(DerivedError):
                await svc.pages.read("page", actor=ACTOR)
            result = await svc.pages.validate_pending(actor=ACTOR)
            assert result == [{"page_id": "page", "state": "valid", "rebuild": "full"}]
            second = await svc.pages.read("page", actor=ACTOR)
            assert first["revision_id"] != second["revision_id"]
            answer = second["blocks"][0]["body"]["answer"]
            assert {s["source_event_id"] for s in answer["processing_references"]} == {"public"}
            assert "secret" not in str(second["blocks"])

    asyncio.run(run())
