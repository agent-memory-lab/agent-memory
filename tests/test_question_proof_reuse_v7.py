"""Q7-21/22 runtime: distinct certificates cannot wash immutable generation inputs."""

import asyncio

import pytest
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, fresh, lease_snapshot, register, runtime

from agent_memory.derived.model import DerivedError, ProcessingGrant, digest

store = project.store


async def setup(engine, scope, clock):
    svc = runtime(engine, scope, clock)
    item = await project.stage(svc.admission, scope)
    await project.qualify(svc.admission, *item)
    await register(svc)
    return svc, item, await fresh(svc, clock)


def test_safe_noop_advances_actual_coverage_and_certificate_but_not_generation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, item, first = await setup(engine, scope, clock)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            with pytest.raises(DerivedError, match="stale"):
                await svc.read("project-a:owner", actor=ACTOR)
            second = await fresh(svc, clock, dedupe="validate")
            assert first["content_revision_id"] == second["content_revision_id"]
            assert first["generation_manifest"] == second["generation_manifest"]
            assert first["certificate_revision_id"] != second["certificate_revision_id"]
            assert first["coverage"] != second["coverage"]
            assert first["digests"]["safety"] != second["digests"]["safety"]
            assert first["digests"]["validation"] != second["digests"]["validation"]
            assert second["compute_mode"] == "proof_reuse"
            assert second["compute_trace"]["groups_evaluated"] == 0
            assert first["result"]["snapshot_id"] != second["result"]["snapshot_id"]
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "question_content")) == 1
                assert len(await uow.derived_records(scope, "question_certificate")) == 2
                jobs = await uow.derived_records(scope, "job")
                assert any(j["payload"]["outcome"] == "noop" for j in jobs)
                metadata = await svc.model_input_header(uow, "project-a:owner", actor=ACTOR)
                assert metadata["source_ids"] == ["source"]
                assert metadata["original_generation_manifest"] == first["generation_manifest"]
                assert metadata["header"]["generation_proof"]["sources"][0]["grant_version"] == 1
                assert metadata["header"]["proof"]["sources"][0]["grant_version"] == 2

    asyncio.run(run())


def test_changed_citations_need_new_render_even_when_business_values_match(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, item, first = await setup(engine, scope, clock)
            # Re-review unchanged business facts gives a new review/known-time
            # explanation. It cannot be certified by comparing displayed Alice.
            new = await project.stage(svc.admission, scope, identity="new-citation", value="Alice")
            await project.qualify(svc.admission, *new)
            await svc.admission.withdraw(
                item[2], expected_version=4, review_id="withdraw-old", reasons=("superseded",)
            )
            second = await fresh(svc, clock, dedupe="evidence-change")
            assert (
                first["result"]["rows"][0]["fields"][0]["known_values"]
                == (second["result"]["rows"][0]["fields"][0]["known_values"])
            )
            assert first["content_revision_id"] != second["content_revision_id"]
            assert first["digests"]["value"] == second["digests"]["value"]
            assert first["digests"]["structure"] != second["digests"]["structure"]
            assert second["compute_mode"] == "delta"

    asyncio.run(run())


def test_revoked_original_private_source_denied_prebody_then_independent_full_public_context(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock)
            public = await project.stage(svc.admission, scope, identity="public", value="Alice")
            await project.qualify(svc.admission, *public)
            item = await project.stage(
                svc.admission,
                scope,
                identity="source",
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="private",
            )
            await project.qualify(svc.admission, *item)
            await register(svc)
            first = await fresh(svc, clock)
            await svc.admission.replace_membership(
                item[2], expected_version=4, membership_id="promise-b"
            )
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",), revoked=True),
                expected_version=1,
            )
            original = engine.repository.unit_of_work
            bodies = []

            class Observed:
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
                    if kind in {"question_content", "question_delta_state"}:
                        bodies.append(kind)
                    return await self.uow.derived_get(scope, kind, key)

            engine.repository.unit_of_work = Observed
            try:
                with pytest.raises(DerivedError, match="processing_denied"):
                    await svc.read("project-a:owner", actor=ACTOR)
                assert not bodies
                _, lease, snapshot, prepared = await lease_snapshot(
                    svc, clock, dedupe="independent"
                )
                assert not bodies
                assert prepared["trace"]["compute_mode"] == "full"
                assert not prepared["reused"]
                await svc.publish(lease.task, snapshot, prepared)
                await svc.queue.complete(lease)
            finally:
                engine.repository.unit_of_work = original
            second = await svc.read("project-a:owner", actor=ACTOR)
            assert second["content_revision_id"] != first["content_revision_id"]
            sources = [
                i["id"] for i in second["generation_manifest"]["inputs"] if i["kind"] == "source"
            ]
            assert sources == ["public"]
            async with engine.repository.unit_of_work() as uow:
                old = await uow.derived_get(scope, "question_content", first["content_revision_id"])
                assert old["generation_manifest"] == first["generation_manifest"]

    asyncio.run(run())


@pytest.mark.parametrize("target", ["state", "logs", "previous_content", "generation_safe"])
def test_forged_optimization_inputs_cannot_cross_atomic_publish(store, target):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, _, first = await setup(engine, scope, clock)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=1
            )
            _, lease, snapshot, prepared = await lease_snapshot(svc, clock, dedupe="tamper")
            if target == "state":
                snapshot["delta_state"]["rows"] = {}
                snapshot["delta_state"]["sha256"] = digest(
                    {k: v for k, v in snapshot["delta_state"].items() if k != "sha256"}
                )
            elif target == "logs":
                snapshot["change_logs"] = {}
            elif target == "previous_content":
                snapshot["previous_content"] = None
            else:
                snapshot["generation_safe"] = False
            with pytest.raises(DerivedError, match="input_changed|invalid_question_fields"):
                await svc.publish(lease.task, snapshot, svc.prepare(snapshot))
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "question_content")) == 1
                assert len(await uow.derived_records(scope, "question_certificate")) == 1

    asyncio.run(run())
