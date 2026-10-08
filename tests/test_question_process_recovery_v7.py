"""B6: actual full/delta/proof-reuse QuestionView commit death on both providers."""

import asyncio
import signal
from datetime import timedelta
from pathlib import Path

import pytest
import test_project_admission_v7 as project
import test_refresh_scheduler_process as processes
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.derived.model import ProcessingGrant

store = project.store
pytestmark = pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="requires real SIGKILL")
KINDS = ("question_content", "question_certificate", "question_head", "question_delta_state")


async def artifacts(repository, scope):
    async with repository.unit_of_work() as uow:
        return {kind: await uow.derived_records(scope, kind) for kind in KINDS}


@pytest.mark.parametrize("phase", ["before_publication_commit", "after_publication_commit"])
@pytest.mark.parametrize("compute_mode", ["full", "delta", "proof_reuse"])
def test_question_commit_sigkill_recovers_all_state_without_duplicate_publication(
    store, tmp_path, monkeypatch, phase, compute_mode
):
    monkeypatch.setattr(
        processes, "CHILD", Path(__file__).parent / "fixtures/question_crash_child.py"
    )

    async def run():
        async with store() as (engine, kernel, scope, clock):
            repository = engine.repository
            service = runtime(engine, scope, clock)
            item = await project.stage(service.admission, scope)
            await project.qualify(service.admission, *item)
            await register(service)
            initial = None
            if compute_mode != "full":
                initial = await fresh(service, clock)
                if compute_mode == "delta":
                    new = await project.stage(service.admission, scope, identity="new", value="Bob")
                    await project.qualify(service.admission, *new)
                else:
                    await service.grant(
                        ProcessingGrant("source", (ACTOR,), ("project_questions",)),
                        expected_version=1,
                    )
            # Admission commits use monotonic microsecond knowledge boundaries.
            clock[0] += timedelta(microseconds=100)
            target = await service.request("project-a:owner", actor=ACTOR, dedupe_key="crash")
            before = await artifacts(repository, scope)
            base_publications = await processes.records(repository, scope, "refresh_publication")
            async with processes.child(
                repository, scope, clock, tmp_path, phase, context=service.context.payload()
            ) as process:
                claimed = await processes.event(process, "claimed")
                execution = claimed["lease"]["task"]["payload"]["refresh_execution"]
                await processes.release(process)
                boundary = await processes.event(process, "boundary")
                assert boundary["compute_mode"] == compute_mode
                await processes.kill(process)
            committed = phase == "after_publication_commit"
            after = await artifacts(repository, scope)
            publications = await processes.records(repository, scope, "refresh_publication")
            assert len(publications) == len(base_publications) + int(committed)
            if not committed:
                assert after == before, "uncommitted content, certificate, head or delta leaked"
            else:
                assert len(after["question_certificate"]) == len(before["question_certificate"]) + 1
                assert any(row["id"] == execution for row in publications)
            assert (await service.queue.status(target["target_id"], actor=ACTOR))["complete"] == (
                committed
            )
            # Recover only from durable state, in another brand-new interpreter.
            clock[0] += timedelta(seconds=6)
            recovered = await processes.drain(
                repository, scope, clock, tmp_path, context=service.context.payload()
            )
            assert len(recovered) == int(not committed)
            assert (await service.queue.status(target["target_id"], actor=ACTOR))["complete"]
            result = await service.read("project-a:owner", actor=ACTOR)
            assert result["availability_status"] == "valid"
            assert result["compute_mode"] == compute_mode
            final = await artifacts(repository, scope)
            assert len(final["question_content"]) == (2 if compute_mode == "delta" else 1)
            assert len(final["question_certificate"]) == (1 if initial is None else 2)
            if compute_mode == "proof_reuse":
                assert result["content_revision_id"] == initial["content_revision_id"]
                assert result["generation_manifest"] == initial["generation_manifest"]
            publications = await processes.records(repository, scope, "refresh_publication")
            assert len(publications) == len(base_publications) + 1
            assert await processes.drain(
                repository, scope, clock, tmp_path, context=service.context.payload()
            ) == []
            assert await artifacts(repository, scope) == final
            assert await processes.records(repository, scope, "refresh_publication") == publications

    asyncio.run(run())
