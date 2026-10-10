"""Rebuild real bitemporal project censuses, with current delivery permissions."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_purge_restore import erase
from test_question_runtime_v7 import ACTOR, fresh, register, runtime

from agent_memory.derived.model import DerivedError, ProcessingGrant

store = project.store


def test_arbitrary_history_replays_pending_qualified_and_withdrawn_versions(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            floor = clock[0]
            empty = await svc.read("project-a:owner", actor=ACTOR, known_at=floor, valid_at=floor)
            assert empty["answer_status"] == "unknown"
            clock[0] += timedelta(seconds=1)
            item = await project.stage(svc.admission, scope)
            pending_at = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            await project.qualify(svc.admission, *item)
            qualified_at = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            await svc.admission.withdraw(
                item[2], expected_version=4, review_id="withdraw", reasons=("ended",)
            )
            clock[0] += timedelta(seconds=1)
            pending = await svc.read(
                "project-a:owner", actor=ACTOR, known_at=pending_at, valid_at=floor
            )
            qualified = await svc.read(
                "project-a:owner", actor=ACTOR, known_at=qualified_at, valid_at=floor
            )
            withdrawn = await svc.read(
                "project-a:owner", actor=ACTOR, known_at=clock[0], valid_at=floor
            )
            assert pending["answer_status"] == "incomplete"
            assert qualified["answer_status"] == "resolved"
            assert qualified["result"]["rows"][0]["fields"][0]["known_values"] == ["Alice"]
            assert withdrawn["answer_status"] == "unknown"
            assert qualified["historical"]["mode"] == "ledger_rebuild"
            assert qualified["model_calls"] == 0
            with pytest.raises(DerivedError, match="coverage_floor"):
                await svc.read(
                    "project-a:owner",
                    actor=ACTOR,
                    known_at=floor - timedelta(microseconds=1),
                    valid_at=floor,
                )

    asyncio.run(run())


def test_missing_ledger_version_refuses_coverage_instead_of_returning_a_partial_past(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            clock[0] += timedelta(seconds=1)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            known = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            async with engine.repository.unit_of_work() as uow:
                if type(uow).__module__.startswith("agent_memory_postgres"):
                    await uow.connection.execute(
                        "DELETE FROM agent_memory_admission_versions "
                        "WHERE record_id=%s AND version=2",
                        (item[2],),
                    )
                else:
                    uow.connection.execute(
                        "DELETE FROM admission_versions WHERE record_id=? AND version=2", (item[2],)
                    )
            with pytest.raises(DerivedError, match="version_coverage_unavailable"):
                await svc.read("project-a:owner", actor=ACTOR, known_at=known, valid_at=known)

    asyncio.run(run())


def test_valid_time_is_recomputed_instead_of_copying_nearest_answer(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            start = clock[0] + timedelta(days=1)
            clock[0] += timedelta(seconds=1)
            item = await project.stage(svc.admission, scope, valid_from=start)
            await project.qualify(
                svc.admission, *item, support=project.SupportRange(start, start + timedelta(days=1))
            )
            clock[0] += timedelta(seconds=1)
            for valid, expected in (
                (start - timedelta(seconds=1), "unknown"),
                (start, "resolved"),
                (start + timedelta(days=1), "incomplete"),
            ):
                result = await svc.read(
                    "project-a:owner", actor=ACTOR, known_at=clock[0], valid_at=valid
                )
                assert result["answer_status"] == expected
                assert result["result"]["valid_at"] == valid.isoformat()

    asyncio.run(run())


def test_historical_membership_and_context_do_not_use_current_business_definition(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc, "commitments")
            clock[0] += timedelta(seconds=1)
            item = await project.stage(
                svc.admission,
                scope,
                subject="promise-1",
                predicate="commitment.state",
                value="open",
                membership="promise-a",
            )
            await project.qualify(svc.admission, *item)
            before = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            await svc.admission.replace_membership(
                item[2], expected_version=4, membership_id="promise-b"
            )
            svc.context = replace(svc.context, revision="context/2")
            await register(svc, "commitments", expected_generation=1)
            clock[0] += timedelta(seconds=1)
            old = await svc.read(
                "project-a:commitments", actor=ACTOR, known_at=before, valid_at=before
            )
            current = await svc.read(
                "project-a:commitments", actor=ACTOR, known_at=clock[0], valid_at=before
            )
            assert len(old["result"]["rows"]) == 1
            assert current["answer_status"] == "empty"
            assert old["historical"]["context_revision"] == "context/1"

    asyncio.run(run())


def test_source_replacement_preserves_old_system_time_and_new_pending_state(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            clock[0] += timedelta(seconds=1)
            first = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *first)
            before = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            event, draft = project.inputs(scope, identity="replacement", value="Bob")
            receipt = await svc.admission.stage_source(
                event,
                (draft,),
                source_authority_id=project.AUTHORITY.source_id,
                request_id="replace",
                membership_ids=("a",),
                base_event_id=first[0].id,
                expected_revision=1,
            )
            await project.grant(engine.repository, scope, event.id)
            pending_at = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            await project.qualify(svc.admission, event, draft, receipt.candidate_ids[0])
            clock[0] += timedelta(seconds=1)
            old = await svc.read("project-a:owner", actor=ACTOR, known_at=before, valid_at=before)
            pending = await svc.read(
                "project-a:owner", actor=ACTOR, known_at=pending_at, valid_at=before
            )
            new = await svc.read("project-a:owner", actor=ACTOR, known_at=clock[0], valid_at=before)
            assert old["result"]["rows"][0]["fields"][0]["known_values"] == ["Alice"]
            assert pending["answer_status"] == "incomplete"
            assert new["result"]["rows"][0]["fields"][0]["known_values"] == ["Bob"]

    asyncio.run(run())


@pytest.mark.parametrize("security_change", ["grant", "erase"])
def test_current_security_and_erasure_override_historical_truth(store, security_change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            clock[0] += timedelta(seconds=1)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            before = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            if security_change == "grant":
                await svc.grant(
                    ProcessingGrant(item[0].id, (ACTOR,), (svc.admission.purpose,), revoked=True),
                    expected_version=1,
                )
            else:
                await erase(kernel, scope, item[0].id)
            with pytest.raises(DerivedError):
                await svc.read("project-a:owner", actor=ACTOR, known_at=before, valid_at=before)

    asyncio.run(run())


def test_historical_pages_rebuild_all_parent_questions_without_current_publication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            await register(svc, "status")
            await svc.pages.register(
                "scene", ["project-a:owner", "project-a:status"], readers=(ACTOR,)
            )
            clock[0] += timedelta(seconds=1)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            known = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            page = await svc.pages.read("scene", actor=ACTOR, known_at=known, valid_at=known)
            assert page["historical"]["mode"] == "ledger_rebuild"
            assert [block["answer_status"] for block in page["blocks"]] == ["resolved", "unknown"]
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "question_head")
                assert not await uow.derived_records(scope, "question_page_head")

    asyncio.run(run())


def test_frozen_domain_contract_survives_replaced_host_business_configuration(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            clock[0] += timedelta(seconds=1)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            known = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            admission = project.service(engine, scope, clock)
            admission.contract = replace(admission.contract, owner_cardinality="multiple")
            svc.admission = admission
            await register(svc, expected_generation=1)
            clock[0] += timedelta(seconds=1)
            result = await svc.read("project-a:owner", actor=ACTOR, known_at=known, valid_at=known)
            assert result["result"]["contract_fingerprint"] == project.CONTRACT.fingerprint
            assert result["answer_status"] == "resolved"

    asyncio.run(run())


def test_old_history_never_loads_new_revision_body_when_current_grant_is_revoked(
    store, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            clock[0] += timedelta(seconds=1)
            first = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *first)
            known = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            event, draft = project.inputs(scope, identity="private-new", value="Bob")
            await svc.admission.stage_source(
                event,
                (draft,),
                source_authority_id=project.AUTHORITY.source_id,
                request_id="revise",
                membership_ids=("a",),
                base_event_id=first[0].id,
                expected_revision=1,
            )
            await project.grant(engine.repository, scope, event.id, revoked=True)
            clock[0] += timedelta(seconds=1)
            async with engine.repository.unit_of_work() as uow:
                cls = type(uow)
            original, loaded = cls.get_source_event, []

            async def guarded(self, requested_scope, source_id):
                loaded.append(source_id)
                assert source_id != event.id
                return await original(self, requested_scope, source_id)

            monkeypatch.setattr(cls, "get_source_event", guarded)
            result = await svc.read("project-a:owner", actor=ACTOR, known_at=known, valid_at=known)
            assert result["answer_status"] == "resolved"
            assert loaded and set(loaded) == {first[0].id}

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_rebuild_transport_preserves_arbitrary_bitemporal_coordinates(store, transport):
    from test_question_transport_v7 import client_for

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            clock[0] += timedelta(seconds=1)
            item = await project.stage(svc.admission, scope)
            await project.qualify(svc.admission, *item)
            known = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            await svc.admission.withdraw(
                item[2], expected_version=4, review_id="gone", reasons=("ended",)
            )
            clock[0] += timedelta(seconds=1)
            async with client_for(kernel, scope, svc, transport) as client:
                capabilities = await client.question_capabilities()
                assert capabilities["historical_mode"] == "ledger_rebuild"
                old = await client.question_read(
                    "project-a:owner", known_at=known.isoformat(), valid_at=known.isoformat()
                )
                assert old["answer_status"] == "resolved"
                assert old["historical"]["known_at"] == known.isoformat()

    asyncio.run(run())


def test_historical_page_layout_replays_host_patch_without_copied_old_answers(store):
    from agent_memory.derived.question_page_patches import RemoveQuestionBlock

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = runtime(engine, scope, clock, history_rebuild=True)
            await register(svc)
            await register(svc, "status")
            await svc.pages.register(
                "scene", ["project-a:owner", "project-a:status"], readers=(ACTOR,)
            )
            await fresh(svc, clock)
            await fresh(svc, clock, "status", dedupe="status")
            first = await svc.pages.publish("scene", actor=ACTOR)
            before = clock[0] + timedelta(milliseconds=1)
            clock[0] += timedelta(seconds=1)
            removed = first["blocks"][0]
            await svc.pages.patch(
                "scene",
                [RemoveQuestionBlock(removed["block_id"], removed["revision_id"])],
                actor=ACTOR,
                expected_revision_id=first["revision_id"],
                expected_certificate_revision_id=first["certificate_revision_id"],
            )
            clock[0] += timedelta(seconds=1)
            old = await svc.pages.read("scene", actor=ACTOR, known_at=before, valid_at=before)
            new = await svc.pages.read("scene", actor=ACTOR, known_at=clock[0], valid_at=before)
            assert [b["block_id"] for b in old["blocks"]] == [
                b["block_id"] for b in first["blocks"]
            ]
            assert [b["body"]["answer"]["question_id"] for b in new["blocks"]] == [
                "project-a:status"
            ]

    asyncio.run(run())
