"""Trusted project bridge: real provider writes and deterministic reviewed census."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_purge_restore import backup_copy, erase, replay, restorer

from agent_memory.conditions import Condition, ContextAttribute
from agent_memory.consolidation.project_admission import (
    ProjectAdmission,
    ProjectMembership,
)
from agent_memory.consolidation.qualification import target_fingerprint
from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.derived.project_questions import ProjectDomainContract, full_project_question
from agent_memory.domain import AtomDraft, SourceAuthority
from agent_memory.evidence_support import EvidenceLink, FieldSupport, SupportRange
from agent_memory.fact_qualification import FieldEvidence, SourceSpan

store = base.store
CONTRACT = ProjectDomainContract("projects", "1", "review/1", "accountable", ("active", "paused"))
FIELDS = ("subject_id", "predicate", "value", "valid_from")
AUTHORITY = SourceAuthority(
    "project-system",
    "tool_observation",
    ("project-a", "project-b", "promise-1"),
    tuple(spec.predicate for spec in CONTRACT.predicate_specs),
)
MEMBERSHIPS = (
    ProjectMembership("a", "registry/1", "project-a", "project-a"),
    ProjectMembership("b", "registry/1", "project-b", "project-b"),
    ProjectMembership("promise-a", "registry/1", "project-a", "promise-1"),
    ProjectMembership("promise-b", "registry/1", "project-b", "promise-1"),
)


def service(engine, scope, clock, **options):
    return ProjectAdmission(
        engine,
        scope,
        principal="host:alice",
        contract=CONTRACT,
        authorities=(AUTHORITY,),
        memberships=MEMBERSHIPS,
        reviewer_version="semantic-host-review/1",
        clock=lambda: clock[0],
        **options,
    )


def inputs(
    scope,
    *,
    identity="source",
    subject="project-a",
    predicate="project.owner",
    value="Alice",
    valid_from=None,
    conditions=(),
    exceptions=(),
):
    text = f"private-project-marker {subject} {predicate} {value}; effective October 1"
    event = base.source(scope, text, identity=identity)
    fields = (
        *FIELDS,
        *(("conditions",) if conditions else ()),
        *(("exceptions",) if exceptions else ()),
    )
    span = SourceSpan(event.id, 0, len(text), text)
    draft = AtomDraft(
        subject,
        predicate,
        value,
        text,
        text,
        valid_from=valid_from or base.at(1),
        conditions=conditions,
        exceptions=exceptions,
        field_evidence=tuple(FieldEvidence(field, ((span,),)) for field in fields),
    )
    return event, draft


async def grant(repo, scope, source_id, **changes):
    async with repo.unit_of_work() as uow:
        value = ProcessingGrant(
            source_id, ("host:alice",), ("project_questions",), **changes
        ).payload()
        await uow.derived_put(scope, "grant", source_id, {**value, "version": 1})


async def stage(svc, scope, *, membership="a", **options):
    event, draft = inputs(scope, **options)
    receipt = await svc.stage_source(
        event,
        (draft,),
        source_authority_id=AUTHORITY.source_id,
        request_id="request:" + event.id,
        membership_ids=(membership,),
    )
    await grant(svc.repository, scope, event.id)
    return event, draft, receipt.candidate_ids[0]


async def qualify(
    svc,
    event,
    draft,
    identity,
    *,
    expected=2,
    support=None,
    conditions=(),
    exceptions=(),
    review_id=None,
):
    fields = tuple(e.field for e in draft.field_evidence)
    evidence = EvidenceLink(
        "evidence",
        fields,
        target_fingerprint(draft),
        SourceSpan(event.id, 0, len(event.content), event.content),
        AUTHORITY,
        support or SupportRange(base.at(1)),
    )
    return await svc.qualify(
        identity,
        expected_version=expected,
        review_id=review_id or "review:" + event.id,
        applicability_id="explicit",
        conditions=conditions,
        exceptions=exceptions,
        links=(evidence,),
        field_support=tuple(FieldSupport(f, ((evidence.id,),)) for f in fields),
    )


async def snapshot(svc, clock, project_id="project-a", **options):
    clock[0] += timedelta(microseconds=100)
    from agent_memory.conditions import QueryContext

    async with svc.repository.unit_of_work() as uow:
        await uow.lock_admission_scope(svc.scope)
        context = QueryContext(
            svc.principal,
            svc.scope,
            project_id,
            svc.purpose,
            clock[0],
            clock[0],
            options.pop("attributes", ()),
            "UTC",
        )
        return await svc._snapshot(uow, context, at=clock[0], **options)


def answer(census, question="owner"):
    return full_project_question(CONTRACT, census.snapshot, question=question)


def test_stage_preserves_field_evidence_and_never_admits_quote_only(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            event, draft, identity = await stage(svc, scope)
            row = await engine.repository.admission_record(scope, identity)
            assert row["payload"]["draft"]["field_evidence"]
            assert row["payload"]["action"] == "PENDING_VERIFICATION"
            assert row["payload"]["claim_id"] is None
            assert row["payload"]["project_candidate"]["review"] is None
            assert not (await engine.state(scope, valid_at=clock[0], known_at=clock[0]))[0]
            pending = await snapshot(svc, clock)
            assert pending.snapshot.coverage.unqualified_candidate_ids == (identity,)
            assert pending.snapshot.coverage.expected_fact_count == 0
            assert answer(pending).status.value == "incomplete"
            duplicate = await svc.stage_source(
                event,
                (draft,),
                source_authority_id=AUTHORITY.source_id,
                request_id="request:" + event.id,
                membership_ids=("a",),
            )
            assert duplicate.duplicate

    asyncio.run(run())


def test_review_persists_real_coordinates_and_overlapping_owners_are_contested(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            a = await stage(svc, scope, identity="one")
            b = await stage(svc, scope, identity="two", value="Bob", valid_from=base.at(2))
            clock[0] = base.at(3)
            assert await qualify(svc, *a) == 4
            assert await qualify(svc, *b) == 4
            census = await snapshot(svc, clock)
            assert census.candidate_count == 2 and census.snapshot.coverage.expected_fact_count == 2
            assert census.registration_fingerprint == svc.registration_fingerprint
            assert {proof["source_event_id"] for proof in census.source_proofs} == {
                a[0].id,
                b[0].id,
            }
            assert all(
                proof["revision"] == 1
                and proof["epoch"] == 0
                and proof["document_head_generation"] == 1
                and proof["grant_version"] == 1
                for proof in census.source_proofs
            )
            assert all(
                "content" not in proof and "payload" not in proof for proof in census.source_proofs
            )
            assert all(base.at(3) <= f.fact.known_from < clock[0] for f in census.snapshot.facts)
            assert answer(census).status.value == "contested"
            assert {f.fact.value for f in census.snapshot.facts} == {"Alice", "Bob"}
            row = await engine.repository.admission_record(scope, a[2])
            review = row["payload"]["project_candidate"]["review"]
            assert review["principal"] == "host:alice" and review["version"] == 4
            assert review["policy_revision"] == CONTRACT.qualification_revision

    asyncio.run(run())


def test_pending_rejected_withdrawn_and_unbound_census_precedes_filtering(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            yes = await stage(svc, scope, identity="yes")
            rejected = await stage(svc, scope, identity="no", value="Bob")
            unbound = await stage(svc, scope, identity="unbound", value="Carol", membership=None)
            await qualify(svc, *yes)
            await svc.reject(
                rejected[2], expected_version=2, review_id="rejected", reasons=("unsupported",)
            )
            census = await snapshot(svc, clock)
            assert census.candidate_count == 3 and census.snapshot.coverage.expected_fact_count == 1
            assert census.snapshot.coverage.unqualified_candidate_ids == (unbound[2],)
            await svc.withdraw(
                unbound[2], expected_version=2, review_id="withdrawn", reasons=("out-of-scope",)
            )
            census = await snapshot(svc, clock)
            assert census.candidate_count == 3 and not census.snapshot.coverage.incomplete_reasons
            assert len(census.source_ids) == 3 and len(census.processing_references) == 3
            assert answer(census).status.value == "resolved"

    asyncio.run(run())


def test_membership_move_drops_review_and_does_not_load_foreign_body(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(
                svc,
                scope,
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="ship",
            )
            await qualify(svc, *item)
            assert (
                await svc.replace_membership(item[2], expected_version=4, membership_id="promise-b")
                == 5
            )
            # A moved-out route must not require the now-foreign source grant.
            await grant(engine.repository, scope, item[0].id, revoked=True)
            census = await snapshot(svc, clock)
            assert census.candidate_count == 1 and census.source_ids == ()
            assert census.snapshot.coverage.expected_fact_count == 0
            assert not census.snapshot.coverage.incomplete_reasons
            with pytest.raises(DerivedError, match="processing_denied"):
                await snapshot(svc, clock, "project-b")
            row = await engine.repository.admission_record(scope, item[2])
            assert row["payload"]["project_candidate"]["review"] is None
            assert len(row["payload"]["project_candidate"]["membership_history"]) == 2

    asyncio.run(run())


def test_conditions_support_expiry_and_missing_context_remain_unknown(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(
                svc, scope, conditions=("Only for EU",), exceptions=("Except holidays",)
            )
            await qualify(
                svc,
                *item,
                conditions=(Condition("eq", "region", "EU"),),
                exceptions=(Condition("eq", "holiday", True),),
                support=SupportRange(base.at(1), base.at(3)),
            )
            assert answer(await snapshot(svc, clock)).status.value == "unknown"
            context = (
                ContextAttribute("region", "EU", "host"),
                ContextAttribute("holiday", False, "host"),
            )
            census = await snapshot(svc, clock, attributes=context)
            assert answer(census).status.value == "resolved"
            assert census.next_transition_at == base.at(3)
            clock[0] = base.at(3)
            census = await snapshot(svc, clock, attributes=context)
            assert census.snapshot.coverage.unqualified_candidate_ids == (item[2],)
            assert answer(census).status.value == "incomplete"

    asyncio.run(run())


@pytest.mark.parametrize("damage", ["review", "policy", "source", "scope", "span", "grant"])
def test_forged_or_stale_binding_fails_closed(store, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(svc, scope)
            await qualify(svc, *item)
            if damage == "grant":
                await grant(engine.repository, scope, item[0].id, revoked=True)
            elif damage == "scope":
                svc.scope = replace(scope, user_id="mallory")
                with pytest.raises(DerivedError):
                    await svc.qualify(
                        item[2], expected_version=4, review_id="forged", applicability_id="x"
                    )
                return
            else:
                async with engine.repository.unit_of_work() as uow:
                    row = await uow.get_admission_record(scope, item[2])
                    binding = row["payload"]["project_candidate"]
                    if damage == "review":
                        binding["review"]["principal"] = "forged"
                    elif damage == "policy":
                        binding["review"]["policy_revision"] = "forged"
                    elif damage == "source":
                        binding["source"]["revision"] = 999
                    else:
                        row["payload"]["qualification"]["links"][0]["span"]["quote"] = "forged"
                    await svc._save(uow, row)
            with pytest.raises(DerivedError):
                await snapshot(svc, clock)

    asyncio.run(run())


def test_source_revision_withdraws_old_contribution_and_cas_rejects_old_review(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            old = await stage(svc, scope)
            await qualify(svc, *old)
            event, draft = inputs(scope, identity="new-source", value="Bob")
            clock[0] = base.at(2)
            new = await svc.stage_source(
                event,
                (draft,),
                source_authority_id=AUTHORITY.source_id,
                request_id="revision",
                membership_ids=("a",),
                base_event_id=old[0].id,
                expected_revision=1,
            )
            await grant(engine.repository, scope, event.id)
            with pytest.raises(DerivedError):
                await qualify(svc, *old, expected=5)
            await qualify(svc, event, draft, new.candidate_ids[0])
            census = await snapshot(svc, clock)
            assert len(census.source_ids) == 2
            assert answer(census).rows[0].fields[0].known_values == ("Bob",)

    asyncio.run(run())


def test_publication_manifest_and_production_indexed_census(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(svc, scope)
            await qualify(svc, *item)
            census = await snapshot(
                svc,
                clock,
                source_basis="publication_manifest",
                publication_request_ids=("request:source",),
            )
            assert census.snapshot.coverage.publication_closed is True
            assert len(census.publication_manifests) == 1
            with pytest.raises(DerivedError, match="publication_target_required"):
                await snapshot(svc, clock, source_basis="publication_manifest")
            async with engine.repository.unit_of_work() as uow:
                supported = callable(getattr(uow, "derived_project_candidates", None))
            assert supported, "production adapter must provide the indexed project census"
            assert (await svc.snapshot("project-a")).candidate_count == 1

    asyncio.run(run())


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_primary_erase_and_real_backup_replay_scrub_all_project_metadata(
    store, tmp_path, all_in_scope
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(
                svc,
                scope,
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="private-action",
            )
            await qualify(svc, *item)
            await svc.replace_membership(item[2], expected_version=4, membership_id="promise-b")
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, item[0].id, all_in_scope=all_in_scope)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        for table in (
                            "admission_records",
                            "admission_versions",
                            "derived_atom_headers",
                            "retention_entries",
                        ):
                            if hasattr(repository, "pool"):
                                cursor = await uow.connection.execute(
                                    "SELECT * FROM agent_memory_" + table
                                )
                                values = await cursor.fetchall()
                            else:
                                values = [
                                    dict(r)
                                    for r in uow.connection.execute(
                                        "SELECT * FROM " + table
                                    ).fetchall()
                                ]
                            serialized = json.dumps(values, default=str)
                            for secret in (
                                "private-project-marker",
                                "private-action",
                                "registry/1",
                                "semantic-host-review/1",
                                "project_candidate",
                            ):
                                assert secret not in serialized, (table, secret)

    asyncio.run(run())


def test_explicit_time_membership_authority_and_enums_are_required(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            event, draft = inputs(scope)
            for bad in (
                replace(draft, predicate="unregistered"),
                replace(draft, predicate="project.status", value="invented"),
                replace(draft, scope_level="user"),
            ):
                with pytest.raises(DerivedError):
                    await svc.stage_source(
                        event,
                        (bad,),
                        source_authority_id=AUTHORITY.source_id,
                        request_id="bad",
                        membership_ids=("a",),
                    )
            with pytest.raises(DerivedError, match="duplicate_candidate"):
                await svc.stage_source(
                    event,
                    (draft, draft),
                    source_authority_id=AUTHORITY.source_id,
                    request_id="duplicate",
                    membership_ids=("a", None),
                )
            with pytest.raises(DerivedError, match="authority_unregistered"):
                await svc.stage_source(
                    event,
                    (draft,),
                    source_authority_id="claimed-in-text",
                    request_id="bad-authority",
                )
            no_time = replace(draft, valid_from=None)
            receipt = await svc.stage_source(
                event,
                (no_time,),
                source_authority_id=AUTHORITY.source_id,
                request_id="no-time",
                membership_ids=("a",),
            )
            await grant(engine.repository, scope, event.id)
            with pytest.raises(DerivedError, match="explicit_reviewable_fact_required"):
                await qualify(svc, event, no_time, receipt.candidate_ids[0])
            assert (await engine.repository.admission_record(scope, receipt.candidate_ids[0]))[
                "version"
            ] == 2

    asyncio.run(run())


def test_failed_review_commit_rolls_back_contextual_qualification(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(svc, scope)

            async def fail(*args):
                raise RuntimeError("injected project review publication failure")

            monkeypatch.setattr(svc, "_save", fail)
            with pytest.raises(RuntimeError):
                await qualify(svc, *item)
            row = await engine.repository.admission_record(scope, item[2])
            assert row["version"] == 2 and "qualification" not in row["payload"]
            assert row["payload"]["project_candidate"]["review"] is None

    asyncio.run(run())


def test_census_and_total_input_sentinels_fail_before_any_body_reads(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            from agent_memory.conditions import QueryContext

            context = QueryContext(
                svc.principal, scope, "project-a", svc.purpose, clock[0], clock[0]
            )

            class Census:
                def __init__(self, headers):
                    self.headers = headers

                async def derived_project_candidates(self, *args):
                    return self.headers

            with pytest.raises(DerivedError, match="candidate_capacity"):
                await svc._snapshot(Census([{}] * 65), context, at=clock[0])
            header = {
                "id": "candidate",
                "version": 1,
                "source_ids": [str(i) for i in range(128)],
                "project": {
                    "contract_fingerprint": CONTRACT.fingerprint,
                    "current_project_id": "project-a",
                },
            }
            with pytest.raises(DerivedError, match="input_capacity"):
                await svc._snapshot(Census([header]), context, at=clock[0])

    asyncio.run(run())


def test_legacy_resolution_cannot_promote_project_review_required_candidate(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            event, draft, identity = await stage(svc, scope)
            from agent_memory.consolidation.admission import AdmissionPolicy
            from agent_memory.domain import PredicateSpec

            with pytest.raises(
                ValueError, match="project candidates require registered project review"
            ):
                await engine.resolve(
                    scope,
                    identity,
                    event=base.source(scope, event.content),
                    authority=AUTHORITY,
                    policy=AdmissionPolicy((PredicateSpec("project.owner"),)),
                    expected_version=2,
                    accept=True,
                    source_quote=event.content,
                    support_from=base.at(1),
                )
            row = await engine.repository.admission_record(scope, identity)
            assert row["version"] == 2 and row["payload"]["claim_id"] is None

    asyncio.run(run())


def test_raw_or_stripped_project_predicates_are_unbound_not_hidden(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            trusted = await stage(svc, scope)
            await qualify(svc, *trusted)
            raw_event, raw_draft = inputs(scope, identity="raw", subject="project-b", value="Bob")
            raw = await engine.admit(
                raw_event, (raw_draft,), authority=AUTHORITY, policy=svc.admission_policy
            )
            assert raw.claim_ids
            await grant(engine.repository, scope, raw_event.id)
            census = await snapshot(svc, clock)
            assert census.candidate_count == 2
            assert census.snapshot.coverage.unqualified_candidate_ids == raw.candidate_ids
            assert census.snapshot.coverage.expected_fact_count == 1
            assert answer(census).status.value == "incomplete"
            async with engine.repository.unit_of_work() as uow:
                row = await uow.get_admission_record(scope, trusted[2])
                row["payload"].pop("project_candidate")
                await svc._save(uow, row)
            census = await snapshot(svc, clock)
            assert set(census.snapshot.coverage.unqualified_candidate_ids) == {
                trusted[2],
                raw.candidate_ids[0],
            }
            assert not census.snapshot.facts

    asyncio.run(run())


def test_reviewed_rejection_of_bad_source_locator_does_not_poison_answer(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            trusted = await stage(svc, scope)
            await qualify(svc, *trusted)
            event, draft = inputs(scope, identity="bad-locator", value="Bob")
            missing = SourceSpan(event.id, 0, 3, "not-present")
            draft = replace(
                draft,
                source_quote="not-present",
                field_evidence=tuple(FieldEvidence(f, ((missing,),)) for f in FIELDS),
            )
            receipt = await svc.stage_source(
                event,
                (draft,),
                source_authority_id=AUTHORITY.source_id,
                request_id="bad-locator",
                membership_ids=("a",),
            )
            await grant(engine.repository, scope, event.id)
            assert answer(await snapshot(svc, clock)).status.value == "incomplete"
            await svc.reject(
                receipt.candidate_ids[0],
                expected_version=2,
                review_id="bad-locator-rejection",
                reasons=("unfaithful-locator",),
            )
            census = await snapshot(svc, clock)
            assert census.candidate_count == 2 and len(census.source_ids) == 2
            assert answer(census).status.value == "resolved"

    asyncio.run(run())
