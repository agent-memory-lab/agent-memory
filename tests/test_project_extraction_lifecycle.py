"""Host-bound raw extraction reaches native project qualification on both providers."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory.conditions import Condition
from agent_memory.consolidation.admission import draft_to_payload
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.project_extraction import ProjectExtractionBridge
from agent_memory.derived.model import ProcessingGrant
from agent_memory.domain import AtomReview
from agent_memory.operations.domain_verification import (
    DomainVerificationQueue,
    VerificationFinding,
    VerificationToolSpec,
    project_publisher,
)
from agent_memory.operations.memory_host import MemoryHost
from agent_memory.operations.refresh_policy import RefreshPolicy
from agent_memory.operations.retention import DurableReceiver

store = base.store


async def assembly(
    engine,
    scope,
    clock,
    *,
    review=None,
    finding="supported",
    memberships=None,
    business_policy=None,
    guarded=True,
    conditions=(),
    exceptions=(),
    typed_conditions=(),
    typed_exceptions=(),
    supported_fields=None,
    explicit_valid_from=True,
    observation_time_predicates=(),
    witness_from=None,
    witness_to=None,
    primary_self_report=False,
):
    questions = runtime(engine, scope, clock)
    tool_authority = project.AUTHORITY
    if business_policy is not None:
        tool_authority = replace(project.AUTHORITY, source_id="independent-registry")
        questions.admission.authorities[tool_authority.source_id] = tool_authority
    capture_authority = project.AUTHORITY
    if primary_self_report:
        capture_authority = replace(project.AUTHORITY, source_id="user:alice", kind="self_report")
        questions.admission.authorities[capture_authority.source_id] = capture_authority
    await register(questions, refresh_policy=RefreshPolicy(mode="on_change"))
    event, draft = project.inputs(scope, conditions=conditions, exceptions=exceptions)
    if primary_self_report:
        event = replace(
            event,
            actor="alice",
            metadata={"lifecycle": {"origin": "user"}},
            content_hash="",
        )
    if conditions or exceptions:
        text = event.content + "; " + "; ".join((*conditions, *exceptions))
        event = replace(event, content=text, content_hash="")
        draft = replace(draft, source_quote=text)

    class Generator:
        version = "project-generator/1"

        async def generate_atoms(self, source):
            value = draft_to_payload(draft)
            value.pop("scope_level")
            value.pop("text")
            # Real model schema provides propositions and spans, not host field
            # evidence. Domain tools establish the latter independently.
            value.pop("field_evidence", None)
            if not explicit_valid_from:
                value.pop("valid_from", None)
            return (value,)

    class Reviewer:
        version = "project-reviewer/1"

        async def review_atoms(self, source, candidates):
            return (review or AtomReview(0, "supported", "durable", ("review",)),)

    async def accept(uow, source):
        if await uow.get_source_event(scope, source.id) is None:
            receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
            ticket = await receiver.issue_ticket(
                source,
                request_id="evidence:" + source.id,
                producer_id="host",
                configuration_sha256="1" * 64,
                _unit_of_work=uow,
            )
            await receiver.submit(
                source,
                ticket=ticket,
                producer_id="host",
                configuration_sha256="1" * 64,
                _unit_of_work=uow,
            )
        await uow.derived_put(
            scope,
            "grant",
            source.id,
            {
                **ProcessingGrant(source.id, (ACTOR,), ("project_questions",)).payload(),
                "version": 1,
            },
        )
        return True

    calls = []

    class Tool:
        spec = VerificationToolSpec(
            "project-system",
            "1",
            tool_authority,
            project.AUTHORITY.subjects,
            project.AUTHORITY.predicates,
        )

        async def verify(self, candidate):
            calls.append(candidate["id"])
            if finding == "unknown":
                return VerificationFinding("unknown")
            evidence = replace(
                event,
                id="independent-record",
                content="Registry: owner Alice",
                content_hash="",
                metadata={"lifecycle": {"origin": "tool"}},
            )
            async with engine.repository.unit_of_work() as uow:
                await accept(uow, evidence)
                evidence = await uow.get_source_event(scope, evidence.id)
            return VerificationFinding(
                finding,
                evidence,
                evidence.content,
                witness_from or base.at(1),
                witness_to,
                supported_fields if supported_fields is not None else project.FIELDS,
                typed_conditions,
                typed_exceptions,
            )

    async def authorized(*_):
        return True

    publisher = project_publisher(questions.admission, accept_evidence=accept)
    if business_policy is not None and guarded:
        publisher = business_policy.guard_domain_publisher(publisher)
    queue = DomainVerificationQueue(
        engine.repository,
        scope,
        (Tool(),),
        authorize=authorized,
        publisher=publisher,
        clock=lambda: clock[0],
        timeout_seconds=5,
    )
    bridge = ProjectExtractionBridge(
        questions.admission,
        membership_ids={"project-a": "a"} if memberships is None else memberships,
        source_authority_id=capture_authority.source_id,
        revision="host-project-map/1",
        observation_time_predicates=observation_time_predicates,
    )
    host = MemoryHost(
        engine.repository,
        scope,
        AtomExtractionPipeline(Generator(), Reviewer()),
        business_policy or questions.admission.admission_policy,
        capture_authority,
        on_accept=accept,
        verification=queue,
        questions=questions,
        project_bridge=bridge,
        clock=lambda: clock[0],
    )
    return host, event, calls


def test_raw_capture_automatic_project_qualification_and_view_refresh(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, calls = await assembly(engine, scope, clock)
            receipt = await host.submit(event, request_id="raw-project", producer_id="host")
            assert not receipt.duplicate
            cycle = await host.run_once()
            assert cycle["extraction"]["completed"] == 1
            assert cycle["verification"] == "supported"
            assert len(calls) == 1
            clock[0] += timedelta(seconds=2)
            next_cycle = await host.run_once()
            assert not next_cycle["stage_errors"]
            async with engine.repository.unit_of_work() as uow:
                jobs = await uow.derived_records(scope, "job")
            assert next_cycle["refresh"]["completed"] == 1, [
                r["payload"].get("reason") for r in jobs
            ]
            result = await host.questions.read("project-a:owner", actor=ACTOR)
            assert result["answer_status"] == "resolved"
            assert result["result"]["rows"][0]["fields"][0]["known_values"] == ["Alice"]
            async with engine.repository.unit_of_work() as uow:
                row = await uow.retention_get(scope, "request", "raw-project")
                identity = row["result"]["candidate_ids"][0]
                candidate = await uow.get_admission_record(scope, identity)
                assert candidate["payload"]["claim_id"] is None
                assert candidate["payload"]["project_candidate"]["membership"]["binding_id"] == "a"
                assert (
                    candidate["payload"]["project_candidate"]["review"]["disposition"]
                    == "qualified"
                )
                assert row["project_stage"]["candidate_ids"] == [identity]
                assert row["publication_manifest"]["closed"] is True
            assert (
                await host.submit(event, request_id="raw-project", producer_id="host")
            ).duplicate
            assert (await host.run_once())["verification_scheduled"] == 0
            assert len(calls) == 1

    asyncio.run(run())


def test_raw_user_conversation_requires_independent_tool_before_project_view(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, calls = await assembly(
                engine,
                scope,
                clock,
                primary_self_report=True,
                explicit_valid_from=False,
                observation_time_predicates=("project.owner",),
            )
            await host.submit(event, request_id="user-conversation", producer_id="host")
            cycle = await host.run_once()
            assert cycle["verification"] == "supported" and len(calls) == 1
            async with engine.repository.unit_of_work() as uow:
                source = await uow.get_source_event(scope, event.id)
                request = await uow.retention_get(scope, "request", "user-conversation")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
            assert source.metadata["lifecycle"]["origin"] == "user"
            assert row["payload"]["authority"]["kind"] == "self_report"
            assert row["payload"]["claim_id"] is None
            clock[0] += timedelta(seconds=2)
            assert not (await host.run_once())["stage_errors"]
            answer = await host.questions.read("project-a:owner", actor=ACTOR)
            assert answer["answer_status"] == "resolved"
            census = await host.questions.admission.snapshot("project-a")
            authority = census.snapshot.facts[0].qualification.authority
            assert authority.kind == "tool_observation"
            assert authority.source_id != host.source_authority.source_id

    asyncio.run(run())


def test_self_report_is_registered_only_for_pending_capture_and_never_field_proof(store):
    from agent_memory.consolidation.admission import draft_from_payload
    from agent_memory.consolidation.project_admission import ProjectAdmission
    from agent_memory.consolidation.qualification import target_fingerprint
    from agent_memory.derived.model import DerivedError
    from agent_memory.evidence_support import EvidenceLink, FieldSupport, SupportRange
    from agent_memory.fact_qualification import SourceSpan

    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, _ = await assembly(engine, scope, clock, primary_self_report=True)
            admission = ProjectAdmission(
                engine,
                scope,
                principal=ACTOR,
                contract=project.CONTRACT,
                authorities=(host.source_authority, project.AUTHORITY),
                memberships=project.MEMBERSHIPS,
                reviewer_version=host.questions.admission.reviewer_version,
                clock=lambda: clock[0],
            )
            assert admission.authorities[host.source_authority.source_id].kind == "self_report"
            await host.submit(event, request_id="pending-user", producer_id="host")
            assert (await host.worker.run_batch(max_tasks=1)).completed == 1
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "pending-user")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
                source = await uow.get_source_event(scope, event.id)
            draft = draft_from_payload(row["payload"]["draft"])
            link = EvidenceLink(
                "forged-user-support",
                project.FIELDS,
                target_fingerprint(draft),
                SourceSpan(source.id, 0, len(source.content), source.content),
                host.source_authority,
                SupportRange(base.at(1)),
            )
            with pytest.raises(DerivedError, match="project_authoritative_evidence_required"):
                await host.questions.admission.qualify(
                    row["id"],
                    expected_version=row["version"],
                    review_id="forged-review",
                    applicability_id="forged",
                    links=(link,),
                    field_support=tuple(
                        FieldSupport(field, ((link.id,),)) for field in project.FIELDS
                    ),
                )
            assert (await engine.repository.admission_record(scope, row["id"]))["payload"][
                "project_candidate"
            ]["review"] is None

    asyncio.run(run())


@pytest.mark.parametrize("witness_from,witness_to", [(base.at(2), None), (base.at(1), base.at(2))])
def test_partial_temporal_refutation_does_not_erase_the_whole_assertion(
    store, witness_from, witness_to
):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, calls = await assembly(
                engine,
                scope,
                clock,
                finding="refuted",
                witness_from=witness_from,
                witness_to=witness_to,
            )
            await host.submit(event, request_id="partial-refutation", producer_id="host")
            assert (await host.run_once())["verification"] == "failed"
            assert len(calls) == 1
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "partial-refutation")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
            assert row["payload"]["action"] == "PENDING_VERIFICATION"
            assert row["payload"]["project_candidate"]["review"] is None

    asyncio.run(run())


def test_refutation_evidence_remains_an_erasure_and_permission_dependency(store):
    from test_purge_restore import erase

    from agent_memory.consolidation.admission_runtime import AdmissionEngine

    async def run():
        async with store() as (engine, kernel, scope, clock):
            host, event, _ = await assembly(engine, scope, clock, finding="refuted")
            await host.submit(event, request_id="refutation-proof", producer_id="host")
            assert (await host.run_once())["verification"] == "refuted"
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "refutation-proof")
                identity = request["result"]["candidate_ids"][0]
                row = await uow.get_admission_record(scope, identity)
            assert "independent-record" in AdmissionEngine.source_dependencies(row["payload"])
            await erase(kernel, scope, "independent-record")
            assert await engine.repository.admission_record(scope, identity) is None

    asyncio.run(run())


def test_missing_effective_time_cannot_be_guessed_from_capture(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, calls = await assembly(engine, scope, clock, explicit_valid_from=False)
            await host.submit(event, request_id="implicit-time", producer_id="host")
            cycle = await host.run_once()
            assert not cycle["stage_errors"] and calls == []
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "implicit-time")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
                assert row["payload"]["action"] == "PENDING_VERIFICATION"
                assert row["payload"]["project_extraction"]["verification_eligible"] is False
                assert row["payload"].get("qualification") is None

    asyncio.run(run())


@pytest.mark.parametrize("time_proof", [True, False])
def test_explicit_host_observation_time_policy_still_requires_independent_temporal_proof(
    store, time_proof
):
    async def run():
        async with store() as (engine, _, scope, clock):
            fields = (
                project.FIELDS
                if time_proof
                else tuple(field for field in project.FIELDS if field != "valid_from")
            )
            host, event, calls = await assembly(
                engine,
                scope,
                clock,
                explicit_valid_from=False,
                observation_time_predicates=("project.owner",),
                supported_fields=fields,
            )
            await host.submit(event, request_id="host-time", producer_id="host")
            cycle = await host.run_once()
            assert len(calls) == 1
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "host-time")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
            audit = request["result"]["extraction"]["project_transfer"]
            assert audit["original_proposals"][0]["valid_from"] is None
            assert audit["host_time_bindings"][0]["original_valid_from"] is None
            assert (
                audit["host_time_bindings"][0]["bound_valid_from"] == event.occurred_at.isoformat()
            )
            assert row["payload"]["valid_time_basis"] == "host_observation_time_policy"
            assert row["payload"]["draft"]["valid_from"] == event.occurred_at.isoformat()
            if time_proof:
                assert cycle["verification"] == "supported"
                clock[0] += timedelta(seconds=2)
                await host.run_once()
                assert (await host.questions.read("project-a:owner", actor=ACTOR))[
                    "answer_status"
                ] == "resolved"
            else:
                assert cycle["verification"] == "failed"
                assert row["payload"].get("qualification") is None

    asyncio.run(run())


@pytest.mark.parametrize("proof", ["complete", "missing_condition", "missing_field", "unexpected"])
def test_authoritative_typed_qualifiers_are_complete_and_never_stripped(store, proof):
    async def run():
        async with store() as (engine, _, scope, clock):
            conditions = ("only while on call",)
            typed = (Condition("eq", "on_call", True),)
            fields = (*project.FIELDS, "conditions", "exceptions")
            if proof == "missing_condition":
                typed = ()
            elif proof == "missing_field":
                fields = (*project.FIELDS, "exceptions")
            elif proof == "unexpected":
                typed = (*typed, Condition("eq", "unauthorized_extra", True))
            host, event, calls = await assembly(
                engine,
                scope,
                clock,
                conditions=conditions,
                exceptions=("except on vacation",),
                typed_conditions=typed,
                typed_exceptions=(Condition("eq", "vacation", True),),
                supported_fields=fields,
            )
            await host.submit(event, request_id="conditional", producer_id="host")
            cycle = await host.run_once()
            assert len(calls) == 1
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "conditional")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
            assert row["payload"]["claim_id"] is None
            assert row["payload"]["draft"]["conditions"] == list(conditions)
            if proof == "complete":
                assert cycle["verification"] == "supported"
                qualification = row["payload"]["qualification"]
                assert qualification["conditions"][0]["attribute"] == "on_call"
                assert qualification["exceptions"][0]["attribute"] == "vacation"
                assert row["payload"]["project_candidate"]["review"]["disposition"] == "qualified"
            else:
                assert cycle["verification"] == "failed"
                assert row["payload"].get("qualification") is None
                assert row["payload"]["project_candidate"]["review"] is None

    asyncio.run(run())


def test_revoked_processing_permission_during_review_rolls_back_transfer(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, calls = await assembly(engine, scope, clock)
            original = host.pipeline.reviewer.review_atoms

            async def revoke(source, candidates):
                await host.questions.grant(
                    ProcessingGrant(source.id, (ACTOR,), ("project_questions",), revoked=True),
                    expected_version=1,
                )
                return await original(source, candidates)

            monkeypatch.setattr(host.pipeline.reviewer, "review_atoms", revoke)
            await host.submit(event, request_id="revoked", producer_id="host")
            assert (await host.run_once())["extraction"]["failed"] == 1
            assert not await engine.repository.admission_records(scope) and calls == []

    asyncio.run(run())


@pytest.mark.parametrize("expiry_stage", ["before_generation", "after_publication_writes"])
def test_project_grant_expiry_is_checked_before_model_and_at_final_commit(
    store, monkeypatch, expiry_stage
):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, calls = await assembly(engine, scope, clock)
            generations = []
            generate = host.pipeline.generator.generate_atoms
            save = host.project_bridge.admission._save

            async def counted(source):
                generations.append(source.id)
                return await generate(source)

            async def expires(uow, row):
                version = await save(uow, row)
                clock[0] += timedelta(seconds=2)
                return version

            monkeypatch.setattr(host.pipeline.generator, "generate_atoms", counted)
            await host.submit(event, request_id="expired-grant", producer_id="host")
            await host.questions.grant(
                ProcessingGrant(
                    event.id,
                    (ACTOR,),
                    ("project_questions",),
                    expires_at=clock[0] + timedelta(seconds=1),
                ),
                expected_version=1,
            )
            if expiry_stage == "before_generation":
                clock[0] += timedelta(seconds=2)
            else:
                monkeypatch.setattr(host.project_bridge.admission, "_save", expires)
            assert (await host.run_once())["extraction"]["failed"] == 1
            assert not await engine.repository.admission_records(scope) and calls == []
            assert generations == ([] if expiry_stage == "before_generation" else [event.id])

    asyncio.run(run())


def test_source_erased_during_review_cannot_reappear(store, monkeypatch):
    from test_purge_restore import erase

    async def run():
        async with store() as (engine, kernel, scope, clock):
            host, event, calls = await assembly(engine, scope, clock)
            original = host.pipeline.reviewer.review_atoms

            async def deleting(source, candidates):
                await erase(kernel, scope, source.id)
                return await original(source, candidates)

            monkeypatch.setattr(host.pipeline.reviewer, "review_atoms", deleting)
            await host.submit(event, request_id="erased", producer_id="host")
            assert (await host.run_once())["extraction"]["completed"] == 0
            assert calls == [] and not await engine.repository.admission_records(scope)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, event.id) is None

    asyncio.run(run())


def test_partial_transfer_rolls_back_then_reuses_saved_stage_on_restart(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, calls = await assembly(engine, scope, clock)
            original_save = host.project_bridge.admission._save
            original_generate = host.pipeline.generator.generate_atoms
            generations = []

            async def generate(source):
                generations.append(source.id)
                return await original_generate(source)

            async def failing_save(uow, row):
                await original_save(uow, row)
                raise RuntimeError("simulated publication write failure")

            monkeypatch.setattr(host.pipeline.generator, "generate_atoms", generate)
            monkeypatch.setattr(host.project_bridge.admission, "_save", failing_save)
            await host.submit(event, request_id="restart", producer_id="host")
            assert (await host.run_once())["extraction"]["failed"] == 1
            assert not await engine.repository.admission_records(scope) and calls == []
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "restart")
                assert request["status"] == "retry_wait" and "prepared" in request
                assert "project_stage" not in request
            monkeypatch.setattr(host.project_bridge.admission, "_save", original_save)
            restarted = MemoryHost(
                engine.repository,
                scope,
                host.pipeline,
                host.policy,
                host.source_authority,
                on_accept=host.on_accept,
                questions=host.questions,
                verification=host.verification,
                project_bridge=host.project_bridge,
                clock=lambda: clock[0],
            )
            clock[0] += timedelta(seconds=2)
            assert (await restarted.run_once())["verification"] == "supported"
            assert generations == [event.id] and len(calls) == 1

    asyncio.run(run())


@pytest.mark.parametrize("guarded", [True, False])
def test_business_domain_policy_is_bound_to_the_native_publisher(store, guarded):
    from agent_memory.consolidation.business_policy import BusinessAdmissionPolicy, MemoryRule

    async def run():
        async with store() as (engine, _, scope, clock):
            policy = BusinessAdmissionPolicy(
                project.CONTRACT.predicate_specs,
                tuple(
                    MemoryRule(
                        p.predicate,
                        verification="domain",
                        verification_sources=("independent-registry",),
                    )
                    for p in project.CONTRACT.predicate_specs
                ),
                revision="business-project/1",
            )
            if not guarded:
                with pytest.raises(ValueError, match="bound business policy guard"):
                    await assembly(engine, scope, clock, business_policy=policy, guarded=False)
                return
            host, event, calls = await assembly(engine, scope, clock, business_policy=policy)
            await host.submit(event, request_id="raw-business", producer_id="host")
            assert (await host.run_once())["verification"] == "supported"
            assert len(calls) == 1
            clock[0] += timedelta(seconds=2)
            await host.run_once()
            answer = await host.questions.read("project-a:owner", actor=ACTOR)
            assert answer["answer_status"] == "resolved"

    asyncio.run(run())


def test_business_storage_denial_wins_over_uncertain_model_reuse(store):
    from agent_memory.consolidation.business_policy import BusinessAdmissionPolicy, MemoryRule

    async def run():
        async with store() as (engine, _, scope, clock):
            policy = BusinessAdmissionPolicy(
                project.CONTRACT.predicate_specs,
                tuple(
                    MemoryRule(
                        spec.predicate,
                        storage="l0_only" if spec.predicate == "project.owner" else "durable",
                        verification="domain",
                        verification_sources=("independent-registry",),
                    )
                    for spec in project.CONTRACT.predicate_specs
                ),
                revision="business-discard/1",
            )
            host, event, calls = await assembly(
                engine,
                scope,
                clock,
                business_policy=policy,
                review=AtomReview(0, "uncertain", "uncertain", ("review",)),
            )
            await host.submit(event, request_id="business-discard", producer_id="host")
            assert (await host.run_once())["extraction"]["completed"] == 1
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "business-discard")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
            assert row["payload"]["action"] == "REJECT" and calls == []
            assert row["payload"]["project_candidate"]["review"]["disposition"] == "rejected"

    asyncio.run(run())


@pytest.mark.parametrize(
    "review, finding, memberships, action, expected_calls",
    [
        (AtomReview(0, "unsupported", "durable", ("review",)), "supported", None, "REJECT", 0),
        (AtomReview(0, "supported", "transient", ("review",)), "supported", None, "REJECT", 0),
        (
            AtomReview(0, "uncertain", "durable", ("review",)),
            "supported",
            None,
            "PENDING_VERIFICATION",
            0,
        ),
        (None, "supported", {}, "PENDING_VERIFICATION", 0),
        (None, "unknown", None, "PENDING_VERIFICATION", 1),
        (None, "refuted", None, "REJECT", 1),
    ],
)
def test_unreviewed_unknown_or_host_unbound_inputs_never_become_project_facts(
    store, review, finding, memberships, action, expected_calls
):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, calls = await assembly(
                engine, scope, clock, review=review, finding=finding, memberships=memberships
            )
            await host.submit(event, request_id="raw-project", producer_id="host")
            assert not (await host.run_once())["stage_errors"]
            clock[0] += timedelta(seconds=2)
            assert not (await host.run_once())["stage_errors"]
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "raw-project")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
                assert row["payload"]["action"] == action
                assert row["payload"].get("qualification") is None
            assert len(calls) == expected_calls
            answer = await host.questions.read("project-a:owner", actor=ACTOR)
            assert answer["answer_status"] != "resolved"

    asyncio.run(run())


def test_changed_trusted_project_binding_fences_saved_extraction(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event, calls = await assembly(engine, scope, clock)
            await host.submit(event, request_id="raw-project", producer_id="host")
            host.project_bridge.revision = "host-project-map/2"
            cycle = await host.run_once()
            assert cycle["extraction"]["completed"] == 0
            assert calls == []
            assert not await engine.repository.admission_records(scope)

    asyncio.run(run())
