"""Exact end-boundary proof in the raw-source/default-file project lifecycle."""

import asyncio
import json
import re
from datetime import timedelta
from hashlib import sha256

import pytest
import test_atom_admission as base
import test_project_admission_v7 as project
from test_question_runtime_v7 import ACTOR, register, runtime

from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.business_policy import BusinessAdmissionPolicy, MemoryRule
from agent_memory.consolidation.project_extraction import ProjectExtractionBridge
from agent_memory.consolidation.verification_tools import LocalRecordVerifier
from agent_memory.derived.model import ProcessingGrant
from agent_memory.domain import AtomReview, MemoryEvent, SourceAuthority
from agent_memory.operations.domain_verification import (
    DomainVerificationQueue,
    VerificationToolSpec,
    project_publisher,
)
from agent_memory.operations.memory_host import MemoryHost
from agent_memory.operations.refresh_policy import RefreshPolicy

store = base.store


class FiniteGrammar:
    """Authored finite extraction grammar, with no input field annotations."""

    version = "finite-validity-fixture/1"

    async def generate_atoms(self, event):
        match = re.fullmatch(r"Alice owns project-a from (\S+) until (\S+)\.", event.content)
        return (
            {
                "subject_id": "project-a",
                "predicate": "project.owner",
                "value": "Alice",
                "kind": "fact",
                "modality": "asserted",
                "source_quote": event.content,
                "valid_from": match[1],
                "valid_to": None if match[2] == "open" else match[2],
            },
        )

    async def review_atoms(self, event, candidates):
        return (AtomReview(0, "supported", "durable", ("authored_fixture",)),)


async def host_for(engine, scope, clock, path, *, candidate_end, record_end, record_value="Alice"):
    capture = SourceAuthority(
        "user:alice", "self_report", project.AUTHORITY.subjects, project.AUTHORITY.predicates
    )
    authority = SourceAuthority(
        "domain-registry",
        "tool_observation",
        project.AUTHORITY.subjects,
        project.AUTHORITY.predicates,
    )
    questions = runtime(engine, scope, clock)
    questions.admission.authorities = {capture.source_id: capture, authority.source_id: authority}
    await register(questions, refresh_policy=RefreshPolicy(mode="on_change"))
    policy = BusinessAdmissionPolicy(
        project.CONTRACT.predicate_specs,
        tuple(
            MemoryRule(
                spec.predicate, verification="domain", verification_sources=(authority.source_id,)
            )
            for spec in project.CONTRACT.predicate_specs
        ),
        revision="finite-validity-business/1",
    )
    records = {
        "schema": "authoritative-domain-records/1",
        "issuer": authority.source_id,
        "records": [
            {
                "subject_id": "project-a",
                "predicate": "project.owner",
                "value": record_value,
                "valid_from": base.at(1).isoformat(),
                "valid_to": record_end.isoformat() if record_end else None,
                "recorded_at": base.at(1).isoformat(),
            }
        ],
    }
    path.write_text(json.dumps(records))
    tool = LocalRecordVerifier(
        path,
        expected_sha256=sha256(path.read_bytes()).hexdigest(),
        spec=VerificationToolSpec(
            "registry", "1", authority, authority.subjects, authority.predicates
        ),
        clock=lambda: clock[0],
    )

    async def accept(uow, source):
        if await uow.get_source_event(scope, source.id) is None:
            await uow.append_event(source)
        previous = await uow.derived_get(scope, "grant", source.id)
        if previous is None:
            grant = ProcessingGrant(source.id, (ACTOR,), ("project_questions",)).payload()
            await uow.derived_put(scope, "grant", source.id, {**grant, "version": 1})
        return True

    async def authorized(*_):
        return True

    queue = DomainVerificationQueue(
        engine.repository,
        scope,
        (tool,),
        authorize=authorized,
        publisher=policy.guard_domain_publisher(
            project_publisher(questions.admission, accept_evidence=accept)
        ),
        clock=lambda: clock[0],
        timeout_seconds=5,
    )
    grammar = FiniteGrammar()
    bridge = ProjectExtractionBridge(
        questions.admission,
        membership_ids={"project-a": "a"},
        source_authority_id=capture.source_id,
        revision="finite-validity-map/1",
    )
    host = MemoryHost(
        engine.repository,
        scope,
        AtomExtractionPipeline(grammar, grammar),
        policy,
        capture,
        on_accept=accept,
        questions=questions,
        verification=queue,
        project_bridge=bridge,
        clock=lambda: clock[0],
    )
    upper = candidate_end.isoformat() if candidate_end else "open"
    event = MemoryEvent(
        scope,
        "message",
        f"Alice owns project-a from {base.at(1).isoformat()} until {upper}.",
        id="finite-source",
        actor="alice",
        occurred_at=base.at(1),
        metadata={"lifecycle": {"origin": "user"}},
    )
    return host, event


@pytest.mark.parametrize("record_value", ["Alice", "Bob"])
def test_exact_record_end_proof_closes_finite_project_candidate(store, tmp_path, record_value):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event = await host_for(
                engine,
                scope,
                clock,
                tmp_path / "records.json",
                candidate_end=base.at(5),
                record_end=base.at(5),
                record_value=record_value,
            )
            await host.submit(event, request_id="finite", producer_id="host")
            cycle = await host.run_once()
            expected = "supported" if record_value == "Alice" else "refuted"
            assert cycle["verification"] == expected and not cycle["stage_errors"]
            clock[0] += timedelta(seconds=2)
            assert not (await host.run_once())["stage_errors"]
            answer = await host.questions.read("project-a:owner", actor=ACTOR)
            assert answer["answer_status"] == ("resolved" if expected == "supported" else "unknown")
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "finite")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
            if expected == "supported":
                fields = row["payload"]["qualification"]["field_support"]
                assert "valid_to" in {field["field"] for field in fields}
                assert answer["result"]["rows"][0]["fields"][0]["known_values"] == ["Alice"]
            else:
                assert "valid_to" in row["payload"]["project_refutation"]["supported_fields"]

    asyncio.run(run())


@pytest.mark.parametrize(
    "candidate_end,record_end",
    [(base.at(5), base.at(6)), (base.at(5), None), (base.at(5), base.at(4)), (None, base.at(5))],
)
def test_containment_or_open_record_cannot_prove_precise_candidate_end(
    store, tmp_path, candidate_end, record_end
):
    async def run():
        async with store() as (engine, _, scope, clock):
            host, event = await host_for(
                engine,
                scope,
                clock,
                tmp_path / "records.json",
                candidate_end=candidate_end,
                record_end=record_end,
            )
            await host.submit(event, request_id="unknown-end", producer_id="host")
            cycle = await host.run_once()
            assert cycle["verification"] == "unknown" and not cycle["stage_errors"]
            async with engine.repository.unit_of_work() as uow:
                request = await uow.retention_get(scope, "request", "unknown-end")
                row = await uow.get_admission_record(scope, request["result"]["candidate_ids"][0])
                tasks = await uow.derived_records(scope, "domain_verification_task")
            assert row["payload"]["action"] == "PENDING_VERIFICATION"
            assert row["payload"].get("qualification") is None
            assert row["payload"]["project_candidate"]["review"] is None
            assert tasks[0]["payload"]["state"] == "completed"
            assert tasks[0]["payload"]["publication_attempt"]["state"] == "completed"
            clock[0] += timedelta(seconds=2)
            await host.run_once()
            assert (await host.questions.read("project-a:owner", actor=ACTOR))[
                "answer_status"
            ] == "incomplete"

    asyncio.run(run())
