"""Explicit host-reviewed handoff into qualified, maintained relation/history views.

Authored proposals demonstrate routing and safety contracts, never model quality.
Project membership and field evidence are supplied by trusted host code below.
"""

import asyncio
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
import test_relation_questions_v7 as relations
from test_atom_extraction import Reviewer, candidate
from test_question_runtime_v7 import ACTOR, register

from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.derived.question_service import QuestionService
from agent_memory.operations.domain_verification import (
    DomainVerificationQueue,
    VerificationFinding,
    VerificationToolSpec,
    project_publisher,
)
from agent_memory.operations.memory_host import MemoryHost

store = project.store


def test_host_reviewed_proposals_qualify_relations_then_guard_current_and_historical_delivery(
    store,
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            base = relations.service(engine, scope, clock)
            questions = QuestionService(base.admission, base.context, history_points=True)
            await register(questions, "owner")
            await register(questions, "risks")
            authority = questions.admission.authorities["project-system"]
            spec = VerificationToolSpec(
                "registry", "1", authority, authority.subjects, authority.predicates
            )
            findings = {}

            class Tool:
                async def verify(self, row):
                    event, draft = findings[row["event_id"]]
                    async with engine.repository.unit_of_work() as uow:
                        event = await uow.get_source_event(scope, event.id)
                    return VerificationFinding(
                        "supported",
                        event,
                        event.content,
                        draft.valid_from,
                        draft.valid_to,
                        tuple(field.field for field in draft.field_evidence),
                    )

            tool = Tool()
            tool.spec = spec

            async def accept(uow, event):
                grant = ProcessingGrant(event.id, (ACTOR,), ("project_questions",)).payload()
                await uow.derived_put(scope, "grant", event.id, {**grant, "version": 1})
                return True

            async def authorize(*_):
                return True  # Only this authored host-controlled registry.

            verification = DomainVerificationQueue(
                engine.repository,
                scope,
                (tool,),
                authorize=authorize,
                publisher=project_publisher(questions.admission, accept_evidence=accept),
                clock=lambda: clock[0],
                timeout_seconds=5,
            )

            class Proposer:
                version = "authored-project-proposer/1"
                result = []

                async def generate_atoms(self, event):
                    return self.result

            generator = Proposer()
            pipeline = AtomExtractionPipeline(generator, Reviewer())
            options = dict(
                on_accept=accept,
                verification=verification,
                questions=questions,
                clock=lambda: clock[0],
            )
            host = MemoryHost(
                engine.repository,
                scope,
                pipeline,
                questions.admission.stage_policy,
                authority,
                **options,
            )
            for identity, subject, predicate, value, member in (
                ("owner", "project-a", "project.owner", "Alice", "member:a"),
                ("edge", "project-a", "project.depends_on", "b", "member:a"),
                ("launch", "project-a", "project.launch_date", "2026-10-10T00:00:00Z", "member:a"),
                ("due", "b", "deliverable.commitment_date", "2026-10-12T00:00:00Z", "member:b"),
            ):
                event, host_draft = project.inputs(
                    scope, identity=identity, subject=subject, predicate=predicate, value=value
                )
                generator.result = [
                    {
                        **candidate(),
                        "subject_id": subject,
                        "predicate": predicate,
                        "value": value,
                        "source_quote": event.content,
                    }
                ]
                preview = await pipeline.prepare(
                    event, authority=authority, policy=questions.admission.stage_policy
                )
                assert len(preview["drafts"]) == 1
                assert preview["audit"]["reports"][0]["faithfulness"] == "supported"
                # The host reviews the proposal and independently binds membership,
                # complete field spans and dates. Models cannot set these controls.
                findings[event.id] = (event, host_draft)
                receipt = await host.submit_project(
                    event, (host_draft,), request_id="host:" + identity, membership_ids=(member,)
                )
                row = await engine.repository.admission_record(scope, receipt.candidate_ids[0])
                assert row["payload"]["action"] == "PENDING_VERIFICATION"
                assert row["payload"]["project_candidate"]["review"] is None
            # Restart after durable staging, before authoritative verification.
            host.stop()
            restarted = MemoryHost(
                engine.repository,
                scope,
                pipeline,
                questions.admission.stage_policy,
                authority,
                **options,
            )
            for _ in range(4):
                result = await restarted.run_once()
                assert result["verification"] == "supported"
            clock[0] += timedelta(seconds=2)
            tasks, leases = [], []
            for question in ("owner", "risks"):
                receipt = await questions.request(
                    "project-a:" + question, actor=ACTOR, dedupe_key="combined:" + question
                )
                lease = await questions.queue.claim(
                    "combined", lease_seconds=30, target_id=receipt["target_id"]
                )
                assert lease
                leases.append(lease)
                tasks.append(lease.task)
            snapshots = await questions.snapshot_many(tasks)
            await questions.publish_many(
                tasks, snapshots, [questions.prepare(s) for s in snapshots]
            )
            for lease in leases:
                await questions.queue.complete(lease)
            known = clock[0]
            results = await questions.read_many(("project-a:owner", "project-a:risks"), actor=ACTOR)
            assert all(result["answer_status"] == "resolved" for result in results)
            conclusion = relations.rule(results[1])["conclusions"][0]
            assert set(conclusion["source_event_ids"]) == {"edge", "launch", "due"}
            historical = await questions.read(
                "project-a:risks", actor=ACTOR, known_at=known, valid_at=known
            )
            assert historical["result"] == results[1]["result"]
            await questions.grant(
                ProcessingGrant("due", (ACTOR,), ("project_questions",), revoked=True),
                expected_version=1,
            )
            with pytest.raises(DerivedError):
                await questions.read_many(("project-a:owner", "project-a:risks"), actor=ACTOR)
            with pytest.raises(DerivedError):
                await questions.read("project-a:risks", actor=ACTOR, known_at=known, valid_at=known)
            restarted.stop()

    asyncio.run(run())
