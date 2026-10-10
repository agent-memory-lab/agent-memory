import asyncio

import pytest
import test_atom_admission as base

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.extraction_rules import RuleBasedAtomAdapter
from agent_memory.domain import MemoryEvent, PredicateSpec, SourceAuthority
from agent_memory.operations.memory_host import MemoryHost

store = base.store


def test_capture_atomic_rejection_restart_dedupe_and_stop(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            rules = RuleBasedAtomAdapter("alice")
            policy = AdmissionPolicy([PredicateSpec("home_city")])
            user = SourceAuthority("alice", subjects=("alice",), predicates=("home_city",))
            accepts = []
            allowed = [False]

            async def accept(uow, event):
                accepts.append(event.id)
                return allowed[0]

            opts = dict(on_accept=accept, clock=lambda: clock[0])
            host = MemoryHost(
                engine.repository, scope, AtomExtractionPipeline(rules, rules), policy, user, **opts
            )
            event = MemoryEvent(scope, "message", "我住在杭州", occurred_at=clock[0])
            with pytest.raises(PermissionError, match="rejected"):
                await host.submit(event, request_id="one", producer_id="host")
            async with engine.repository.unit_of_work() as uow:
                assert await uow.get_source_event(scope, event.id) is None
            allowed[0] = True
            first = await host.submit(event, request_id="one", producer_id="host")
            assert not first.duplicate
            again = await host.submit(event, request_id="one", producer_id="host")
            assert again.duplicate and len(accepts) == 2
            assert (await host.run_once())["extraction"]["completed"] == 1
            restarted = MemoryHost(engine.repository, scope, host.pipeline, policy, user, **opts)
            assert (await restarted.run_once())["extraction"]["claimed"] == 0
            task = asyncio.create_task(restarted.run(poll_seconds=0.01))
            await asyncio.sleep(0.03)
            restarted.stop()
            await asyncio.wait_for(task, timeout=2)
            assert await restarted.run_once() == {"state": "stopped"}
            assert (await restarted.metrics())["stopped"] is True

    asyncio.run(run())


def test_host_connects_native_project_verification_and_question_refresh(store):
    import test_project_admission_v7 as project
    from test_question_runtime_v7 import ACTOR, register, runtime

    from agent_memory.derived.model import ProcessingGrant
    from agent_memory.operations.domain_verification import (
        DomainVerificationQueue,
        VerificationFinding,
        VerificationToolSpec,
        project_publisher,
    )

    async def run():
        async with store() as (engine, kernel, scope, clock):
            questions = runtime(engine, scope, clock)
            await register(questions)
            spec = VerificationToolSpec(
                "system",
                "1",
                project.AUTHORITY,
                project.AUTHORITY.subjects,
                project.AUTHORITY.predicates,
            )
            event, draft = project.inputs(scope)

            class Tool:
                async def verify(self, candidate):
                    async with engine.repository.unit_of_work() as uow:
                        current = await uow.get_source_event(scope, event.id)
                    return VerificationFinding(
                        "supported", current, current.content, base.at(1), None, project.FIELDS
                    )

            tool = Tool()
            tool.spec = spec

            async def accept(uow, source):
                grant = ProcessingGrant(source.id, (ACTOR,), ("project_questions",)).payload()
                await uow.derived_put(scope, "grant", source.id, {**grant, "version": 1})
                return True

            async def authorize(*_):
                return True

            queue = DomainVerificationQueue(
                engine.repository,
                scope,
                (tool,),
                authorize=authorize,
                publisher=project_publisher(questions.admission, accept_evidence=accept),
                clock=lambda: clock[0],
                timeout_seconds=5,
            )
            rules = RuleBasedAtomAdapter("alice")
            host = MemoryHost(
                engine.repository,
                scope,
                AtomExtractionPipeline(rules, rules),
                questions.admission.stage_policy,
                project.AUTHORITY,
                on_accept=accept,
                questions=questions,
                verification=queue,
                clock=lambda: clock[0],
            )
            receipt = await host.submit_project(
                event, (draft,), request_id="host-project", membership_ids=("a",)
            )
            pending = await engine.repository.admission_record(scope, receipt.candidate_ids[0])
            assert pending["payload"]["action"] == "PENDING_VERIFICATION"
            await questions.request("project-a:owner", actor=ACTOR, dedupe_key="project")
            cycle = await host.run_once()
            assert cycle["verification"] == "supported" and cycle["extraction"]["claimed"] == 0
            from datetime import timedelta

            clock[0] += timedelta(seconds=2)  # Let the existing coalescing boundary become due.
            assert (await host.run_once())["verification_scheduled"] == 0
            result = await questions.read("project-a:owner", actor=ACTOR)
            assert result["answer_status"] == "resolved"
            assert result["result"]["rows"][0]["fields"][0]["known_values"] == ["Alice"]
            host.stop()

    asyncio.run(run())
