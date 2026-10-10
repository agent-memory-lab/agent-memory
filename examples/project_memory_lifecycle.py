"""Run an authored L0 -> L1 -> project/L2/L3 lifecycle in a disposable database.

python examples/project_memory_lifecycle.py --output /tmp/project-lifecycle.json
The fixed grammar and host registry demonstrate orchestration, not real model or
business quality. No source annotations or accepted facts are fed to extraction.
"""

import argparse
import asyncio
import json
import re
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.business_policy import BusinessAdmissionPolicy, MemoryRule
from agent_memory.consolidation.project_admission import ProjectAdmission, ProjectMembership
from agent_memory.consolidation.project_extraction import ProjectExtractionBridge
from agent_memory.consolidation.verification_tools import LocalRecordVerifier
from agent_memory.derived.contracts import HostGrantAuthority
from agent_memory.derived.evolution import (
    PersonaDefinition,
    ScenarioEvolution,
    categorical_persona_evolution,
)
from agent_memory.derived.model import ProcessingGrant
from agent_memory.derived.persona_policy import CategoricalPersonaPolicy
from agent_memory.derived.project_questions import ProjectDomainContract
from agent_memory.derived.question_model import QuestionContext
from agent_memory.derived.question_service import QuestionService
from agent_memory.derived.service import ObservationService
from agent_memory.domain import (
    AtomReview,
    MemoryEvent,
    MemoryScope,
    PredicateSpec,
    SourceAuthority,
    utc_now,
)
from agent_memory.operations.domain_verification import (
    DomainVerificationQueue,
    VerificationToolSpec,
    project_publisher,
)
from agent_memory.operations.memory_host import MemoryHost
from agent_memory.operations.refresh_policy import RefreshPolicy
from agent_memory.sqlite import SQLiteMemoryRepository


class AuthoredGrammar:
    """Published finite grammar, never a benchmark semantic-model substitute."""

    version = "authored-project-grammar/1"

    async def generate_atoms(self, event):
        owner = re.fullmatch(r"Project A is owned by ([A-Za-z]+)\.", event.content)
        status = re.fullmatch(r"Project A status is (active|paused)\.", event.content)
        if owner or status:
            predicate = "project.owner" if owner else "project.status"
            value = (owner or status).group(1)
            return (
                {
                    "subject_id": "project-a",
                    "predicate": predicate,
                    "value": value,
                    "kind": "fact",
                    "modality": "asserted",
                    "source_quote": event.content,
                },
            )
        if event.content == "I explicitly prefer Chinese responses.":
            return (
                {
                    "subject_id": "alice",
                    "predicate": "locale",
                    "value": "zh-CN",
                    "kind": "preference",
                    "modality": "asserted",
                    "source_quote": event.content,
                },
            )
        return ()

    async def review_atoms(self, event, candidates):
        expected = await self.generate_atoms(event)
        return tuple(
            AtomReview(
                i,
                "supported"
                if any(
                    (c.draft.subject_id, c.draft.predicate, c.draft.value)
                    == (value["subject_id"], value["predicate"], value["value"])
                    for value in expected
                )
                else "unsupported",
                "durable",
                ("authored_finite_grammar",),
            )
            for i, c in enumerate(candidates)
        )


async def run():
    with TemporaryDirectory(prefix="project-lifecycle-") as folder:
        repository = SQLiteMemoryRepository(Path(folder) / "memory.db")
        await repository.initialize()
        scope = MemoryScope("authored-lifecycle", user_id="alice", session_id="demo")
        start, expires = utc_now(), utc_now() + timedelta(hours=1)
        contract = ProjectDomainContract(
            "projects", "1", "registry/1", "accountable", ("active", "paused")
        )
        predicates = (*contract.predicate_specs, PredicateSpec("locale"))
        names = tuple(spec.predicate for spec in predicates)
        capture = SourceAuthority(
            "authenticated-input", "self_report", ("project-a", "alice"), names
        )
        registry = SourceAuthority(
            "authoritative-registry", "tool_observation", ("project-a",), names
        )
        policy = BusinessAdmissionPolicy(
            predicates,
            tuple(
                MemoryRule(
                    spec.predicate,
                    verification="domain",
                    verification_sources=(registry.source_id,),
                )
                if spec.predicate != "locale"
                else MemoryRule("locale")
                for spec in predicates
            ),
            revision="authored-business-policy/1",
            source_families=((capture.source_id, "input"), (registry.source_id, "registry")),
        )
        observations = ObservationService(
            repository, scope, policy, authority_id="demo-host", authority_min_version=0
        )
        await observations.set_authority(
            HostGrantAuthority(
                "demo-host", ("alice",), expires, purposes=("agent_context", "project_questions")
            )
        )
        observations.authority_min_version = 1
        admission = ProjectAdmission(
            AdmissionEngine(repository),
            scope,
            principal="alice",
            contract=contract,
            authorities=(capture, registry),
            memberships=(ProjectMembership("a", "1", "project-a", "project-a"),),
            reviewer_version="authoritative-field-review/1",
            authority_id="demo-host",
            authority_min_version=1,
        )
        questions = QuestionService(
            admission, QuestionContext("host", "1", {}, expires), history_rebuild=True
        )
        for question in ("owner", "status"):
            await questions.register(
                "project-a:" + question,
                "project-a",
                question,
                readers=("alice",),
                refresh_policy=RefreshPolicy(mode="on_change"),
            )
        scenes = ScenarioEvolution(questions)
        await scenes.register(
            "project-a:scene", ("project-a:owner", "project-a:status"), readers=("alice",)
        )
        personas = categorical_persona_evolution(
            observations,
            CategoricalPersonaPolicy("locale", "zh-CN", "Prefers Chinese communication", {}),
            queue=questions.queue,
        )
        await personas.register(
            PersonaDefinition("language", "alice", ("locale",), minimum_span_seconds=0)
        )

        async def accept(uow, event):
            # Domain records are independently authenticated tool evidence, not
            # fresh extraction requests. The host retains and grants them here.
            if await uow.get_source_event(scope, event.id) is None:
                await uow.append_event(event)
            old = await uow.derived_get(scope, "grant", event.id)
            if old is None:
                await uow.derived_put(
                    scope,
                    "grant",
                    event.id,
                    {
                        **ProcessingGrant(
                            event.id, ("alice",), ("agent_context", "project_questions")
                        ).payload(),
                        "version": 1,
                        "authority_id": "demo-host",
                        "authority_version": 1,
                    },
                )
            return True

        records = {
            "schema": "authoritative-domain-records/1",
            "issuer": registry.source_id,
            "records": [
                {
                    "subject_id": "project-a",
                    "predicate": predicate,
                    "value": value,
                    "valid_from": start.isoformat(),
                    "valid_to": None,
                    "recorded_at": start.isoformat(),
                }
                for predicate, value in (("project.owner", "Alice"), ("project.status", "active"))
            ],
        }
        path = Path(folder) / "registry.json"
        path.write_text(json.dumps(records))
        tool = LocalRecordVerifier(
            path,
            expected_sha256=sha256(path.read_bytes()).hexdigest(),
            spec=VerificationToolSpec(
                "registry", "1", registry, registry.subjects, registry.predicates
            ),
        )

        async def authorize(*_):
            return True  # Exactly this authored, approved local registry.

        verification = DomainVerificationQueue(
            repository,
            scope,
            (tool,),
            authorize=authorize,
            publisher=policy.guard_domain_publisher(
                project_publisher(admission, accept_evidence=accept)
            ),
        )
        bridge = ProjectExtractionBridge(
            admission,
            membership_ids={"project-a": "a"},
            source_authority_id=capture.source_id,
            revision="host-bindings/1",
            observation_time_predicates=("project.owner", "project.status"),
        )
        grammar = AuthoredGrammar()
        host = MemoryHost(
            repository,
            scope,
            AtomExtractionPipeline(grammar, grammar),
            policy,
            capture,
            on_accept=accept,
            verification=verification,
            questions=questions,
            project_bridge=bridge,
            evolutions=(personas, scenes),
        )
        cycles = []
        for index, content in enumerate(
            (
                "Project A is owned by Alice.",
                "Project A status is active.",
                *("I explicitly prefer Chinese responses." for _ in range(3)),
            )
        ):
            event = MemoryEvent(
                scope,
                "message",
                content,
                id="input:" + str(index),
                actor="alice",
                metadata={"lifecycle": {"origin": "user"}},
            )
            await host.submit(event, request_id="raw:" + str(index), producer_id="demo")
            cycle = await host.run_once()
            if cycle["stage_errors"]:
                raise RuntimeError(cycle["stage_errors"])
            cycles.append(cycle)
        for _ in range(96):
            cycle = await host.run_once()
            if cycle["stage_errors"]:
                raise RuntimeError(cycle["stage_errors"])
            cycles.append(cycle)
            await asyncio.sleep(0.03)  # Honor durable parent/backoff due times.
        known = utc_now()
        scene = await scenes.read("project-a:scene", actor="alice")
        persona = await personas.read("language", actor="alice", context={})
        historical = await scenes.read(
            "project-a:scene", actor="alice", known_at=known, valid_at=known
        )
        host.stop()
        return {
            "schema": "project-lifecycle-smoke/1",
            "evidence_class": "authored_synthetic",
            "real_business_acceptance": False,
            "model_calls": 0,
            "real_model_quality": "not_assessed",
            "total_cost": "unmeasured",
            "persona_stability_demo_seconds": 0,
            "scene": scene,
            "persona": persona,
            "historical_scene": historical,
            "host_cycles": len(cycles),
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(run())
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "scene": result["scene"]["availability_status"],
                "persona": result["persona"]["state"],
                "history": "ledger_rebuild",
            }
        )
    )
