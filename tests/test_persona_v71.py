import asyncio
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_controls import configured
from test_durable_purge import source_id

from agent_memory.derived.model import DerivedError, ProcessingGrant
from agent_memory.derived.persona import KINDS, PersonaEvidence, PersonaViews
from agent_memory.domain import ForgetMode, ForgetRequest

store = base.store


def test_inference_families_counterexamples_context_expiry_and_erasure(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, *_ = await configured(engine, kernel, scope, clock, inputs=3)
            persona = PersonaViews(svc, reviewer_revision="host-reviewed/1")
            evidence = []
            rows = await engine.repository.admission_records(scope)
            async with engine.repository.unit_of_work() as uow:
                for n in range(1, 4):
                    source = await uow.get_source_event(scope, source_id(scope, str(n)))
                    evidence.append(
                        PersonaEvidence(
                            source.id,
                            0,
                            len(source.content),
                            source.content,
                            source.metadata["_retention"]["document_id"],
                            atom_id=next(r["id"] for r in rows if r["event_id"] == source.id),
                            atom_version=next(
                                r["version"] for r in rows if r["event_id"] == source.id
                            ),
                        )
                    )
            opts = dict(
                origin="inferred",
                readers=("alice",),
                purpose="agent_context",
                context={"project": "a"},
                valid_from=clock[0],
                valid_to=clock[0] + timedelta(minutes=10),
            )
            with pytest.raises(DerivedError, match="independent_support"):
                await persona.publish(
                    "language-pattern", "Prefers concise answers", evidence=evidence[:2], **opts
                )
            receipt = await persona.publish(
                "language-pattern", "Prefers concise answers", evidence=evidence, **opts
            )
            result = await persona.read(
                "language-pattern", actor="alice", purpose="agent_context", context={"project": "a"}
            )
            assert result["truth_status"] == "hypothesis"
            counter = PersonaEvidence(
                evidence[0].source_id,
                evidence[0].start,
                evidence[0].end,
                evidence[0].quote,
                evidence[0].family,
                "counterexample",
            )
            receipt = await persona.publish(
                "language-pattern",
                "Prefers concise answers",
                evidence=(*evidence, counter),
                expected_version=1,
                **opts,
            )
            assert receipt["state"] == "contested"
            with pytest.raises(DerivedError, match="context_mismatch"):
                await persona.read(
                    "language-pattern",
                    actor="alice",
                    purpose="agent_context",
                    context={"project": "b"},
                )
            clock[0] += timedelta(minutes=11)
            with pytest.raises(DerivedError, match="expired"):
                await persona.read(
                    "language-pattern",
                    actor="alice",
                    purpose="agent_context",
                    context={"project": "a"},
                )
            await kernel.forget(
                ForgetRequest(scope, (evidence[0].source_id,), mode=ForgetMode.ERASE)
            )
            async with engine.repository.unit_of_work() as uow:
                for kind in KINDS:
                    assert await uow.derived_get(scope, kind, receipt["id"]) == {"state": "erased"}

    asyncio.run(run())


def test_explicit_persona_still_requires_current_read_and_processing_rights(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, *_ = await configured(engine, kernel, scope, clock)
            persona = PersonaViews(svc, reviewer_revision="host/1")
            async with engine.repository.unit_of_work() as uow:
                source = await uow.get_source_event(scope, source_id(scope, "1"))
            proof = PersonaEvidence(
                source.id,
                0,
                len(source.content),
                source.content,
                source.metadata["_retention"]["document_id"],
            )
            await persona.publish(
                "explicit",
                "Declared preference",
                origin="explicit",
                evidence=(proof,),
                readers=("alice",),
                purpose="agent_context",
                context={},
                valid_from=clock[0],
                valid_to=clock[0] + timedelta(minutes=10),
            )
            await svc.grant(
                ProcessingGrant(source.id, ("alice",), revoked=True), expected_version=2
            )
            with pytest.raises(DerivedError, match="processing_denied"):
                await persona.read("explicit", actor="alice", purpose="agent_context", context={})

    asyncio.run(run())


def test_l3_rechecks_atom_version_after_native_retraction(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, *_ = await configured(engine, kernel, scope, clock)
            persona = PersonaViews(svc, reviewer_revision="host/1")
            rows = await engine.repository.admission_records(scope)
            row = next(r for r in rows if r["event_id"] == source_id(scope, "1"))
            async with engine.repository.unit_of_work() as uow:
                source = await uow.get_source_event(scope, row["event_id"])
            evidence = PersonaEvidence(
                source.id,
                0,
                len(source.content),
                source.content,
                source.metadata["_retention"]["document_id"],
                atom_id=row["id"],
                atom_version=row["version"],
            )
            await persona.publish(
                "derived-explicit",
                "Language summary",
                origin="explicit",
                evidence=(evidence,),
                readers=("alice",),
                purpose="agent_context",
                context={},
                valid_from=clock[0],
                valid_to=clock[0] + timedelta(hours=1),
            )
            from agent_memory.domain import MemoryEvent

            event = MemoryEvent(
                scope, "message", "The previous language preference ends now", occurred_at=clock[0]
            )
            await engine.retract(
                scope,
                row["id"],
                event=event,
                authority=base.SELF,
                policy=base.POLICY,
                expected_version=row["version"],
                valid_to=clock[0],
                source_quote=event.content,
            )
            with pytest.raises(DerivedError, match="atom_changed"):
                await persona.read(
                    "derived-explicit", actor="alice", purpose="agent_context", context={}
                )

    asyncio.run(run())
