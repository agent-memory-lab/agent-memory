import asyncio
from dataclasses import asdict, replace

import pytest
import test_atom_admission as base

from agent_memory.consolidation.admission import (
    AdmissionPolicy,
    _canonical,
    candidate_id,
    draft_from_payload,
    draft_to_payload,
)
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.domain import AtomReview, ForgetMode, ForgetRequest, PredicateSpec
from agent_memory.fact_qualification import FieldEvidence, SourceSpan

store = base.store


def test_default_serialization_preserves_legacy_identity():
    from hashlib import sha256

    from agent_memory.domain import MemoryScope

    draft = base.atom()
    event = base.source(MemoryScope("tenant", session_id="session"))
    payload = draft_to_payload(draft)
    assert not ({"conditions", "exceptions", "negated", "field_evidence"} & payload.keys())
    expected = (
        "candidate:"
        + sha256(
            _canonical(
                {"event_id": event.id, "scope": asdict(event.scope), "draft": payload}
            ).encode()
        ).hexdigest()
    )
    assert candidate_id(event, draft) == expected
    assert "required_evidence_fields" not in base.POLICY.config_payload()["predicates"][0]


def test_field_evidence_and_or_and_required_fields():
    from agent_memory.domain import MemoryScope

    event = base.source(MemoryScope("tenant", session_id="session"))
    good = SourceSpan(event.id, 0, 5, "Alice")
    bad = SourceSpan("unavailable-source", 0, 5, "Alice")
    evidence = FieldEvidence("value", ((good, bad), (good,)))
    draft = replace(base.atom(), field_evidence=(evidence,))
    policy = AdmissionPolicy([PredicateSpec("city", required_evidence_fields=("value",))])
    assert policy.evaluate(event, draft, base.SELF)[0] == "ACCEPT"
    assert draft_from_payload(draft_to_payload(draft)) == draft
    broken = replace(draft, field_evidence=(FieldEvidence("value", ((good, bad),)),))
    assert policy.evaluate(event, broken, base.SELF)[0] == "PENDING_VERIFICATION"
    assert "required_field_evidence_missing" in policy.evaluate(event, base.atom(), base.SELF)[1]
    with pytest.raises(ValueError):
        replace(draft, negated="false")


@pytest.mark.parametrize(
    "qualifier",
    [{"conditions": ["only on work days"]}, {"exceptions": ["except holidays"]}, {"negated": True}],
)
def test_extraction_preserves_qualifiers_without_unconditional_publication(store, qualifier):
    class Adapter:
        version = "qualified-1"

        async def generate_atoms(self, event):
            return [
                {
                    "subject_id": "alice",
                    "predicate": "city",
                    "value": "Hangzhou",
                    "kind": "fact",
                    "modality": "asserted",
                    "source_quote": event.content,
                    **qualifier,
                }
            ]

        async def review_atoms(self, event, candidates):
            return [AtomReview(0, "supported", "durable", ("reviewed",))]

    async def run():
        async with store() as (engine, _, scope, _):
            pipeline = AtomExtractionPipeline(Adapter(), Adapter())
            receipt = await pipeline.process(
                engine.repository, base.source(scope), authority=base.SELF, policy=base.POLICY
            )
            assert not receipt.admission.claim_ids
            assert receipt.admission.pending_ids
            row = await engine.repository.admission_record(
                scope, receipt.admission.candidate_ids[0]
            )
            for name, value in qualifier.items():
                assert row["payload"]["draft"][name] == value

    asyncio.run(run())


def test_retraction_has_two_times_and_never_revives_previous_value(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            first = await engine.admit(
                base.source(scope), (base.atom(),), authority=base.SELF, policy=base.POLICY
            )
            clock[0] = base.at(5)
            moved = await engine.admit(
                base.source(scope, "Alice lives in Shanghai", day=5),
                (base.atom("Shanghai", valid_from=base.at(5)),),
                authority=base.SELF,
                policy=base.POLICY,
            )
            clock[0] = base.at(20)
            event = base.source(scope, "Alice stopped living in Shanghai on day 10", day=10)
            options = dict(
                event=event,
                authority=base.SELF,
                policy=base.POLICY,
                expected_version=1,
                valid_to=base.at(10),
                source_quote=event.content,
            )
            await engine.retract(scope, moved.candidate_ids[0], **options)
            assert (await engine.retract(scope, moved.candidate_ids[0], **options)).duplicate
            before = (await engine.state(scope, valid_at=base.at(12), known_at=base.at(19)))[0]
            assert before[0].id == moved.claim_ids[0]
            after = (await engine.state(scope, valid_at=base.at(12), known_at=base.at(20)))[0]
            assert not after
            earlier = (await engine.state(scope, valid_at=base.at(7), known_at=base.at(20)))[0]
            assert earlier[0].valid_to == base.at(10)
            row = await engine.repository.admission_record(scope, moved.candidate_ids[0])
            assert row["payload"]["termination"]["source_event_id"] == event.id
            assert row["payload"]["termination"]["recorded_at"] != row["payload"]["valid_to"]
            with pytest.raises(ValueError, match="version"):
                await engine.retract(
                    scope,
                    moved.candidate_ids[0],
                    **{**options, "event": base.source(scope), "valid_to": base.at(9)},
                )
            await kernel.forget(ForgetRequest(scope, memory_ids=(event.id,), mode=ForgetMode.ERASE))
            assert not (await engine.state(scope, valid_at=base.at(12), known_at=base.at(20)))[0]
            assert first.claim_ids

    asyncio.run(run())
