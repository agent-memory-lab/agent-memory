import asyncio
import json
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_controls import configured
from test_durable_purge import source_id
from test_governed_models_v7 import configuration

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.model_extraction import (
    GENERATOR_PROMPT,
    GENERATOR_SCHEMA,
    REVIEWER_PROMPT,
    REVIEWER_SCHEMA,
    GovernedSourceCalls,
    ModelAtomGenerator,
    ModelAtomReviewer,
)
from agent_memory.domain import PredicateSpec, SourceAuthority
from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
from agent_memory.retrieval.model_contracts import ModelError, ModelResponse, canonical, digest

store = base.store


class ModelPort:
    def __init__(self, prompt, schema, *, bad=False):
        self.configuration = configuration(
            template_sha256=digest(prompt), output_schema_json=canonical(schema)
        )
        self.calls, self.bad = [], bad

    async def generate(self, request):
        self.calls.append(request)
        messages = json.loads(request.payload_json)["messages"]
        task = json.loads(messages[-1]["content"])
        if task["operation"] == "extract_atoms":
            source = json.loads(messages[1]["content"])
            result = {
                "atoms": [
                    {
                        "subject_id": "alice",
                        "predicate": "response_language",
                        "value": "zh-CN",
                        "kind": "preference",
                        "modality": "asserted",
                        "source_quote": source["content"],
                    }
                ]
            }
            if self.bad:
                result["atoms"][0]["subject_id"] = "unregistered"
        else:
            result = {
                "reviews": [
                    {
                        "candidate_index": i,
                        "faithfulness": "supported",
                        "retention": "durable",
                        "reasons": ["explicit_assertion"],
                    }
                    for i in range(len(task["candidates"]))
                ]
            }
        return ModelResponse(canonical(result), 25, 10, 1000)


async def setup(engine, scope, clock, *, context=False, bad=False):
    service, *_ = await configured(engine, None, scope, clock, inputs=2)
    ledger = ModelBudget(engine.repository)
    accounts = await ledger.configure((BudgetAccount("model-extraction", "run", "USD", "1", None),))
    ports = (
        ModelPort(GENERATOR_PROMPT, GENERATOR_SCHEMA, bad=bad),
        ModelPort(REVIEWER_PROMPT, REVIEWER_SCHEMA),
    )

    async def guard(uow, coordinates):
        return coordinates.principal == "alice" and coordinates.purpose == "agent_context"

    calls = tuple(
        GovernedSourceCalls(
            service,
            port,
            public_template=prompt,
            account_keys=accounts,
            principal="alice",
            project="project-a",
            purpose="agent_context",
            host_guard=guard,
            role=role,
        )
        for port, prompt, role in zip(
            ports,
            (GENERATOR_PROMPT, REVIEWER_PROMPT),
            ("atom_generation", "atom_review"),
            strict=True,
        )
    )
    event_id = source_id(scope, "1")
    async with engine.repository.unit_of_work() as uow:
        event = await uow.get_source_event(scope, event_id)
    context_ids = (source_id(scope, "2"),) if context else ()
    for call in calls:
        authority = call.authority({}, clock[0])
        for identity in (event_id, *context_ids):
            # Both configurations share one recipient, so grant once.
            if call is calls[0]:
                await authority.allow_processing(
                    identity,
                    readers=("alice",),
                    purposes=("agent_context",),
                    expires_at=clock[0] + timedelta(hours=1),
                )
    generator = ModelAtomGenerator(
        calls[0],
        subjects=("alice",),
        predicates=("response_language",),
        context_source_ids=context_ids,
    )
    reviewer = ModelAtomReviewer(calls[1], context_source_ids=context_ids)
    return event, AtomExtractionPipeline(generator, reviewer), ports, calls, ledger


def test_real_governor_used_for_generation_and_review_preview(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, pipeline, ports, calls, ledger = await setup(engine, scope, clock)
            prepared = await pipeline.prepare(
                event,
                authority=SourceAuthority(
                    "alice-login", subjects=("alice",), predicates=("response_language",)
                ),
                policy=AdmissionPolicy([PredicateSpec("response_language")]),
            )
            assert len(prepared["drafts"]) == 1
            assert all(len(p.calls) == 1 for p in ports)
            assert len(await ledger.snapshot()) == 2
            assert set(prepared["audit"]["processing_inputs"]) == {"generator", "reviewer"}
            assert prepared["audit"]["reports"][0]["faithfulness"] == "supported"
            assert not await engine.repository.admission_record(scope, "fabricated")

    asyncio.run(run())


def test_unregistered_model_identity_never_becomes_candidate(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, pipeline, ports, calls, ledger = await setup(engine, scope, clock, bad=True)
            prepared = await pipeline.prepare(
                event,
                authority=SourceAuthority(
                    "alice-login", subjects=("alice",), predicates=("response_language",)
                ),
                policy=AdmissionPolicy([PredicateSpec("response_language")]),
            )
            assert not prepared["drafts"] and prepared["audit"]["processing_state"] == "failed"
            assert len(ports[0].calls) == 1 and not ports[1].calls

    asyncio.run(run())


def test_cross_turn_context_stays_pending_and_publication_checks_all_inputs(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, pipeline, ports, calls, ledger = await setup(engine, scope, clock, context=True)
            authority = SourceAuthority(
                "alice-login", subjects=("alice",), predicates=("response_language",)
            )
            policy = AdmissionPolicy([PredicateSpec("response_language")])
            prepared = await pipeline.prepare(event, authority=authority, policy=policy)
            assert all(action == "PENDING_VERIFICATION" for action, _ in prepared["gates"].values())
            model_authority = calls[0].authority({}, clock[0])
            await model_authority.allow_processing(
                source_id(scope, "2"),
                readers=("alice",),
                purposes=("agent_context",),
                expires_at=clock[0] + timedelta(hours=1),
                expected_version=1,
                revoked=True,
            )
            with pytest.raises(ModelError):
                await pipeline.publish_prepared(
                    engine.repository, event, prepared, authority=authority, policy=policy
                )

    asyncio.run(run())


def test_strict_overflow_creates_no_finance_or_provider_dispatch(store):
    from dataclasses import replace

    from agent_memory.context.model_input_budget import ModelInputBudget, TokenBudgetPort

    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, pipeline, ports, calls, ledger = await setup(engine, scope, clock)
            port = ports[0]
            spec = ModelInputBudget(
                port.configuration.model_revision,
                "fixture-tokenizer/1",
                "fixture-renderer/1",
                100,
                20,
            )
            port.configuration = replace(
                port.configuration,
                tokenizer_revision=spec.tokenizer_revision,
                overflow_guard_sha256=spec.fingerprint,
                options_json=canonical({"num_ctx": 100, "num_predict": 20}),
            )
            guarded = TokenBudgetPort(port, spec, lambda _: 81, verify_binding=lambda *_: True)
            new_calls = GovernedSourceCalls(
                calls[0].service,
                guarded,
                public_template=GENERATOR_PROMPT,
                account_keys=calls[0].accounts,
                principal="alice",
                project="project-a",
                purpose="agent_context",
                host_guard=calls[0].host_guard,
                role="atom_generation",
            )
            generator = ModelAtomGenerator(
                new_calls, subjects=("alice",), predicates=("response_language",)
            )
            with pytest.raises(ModelError, match="input_token_budget_exceeded"):
                await generator.generate_atoms(event)
            assert not await ledger.snapshot() and port.calls == []

    asyncio.run(run())


def test_erasing_processed_context_erases_published_candidate(store):
    from agent_memory.domain import ForgetMode, ForgetRequest

    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, pipeline, ports, calls, ledger = await setup(engine, scope, clock, context=True)
            authority = SourceAuthority(
                "alice-login", subjects=("alice",), predicates=("response_language",)
            )
            policy = AdmissionPolicy([PredicateSpec("response_language")])
            prepared = await pipeline.prepare(event, authority=authority, policy=policy)
            receipt = await pipeline.publish_prepared(
                engine.repository,
                event,
                prepared,
                authority=authority,
                policy=policy,
                retained=True,
            )
            candidate = receipt.candidate_ids[0]
            assert await engine.repository.admission_record(scope, candidate)
            await kernel.forget(
                ForgetRequest(scope, (source_id(scope, "2"),), mode=ForgetMode.ERASE)
            )
            assert await engine.repository.admission_record(scope, candidate) is None

    asyncio.run(run())


def test_revoked_processing_rights_stop_even_tokenizer_before_finance(store):
    from dataclasses import replace

    from agent_memory.context.model_input_budget import ModelInputBudget, TokenBudgetPort
    from agent_memory.retrieval.model_answers import GovernedModelAnswers

    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, pipeline, ports, calls, ledger = await setup(engine, scope, clock)
            port = ports[0]
            spec = ModelInputBudget(
                port.configuration.model_revision, "tokens/1", "renderer/1", 100, 20
            )
            port.configuration = replace(
                port.configuration,
                tokenizer_revision=spec.tokenizer_revision,
                overflow_guard_sha256=spec.fingerprint,
                options_json=canonical({"num_ctx": 100, "num_predict": 20}),
            )
            counts = []

            def counter(payload):
                counts.append(payload)
                return 10

            guarded = TokenBudgetPort(port, spec, counter, verify_binding=lambda *_: True)
            source_calls = GovernedSourceCalls(
                calls[0].service,
                guarded,
                public_template=GENERATOR_PROMPT,
                account_keys=calls[0].accounts,
                principal="alice",
                project="project-a",
                purpose="agent_context",
                host_guard=calls[0].host_guard,
                role="atom_generation",
            )
            authority = source_calls.authority({}, clock[0])
            sealed = await authority.prepare(authority.expected, (event.id,))
            await authority.allow_processing(
                event.id,
                readers=("alice",),
                purposes=("agent_context",),
                expires_at=clock[0] + timedelta(hours=1),
                expected_version=1,
                revoked=True,
            )
            governor = GovernedModelAnswers(
                authority,
                guarded,
                account_keys=source_calls.accounts,
                validate_output=lambda *_: True,
            )
            with pytest.raises(ModelError):
                await governor.answer(sealed)
            assert counts == [] and port.calls == [] and not await ledger.snapshot()

    asyncio.run(run())
