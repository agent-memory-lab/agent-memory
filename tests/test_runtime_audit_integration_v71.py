"""Combined governed-model/audit boundaries; fixture outputs make no quality claim."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_model_extraction import setup
from test_source_omission_audit import Auditor, Host

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.model_extraction import ModelAtomGenerator
from agent_memory.consolidation.source_audit import (
    RetainedSourceAuditHost,
    SourceAuditObservation,
    SourceAuditTarget,
    SourceAuditUnavailable,
    SourceOmissionAudit,
)
from agent_memory.derived.model import DerivedError
from agent_memory.domain import MemoryEvent, PredicateSpec, SourceAuthority
from agent_memory.retrieval.model_contracts import ModelError, ModelResponse, canonical

store = base.store
AUTHORITY = SourceAuthority("alice-login", subjects=("alice",), predicates=("response_language",))
POLICY = AdmissionPolicy([PredicateSpec("response_language")])


async def governed_audit(engine, scope, clock):
    event, pipeline, ports, calls, _ = await setup(engine, scope, clock)
    original_generate = ports[0].generate

    async def empty(request):
        ports[0].calls.append(request)
        return ModelResponse(canonical({"atoms": []}), 25, 10, 1000)

    ports[0].generate = empty
    host = Host(
        targets=(
            SourceAuditTarget(
                "language",
                "Which response language?",
                ("response_language",),
                0,
                len(event.content),
            ),
        )
    )
    proposal = dict(
        subject_id="alice",
        predicate="response_language",
        value="zh-CN",
        kind="preference",
        modality="asserted",
        source_quote=event.content,
    )
    auditor = Auditor(
        (
            SourceAuditObservation(
                "language",
                "response_language",
                "missing_candidate",
                ("explicit_source",),
                (proposal,),
            ),
        )
    )
    pipeline.source_audit = SourceOmissionAudit(
        auditor,
        RetainedSourceAuditHost(engine.repository, host),
        local_only=True,
        enabled=True,
        contract_test_only=True,
        clock=lambda: clock[0],
    )
    return event, pipeline, host, ports, calls, original_generate


@pytest.mark.parametrize(
    "mutation", ["audit-revoke", "audit-expiry", "model-config", "model-expiry"]
)
def test_model_and_audit_final_guards_share_publication_transaction(store, monkeypatch, mutation):
    async def run():
        async with store() as (engine, _, scope, clock):
            event, pipeline, host, *_ = await governed_audit(engine, scope, clock)
            prepared = await pipeline.prepare(event, authority=AUTHORITY, policy=POLICY)
            assert prepared["audit"]["processing_inputs"]
            assert prepared["audit"]["source_audit"]["calls"] == 1
            before = await engine.repository.admission_records(scope)
            cls = type(engine.repository.unit_of_work())
            original = cls.save_admission_record
            wrote = []

            async def change_after_write(uow, *args, **kwargs):
                result = await original(uow, *args, **kwargs)
                wrote.append(True)
                if mutation == "audit-revoke":
                    host.denied = True
                elif mutation == "audit-expiry":
                    host.fence = replace(host.fence, expires_at=clock[0])
                elif mutation == "model-config":
                    pipeline.generator.context_source_ids = ("unregistered-context",)
                else:
                    clock[0] += timedelta(hours=1)
                return result

            monkeypatch.setattr(cls, "save_admission_record", change_after_write)
            with pytest.raises((SourceAuditUnavailable, ModelError, DerivedError)):
                await pipeline.publish_prepared(
                    engine.repository,
                    event,
                    prepared,
                    authority=AUTHORITY,
                    policy=POLICY,
                    retained=True,
                )
            assert wrote
            assert await engine.repository.admission_records(scope) == before

    asyncio.run(run())


def test_governed_reprocessing_keeps_recovery_pending_until_explicit_host_resolution(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, pipeline, _, ports, calls, original_generate = await governed_audit(
                engine, scope, clock
            )
            prepared = await pipeline.prepare(event, authority=AUTHORITY, policy=POLICY)
            receipt = await pipeline.publish_prepared(
                engine.repository,
                event,
                prepared,
                authority=AUTHORITY,
                policy=POLICY,
                retained=True,
            )
            assert receipt.decisions[0].action == "PENDING_VERIFICATION"
            assert not receipt.claim_ids
            assert prepared["audit"]["source_audit"]["recall_proven"] is False
            assert prepared["audit"]["source_audit"]["world_negative_proof"] is False
            # A later normal model extraction has the same source and slot but a
            # fresh publication identity. Removing auditing cannot remove its hold.
            ports[0].generate = original_generate
            pipeline.generator = ModelAtomGenerator(
                calls[0],
                subjects=("alice",),
                predicates=("response_language",),
                max_candidates=31,
            )
            pipeline.source_audit = None
            regenerated = await pipeline.prepare(event, authority=AUTHORITY, policy=POLICY)
            assert len(regenerated["drafts"]) == 1
            assert all(action == "ACCEPT" for action, _ in regenerated["gates"].values())
            later = await pipeline.publish_prepared(
                engine.repository,
                event,
                regenerated,
                authority=AUTHORITY,
                policy=POLICY,
                retained=True,
                publication_id="model-reinterpretation-2",
            )
            key = later.candidate_ids[0]
            assert key != receipt.candidate_ids[0]
            assert later.decisions[0].action == "PENDING_VERIFICATION"
            assert not later.claim_ids
            row = await engine.repository.admission_record(scope, key)
            assert (
                row["payload"]["source_audit_hold"]["origin_candidate_id"]
                == receipt.candidate_ids[0]
            )
            verified = await kernel.resolve_atom(
                scope,
                key,
                event=MemoryEvent(
                    scope, "host.review", event.content, actor="alice", occurred_at=clock[0]
                ),
                authority=AUTHORITY,
                policy=POLICY,
                expected_version=row["version"],
                accept=True,
                source_quote=event.content,
            )
            assert verified.decisions[0].action == "ACCEPT"
            current = await engine.repository.admission_record(scope, key)
            assert current["payload"]["claim_id"] in {
                claim.id
                for claim in (await engine.state(scope, valid_at=clock[0], known_at=base.at(30)))[0]
            }

    asyncio.run(run())


@pytest.mark.parametrize(
    "mutation",
    [
        "model-expiry",
        "model-config",
        "model-floor",
        "adapter",
        "adapter-version",
        "pipeline-config",
    ],
)
def test_last_audit_await_rechecks_prior_model_fences(store, monkeypatch, mutation):
    async def run():
        async with store() as (engine, _, scope, clock):
            event, pipeline, host, _, calls, _ = await governed_audit(engine, scope, clock)
            prepared = await pipeline.prepare(event, authority=AUTHORITY, policy=POLICY)
            before = await engine.repository.admission_records(scope)
            cls = type(engine.repository.unit_of_work())
            original = cls.save_admission_record
            wrote = []

            async def record(uow, *args, **kwargs):
                result = await original(uow, *args, **kwargs)
                wrote.append(True)
                return result

            def late(uow):
                if uow is not None and wrote:
                    if mutation == "model-expiry":
                        clock[0] += timedelta(hours=1)
                    elif mutation == "model-config":
                        pipeline.generator.context_source_ids = ("forged",)
                    elif mutation == "model-floor":
                        calls[0].service.authority_min_version += 1
                    elif mutation == "adapter-version":
                        pipeline.generator.version = "changed"
                    elif mutation == "pipeline-config":
                        pipeline.max_candidates = 1
                    else:
                        pipeline.generator = ModelAtomGenerator(
                            calls[0],
                            subjects=("alice",),
                            predicates=("response_language",),
                        )

            host.callback = late
            monkeypatch.setattr(cls, "save_admission_record", record)
            with pytest.raises((SourceAuditUnavailable, ModelError, DerivedError, ValueError)):
                await pipeline.publish_prepared(
                    engine.repository,
                    event,
                    prepared,
                    authority=AUTHORITY,
                    policy=POLICY,
                    retained=True,
                )
            assert wrote
            assert await engine.repository.admission_records(scope) == before

    asyncio.run(run())
