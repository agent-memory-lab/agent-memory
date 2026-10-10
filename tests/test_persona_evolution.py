"""Evidence-driven L3 lifecycle on the existing fenced scheduler."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_controls import configured
from test_durable_purge import envelope, source_id

from agent_memory.derived.evolution import (
    PersonaDefinition,
    PersonaEvolution,
    PersonaProposal,
    PersonaReview,
)
from agent_memory.derived.model import DerivedError, ProcessingGrant, digest
from agent_memory.derived.persona import PersonaViews
from agent_memory.domain import ForgetMode, ForgetRequest, MemoryEvent
from agent_memory.operations.refresh_host import RefreshHost
from agent_memory.operations.worker_tasks import WorkerLimits

store = base.store


class Proposer:
    revision = "host-language-proposer/1"

    def __init__(self):
        self.calls = 0

    async def propose(self, definition, facts):
        self.calls += 1
        return PersonaProposal(
            "Prefers Chinese communication",
            tuple(
                (f["atom_id"], "supports" if f["value"] == "zh-CN" else "counterexample")
                for f in facts
            ),
        )


class Reviewer:
    revision = "host-language-review/1"

    async def review(self, definition, proposal, facts):
        return PersonaReview("publish", "reviewed")


async def fixture(engine, kernel, scope, clock, *, inputs=3):
    svc, _, capture, *_ = await configured(engine, kernel, scope, clock, inputs=inputs)
    views = PersonaViews(svc, reviewer_revision="host-language-review/1")
    proposer, reviewer = Proposer(), Reviewer()
    lifecycle = PersonaEvolution(views, proposer=proposer, reviewer=reviewer)
    await lifecycle.register(
        PersonaDefinition("communication", "alice", ("locale",), minimum_span_seconds=0)
    )
    host = RefreshHost(
        lifecycle.queue,
        worker_id="persona-host",
        limits=WorkerLimits(max_concurrency=1),
        clock_tolerance_seconds=300,
    )
    return svc, lifecycle, host, capture, proposer


def test_registered_persona_recomputes_new_counterexamples_and_preserves_history(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, capture, proposer = await fixture(engine, kernel, scope, clock)
            result = await host.run_once()
            assert result.completed == 1 and result.failed == 0
            first = await lifecycle.read("communication", actor="alice", context={})
            assert first["truth_status"] == "hypothesis" and first["state"] == "active"
            clock[0] += timedelta(seconds=2)
            # A newly admitted contradicting member must invalidate even though
            # it was never cited in the previous profile.
            _, session, client, _, generator, worker, *_ = capture
            generator.values = (("locale", "en-US"),)
            await client.durable_append(envelope(scope, "english", clock), session, 4)
            assert await worker.run_once()
            async with engine.repository.unit_of_work() as uow:
                event = await uow.get_source_event(scope, source_id(scope, "english"))
            await svc.grant(ProcessingGrant(event.id, ("alice",)))
            with pytest.raises(DerivedError, match="persona_evolution_stale"):
                await lifecycle.read("communication", actor="alice", context={})
            clock[0] += timedelta(seconds=2)
            assert (await host.run_once()).completed == 1
            current = await lifecycle.read("communication", actor="alice", context={})
            assert current["state"] == "contested" and current["version"] == 2
            old = await lifecycle.views.read_history(
                "communication",
                revision_id=first["revision_id"],
                actor="alice",
                purpose="agent_context",
                context={},
            )
            assert old["version"] == 1 and old["state"] == "active"
            assert proposer.calls == 2

    asyncio.run(run())


def test_retracted_support_withdraws_current_but_keeps_prior_revision_until_erasure(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, _, _ = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            first = await lifecycle.read("communication", actor="alice", context={})
            row = next(
                r
                for r in await engine.repository.admission_records(scope)
                if r["payload"]["draft"]["predicate"] == "locale"
            )
            clock[0] += timedelta(seconds=2)
            event = MemoryEvent(
                scope, "message", "The earlier statement ends", occurred_at=clock[0]
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
            clock[0] += timedelta(seconds=2)
            assert (await host.run_once()).completed == 1
            assert (await lifecycle.status("communication", actor="alice"))["state"] == "withdrawn"
            with pytest.raises(DerivedError, match="persona_unavailable"):
                await lifecycle.read("communication", actor="alice", context={})
            old = await lifecycle.views.read_history(
                "communication",
                revision_id=first["revision_id"],
                actor="alice",
                purpose="agent_context",
                context={},
            )
            assert old["truth_status"] == "hypothesis"
            await kernel.forget(ForgetRequest(scope, (row["event_id"],), mode=ForgetMode.ERASE))
            with pytest.raises(DerivedError, match="persona_history_unavailable"):
                await lifecycle.views.read_history(
                    "communication",
                    revision_id=first["revision_id"],
                    actor="alice",
                    purpose="agent_context",
                    context={},
                )

    asyncio.run(run())


def test_semantic_review_cannot_omit_counterexamples_or_publish_feedback(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, _, proposer = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            first = await lifecycle.read("communication", actor="alice", context={})
            original = proposer.propose

            async def omit(definition, facts):
                proposal = await original(definition, facts)
                return replace(proposal, relations=proposal.relations[:-1])

            proposer.propose = omit
            await lifecycle.mark_dirty("communication", reason="review_requested")
            clock[0] += timedelta(seconds=2)
            result = await host.run_once()
            assert result.failed == 1 and result.completed == 0
            assert (await lifecycle.status("communication", actor="alice"))["version"] == first[
                "version"
            ]

    asyncio.run(run())


def test_permission_expiry_schedules_withdrawal_and_current_acl_hides_history(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, _, proposer = await fixture(engine, kernel, scope, clock)
            source = source_id(scope, "1")
            await svc.grant(
                ProcessingGrant(source, ("alice",), expires_at=clock[0] + timedelta(seconds=5)),
                expected_version=2,
            )
            assert (await host.run_once()).completed == 1
            first = await lifecycle.read("communication", actor="alice", context={})
            assert first["valid_until"] == (clock[0] + timedelta(seconds=5)).isoformat()
            clock[0] += timedelta(seconds=6)
            with pytest.raises(DerivedError, match="processing_grant_expired"):
                await lifecycle.views.read_history(
                    "communication",
                    revision_id=first["revision_id"],
                    actor="alice",
                    purpose="agent_context",
                    context={},
                )
            assert (await host.run_once()).completed == 1
            assert (await lifecycle.status("communication", actor="alice"))["state"] == "withdrawn"
            assert proposer.calls == 1

    asyncio.run(run())


@pytest.mark.parametrize("case", ["planned", "unsupported", "model_feedback"])
def test_non_factual_or_unfaithful_or_model_origin_inputs_cannot_contest_profile(store, case):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, capture, _ = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            producer, session, client, _, generator, worker, *_ = capture
            generator.values = (("locale", "en-US"),)
            if case == "unsupported":
                generator.verdict = "unsupported"
            if case == "planned":
                original = generator.generate_atoms

                async def planned(event):
                    return [{**item, "modality": "planned"} for item in await original(event)]

                generator.generate_atoms = planned
            clock[0] += timedelta(seconds=2)
            raw = envelope(scope, case, clock)
            if case == "model_feedback":
                from agent_memory.lifecycle import LifecycleEvent

                raw["origin"] = "model"
                await producer.append(
                    replace(
                        LifecycleEvent.from_dict(raw, trusted_scope=scope).to_memory_event(),
                        id=source_id(scope, case),
                    ),
                    session,
                    sequence=4,
                    actor="alice",
                )
            else:
                await client.durable_append(raw, session, 4)
            assert await worker.run_once()
            await svc.grant(ProcessingGrant(source_id(scope, case), ("alice",)))
            clock[0] += timedelta(seconds=2)
            result = await host.run_once()
            assert result.completed == 1 and result.failed == 0
            current = await lifecycle.read("communication", actor="alice", context={})
            assert current["state"] == "active" and len(current["evidence"]) == 3

    asyncio.run(run())


def test_changes_during_semantic_review_cannot_publish_or_discharge_new_inputs(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, _, _ = await fixture(engine, kernel, scope, clock)
            await lifecycle.queue.initialize()
            lease = await lifecycle.queue.claim("first-host", lease_seconds=10)
            snapshot = await lifecycle.snapshot(lease.task)
            await svc.grant(
                ProcessingGrant(source_id(scope, "1"), ("alice",), revoked=True), expected_version=2
            )
            with pytest.raises(DerivedError, match="derived_snapshot_changed"):
                await lifecycle.publish(lease.task, snapshot, lifecycle.prepare(snapshot))
            async with svc.repository.unit_of_work() as uow:
                assert (
                    await uow.derived_get(
                        scope,
                        "persona_header",
                        "persona:" + digest([scope.partition_key(), "communication"]),
                    )
                    is None
                )
                job = await uow.derived_get(scope, "job", lease.task.id)
                assert job["status"] == "running"
            await lifecycle.queue.fail(lease, DerivedError("derived_snapshot_changed"))

    asyncio.run(run())


def test_forged_snapshot_is_rejected_before_semantic_adapters_see_data(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, lifecycle, _, _, proposer = await fixture(engine, kernel, scope, clock)
            await lifecycle.queue.initialize()
            lease = await lifecycle.queue.claim("first-host", lease_seconds=10)
            snapshot = await lifecycle.snapshot(lease.task)
            snapshot["inventory"]["facts"][0]["value"] = "forged-data"
            with pytest.raises(DerivedError, match="derived_snapshot_changed"):
                await lifecycle.publish(lease.task, snapshot, lifecycle.prepare(snapshot))
            assert proposer.calls == 0

    asyncio.run(run())


def test_expired_or_superseded_worker_cannot_run_semantic_review_or_publish(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, lifecycle, _, _, proposer = await fixture(engine, kernel, scope, clock)
            await lifecycle.queue.initialize()
            lease = await lifecycle.queue.claim("dead-host", lease_seconds=5)
            snapshot = await lifecycle.snapshot(lease.task)
            clock[0] += timedelta(seconds=6)
            from agent_memory.operations.worker_tasks import WorkerQueueError

            with pytest.raises(WorkerQueueError, match="stale"):
                await lifecycle.publish(lease.task, snapshot, lifecycle.prepare(snapshot))
            assert proposer.calls == 0
            restarted = PersonaEvolution(lifecycle.views, proposer=Proposer(), reviewer=Reviewer())
            assert (
                await RefreshHost(
                    restarted.queue,
                    worker_id="restart",
                    limits=WorkerLimits(max_concurrency=1),
                    clock_tolerance_seconds=300,
                ).run_once()
            ).completed == 1
            assert (await restarted.read("communication", actor="alice", context={}))[
                "version"
            ] == 1

    asyncio.run(run())


def test_stability_requires_observations_across_time_not_merely_waiting(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, capture, proposer = await fixture(engine, kernel, scope, clock)
            await lifecycle.register(
                PersonaDefinition("communication", "alice", ("locale",), minimum_span_seconds=60),
                expected_generation=1,
            )
            assert (await host.run_once()).completed == 1
            assert (await lifecycle.status("communication", actor="alice"))["state"] == "withdrawn"
            assert proposer.calls == 0
            clock[0] += timedelta(seconds=120)
            await lifecycle.mark_dirty("communication")
            assert (await host.run_once()).completed == 1
            assert (await lifecycle.status("communication", actor="alice"))["state"] == "withdrawn"
            assert proposer.calls == 0
            _, session, client, _, _, worker, *_ = capture
            await client.durable_append(envelope(scope, "later", clock), session, 4)
            assert await worker.run_once()
            await svc.grant(ProcessingGrant(source_id(scope, "later"), ("alice",)))
            clock[0] += timedelta(seconds=2)
            assert (await host.run_once()).completed == 1
            assert (await lifecycle.read("communication", actor="alice", context={}))[
                "state"
            ] == "active"
            assert proposer.calls == 1

    asyncio.run(run())


def test_concrete_categorical_policy_runs_reviewed_pipeline_and_rejects_adapter_change(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived.evolution import categorical_persona_evolution
            from agent_memory.derived.persona_policy import CategoricalPersonaPolicy

            svc, *_ = await configured(engine, kernel, scope, clock, inputs=3)
            lifecycle = categorical_persona_evolution(
                svc,
                CategoricalPersonaPolicy("locale", "zh-CN", "Often prefers Chinese communication"),
            )
            await lifecycle.register(
                PersonaDefinition("language", "alice", ("locale",), minimum_span_seconds=0)
            )
            host = RefreshHost(
                lifecycle.queue,
                worker_id="approved-policy",
                limits=WorkerLimits(max_concurrency=1),
                clock_tolerance_seconds=300,
            )
            assert (await host.run_once()).completed == 1
            current = await lifecycle.read("language", actor="alice", context={})
            assert current["text"] == "Often prefers Chinese communication"
            assert current["truth_status"] == "hypothesis"
            lifecycle.proposer.policy = replace(lifecycle.proposer.policy, target_value="en-US")
            with pytest.raises(DerivedError, match="adapter_configuration_changed"):
                await lifecycle.read("language", actor="alice", context={})

    asyncio.run(run())


def test_source_revision_withdraws_current_and_preserves_old_review_then_rebuilds(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, capture, _ = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            first = await lifecycle.read("communication", actor="alice", context={})
            producer, session, _, _, _, worker, *_ = capture
            clock[0] += timedelta(seconds=2)
            new = MemoryEvent(
                scope,
                "message",
                "Revised language preference",
                id="revised-source",
                occurred_at=clock[0],
                actor="alice",
            )
            await producer.revise(
                new,
                session,
                sequence=4,
                actor="alice",
                base_event_id=source_id(scope, "1"),
                expected_revision=1,
            )
            assert (await host.run_once()).completed == 1
            assert (await lifecycle.status("communication", actor="alice"))["state"] == "withdrawn"
            old = await lifecycle.views.read_history(
                "communication",
                revision_id=first["revision_id"],
                actor="alice",
                purpose="agent_context",
                context={},
            )
            assert old["state"] == "active" and old["version"] == 1
            assert await worker.run_once()
            await svc.grant(ProcessingGrant(new.id, ("alice",)))
            clock[0] += timedelta(seconds=2)
            assert (await host.run_once()).completed == 1
            rebuilt = await lifecycle.read("communication", actor="alice", context={})
            assert rebuilt["state"] == "active" and rebuilt["version"] == 3
            assert {e["source_id"] for e in rebuilt["evidence"]} == {
                new.id,
                source_id(scope, "2"),
                source_id(scope, "3"),
            }

    asyncio.run(run())


def test_distinct_source_families_cannot_reuse_one_accepted_atom_to_fake_independence(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived.persona import PersonaEvidence

            svc, *_ = await configured(engine, kernel, scope, clock, inputs=3)
            views = PersonaViews(svc, reviewer_revision="host-language-review/1")
            rows = await engine.repository.admission_records(scope)
            genuine = next(
                r
                for r in rows
                if r["event_id"] == source_id(scope, "3")
                and r["payload"]["draft"]["predicate"] == "locale"
            )
            references = []
            async with svc.repository.unit_of_work() as uow:
                for number in ("1", "2", "3"):
                    source = await uow.get_source_event(scope, source_id(scope, number))
                    references.append(
                        PersonaEvidence(
                            source.id,
                            0,
                            len(source.content),
                            source.content,
                            source.metadata["_retention"]["document_id"],
                            atom_id=genuine["id"],
                            atom_version=genuine["version"],
                        )
                    )
            with pytest.raises(DerivedError, match="persona_atom_changed"):
                await views.publish(
                    "forged-families",
                    "Prefers Chinese",
                    origin="inferred",
                    evidence=references,
                    readers=("alice",),
                    purpose="agent_context",
                    context={},
                    valid_from=clock[0],
                    valid_to=clock[0] + timedelta(minutes=10),
                )

    asyncio.run(run())


def test_secondary_domain_witness_rights_are_required_and_rechecked_before_refresh(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from datetime import datetime

            svc, lifecycle, host, capture, proposer = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            _, session, client, _, generator, worker, *_ = capture
            clock[0] += timedelta(seconds=2)
            generator.values = (("locale", "en-US"),)
            await client.durable_append(envelope(scope, "english", clock), session, 4)
            assert await worker.run_once()
            pending = next(
                r
                for r in await engine.repository.admission_records(scope)
                if r["event_id"] == source_id(scope, "english")
            )
            witness = MemoryEvent(
                scope,
                "tool",
                "Verified English preference",
                id="domain-witness",
                occurred_at=clock[0],
            )
            await engine.resolve(
                scope,
                pending["id"],
                event=witness,
                authority=base.TOOL,
                policy=base.POLICY,
                expected_version=pending["version"],
                accept=True,
                source_quote=witness.content,
                support_from=datetime.fromisoformat(pending["payload"]["valid_from"]),
            )
            await svc.grant(ProcessingGrant(source_id(scope, "english"), ("alice",)))
            await svc.grant(ProcessingGrant(witness.id, ("alice",)))
            clock[0] += timedelta(seconds=2)
            assert (await host.run_once()).completed == 1
            second = await lifecycle.read("communication", actor="alice", context={})
            assert second["state"] == "contested"
            async with svc.repository.unit_of_work() as uow:
                archive = await uow.derived_get(scope, "persona_header", second["revision_id"])
                assert witness.id in archive["sources"]
                assert witness.id in archive["source_hashes"]
            await svc.grant(
                ProcessingGrant(witness.id, ("alice",), revoked=True), expected_version=1
            )
            with pytest.raises(DerivedError, match="processing_denied"):
                await lifecycle.views.read_history(
                    "communication",
                    revision_id=second["revision_id"],
                    actor="alice",
                    purpose="agent_context",
                    context={},
                )
            clock[0] += timedelta(seconds=2)
            assert (await host.run_once()).completed == 1
            assert (await lifecycle.status("communication", actor="alice"))["state"] == "withdrawn"
            assert proposer.calls == 2

    asyncio.run(run())


def test_final_source_grant_await_cannot_bypass_raised_authority_floor(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, _, _ = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            current = await lifecycle.read("communication", actor="alice", context={})
            cls = type(svc.repository.unit_of_work())
            original = cls.derived_get
            raised = False

            async def raise_floor(self, *args):
                nonlocal raised
                value = await original(self, *args)
                if len(args) >= 2 and args[1] == "grant" and not raised:
                    raised = True
                    svc.authority_min_version += 1
                return value

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_get", raise_floor)
                with pytest.raises(DerivedError, match="authority_rollback"):
                    await lifecycle.views.read_history(
                        "communication",
                        revision_id=current["revision_id"],
                        actor="alice",
                        purpose="agent_context",
                        context={},
                    )
            assert raised

    asyncio.run(run())


def test_trusted_reviewer_withdraws_hypothesis_and_keeps_auditable_previous_version(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, lifecycle, host, _, _ = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            before = await lifecycle.read("communication", actor="alice", context={})

            async def no_longer_stable(definition, proposal, facts):
                return PersonaReview("withdraw", "unstable")

            lifecycle.reviewer.review = no_longer_stable
            clock[0] += timedelta(seconds=2)
            await lifecycle.mark_dirty("communication")
            assert (await host.run_once()).completed == 1
            assert (await lifecycle.status("communication", actor="alice"))["state"] == "withdrawn"
            old = await lifecycle.views.read_history(
                "communication",
                revision_id=before["revision_id"],
                actor="alice",
                purpose="agent_context",
                context={},
                valid_at=clock[0] - timedelta(seconds=2),
            )
            assert old["text"] == before["text"] and old["known_at"] == before["published_at"]
            with pytest.raises(DerivedError, match="history_validity_unavailable"):
                await lifecycle.views.read_history(
                    "communication",
                    revision_id=before["revision_id"],
                    actor="alice",
                    purpose="agent_context",
                    context={},
                    valid_at=clock[0] + timedelta(days=1),
                )
            with pytest.raises(DerivedError, match="context_mismatch"):
                await lifecycle.views.read_history(
                    "communication",
                    revision_id=before["revision_id"],
                    actor="alice",
                    purpose="agent_context",
                    context={"project": "another"},
                )
            with pytest.raises(DerivedError, match="access_or_integrity"):
                await lifecycle.views.read_history(
                    "communication",
                    revision_id=before["revision_id"],
                    actor="mallory",
                    purpose="agent_context",
                    context={},
                )

    asyncio.run(run())


@pytest.mark.parametrize("adapter", ["proposer", "reviewer"])
def test_adapter_revision_changes_during_review_cannot_publish_with_old_approval(store, adapter):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, lifecycle, host, _, proposer = await fixture(engine, kernel, scope, clock)
            if adapter == "proposer":
                original = lifecycle.proposer.propose

                async def changed(definition, facts):
                    proposal = await original(definition, facts)
                    lifecycle.proposer.revision = "unapproved-proposer/2"
                    return proposal

                lifecycle.proposer.propose = changed
            else:
                original = lifecycle.reviewer.review

                async def changed(definition, proposal, facts):
                    verdict = await original(definition, proposal, facts)
                    lifecycle.reviewer.revision = "unapproved-reviewer/2"
                    return verdict

                lifecycle.reviewer.review = changed
            result = await host.run_once()
            assert result.failed == 1 and result.completed == 0
            assert proposer.calls == 1
            async with lifecycle.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "persona_header")

    asyncio.run(run())


def test_registration_rejects_scope_expansion_and_stale_changes_preserves_idempotency(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, lifecycle, host, _, _ = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            definition = PersonaDefinition(
                "communication", "alice", ("locale",), minimum_span_seconds=0
            )
            repeated = await lifecycle.register(definition)
            assert repeated["generation"] == 1
            with pytest.raises(DerivedError, match="selector_unregistered"):
                await lifecycle.register(replace(definition, subject_id="another-user"))
            with pytest.raises(DerivedError, match="authority_denied"):
                await lifecycle.register(replace(definition, label="secret", readers=("mallory",)))
            with pytest.raises(DerivedError, match="definition_conflict"):
                await lifecycle.register(replace(definition, context={"project": "new"}))
            with pytest.raises(DerivedError, match="derived_read_denied"):
                await lifecycle.status("communication", actor="mallory")
            assert (await lifecycle.read("communication", actor="alice", context={}))[
                "version"
            ] == 1

    asyncio.run(run())


@pytest.mark.parametrize(
    "mode", ["unverified_support", "unfaithful_counterexample", "invalid_review", "few_supports"]
)
def test_semantic_pipeline_denies_unsafe_classification_or_review_and_never_promotes_it(
    store, mode
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, lifecycle, host, capture, proposer = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            first = await lifecycle.read("communication", actor="alice", context={})
            if mode in {"unverified_support", "unfaithful_counterexample"}:
                _, session, client, _, generator, worker, *_ = capture
                generator.values = (("locale", "en-US"),)
                if mode == "unfaithful_counterexample":
                    generator.verdict = "uncertain"
                clock[0] += timedelta(seconds=2)
                await client.durable_append(envelope(scope, mode, clock), session, 4)
                assert await worker.run_once()
                await svc.grant(ProcessingGrant(source_id(scope, mode), ("alice",)))
            if mode == "unverified_support":

                async def wrong(definition, facts):
                    return PersonaProposal(
                        "Unsafe support", tuple((f["atom_id"], "supports") for f in facts)
                    )

                proposer.propose = wrong
            elif mode == "invalid_review":

                async def invalid(definition, proposal, facts):
                    return {"decision": "publish", "confidence": 1.0}

                lifecycle.reviewer.review = invalid
            elif mode == "few_supports":

                async def too_little(definition, facts):
                    return PersonaProposal(
                        "Insufficient signal",
                        tuple(
                            (f["atom_id"], "supports" if i < 2 else "not_relevant")
                            for i, f in enumerate(facts)
                        ),
                    )

                proposer.propose = too_little
            clock[0] += timedelta(seconds=2)
            await lifecycle.mark_dirty("communication")
            result = await host.run_once()
            if mode == "few_supports":
                assert result.completed == 1
                assert (await lifecycle.status("communication", actor="alice"))[
                    "state"
                ] == "withdrawn"
            else:
                assert result.failed == 1 and result.completed == 0
                assert (await lifecycle.status("communication", actor="alice"))["version"] == first[
                    "version"
                ]
            old = await lifecycle.views.read_history(
                "communication",
                revision_id=first["revision_id"],
                actor="alice",
                purpose="agent_context",
                context={},
            )
            assert old["text"] == first["text"]

    asyncio.run(run())


@pytest.mark.parametrize("inputs", [33, 65])
def test_oversized_evidence_census_fails_before_review_and_never_truncates_to_a_profile(
    store, inputs
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, lifecycle, host, _, proposer = await fixture(
                engine, kernel, scope, clock, inputs=inputs
            )
            receipt = await lifecycle.queue.request(
                lifecycle.definition_id("communication"), dedupe_key="oversized", actor="alice"
            )
            assert (await host.run_once()).idle
            status = await lifecycle.queue.status(receipt["target_id"], actor="alice")
            assert not status["complete"] and status["reason"] == "persona_evidence_capacity"
            assert proposer.calls == 0
            assert (await lifecycle.status("communication", actor="alice"))["state"] == "building"

    asyncio.run(run())


def test_erasure_restart_discovers_dirty_state_without_resurrecting_deleted_evidence(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, lifecycle, host, _, _ = await fixture(engine, kernel, scope, clock)
            assert (await host.run_once()).completed == 1
            first = await lifecycle.read("communication", actor="alice", context={})
            await kernel.forget(
                ForgetRequest(scope, (source_id(scope, "1"),), mode=ForgetMode.ERASE)
            )
            with pytest.raises(DerivedError, match="history_unavailable"):
                await lifecycle.views.read_history(
                    "communication",
                    revision_id=first["revision_id"],
                    actor="alice",
                    purpose="agent_context",
                    context={},
                )
            restarted = PersonaEvolution(lifecycle.views, proposer=Proposer(), reviewer=Reviewer())
            pending = await restarted.discover_changes()
            assert restarted.definition_id("communication") in pending
            async with lifecycle.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", pending[0])
                assert definition["epoch"] == await uow.retention_epoch(scope)
            assert (await restarted.status("communication", actor="alice"))["dirty"]

    asyncio.run(run())


@pytest.mark.parametrize("signal", ["pending_target", "faithful_counter", "negated_other"])
def test_concrete_policy_distinguishes_uncertain_support_from_counterexamples_and_negation(
    store, signal
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived.evolution import categorical_persona_evolution
            from agent_memory.derived.persona_policy import CategoricalPersonaPolicy

            svc, _, capture, *_ = await configured(engine, kernel, scope, clock, inputs=3)
            lifecycle = categorical_persona_evolution(
                svc,
                CategoricalPersonaPolicy("locale", "zh-CN", "Often prefers Chinese communication"),
            )
            await lifecycle.register(
                PersonaDefinition("language", "alice", ("locale",), minimum_span_seconds=0)
            )
            host = RefreshHost(
                lifecycle.queue,
                worker_id="categorical-distinctions",
                limits=WorkerLimits(max_concurrency=1),
                clock_tolerance_seconds=300,
            )
            assert (await host.run_once()).completed == 1
            _, session, client, _, generator, worker, *_ = capture
            generator.values = (("locale", "zh-CN" if signal == "pending_target" else "en-US"),)
            if signal == "pending_target":
                generator.verdict = "uncertain"
            if signal == "negated_other":
                original = generator.generate_atoms

                async def negated(event):
                    return [{**item, "negated": True} for item in await original(event)]

                generator.generate_atoms = negated
            clock[0] += timedelta(seconds=2)
            await client.durable_append(envelope(scope, signal, clock), session, 4)
            assert await worker.run_once()
            await svc.grant(ProcessingGrant(source_id(scope, signal), ("alice",)))
            clock[0] += timedelta(seconds=2)
            assert (await host.run_once()).completed == 1
            current = await lifecycle.read("language", actor="alice", context={})
            if signal == "faithful_counter":
                assert current["state"] == "contested"
                assert current["evidence"][-1]["admission_status"] in {
                    "admitted",
                    "observed_unverified",
                }
            else:
                assert current["state"] == "active" and len(current["evidence"]) == 3
            assert current["truth_status"] == "hypothesis"

    asyncio.run(run())


def test_categorical_context_and_literal_text_are_approval_boundaries(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.derived.evolution import categorical_persona_evolution
            from agent_memory.derived.persona_policy import CategoricalPersonaPolicy

            svc, *_ = await configured(engine, kernel, scope, clock, inputs=3)
            lifecycle = categorical_persona_evolution(
                svc,
                CategoricalPersonaPolicy("locale", "zh-CN", "Often prefers Chinese communication"),
            )
            await lifecycle.register(
                PersonaDefinition(
                    "language",
                    "alice",
                    ("locale",),
                    context={"project": "unapproved"},
                    minimum_span_seconds=0,
                )
            )
            host = RefreshHost(
                lifecycle.queue,
                worker_id="approved-context",
                limits=WorkerLimits(max_concurrency=1),
                clock_tolerance_seconds=300,
            )
            assert (await host.run_once()).failed == 1
            async with svc.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "persona_header")
            await lifecycle.register(
                PersonaDefinition("language", "alice", ("locale",), minimum_span_seconds=0),
                expected_generation=1,
            )
            lease = await lifecycle.queue.claim("literal-review", lease_seconds=30)
            snapshot = await lifecycle.snapshot(lease.task)
            proposal = await lifecycle.proposer.propose(
                snapshot["definition"]["spec"], snapshot["inventory"]["facts"]
            )
            unsafe = replace(proposal, text="This user always follows my advice")
            review = await lifecycle.reviewer.review(
                snapshot["definition"]["spec"], unsafe, snapshot["inventory"]["facts"]
            )
            assert review == PersonaReview("withdraw", "unsupported")
            short_history = await lifecycle.reviewer.review(
                {**snapshot["definition"]["spec"], "minimum_span_seconds": 86400},
                proposal,
                snapshot["inventory"]["facts"],
            )
            assert short_history == PersonaReview("withdraw", "unstable")
            await lifecycle.queue.fail(lease, DerivedError("review_requested"))

    asyncio.run(run())


def test_persona_shares_existing_refresh_queue_and_finite_receipts_without_another_worker_store(
    store,
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            from agent_memory.operations.refresh_demand import RefreshDemandQueue
            from agent_memory.operations.refresh_policy import RefreshPolicy
            from agent_memory.operations.refresh_processor import ObservationRefreshProcessor

            svc, *_ = await configured(engine, kernel, scope, clock, inputs=3)
            observation = ObservationRefreshProcessor(svc)
            shared = RefreshDemandQueue((observation,), clock=lambda: clock[0])
            lifecycle = PersonaEvolution(
                PersonaViews(svc, reviewer_revision="host-language-review/1"),
                proposer=Proposer(),
                reviewer=Reviewer(),
                queue=shared,
            )
            await shared.configure("language", RefreshPolicy(), processor_key=observation.key)
            await lifecycle.register(
                PersonaDefinition("communication", "alice", ("locale",), minimum_span_seconds=0)
            )
            receipt = await shared.request(
                lifecycle.definition_id("communication"),
                dedupe_key="shared-persona",
                actor="alice",
                processor_key=lifecycle.key,
            )
            host = RefreshHost(
                shared,
                worker_id="one-shared-host",
                limits=WorkerLimits(max_concurrency=1),
                clock_tolerance_seconds=300,
            )
            for _ in range(3):
                await host.run_once()
                clock[0] += timedelta(seconds=2)
            status = await shared.status(
                receipt["target_id"], actor="alice", processor_key=lifecycle.key
            )
            assert status["complete"]
            assert (await svc.read("language", actor="alice"))["state"] == "ready"
            assert (await lifecycle.read("communication", actor="alice", context={}))[
                "state"
            ] == "active"
            with pytest.raises(DerivedError, match="processor_configuration_changed"):
                PersonaEvolution(
                    lifecycle.views, proposer=Proposer(), reviewer=Reviewer(), queue=shared
                )
            with pytest.raises(DerivedError, match="independent_review_required"):
                PersonaEvolution(
                    lifecycle.views, proposer=lifecycle.proposer, reviewer=lifecycle.proposer
                )
            different = Reviewer()
            different.revision = "another-host-review-policy/1"
            with pytest.raises(DerivedError, match="reviewer_configuration_changed"):
                PersonaEvolution(lifecycle.views, proposer=Proposer(), reviewer=different)

    asyncio.run(run())
