"""Source-first coverage contracts; fakes prove fences, not real-model recall."""

import asyncio
from dataclasses import replace

import pytest
import test_atom_admission as admission_tests
from test_atom_admission import at, source
from test_atom_extraction import AUTHORITY, POLICY, Generator, Reviewer, candidate

from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.source_audit import (
    RetainedSourceAuditHost,
    SourceAuditFence,
    SourceAuditObservation,
    SourceAuditTarget,
    SourceAuditUnavailable,
    SourceOmissionAudit,
)
from agent_memory.domain import AtomDraft, ForgetMode, ForgetRequest
from agent_memory.operations.retention import DurableReceiver

store = admission_tests.store


class Host:
    version = "test-source-host/1"

    def __init__(self, *, targets=None):
        self.targets = targets
        self.fence = SourceAuditFence("revision-1", "acl-1", 0, at(1), at(1), at(30))
        self.denied = False
        self.transaction_checks = 0
        self.callback = None

    def select_targets(self, event):
        return (
            self.targets
            if self.targets is not None
            else (
                SourceAuditTarget(
                    "home", "Where does Alice live?", ("home_city",), 0, len(event.content)
                ),
            )
        )

    async def authorize_source(self, event, authority, *, unit_of_work=None):
        if self.callback:
            self.callback(unit_of_work)
        self.transaction_checks += int(unit_of_work is not None)
        return None if self.denied else self.fence


class Auditor:
    version = "test-source-auditor/1"

    def __init__(self, observations=None, *, callback=None):
        self.observations = observations
        self.callback = callback
        self.requests = []

    async def audit_source(self, request):
        self.requests.append(request)
        if self.callback:
            await self.callback()
        return (
            self.observations
            if self.observations is not None
            else (
                SourceAuditObservation(
                    "home", "home_city", "missing_candidate", ("explicit_source",), (candidate(),)
                ),
            )
        )


def pipeline(host, auditor, *, clock=None, generator=None, reviewer=None, **options):
    return AtomExtractionPipeline(
        generator or Generator([]),
        reviewer or Reviewer(),
        source_audit=SourceOmissionAudit(
            auditor,
            host,
            local_only=True,
            enabled=True,
            contract_test_only=True,
            clock=clock or (lambda: at(1)),
        ),
        **options,
    )


async def extract(kernel, event, selected):
    return await kernel.extract_event(event, pipeline=selected, authority=AUTHORITY, policy=POLICY)


async def retained(engine, scope, clock):
    event = source(scope, "我住在杭州", identity="retained", idempotency="retained")
    receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
    ticket = await receiver.issue_ticket(
        event,
        request_id="audit-request",
        producer_id="host",
        configuration_sha256="a" * 64,
    )
    await receiver.submit(event, ticket=ticket, producer_id="host", configuration_sha256="a" * 64)
    async with engine.repository.unit_of_work() as uow:
        return await uow.get_source_event(scope, event.id), receiver


def test_zero_generator_candidates_are_source_audited_and_normally_reviewed(store):
    async def run():
        async with store() as (engine, kernel, scope, _):
            host, auditor = Host(), Auditor()
            selected = pipeline(host, auditor)
            event = source(scope, "我住在杭州", idempotency="zero")
            result = await extract(kernel, event, selected)
            assert result.generation_calls == result.review_calls == 1
            assert result.decisions[0].action == "PENDING_VERIFICATION"
            assert auditor.requests[0].candidates == ()
            assert auditor.requests[0].ranges[0].text == event.content
            coverage = result.source_audit
            assert coverage["processing_state"] == "processed" and coverage["calls"] == 1
            assert not coverage["recall_proven"] and not coverage["world_negative_proof"]
            assert coverage["observations"][0]["status"] == "missing_candidate"
            assert host.transaction_checks >= 2
            claims, _ = await engine.state(scope, valid_at=at(2), known_at=at(30))
            assert not claims
            assert "source_audit_recovery_requires_host_verification" in result.decisions[0].reasons
            row = await kernel.admission_status(scope, result.admission.candidate_ids[0])
            assert row["payload"]["extraction"]["reports"][0]["source_audit_target"] == "home"
            assert (await kernel.extraction_status(scope, "zero")).source_audit == coverage

    asyncio.run(run())


@pytest.mark.parametrize(
    "faithfulness,action", [("unsupported", "REJECT"), ("uncertain", "PENDING_VERIFICATION")]
)
def test_recovered_proposal_cannot_bypass_candidate_review(store, faithfulness, action):
    async def run():
        async with store() as (_, kernel, scope, _):
            result = await extract(
                kernel,
                source(scope, "我住在杭州"),
                pipeline(Host(), Auditor(), reviewer=Reviewer(faithfulness)),
            )
            assert result.decisions[0].action == action
            assert not result.admission.claim_ids

    asyncio.run(run())


def test_recovered_proposal_cannot_invent_authority(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            selected = pipeline(Host(), Auditor())
            result = await kernel.extract_event(
                source(scope, "我住在杭州"),
                pipeline=selected,
                policy=POLICY,
                authority=replace(AUTHORITY, subjects=("bob",)),
            )
            assert result.decisions[0].action == "PENDING_VERIFICATION"
            assert "subject_not_authorized" in result.decisions[0].reasons
            assert not result.admission.claim_ids

    asyncio.run(run())


@pytest.mark.parametrize("status", ["no_additional_candidate", "unresolved", "conflict"])
def test_empty_coverage_is_explicit_and_never_world_negative_proof(store, status):
    async def run():
        async with store() as (_, kernel, scope, _):
            auditor = Auditor((SourceAuditObservation("home", "home_city", status, ("checked",)),))
            result = await extract(kernel, source(scope, "我住在杭州"), pipeline(Host(), auditor))
            assert not result.decisions and not result.admission.claim_ids
            assert result.source_audit["observations"][0]["status"] == status
            assert result.source_audit["processing_state"] == "processed"
            assert result.source_audit["recall_proven"] is False
            assert result.review_calls == 0

    asyncio.run(run())


def test_conflict_finding_holds_baseline_candidate_for_review(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            auditor = Auditor(
                (SourceAuditObservation("home", "home_city", "conflict", ("contradiction",)),)
            )
            result = await extract(
                kernel,
                source(scope, "我住在杭州"),
                pipeline(Host(), auditor, generator=Generator([candidate()])),
            )
            assert result.decisions[0].action == "PENDING_VERIFICATION"
            assert "source_audit_conflict" in result.decisions[0].reasons
            assert not result.admission.claim_ids

    asyncio.run(run())


@pytest.mark.parametrize(
    "change",
    [
        {"predicate": "response_language"},
        {"source_quote": "other"},
        {"source_start": 0, "source_end": 100},
        {"scope_level": "tenant"},
        {"change_kind": "correct", "corrects_id": "victim"},
    ],
)
def test_out_of_target_or_privileged_proposals_are_rejected(store, change):
    async def run():
        async with store() as (_, kernel, scope, _):
            auditor = Auditor(
                (
                    SourceAuditObservation(
                        "home",
                        "home_city",
                        "missing_candidate",
                        ("untrusted",),
                        ({**candidate(), **change},),
                    ),
                )
            )
            result = await extract(kernel, source(scope, "我住在杭州"), pipeline(Host(), auditor))
            assert result.decisions[0].action == "REJECT"
            assert result.source_audit["observations"][0]["status"] == "unresolved"
            assert not result.admission.claim_ids

    asyncio.run(run())


def test_audit_only_transmits_selected_ranges_and_bounded_candidate_context(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            target = SourceAuditTarget(
                "home", "Where does Alice live?", ("home_city",), 7, 12, "changed"
            )
            host, auditor = Host(targets=(target,)), Auditor()
            event = source(scope, "private我住在杭州outside")
            # The location is deliberately corrected before dispatch; offsets are characters.
            target = replace(target, source_start=7, source_end=12)
            assert event.content[7:12] == "我住在杭州"
            result = await extract(kernel, event, pipeline(host, auditor))
            assert auditor.requests[0].ranges[0].text == "我住在杭州"
            assert "private" not in repr(auditor.requests[0])
            assert result.decisions[0].action == "PENDING_VERIFICATION"

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["missing", "duplicate", "foreign", "exception", "timeout"])
def test_incomplete_or_failed_audit_records_unresolved_without_fabricating_coverage(store, failure):
    async def callback():
        if failure == "exception":
            raise RuntimeError("provider-secret")
        if failure == "timeout":
            await asyncio.Event().wait()

    async def run():
        async with store() as (_, kernel, scope, _):
            observation = SourceAuditObservation(
                "home", "home_city", "no_additional_candidate", ("checked",)
            )
            values = {
                "missing": (),
                "duplicate": (observation, observation),
                "foreign": (replace(observation, target_id="foreign"),),
            }
            result = await extract(
                kernel,
                source(scope, "我住在杭州"),
                pipeline(
                    Host(),
                    Auditor(values.get(failure), callback=callback),
                    timeout_seconds=0.01,
                ),
            )
            assert result.processing_state == "failed"
            assert result.source_audit["processing_state"] == "failed"
            assert result.source_audit["observations"][0]["status"] == "unresolved"
            assert "provider-secret" not in repr(result)
            assert not result.admission.claim_ids

    asyncio.run(run())


def test_candidate_budget_is_shared_and_overflow_is_unresolved(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            result = await extract(
                kernel,
                source(scope, "我住在杭州"),
                pipeline(
                    Host(),
                    Auditor(),
                    generator=Generator([candidate()]),
                    max_candidates=1,
                ),
            )
            assert len(result.admission.candidate_ids) == 1
            assert result.decisions[1].reasons == ("source_audit_candidate_budget_exhausted",)
            assert result.source_audit["observations"][0]["status"] == "unresolved"

    asyncio.run(run())


def test_default_off_and_unselected_ranges_preserve_baseline_and_typed_paths(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            auditor = Auditor()
            result = await extract(
                kernel, source(scope, "我住在杭州"), pipeline(Host(targets=()), auditor)
            )
            assert not auditor.requests
            assert result.source_audit["processing_state"] == "not_selected"
            baseline = await extract(
                kernel,
                source(scope, "我住在杭州"),
                AtomExtractionPipeline(Generator([]), Reviewer()),
            )
            assert baseline.source_audit is None
            typed = await kernel.admit_event(
                source(scope, "我住在杭州"),
                (AtomDraft("alice", "home_city", "Hangzhou", "home", "我住在杭州"),),
                authority=AUTHORITY,
                policy=POLICY,
            )
            assert typed.decisions[0].action == "ACCEPT"

    asyncio.run(run())


@pytest.mark.parametrize(
    "mutation", ["deny", "authorization", "erasure", "version", "expiry", "known_at", "valid_at"]
)
def test_inflight_audit_response_cannot_outlive_source_fence(store, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            host = Host()

            async def mutate():
                if mutation == "deny":
                    host.denied = True
                elif mutation == "expiry":
                    clock[0] = at(30)
                else:
                    changes = {
                        "authorization": {"authorization_version": "acl-2"},
                        "erasure": {"erasure_epoch": 1},
                        "version": {"source_version": "revision-2"},
                        "known_at": {"known_at": at(2)},
                        "valid_at": {"valid_at": at(2)},
                    }
                    host.fence = replace(host.fence, **changes[mutation])

            selected = pipeline(host, Auditor(callback=mutate), clock=lambda: clock[0])
            event = source(scope, "我住在杭州", idempotency="race")
            with pytest.raises(SourceAuditUnavailable):
                await extract(kernel, event, selected)
            assert await kernel.extraction_status(scope, "race") is None
            assert not (await engine.state(scope, valid_at=at(2), known_at=at(30)))[0]

    asyncio.run(run())


def test_denied_before_dispatch_does_not_call_either_model(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            host, auditor, generator = Host(), Auditor(), Generator([])
            host.denied = True
            with pytest.raises(SourceAuditUnavailable):
                await extract(
                    kernel,
                    source(scope, "我住在杭州"),
                    pipeline(host, auditor, generator=generator),
                )
            assert not auditor.requests and not generator.calls

    asyncio.run(run())


@pytest.mark.parametrize(
    "mutation",
    ["deny", "expiry", "version", "source", "time", "identity", "scope", "authority", "auditor"],
)
def test_exact_cached_reuse_rechecks_authority_time_versions_and_input(store, mutation):
    async def run():
        async with store() as (_, kernel, scope, clock):
            host, auditor = Host(), Auditor()
            selected = pipeline(host, auditor, clock=lambda: clock[0])
            event = source(scope, "我住在杭州", idempotency="cached")
            first = await extract(kernel, event, selected)
            again = await extract(kernel, event, selected)
            assert again.admission.duplicate and again.source_audit == first.source_audit
            assert len(auditor.requests) == 1
            authority = AUTHORITY
            if mutation == "deny":
                host.denied = True
            if mutation == "expiry":
                clock[0] = at(30)
            if mutation == "version":
                host.fence = replace(host.fence, source_version="revision-2")
            if mutation == "source":
                event = replace(event, content="我住在上海")
            if mutation == "time":
                event = replace(event, occurred_at=at(2))
            if mutation == "identity":
                event = replace(event, id="different-source")
            if mutation == "scope":
                event = replace(event, scope=replace(scope, user_id="mallory"))
            if mutation == "authority":
                authority = replace(AUTHORITY, source_id="different-auth")
            if mutation == "auditor":
                auditor.version = "auditor/2"
            if mutation == "scope":
                # New partitions cannot reuse another partition's receipt.
                host.denied = True
            with pytest.raises(ValueError):
                await kernel.extract_event(
                    event, pipeline=selected, authority=authority, policy=POLICY
                )
            assert len(auditor.requests) == 1

    asyncio.run(run())


def test_admission_guard_runs_under_lock_and_checks_expiry_after_callback(store):
    async def run():
        async with store() as (_, kernel, scope, clock):
            host = Host()

            def expire(uow):
                if uow is not None:
                    clock[0] = at(30)

            host.callback = expire
            event = source(scope, "我住在杭州", idempotency="publication-race")
            with pytest.raises(SourceAuditUnavailable, match="expired"):
                await extract(kernel, event, pipeline(host, Auditor(), clock=lambda: clock[0]))
            assert host.transaction_checks == 1
            assert await kernel.extraction_status(scope, "publication-race") is None

    asyncio.run(run())


def test_prepared_reuse_requires_matching_fence_and_never_accepts_legacy_audit(store):
    async def run():
        async with store() as (engine, kernel, scope, _):
            host, auditor = Host(), Auditor()
            selected = pipeline(host, auditor)
            event = source(scope, "我住在杭州", idempotency="prepared")
            prepared = await selected.prepare(event, authority=AUTHORITY, policy=POLICY)
            host.fence = replace(host.fence, authorization_version="revoked")
            with pytest.raises(SourceAuditUnavailable):
                await selected.publish_prepared(
                    engine.repository, event, prepared, authority=AUTHORITY, policy=POLICY
                )
            host.fence = replace(host.fence, authorization_version="acl-1")
            prepared["audit"].pop("source_audit")
            with pytest.raises(SourceAuditUnavailable, match="receipt_missing"):
                await selected.publish_prepared(
                    engine.repository, event, prepared, authority=AUTHORITY, policy=POLICY
                )
            assert await kernel.extraction_status(scope, "prepared") is None
            assert len(auditor.requests) == 1

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["erase", "revision"])
def test_retained_repository_checks_erase_and_revision_during_audit(store, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, receiver = await retained(engine, scope, clock)
            host = RetainedSourceAuditHost(engine.repository, Host())

            async def mutate():
                if mutation == "erase":
                    await kernel.forget(
                        ForgetRequest(scope, memory_ids=(event.id,), mode=ForgetMode.ERASE)
                    )
                else:
                    await receiver.revise(
                        source(scope, "我住在上海"),
                        base_event_id=event.id,
                        expected_revision=1,
                        request_id="revision",
                        producer_id="host",
                        configuration_sha256="a" * 64,
                    )

            selected = pipeline(host, Auditor(callback=mutate), clock=lambda: clock[0])
            with pytest.raises(SourceAuditUnavailable):
                await selected.prepare(event, authority=AUTHORITY, policy=POLICY)
            assert not (await engine.state(scope, valid_at=at(2), known_at=at(30)))[0]

    asyncio.run(run())


def test_retained_prepared_result_is_rechecked_inside_real_admission(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, _ = await retained(engine, scope, clock)
            host = Host()
            selected = pipeline(RetainedSourceAuditHost(engine.repository, host), Auditor())
            prepared = await selected.prepare(event, authority=AUTHORITY, policy=POLICY)
            async with engine.repository.unit_of_work() as uow:
                result = await selected.publish_prepared(
                    engine.repository,
                    event,
                    prepared,
                    authority=AUTHORITY,
                    policy=POLICY,
                    unit_of_work=uow,
                    retained=True,
                )
            assert result.decisions[0].action == "PENDING_VERIFICATION"
            await kernel.forget(ForgetRequest(scope, memory_ids=(event.id,), mode=ForgetMode.ERASE))
            with pytest.raises(SourceAuditUnavailable):
                async with engine.repository.unit_of_work() as uow:
                    await selected.publish_prepared(
                        engine.repository,
                        event,
                        prepared,
                        authority=AUTHORITY,
                        policy=POLICY,
                        unit_of_work=uow,
                        retained=True,
                    )

    asyncio.run(run())


@pytest.mark.parametrize("changed", ["question", "predicates", "span", "trigger", "none"])
def test_changed_target_plan_cannot_reuse_old_coverage_even_without_version_bump(store, changed):
    async def run():
        async with store() as (_, kernel, scope, _):
            host, auditor = Host(), Auditor()
            selected = pipeline(host, auditor)
            event = source(scope, "我住在杭州", idempotency="plan")
            await extract(kernel, event, selected)
            target = host.select_targets(event)[0]
            changes = {
                "question": {"question": "What is Alice's historical home?"},
                "predicates": {"predicates": ("response_language",)},
                "span": {"source_start": 1},
                "trigger": {"trigger": "high_value"},
            }
            host.targets = () if changed == "none" else (replace(target, **changes[changed]),)
            with pytest.raises(SourceAuditUnavailable, match="targets_changed"):
                await extract(kernel, event, selected)
            assert len(auditor.requests) == 1

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["expiry", "revoke", "configuration", "targets"])
def test_final_guard_rolls_back_after_storage_await(store, monkeypatch, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            host, auditor = Host(), Auditor()
            selected = pipeline(host, auditor, clock=lambda: clock[0])
            event = source(scope, "我住在杭州", idempotency="late-write")
            cls = type(engine.repository.unit_of_work())
            original = cls.save_admission_record

            async def invalidate(self, *args, **kwargs):
                result = await original(self, *args, **kwargs)
                if mutation == "expiry":
                    clock[0] = at(30)
                if mutation == "revoke":
                    host.denied = True
                if mutation == "configuration":
                    auditor.version = "auditor/2"
                if mutation == "targets":
                    host.targets = ()
                return result

            monkeypatch.setattr(cls, "save_admission_record", invalidate)
            with pytest.raises(SourceAuditUnavailable):
                await extract(kernel, event, selected)
            assert await kernel.extraction_status(scope, "late-write") is None
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.list_admission_records(scope)

    asyncio.run(run())


def test_erased_cached_source_cannot_be_reaudited_or_restored(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            auditor = Auditor()
            selected = pipeline(Host(), auditor)
            event = source(scope, "我住在杭州", idempotency="erase-cached")
            result = await extract(kernel, event, selected)
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(result.admission.event_id,), mode=ForgetMode.ERASE)
            )
            with pytest.raises(ValueError, match="deleted|forgotten"):
                await extract(kernel, event, selected)
            assert len(auditor.requests) == 1

    asyncio.run(run())


def test_expired_cached_delivery_after_storage_read_fails_closed(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            selected = pipeline(Host(), Auditor(), clock=lambda: clock[0])
            event = source(scope, "我住在杭州", idempotency="late-read")
            await extract(kernel, event, selected)
            cls = type(engine.repository.unit_of_work())
            original = cls.get_admission_record

            async def expire(self, *args, **kwargs):
                result = await original(self, *args, **kwargs)
                clock[0] = at(30)
                return result

            monkeypatch.setattr(cls, "get_admission_record", expire)
            with pytest.raises(SourceAuditUnavailable, match="expired"):
                await extract(kernel, event, selected)

    asyncio.run(run())


@pytest.mark.parametrize(
    "targets",
    [
        (SourceAuditTarget("outside", "Outside?", ("home_city",), 0, 100),),
        (SourceAuditTarget("unknown", "Unknown?", ("unregistered",), 0, 1),),
        tuple(SourceAuditTarget(str(i), "Home?", ("home_city",), 0, 1) for i in range(9)),
    ],
)
def test_invalid_or_unbounded_host_plan_never_dispatches_auditor(store, targets):
    async def run():
        async with store() as (_, kernel, scope, _):
            auditor = Auditor()
            with pytest.raises(ValueError, match="invalid source audit plan"):
                await extract(
                    kernel, source(scope, "我住在杭州"), pipeline(Host(targets=targets), auditor)
                )
            assert not auditor.requests

    asyncio.run(run())


async def durable_setup(
    engine,
    scope,
    clock,
    *,
    publication_policy=None,
    auditor=None,
    generator=None,
):
    from agent_memory.operations.extraction_worker import (
        DurableAtomHandler,
        ExtractionQueue,
        processing_configuration_sha256,
    )
    from agent_memory.operations.worker_runtime import BoundedWorker

    host = Host()
    selected = pipeline(
        RetainedSourceAuditHost(engine.repository, host),
        auditor or Auditor(),
        generator=generator,
        clock=lambda: clock[0],
    )
    configuration = processing_configuration_sha256(
        selected,
        POLICY,
        AUTHORITY,
        publication_policy=publication_policy,
    )
    queue = ExtractionQueue(
        engine.repository, scope, configuration, clock=lambda: clock[0], retry_seconds=0
    )
    handler = DurableAtomHandler(
        queue,
        selected,
        POLICY,
        AUTHORITY,
        local_only=True,
        publication_policy=publication_policy,
    )
    runner = BoundedWorker(queue, {"memory.extract": handler}, worker_id="audit-worker")
    event = source(scope, "我住在杭州", identity="durable-audit", idempotency="durable-audit")
    receiver = DurableReceiver(engine.repository, clock=lambda: clock[0])
    ticket = await receiver.issue_ticket(
        event,
        request_id="audit-request",
        producer_id="host",
        configuration_sha256=configuration,
    )
    await receiver.submit(
        event, ticket=ticket, producer_id="host", configuration_sha256=configuration
    )
    return event, receiver, host, selected, queue, runner


@pytest.mark.parametrize("allow_pending", [False, True])
def test_later_model_reprocessing_cannot_activate_unverified_audit_recovery(store, allow_pending):
    from agent_memory.operations.extraction_worker import (
        DurableAtomHandler,
        ExtractionQueue,
        processing_configuration_sha256,
    )
    from agent_memory.operations.reprocessing import ReprocessingService
    from agent_memory.operations.worker_runtime import BoundedWorker

    async def run():
        async with store() as (engine, _, scope, clock):
            event, receiver, _, _, queue, runner = await durable_setup(engine, scope, clock)
            assert await runner.run_once()
            first = await queue.status("audit-request")
            assert first["status"] == "completed"
            assert first["result"]["decisions"][0]["action"] == "PENDING_VERIFICATION"
            # Turning the optional audit off does not remove a persisted review requirement.
            selected = AtomExtractionPipeline(Generator([]), Reviewer())
            config = processing_configuration_sha256(selected, POLICY, AUTHORITY)
            later = ExtractionQueue(
                engine.repository, scope, config, clock=lambda: clock[0], retry_seconds=0
            )
            handler = DurableAtomHandler(later, selected, POLICY, AUTHORITY, local_only=True)
            worker = BoundedWorker(later, {"memory.extract": handler}, worker_id="later")
            service = ReprocessingService(receiver, producer_id="host", actor=event.actor)
            await service.submit(
                scope,
                source_event_id=event.id,
                request_id="reinterpret",
                mode="replace_interpretation",
                configuration_sha256=config,
                expected_head_generation=1,
                allow_pending=allow_pending,
            )
            assert await worker.run_once()
            second = await later.status("reinterpret")
            assert second["status"] == ("completed" if allow_pending else "needs_resolution")
            assert not (await engine.state(scope, valid_at=at(2), known_at=at(30)))[0]

    asyncio.run(run())


@pytest.mark.parametrize("batched", [False, True])
def test_durable_completion_writes_cannot_cross_audit_expiry(store, monkeypatch, batched):
    from agent_memory.operations.publication_batches import PublicationPolicy

    async def run():
        async with store() as (engine, _, scope, clock):
            _, _, _, _, queue, runner = await durable_setup(
                engine,
                scope,
                clock,
                publication_policy=PublicationPolicy(1) if batched else None,
            )
            cls = type(engine.repository.unit_of_work())
            original = cls.retention_update

            async def expire(self, scope_arg, request_id, row):
                result = await original(self, scope_arg, request_id, row)
                if row["status"] == "completed":
                    clock[0] = at(30)
                return result

            monkeypatch.setattr(cls, "retention_update", expire)
            await runner.run_once()  # Expiry can also invalidate the worker's lease.
            result = await queue.status("audit-request")
            assert result["status"] != "completed"
            assert not result["l1_decided"]
            async with engine.repository.unit_of_work() as uow:
                head = await uow.retention_head_get(scope, "interpretation", "durable-audit")
                assert (
                    head is None if not batched else head["payload"]["publication_closed"] is False
                )

    asyncio.run(run())


def test_explicit_host_verification_can_activate_recovered_candidate(store):
    async def run():
        async with store() as (engine, kernel, scope, _):
            result = await extract(kernel, source(scope, "我住在杭州"), pipeline(Host(), Auditor()))
            identity = result.admission.candidate_ids[0]
            row = await kernel.admission_status(scope, identity)
            verified = await kernel.resolve_atom(
                scope,
                identity,
                event=source(scope, "我住在杭州", day=2),
                authority=AUTHORITY,
                policy=POLICY,
                expected_version=row["version"],
                accept=True,
                source_quote="我住在杭州",
            )
            assert verified.decisions[0].action == "ACCEPT"
            assert len((await engine.state(scope, valid_at=at(3), known_at=at(30)))[0]) == 1

    asyncio.run(run())


def test_audit_evaluation_reports_extra_calls_without_counting_pending_as_recall(store):
    from agent_memory.evaluation.extraction import evaluate_extraction

    async def run():
        async with store() as (engine, _, scope, _):
            auditor = Auditor()
            report = await evaluate_extraction(
                engine.repository,
                [
                    {
                        "id": "omission",
                        "content": "我住在杭州",
                        "proposals": [],
                        "expected": [
                            {"subject_id": "alice", "predicate": "home_city", "value": "Hangzhou"},
                        ],
                    }
                ],
                pipeline=pipeline(Host(), auditor),
                scope=scope,
                authority=AUTHORITY,
                policy=POLICY,
            )
            assert report["source_audit_calls"] == 1
            assert report["source_audit_missing_candidate_findings"] == 1
            assert report["source_audit_unresolved_scopes"] == 0
            assert report["recall"] == 0 and report["false_negatives"] == 1
            assert report["token_usage"] is None and report["monetary_cost"] is None
            assert len(auditor.requests) == 1

    asyncio.run(run())


def test_candidate_context_does_not_send_forged_or_overlapping_source_quotes(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            event = source(scope, "private我住在杭州")
            host = Host(targets=(SourceAuditTarget("home", "Home?", ("home_city",), 7, 12),))
            auditor = Auditor(
                (SourceAuditObservation("home", "home_city", "unresolved", ("limited",)),)
            )
            generated = {
                **candidate(),
                "source_quote": event.content,
                "source_start": 0,
                "source_end": 12,
            }
            result = await extract(
                kernel, event, pipeline(host, auditor, generator=Generator([generated]))
            )
            assert auditor.requests[0].candidates == ()
            assert auditor.requests[0].omitted_candidate_count == 1
            assert result.source_audit["omitted_candidate_count"] == 1
            assert "private" not in repr(auditor.requests[0])

    asyncio.run(run())


def test_retained_source_received_after_known_at_is_not_audited(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            clock[0] = at(2)
            event, _ = await retained(engine, scope, clock)
            auditor = Auditor()
            selected = pipeline(
                RetainedSourceAuditHost(engine.repository, Host()), auditor, clock=lambda: clock[0]
            )
            with pytest.raises(SourceAuditUnavailable):
                await selected.prepare(event, authority=AUTHORITY, policy=POLICY)
            assert not auditor.requests

    asyncio.run(run())


def approved_audit(auditor, host, *, verify=None, clock=None):
    from agent_memory.consolidation.source_audit import SourceAuditApproval

    preview = SourceOmissionAudit(auditor, host, local_only=True)
    # Synthetic approval-verifier fixture only, not a real quality report.
    approval = SourceAuditApproval(
        preview.configuration_sha256, "e" * 64, "f" * 64, "disable-audit"
    )
    return SourceOmissionAudit(
        auditor,
        host,
        local_only=True,
        enabled=True,
        approval=approval,
        verify_approval=verify or (lambda value: value == approval),
        clock=clock or (lambda: at(1)),
    )


def test_source_audit_activation_is_disabled_without_explicit_quality_mode(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            host, auditor, generator = Host(), Auditor(), Generator([])
            disabled = SourceOmissionAudit(auditor, host, local_only=True)
            selected = AtomExtractionPipeline(generator, Reviewer(), source_audit=disabled)
            with pytest.raises(SourceAuditUnavailable, match="disabled"):
                await extract(kernel, source(scope, "我住在杭州"), selected)
            assert not auditor.requests and not generator.calls
            with pytest.raises(SourceAuditUnavailable, match="qualified_approval_required"):
                SourceOmissionAudit(auditor, host, local_only=True, enabled=True)
            with pytest.raises(SourceAuditUnavailable, match="contract_mode_has_approval"):
                SourceOmissionAudit(
                    auditor,
                    host,
                    local_only=True,
                    enabled=True,
                    contract_test_only=True,
                    verify_approval=lambda _: True,
                )

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["wrong_config", "unverified", "async_verifier"])
def test_unverified_or_mismatched_quality_approval_never_enables_audit(failure):
    from agent_memory.consolidation.source_audit import SourceAuditApproval

    host, auditor = Host(), Auditor()
    preview = SourceOmissionAudit(auditor, host, local_only=True)
    approval = SourceAuditApproval(
        "0" * 64 if failure == "wrong_config" else preview.configuration_sha256,
        "e" * 64,
        "f" * 64,
        "disable-audit",
    )

    async def asynchronous(_):
        return True

    with pytest.raises(SourceAuditUnavailable):
        SourceOmissionAudit(
            auditor,
            host,
            local_only=True,
            enabled=True,
            approval=approval,
            verify_approval=asynchronous if failure == "async_verifier" else lambda _: False,
        )
    assert not auditor.requests


@pytest.mark.parametrize("boundary", ["audit", "review", "storage", "reuse"])
def test_promotion_revocation_is_checked_after_awaits_and_on_cached_reuse(
    store, monkeypatch, boundary
):
    async def run():
        async with store() as (engine, kernel, scope, _):
            live = [True]

            async def revoke():
                live[0] = False

            auditor = Auditor(callback=revoke if boundary == "audit" else None)
            reviewer = Reviewer()
            if boundary == "review":
                original_review = reviewer.review_atoms

                async def review(event, candidates):
                    result = await original_review(event, candidates)
                    live[0] = False
                    return result

                reviewer.review_atoms = review
            audit = approved_audit(auditor, Host(), verify=lambda _: live[0])
            selected = AtomExtractionPipeline(Generator([]), reviewer, source_audit=audit)
            event = source(scope, "我住在杭州", idempotency="approval-race")
            if boundary == "storage":
                cls = type(engine.repository.unit_of_work())
                original = cls.save_admission_record

                async def save(self, *args, **kwargs):
                    result = await original(self, *args, **kwargs)
                    live[0] = False
                    return result

                monkeypatch.setattr(cls, "save_admission_record", save)
            if boundary == "reuse":
                first = await extract(kernel, event, selected)
                assert first.source_audit["config"]["activation_mode"] == "controlled-real"
                live[0] = False
            with pytest.raises(SourceAuditUnavailable, match="approval_revoked"):
                await extract(kernel, event, selected)
            if boundary != "reuse":
                assert await kernel.extraction_status(scope, "approval-race") is None
            assert len(auditor.requests) == 1

    asyncio.run(run())


def test_synchronous_approval_verifier_cannot_change_its_approved_configuration(store):
    async def run():
        async with store() as (_, kernel, scope, _):
            mutate = [False]
            auditor = Auditor()

            def verify(_):
                if mutate[0]:
                    auditor.version = "changed-during-verification"
                return True

            selected = AtomExtractionPipeline(
                Generator([]),
                Reviewer(),
                source_audit=approved_audit(
                    auditor,
                    Host(),
                    verify=verify,
                ),
            )
            mutate[0] = True
            with pytest.raises(SourceAuditUnavailable, match="activation_changed"):
                await extract(kernel, source(scope, "我住在杭州"), selected)
            assert not auditor.requests

    asyncio.run(run())


def test_direct_duplicate_publication_revalidates_committed_winners_fence(store):
    async def run():
        async with store() as (engine, kernel, scope, _):
            host = Host()
            selected = pipeline(host, Auditor())
            event = source(scope, "我住在杭州", idempotency="winner-fence")
            await extract(kernel, event, selected)
            host.fence = replace(host.fence, authorization_version="acl-2")
            prepared = await selected.prepare(event, authority=AUTHORITY, policy=POLICY)
            with pytest.raises(SourceAuditUnavailable, match="fence_changed"):
                await selected.publish_prepared(
                    engine.repository,
                    event,
                    prepared,
                    authority=AUTHORITY,
                    policy=POLICY,
                )

    asyncio.run(run())


@pytest.mark.parametrize("modality", ["planned", "negated"])
def test_audit_recovery_preserves_nonasserted_admission_rules(store, modality):
    async def run():
        async with store() as (_, kernel, scope, _):
            auditor = Auditor(
                (
                    SourceAuditObservation(
                        "home",
                        "home_city",
                        "missing_candidate",
                        ("nonasserted",),
                        ({**candidate(), "modality": modality},),
                    ),
                )
            )
            result = await extract(kernel, source(scope, "我住在杭州"), pipeline(Host(), auditor))
            assert result.decisions[0].action == "L0_ONLY"
            assert not result.admission.claim_ids

    asyncio.run(run())


async def reinterpret_audit_source(
    engine,
    scope,
    clock,
    event,
    receiver,
    proposals,
    *,
    generation=1,
    mode="replace_interpretation",
    reviewer=None,
    authority=AUTHORITY,
):
    from agent_memory.operations.extraction_worker import (
        DurableAtomHandler,
        ExtractionQueue,
        processing_configuration_sha256,
    )
    from agent_memory.operations.reprocessing import ReprocessingService
    from agent_memory.operations.worker_runtime import BoundedWorker

    selected = AtomExtractionPipeline(Generator(proposals), reviewer or Reviewer())
    config = processing_configuration_sha256(selected, POLICY, authority)
    queue = ExtractionQueue(
        engine.repository, scope, config, clock=lambda: clock[0], retry_seconds=0
    )
    handler = DurableAtomHandler(queue, selected, POLICY, authority, local_only=True)
    runner = BoundedWorker(queue, {"memory.extract": handler}, worker_id="reinterpret-audit")
    service = ReprocessingService(receiver, producer_id="host", actor=event.actor)
    request_id = f"audit-reinterpret-{generation}"
    await service.submit(
        scope,
        source_event_id=event.id,
        request_id=request_id,
        mode=mode,
        configuration_sha256=config,
        expected_head_generation=generation,
        allow_pending=True,
    )
    assert await runner.run_once()
    return await queue.status(request_id)


@pytest.mark.parametrize("mode", ["replace_interpretation", "additive"])
@pytest.mark.parametrize("origin", ["recovery", "conflict"])
@pytest.mark.parametrize(
    "changes",
    [
        {},
        {"source_quote": "杭州", "source_start": 3, "source_end": 5},
        {"valid_from": at(1).isoformat()},
        {"conditions": ["when traveling"]},
        {"value": "Shanghai"},
        {"kind": "preference"},
    ],
)
def test_regenerated_audited_slot_keeps_host_review_across_draft_rewrites(
    store,
    mode,
    origin,
    changes,
):
    async def run():
        async with store() as (engine, _, scope, clock):
            options = {}
            if origin == "conflict":
                options = {
                    "generator": Generator([candidate()]),
                    "auditor": Auditor(
                        (
                            SourceAuditObservation(
                                "home",
                                "home_city",
                                "conflict",
                                ("source_contradiction",),
                            ),
                        )
                    ),
                }
            event, receiver, _, _, queue, runner = await durable_setup(
                engine, scope, clock, **options
            )
            assert await runner.run_once()
            first = await queue.status("audit-request")
            assert first["result"]["decisions"][0]["action"] == "PENDING_VERIFICATION"
            second = await reinterpret_audit_source(
                engine,
                scope,
                clock,
                event,
                receiver,
                [{**candidate(), **changes}],
                mode=mode,
            )
            assert second["status"] == "completed"
            assert {d["action"] for d in second["result"]["decisions"]} == {"PENDING_VERIFICATION"}
            assert not (await engine.state(scope, valid_at=at(2), known_at=at(30)))[0]
            if not changes:
                assert second["result"]["candidate_ids"] == first["result"]["candidate_ids"]
            else:
                new_ids = set(second["result"]["candidate_ids"]) - set(
                    first["result"]["candidate_ids"]
                )
                for identity in new_ids:
                    row = await engine.repository.admission_record(scope, identity)
                    assert (
                        row["payload"]["source_audit_hold"]["identity_policy"]
                        == "same-source-subject-predicate-slot/1"
                    )

    asyncio.run(run())


@pytest.mark.parametrize("remove_review", ["unsupported", "supported"])
def test_historical_audit_hold_survives_model_withdrawal_and_nonasserted_regeneration(
    store, remove_review
):
    async def run():
        async with store() as (engine, _, scope, clock):
            event, receiver, _, _, queue, runner = await durable_setup(engine, scope, clock)
            assert await runner.run_once()
            first = await queue.status("audit-request")
            removed = await reinterpret_audit_source(
                engine,
                scope,
                clock,
                event,
                receiver,
                [] if remove_review == "unsupported" else [{**candidate(), "modality": "planned"}],
                reviewer=Reviewer(remove_review),
            )
            assert removed["status"] == "completed"
            restored = await reinterpret_audit_source(
                engine,
                scope,
                clock,
                event,
                receiver,
                [{**candidate(), "source_quote": "杭州", "source_start": 3, "source_end": 5}],
                generation=2,
            )
            assert restored["status"] == "completed"
            assert {d["action"] for d in restored["result"]["decisions"]} == {
                "PENDING_VERIFICATION"
            }
            assert not (await engine.state(scope, valid_at=at(2), known_at=at(30)))[0]
            if remove_review == "unsupported":
                old = await engine.repository.admission_record(
                    scope, first["result"]["candidate_ids"][0]
                )
                assert old["payload"]["action"] == "WITHDRAWN"

    asyncio.run(run())


def test_audit_hold_does_not_block_unrelated_subjects_predicates_or_new_sources(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event, receiver, _, _, _, runner = await durable_setup(engine, scope, clock)
            assert await runner.run_once()
            broader_authority = replace(AUTHORITY, subjects=("alice", "bob"))
            second = await reinterpret_audit_source(
                engine,
                scope,
                clock,
                event,
                receiver,
                [
                    candidate(),
                    {**candidate(), "subject_id": "bob"},
                    {**candidate(), "predicate": "response_style", "value": "concise"},
                ],
                authority=broader_authority,
            )
            rows = [
                await engine.repository.admission_record(scope, identity)
                for identity in second["result"]["candidate_ids"]
            ]
            actions = {
                (r["payload"]["draft"]["subject_id"], r["payload"]["draft"]["predicate"]): r[
                    "payload"
                ]["action"]
                for r in rows
            }
            assert actions == {
                ("alice", "home_city"): "PENDING_VERIFICATION",
                ("bob", "home_city"): "ACCEPT",
                ("alice", "response_style"): "ACCEPT",
            }
            independent = await extract(
                kernel,
                source(scope, "我住在杭州"),
                AtomExtractionPipeline(Generator([candidate()]), Reviewer()),
            )
            assert independent.decisions[0].action == "ACCEPT"

    asyncio.run(run())
