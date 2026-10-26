"""Native feedback storage, measured refresh CAS, erasure and unknown-cost boundaries."""

import asyncio
import json
from dataclasses import asdict, replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_observations import setup
from test_refresh_demand import finish, scheduler

from agent_memory.consolidation.business_policy import BusinessDecision
from agent_memory.derived.model import DerivedError, digest
from agent_memory.domain import (
    DecisionRecord,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryScope,
    PolicyDecisionDetails,
)
from agent_memory.operations.decision_records import (
    DecisionJournal,
    PolicyDecisionRecord,
    boolean_decision_record,
    business_decision_record,
)
from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
from agent_memory.operations.refresh_observations import (
    WORK_PHASES,
    PairedWorkObservation,
    StageWork,
    WorkloadObservation,
)
from agent_memory.operations.refresh_policy import RefreshPolicy

store = base.store


def record(scope, **changes):
    return replace(
        PolicyDecisionRecord(
            "decision-one",
            scope,
            "retrieval",
            "research_retrieve",
            ("missing_subject",),
            "research/1",
            (digest("query"),),
            created_at=base.at(1),
        ),
        **changes,
    )


def test_policy_details_immutable_bounded_json_and_legacy_decision():
    details = PolicyDecisionDetails(
        "refresh", ["cost_unknown"], [digest("input")], measurements=[["work", None]]
    )
    assert details.reasons == ("cost_unknown",) and details.measurements == (("work", None),)
    with pytest.raises(AttributeError):
        details.stage = "storage"
    parsed = DecisionRecord(MemoryScope("tenant"), "select", (), policy_details=asdict(details))
    assert parsed.policy_details == details
    assert DecisionRecord(MemoryScope("tenant"), "legacy", ()).policy_details is None
    for changes in (
        {"reasons": ("This is raw source text",)},
        {"evidence_references": ("raw document",)},
        {"input_versions": (digest("same"), digest("same"))},
        {"measurements": (("cost", float("nan")),)},
        {"model_version": "prompt text"},
        {"schema": "unknown"},
    ):
        with pytest.raises((TypeError, ValueError)):
            replace(details, **changes)
    from agent_memory.evaluation.question_cost import CostPhase
    from agent_memory.kernel import MemoryKernel

    assert WORK_PHASES == {phase.value for phase in CostPhase}
    assert MemoryKernel._feedback_comparable({"action": "legacy"}) == (
        MemoryKernel._feedback_comparable({"action": "legacy", "policy_details": None})
    )


def test_business_and_boolean_adapters_preserve_decision_meaning():
    scope = MemoryScope("tenant")
    decision = BusinessDecision(
        "PENDING_VERIFICATION", ("domain_required",), digest("policy"), "session", "domain"
    )
    result = business_decision_record(
        decision, id="policy", scope=scope, input_versions=(digest("input"),)
    )
    assert result.action == "PENDING_VERIFICATION" and result.outcome == "proposed"
    assert result.reasons == ("domain_required", "storage.session", "verification.domain")
    denied = boolean_decision_record(
        False,
        id="legacy",
        scope=scope,
        stage="verification",
        policy_version="1",
        input_versions=(),
        passed_reason="allowed",
        rejected_reason="denied",
    )
    assert denied.action == "deny" and denied.reasons == ("denied",)


def test_journal_reuses_feedback_roundtrip_idempotency_scope_and_erasure(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            source = MemoryEvent(scope, "message", "private evidence")
            await kernel.ingest_event(source)
            journal = DecisionJournal(engine.repository, scope)
            first = record(scope, memory_ids=(source.id,))
            assert await journal.append(first) == first
            assert await journal.append(replace(first, created_at=base.at(2))) == first
            assert await DecisionJournal(engine.repository, scope).get(first.id) == first
            page = await kernel.feedback_history(scope, "decision")
            assert [item.record_id for item in page.items] == [first.id]
            async with engine.repository.unit_of_work() as uow:
                stored = await uow.find_feedback_record(first.id, "decision")
                assert stored["payload"]["policy_details"]["stage"] == "retrieval"
                assert "private evidence" not in json.dumps(stored["payload"])
                # The existing domain constructor can consume both backend JSON payloads.
                payload = dict(stored["payload"])
                payload["scope"] = MemoryScope(**payload["scope"])
                reconstructed = DecisionRecord(**payload)
                assert reconstructed.policy_details == first.details
            with pytest.raises(DerivedError, match="idempotency_conflict"):
                await journal.append(replace(first, action="different"))
            with pytest.raises(DerivedError, match="scope_mismatch"):
                await DecisionJournal(engine.repository, replace(scope, session_id="foreign")).get(
                    first.id
                )
            await kernel.forget(ForgetRequest(scope, (source.id,), mode=ForgetMode.ERASE))
            with pytest.raises(DerivedError, match="unavailable"):
                await journal.get(first.id)
            with pytest.raises(DerivedError, match="unavailable"):
                await journal.append(first)
            with pytest.raises(DerivedError, match="sources_unavailable"):
                await journal.append(replace(first, id="new-id-after-erasure"))

    asyncio.run(run())


def test_native_claim_reference_and_foreign_or_unknown_source_rejection(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event = base.source(scope)
            admission = await engine.admit(
                event, [base.atom()], authority=base.SELF, policy=base.POLICY
            )
            journal = DecisionJournal(engine.repository, scope)
            claim = record(scope, memory_ids=admission.claim_ids)
            assert await journal.append(claim) == claim
            foreign = replace(scope, session_id="foreign")
            with pytest.raises(DerivedError, match="sources_unavailable"):
                await DecisionJournal(engine.repository, foreign).append(
                    record(foreign, id="foreign", memory_ids=(event.id,))
                )
            with pytest.raises(DerivedError, match="sources_unavailable"):
                await journal.append(
                    record(scope, id="unknown", memory_ids=("external-or-unknown",))
                )
            await kernel.forget(ForgetRequest(scope, (event.id,), mode=ForgetMode.ERASE))
            with pytest.raises(DerivedError, match="sources_unavailable"):
                await journal.append(replace(claim, id="new-claim-decision"))

    asyncio.run(run())


def test_journal_write_rolls_back_with_owning_transaction(store):
    async def run():
        async with store() as (engine, _, scope, _):
            journal = DecisionJournal(engine.repository, scope)
            with pytest.raises(RuntimeError, match="rollback"):
                async with engine.repository.unit_of_work() as uow:
                    await journal.append_in(uow, record(scope))
                    raise RuntimeError("rollback")
            assert await journal.get("decision-one") is None

    asyncio.run(run())


async def configured_observation(engine, kernel, scope, clock):
    service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
    queue = scheduler(service, clock)
    policy = RefreshPolicy(
        mode="on_demand",
        revision="one",
        hotness_window_seconds=10,
        min_residence_seconds=10,
        minimum_samples=3,
        promote_reuses=3,
        demote_reuses=1,
    )
    await queue.configure("language", policy)
    await queue.request("language", dedupe_key="build", actor="alice")
    await finish(queue, await queue.claim("worker", lease_seconds=60))
    for _ in range(3):
        assert (await service.read("language", actor="alice"))["state"] == "empty"
    context = await queue.observation_context("language")
    start = clock[0]
    candidate_policy = replace(policy, mode="on_change", revision="two")
    common = dict(
        workload_sha256=digest("same workload"),
        definition_sha256=context["definition_sha256"],
        environment_sha256=digest("same environment"),
        window_start=start,
        window_end=start + timedelta(seconds=10),
        unit="cpu_ns",
        quality_protocol_sha256=digest("held out quality protocol"),
        quality_passed=True,
        p95_latency_ns=50,
    )
    baseline = WorkloadObservation(
        **common,
        policy_mode=policy.mode,
        policy_sha256=digest(policy.payload()),
        stages=tuple(
            StageWork(phase, 100 if phase == "foreground" else 0, model_calls=0)
            for phase in sorted(WORK_PHASES)
        ),
        receipt_sha256=digest("baseline measurement"),
        quality_receipt_sha256=digest("baseline quality"),
    )
    candidate = WorkloadObservation(
        **common,
        policy_mode=candidate_policy.mode,
        policy_sha256=digest(candidate_policy.payload()),
        stages=tuple(
            StageWork(phase, 20 if phase in {"foreground", "background"} else 0, model_calls=0)
            for phase in sorted(WORK_PHASES)
        ),
        receipt_sha256=digest("candidate measurement"),
        quality_receipt_sha256=digest("candidate quality"),
    )
    observation = PairedWorkObservation(scope, "language", baseline, candidate, 100)
    clock[0] = observation.baseline.window_end
    return service, queue, observation


async def observe(queue, pair, **kwargs):
    return await queue.observe_temperature(
        "language",
        pair,
        expected_revision="one",
        next_revision="two",
        budget_available=True,
        **kwargs,
    )


def test_measured_recommendation_explicit_apply_cas_and_replay(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, queue, pair = await configured_observation(engine, kernel, scope, clock)
            assert pair.net_work_saved == 60
            proposed = await observe(queue, pair)
            assert proposed.action == "on_change" and proposed.outcome == "proposed"
            assert (await queue.observation_context("language"))["policy"]["revision"] == "one"
            applied = await observe(queue, pair, apply=True)
            assert applied.action == "on_change" and applied.outcome == "applied"
            context = await queue.observation_context("language")
            assert context["policy"]["revision"] == "two"
            assert context["policy"]["mode"] == "on_change"
            assert await observe(queue, pair, apply=True) == applied
            assert dict(applied.measurements)["candidate.background"] == 20
            assert dict(applied.measurements)["baseline.retry"] == 0
            assert await DecisionJournal(engine.repository, scope).get(applied.id) == applied
            with pytest.raises(DerivedError, match="revision_conflict"):
                await observe(queue, replace(pair, latency_limit_ns=101), apply=True)

    asyncio.run(run())


@pytest.mark.parametrize(
    "problem",
    ["missing_phase", "unknown", "pending", "quality", "latency", "no_saving", "call_census"],
)
def test_incomplete_or_regressing_observation_cannot_promote(store, problem):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, queue, pair = await configured_observation(engine, kernel, scope, clock)
            candidate = pair.candidate
            if problem == "missing_phase":
                candidate = replace(
                    candidate, stages=tuple(s for s in candidate.stages if s.phase != "failure")
                )
            elif problem == "unknown":
                candidate = replace(
                    candidate,
                    stages=tuple(
                        replace(s, work_units=None) if s.phase == "retry" else s
                        for s in candidate.stages
                    ),
                )
            elif problem == "pending":
                candidate = replace(candidate, pending_operations=1)
            elif problem == "quality":
                candidate = replace(candidate, quality_passed=False)
            elif problem == "latency":
                candidate = replace(candidate, p95_latency_ns=101)
            elif problem == "call_census":
                candidate = replace(
                    candidate,
                    stages=tuple(
                        replace(s, model_calls=None) if s.phase == "background" else s
                        for s in candidate.stages
                    ),
                )
            else:
                candidate = replace(
                    candidate,
                    stages=tuple(
                        replace(s, work_units=200) if s.phase == "background" else s
                        for s in candidate.stages
                    ),
                )
            result = await observe(queue, replace(pair, candidate=candidate), apply=True)
            assert result.action == "on_demand" and result.outcome == "skipped"
            assert (await queue.observation_context("language"))["policy"]["revision"] == "one"
            if problem in {"missing_phase", "unknown", "pending"}:
                assert dict(result.measurements)["net_work_saved"] is None
            if problem == "call_census":
                assert "model_call_census_unknown" in result.reasons
                assert dict(result.measurements)["candidate.model_microunits"] is None

    asyncio.run(run())


def test_observation_rejects_scope_inputs_window_and_revision_mismatch(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, queue, pair = await configured_observation(engine, kernel, scope, clock)
            with pytest.raises(DerivedError, match="scope_mismatch"):
                await observe(queue, replace(pair, scope=replace(scope, tenant_id="foreign")))
            clock[0] -= timedelta(seconds=1)
            with pytest.raises(DerivedError, match="window_incomplete"):
                await observe(queue, pair)
            clock[0] += timedelta(seconds=1)
            changed = replace(
                pair,
                baseline=replace(pair.baseline, definition_sha256=digest("other")),
                candidate=replace(pair.candidate, definition_sha256=digest("other")),
            )
            with pytest.raises(DerivedError, match="inputs_changed"):
                await observe(queue, changed)
            with pytest.raises(ValueError, match="freeze workload"):
                replace(
                    pair, candidate=replace(pair.candidate, workload_sha256=digest("different"))
                )
            clock[0] += timedelta(seconds=11)
            with pytest.raises(DerivedError, match="window_stale"):
                await observe(queue, pair)

    asyncio.run(run())


@pytest.mark.parametrize("settled_cost", [0, 10])
def test_unsettled_real_model_receipt_blocks_policy_promotion(store, settled_cost):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, queue, pair = await configured_observation(engine, kernel, scope, clock)
            ledger = ModelBudget(engine.repository)
            accounts = await ledger.configure(
                (BudgetAccount("policy", "window", "CNY", "1", None),)
            )
            call = await ledger.reserve(
                operation_id="measure",
                attempt_id="1",
                request_sha256=digest("request"),
                account_keys=accounts,
                maximum_microunits=None,
                phase="background",
            )
            candidate = replace(
                pair.candidate,
                stages=tuple(
                    replace(s, model_cost_keys=(call["key"],), model_calls=1)
                    if s.phase == "background"
                    else s
                    for s in pair.candidate.stages
                ),
            )
            result = await observe(queue, replace(pair, candidate=candidate), apply=True)
            assert "model_cost_unsettled" in result.reasons
            assert result.outcome == "skipped"
            assert (await queue.observation_context("language"))["policy"]["revision"] == "one"
            async with engine.repository.unit_of_work() as uow:
                await ledger.intent(uow, call["key"], digest("request"))
            await ledger.settle(call["key"], actual_microunits=settled_cost, receipt_id="measured")
            settled = await observe(queue, replace(pair, candidate=candidate), apply=True)
            assert settled.id != result.id  # New settlement state is a new decision input.
            assert "currency.CNY" in settled.reasons
            assert dict(settled.measurements)["candidate.model_microunits"] == settled_cost
            assert settled.outcome == ("applied" if settled_cost == 0 else "skipped")
            if settled_cost:
                assert "model_cost_regression" in settled.reasons

    asyncio.run(run())


@pytest.mark.parametrize("shared_attempt", [True, False])
def test_paired_policy_measurements_require_independent_model_attempts(store, shared_attempt):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, queue, pair = await configured_observation(engine, kernel, scope, clock)
            ledger = ModelBudget(engine.repository)
            accounts = await ledger.configure(
                (BudgetAccount("paired-policy", "window", "CNY", "1", None),)
            )

            async def measured_attempt(name):
                call = await ledger.reserve(
                    operation_id="paired-measurement",
                    attempt_id=name,
                    request_sha256=digest(name),
                    account_keys=accounts,
                    maximum_microunits=None,
                    phase="foreground",
                )
                async with engine.repository.unit_of_work() as uow:
                    await ledger.intent(uow, call["key"], digest(name))
                await ledger.settle(call["key"], actual_microunits=100, receipt_id=name)
                return call["key"]

            baseline_key = await measured_attempt("baseline")
            candidate_key = baseline_key if shared_attempt else await measured_attempt("candidate")

            def with_attempt(arm, key):
                return replace(
                    arm,
                    stages=tuple(
                        replace(stage, model_cost_keys=(key,), model_calls=1)
                        if stage.phase == "foreground" else stage
                        for stage in arm.stages
                    ),
                )

            async def apply_pair():
                measured = replace(
                    pair,
                    baseline=with_attempt(pair.baseline, baseline_key),
                    candidate=with_attempt(pair.candidate, candidate_key),
                )
                return await observe(queue, measured, apply=True)

            before = await ledger.snapshot()
            if shared_attempt:
                with pytest.raises(ValueError, match="across paired arms"):
                    await apply_pair()
                assert (await queue.observation_context("language"))["policy"]["revision"] == "one"
                assert not (await kernel.feedback_history(scope, "decision")).items
            else:
                result = await apply_pair()
                assert result.outcome == "applied"
                assert set(result.cost_references) == {baseline_key, candidate_key}
                assert dict(result.measurements)["baseline.model_microunits"] == 100
                assert dict(result.measurements)["candidate.model_microunits"] == 100
            assert await ledger.snapshot() == before

    asyncio.run(run())


def test_policy_update_and_decision_receipt_rollback_together(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, queue, pair = await configured_observation(engine, kernel, scope, clock)

            async def failed_admission(*args):
                raise RuntimeError("queue failure")

            monkeypatch.setattr(queue, "_admit", failed_admission)
            with pytest.raises(RuntimeError, match="queue failure"):
                await observe(queue, pair, apply=True)
            assert (await queue.observation_context("language"))["policy"]["revision"] == "one"
            assert not (await kernel.feedback_history(scope, "decision")).items

    asyncio.run(run())
