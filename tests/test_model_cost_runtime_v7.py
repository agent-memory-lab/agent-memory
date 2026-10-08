"""Recorded synthetic workload, full phases and B0 dual-denominator accounting."""

import asyncio
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_governed_models_v7 import setup
from test_question_cost_v7 import gate, request
from test_question_cost_v7 import run as fixture_run

from agent_memory.evaluation.model_experiment import ModelExperimentAccounting
from agent_memory.evaluation.question_cost import (
    AnswerOutcome,
    CostPhase,
    ModelEvidence,
    summarize_costs,
)
from agent_memory.retrieval.model_contracts import ModelError

store = base.store


def test_actual_runtime_projection_counts_calls_once_and_never_prices_local_unknown_zero(store):
    async def run():
        accounting = ModelExperimentAccounting("USD")
        with accounting.measure(CostPhase.COLD_START, evidence="open test database"):
            context = store()
            engine, kernel, scope, clock = await context.__aenter__()
        try:
            with accounting.measure(
                CostPhase.REGISTRATION, evidence="configure sources, grants, accounts"
            ):
                authority, sealed, ledger, port, answers, _ = await setup(
                    engine, kernel, scope, clock
                )
            with accounting.measure(
                CostPhase.PREWARM, evidence="perform one explicitly counted prewarm"
            ):
                answers.cost_phase = "prewarm"
                warm = await answers.answer(sealed)
            with accounting.measure(CostPhase.WRITE, evidence="advance test cache retention clock"):
                clock[0] += timedelta(seconds=301)
            with accounting.measure(
                CostPhase.DEPENDENCY, evidence="revalidate complete source dependency set"
            ):
                async with engine.repository.unit_of_work() as uow:
                    await authority.validate(uow, sealed)
            with accounting.measure(
                CostPhase.BACKGROUND, evidence="inspect absent background model work"
            ):
                assert len(await ledger.snapshot()) == 1
            with pytest.raises(ModelError, match="synthetic_failure"):
                with accounting.measure(
                    CostPhase.FAILURE, evidence="actual stub failure after dispatch"
                ):

                    async def fail():
                        raise ModelError("synthetic_failure")

                    port.before_return = fail
                    answers.cost_phase = "failure"
                    await answers.answer(sealed)
            with accounting.measure(CostPhase.RETRY, evidence="new reservation for explicit retry"):
                port.before_return = None
                answers.cost_phase = "retry"
                retried = await answers.answer(sealed)
            with accounting.measure(
                CostPhase.FOREGROUND, evidence="two guarded exact-cache requests"
            ):
                first = await answers.answer(sealed)
                second = await answers.answer(sealed)
                assert first.cache_hit and second.cache_hit
                assert first.call_id == second.call_id == retried.call_id != warm.call_id
            with accounting.measure(
                CostPhase.DRAIN, evidence="inspect finite remaining invoice debt"
            ):
                calls = await ledger.snapshot()
                assert len(calls) == 3
                assert all(row["actual_microunits"] is None for row in calls)
            snapshot = accounting.snapshot(calls)
            assert set(snapshot.accounted_phases) == set(CostPhase)
            assert len([entry for entry in snapshot.entries if entry.model_call]) == 3
            assert {entry.phase for entry in snapshot.entries if entry.model_call} == {
                CostPhase.PREWARM,
                CostPhase.FAILURE,
                CostPhase.RETRY,
            }
            candidate = fixture_run(
                costs=snapshot,
                dataset_kind="synthetic",
                requests=(
                    request("failed", outcome=AnswerOutcome.SYSTEM_ERROR),
                    request("retry"),
                    request("hit1"),
                    request("hit2"),
                ),
                generation_model=ModelEvidence(sealed.configuration.fingerprint, "stub"),
            )
            summary = summarize_costs(candidate)
            assert summary.all_requests == 4 and summary.effective_answers == 3
            assert summary.total_actual_microunits is None
            assert summary.cost_per_request_microunits is None
            assert summary.cost_per_effective_answer_microunits is None
            assert summary.unaccounted_phases == ()
            assert summary.model_calls == 3 and summary.unresolved_cost_entries == 13
            assert not gate(candidate).ready
            assert all(row["pricing"] == "unknown" for row in accounting.observations)
        finally:
            await context.__aexit__(None, None, None)

    asyncio.run(run())


def test_partial_phase_inventory_stays_unaccounted_and_overlap_is_rejected():
    accounting = ModelExperimentAccounting("USD")
    with accounting.measure("foreground", evidence="actual cache guard work"):
        with pytest.raises(ValueError, match="overlapping"):
            with accounting.measure("foreground", evidence="invalid nested measurement"):
                pass
    snapshot = accounting.snapshot(())
    assert snapshot.accounted_phases == (CostPhase.FOREGROUND,)
    assert snapshot.entries[0].actual_microunits is None
    with pytest.raises(ValueError, match="phase evidence"):
        with accounting.measure("drain", evidence=""):
            pass


def test_model_invoice_alone_cannot_mark_unobserved_resource_phase_accounted(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            accounting = ModelExperimentAccounting("USD")
            with pytest.raises(ValueError, match="no complete resource observation"):
                accounting.snapshot(await ledger.snapshot())

    asyncio.run(run())
