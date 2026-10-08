"""Actual SQLite/PG money state contracts; no prices or real-model outcomes invented."""

import asyncio

import pytest
import test_atom_admission as base

from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
from agent_memory.retrieval.model_contracts import ModelError

store = base.store


async def reserve(budget, keys, *, attempt="one", maximum=100, evidence="host-bound/1"):
    return await budget.reserve(
        operation_id="operation",
        attempt_id=attempt,
        request_sha256="a" * 64,
        account_keys=keys,
        maximum_microunits=maximum,
        upper_bound_evidence=evidence,
    )


async def intent(budget, row):
    async with budget.repository.unit_of_work() as uow:
        await budget.intent(uow, row["key"], row["request_sha256"])


def test_ten_workers_compete_for_one_atomic_multilayer_allowance(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            keys = await budget.configure(
                (
                    BudgetAccount("global", "run", "USD", "1", 100),
                    BudgetAccount("tenant", "run", "USD", "1", 200),
                )
            )
            results = await asyncio.gather(
                *(reserve(budget, keys, attempt=str(i)) for i in range(10)), return_exceptions=True
            )
            assert sum(isinstance(r, dict) for r in results) == 1
            assert all(
                isinstance(r, dict)
                or isinstance(r, ModelError)
                and r.code == "model_budget_exhausted"
                for r in results
            )
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.model_budget_records("account")
                assert [r["reserved"] for r in rows] == [100, 100]
            assert (
                len(await budget.snapshot()) == 1
            )  # Same call is not billed twice for two accounts.

    asyncio.run(run())


def test_any_failed_account_rolls_back_all_accounts(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            keys = await budget.configure(
                (
                    BudgetAccount("global", "run", "USD", "1", 1000),
                    BudgetAccount("tenant", "run", "USD", "1", 1),
                )
            )
            with pytest.raises(ModelError, match="budget_exhausted"):
                await reserve(budget, keys)
            async with engine.repository.unit_of_work() as uow:
                assert all(r["reserved"] == 0 for r in await uow.model_budget_records("account"))
            assert await budget.snapshot() == ()

    asyncio.run(run())


def test_intent_unknown_receipt_survives_restart_and_cannot_release(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            keys = await budget.configure((BudgetAccount("global", "run", "USD", "1", 100),))
            row = await reserve(budget, keys)
            await intent(budget, row)
            restarted = ModelBudget(engine.repository)
            with pytest.raises(ModelError, match="cannot_release"):
                await restarted.release(row["key"])
            with pytest.raises(ModelError, match="budget_exhausted"):
                await reserve(restarted, keys, attempt="retry")
            await restarted.pending(row["key"])
            assert (await restarted.snapshot())[0]["state"] == "reconciliation_pending"
            await restarted.settle(
                row["key"], actual_microunits=60, receipt_id="bill1", provider_request_id="p1"
            )
            await restarted.settle(
                row["key"], actual_microunits=60, receipt_id="bill1", provider_request_id="p1"
            )
            with pytest.raises(ModelError, match="settlement_conflict"):
                await restarted.settle(
                    row["key"], actual_microunits=0, receipt_id="bill1", provider_request_id="p1"
                )
            await reserve(restarted, keys, attempt="new", maximum=40)
            async with engine.repository.unit_of_work() as uow:
                account = await uow.model_budget_get("account", keys[0])
                assert account["settled"] == 60 and account["reserved"] == 40

    asyncio.run(run())


def test_unknown_local_compute_is_unbounded_debt_and_not_zero(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            hard = await budget.configure((BudgetAccount("hard", "run", "USD", "1", 100),))
            with pytest.raises(ModelError, match="strict_model_cost_bound_unavailable"):
                await reserve(budget, hard, maximum=None, evidence=None)
            with pytest.raises(ModelError, match="strict_model_cost_bound_unavailable"):
                await reserve(budget, hard, evidence=None)
            soft = await budget.configure((BudgetAccount("local", "run", "USD", "1", None),))
            row = await reserve(budget, soft, maximum=None, evidence=None)
            await intent(budget, row)
            await budget.pending(row["key"])
            async with engine.repository.unit_of_work() as uow:
                account = await uow.model_budget_get("account", soft[0])
                assert account["unknown_reservations"] == 1 and account["settled"] == 0
            assert (await budget.snapshot())[0]["actual_microunits"] is None
            with pytest.raises(ModelError, match="cannot_reserve_zero"):
                await reserve(budget, soft, attempt="free", maximum=0)

    asyncio.run(run())


def test_only_never_dispatched_reservations_can_release(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            keys = await budget.configure((BudgetAccount("global", "run", "USD", "1", 100),))
            row = await reserve(budget, keys)
            assert await reserve(budget, keys) == row
            with pytest.raises(ModelError, match="reservation_conflict"):
                await reserve(budget, keys, maximum=99)
            await budget.release(row["key"])
            await budget.release(row["key"])
            with pytest.raises(ModelError, match="dispatch_already_claimed"):
                await intent(budget, row)
            await reserve(budget, keys, attempt="next")

    asyncio.run(run())


def test_duplicate_provider_bill_cannot_charge_second_attempt(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            keys = await budget.configure((BudgetAccount("global", "run", "USD", "1", 500),))
            first = await reserve(budget, keys)
            second = await reserve(budget, keys, attempt="two")
            await intent(budget, first)
            await intent(budget, second)
            await budget.settle(
                first["key"], actual_microunits=30, receipt_id="bill1", provider_request_id="same"
            )
            with pytest.raises(ModelError, match="provider_request_reused"):
                await budget.settle(
                    second["key"],
                    actual_microunits=30,
                    receipt_id="bill2",
                    provider_request_id="same",
                )
            assert [r["state"] for r in await budget.snapshot()].count("settled") == 1

    asyncio.run(run())
