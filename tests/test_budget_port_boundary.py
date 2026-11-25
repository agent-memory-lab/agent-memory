"""The budget use case runs with only its port, including cross-feature rollback."""

import asyncio
from contextlib import asynccontextmanager

import pytest
import test_atom_admission as base
from test_model_budget_v7 import reserve

from agent_memory.domain import MemoryEvent
from agent_memory.operations.model_budget import BudgetAccount, ModelBudget

store = base.store


class BudgetOnlyTransaction:
    model_budget_contract = "model-budget/1"

    def __init__(self, native):
        self.native = native

    async def model_budget_lock(self):
        return await self.native.model_budget_lock()

    async def model_budget_get(self, kind, key):
        return await self.native.model_budget_get(kind, key)

    async def model_budget_put(self, kind, key, payload):
        return await self.native.model_budget_put(kind, key, payload)

    async def model_budget_records(self, kind):
        return await self.native.model_budget_records(kind)


class BudgetOnlyRepository:
    def __init__(self, native):
        self.native = native

    @asynccontextmanager
    async def unit_of_work(self):
        async with self.native.unit_of_work() as transaction:
            yield BudgetOnlyTransaction(transaction)


def test_budget_narrow_port_keeps_the_original_atomic_transaction(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(BudgetOnlyRepository(engine.repository))
            keys = await budget.configure((BudgetAccount("one", "run", "USD", "v1", 1000),))
            row = await reserve(budget, keys)
            event = MemoryEvent(scope, "test", "Rollback with budget dispatch.")
            with pytest.raises(RuntimeError, match="abort"):
                async with engine.repository.unit_of_work() as transaction:
                    await budget.intent(
                        BudgetOnlyTransaction(transaction), row["key"], row["request_sha256"]
                    )
                    await transaction.append_event(event)
                    raise RuntimeError("abort")
            assert (await budget.snapshot())[0]["state"] == "reserved"
            async with engine.repository.unit_of_work() as transaction:
                assert not await transaction.events_exist(scope, (event.id,))

    asyncio.run(run())
