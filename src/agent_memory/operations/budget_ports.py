"""The budget use case needs four ledger operations, not the entire memory UoW.

Native UoWs satisfy this port on the same connection and transaction used by
admission/delivery. Callers may pass that existing UoW for atomic operations.
"""

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol


class BudgetTransaction(Protocol):
    @property
    def model_budget_contract(self) -> str: ...

    async def model_budget_lock(self) -> None: ...

    async def model_budget_get(self, kind: str, key: str) -> dict[str, Any] | None: ...

    async def model_budget_put(self, kind: str, key: str, payload: Mapping[str, Any]) -> None: ...

    async def model_budget_records(self, kind: str) -> Sequence[dict[str, Any]]: ...


class BudgetRepository(Protocol):
    def unit_of_work(self) -> AbstractAsyncContextManager[BudgetTransaction]: ...


@dataclass(frozen=True, slots=True)
class _BudgetView:
    """Expose only budget operations while the native UoW owns commit/rollback."""

    _transaction: BudgetTransaction

    @property
    def model_budget_contract(self):
        return getattr(self._transaction, "model_budget_contract", None)

    async def model_budget_lock(self):
        return await self._transaction.model_budget_lock()

    async def model_budget_get(self, kind, key):
        return await self._transaction.model_budget_get(kind, key)

    async def model_budget_put(self, kind, key, payload):
        return await self._transaction.model_budget_put(kind, key, payload)

    async def model_budget_records(self, kind):
        return await self._transaction.model_budget_records(kind)


@asynccontextmanager
async def budget_transaction(repository: BudgetRepository):
    async with repository.unit_of_work() as transaction:
        yield _BudgetView(transaction)
