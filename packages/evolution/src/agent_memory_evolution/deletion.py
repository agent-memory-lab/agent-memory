"""Evolution participant for UnifiedMemory's existing durable deletion journal."""

from agent_memory.domain import ForgetMode, ForgetRequest, MemoryScope

from .engine import EvolutionWithdrawalError


class EvolutionDeletionTarget:
    def __init__(self, engine, scope: MemoryScope):
        if not isinstance(scope, MemoryScope):
            raise ValueError("an exact deletion target scope is required")
        self.engine, self.scope = engine, scope

    async def forget_sources(self, request: ForgetRequest):
        if (
            not isinstance(request, ForgetRequest)
            or request.scope != self.scope
            or request.mode not in {ForgetMode.ARCHIVE, ForgetMode.ERASE}
        ):
            raise ValueError("evolution deletion request must match the registered exact scope")
        records = await self.engine.invalidate_sources(
            request.memory_ids,
            scope=self.scope,
            all_in_scope=request.all_in_scope,
            erase=request.mode == ForgetMode.ERASE,
            actor="unified-memory",
            reason="source " + request.mode.value,
        )
        pending = await self.engine.pending_withdrawals(self.scope)
        if pending:
            # In-flight activation cannot acknowledge deletion. The existing
            # UnifiedMemory journal remains pending until compensation completes.
            raise EvolutionWithdrawalError(tuple(candidate.id for candidate in pending))
        return records
