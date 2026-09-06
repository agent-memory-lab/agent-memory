from __future__ import annotations

from dataclasses import replace

from agent_memory.domain import ArtifactStatus, ForgetMode, ForgetRequest
from agent_memory.ports import MemoryProvider

from .domain import EvolutionCandidate


class MemoryProviderProcedureDeployment:
    """Activate through the provider and rollback through its archive contract."""

    def __init__(self, provider: MemoryProvider) -> None:
        self._provider = provider

    async def activate(self, candidate: EvolutionCandidate) -> None:
        procedure = replace(candidate.procedure, status=ArtifactStatus.ACTIVE)
        await self._provider.publish_procedure(procedure)

    async def deactivate(self, candidate: EvolutionCandidate) -> None:
        await self._provider.forget(
            ForgetRequest(
                scope=candidate.scope,
                memory_ids=(candidate.procedure.id,),
                mode=ForgetMode.ARCHIVE,
            )
        )


class NullProcedureDeployment:
    """Safe control-plane-only target for dry runs and evaluation environments."""

    async def activate(self, candidate: EvolutionCandidate) -> None:
        return None

    async def deactivate(self, candidate: EvolutionCandidate) -> None:
        return None
