"""Compose durable backfill completion with trusted host switch approval."""

from __future__ import annotations

from typing import Protocol

from .ontology_backfill import OntologyBackfillCheckpoint
from .ontology_registry import OntologySwitchAuthorizer, OntologySwitchRequest
from .ontology_schema import ontology_schema_digest


class OntologyCheckpointReader(Protocol):
    async def load(self) -> OntologyBackfillCheckpoint | None: ...


class BackfillGatedOntologyAuthorizer:
    """Require one completed snapshot before delegating switch approval.

    The host binds a checkpoint reader to the target job and supplies the
    expected snapshot identity. Checkpoints are read before and after approval.
    Missing, mismatched, or unfinished progress denies the switch. Reader and
    host exceptions propagate to the registry, which must not commit a switch.

    Completion is not an index integrity proof: host_policy must authenticate
    approval and check target index availability, evidence freshness and quality.
    This adapter does not make a cross-database transaction or acquire a worker
    lease. Hosts must prevent index replacement between approval and use.
    Rollback targets need their own matching retained checkpoint and host policy.
    """

    def __init__(
        self,
        checkpoint_reader: OntologyCheckpointReader,
        host_policy: OntologySwitchAuthorizer,
        *,
        snapshot_id: str,
    ) -> None:
        if not isinstance(snapshot_id, str) or not snapshot_id.strip() or len(snapshot_id) > 4_096:
            raise ValueError("snapshot_id must contain 1 to 4096 characters")
        self._reader = checkpoint_reader
        self._host_policy = host_policy
        self._snapshot_id = snapshot_id

    async def authorize(self, request: OntologySwitchRequest) -> bool:
        if not isinstance(request, OntologySwitchRequest):
            raise TypeError("request must be an OntologySwitchRequest")
        if request.action not in {"activate", "rollback"}:
            return False
        if request.target_digest != ontology_schema_digest(request.target):
            return False
        before = await self._reader.load()
        if not self._matches(before, request):
            return False
        if await self._host_policy.authorize(request) is not True:
            return False
        after = await self._reader.load()
        return self._matches(after, request) and after == before

    def _matches(self, checkpoint, request) -> bool:
        return (
            isinstance(checkpoint, OntologyBackfillCheckpoint)
            and checkpoint.completed
            and checkpoint.scope == request.scope
            and checkpoint.snapshot_id == self._snapshot_id
            and checkpoint.schema_digest == request.target_digest
        )
