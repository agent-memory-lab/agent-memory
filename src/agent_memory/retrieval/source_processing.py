"""Shared source read/processing controls; no generation or embedding payloads.

Only trusted host authorities assemble inputs through this internal seam. A
coordinate validator must check the whole current proof using the caller UoW;
an asserted epoch or ACL in user metadata never grants access.
"""

from contextlib import nullcontext
from datetime import datetime

from ..derived.model import ProcessingGrant
from ..derived.service import open_derived
from .model_contracts import ModelError, digest


class SourceProcessingAuthority:
    processing_grant_kind = "model_processing_grant"

    def __init__(self, service, *, configuration, verify_coordinates):
        if not service.authority_id or service.authority_min_version == 0:
            raise ModelError("model_current_authority_required")
        if not callable(verify_coordinates):
            raise ModelError("model_query_proof_guard_required")
        self.service = service
        self.repository, self.scope, self.clock = service.repository, service.scope, service.clock
        self.configuration = configuration
        self.verify_coordinates = verify_coordinates

    async def clock_barrier(self, *, observed_at=None):
        from ..operations.refresh_demand import observed_clock

        async with self.repository.unit_of_work() as uow:
            return await observed_clock(
                uow, self.scope, self.clock if observed_at is None else lambda: observed_at
            )

    async def failure_barrier(self, observed):
        from ..derived.model import DerivedError

        try:
            await self.clock_barrier(observed_at=max(observed, self.clock()))
        except DerivedError as error:
            if error.code != "refresh_clock_discontinuity":
                raise

    async def allow_processing(
        self,
        source_id,
        *,
        readers,
        purposes,
        expires_at,
        expected_version=0,
        revoked=False,
        _unit_of_work=None,
    ):
        """Explicit host grant for THIS recipient, independent of ordinary read rights."""
        grant = ProcessingGrant(
            source_id, tuple(readers), tuple(purposes), expires_at=expires_at, revoked=revoked
        )
        if type(expected_version) is not int or expected_version < 0 or expires_at <= self.clock():
            raise ModelError("invalid_model_processing_grant")
        key = digest([source_id, self.configuration.recipient])
        transaction = (
            nullcontext(_unit_of_work)
            if _unit_of_work is not None
            else self.repository.unit_of_work()
        )
        async with transaction as uow:
            epoch = await open_derived(uow, self.scope)
            authority = await self.service.registry.authority(uow, self.service.authority_id)
            self.service.registry.permission(authority, readers, purposes)
            old = await uow.derived_get(self.scope, self.processing_grant_kind, key)
            if (
                old is None
                and len(await uow.derived_records(self.scope, self.processing_grant_kind)) >= 4096
            ):
                raise ModelError("model_processing_grant_capacity")
            if (old["version"] if old else 0) != expected_version:
                raise ModelError("model_processing_grant_conflict")
            row = dict(
                source_id=source_id,
                recipient=self.configuration.recipient,
                grant=grant.payload(),
                version=expected_version + 1,
                authority_version=authority["version"],
                epoch=epoch,
            )
            await uow.derived_put(self.scope, self.processing_grant_kind, key, row)
            return row

    async def _metadata(self, uow, coordinates, source_ids):
        epoch = await open_derived(uow, self.scope)
        from ..operations.refresh_demand import observed_clock

        await observed_clock(uow, self.scope, self.clock)
        if coordinates.scope_key != self.scope.partition_key():
            raise ModelError("model_scope_mismatch")
        if coordinates.audience != coordinates.principal:
            raise ModelError("model_shared_audience_unsupported")
        proof = await self.verify_coordinates(uow, coordinates)
        if proof is not True:
            raise ModelError("model_query_proof_unavailable")
        result = await self._processing(uow, coordinates, source_ids, epoch)
        if await self.verify_coordinates(uow, coordinates) is not True:
            raise ModelError("model_query_proof_unavailable")
        self._check_expiry(result[1], result[2])
        return result

    def _check_expiry(self, authority, grants):
        now = self.clock()
        if datetime.fromisoformat(authority["spec"]["expires_at"]) <= now:
            raise ModelError("model_processing_unauthorized")
        if any(datetime.fromisoformat(row["valid_until"]) <= now for row in grants.values()):
            raise ModelError("model_processing_unauthorized")

    async def _processing(self, uow, coordinates, source_ids, epoch):
        authority = await self.service.registry.authority(uow, self.service.authority_id)
        self.service.registry.permission(
            authority, (coordinates.principal,), (coordinates.purpose,)
        )
        grants = {}
        # Permission headers before loading ANY source bodies.
        for source_id in source_ids:
            grant = await uow.derived_get(self.scope, "grant", source_id)
            self.service.registry.grant_binding(grant, authority)
            if (
                not grant
                or grant.get("revoked")
                or coordinates.principal not in grant["readers"]
                or coordinates.purpose not in grant["purposes"]
                or grant.get("expires_at")
                and datetime.fromisoformat(grant["expires_at"]) <= self.clock()
            ):
                raise ModelError("model_read_unauthorized")
            key = digest([source_id, self.configuration.recipient])
            processing = await uow.derived_get(self.scope, self.processing_grant_kind, key)
            if (
                not processing
                or processing.get("state") == "erased"
                or processing.get("epoch") != epoch
                or processing.get("recipient") != self.configuration.recipient
                or processing.get("authority_version") != authority["version"]
            ):
                raise ModelError("model_processing_unauthorized")
            policy = processing["grant"]
            if (
                policy["revoked"]
                or coordinates.principal not in policy["readers"]
                or coordinates.purpose not in policy["purposes"]
                or datetime.fromisoformat(policy["expires_at"]) <= self.clock()
            ):
                raise ModelError("model_processing_unauthorized")
            boundaries = [authority["spec"]["expires_at"], policy["expires_at"]]
            if grant.get("expires_at"):
                boundaries.append(grant["expires_at"])
            grants[source_id] = {
                "read": digest(grant),
                "processing": digest(processing),
                "valid_until": min(datetime.fromisoformat(v) for v in boundaries).isoformat(),
            }
        self._check_expiry(authority, grants)
        return epoch, authority, grants
