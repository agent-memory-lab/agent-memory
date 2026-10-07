"""Transactional host query/authority registry and current control proofs."""

from copy import deepcopy
from datetime import datetime

from ..consolidation.admission import slot_key
from ..domain import AtomDraft, ScopeLevel
from .contracts import HostGrantAuthority, QueryDefinition
from .coverage import close_coverage
from .model import DerivedError, digest


def slots(scope, subject, predicates):
    for level in ScopeLevel:
        try:
            if scope.project(level) == scope:
                return [
                    slot_key(
                        scope, AtomDraft(subject, p, "slot", "slot", "slot", scope_level=level)
                    )
                    for p in predicates
                ]
        except ValueError:
            continue
    raise DerivedError("derived_scope_unsupported")


def expected(value):
    if type(value) is not int or value < 0:
        raise DerivedError("invalid_derived_control_version")


class DerivedRegistry:
    """Shares the Observation scope lock, retention epoch and durable outbox."""

    def __init__(self, service):
        self.service, self.scope = service, service.scope

    async def _invalidate(self, uow, field, key, *, safety=False):
        for item in await uow.derived_records(self.scope, "definition"):
            row = item["payload"]
            if row["spec"].get(field) == key and not row.get("disabled"):
                if not safety:
                    await close_coverage(
                        uow, self.scope, item["identity"], at=self.service.clock(), reason="query"
                    )
                row["dirty"] = True
                if safety:
                    row["safety_generation"] += 1
                await uow.derived_put(self.scope, "definition", item["identity"], row)

    async def register_query(self, uow, epoch, definition, generation):
        expected(generation)
        if not isinstance(definition, QueryDefinition):
            raise TypeError("trusted QueryDefinition required")
        if definition.scope != self.scope:
            raise DerivedError("derived_query_scope_mismatch")
        predicates = {p["predicate"] for p in self.service.policy["predicates"]}
        if set(definition.predicates) - predicates:
            raise DerivedError("derived_predicate_unregistered")
        spec = definition.payload()
        old = await uow.derived_get(self.scope, "query", definition.id)
        if (old["generation"] if old else 0) != generation:
            raise DerivedError("derived_query_conflict")
        if old is None and len(await uow.derived_records(self.scope, "query")) >= 128:
            raise DerivedError("derived_query_capacity")
        for item in await uow.derived_records(self.scope, "definition"):
            facet = item["payload"]
            if facet["spec"].get("query_id") == definition.id and not facet.get("disabled"):
                self._consumer(spec, facet["spec"])
        row = dict(
            spec=spec,
            generation=generation + 1,
            epoch=epoch,
            disabled=False,
            fingerprint=digest(dict(query=spec, policy=self.service.policy)),
            slots=slots(self.scope, definition.subject_id, definition.predicates),
        )
        await uow.derived_put(self.scope, "query", definition.id, row)
        await self._invalidate(uow, "query_id", definition.id)
        return deepcopy(row)

    async def set_authority(self, uow, epoch, authority, version):
        expected(version)
        if not isinstance(authority, HostGrantAuthority):
            raise TypeError("trusted HostGrantAuthority required")
        if authority.id != self.service.authority_id:
            raise DerivedError("derived_authority_mismatch")
        if not authority.revoked and not (
            0 < (authority.expires_at - self.service.clock()).total_seconds() <= 86400
        ):
            raise DerivedError("invalid_derived_authority_expiry")
        old = await uow.derived_get(self.scope, "authority", authority.id)
        if old is not None:
            self._authority_floor(old)
        elif self.service.authority_min_version != 0:
            raise DerivedError("derived_authority_rollback")
        if (old["version"] if old else 0) != version:
            raise DerivedError("derived_authority_conflict")
        if old is None and len(await uow.derived_records(self.scope, "authority")) >= 128:
            raise DerivedError("derived_authority_capacity")
        row = dict(spec=authority.payload(), version=version + 1, epoch=epoch)
        row["fingerprint"] = digest(row["spec"])
        await uow.derived_put(self.scope, "authority", authority.id, row)
        await self._invalidate(uow, "authority_id", authority.id, safety=True)
        return deepcopy(row)

    async def authority(self, uow, key):
        if key != self.service.authority_id:
            raise DerivedError("derived_authority_mismatch")
        if key is None:
            return None
        row = await uow.derived_get(self.scope, "authority", key)
        if row is not None:
            self._authority_floor(row)
        if (
            not row
            or row["epoch"] != await uow.retention_epoch(self.scope)
            or row["spec"].get("revoked")
            or digest(row["spec"]) != row["fingerprint"]
        ):
            raise DerivedError("derived_authority_unavailable")
        if datetime.fromisoformat(row["spec"]["expires_at"]) <= self.service.clock():
            raise DerivedError("derived_authority_expired")
        return row

    def _authority_floor(self, row):
        if self.service.authority_min_version == 0:
            raise DerivedError("trusted_authority_version_required")
        if row["version"] < self.service.authority_min_version:
            raise DerivedError("derived_authority_rollback")

    @staticmethod
    def _consumer(query, facet):
        if query["subject_id"] != facet["subject_id"] or (
            query["predicates"] != facet["predicates"]
        ):
            raise DerivedError("derived_query_consumer_unsupported")

    async def bindings(self, uow, facet):
        proof = {}
        if facet.get("query_id"):
            key = facet["query_id"]
            query = await uow.derived_get(self.scope, "query", key)
            if (
                not query
                or query.get("disabled")
                or query["epoch"] != await uow.retention_epoch(self.scope)
            ):
                raise DerivedError("derived_query_unavailable")
            if (
                digest(dict(query=query["spec"], policy=self.service.policy))
                != query["fingerprint"]
            ):
                raise DerivedError("derived_query_configuration_changed")
            self._consumer(query["spec"], facet)
            proof["query"] = dict(
                id=key, generation=query["generation"], sha256=query["fingerprint"]
            )
        authority = await self.authority(uow, facet.get("authority_id"))
        if authority is not None:
            self.permission(authority, facet["readers"], (facet["purpose"],))
            proof["authority"] = dict(
                id=facet["authority_id"],
                version=authority["version"],
                sha256=authority["fingerprint"],
            )
        return proof or None, authority

    @staticmethod
    def permission(authority, readers, purposes):
        if authority is not None and (
            not set(readers).issubset(authority["spec"]["readers"])
            or not set(purposes).issubset(authority["spec"]["purposes"])
        ):
            raise DerivedError("derived_authority_denied")

    @staticmethod
    def grant_binding(grant, authority):
        key = authority["spec"]["id"] if authority else None
        if grant is not None and (
            grant.get("authority_id") != key
            or (authority is not None and grant.get("authority_version") != authority["version"])
        ):
            raise DerivedError("derived_grant_authority_changed")
