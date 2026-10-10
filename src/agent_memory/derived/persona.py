"""Host-reviewed L3 hypotheses: explicit origin, context, evidence and counterexamples.

This is a scoped derived view, never source authority or unconditional L1 truth.
Inference thresholds count independent source families, not repeated quotations.
"""

from dataclasses import dataclass
from datetime import datetime

from ..lifecycle import is_memory_context
from ..operations.source_revisions import source_is_current
from .model import DerivedError, digest, identity, timestamp
from .service import open_derived

KINDS = ("persona_header", "persona_body")


@dataclass(frozen=True, slots=True)
class PersonaEvidence:
    source_id: str
    start: int
    end: int
    quote: str
    family: str
    relation: str = "supports"
    atom_id: str | None = None
    atom_version: int | None = None

    def __post_init__(self):
        identity(self.source_id)
        identity(self.family)
        if self.atom_id is not None:
            identity(self.atom_id)
            if type(self.atom_version) is not int or self.atom_version < 1:
                raise DerivedError("persona_atom_version_required")
        elif self.atom_version is not None:
            raise DerivedError("persona_atom_identity_required")
        if (
            type(self.start) is not int
            or type(self.end) is not int
            or not 0 <= self.start < self.end
            or type(self.quote) is not str
            or not 1 <= len(self.quote) <= 4096
            or self.relation not in {"supports", "counterexample"}
        ):
            raise DerivedError("invalid_persona_evidence")


class PersonaViews:
    def __init__(self, service, *, reviewer_revision, minimum_families=3):
        if not service.authority_id or not service.authority_min_version:
            raise DerivedError("persona_current_authority_required")
        identity(reviewer_revision)
        if type(minimum_families) is not int or not 2 <= minimum_families <= 16:
            raise DerivedError("invalid_persona_inference_policy")
        self.service, self.scope, self.repository = service, service.scope, service.repository
        self.reviewer_revision, self.minimum_families = reviewer_revision, minimum_families
        self.policy_sha256 = digest([reviewer_revision, minimum_families])

    async def _permission(self, uow, sources, readers, purpose):
        authority = await self.service.registry.authority(uow, self.service.authority_id)
        self.service.registry.permission(authority, readers, (purpose,))
        proofs = {}
        raw_grants = {}
        for source_id in sources:
            grant = await uow.derived_get(self.scope, "grant", source_id)
            self.service._permission(grant, readers, purpose, self.service.clock(), authority)
            proofs[source_id] = digest(grant)
            raw_grants[source_id] = grant
        final = self.service.clock()
        if datetime.fromisoformat(authority["spec"]["expires_at"]) <= final:
            raise DerivedError("persona_authority_expired")
        for grant in raw_grants.values():
            self.service._permission(grant, readers, purpose, final, authority)
        return proofs

    async def _atoms(self, uow, references):
        from ..serialization import to_jsonable

        for atom_id, proof in references.items():
            row = await uow.get_admission_record(self.scope, atom_id)
            if (
                row is None
                or row["scope"] != to_jsonable(self.scope)
                or row["version"] != proof["version"]
                or row["event_id"] != proof["source_id"]
                or row["payload"].get("deleted")
                or row["payload"]["action"] != "ACCEPT"
                or digest(row["payload"]["draft"]) != proof["draft_sha256"]
            ):
                raise DerivedError("persona_atom_changed")
            now = self.service.clock()
            if (
                datetime.fromisoformat(row["payload"]["valid_from"]) > now
                or row["payload"].get("valid_to")
                and datetime.fromisoformat(row["payload"]["valid_to"]) <= now
            ):
                raise DerivedError("persona_atom_expired")

    async def publish(
        self,
        label,
        text,
        *,
        origin,
        evidence,
        readers,
        purpose,
        context,
        valid_from,
        valid_to,
        expected_version=0,
    ):
        """Trusted semantic review only; transports cannot publish or pick authority."""
        from dataclasses import asdict

        identity(label)
        identity(purpose)
        readers, evidence = tuple(readers), tuple(evidence)
        if (
            type(text) is not str
            or not 1 <= len(text) <= 4096
            or origin not in {"explicit", "inferred"}
            or not 1 <= len(evidence) <= 32
            or any(type(item) is not PersonaEvidence for item in evidence)
            or not 1 <= len(readers) <= 32
            or any(type(r) is not str for r in readers)
            or type(context) is not dict
            or len(__import__("json").dumps(context)) > 4096
            or type(expected_version) is not int
            or expected_version < 0
        ):
            raise DerivedError("invalid_persona_statement")
        start, end = timestamp(valid_from), timestamp(valid_to)
        if start >= end or self.service.clock() >= end:
            raise DerivedError("invalid_persona_validity")
        families = {item.family for item in evidence if item.relation == "supports"}
        if not families or (origin == "inferred" and len(families) < self.minimum_families):
            raise DerivedError("persona_independent_support_insufficient")
        if origin == "inferred" and any(
            e.atom_id is None for e in evidence if e.relation == "supports"
        ):
            raise DerivedError("persona_inference_admitted_atoms_required")
        key = "persona:" + digest([self.scope.partition_key(), label])
        source_ids = sorted({item.source_id for item in evidence})
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            await self._permission(uow, source_ids, readers, purpose)
            old = await uow.derived_get(self.scope, KINDS[0], key)
            if (old.get("version", 0) if old else 0) != expected_version:
                raise DerivedError("persona_version_conflict")
            if old is None and len(await uow.derived_records(self.scope, KINDS[0])) >= 128:
                raise DerivedError("persona_capacity")
            hashes, source_families = {}, {}
            for item in evidence:
                source = await uow.get_source_event(self.scope, item.source_id)
                if (
                    source is None
                    or is_memory_context(source)
                    or not await source_is_current(uow, source)
                    or source.metadata.get("lifecycle", {}).get("origin") == "model"
                    or source.content[item.start : item.end] != item.quote
                ):
                    raise DerivedError("persona_source_unavailable")
                retained_family = source.metadata.get("_retention", {}).get(
                    "document_id", source.id
                )
                if item.family != retained_family:
                    raise DerivedError("persona_source_family_mismatch")
                if (
                    item.source_id in source_families
                    and source_families[item.source_id] != item.family
                ):
                    raise DerivedError("persona_source_family_mismatch")
                source_families[item.source_id] = item.family
                hashes[item.source_id] = source.content_hash
            atoms = {}
            for item in evidence:
                if item.atom_id is None:
                    continue
                row = await uow.get_admission_record(self.scope, item.atom_id)
                if row is None:
                    raise DerivedError("persona_atom_changed")
                if row["payload"].get("valid_to"):
                    end = min(end, timestamp(datetime.fromisoformat(row["payload"]["valid_to"])))
                atoms[item.atom_id] = dict(
                    version=item.atom_version,
                    source_id=item.source_id,
                    draft_sha256=digest(row["payload"]["draft"]),
                )
            await self._atoms(uow, atoms)
            parents = ["atom:" + atom_id for atom_id in sorted(atoms)]
            body = dict(
                text=text,
                origin=origin,
                truth_status="host_declared" if origin == "explicit" else "hypothesis",
                context=context,
                evidence=[asdict(e) for e in evidence],
            )
            header = dict(
                schema="persona-view/1",
                version=expected_version + 1,
                epoch=epoch,
                state="contested"
                if any(e.relation == "counterexample" for e in evidence)
                else "active",
                sources=source_ids,
                parents=parents,
                atoms=atoms,
                source_hashes=hashes,
                readers=list(readers),
                purpose=purpose,
                valid_from=start.isoformat(),
                valid_to=end.isoformat(),
                policy_sha256=self.policy_sha256,
                body_sha256=digest(body),
            )
            header["sha256"] = digest(header)
            await uow.derived_put(self.scope, KINDS[0], key, header)
            await uow.derived_put(
                self.scope, KINDS[1], key, dict(sources=source_ids, parents=parents, result=body)
            )
            await uow.derived_edges(
                self.scope,
                key,
                [("support", "source:" + s) for s in source_ids]
                + [("support", parent) for parent in parents],
            )
            await self._permission(uow, source_ids, readers, purpose)
            await self._atoms(uow, atoms)
            if self.service.clock() >= end:
                raise DerivedError("persona_expired")
        return dict(id=key, version=header["version"], state=header["state"])

    async def _barrier(self):
        from ..operations.refresh_demand import observed_clock

        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope)
            await observed_clock(uow, self.scope, self.service.clock)

    async def read(self, label, *, actor, purpose, context):
        await self._barrier()
        try:
            return await self._read(label, actor=actor, purpose=purpose, context=context)
        except BaseException:
            await self._barrier()
            raise

    async def _read(self, label, *, actor, purpose, context):
        identity(label)
        identity(actor)
        key = "persona:" + digest([self.scope.partition_key(), label])
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            header = await uow.derived_get(self.scope, KINDS[0], key)
            if not header or header.get("state") not in {"active", "contested"}:
                raise DerivedError("persona_unavailable")
            if (
                header["epoch"] != epoch
                or actor not in header["readers"]
                or purpose != header["purpose"]
                or header["policy_sha256"] != self.policy_sha256
                or digest({k: v for k, v in header.items() if k != "sha256"}) != header["sha256"]
            ):
                raise DerivedError("persona_access_or_integrity_failed")

            async def guard():
                await self._atoms(uow, header["atoms"])
                await self._permission(uow, header["sources"], (actor,), purpose)
                for source_id in header["sources"]:
                    source = await uow.get_source_event(self.scope, source_id)
                    if (
                        source is None
                        or source.content_hash != header["source_hashes"][source_id]
                        or not await source_is_current(uow, source)
                    ):
                        raise DerivedError("persona_source_unavailable")
                if (
                    not datetime.fromisoformat(header["valid_from"])
                    <= self.service.clock()
                    < datetime.fromisoformat(header["valid_to"])
                ):
                    raise DerivedError("persona_expired")

            await self._permission(uow, header["sources"], (actor,), purpose)
            await guard()
            stored = await uow.derived_get(self.scope, KINDS[1], key)
            if (
                not stored
                or "result" not in stored
                or digest(stored["result"]) != header["body_sha256"]
            ):
                raise DerivedError("persona_integrity_failed")
            if stored["result"]["context"] != context:
                raise DerivedError("persona_context_mismatch")
            await guard()
            if await uow.derived_get(self.scope, KINDS[0], key) != header:
                raise DerivedError("persona_changed")
            await self._permission(uow, header["sources"], (actor,), purpose)
            if (
                not datetime.fromisoformat(header["valid_from"])
                <= self.service.clock()
                < datetime.fromisoformat(header["valid_to"])
            ):
                raise DerivedError("persona_expired")
            return dict(
                schema="persona-answer/1",
                id=key,
                state=header["state"],
                version=header["version"],
                valid_until=header["valid_to"],
                **stored["result"],
            )
