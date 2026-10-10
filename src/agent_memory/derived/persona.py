"""Host-reviewed L3 hypotheses: explicit origin, context, evidence and counterexamples.

This is a scoped derived view, never source authority or unconditional L1 truth.
Inference thresholds count independent source families, not repeated quotations.
"""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime

from ..consolidation.admission_runtime import AdmissionEngine
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
    admission_status: str | None = None

    def __post_init__(self):
        if self.admission_status is None:
            object.__setattr__(
                self, "admission_status", "admitted" if self.atom_id else "source_declared"
            )
        if (
            self.admission_status not in {"admitted", "observed_unverified", "source_declared"}
            or self.admission_status == "admitted"
            and self.atom_id is None
            or self.admission_status == "observed_unverified"
            and self.relation == "supports"
        ):
            raise DerivedError("invalid_persona_evidence_status")
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
        from ..operations.refresh_demand import observed_clock

        observed = await observed_clock(uow, self.scope, self.service.clock)
        final = self.service.clock()
        if final < observed:
            raise DerivedError("refresh_clock_discontinuity")
        self.service.registry._authority_floor(authority)
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
                or sorted({row["event_id"], *AdmissionEngine.source_dependencies(row["payload"])})
                != proof.get("source_ids", [proof["source_id"]])
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

        label, text, origin, evidence, readers, purpose, context = deepcopy(
            (label, text, origin, tuple(evidence), tuple(readers), purpose, context)
        )
        async with self.repository.unit_of_work() as uow:
            return await self._publish_in_uow(
                uow,
                label,
                text,
                origin=origin,
                evidence=evidence,
                readers=readers,
                purpose=purpose,
                context=context,
                valid_from=valid_from,
                valid_to=valid_to,
                expected_version=expected_version,
            )

    async def _publish_in_uow(
        self,
        uow,
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
        evolution=None,
    ):
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
        primary_sources = sorted({item.source_id for item in evidence})
        epoch = await open_derived(uow, self.scope)
        source_set = set(primary_sources) | set(
            (evolution or {}).get("processing_source_hashes", {})
        )
        atom_sources = {}
        # Metadata dependencies are authorized before any source/Atom body read.
        for item in evidence:
            if item.atom_id is None:
                continue
            candidate = await uow.derived_header(self.scope, item.atom_id)
            if (
                not candidate
                or candidate["event_id"] != item.source_id
                or candidate["version"] != item.atom_version
            ):
                raise DerivedError("persona_atom_changed")
            dependencies = sorted({candidate["event_id"], *candidate["source_ids"]})
            atom_sources[item.atom_id] = dependencies
            source_set.update(dependencies)
        if len(source_set) > 128:
            raise DerivedError("persona_dependency_capacity")
        source_ids = sorted(source_set)
        await self._permission(uow, source_ids, readers, purpose)
        old = await uow.derived_get(self.scope, KINDS[0], key)
        if (old.get("version", 0) if old else 0) != expected_version:
            raise DerivedError("persona_version_conflict")
        if (
            old is None
            and sum(
                r["identity"].startswith("persona:")
                for r in await uow.derived_records(self.scope, KINDS[0])
            )
            >= 128
        ):
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
            retained_family = source.metadata.get("_retention", {}).get("document_id", source.id)
            if item.family != retained_family:
                raise DerivedError("persona_source_family_mismatch")
            if item.source_id in source_families and source_families[item.source_id] != item.family:
                raise DerivedError("persona_source_family_mismatch")
            source_families[item.source_id] = item.family
            hashes[item.source_id] = source.content_hash
        atoms = {}
        for item in evidence:
            if item.atom_id is None:
                continue
            row = await uow.get_admission_record(self.scope, item.atom_id)
            if (
                row is None
                or row["event_id"] != item.source_id
                or row["version"] != item.atom_version
            ):
                raise DerivedError("persona_atom_changed")
            if row["payload"].get("valid_to"):
                end = min(end, timestamp(datetime.fromisoformat(row["payload"]["valid_to"])))
            atoms[item.atom_id] = dict(
                version=item.atom_version,
                source_id=item.source_id,
                source_ids=atom_sources[item.atom_id],
                draft_sha256=digest(row["payload"]["draft"]),
            )
        await self._atoms(uow, atoms)
        for source_id in sorted(set(source_ids) - set(primary_sources)):
            source = await uow.get_source_event(self.scope, source_id)
            if (
                source is None
                or isinstance(source.metadata.get("_retention"), dict)
                and not await source_is_current(uow, source)
            ):
                raise DerivedError("persona_source_unavailable")
            declared_hash = (evolution or {}).get("processing_source_hashes", {}).get(source_id)
            if declared_hash is not None and declared_hash != source.content_hash:
                raise DerivedError("persona_source_unavailable")
            hashes[source_id] = source.content_hash
        parents = sorted(
            {
                *("atom:" + atom_id for atom_id in atoms),
                *((evolution or {}).get("candidate_parents", ())),
            }
        )
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
            primary_sources=primary_sources,
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
        header.update(label=label, published_at=self.service.clock().isoformat())
        if evolution is not None:
            header["evolution"] = deepcopy(evolution)
        header["revision_id"] = "persona-revision:" + digest([key, header])
        header["sha256"] = digest(header)
        await self._archive(uow, header, body)
        await uow.derived_put(self.scope, KINDS[0], key, header)
        await uow.derived_put(
            self.scope, KINDS[1], key, dict(sources=source_ids, parents=parents, result=body)
        )
        await uow.derived_edges(
            self.scope,
            key,
            [
                ("support" if s in primary_sources else "processing", "source:" + s)
                for s in source_ids
            ]
            + [
                (
                    "support" if parent in {"atom:" + atom for atom in atoms} else "processing",
                    parent,
                )
                for parent in parents
            ],
        )
        await self._atoms(uow, atoms)
        await self._permission(uow, source_ids, readers, purpose)
        if self.service.clock() >= end:
            raise DerivedError("persona_expired")
        return dict(
            id=key,
            version=header["version"],
            state=header["state"],
            revision_id=header["revision_id"],
        )

    async def _archive(self, uow, header, body):
        """Immutable reviewed beliefs; normal correction never deletes old beliefs."""
        key = header["revision_id"]
        if len(await uow.derived_records(self.scope, KINDS[0])) >= 4096:
            raise DerivedError("persona_history_capacity")
        stored = await uow.derived_get(self.scope, KINDS[0], key)
        if stored is not None and stored != header:
            raise DerivedError("persona_history_conflict")
        await uow.derived_put(self.scope, KINDS[0], key, deepcopy(header))
        await uow.derived_put(
            self.scope,
            KINDS[1],
            key,
            dict(sources=header["sources"], parents=header["parents"], result=deepcopy(body)),
        )
        await uow.derived_edges(
            self.scope,
            key,
            [
                (
                    "support" if s in header.get("primary_sources", ()) else "processing",
                    "source:" + s,
                )
                for s in header["sources"]
            ]
            + [
                (
                    "support"
                    if parent in {"atom:" + atom for atom in header["atoms"]}
                    else "processing",
                    parent,
                )
                for parent in header["parents"]
            ],
        )

    async def _withdraw_in_uow(
        self,
        uow,
        label,
        *,
        reason,
        readers,
        purpose,
        context,
        expected_version,
        evolution=None,
        sources=(),
        parents=(),
    ):
        """Withdraw current use, preserving reviewed revisions until explicit erasure."""
        if reason not in {
            "insufficient_support",
            "expired",
            "reviewer_withdrawal",
            "source_changed",
            "policy_changed",
        }:
            raise DerivedError("invalid_persona_withdrawal_reason")
        key = "persona:" + digest([self.scope.partition_key(), identity(label)])
        epoch = await open_derived(uow, self.scope)
        await self._permission(uow, (), readers, purpose)
        old = await uow.derived_get(self.scope, KINDS[0], key)
        if (old.get("version", 0) if old else 0) != expected_version:
            raise DerivedError("persona_version_conflict")
        all_sources = sorted(set(sources) | set((old or {}).get("sources", ())))
        all_parents = sorted(set(parents) | set((old or {}).get("parents", ())))
        body = dict(
            origin="inferred",
            truth_status="withdrawn",
            context=deepcopy(context),
            evidence=[],
            reason=reason,
        )
        now = self.service.clock().isoformat()
        header = dict(
            schema="persona-view/1",
            version=expected_version + 1,
            epoch=epoch,
            label=label,
            state="withdrawn",
            sources=all_sources,
            parents=all_parents,
            atoms={},
            source_hashes={},
            readers=list(readers),
            purpose=purpose,
            valid_from=now,
            valid_to=now,
            published_at=now,
            policy_sha256=self.policy_sha256,
            body_sha256=digest(body),
        )
        if evolution is not None:
            header["evolution"] = deepcopy(evolution)
        header["revision_id"] = "persona-revision:" + digest([key, header])
        header["sha256"] = digest(header)
        await self._archive(uow, header, body)
        await uow.derived_put(self.scope, KINDS[0], key, header)
        await uow.derived_put(
            self.scope, KINDS[1], key, dict(sources=all_sources, parents=all_parents, result=body)
        )
        return dict(
            id=key, state="withdrawn", version=header["version"], revision_id=header["revision_id"]
        )

    async def read_history(
        self,
        label,
        *,
        revision_id,
        actor,
        purpose,
        context,
        valid_at=None,
    ):
        """Read the recorded reviewed belief, not an extrapolated continuous interval.

        Source/Atom corrections do not erase what was believed. Current source
        permissions, physical retention, scope epoch and exact archive integrity
        still apply; these archived hypotheses never re-enter L1 as authority.
        """
        await self._barrier()
        identity(label)
        identity(revision_id)
        identity(actor)
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            header = await uow.derived_get(self.scope, KINDS[0], revision_id)
            if (
                not header
                or header.get("state") == "erased"
                or header.get("revision_id") != revision_id
                or header.get("label") != label
                or header.get("epoch") != epoch
            ):
                raise DerivedError("persona_history_unavailable")
            if (
                actor not in header["readers"]
                or purpose != header["purpose"]
                or digest({k: v for k, v in header.items() if k != "sha256"}) != header["sha256"]
            ):
                raise DerivedError("persona_access_or_integrity_failed")
            await self._permission(uow, header["sources"], (actor,), purpose)
            for source_id in header["sources"]:
                source = await uow.get_source_event(self.scope, source_id)
                expected_hash = header["source_hashes"].get(source_id)
                if source is None or expected_hash and source.content_hash != expected_hash:
                    raise DerivedError("persona_history_unavailable")
            if valid_at is not None:
                timestamp(valid_at)
                if not (
                    datetime.fromisoformat(header["valid_from"])
                    <= valid_at
                    < datetime.fromisoformat(header["valid_to"])
                ):
                    raise DerivedError("persona_history_validity_unavailable")
            stored = await uow.derived_get(self.scope, KINDS[1], revision_id)
            if (
                not stored
                or "result" not in stored
                or digest(stored["result"]) != header["body_sha256"]
            ):
                raise DerivedError("persona_history_unavailable")
            if stored["result"]["context"] != context:
                raise DerivedError("persona_context_mismatch")
            if await uow.derived_get(self.scope, KINDS[0], revision_id) != header:
                raise DerivedError("persona_history_unavailable")
            await self._permission(uow, header["sources"], (actor,), purpose)
            return dict(
                schema="persona-history-answer/1",
                label=label,
                revision_id=revision_id,
                version=header["version"],
                state=header["state"],
                known_at=header["published_at"],
                valid_from=header["valid_from"],
                valid_until=header["valid_to"],
                historical=True,
                **deepcopy(stored["result"]),
            )

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
                if header.get("evolution"):
                    definition = await uow.derived_get(
                        self.scope, "definition", header["evolution"]["definition_id"]
                    )
                    if (
                        not definition
                        or definition.get("disabled")
                        or definition.get("dirty")
                        or definition.get("last_persona_revision") != header["revision_id"]
                    ):
                        raise DerivedError("persona_evolution_stale")
                await self._atoms(uow, header["atoms"])
                await self._permission(uow, header["sources"], (actor,), purpose)
                for source_id in header["sources"]:
                    source = await uow.get_source_event(self.scope, source_id)
                    if (
                        source is None
                        or source.content_hash != header["source_hashes"][source_id]
                        or isinstance(source.metadata.get("_retention"), dict)
                        and not await source_is_current(uow, source)
                        or source_id in header.get("primary_sources", header["sources"])
                        and not isinstance(source.metadata.get("_retention"), dict)
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
                revision_id=header.get("revision_id"),
                published_at=header.get("published_at"),
                valid_until=header["valid_to"],
                **stored["result"],
            )
