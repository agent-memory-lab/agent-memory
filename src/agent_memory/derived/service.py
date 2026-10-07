"""Host-owned derived lifecycle: authorize snapshot, prepare, CAS publish, guarded read."""

from copy import deepcopy
from datetime import datetime

from ..domain import canonical_json, utc_now
from ..operations.retention import RetentionError
from ..operations.source_revisions import source_is_current
from .model import (
    DerivedError,
    FacetContext,
    FacetDefinition,
    FacetRefreshUnit,
    ProcessingGrant,
    digest,
    identity,
    source_ids,
    timestamp,
    validate_edges,
)
from .observation import compose
from .registry import DerivedRegistry, slots


async def open_derived(uow, scope):
    required = (
        "derived_get",
        "derived_put",
        "derived_records",
        "derived_candidates",
        "derived_edges",
        "derived_reverse",
        "lock_admission_scope",
        "retention_epoch",
        "get_source_event",
        "get_admission_record",
    )
    if any(not callable(getattr(uow, name, None)) for name in required):
        raise DerivedError("derived_backend_unsupported")
    await uow.lock_admission_scope(scope)
    return await uow.retention_epoch(scope)


async def mark_slot_changed(uow, scope, key):
    """Always maintained, including slots with no registered subscribers yet."""
    barrier = await uow.derived_get(scope, "barrier", key) or {"generation": 0}
    await uow.derived_put(scope, "barrier", key, {"generation": barrier["generation"] + 1})
    for item in await uow.derived_records(scope, "definition"):
        row = item["payload"]
        if key in row["slots"] and not row.get("disabled"):
            row["dirty"] = True  # durable refresh responsibility in original write UoW
            await uow.derived_put(scope, "definition", item["identity"], row)


async def interpretation_changed(uow, scope, source_id):
    # Query membership includes every candidate, not just already cited parents.
    changed_slots = set()
    for item in await uow.derived_records(scope, "definition"):
        row = item["payload"]
        headers = await uow.derived_candidates(scope, row["slots"])
        if any(source_id in h["source_ids"] for h in headers):
            changed_slots.update(row["slots"])
    for key in sorted(changed_slots):
        await mark_slot_changed(uow, scope, key)


async def source_document(uow, source):
    retained = source.metadata.get("_retention")
    if not isinstance(retained, dict):
        return None
    return await uow.retention_head_get(
        source.scope, "document", retained.get("document_id", source.id)
    )


def authorization_summary(spec, sources, grants):
    sensitivity_order = {"public": 0, "internal": 1, "private": 2, "restricted": 3}
    retention_order = {"ephemeral": 0, "session": 1, "standard": 2, "persistent": 3}
    sensitivities = [g["sensitivity"] for g in grants.values()] + [
        s.sensitivity for s in sources.values()
    ]
    retentions = [g["retention_class"] for g in grants.values()] + [
        s.retention_class for s in sources.values()
    ]
    if any(v not in sensitivity_order for v in sensitivities) or any(
        v not in retention_order for v in retentions
    ):
        raise DerivedError("derived_input_policy_unsupported")
    return dict(
        readers=spec["readers"],
        purpose=spec["purpose"],
        sensitivity=max(sensitivities, key=sensitivity_order.get) if sensitivities else "private",
        retention_class=min(retentions, key=retention_order.get) if retentions else "session",
    )


class ObservationService:
    """Trusted host API. Transport exposes only call(), never these write methods."""

    def __init__(
        self,
        repository,
        scope,
        policy,
        *,
        clock=utc_now,
        context_token=None,
        authority_id=None,
        authority_min_version=None,
    ):
        self.repository, self.scope, self.clock = repository, scope, clock
        self.policy = policy.config_payload()
        self.context_token = identity(context_token) if context_token is not None else None
        self.authority_id = identity(authority_id) if authority_id is not None else None
        if self.authority_id is not None and (
            type(authority_min_version) is not int or authority_min_version < 0
        ):
            raise DerivedError("trusted_authority_version_required")
        if self.authority_id is None and authority_min_version is not None:
            raise DerivedError("derived_authority_mismatch")
        self.authority_min_version = authority_min_version
        self.registry = DerivedRegistry(self)
        predicates = {s["predicate"]: s for s in self.policy["predicates"]}
        if "locale" not in predicates or predicates["locale"]["value_type"] != "string":
            raise DerivedError("derived_predicate_unregistered")

    async def register_query(self, definition, *, expected_generation=0):
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            return await self.registry.register_query(uow, epoch, definition, expected_generation)

    async def set_authority(self, authority, *, expected_version=0):
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            row = await self.registry.set_authority(uow, epoch, authority, expected_version)
        # Advance the host floor only after a successful commit. Persist this floor
        # outside restored database snapshots before using the new ACL revision.
        self.authority_min_version = max(self.authority_min_version, row["version"])
        return row

    async def register(self, definition, *, expected_generation=0):
        if not isinstance(definition, FacetDefinition):
            raise TypeError("trusted FacetDefinition required")
        if definition.subject_id != self.scope.user_id:
            raise DerivedError("derived_subject_scope_mismatch")
        spec = definition.payload()
        self._context_binding(spec)
        if definition.context is not None:
            definition.context.current(self.clock())
        fingerprint = digest(dict(definition=spec, policy=self.policy))
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            await self.registry.bindings(uow, spec)
            old = await uow.derived_get(self.scope, "definition", definition.id)
            if old and old["spec"].get("authority_id") not in {None, self.authority_id}:
                raise DerivedError("derived_authority_mismatch")
            if old and not old.get("disabled") and old["fingerprint"] == fingerprint:
                return deepcopy(old)
            if (old["generation"] if old else 0) != expected_generation:
                raise DerivedError("derived_definition_conflict")
            if not old and len(await uow.derived_records(self.scope, "definition")) >= 128:
                raise DerivedError("derived_definition_capacity")
            row = dict(
                spec=spec,
                facet_id=definition.id,
                slots=slots(self.scope, definition.subject_id, definition.predicates),
                fingerprint=fingerprint,
                generation=(old["generation"] if old else 0) + 1,
                epoch=epoch,
                safety_generation=(old["safety_generation"] if old else 0),
                time_generation=0,
                dirty=True,
                disabled=False,
            )
            await uow.derived_put(self.scope, "definition", definition.id, row)
            return deepcopy(row)

    async def grant(self, grant, *, expected_version=0):
        if not isinstance(grant, ProcessingGrant):
            raise TypeError("trusted ProcessingGrant required")
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope)
            authority = await self.registry.authority(uow, self.authority_id)
            self.registry.permission(authority, grant.readers, grant.purposes)
            old = await uow.derived_get(self.scope, "grant", grant.source_id)
            if old and old.get("authority_id") not in {None, self.authority_id}:
                raise DerivedError("derived_authority_mismatch")
            if (old["version"] if old else 0) != expected_version:
                raise DerivedError("derived_grant_conflict")
            if old is None and len(await uow.derived_records(self.scope, "grant")) >= 4096:
                raise DerivedError("derived_grant_capacity")
            source = await uow.get_source_event(self.scope, grant.source_id)
            if source is None:
                raise DerivedError("derived_grant_source_missing")
            row = dict(**grant.payload(), version=expected_version + 1)
            if authority is not None:
                row.update(authority_id=self.authority_id, authority_version=authority["version"])
            await uow.derived_put(self.scope, "grant", grant.source_id, row)
            for item in await uow.derived_records(self.scope, "definition"):
                definition = item["payload"]
                headers = await uow.derived_candidates(self.scope, definition["slots"])
                if any(grant.source_id in h["source_ids"] for h in headers):
                    definition.update(
                        dirty=True, safety_generation=definition["safety_generation"] + 1
                    )
                    await uow.derived_put(self.scope, "definition", item["identity"], definition)
            return deepcopy(row)

    async def _unit(self, uow, row):
        epoch = await uow.retention_epoch(self.scope)
        bindings, _ = await self.registry.bindings(uow, row["spec"])
        query = {
            key: (await uow.derived_get(self.scope, "barrier", key) or {"generation": 0})[
                "generation"
            ]
            for key in row["slots"]
        }
        return FacetRefreshUnit(
            row["facet_id"],
            row["fingerprint"],
            row["generation"],
            epoch,
            query,
            row["safety_generation"],
            row["time_generation"],
            schema="facet-refresh-unit/2" if bindings is not None else "facet-refresh-unit/1",
            bindings=bindings,
        )

    async def _definition(self, uow, facet_id):
        row = await uow.derived_get(self.scope, "definition", identity(facet_id))
        if (
            row is None
            or row.get("disabled")
            or row["epoch"] != await uow.retention_epoch(self.scope)
        ):
            raise DerivedError("derived_definition_unavailable")
        if digest(dict(definition=row["spec"], policy=self.policy)) != row["fingerprint"]:
            raise DerivedError("derived_definition_configuration_changed")
        self._context_binding(row["spec"])
        await self.registry.bindings(uow, row["spec"])
        return row

    def accepts_definition(self, row):
        binding = row["spec"].get("context")
        return row["spec"].get("authority_id") == self.authority_id and (
            binding is None or binding["query"]["snapshot_token"] == self.context_token
        )

    def _context_binding(self, spec):
        if spec.get("context") is None:
            return None
        binding = FacetContext.from_payload(spec["context"])
        if (
            binding.query.snapshot_token != self.context_token
            or binding.query.scope != self.scope
            or binding.query.subject_id != spec["subject_id"]
            or binding.query.purpose != spec["purpose"]
        ):
            raise DerivedError("derived_context_mismatch")
        return binding

    async def _check_unit(self, uow, unit):
        row = await self._definition(uow, unit["facet_id"])
        if (await self._unit(uow, row)).payload() != unit:
            raise DerivedError("derived_snapshot_changed")
        return row

    def _permission(self, grant, readers, purpose, at, authority=None):
        self.registry.grant_binding(grant, authority)
        if grant is None or grant.get("revoked"):
            raise DerivedError("derived_processing_denied")
        if not set(readers).issubset(grant["readers"]) or purpose not in grant["purposes"]:
            raise DerivedError("derived_processing_denied")
        if grant.get("expires_at") and datetime.fromisoformat(grant["expires_at"]) <= at:
            raise DerivedError("derived_processing_grant_expired")

    async def snapshot(self, task):
        timestamp(self.clock())
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope)
            from ..operations.facet_refresh import checked_job

            await checked_job(self, uow, task)
            unit = task.payload["unit"]
            definition = await self._check_unit(uow, unit)
            at = self.clock()
            _, authority = await self.registry.bindings(uow, definition["spec"])
            binding = self._context_binding(definition["spec"])
            if binding is not None:
                binding.current(at)  # Expired routing must not fetch source bodies.
            headers = await uow.derived_candidates(self.scope, definition["slots"])
            if len(headers) > 64:
                raise DerivedError("derived_snapshot_capacity")
            ids = sorted({key for header in headers for key in header["source_ids"]})
            if len(ids) + len(headers) > 128:
                raise DerivedError("derived_input_capacity")
            grants, sources, interpretations, records = {}, {}, {}, []
            # Authorization of every actual source precedes fetching any L0/L1 body.
            for key in ids:
                grant = await uow.derived_get(self.scope, "grant", key)
                self._permission(
                    grant,
                    definition["spec"]["readers"],
                    definition["spec"]["purpose"],
                    at,
                    authority,
                )
                grants[key] = grant
            primary_ids = {header["event_id"] for header in headers}
            for key in ids:
                source = await uow.get_source_event(self.scope, key)
                if source is None or source.scope != self.scope:
                    raise DerivedError("derived_source_unavailable")
                if key not in primary_ids:
                    if (
                        binding is not None
                        and "_retention" in source.metadata
                        and not await source_is_current(uow, source)
                    ):
                        raise DerivedError("derived_evidence_superseded")
                    interpretations[key] = {"context": True}
                    sources[key] = source
                    continue
                try:
                    current = await source_is_current(uow, source)
                except RetentionError:
                    raise DerivedError("derived_source_not_retained") from None
                if not current:
                    # A withdrawn revision can remain in query coverage, not a body input.
                    sources[key] = source
                    interpretations[key] = {"inactive": True}
                    continue
                head = await uow.retention_head_get(self.scope, "interpretation", key)
                if head is None or head["payload"].get("publication_closed") is False:
                    raise DerivedError("derived_interpretation_incomplete")
                interpretations[key] = head
                sources[key] = source
            for header in headers:
                row = await uow.get_admission_record(self.scope, header["id"])
                if (
                    row is None
                    or row["version"] != header["version"]
                    or row["payload"].get("deleted")
                ):
                    raise DerivedError("derived_snapshot_changed")
                if set(source_ids(row["payload"])) - set(ids):
                    raise DerivedError("derived_input_dependency_unknown")
                if datetime.fromisoformat(row["recorded_at"]) > at:
                    raise DerivedError("derived_future_knowledge")
                current_head = interpretations[row["event_id"]]
                if row["payload"]["action"] not in {"WITHDRAWN", "REJECT", "L0_ONLY"} and (
                    current_head.get("inactive")
                    or row["id"] not in current_head["payload"].get("active_ids", [])
                ):
                    raise DerivedError("derived_interpretation_changed")
                records.append(row)
            manifest = dict(
                schema="derived-input-manifest/1",
                unit=unit,
                policy_sha256=digest(self.policy),
                template=definition["spec"]["template_version"],
                atoms={r["id"]: r["version"] for r in records},
                sources={
                    key: dict(
                        hash=source.content_hash,
                        interpretation=interpretations[key],
                        document=await source_document(uow, source),
                        grant_version=grants[key]["version"],
                    )
                    for key, source in sources.items()
                },
                query_complete=True,
            )
            manifest["authorization"] = authorization_summary(definition["spec"], sources, grants)
            if len(canonical_json(manifest).encode()) > 262144:
                raise DerivedError("derived_manifest_capacity")
            await self._check_unit(uow, unit)  # Legacy head adoption can advance a barrier.
            head = await uow.derived_get(self.scope, "head", unit["facet_id"])
            return dict(
                definition=definition,
                at=at,
                records=records,
                sources=sources,
                grants=grants,
                manifest=manifest,
                expected_head=head,
                authority=authority,
            )

    def prepare(self, snapshot):
        result = compose(
            snapshot["definition"]["spec"],
            snapshot["records"],
            snapshot["sources"],
            snapshot["at"],
            admission_policy=self.policy,
        )
        expiries = [g["expires_at"] for g in snapshot["grants"].values() if g.get("expires_at")]
        if snapshot.get("authority") is not None:
            expiries.append(snapshot["authority"]["spec"]["expires_at"])
        transitions = [v for v in [result["next_transition_at"], *expiries] if v]
        result["next_transition_at"] = (
            min(transitions, key=datetime.fromisoformat) if transitions else None
        )
        edges = result["support"] + [("processing", "atom:" + r["id"]) for r in snapshot["records"]]
        edges += [("processing", "source:" + key) for key in snapshot["sources"]]
        edges += [
            ("query", "facet:" + key) for key in snapshot["manifest"]["unit"]["query_generation"]
        ]
        if len(edges) > 512:
            raise DerivedError("derived_dependency_capacity")
        validate_edges(
            edges,
            atom_ids=snapshot["manifest"]["atoms"],
            source_event_ids=snapshot["manifest"]["sources"],
            slot_ids=snapshot["manifest"]["unit"]["query_generation"],
        )
        result["edges"] = sorted(set(edges))
        result["manifest_sha256"] = digest(snapshot["manifest"])
        return result

    async def publish(self, task, snapshot, prepared):
        # Deterministic recomputation is an output-validation boundary, not a confidence threshold.
        if prepared != self.prepare(snapshot):
            raise DerivedError("derived_output_invalid")
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope)
            from ..operations.facet_refresh import checked_job

            job = await checked_job(self, uow, task)
            definition = await self._check_unit(uow, task.payload["unit"])
            _, authority = await self.registry.bindings(uow, definition["spec"])
            if snapshot.get("authority") != authority:
                raise DerivedError("derived_safety_changed")
            now = self.clock()
            headers = await uow.derived_candidates(self.scope, definition["slots"])
            if len(headers) > 64:
                raise DerivedError("derived_snapshot_capacity")
            # Re-authorize the actual census, not a caller-supplied subset of a manifest.
            actual_sources = {key for h in headers for key in h["source_ids"]}
            if len(actual_sources) + len(headers) > 128:
                raise DerivedError("derived_input_capacity")
            for key in sorted(actual_sources):
                grant = await uow.derived_get(self.scope, "grant", key)
                self._permission(
                    grant,
                    definition["spec"]["readers"],
                    definition["spec"]["purpose"],
                    now,
                    authority,
                )
            if now < snapshot["at"] or (
                prepared["next_transition_at"]
                and datetime.fromisoformat(prepared["next_transition_at"]) <= now
            ):
                raise DerivedError("derived_time_coverage_expired")
            if (
                await uow.derived_get(self.scope, "head", definition["facet_id"])
                != snapshot["expected_head"]
            ):
                raise DerivedError("derived_head_conflict")
            for key, version in snapshot["manifest"]["atoms"].items():
                row = await uow.get_admission_record(self.scope, key)
                if row is None or row["version"] != version or row["payload"].get("deleted"):
                    raise DerivedError("derived_snapshot_changed")
            for key, data in snapshot["manifest"]["sources"].items():
                grant = await uow.derived_get(self.scope, "grant", key)
                self._permission(
                    grant,
                    definition["spec"]["readers"],
                    definition["spec"]["purpose"],
                    now,
                    authority,
                )
                if grant["version"] != data["grant_version"]:
                    raise DerivedError("derived_safety_changed")
                source = await uow.get_source_event(self.scope, key)
                head = await uow.retention_head_get(self.scope, "interpretation", key)
                if (
                    source is None
                    or source.content_hash != data["hash"]
                    or await source_document(uow, source) != data["document"]
                    or (
                        not (
                            data["interpretation"].get("inactive")
                            or data["interpretation"].get("context")
                        )
                        and head != data["interpretation"]
                    )
                ):
                    raise DerivedError("derived_source_changed")
            manifest = snapshot["manifest"]
            if (
                set(manifest)
                != {
                    "schema",
                    "unit",
                    "policy_sha256",
                    "template",
                    "atoms",
                    "sources",
                    "query_complete",
                    "authorization",
                }
                or manifest["schema"] != "derived-input-manifest/1"
            ):
                raise DerivedError("derived_manifest_invalid")
            if len(canonical_json(manifest).encode()) > 262144:
                raise DerivedError("derived_manifest_capacity")
            if (
                len(headers) > 64
                or {h["id"]: h["version"] for h in headers} != manifest["atoms"]
                or ({key for h in headers for key in h["source_ids"]} != set(manifest["sources"]))
            ):
                raise DerivedError("derived_query_coverage_incomplete")
            if (
                manifest["unit"] != task.payload["unit"]
                or manifest["policy_sha256"] != digest(self.policy)
                or (
                    manifest["template"] != definition["spec"]["template_version"]
                    or not manifest["query_complete"]
                    or snapshot["definition"]["spec"] != definition["spec"]
                    or digest(manifest) != prepared["manifest_sha256"]
                )
            ):
                raise DerivedError("derived_manifest_invalid")
            for input_row in snapshot["records"]:
                actual = await uow.get_admission_record(self.scope, input_row["id"])
                if actual != input_row:
                    raise DerivedError("derived_input_changed")
            if {r["id"] for r in snapshot["records"]} != set(manifest["atoms"]):
                raise DerivedError("derived_query_coverage_incomplete")
            for key in manifest["sources"]:
                source = await uow.get_source_event(self.scope, key)
                grant = await uow.derived_get(self.scope, "grant", key)
                if source != snapshot["sources"][key] or grant != snapshot["grants"][key]:
                    raise DerivedError("derived_input_changed")
            if manifest["authorization"] != authorization_summary(
                definition["spec"], snapshot["sources"], snapshot["grants"]
            ):
                raise DerivedError("derived_manifest_invalid")
            facet_id = definition["facet_id"]
            all_revisions = await uow.derived_records(self.scope, "revision")
            if len(all_revisions) >= 4096:
                raise DerivedError("derived_revision_capacity")
            revisions = [r for r in all_revisions if r["payload"]["facet_id"] == facet_id]
            if len(revisions) >= 128:
                raise DerivedError("derived_revision_capacity")
            old = snapshot["expected_head"]
            previous = (
                await uow.derived_get(self.scope, "revision", old["revision_id"])
                if old and old.get("revision_id")
                else None
            )
            outcome = "applied"
            revision_id = None
            if not prepared["no_outputs"]:
                revision_id = "observation:" + digest(
                    [
                        self.scope.partition_key(),
                        task.id,
                        prepared["manifest_sha256"],
                        prepared["body_sha256"],
                    ]
                )
                if (
                    previous
                    and previous.get("body_sha256") == prepared["body_sha256"]
                    and digest(previous.get("body")) == previous["body_sha256"]
                    and digest(previous["manifest"]) == previous["manifest_sha256"]
                ):
                    before_manifest = deepcopy(previous["manifest"])
                    after_manifest = deepcopy(snapshot["manifest"])
                    before_manifest["unit"].pop("time_generation", None)
                    after_manifest["unit"].pop("time_generation", None)
                    if before_manifest == after_manifest:
                        outcome = "noop"
                if previous is None or revision_id != previous["id"]:
                    revision = dict(
                        id=revision_id,
                        facet_id=facet_id,
                        state="ready",
                        body=prepared["body"],
                        body_sha256=prepared["body_sha256"],
                        manifest=snapshot["manifest"],
                        manifest_sha256=prepared["manifest_sha256"],
                        unit=task.payload["unit"],
                        parents=sorted({parent for _, parent in prepared["edges"]}),
                        built_at=snapshot["at"].isoformat(),
                        next_transition_at=prepared["next_transition_at"],
                    )
                    await uow.derived_put(self.scope, "revision", revision_id, revision)
                    await uow.derived_edges(self.scope, revision_id, prepared["edges"])
            audit_revision_id = revision_id
            if prepared["no_outputs"]:
                audit_revision_id = "observation-empty:" + digest(
                    [self.scope.partition_key(), task.id, prepared["manifest_sha256"]]
                )
                await uow.derived_put(
                    self.scope,
                    "revision",
                    audit_revision_id,
                    dict(
                        id=audit_revision_id,
                        facet_id=facet_id,
                        state="empty",
                        manifest=snapshot["manifest"],
                        manifest_sha256=prepared["manifest_sha256"],
                        unit=task.payload["unit"],
                        parents=sorted({parent for _, parent in prepared["edges"]}),
                        built_at=snapshot["at"].isoformat(),
                        next_transition_at=prepared["next_transition_at"],
                    ),
                )
                await uow.derived_edges(self.scope, audit_revision_id, prepared["edges"])
            await uow.derived_put(
                self.scope,
                "head",
                facet_id,
                dict(
                    facet_id=facet_id,
                    revision_id=revision_id,
                    audit_revision_id=audit_revision_id,
                    state="empty" if prepared["no_outputs"] else "ready",
                    unit=task.payload["unit"],
                    next_transition_at=prepared["next_transition_at"],
                ),
            )
            definition.update(next_transition_at=prepared["next_transition_at"])
            await uow.derived_put(self.scope, "definition", facet_id, definition)
            token = "derived-commit:" + digest(
                [self.scope.partition_key(), job["unit"], revision_id, outcome]
            )
            job.update(
                status="completed",
                outcome=outcome,
                no_outputs=prepared["no_outputs"],
                revision_id=revision_id,
                commit_token=token,
                completed_at=now.isoformat(),
            )
            await uow.derived_put(self.scope, "job", task.id, job)
            return dict(
                outcome=outcome,
                no_outputs=prepared["no_outputs"],
                revision_id=revision_id,
                commit_token=token,
            )

    async def apply(self, task, checkpoint=None):
        snapshot = await self.snapshot(task)
        return await self.publish(task, snapshot, self.prepare(snapshot))

    async def _read(self, uow, facet_id, actor, purpose):
        definition = await self._definition(uow, facet_id)
        _, authority = await self.registry.bindings(uow, definition["spec"])
        if actor not in definition["spec"]["readers"] or purpose != definition["spec"]["purpose"]:
            raise DerivedError("derived_read_denied")
        binding = self._context_binding(definition["spec"])
        if binding is not None:
            try:
                binding.current(self.clock())
            except DerivedError as error:
                return dict(facet_id=facet_id, state="invalid", body=None, reason=error.code)
        head = await uow.derived_get(self.scope, "head", facet_id)
        if head is None:
            return dict(facet_id=facet_id, state="stale", body=None, reason="not_built")
        if head.get("state") == "erased":
            return dict(facet_id=facet_id, state="erased", body=None)
        if head.get("unit", {}).get("safety_generation") != definition["safety_generation"]:
            return dict(facet_id=facet_id, state="invalid", body=None, reason="safety_changed")
        if head.get("unit") != (await self._unit(uow, definition)).payload():
            return dict(facet_id=facet_id, state="stale", body=None, reason="generation_changed")
        now = self.clock()
        if (
            head.get("next_transition_at")
            and datetime.fromisoformat(head["next_transition_at"]) <= now
        ):
            return dict(facet_id=facet_id, state="stale", body=None, reason="time_coverage_expired")
        if not head.get("revision_id"):
            return dict(facet_id=facet_id, state="empty", body=None)
        revision = await uow.derived_get(self.scope, "revision", head["revision_id"])
        if not revision or revision["state"] != "ready":
            return dict(facet_id=facet_id, state="invalid", body=None)
        if (
            digest(revision["body"]) != revision["body_sha256"]
            or digest(revision["manifest"]) != revision["manifest_sha256"]
            or revision["unit"] != head["unit"]
        ):
            return dict(
                facet_id=facet_id, state="invalid", body=None, reason="revision_integrity_failed"
            )
        if datetime.fromisoformat(revision["built_at"]) > now:
            return dict(facet_id=facet_id, state="invalid", body=None, reason="future_knowledge")
        for key, data in revision["manifest"]["sources"].items():
            grant = await uow.derived_get(self.scope, "grant", key)
            self._permission(grant, (actor,), purpose, now, authority)
            if grant["version"] != data["grant_version"]:
                return dict(facet_id=facet_id, state="invalid", body=None, reason="safety_changed")
        for key, data in revision["manifest"]["sources"].items():
            source = await uow.get_source_event(self.scope, key)
            head_source = await uow.retention_head_get(self.scope, "interpretation", key)
            if (
                source is None
                or source.content_hash != data["hash"]
                or await source_document(uow, source) != data["document"]
                or (
                    not (
                        data["interpretation"].get("inactive")
                        or data["interpretation"].get("context")
                    )
                    and head_source != data["interpretation"]
                )
            ):
                return dict(facet_id=facet_id, state="invalid", body=None, reason="source_changed")
        for key, version in revision["manifest"]["atoms"].items():
            row = await uow.get_admission_record(self.scope, key)
            if row is None or row["version"] != version or row["payload"].get("deleted"):
                return dict(facet_id=facet_id, state="invalid", body=None, reason="atom_changed")
        return dict(
            facet_id=facet_id,
            state="ready",
            revision_id=revision["id"],
            body=deepcopy(revision["body"]),
        )

    async def read(self, facet_id, *, actor, purpose="agent_context", known_at=None, valid_at=None):
        if known_at is not None or valid_at is not None:
            raise DerivedError("derived_history_unsupported")
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope)
            return await self._read(uow, facet_id, actor, purpose)

    async def call(self, operation, payload, context):
        if not isinstance(operation, str):
            raise DerivedError("invalid_derived_request")
        if context.scope != self.scope:
            raise DerivedError("derived_scope_mismatch")
        if not isinstance(payload, dict) or set(payload) - {
            "facet_id",
            "purpose",
            "valid_at",
            "known_at",
            "target_id",
        }:
            raise DerivedError("invalid_derived_request")
        if operation == "capabilities":
            async with self.repository.unit_of_work() as uow:
                await open_derived(uow, self.scope)
            return dict(
                schema="derived-capabilities/1",
                readonly=True,
                facets=["communication.language"],
                operations=["capabilities", "read", "status", "derived_context"],
                historical=False,
                query_definitions="derived-query/1",
                query_membership="all_candidates",
                grant_authority="host-grant-authority/1" if self.authority_id else None,
                authority_restore_pin="host_min_version" if self.authority_id else None,
                remote_acl=False,
                qualified_inputs=self.context_token is not None,
                qualified_templates=["locale-context/1"] if self.context_token is not None else [],
                context_attributes=["project", "holiday"] if self.context_token is not None else [],
                derived_parents=False,
                renderer="deterministic_full_snapshot",
            )
        if operation in {"read", "derived_context"}:
            result = await self.read(
                payload.get("facet_id"),
                actor=context.actor,
                purpose=payload.get("purpose", "agent_context"),
                known_at=payload.get("known_at"),
                valid_at=payload.get("valid_at"),
            )
            if operation == "derived_context":
                # Read again at the final delivery boundary; callers may not use a cached view.
                result = await self.read(
                    payload.get("facet_id"),
                    actor=context.actor,
                    purpose=payload.get("purpose", "agent_context"),
                )
                return dict(
                    schema="derived-context/1",
                    state=result["state"],
                    observations=[result] if result["state"] == "ready" else [],
                )
            return result
        if operation == "status":
            from ..operations.facet_refresh import FacetRefreshQueue

            return await FacetRefreshQueue(self).status(
                payload.get("target_id"), actor=context.actor
            )
        raise DerivedError("unsupported_derived_operation")
