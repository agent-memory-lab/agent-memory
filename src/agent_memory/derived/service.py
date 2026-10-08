"""Host-owned derived lifecycle: authorize snapshot, prepare, CAS publish, guarded read."""

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime

from ..domain import canonical_json, utc_now
from ..operations.retention import RetentionError
from ..operations.source_revisions import source_is_current
from . import subscriptions
from .contracts import HistoricalQuery
from .coverage import close_coverage
from .history import PublishedHistory
from .model import (
    HISTORY_INTERVAL,
    HISTORY_MODES,
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
from .pages import KnowledgePages, compose_page, is_page
from .parents import (
    ParentGraph,
    compose_parents,
    invalidate_descendants,
    revision_header,
    supported,
)
from .qualified import (
    QUALIFIED_PARENT_TEMPLATE,
    ROUTE_CONTRACT,
    manifest_fields,
    manifest_schema,
    qualified_supported,
    validate_header_route,
    validate_manifest_route,
)
from .registry import DerivedRegistry, expected, slots


async def open_derived(uow, scope, history_mode=None):
    required = (
        "derived_get",
        "derived_put",
        "derived_records",
        "derived_candidates",
        "derived_edges",
        "derived_reverse",
        "derived_header",
        "derived_headers",
        "lock_admission_scope",
        "retention_epoch",
        "get_source_event",
        "get_admission_record",
    )
    if any(not callable(getattr(uow, name, None)) for name in required):
        raise DerivedError("derived_backend_unsupported")
    if history_mode == HISTORY_INTERVAL and (
        getattr(uow, "derived_coverage_contract", None) != "write-hooks/1"
    ):
        raise DerivedError("derived_history_coverage_backend_unsupported")
    await uow.lock_admission_scope(scope)
    await subscriptions.ensure_index(uow, scope)
    return await uow.retention_epoch(scope)


async def mark_slot_changed(
    uow, scope, key, *, at=None, reason="candidate", old_header=None, new_header=None
):
    """Always maintained, including slots with no registered subscribers yet."""
    old_header, new_header = deepcopy((old_header, new_header))
    await subscriptions.ensure_index(uow, scope)
    await subscriptions.bump(uow, scope, subscriptions.SCOPE_BARRIER)
    keys = {key}
    keys.update(header["slot_key"] for header in (old_header, new_header) if header)
    for changed in sorted(keys):
        await subscriptions.bump(uow, scope, changed)
    await subscriptions.invalidate(
        uow, scope, tuple(subscriptions.slot_key(changed) for changed in sorted(keys)),
        at=at, reason=reason,
    )

    from . import project_index

    await project_index.changed(uow, scope, old_header, new_header, at=at, reason=reason)


async def interpretation_changed(uow, scope, source_id, *, at=None):
    # Source routes include pending/rejected candidates, not merely cited inputs.
    keys = await subscriptions.source_slots(uow, scope, source_id)
    if keys is None:
        await subscriptions.bump(uow, scope, subscriptions.SCOPE_BARRIER)
        await subscriptions.scope_fallback(uow, scope, at=at, reason="interpretation")
        return
    for key in sorted(keys):
        await mark_slot_changed(uow, scope, key, at=at, reason="interpretation")
    if not keys:
        await subscriptions.bump(uow, scope, subscriptions.SCOPE_BARRIER)
    from . import project_index

    await project_index.source_changed(uow, scope, source_id, at=at, reason="interpretation")


async def document_changed(uow, scope, *, at):
    # A document head can change interpretation before any new candidate exists.
    # Exact document memberships are outside this admitted-L1 slice; the bounded,
    # indexed scope subscription is deliberately conservative and metered.
    await subscriptions.ensure_index(uow, scope)
    await subscriptions.bump(uow, scope, subscriptions.SCOPE_BARRIER)
    await subscriptions.scope_fallback(uow, scope, at=at, reason="document")


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
        history_mode=None,
        qualified_current=False,
    ):
        self.repository, self.scope, self.clock = repository, scope, clock
        self.policy = policy.config_payload()
        if type(qualified_current) is not bool or (
            qualified_current and (context_token is None or history_mode is not None)
        ):
            raise DerivedError("derived_qualified_context_unsupported")
        self.qualified_current = qualified_current
        self.context_token = identity(context_token) if context_token is not None else None
        self.authority_id = identity(authority_id) if authority_id is not None else None
        if self.authority_id is not None and (
            type(authority_min_version) is not int or authority_min_version < 0
        ):
            raise DerivedError("trusted_authority_version_required")
        if self.authority_id is None and authority_min_version is not None:
            raise DerivedError("derived_authority_mismatch")
        self.authority_min_version = authority_min_version
        if history_mode is not None and history_mode not in HISTORY_MODES:
            raise DerivedError("unsupported_derived_history_mode")
        if history_mode is not None and self.authority_id is None:
            raise DerivedError("trusted_history_authority_required")
        self.history_mode = history_mode
        self.history = PublishedHistory(self)
        self.registry = DerivedRegistry(self)
        self.parents = ParentGraph(self)
        self.pages = KnowledgePages(self)
        predicates = {s["predicate"]: s for s in self.policy["predicates"]}
        if "locale" not in predicates or predicates["locale"]["value_type"] != "string":
            raise DerivedError("derived_predicate_unregistered")

    async def register_query(self, definition, *, expected_generation=0):
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope, self.history_mode)
            return await self.registry.register_query(uow, epoch, definition, expected_generation)

    async def set_authority(self, authority, *, expected_version=0):
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope, self.history_mode)
            row = await self.registry.set_authority(uow, epoch, authority, expected_version)
        # Advance the host floor only after a successful commit. Persist this floor
        # outside restored database snapshots before using the new ACL revision.
        self.authority_min_version = max(self.authority_min_version, row["version"])
        return row

    async def register(self, definition, *, expected_generation=0):
        if not isinstance(definition, FacetDefinition):
            raise TypeError("trusted FacetDefinition required")
        if definition.context is not None:
            definition.context.current(self.clock())
        return await self._register_spec(
            definition.payload(), expected_generation=expected_generation,
            definition_slots=([] if definition.parent_facets else
                              slots(self.scope, definition.subject_id, definition.predicates)),
        )

    async def register_page(self, definition, *, expected_generation=0):
        return await self.pages.register(definition, expected_generation=expected_generation)

    async def _register_spec(self, spec, *, expected_generation, definition_slots):
        expected(expected_generation)
        if spec["subject_id"] != self.scope.user_id:
            raise DerivedError("derived_subject_scope_mismatch")
        if spec.get("history_mode") != self.history_mode:
            raise DerivedError("derived_history_configuration_mismatch")
        self._context_binding(spec)
        fingerprint = self._definition_fingerprint(spec)
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope, self.history_mode)
            if self.qualified_current and not qualified_supported(uow):
                raise DerivedError("derived_qualified_backend_unsupported")
            await self.pages.check(uow, spec)
            await self.registry.bindings(uow, spec)
            await self.parents.validate_registration(uow, spec)
            old = await uow.derived_get(self.scope, "definition", spec["id"])
            if old and not old.get("disabled") and old.get("refresh_managed") and (
                old.get("qualified_current", False) != self.qualified_current
            ):
                # B2 binds a managed facet to one immutable processor identity.
                # A new proof mode needs a new host facet, not a stranded policy.
                raise DerivedError("derived_definition_configuration_changed")
            if old and not old.get("disabled") and old.get("qualified_current") and (
                not self.qualified_current
            ):
                raise DerivedError("derived_definition_configuration_changed")
            if old and is_page(old["spec"]) != is_page(spec):
                raise DerivedError("derived_resource_kind_conflict")
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
                facet_id=spec["id"], slots=definition_slots,
                fingerprint=fingerprint,
                generation=(old["generation"] if old else 0) + 1,
                epoch=epoch,
                safety_generation=(old["safety_generation"] if old else 0),
                time_generation=0,
                dirty=True,
                disabled=False,
            )
            if self.qualified_current:
                row["qualified_current"] = True
            if old and old.get("refresh_managed"):
                row["refresh_managed"] = True
            if old is not None:
                await close_coverage(
                    uow, self.scope, spec["id"], at=self.clock(), reason="definition"
                )
            await uow.derived_put(self.scope, "definition", spec["id"], row)
            await subscriptions.install(uow, self.scope, row)
            from ..operations.refresh_demand import record_dirty

            await record_dirty(uow, self.scope, row, at=self.clock(), reason="definition")
            await invalidate_descendants(uow, self.scope, (spec["id"],), at=self.clock())
            return deepcopy(row)

    async def grant(self, grant, *, expected_version=0):
        if not isinstance(grant, ProcessingGrant):
            raise TypeError("trusted ProcessingGrant required")
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope, self.history_mode)
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
            keys = await subscriptions.source_slots(uow, self.scope, grant.source_id)
            await subscriptions.bump(uow, self.scope, subscriptions.SCOPE_BARRIER)
            if keys is None:
                await subscriptions.scope_fallback(
                    uow, self.scope, at=self.clock(), reason="grant", safety=True
                )
            else:
                await subscriptions.invalidate(
                    uow, self.scope, tuple(subscriptions.slot_key(key) for key in keys),
                    at=self.clock(), reason="grant", safety=True,
                )
                from . import project_index

                await project_index.source_changed(
                    uow, self.scope, grant.source_id, at=self.clock(), reason="grant", safety=True
                )
            return deepcopy(row)

    async def _unit(self, uow, row):
        subscription = await subscriptions.subscription(uow, self.scope, row)
        epoch = await uow.retention_epoch(self.scope)
        bindings, _ = await self.registry.bindings(uow, row["spec"])
        query = {
            key: (await uow.derived_get(self.scope, "barrier", key) or {"generation": 0})[
                "generation"
            ]
            for key in (*row["slots"], subscriptions.FALLBACK_BARRIER)
        }
        query[subscriptions.SUBSCRIPTION_BARRIER] = subscription["generation"]
        parents = await self.parents.bindings(uow, row["spec"])
        return FacetRefreshUnit(
            row["facet_id"],
            row["fingerprint"],
            row["generation"],
            epoch,
            query,
            row["safety_generation"],
            row["time_generation"],
            schema=("facet-refresh-unit/3" if parents is not None else
                    "facet-refresh-unit/2" if bindings is not None else "facet-refresh-unit/1"),
            bindings=bindings,
            parents=parents,
        )

    async def _candidates(self, uow, definition):
        return (await uow.derived_candidates(self.scope, definition["slots"])
                if definition["slots"] else ())

    async def _definition(self, uow, facet_id):
        row = await uow.derived_get(self.scope, "definition", identity(facet_id))
        if (
            row is None
            or row.get("disabled")
            or row["epoch"] != await uow.retention_epoch(self.scope)
        ):
            raise DerivedError("derived_definition_unavailable")
        if (self._definition_fingerprint(row["spec"]) != row["fingerprint"] or
            row.get("qualified_current", False) != self.qualified_current):
            raise DerivedError("derived_definition_configuration_changed")
        if self.qualified_current and not qualified_supported(uow):
            raise DerivedError("derived_qualified_backend_unsupported")
        await self.pages.check(uow, row["spec"])
        self.parents.check_qualified(uow, row["spec"])
        self._context_binding(row["spec"])
        if row["spec"].get("history_mode") != self.history_mode:
            raise DerivedError("derived_history_configuration_mismatch")
        await self.registry.bindings(uow, row["spec"])
        return row

    def accepts_definition(self, row):
        binding = row["spec"].get("context")
        return (
            row["spec"].get("authority_id") == self.authority_id
            and (binding is None or binding["query"]["snapshot_token"] == self.context_token)
            and row["spec"].get("history_mode") == self.history_mode
            and row.get("qualified_current", False) == self.qualified_current
        )

    def _definition_fingerprint(self, spec):
        configuration = dict(definition=spec, policy=self.policy)
        if self.qualified_current:
            configuration["qualified_current"] = ROUTE_CONTRACT
        return digest(configuration)

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
        task = deepcopy(task)
        await self._read_preflight(task.payload["unit"]["facet_id"])
        try:
            return await self._snapshot(task)
        except Exception:
            # The failed body transaction cannot preserve an observed expiry.
            await self._read_preflight(task.payload["unit"]["facet_id"])
            raise

    async def _snapshot(self, task):
        timestamp(self.clock())
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope, self.history_mode)
            from ..operations.facet_refresh import checked_job

            job = await checked_job(self, uow, task)
            guarded_at = None
            if job.get("refresh_execution"):
                from ..operations.refresh_demand import observed_clock

                guarded_at = await observed_clock(uow, self.scope, self.clock)
            unit = task.payload["unit"]
            definition = await self._check_unit(uow, unit)
            at = self._checked_clock(guarded_at)
            _, authority = await self.registry.bindings(uow, definition["spec"])
            binding = self._context_binding(definition["spec"])
            if binding is not None:
                binding.current(at)  # Expired routing must not fetch source bodies.
            parents, parent_headers = await self.parents.inputs(
                uow, definition["spec"], unit.get("parents"), at
            )
            headers = await self._candidates(uow, definition)
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
            security_at = at
            for key in ids:
                security_at = self._checked_clock(security_at)
                if binding is not None:
                    binding.current(security_at)
                if authority is not None and (
                    datetime.fromisoformat(authority["spec"]["expires_at"]) <= security_at
                ):
                    raise DerivedError("derived_authority_expired")
                self._permission(grants[key], definition["spec"]["readers"],
                                 definition["spec"]["purpose"], security_at, authority)
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
            if parents:
                manifest.update(
                    schema="derived-input-manifest/2", parents=unit["parents"],
                    lineage={key: dict(revision_id=h["id"], sha256=digest(h))
                             for key, h in parent_headers.items()},
                )
                manifest["authorization"] = self._parent_authorization(
                    definition["spec"], parent_headers
                )
            if self.qualified_current:
                manifest.update(manifest_fields(definition["spec"]))
                manifest["schema"] = manifest_schema(definition["spec"], bool(parents))
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
                **(dict(parents=parents, parent_headers=parent_headers) if parents else {}),
            )

    @staticmethod
    def _parent_authorization(spec, headers):
        summaries = [h["manifest"]["authorization"] for h in headers.values()]
        sensitivity = {"public": 0, "internal": 1, "private": 2, "restricted": 3}
        retention = {"ephemeral": 0, "session": 1, "standard": 2, "persistent": 3}
        return dict(
            readers=spec["readers"], purpose=spec["purpose"],
            sensitivity=max((h["sensitivity"] for h in summaries), key=sensitivity.get),
            retention_class=min((h["retention_class"] for h in summaries), key=retention.get),
        )

    def prepare(self, snapshot):
        spec = snapshot["definition"]["spec"]
        if is_page(spec):
            result = compose_page(snapshot, self.scope)
        elif spec.get("parent_facets"):
            result = compose_parents(spec, snapshot["parents"])
        else:
            result = compose(
                spec, snapshot["records"], snapshot["sources"], snapshot["at"],
                admission_policy=self.policy,
            )
        expiries = [g["expires_at"] for g in snapshot["grants"].values() if g.get("expires_at")]
        if snapshot.get("authority") is not None:
            expiries.append(snapshot["authority"]["spec"]["expires_at"])
        if self.qualified_current and spec.get("context"):
            expiries.append(spec["context"]["expires_at"])
        transitions = [v for v in [result["next_transition_at"], *expiries] if v]
        result["next_transition_at"] = (
            min(transitions, key=datetime.fromisoformat) if transitions else None
        )
        edges = result["support"] + [("processing", "atom:" + r["id"]) for r in snapshot["records"]]
        edges += [("processing", "source:" + key) for key in snapshot["sources"]]
        edges += [("processing", "derived:" + parent["id"])
                  for parent in snapshot.get("parents", {}).values()]
        edges += [
            ("query", "facet:" + key) for key in snapshot["definition"]["slots"]
        ]
        if len(edges) > 512:
            raise DerivedError("derived_dependency_capacity")
        validate_edges(
            edges,
            atom_ids=snapshot["manifest"]["atoms"],
            source_event_ids=snapshot["manifest"]["sources"],
            slot_ids=snapshot["definition"]["slots"],
            derived_ids=[p["id"] for p in snapshot.get("parents", {}).values()],
        )
        result["edges"] = sorted(set(edges))
        result["manifest_sha256"] = digest(snapshot["manifest"])
        return result

    async def publish(self, task, snapshot, prepared):
        # Own all caller data before preflight can yield to another coroutine.
        task, snapshot, prepared = deepcopy((task, snapshot, prepared))
        await self._read_preflight(task.payload["unit"]["facet_id"])
        try:
            return await self._publish(task, snapshot, prepared)
        except Exception:
            await self._read_preflight(task.payload["unit"]["facet_id"])
            raise

    async def _publish(self, task, snapshot, prepared):
        # Deterministic recomputation is an output-validation boundary, not a confidence threshold.
        if prepared != self.prepare(snapshot):
            raise DerivedError("derived_output_invalid")
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope, self.history_mode)
            from ..operations.facet_refresh import checked_job

            job = await checked_job(self, uow, task)
            guarded_at = None
            if job.get("refresh_execution"):
                from ..operations.refresh_demand import observed_clock

                guarded_at = await observed_clock(uow, self.scope, self.clock)
            definition = await self._check_unit(uow, task.payload["unit"])
            _, authority = await self.registry.bindings(uow, definition["spec"])
            if snapshot.get("authority") != authority:
                raise DerivedError("derived_safety_changed")
            now = self._checked_clock(guarded_at)
            route_fields = manifest_fields(definition["spec"]) if self.qualified_current else {}
            if route_fields:
                validate_manifest_route(definition["spec"], snapshot["manifest"], now)
            parents, parent_headers = await self.parents.inputs(
                uow, definition["spec"], task.payload["unit"].get("parents"), now
            )
            if parents != snapshot.get("parents", {}) or (
                parent_headers != snapshot.get("parent_headers", {})
            ):
                raise DerivedError("derived_input_changed")
            headers = await self._candidates(uow, definition)
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
                set(manifest) != ({
                    "schema",
                    "unit",
                    "policy_sha256",
                    "template",
                    "atoms",
                    "sources",
                    "query_complete",
                    "authorization",
                } | ({"parents", "lineage"} if parents else set()) | set(route_fields))
                or manifest["schema"] != (
                    manifest_schema(definition["spec"], bool(parents)) if self.qualified_current
                    else "derived-input-manifest/2" if parents else "derived-input-manifest/1"
                )
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
            if parents and (manifest["parents"] != task.payload["unit"]["parents"] or
                manifest["lineage"] != {key: dict(revision_id=h["id"], sha256=digest(h))
                                        for key, h in parent_headers.items()}):
                raise DerivedError("derived_manifest_invalid")
            authorization = self._parent_authorization(definition["spec"], parent_headers) if (
                parents
            ) else authorization_summary(
                definition["spec"], snapshot["sources"], snapshot["grants"]
            )
            if manifest["authorization"] != authorization:
                raise DerivedError("derived_manifest_invalid")
            archive = await self.history.archive(uow, snapshot, definition=definition, known_at=now)
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
                prefix = "page-version:" if is_page(definition["spec"]) else "observation:"
                revision_id = prefix + digest(
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
                    if archive is not None:
                        revision.update(history=archive, history_sha256=digest(archive))
                    await uow.derived_put(self.scope, "revision", revision_id, revision)
                    await uow.derived_edges(self.scope, revision_id, prepared["edges"])
            audit_revision_id = revision_id
            if prepared["no_outputs"]:
                prefix = "page-empty:" if is_page(definition["spec"]) else "observation-empty:"
                audit_revision_id = prefix + digest(
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
                        **(
                            dict(history=archive, history_sha256=digest(archive)) if archive else {}
                        ),
                    ),
                )
                await uow.derived_edges(self.scope, audit_revision_id, prepared["edges"])
            await self.pages.persist(uow, snapshot, prepared, audit_revision_id)
            await self.history.persist(uow, task, snapshot, archive, audit_revision_id)
            header_proof = {}
            if supported(uow):
                input_header = revision_header(
                    await uow.derived_get(self.scope, "revision", audit_revision_id)
                )
                await uow.derived_put(
                    self.scope, "revision_header", audit_revision_id, input_header
                )
                header_proof["input_header_sha256"] = digest(input_header)
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
                    **header_proof,
                ),
            )
            definition.update(next_transition_at=prepared["next_transition_at"])
            await uow.derived_put(self.scope, "definition", facet_id, definition)
            await invalidate_descendants(uow, self.scope, (facet_id,), at=now)
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
            from ..operations.refresh_demand import publish_coverage

            await publish_coverage(
                uow, self, job, definition, manifest=manifest, now=now
            )
            if definition.get("qualified_current"):
                # A lease, route or authority can expire during storage awaits.
                # Check before leaving the UoW so blocks/head/receipts roll back
                # together instead of certifying an already unusable result.
                await self._qualified_delivery_guard(uow, definition)
                finished_at = self.clock()
                if any(
                    finished_at >= datetime.fromisoformat(job[key])
                    for key in ("lease_until", "expires_at") if job.get(key)
                ):
                    from ..operations.facet_refresh import stale

                    raise stale()
            return dict(
                outcome=outcome,
                no_outputs=prepared["no_outputs"],
                revision_id=revision_id,
                commit_token=token,
            )

    async def apply(self, task, checkpoint=None):
        snapshot = await self.snapshot(task)
        return await self.publish(task, snapshot, self.prepare(snapshot))

    async def _qualified_clock_barrier(self, *, observed_at=None):
        from ..operations.refresh_demand import observed_clock

        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope, self.history_mode)
            return await observed_clock(
                uow, self.scope, self.clock if observed_at is None else lambda: observed_at
            )

    @asynccontextmanager
    async def _current_read_scope(self):
        # Persist the observed wall floor independently of a denied body read.
        # Otherwise a rollback would forget an expiry seen before a restart.
        observed = await self._qualified_clock_barrier() if self.qualified_current else None
        try:
            async with self.repository.unit_of_work() as uow:
                await open_derived(uow, self.scope, self.history_mode)
                yield uow
        except BaseException:
            if observed is not None:
                observed = max(observed, timestamp(self.clock()))
                try:
                    await self._qualified_clock_barrier(observed_at=observed)
                except DerivedError as error:
                    if error.code != "refresh_clock_discontinuity":
                        raise
            raise

    async def _qualified_delivery_guard(self, uow, definition):
        """Recheck dynamic host controls and time after the last body await.

        The scope lock keeps persisted ACLs/inputs fixed for this read, but does
        not freeze the clock or authenticated host configuration. Full rebuilds
        propagate every parent, grant and authority expiry into the head's next
        transition, so this final metadata-only check covers the whole graph.
        """
        if not definition.get("qualified_current"):
            return
        from ..operations.refresh_demand import observed_clock

        guarded_at = await observed_clock(uow, self.scope, self.clock)
        current = await self._definition(uow, definition["facet_id"])
        if current != definition:
            raise DerivedError("derived_snapshot_changed")
        head = await uow.derived_get(self.scope, "head", definition["facet_id"])
        header = await uow.derived_get(
            self.scope, "revision_header", head.get("audit_revision_id") if head else None
        )
        if not header or digest(header) != head.get("input_header_sha256"):
            raise DerivedError("derived_parent_integrity_failed")
        at = self._checked_clock(guarded_at)
        # These checks are deliberately synchronous after the final metadata
        # await. Do not reopen a route that changed while the body was loading.
        if not self.qualified_current or (
            self._definition_fingerprint(definition["spec"]) != definition["fingerprint"]
        ):
            raise DerivedError("derived_definition_configuration_changed")
        binding = self._context_binding(definition["spec"])
        if binding is not None:
            binding.current(at)
        if definition["spec"].get("authority_id") != self.authority_id:
            raise DerivedError("derived_authority_mismatch")
        if self.authority_id is not None:
            self.registry._authority_floor(head["unit"]["bindings"]["authority"])
        if datetime.fromisoformat(header["built_at"]) > at:
            raise DerivedError("derived_future_knowledge")
        if header.get("next_transition_at") and (
            datetime.fromisoformat(header["next_transition_at"]) <= at
        ):
            raise DerivedError("derived_time_coverage_expired")

    def _checked_clock(self, minimum=None):
        at = timestamp(self.clock())
        if minimum is not None and at < minimum:
            raise DerivedError("refresh_clock_discontinuity")
        return at

    async def _managed_lineage(self, uow, facet_id):
        pending, seen = [facet_id], set()
        while pending:
            key = pending.pop()
            if key in seen:
                continue
            seen.add(key)
            if len(seen) > 32:
                raise DerivedError("derived_parent_capacity")
            definition = await uow.derived_get(self.scope, "definition", key)
            if definition and definition.get("refresh_managed"):
                return True
            if definition:
                pending.extend(definition.get("spec", {}).get("parent_facets", ()))
        return False

    async def _read_preflight(self, facet_id=None, *, target_id=None):
        # Persist observed time separately: authority/context/grant rejection in
        # the body transaction must not roll back an already observed expiry.
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope, self.history_mode)
            if target_id is not None:
                receipt = await uow.derived_get(self.scope, "request", identity(target_id))
                facet_id = (receipt or {}).get("facet_id")
            if facet_id is not None and await self._managed_lineage(uow, facet_id):
                from ..operations.refresh_demand import observed_clock

                await observed_clock(uow, self.scope, self.clock)

    async def _clock_delivery(self, uow, facet_id, result):
        definition = None
        if result["state"] in {"ready", "empty"}:
            try:
                definition = await self._definition(uow, facet_id)
            except DerivedError as error:
                return {**result, "state": "invalid", "body": None, "reason": error.code}
        if await self._managed_lineage(uow, facet_id):
            from ..operations.refresh_demand import observed_clock

            head = await uow.derived_get(self.scope, "head", facet_id)
            try:
                now = await observed_clock(uow, self.scope, self.clock)
            except DerivedError as error:
                if error.code != "refresh_clock_discontinuity":
                    raise
                return {**result, "state": "invalid", "body": None, "reason": error.code}
            if (result["state"] in {"ready", "empty"} and head
                    and head.get("next_transition_at")
                    and datetime.fromisoformat(head["next_transition_at"]) <= now):
                return {**result, "state": "stale", "body": None, "reason": "time_coverage_expired"}
        # The clock check itself awaits storage. Recheck qualified host routes
        # after it, including on legacy queues without refresh-managed ancestry.
        if definition is not None and definition.get("qualified_current"):
            try:
                await self._qualified_delivery_guard(uow, definition)
            except DerivedError as error:
                return {**result, "state": "invalid", "body": None, "reason": error.code}
        return result

    async def _read(self, uow, facet_id, actor, purpose, *, lineage_checked=False):
        definition = await self._definition(uow, facet_id)
        _, authority = await self.registry.bindings(uow, definition["spec"])
        if actor not in definition["spec"]["readers"] or purpose != definition["spec"]["purpose"]:
            raise DerivedError("derived_read_denied")
        guarded_at = None
        if await self._managed_lineage(uow, facet_id):
            from ..operations.refresh_demand import observed_clock

            try:
                guarded_at = await observed_clock(uow, self.scope, self.clock)
            except DerivedError as error:
                if error.code != "refresh_clock_discontinuity":
                    raise
                # This shared current-read path also guards parent and page reads.
                # A lower wall clock cannot revive an already observed expiry.
                return dict(facet_id=facet_id, state="invalid", body=None, reason=error.code)
        binding = self._context_binding(definition["spec"])
        if binding is not None:
            try:
                binding.current(self._checked_clock(guarded_at))
            except DerivedError as error:
                return dict(facet_id=facet_id, state="invalid", body=None, reason=error.code)
        head = await uow.derived_get(self.scope, "head", facet_id)
        if head is None:
            return dict(facet_id=facet_id, state="stale", body=None, reason="not_built")
        if head.get("state") == "erased":
            return dict(facet_id=facet_id, state="erased", body=None)
        if head.get("unit", {}).get("safety_generation") != definition["safety_generation"]:
            return dict(facet_id=facet_id, state="invalid", body=None, reason="safety_changed")
        input_header = None
        try:
            unit = (await self._unit(uow, definition)).payload()
            now = self._checked_clock(guarded_at)
            if head.get("next_transition_at") and (
                datetime.fromisoformat(head["next_transition_at"]) <= now
            ):
                return dict(facet_id=facet_id, state="stale", body=None,
                            reason="time_coverage_expired")
            if self.qualified_current and binding is not None:
                header = await uow.derived_get(
                    self.scope, "revision_header", head.get("audit_revision_id")
                )
                if not header or digest(header) != head.get("input_header_sha256") or (
                    header.get("unit") != head.get("unit")
                    or header.get("id") != head.get("audit_revision_id")
                    or header.get("facet_id") != facet_id
                    or digest(header.get("manifest")) != header.get("manifest_sha256")
                    or datetime.fromisoformat(header["built_at"]) > self.clock()
                ):
                    raise DerivedError("derived_parent_integrity_failed")
                validate_header_route(definition["spec"], header, self.clock())
                input_header = header
                for key, data in header["manifest"]["sources"].items():
                    grant = await uow.derived_get(self.scope, "grant", key)
                    self._permission(grant, (actor,), purpose, self.clock(), authority)
                    if grant["version"] != data["grant_version"]:
                        raise DerivedError("derived_safety_changed")
            if not lineage_checked:
                await self.parents.inputs(
                    uow, definition["spec"], unit.get("parents"), now, readers=(actor,)
                )
            now = (await observed_clock(uow, self.scope, self.clock)
                   if guarded_at is not None else self._checked_clock(now))
        except DerivedError as error:
            return dict(facet_id=facet_id, state="invalid", body=None, reason=error.code)
        if head.get("unit") != unit:
            return dict(facet_id=facet_id, state="stale", body=None, reason="generation_changed")
        if (
            head.get("next_transition_at")
            and datetime.fromisoformat(head["next_transition_at"]) <= now
        ):
            return dict(facet_id=facet_id, state="stale", body=None, reason="time_coverage_expired")
        if not head.get("revision_id") and not head.get("audit_revision_id"):
            return dict(facet_id=facet_id, state="empty", body=None)
        revision = await uow.derived_get(
            self.scope, "revision", head.get("revision_id") or head["audit_revision_id"]
        )
        if not revision or revision["state"] not in {"ready", "empty"}:
            return dict(facet_id=facet_id, state="invalid", body=None)
        if (
            (revision["state"] == "ready" and digest(revision["body"]) != revision["body_sha256"])
            or digest(revision["manifest"]) != revision["manifest_sha256"]
            or revision["unit"] != head["unit"]
            or (input_header is not None and revision_header(revision) != input_header)
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
        try:
            await self._qualified_delivery_guard(uow, definition)
        except DerivedError as error:
            return dict(facet_id=facet_id, state="invalid", body=None, reason=error.code)
        if revision["state"] == "empty":
            return await self._clock_delivery(
                uow, facet_id, dict(facet_id=facet_id, state="empty", body=None)
            )
        result = dict(
            facet_id=facet_id,
            state="ready",
            revision_id=revision["id"],
            body=deepcopy(revision["body"]),
            **(
                dict(known_at=revision["history"]["known_at"])
                if self.history_mode is not None
                else {}
            ),
        )
        return await self._clock_delivery(uow, facet_id, result)

    async def read(self, facet_id, *, actor, purpose="agent_context", known_at=None, valid_at=None):
        if known_at is not None or valid_at is not None:
            if self.history_mode is None:
                raise DerivedError("derived_history_unsupported")
            query = HistoricalQuery.parse(known_at, valid_at)
            async with self.repository.unit_of_work() as uow:
                await open_derived(uow, self.scope, self.history_mode)
                return await self.history.read(uow, facet_id, actor, purpose, query)
        await self._read_preflight(facet_id)
        try:
            async with self._current_read_scope() as uow:
                definition = await self._definition(uow, facet_id)
                if is_page(definition["spec"]):
                    raise DerivedError("derived_resource_kind_mismatch")
                result = await self._read(uow, facet_id, actor, purpose)
                from ..operations.refresh_demand import record_guarded_read

                await record_guarded_read(uow, self.scope, definition, result, at=self.clock())
                return await self._clock_delivery(uow, facet_id, result)
        except DerivedError:
            await self._read_preflight(facet_id)
            raise

    async def history_points(self, facet_id, *, actor, purpose="agent_context"):
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope, self.history_mode)
            return await self.history.points(uow, facet_id, actor, purpose)

    async def call(self, operation, payload, context):
        if not isinstance(operation, str):
            raise DerivedError("invalid_derived_request")
        if context.scope != self.scope:
            raise DerivedError("derived_scope_mismatch")
        if operation.startswith("page_"):
            return await self.pages.call(operation, payload, context)
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
                await open_derived(uow, self.scope, self.history_mode)
                parent_supported = supported(uow) and self.history_mode is None
                from .pages import page_supported

                qualified = (parent_supported and qualified_supported(uow)
                             and self.qualified_current)
                pages_enabled = (page_supported(uow) and self.history_mode is None
                                 and (self.context_token is None or qualified))
            return dict(
                schema="derived-capabilities/1",
                readonly=True,
                facets=["communication.language"],
                operations=["capabilities", "read", "status", "derived_context"]
                + (["history_points"] if self.history_mode else [])
                + (["page_capabilities", "page_read", "page_status", "page_context"]
                   if pages_enabled else []),
                pages=pages_enabled,
                historical=self.history_mode is not None,
                historical_mode=self.history_mode,
                historical_coverage=(
                    ["published_points", "certified_intervals"]
                    if self.history_mode == HISTORY_INTERVAL
                    else (["published_points"] if self.history_mode else [])
                ),
                historical_templates=(
                    ["locale-snapshot/1"]
                    + (["locale-context/1"] if self.context_token is not None else [])
                    if self.history_mode else []
                ),
                historical_context=(
                    "frozen-host-route/1"
                    if self.history_mode and self.context_token is not None else None
                ),
                query_definitions="derived-query/1",
                query_membership="all_candidates",
                grant_authority="host-grant-authority/1" if self.authority_id else None,
                authority_restore_pin="host_min_version" if self.authority_id else None,
                remote_acl=False,
                qualified_inputs=self.context_token is not None,
                qualified_templates=["locale-context/1"] if self.context_token is not None else [],
                context_attributes=["project", "holiday"] if self.context_token is not None else [],
                derived_parents=parent_supported,
                derived_parent_templates=(["locale-parents/1"] if parent_supported else [])
                + ([QUALIFIED_PARENT_TEMPLATE] if qualified else []),
                qualified_current=qualified,
                qualified_route_contract=ROUTE_CONTRACT if qualified else None,
                derived_parent_history=False,
                derived_parent_contract="processing-graph/1" if parent_supported else None,
                derived_parent_limits=dict(parents=4, depth=4, nodes=32, input_bytes=262144),
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
                    known_at=payload.get("known_at"),
                    valid_at=payload.get("valid_at"),
                )
                return dict(
                    schema="derived-context/1",
                    state=result["state"],
                    observations=[result] if result["state"] == "ready" else [],
                )
            return result
        if operation == "history_points" and self.history_mode is not None:
            return await self.history_points(
                payload.get("facet_id"),
                actor=context.actor,
                purpose=payload.get("purpose", "agent_context"),
            )
        if operation == "status":
            from ..operations.facet_refresh import FacetRefreshQueue

            return await FacetRefreshQueue(self).status(
                payload.get("target_id"), actor=context.actor
            )
        raise DerivedError("unsupported_derived_operation")
