"""Observation adapter for the backend-neutral durable refresh scheduler.

Only this adapter knows how a legacy Observation census is frozen and verified.
The scheduler receives an immutable normal job and exact responsibility proof.
"""

from datetime import datetime

from ..derived.model import DerivedError, digest
from .facet_refresh import FacetRefreshQueue, checked_job, valid_completion


def _parse(value):
    return datetime.fromisoformat(value)


class ObservationRefreshProcessor:
    """Full census adapter. Normal ObservationService remains the publish authority."""

    task_type = "memory.facet_refresh"

    def __init__(self, service):
        self.service, self.repository, self.scope = service, service.repository, service.scope
        self.queue = FacetRefreshQueue(service)
        self.key = "observation-refresh/1:" + digest(
            [
                self.scope.partition_key(),
                service.policy,
                service.context_token,
                service.authority_id,
                service.history_mode,
            ]
        )

    def instance_id(self, definition):
        return "question-instance:" + digest(
            [self.scope.partition_key(), "observation-adapter/1", definition["facet_id"]]
        )

    def target_metadata(self, definition):
        return dict(
            definition_fingerprint=definition["fingerprint"],
            context_fingerprint=digest(
                [
                    "observation-adapter/1",
                    self.key,
                    self.scope.partition_key(),
                    definition["spec"].get("context"),
                    definition["spec"]["readers"],
                ]
            ),
            request_semantics_digest=digest(
                [
                    "observation-adapter/1",
                    "full-current-census",
                    self.key,
                    definition["facet_id"],
                    definition["spec"]["purpose"],
                ]
            ),
        )

    async def definition(self, uow, facet_id):
        definition = await self.service._definition(uow, facet_id)
        if not self.service.accepts_definition(definition) or self.service.history_mode is not None:
            raise DerivedError("refresh_processor_unsupported")
        return definition

    async def freeze(self, uow, definition):
        # Retire obsolete exact units before the legacy bounded job allocator
        # counts active rows. Their immutable receipts remain superseded, never
        # completed by this newer unit. Live leases and committed jobs are retained.
        unit = (await self.service._unit(uow, definition)).payload()
        for item in await uow.derived_records(self.scope, "job"):
            old = item["payload"]
            recoverable = old.get("status") in {"pending", "retry", "deferred"} or (
                old.get("status") == "running"
                and _parse(old["lease_until"]) <= self.service.clock()
            )
            if (
                old.get("unit", {}).get("facet_id") == definition["facet_id"]
                and recoverable
                and old.get("unit") != unit
            ):
                old.update(status="superseded", reason="derived_snapshot_changed")
                await uow.derived_put(self.scope, "job", item["identity"], old)
        # _job's exact wire unit and bounded parent validation are reused unchanged.
        return await self.queue._job(uow, definition)

    async def snapshot(self, task):
        return await self.service.snapshot(task)

    def prepare(self, snapshot):
        return self.service.prepare(snapshot)

    async def publish(self, task, snapshot, prepared):
        return await self.service.publish(task, snapshot, prepared)

    async def authorize(self, uow, definition, actor):
        if actor not in definition["spec"]["readers"]:
            raise DerivedError("derived_read_denied")
        authority = await self.service.registry.authority(uow, self.service.authority_id)
        self.service.registry.permission(authority, (actor,), ())

    async def required_parents(self, uow, definition):
        """Bounded parent-first readiness census, including proof-only gaps.

        Read metadata only. Processing authorization is checked by normal freeze
        and publication before bodies; a permission denial never triggers a body
        read here or gets reinterpreted as successful empty coverage.
        """
        result, seen = [], set()

        async def visit(parent_id, depth):
            if parent_id in seen:
                return
            if depth > 4 or len(seen) >= 32:
                raise DerivedError("derived_parent_capacity")
            seen.add(parent_id)
            parent = await self.definition(uow, parent_id)
            for ancestor in parent["spec"].get("parent_facets", ()):
                await visit(ancestor, depth + 1)
            head = await uow.derived_get(self.scope, "head", parent_id)
            try:
                unit = (await self.service._unit(uow, parent)).payload()
            except DerivedError:
                unit = None
            header = (
                await uow.derived_get(self.scope, "revision_header", head["audit_revision_id"])
                if head and head.get("audit_revision_id")
                else None
            )
            invalid = (
                not head
                or head.get("state") not in {"ready", "empty"}
                or unit is None
                or head.get("unit") != unit
                or (
                    head.get("next_transition_at")
                    and _parse(head["next_transition_at"]) <= self.service.clock()
                )
                or not header
                or digest(header) != head.get("input_header_sha256")
                or header.get("unit") != unit
                or header.get("facet_id") != parent_id
                or header.get("id") != head.get("audit_revision_id")
                or digest(header.get("manifest")) != header.get("manifest_sha256")
            )
            if header and not invalid:
                for source_id, data in header["manifest"]["sources"].items():
                    grant = await uow.derived_get(self.scope, "grant", source_id)
                    if not grant or grant.get("version") != data["grant_version"]:
                        invalid = True
            if invalid:
                result.append(parent_id)

        for parent_id in definition["spec"].get("parent_facets", ()):
            await visit(parent_id, 1)
        return result

    async def check_task(self, uow, task, *, completed=False):
        return await checked_job(self.service, uow, task, completed=completed)

    async def verify_coverage(self, uow, execution):
        job = await uow.derived_get(self.scope, "job", execution["unit_id"])
        if not job or job.get("unit") != execution["unit"] or not valid_completion(self.scope, job):
            return None
        publication = await uow.derived_get(self.scope, "refresh_publication", execution["id"])
        if not publication or publication.get("state") != "committed":
            return None
        if (
            publication.get("commit_token") != job["commit_token"]
            or publication.get("claimed") != execution["claimed"]
            or publication.get("unit") != execution["unit"]
            or publication.get("compatibility") != execution["compatibility"]
            or publication.get("sha256")
            != digest({key: value for key, value in publication.items() if key != "sha256"})
        ):
            return None
        return publication
