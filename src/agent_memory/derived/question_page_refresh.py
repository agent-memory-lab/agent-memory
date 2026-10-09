"""Finite page work on the QuestionView scheduler, never a second queue.

Only published pages are enrolled. The normal page publication transaction owns
both immutable output and exact responsibility discharge; hosts use RefreshHost.
"""

from copy import deepcopy
from datetime import datetime, timedelta

from ..operations.facet_refresh import checked_job, stale
from ..operations.refresh_demand import record_dirty
from ..operations.refresh_policy import RefreshPolicy
from . import subscriptions
from .model import DerivedError, digest
from .question_pages import checked

SCHEMA = "question-page-refresh/1"
UNIT_SCHEMA = "question-page-refresh-unit/1"
PARENT_WAIT = {
    "question_page_parent_unavailable",
    "question_view_unavailable",
    "question_view_stale",
    "derived_time_coverage_expired",
    "project_processing_denied",
    "project_processing_grant_expired",
    "question_original_generation_unavailable",
    "question_original_generation_changed",
}


class QuestionPageRefresh:
    def __init__(self, service):
        self.service, self.scope = service, service.scope

    async def install(self, uow, registration):
        """Atomic enrollment/backfill; existing host policy and budgets survive restart."""
        key = registration["instance_id"]
        old = await uow.derived_get(self.scope, "definition", key)
        if old and old.get("spec", {}).get("registration_sha256") == registration["sha256"]:
            return old
        if not old and len(await uow.derived_records(self.scope, "definition")) >= 128:
            raise DerivedError("derived_subscription_capacity")
        definition = dict(
            facet_id=key,
            spec=dict(
                schema=SCHEMA,
                id=key,
                page_id=registration["page_id"],
                registration_sha256=registration["sha256"],
                readers=registration["readers"],
                purpose=registration["purpose"],
                context=registration["context"],
                parent_facets=[p["instance_id"] for p in registration["parents"]],
            ),
            fingerprint=registration["sha256"],
            generation=registration["generation"],
            epoch=registration["epoch"],
            safety_generation=0,
            time_generation=0,
            slots=[],
            dirty=False,
            disabled=False,
            refresh_managed=True,
        )
        now = self.service.clock()
        config = await uow.derived_get(self.scope, "refresh_policy", key)
        if not config or config.get("state") == "erased":
            config = dict(
                schema="refresh-policy-binding/1",
                facet_id=key,
                adapter_key=self.service.page_processor.key,
                instance_key=key,
                policy=RefreshPolicy(mode="on_change").payload(),
                state="configured",
                epoch=registration["epoch"],
                configured_at=now.isoformat(),
                limits=self.service.queue.limits.payload(),
            )
        config.update(adapter_key=self.service.page_processor.key, epoch=registration["epoch"])
        await uow.derived_put(self.scope, "refresh_policy", key, config)
        await uow.derived_put(self.scope, "definition", key, definition)
        await subscriptions.install(uow, self.scope, definition)
        return definition

    async def published(self, uow, registration, head):
        """Schedule the first time boundary without inventing pending coverage.

        A manual publication may create a dormant boundary row, but must never
        discharge a preexisting lease or coalesced obligation.
        """
        from ..operations.refresh_demand import _compatibility, _due

        definition = await self.install(uow, registration)
        definition["next_transition_at"] = head["valid_until"]
        await uow.derived_put(self.scope, "definition", definition["facet_id"], definition)
        key = "refresh-demand:" + digest(
            [self.scope.partition_key(), definition["facet_id"], _compatibility(definition)]
        )
        row = await uow.derived_get(self.scope, "refresh_demand", key)
        if row is None:
            row = await record_dirty(
                uow,
                self.scope,
                definition,
                at=self.service.clock(),
                reason="page_first_publication",
            )
            row.update(requested=[], obligation_at={}, unsealed=None)
        row["next_transition_at"] = head["valid_until"]
        _due(row, RefreshPolicy.from_payload(row["policy"]), self.service.clock())
        await uow.derived_put(self.scope, "refresh_demand", key, row)

    async def initialize(self, uow):
        pages = await uow.derived_records(self.scope, "question_page_registration")
        if len(pages) > 128:
            raise DerivedError("question_page_capacity")
        for item in pages:
            registration = item["payload"]
            if registration.get("state") == "erased":
                continue
            checked(registration, "question-page-registration/1")
            if (
                registration["context"] != self.service._context()
                or registration["purpose"] != self.service.admission.purpose
                or registration["epoch"] != await uow.retention_epoch(self.scope)
            ):
                continue
            head = await uow.derived_get(self.scope, "question_page_head", item["identity"])
            if not head or head.get("state") == "erased":
                continue
            definition = await self.install(uow, registration)
            # Startup only backfills missing responsibility. It never reschedules
            # an existing row, resets retry/aging, or revives exhausted work.
            from ..operations.refresh_demand import _compatibility

            key = "refresh-demand:" + digest(
                [self.scope.partition_key(), definition["facet_id"], _compatibility(definition)]
            )
            if await uow.derived_get(self.scope, "refresh_demand", key):
                continue
            pending = await uow.derived_get(
                self.scope, "question_page_validation", item["identity"]
            )
            if pending and pending.get("state") == "validation_pending":
                await record_dirty(
                    uow,
                    self.scope,
                    definition,
                    at=self.service.clock(),
                    reason="page_validation_backfill",
                )
            else:
                await self.published(uow, registration, head)

    async def definition(self, uow, key):
        await self.service.pages._open(uow)
        registration = checked(
            await uow.derived_get(self.scope, "question_page_registration", key),
            "question-page-registration/1",
        )
        await self.service.pages._registration(
            uow, registration["page_id"], registration["readers"][0]
        )
        definition = await uow.derived_get(self.scope, "definition", key)
        if (
            not definition
            or definition.get("disabled")
            or definition.get("spec", {}).get("schema") != SCHEMA
            or definition["spec"]["registration_sha256"] != registration["sha256"]
        ):
            raise DerivedError("question_page_stale")
        return definition

    async def authorize(self, uow, definition, actor):
        await self.service.pages._registration(uow, definition["spec"]["page_id"], actor)

    async def required_parents(self, uow, definition):
        needed = []
        actor = definition["spec"]["readers"][0]
        for key in definition["spec"]["parent_facets"]:
            parent = await self.service._definition(uow, key)
            head = await uow.derived_get(self.scope, "question_head", key)
            try:
                if not head:
                    raise DerivedError("question_page_parent_unavailable")
                await self.service._guard(uow, parent, head, actor=actor)
            except DerivedError as error:
                if error.code not in PARENT_WAIT:
                    raise
                needed.append(key)
        return needed

    async def unit(self, uow, definition):
        registration = await self.service.pages._registration(
            uow, definition["spec"]["page_id"], definition["spec"]["readers"][0]
        )
        try:
            parents = await self.service.pages._parents(
                uow, registration, definition["spec"]["readers"][0]
            )
        except DerivedError as error:
            if error.code in PARENT_WAIT:
                raise DerivedError("derived_parent_stale") from error
            raise
        head = await uow.derived_get(self.scope, "question_page_head", definition["facet_id"])
        return dict(
            schema=UNIT_SCHEMA,
            facet_id=definition["facet_id"],
            epoch=definition["epoch"],
            definition_sha256=definition["fingerprint"],
            generation=definition["generation"],
            safety_generation=definition["safety_generation"],
            time_generation=definition["time_generation"],
            parents={key: value["sha256"] for key, value in parents.items()},
            previous_head_sha256=(head or {}).get("sha256"),
        )

    async def freeze(self, uow, definition):
        unit = await self.unit(uow, definition)
        key = "question-unit:" + digest(unit)
        old = await uow.derived_get(self.scope, "job", key)
        if old:
            return old
        if len(await uow.derived_records(self.scope, "job")) >= 4096:
            raise DerivedError("derived_refresh_backpressure")
        now = self.service.clock()
        job = dict(
            id=key,
            unit=unit,
            status="pending",
            attempts=0,
            generation=0,
            created_at=now.isoformat(),
            next_attempt_at=now.isoformat(),
            expires_at=(now + timedelta(days=1)).isoformat(),
        )
        await uow.derived_put(self.scope, "job", key, job)
        return job

    async def check(self, uow, task):
        job = await checked_job(self.service, uow, task)
        definition = await self.definition(uow, job["unit"]["facet_id"])
        if await self.unit(uow, definition) != job["unit"]:
            raise DerivedError("derived_snapshot_changed")
        return job, definition

    async def snapshot(self, task):
        async with self.service.repository.unit_of_work() as uow:
            await self.service.pages._open(uow)
            job, _ = await self.check(uow, task)
            return {"page_refresh": True, "unit": deepcopy(job["unit"])}

    async def publish(self, task, snapshot, prepared):
        if snapshot != prepared or snapshot != {"page_refresh": True, "unit": task.payload["unit"]}:
            raise DerivedError("derived_input_changed")
        return await self.service.pages._publish(None, actor=None, maintenance_task=task)

    async def complete(self, uow, task, job, definition, content, certificate, head):
        from ..operations.refresh_demand import publish_coverage

        # Publication may have awaited storage after the last lease check.
        await checked_job(self.service, uow, task)
        outcome = "noop" if certificate["rebuild"] == "proof_reuse" else "applied"
        job.update(
            status="completed",
            outcome=outcome,
            no_outputs=False,
            revision_id=content["id"],
            content_sha256=digest(content),
            certificate_revision_id=certificate["id"],
            certificate_sha256=digest(certificate),
            commit_token="derived-commit:"
            + digest([self.scope.partition_key(), job["unit"], content["id"], outcome]),
            completed_at=self.service.clock().isoformat(),
        )
        await uow.derived_put(self.scope, "job", job["id"], job)
        definition["next_transition_at"] = head["valid_until"]
        await publish_coverage(
            uow,
            self.service,
            job,
            definition,
            manifest=dict(
                schema="question-page-publication/1",
                unit=job["unit"],
                query_complete=True,
                head_sha256=head["sha256"],
            ),
            now=self.service.clock(),
        )
        # Last storage await cannot permit an expired fence to commit.
        await checked_job(self.service, uow, task, completed=True)
        if self.service.clock() >= datetime.fromisoformat(job["lease_until"]):
            raise stale()
