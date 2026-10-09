"""QuestionView adapter for the shared durable refresh admission and worker path."""

from datetime import timedelta

from ..operations.facet_refresh import checked_job, valid_completion
from .model import DerivedError, digest


class QuestionRefreshProcessor:
    task_type = "memory.facet_refresh"

    def __init__(self, service):
        self.service, self.repository, self.scope = service, service.repository, service.scope
        self.key = "question-refresh/2:" + digest(
            [
                self.scope.partition_key(),
                service.admission.registration_fingerprint,
                service.context.fingerprint,
            ]
        )

    def instance_id(self, definition):
        return definition["facet_id"]

    def target_metadata(self, definition):
        if definition["spec"].get("schema") == "question-page-refresh/1":
            return dict(definition_fingerprint=definition["fingerprint"],
                        context_fingerprint=digest(definition["spec"]["context"]),
                        request_semantics_digest=digest([self.key, definition["spec"]]))
        instance = definition["spec"]["instance"]
        return dict(
            definition_fingerprint=definition["fingerprint"],
            context_fingerprint=digest(instance["context"]),
            request_semantics_digest=digest(
                [
                    "project-full-current-census/1",
                    self.key,
                    instance["parameters"],
                    definition["spec"]["publication_request_ids"],
                ]
            ),
        )

    async def definition(self, uow, facet_id):
        await self.service._open(uow)
        if facet_id.startswith("question-page:"):
            return await self.service.pages.maintenance.definition(uow, facet_id)
        return await self.service._definition(uow, facet_id)

    async def authorize(self, uow, definition, actor):
        if definition["spec"].get("schema") == "question-page-refresh/1":
            return await self.service.pages.maintenance.authorize(uow, definition, actor)
        return await self.service._authorize(uow, definition, actor)

    async def required_parents(self, uow, definition):
        if definition["spec"].get("schema") == "question-page-refresh/1":
            return await self.service.pages.maintenance.required_parents(uow, definition)
        return []

    async def freeze(self, uow, definition):
        if definition["spec"].get("schema") == "question-page-refresh/1":
            return await self.service.pages.maintenance.freeze(uow, definition)
        proof = await self.service._proof(uow, definition, at=self.service.clock())
        unit = self.service._unit(definition, proof)
        key = "question-unit:" + digest(unit)
        old = await uow.derived_get(self.scope, "job", key)
        if old:
            return old
        jobs = await uow.derived_records(self.scope, "job")
        if len(jobs) >= 4096:
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

    async def check_task(self, uow, task, *, completed=False):
        if task.payload.get("adapter_key") != self.key or not task.payload.get("refresh_execution"):
            raise DerivedError("refresh_processor_unsupported")
        return await checked_job(self.service, uow, task, completed=completed)

    async def snapshot(self, task):
        if task.payload["unit"].get("schema") == "question-page-refresh-unit/1":
            return await self.service.pages.maintenance.snapshot(task)
        return await self.service.snapshot(task)

    def prepare(self, snapshot):
        if snapshot.get("page_refresh"):
            from copy import deepcopy

            return deepcopy(snapshot)
        return self.service.prepare(snapshot)

    async def publish(self, task, snapshot, prepared):
        if task.payload["unit"].get("schema") == "question-page-refresh-unit/1":
            return await self.service.pages.maintenance.publish(task, snapshot, prepared)
        return await self.service.publish(task, snapshot, prepared)

    async def verify_coverage(self, uow, execution):
        if not execution or execution.get("adapter_key") != self.key:
            return None
        job = await uow.derived_get(self.scope, "job", execution["unit_id"])
        if not job or job.get("unit") != execution["unit"] or not valid_completion(self.scope, job):
            return None
        publication = await uow.derived_get(self.scope, "refresh_publication", execution["id"])
        if (
            not publication
            or publication.get("state") != "committed"
            or publication.get("commit_token") != job["commit_token"]
            or publication.get("claimed") != execution["claimed"]
            or publication.get("unit") != execution["unit"]
            or publication.get("compatibility") != execution["compatibility"]
            or publication.get("sha256")
            != digest({k: v for k, v in publication.items() if k != "sha256"})
        ):
            return None
        if job["unit"].get("schema") == "question-page-refresh-unit/1":
            from .question_pages import checked

            try:
                for kind, revision, sha in (
                    ("question_page_content", "revision_id", "content_sha256"),
                    ("question_page_certificate", "certificate_revision_id", "certificate_sha256"),
                ):
                    value = await uow.derived_get(self.scope, kind, job[revision])
                    checked(value, kind.replace("_", "-") + "/1")
                    if digest(value) != job[sha] or value["instance_id"] != job["unit"]["facet_id"]:
                        return None
                if value["content_revision_id"] != job["revision_id"]:
                    return None
            except (DerivedError, KeyError, TypeError):
                return None
            return publication
        # Finite completion is not current-readiness. It verifies the actual
        # committed immutable content/certificate, independent of a later head.
        cert = await uow.derived_get(
            self.scope, "question_certificate", job["certificate_revision_id"]
        )
        if not cert or digest(cert) != job.get("certificate_sha256"):
            return None
        from .question_model import QuestionCertificate

        try:
            certificate = QuestionCertificate.from_payload(cert)
            if (
                certificate.content_revision_id != job["revision_id"]
                or certificate.id != job["certificate_revision_id"]
            ):
                return None
        except (DerivedError, ValueError, TypeError):
            return None
        return publication


class QuestionPageRefreshProcessor(QuestionRefreshProcessor):
    """A distinct route prevents older QuestionView workers claiming page units."""

    def __init__(self, service):
        super().__init__(service)
        self.key = "question-page-refresh/1:" + digest(self.key)

    async def initialize(self, uow):
        await self.service.pages.maintenance.initialize(uow)
