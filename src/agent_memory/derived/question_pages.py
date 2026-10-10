"""Finite, host-published L2 projection of current project QuestionViews.

Parent full computation remains on the shared refresh queue. This projection only
consumes already-ready parents, atomically records the exact content/certificate
inputs and materialized blocks, and never promotes a page into fact authority.
"""

from copy import deepcopy
from datetime import datetime

from .model import DerivedError, digest, identity
from .question_materialize import budget

PAGE_TEMPLATE = "project-question-scenario/1"
PAGE_KINDS = (
    "question_page_registration",
    "question_page_content",
    "question_page_certificate",
    "question_page_block",
    "question_page_head",
    "question_page_validation",
)


def sealed(value):
    return {**value, "sha256": digest(value)}


def checked(value, schema):
    if (
        not value
        or value.get("state") == "erased"
        or value.get("schema") != schema
        or value.get("sha256") != digest({k: v for k, v in value.items() if k != "sha256"})
    ):
        raise DerivedError("question_page_unavailable")
    return value


class ProjectQuestionPages:
    """Registration/publication are trusted host methods; transports expose reads."""

    def __init__(self, service):
        self.service, self.scope = service, service.scope
        from .question_page_refresh import QuestionPageRefresh

        self.maintenance = QuestionPageRefresh(service)

    def key(self, page_id):
        return "question-page:" + digest([self.scope.partition_key(), identity(page_id)])

    async def _open(self, uow):
        if getattr(uow, "question_page_contract", None) != PAGE_TEMPLATE:
            raise DerivedError("question_page_backend_unsupported")
        return await self.service._open(uow)

    async def register(
        self, page_id, question_ids, *, readers, expected_generation=0, max_output_bytes=262144
    ):
        page_id, question_ids, readers = deepcopy((page_id, tuple(question_ids), tuple(readers)))
        identity(page_id)
        if (
            not 1 <= len(question_ids) <= 4
            or len(set(question_ids)) != len(question_ids)
            or not 1 <= len(readers) <= 16
            or len(set(readers)) != len(readers)
            or type(expected_generation) is not int
            or expected_generation < 0
            or type(max_output_bytes) is not int
            or not 1024 <= max_output_bytes <= 1048576
        ):
            raise DerivedError("invalid_question_page_registration")
        for reader in readers:
            identity(reader)
        key = self.key(page_id)
        async with self.service.repository.unit_of_work() as uow:
            epoch = await self._open(uow)
            old = await uow.derived_get(self.scope, "question_page_registration", key)
            generation = old["generation"] if old and old.get("state") != "erased" else 0
            if generation != expected_generation:
                raise DerivedError("question_page_registration_conflict")
            if (
                not old
                and len(await uow.derived_records(self.scope, "question_page_registration")) >= 128
            ):
                raise DerivedError("question_page_capacity")
            parents, projects, templates = [], set(), set()
            for question_id in question_ids:
                definition = await self.service._registration(uow, question_id, readers[0])
                spec = definition["spec"]
                if not set(readers) <= set(spec["readers"]):
                    raise DerivedError("derived_read_denied")
                for reader in readers:
                    await self.service._authorize(uow, definition, reader)
                projects.add(spec["project_id"])
                templates.add(spec["question"])
                parents.append(
                    dict(
                        question_id=question_id,
                        instance_id=definition["facet_id"],
                        definition_sha256=digest(definition["spec"]),
                    )
                )
            if len(projects) != 1 or len(templates) != len(parents):
                raise DerivedError("question_page_parent_incompatible")
            value = sealed(
                dict(
                    schema="question-page-registration/1",
                    state="registered",
                    page_id=page_id,
                    instance_id=key,
                    generation=generation + 1,
                    epoch=epoch,
                    readers=list(readers),
                    parents=parents,
                    project_id=next(iter(projects)),
                    context=self.service._context(),
                    purpose=self.service.admission.purpose,
                    max_output_bytes=max_output_bytes,
                )
            )
            await uow.derived_put(self.scope, "question_page_registration", key, value)
            await self.service.project_history.register(uow, page_id, value, kind="page")
            await uow.derived_edges(
                self.scope,
                key,
                [("query", "question-instance:" + parent["instance_id"]) for parent in parents],
            )
            head = await uow.derived_get(self.scope, "question_page_head", key)
            if head and head.get("state") != "erased":
                definition = await self.maintenance.install(uow, value)
                from ..operations.refresh_demand import record_dirty

                await record_dirty(uow, self.scope, definition, at=self.service.clock(),
                                   reason="page_registration_changed")
            return deepcopy(value)

    async def _registration(self, uow, page_id, actor):
        identity(actor)
        row = checked(
            await uow.derived_get(self.scope, "question_page_registration", self.key(page_id)),
            "question-page-registration/1",
        )
        if actor not in row["readers"]:
            raise DerivedError("derived_read_denied")
        if (
            row["epoch"] != await uow.retention_epoch(self.scope)
            or row["context"] != self.service._context()
            or row["purpose"] != self.service.admission.purpose
        ):
            raise DerivedError("question_page_stale")
        for parent in row["parents"]:
            definition = await self.service._registration(uow, parent["question_id"], actor)
            if (
                definition["facet_id"] != parent["instance_id"]
                or digest(definition["spec"]) != parent["definition_sha256"]
                or not set(row["readers"]) <= set(definition["spec"]["readers"])
            ):
                raise DerivedError("question_page_parent_changed")
        return row

    async def _parents(self, uow, registration, actor, *, expected=None):
        headers = {}
        for parent in registration["parents"]:
            definition = await self.service._registration(uow, parent["question_id"], actor)
            header = await uow.derived_get(self.scope, "question_head", parent["instance_id"])
            if not header:
                raise DerivedError("question_page_parent_unavailable")
            await self.service._guard(uow, definition, header, actor=actor)
            headers[parent["instance_id"]] = header
        if expected is not None and headers != expected:
            raise DerivedError("question_page_stale")
        return headers

    async def parent_published(self, uow, instance_id, header):
        """Persist bounded validation responsibility; never mark a child current by value."""
        owners = await uow.derived_reverse(self.scope, "question-instance:" + instance_id)
        if len(owners) > 128:
            raise DerivedError("question_page_capacity")
        for key in owners:
            head = await uow.derived_get(self.scope, "question_page_head", key)
            if not head or head.get("state") == "erased":
                continue
            registration = checked(await uow.derived_get(
                self.scope, "question_page_registration", key
            ), "question-page-registration/1")
            definition = await self.maintenance.install(uow, registration)
            from ..operations.refresh_demand import record_dirty

            await record_dirty(uow, self.scope, definition, at=self.service.clock(),
                               reason="page_parent_published")
            pending = await uow.derived_get(self.scope, "question_page_validation", key) or {}
            targets = dict(pending.get("targets", {}))
            targets[instance_id] = header["sha256"]
            await uow.derived_put(
                self.scope,
                "question_page_validation",
                key,
                sealed(
                    dict(
                        schema="question-page-validation/1",
                        instance_id=key,
                        state="validation_pending",
                        attempts=pending.get("attempts", 0),
                        targets=targets,
                    )
                ),
            )

    async def validate_pending(self, *, actor, max_pages=8):
        """One bounded topological layer. Unsupported/deeper graphs are not admitted.

        The registered page consumes complete source-sensitive answers. Equality
        of business values alone is insufficient; only unchanged parent content
        can skip rendering. Every current proof and original lineage is checked.
        """
        if type(max_pages) is not int or not 1 <= max_pages <= 32:
            raise DerivedError("invalid_question_page_work_budget")
        async with self.service.repository.unit_of_work() as uow:
            await self._open(uow)
            pending = await uow.derived_records(self.scope, "question_page_validation")
            todo = []
            for item in sorted(
                pending, key=lambda row: (row["payload"].get("attempts", 0), row["identity"])
            ):
                if item["payload"].get("state") != "validation_pending":
                    continue
                registration = await uow.derived_get(
                    self.scope, "question_page_registration", item["identity"]
                )
                if registration and actor in registration.get("readers", ()):
                    todo.append(registration["page_id"])
                    value = {k: v for k, v in item["payload"].items() if k != "sha256"}
                    value["attempts"] = value.get("attempts", 0) + 1
                    await uow.derived_put(
                        self.scope, "question_page_validation", item["identity"], sealed(value)
                    )
                if len(todo) == max_pages:
                    break
        results = []
        for page_id in todo:
            try:
                answer = await self.publish(page_id, actor=actor)
                results.append(dict(page_id=page_id, state="valid", rebuild=answer["rebuild"]))
            except DerivedError as error:
                # Keep responsibility durable when a parent is still dirty,
                # unavailable, unsafe, or outside the caller's permission.
                results.append(dict(page_id=page_id, state="validation_pending", reason=error.code))
        return results

    async def _original_guard(self, uow, registration, originals, actor):
        # Legacy full pages key snapshots by instance. Typed patches key them by
        # immutable header digest, preserving several generations of one parent.
        definitions = {
            parent["instance_id"]: await self.service._registration(
                uow, parent["question_id"], actor
            )
            for parent in registration["parents"]
        }
        seen, boundaries = set(), []
        if not 1 <= len(originals) <= 128:
            raise DerivedError("question_page_integrity_failed")
        for key, original in originals.items():
            instance = original.get("head", {}).get("instance_id")
            if (
                instance not in definitions
                or key not in {instance, "question-page-parent-generation:" + digest(original)}
                or original.get("sha256")
                != digest({k: v for k, v in original.items() if k != "sha256"})
            ):
                raise DerivedError("question_page_integrity_failed")
            seen.add(instance)
            for proof in ("generation_proof", "proof"):
                boundary = await self.service._generation_guard(
                    uow, definitions[instance], original.get(proof)
                )
                if boundary:
                    boundaries.append(datetime.fromisoformat(boundary))
        if seen != set(definitions):
            raise DerivedError("question_page_integrity_failed")
        boundary = min(boundaries) if boundaries else None
        if boundary is not None and self.service.clock() >= boundary:
            raise DerivedError("project_processing_grant_expired")
        if self.service._context() != registration["context"] or any(
            self.service.admission.registration_fingerprint
            != definition["spec"]["registration_fingerprint"]
            for definition in definitions.values()
        ):
            raise DerivedError("question_registration_changed")
        return boundary

    async def patch(
        self,
        page_id,
        operations,
        *,
        actor,
        expected_revision_id,
        expected_certificate_revision_id,
    ):
        """Apply a bounded host-only typed plan; never accept prose or model patches."""
        from .question_page_patches import apply_patch, snapshot_operations

        operations = snapshot_operations(operations)
        identity(expected_revision_id)
        identity(expected_certificate_revision_id)
        observed = await self.service._clock_barrier()
        try:
            await apply_patch(
                self,
                page_id,
                operations,
                actor=actor,
                expected_revision_id=expected_revision_id,
                expected_certificate_revision_id=expected_certificate_revision_id,
            )
            return await self.read(page_id, actor=actor)
        except BaseException:
            observed = max(observed, self.service.clock())
            try:
                await self.service._clock_barrier(observed_at=observed)
            except DerivedError as error:
                if error.code != "refresh_clock_discontinuity":
                    raise
            raise

    @staticmethod
    def _split_answer(answer):
        stable = deepcopy(answer)
        proof = {
            key: stable.pop(key)
            for key in (
                "certificate_revision_id",
                "availability_status",
                "refresh_status",
                "compute_mode",
                "compute_trace",
                "runtime_current_validated",
                "coverage",
                "valid_until",
                "validation_manifest",
                "digests",
            )
            if key in stable
        }
        proof["result_metadata"] = {
            key: stable["result"].pop(key)
            for key in (
                "snapshot_id",
                "input_fingerprint",
                "valid_at",
                "known_at",
                "next_transition_at",
            )
            if key in stable["result"]
        }
        return stable, proof

    async def publish(self, page_id, *, actor):
        """Full bounded template only; missing parents defer rather than trigger hidden work."""
        observed = await self.service._clock_barrier()
        try:
            return await self._publish(page_id, actor=actor)
        except BaseException:
            observed = max(observed, self.service.clock())
            try:
                await self.service._clock_barrier(observed_at=observed)
            except DerivedError as error:
                if error.code != "refresh_clock_discontinuity":
                    raise
            raise

    async def _publish(self, page_id, *, actor, maintenance_task=None):
        async with self.service.repository.unit_of_work() as uow:
            await self._open(uow)
            if maintenance_task is not None:
                job, definition = await self.maintenance.check(uow, maintenance_task)
                page_id, actor = definition["spec"]["page_id"], definition["spec"]["readers"][0]
            registration = await self._registration(uow, page_id, actor)
            parents = await self._parents(uow, registration, actor)
            answers = [
                await self.service._read_in_uow(uow, p["question_id"], actor=actor)
                for p in registration["parents"]
            ]
            inputs = []
            for answer in answers:
                inputs.extend(
                    [
                        dict(kind="derived_content", id=answer["content_revision_id"]),
                        dict(kind="derived_certificate", id=answer["certificate_revision_id"]),
                    ]
                )
            manifest = dict(
                schema="question-page-generation/1", inputs=inputs, parents=deepcopy(parents)
            )
            old_head = await uow.derived_get(
                self.scope, "question_page_head", registration["instance_id"]
            )
            old_content = old_certificate = None
            old_blocks = []
            if old_head and old_head.get("state") != "erased":
                checked(old_head, "question-page-head/1")
                try:
                    await self._original_guard(
                        uow, registration, old_head.get("generation_parents", {}), actor
                    )
                    await self._original_guard(uow, registration, old_head["parents"], actor)
                except DerivedError:
                    pass  # New independent full generation; do not load old bodies.
                else:
                    if old_head["registration_sha256"] == registration["sha256"]:
                        old_content, old_certificate, old_blocks = await self._load(
                            uow, old_head, registration, actor
                        )
            answer_proofs = {}
            blocks, references = [], []
            by_question = {answer["question_id"]: answer for answer in answers}
            layout = [
                (
                    "question-page-block:"
                    + digest([registration["instance_id"], answer["question_id"]]),
                    answer["question_id"],
                )
                for answer in answers
            ]
            generation_manifest = manifest
            if old_content and old_content.get("rebuild") == "typed_patch":
                from .question_page_patches import merge_manifests

                layout = [(b["block_id"], b["body"]["answer"]["question_id"]) for b in old_blocks]
                generation_manifest = merge_manifests(
                    old_content["generation_manifest"],
                    old_certificate["validation_manifest"],
                    manifest,
                    dict(
                        parents={},
                        inputs=[
                            dict(kind="derived_content", id=old_content["id"]),
                            dict(kind="derived_certificate", id=old_certificate["id"]),
                        ],
                    ),
                )
            for block_id, question_id in layout:
                answer = by_question[question_id]
                stable, answer_proofs[question_id] = self._split_answer(answer)
                body = dict(kind="project_question", answer=stable)
                block = dict(
                    schema="question-page-block/1",
                    instance_id=registration["instance_id"],
                    block_id=block_id,
                    body=body,
                    generation_manifest=generation_manifest,
                )
                revision_id = "question-page-block-version:" + digest(block)
                block = sealed({**block, "id": revision_id})
                blocks.append(block)
                references.append(
                    dict(block_id=block_id, revision_id=revision_id, sha256=block["sha256"])
                )
            content = dict(
                schema="question-page-content/1",
                instance_id=registration["instance_id"],
                page_id=page_id,
                template=PAGE_TEMPLATE,
                rebuild=(
                    "typed_patch"
                    if old_content and old_content.get("rebuild") == "typed_patch"
                    else "full"
                ),
                registration_sha256=registration["sha256"],
                blocks=references,
                generation_manifest=generation_manifest,
            )
            revision_id = "question-page-content:" + digest(content)
            content = sealed({**content, "id": revision_id})
            reused = bool(
                old_content and [b["body"] for b in old_blocks] == [b["body"] for b in blocks]
            )
            if reused:
                content, blocks, revision_id = old_content, old_blocks, old_content["id"]
            rebuild = "proof_reuse" if reused else "full"
            if reused and old_head["parents"] == parents:
                rebuild = old_certificate.get("rebuild", "full")
            certificate = dict(
                schema="question-page-certificate/1",
                instance_id=registration["instance_id"],
                content_revision_id=revision_id,
                generation_manifest=content["generation_manifest"],
                validation_manifest=manifest,
                answer_proofs=answer_proofs,
                rebuild=rebuild,
                registration_sha256=registration["sha256"],
                valid_until=min((a["valid_until"] for a in answers), key=datetime.fromisoformat),
            )
            certificate_id = "question-page-certificate:" + digest(certificate)
            certificate = sealed({**certificate, "id": certificate_id})
            head = sealed(
                dict(
                    schema="question-page-head/1",
                    instance_id=registration["instance_id"],
                    content_revision_id=revision_id,
                    content_sha256=content["sha256"],
                    certificate_revision_id=certificate_id,
                    certificate_sha256=certificate["sha256"],
                    registration_sha256=registration["sha256"],
                    parents=parents,
                    generation_parents=content["generation_manifest"]["parents"],
                    valid_until=certificate["valid_until"],
                )
            )
            budget(self._response(content, certificate, blocks), registration["max_output_bytes"])
            await self._store(uow, registration, content, certificate, blocks, head)
            await self._guard(uow, page_id, actor, registration, head)
            if maintenance_task is not None:
                await self.maintenance.complete(
                    uow, maintenance_task, job, definition, content, certificate, head
                )
                from ..operations.facet_refresh import checked_job

                await checked_job(self.service, uow, maintenance_task, completed=True)
                await self._guard(uow, page_id, actor, registration, head)
                if any(self.service.clock() >= datetime.fromisoformat(job[key])
                       for key in ("lease_until", "expires_at")):
                    from ..operations.facet_refresh import stale

                    raise stale()
        if maintenance_task is not None:
            return dict(page_id=page_id, state="valid", rebuild=certificate["rebuild"])
        return await self.read(page_id, actor=actor)

    async def _store(self, uow, registration, content, certificate, blocks, head):
        """Immutable rows, dependency edges and the head share the locked scope UoW."""
        await self.maintenance.published(uow, registration, head)
        for kind, values in (
            ("question_page_block", blocks),
            ("question_page_content", [content]),
            ("question_page_certificate", [certificate]),
        ):
            values = {v["id"]: v for v in values}
            rows = await uow.derived_records(self.scope, kind)
            existing = {row["identity"] for row in rows}
            if len(existing | set(values)) > 4096:
                raise DerivedError("question_page_capacity")
            for value in values.values():
                old = await uow.derived_get(self.scope, kind, value["id"])
                if old is not None and old != value:
                    raise DerivedError("question_page_revision_conflict")
                if old is not None:
                    continue  # Never rewrite retained immutable blocks or their edges.
                await uow.derived_put(self.scope, kind, value["id"], value)
                dependencies = list(value["generation_manifest"]["inputs"])
                dependencies.extend(value.get("validation_manifest", {}).get("inputs", ()))
                await uow.derived_edges(
                    self.scope,
                    value["id"],
                    sorted(
                        set(
                            [("processing", "derived:" + ref["id"]) for ref in dependencies]
                            + [
                                ("support", "derived:" + ref["id"])
                                for ref in dependencies
                                if ref["kind"] == "derived_content"
                            ]
                        )
                    ),
                )
        await uow.derived_put(self.scope, "question_page_head", registration["instance_id"], head)
        await self.service.project_history.layout(uow, registration, blocks)
        pending = await uow.derived_get(
            self.scope, "question_page_validation", registration["instance_id"]
        )
        if pending:
            await uow.derived_put(
                self.scope,
                "question_page_validation",
                registration["instance_id"],
                sealed(
                    dict(
                        schema="question-page-validation/1",
                        instance_id=registration["instance_id"],
                        state="valid",
                        attempts=pending.get("attempts", 0),
                        targets={k: h["sha256"] for k, h in head["parents"].items()},
                    )
                ),
            )

    async def _guard(self, uow, page_id, actor, registration, head):
        if (
            await self._registration(uow, page_id, actor) != registration
            or head["registration_sha256"] != registration["sha256"]
            or await uow.derived_get(self.scope, "question_page_head", registration["instance_id"])
            != head
        ):
            raise DerivedError("question_page_stale")
        pending = await uow.derived_get(
            self.scope, "question_page_validation", registration["instance_id"]
        )
        if pending and pending.get("state") == "validation_pending":
            raise DerivedError("question_page_stale")
        original_until = await self._original_guard(
            uow, registration, head.get("generation_parents", {}), actor
        )
        await self._parents(uow, registration, actor, expected=head["parents"])
        from ..operations.refresh_demand import observed_clock
        from .question_materialize import check_time

        observed = await observed_clock(uow, self.scope, self.service.clock)
        now = self.service.clock()
        if now < observed:
            raise DerivedError("refresh_clock_discontinuity")
        # Host configuration and wall time can change while the final parent's
        # metadata is awaited. Check every parent after the final storage await.
        for parent in head["parents"].values():
            check_time(parent, now)
            if (
                self.service.admission.registration_fingerprint
                != parent["proof"]["registration_fingerprint"]
            ):
                raise DerivedError("question_registration_changed")
        if original_until is not None and now >= original_until:
            raise DerivedError("project_processing_grant_expired")
        if now >= datetime.fromisoformat(head["valid_until"]):
            raise DerivedError("derived_time_coverage_expired")
        if self.service._context() != registration["context"]:
            raise DerivedError("question_page_stale")

    @staticmethod
    def _response(content, certificate, blocks):
        delivered = []
        for block in blocks:
            body = deepcopy(block["body"])
            proof = deepcopy(
                certificate.get("answer_proofs", {}).get(body["answer"]["question_id"], {})
            )
            body["answer"]["result"].update(proof.pop("result_metadata", {}))
            body["answer"].update(proof)
            delivered.append(dict(block_id=block["block_id"], revision_id=block["id"], body=body))
        return dict(
            schema="question-page/1",
            page_id=content["page_id"],
            template=PAGE_TEMPLATE,
            rebuild=certificate.get("rebuild", "full"),
            availability_status="valid",
            revision_id=content["id"],
            certificate_revision_id=certificate["id"],
            blocks=delivered,
            # Full query proofs include moved-out candidates whose source is
            # no longer authorized for this project's body. Keep that metadata
            # internal; expose immutable input IDs and authenticated digests.
            generation_manifest=dict(
                schema="question-page-generation-references/1",
                inputs=deepcopy(content["generation_manifest"]["inputs"]),
                parents={
                    key: {"sha256": digest(parent)}
                    for key, parent in content["generation_manifest"]["parents"].items()
                },
            ),
            valid_until=certificate["valid_until"],
            model_calls=0,
        )

    async def read(self, page_id, *, actor, valid_at=None, known_at=None):
        if valid_at is not None or known_at is not None:
            if not self.service.history_points and not self.service.history_rebuild:
                raise DerivedError("question_historical_unsupported")
            if valid_at is None or known_at is None:
                raise DerivedError("question_history_both_times_required")
            history = self.service.project_history if self.service.history_rebuild else self.service.history
            return await history.read(page_id, actor=actor, kind="page", valid_at=valid_at, known_at=known_at)
        observed = await self.service._clock_barrier()
        try:
            return await self._read(page_id, actor=actor)
        except BaseException:
            observed = max(observed, self.service.clock())
            try:
                await self.service._clock_barrier(observed_at=observed)
            except DerivedError as error:
                if error.code != "refresh_clock_discontinuity":
                    raise
            raise

    async def _read(self, page_id, *, actor):
        async with self.service.repository.unit_of_work() as uow:
            await self._open(uow)
            registration = await self._registration(uow, page_id, actor)
            head = checked(
                await uow.derived_get(
                    self.scope, "question_page_head", registration["instance_id"]
                ),
                "question-page-head/1",
            )
            await self._guard(uow, page_id, actor, registration, head)
            content, certificate, blocks = await self._load(
                uow, head, registration, actor, current=True
            )
            result = budget(
                self._response(content, certificate, blocks), registration["max_output_bytes"]
            )
            await self._guard(uow, page_id, actor, registration, head)
            return result

    @staticmethod
    def _contains_manifest(whole, part):
        # Compare complete headers, never only parent instance keys.
        parents = {digest(p) for p in whole["parents"].values()}
        inputs = {digest(ref) for ref in whole["inputs"]}
        return all(digest(p) in parents for p in part["parents"].values()) and all(
            digest(ref) in inputs for ref in part["inputs"]
        )

    async def _load(self, uow, head, registration, actor, *, current=False):
        originals = {
            "question-page-parent-generation:" + digest(parent): parent
            for parent in (*head["generation_parents"].values(), *head["parents"].values())
        }

        async def guard():
            await self._original_guard(uow, registration, originals, actor)
            if current:
                await self._guard(uow, registration["page_id"], actor, registration, head)

        async def guarded_get(kind, key):
            await guard()
            value = await uow.derived_get(self.scope, kind, key)
            await guard()
            return value

        content = checked(
            await guarded_get("question_page_content", head["content_revision_id"]),
            "question-page-content/1",
        )
        certificate = checked(
            await guarded_get("question_page_certificate", head["certificate_revision_id"]),
            "question-page-certificate/1",
        )
        if (
            content["sha256"] != head["content_sha256"]
            or certificate["sha256"] != head["certificate_sha256"]
            or certificate["content_revision_id"] != content["id"]
            or content["generation_manifest"] != certificate["generation_manifest"]
            or content["generation_manifest"]["parents"] != head["generation_parents"]
            or certificate["validation_manifest"]["parents"] != head["parents"]
        ):
            raise DerivedError("question_page_integrity_failed")
        if not 1 <= len(content["blocks"]) <= 16 or len(
            {ref["block_id"] for ref in content["blocks"]}
        ) != len(content["blocks"]):
            raise DerivedError("question_page_integrity_failed")
        blocks = []
        for ref in content["blocks"]:
            block = checked(
                await guarded_get("question_page_block", ref["revision_id"]),
                "question-page-block/1",
            )
            if (
                block["sha256"] != ref["sha256"]
                or block["block_id"] != ref["block_id"]
                or not self._contains_manifest(
                    content["generation_manifest"], block["generation_manifest"]
                )
            ):
                raise DerivedError("question_page_integrity_failed")
            blocks.append(block)
        return content, certificate, blocks
