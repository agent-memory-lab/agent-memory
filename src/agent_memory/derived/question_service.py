"""Host-registered project QuestionViews with atomic full publication and delivery.

The public read path accepts only registered identifiers. Admission owns semantic
review and membership; the scheduler owns every direct/background lease. No raw
caller qualification, source count, or constructed certificate can authorize a read.
"""

from copy import deepcopy
from datetime import datetime

from ..conditions import ContextAttribute, QueryContext, instant
from ..consolidation.project_admission import MAX_CANDIDATES, MAX_INPUTS, ProjectAdmission
from ..serialization import to_jsonable
from . import subscriptions
from .model import DerivedError, ProcessingGrant, digest, identity
from .project_index import query_keys
from .question_contracts import (
    REGISTRATION_SCHEMA,
    RUNTIME_SCHEMA,
    project_definition,
    registration_key,
)
from .question_dependencies import readset
from .question_inputs import MAX_BATCH, QuestionInputWork, SharedQuestionInputs, compatibility
from .question_materialize import (
    budget,
    check_time,
    content_digests,
    materialize,
    refresh_state,
    response,
)
from .question_model import (
    QuestionCertificate,
    QuestionContent,
    QuestionContext,
    QuestionHead,
    QuestionInstance,
    QuestionTime,
    RefreshPolicyRef,
    SourceBasis,
    TimeMode,
)
from .service import open_derived


def _fail(code):
    raise DerivedError(code)


def _seal(value):
    value["sha256"] = digest(value)
    return value


def _checked(value, schema):
    if (
        not value
        or value.get("schema") != schema
        or value.get("sha256") != digest({k: v for k, v in value.items() if k != "sha256"})
    ):
        _fail("question_record_unavailable")
    return value


class QuestionService:
    """Trusted host setup; transport may invoke read/request/answer, never register."""

    async def call(self, operation, payload, context):
        from .question_transport import call

        return await call(self, operation, payload, context)

    def __init__(self, admission, context, *, limits=None):
        if type(admission) is not ProjectAdmission or type(context) is not QuestionContext:
            _fail("question_trusted_registration_required")
        # Context attributes must match the supported deterministic evaluator;
        # reject nested values before persisting an unexecutable registration.
        for name, value in context.attributes.items():
            ContextAttribute(name, value, context.issuer_id)
        self.admission, self.context = admission, context
        self.repository, self.scope, self.clock = (
            admission.repository,
            admission.scope,
            admission.clock,
        )
        self.registry = admission.registry
        self.input_work = QuestionInputWork()
        from ..operations.refresh_demand import RefreshDemandQueue
        from .question_refresh import QuestionPageRefreshProcessor, QuestionRefreshProcessor

        self.processor = QuestionRefreshProcessor(self)
        self.page_processor = QuestionPageRefreshProcessor(self)
        self.queue = RefreshDemandQueue(
            (self.processor, self.page_processor), limits=limits, clock=self.clock,
            default_processor_key=self.processor.key,
        )
        from .question_pages import ProjectQuestionPages

        self.pages = ProjectQuestionPages(self)

    async def collect_garbage(self, policy):
        """Explicit trusted-host retention; never exposed through model transports."""
        from .question_gc import collect

        return await collect(self, policy)

    async def _open(self, uow):
        if getattr(uow, "question_runtime_contract", None) != RUNTIME_SCHEMA or any(
            not callable(getattr(uow, name, None))
            for name in ("derived_project_candidates", "derived_project_source_proof")
        ):
            _fail("question_backend_unsupported")
        return await open_derived(uow, self.scope)

    async def _clock_barrier(self, *, observed_at=None):
        # Commit this independently of a denied/expired read. Otherwise rolling
        # back the body transaction would also forget the time it just observed.
        from ..operations.refresh_demand import observed_clock

        async with self.repository.unit_of_work() as uow:
            await self._open(uow)
            return await observed_clock(
                uow, self.scope, self.clock if observed_at is None else lambda: observed_at
            )

    def _context(self):
        if type(self.context) is not QuestionContext or self.context.expires_at <= self.clock():
            _fail("question_context_expired")
        return self.context.payload()

    async def register(
        self,
        question_id,
        project_id,
        question,
        *,
        readers,
        aliases=(),
        overdue_only=False,
        source_basis=SourceBasis.ADMITTED_L1,
        publication_request_ids=(),
        max_output_bytes=262144,
        expected_generation=0,
        refresh_policy=None,
    ):
        from ..operations.refresh_policy import RefreshPolicy
        from ..retrieval.question_router import normalize_alias

        readers, aliases, requests = tuple(readers), tuple(aliases), tuple(publication_request_ids)
        context = QuestionContext.from_payload(self._context())
        registration = self.admission.registration_fingerprint
        if type(expected_generation) is not int or expected_generation < 0:
            _fail("invalid_question_generation")
        if len(aliases) > 32 or any(type(a) is not str for a in aliases):
            _fail("invalid_question_aliases")
        aliases = tuple(sorted({normalize_alias(a) for a in aliases}))
        if source_basis == SourceBasis.PUBLICATION_MANIFEST:
            if not 1 <= len(requests) <= 64 or len(set(requests)) != len(requests):
                _fail("project_publication_target_required")
            for key in requests:
                identity(key)
        elif requests:
            _fail("unexpected_project_publication_manifest")
        policy = refresh_policy or RefreshPolicy(mode="on_demand")
        if type(policy) is not RefreshPolicy:
            _fail("invalid_refresh_policy")
        definition = project_definition(
            self.admission,
            question_id,
            project_id,
            question,
            readers=readers,
            generation=expected_generation + 1,
            overdue_only=overdue_only,
            source_basis=source_basis,
            max_output_bytes=max_output_bytes,
            refresh_policy_id="project-refresh-policy:" + digest(policy.payload()),
        )
        instance = QuestionInstance(
            definition,
            {"project_id": project_id, "overdue_only": overdue_only},
            context,
            readers,
            self.admission.purpose,
            QuestionTime(TimeMode.CURRENT, None, None),
        )
        spec = dict(
            schema=REGISTRATION_SCHEMA,
            id=instance.id,
            question_id=question_id,
            project_id=project_id,
            question=question,
            contract_fingerprint=self.admission.contract.fingerprint,
            registration_fingerprint=registration,
            readers=list(instance.audience),
            purpose=self.admission.purpose,
            context=context.payload(),
            authority_id=self.admission.authority_id,
            parent_facets=[],
            publication_request_ids=list(requests),
            instance=instance.payload(),
            aliases=list(aliases),
            semantic_readset=readset(self.admission.contract, self.scope, project_id, question,
                                    overdue_only=overdue_only),
        )
        async with self.repository.unit_of_work() as uow:
            epoch = await self._open(uow)
            if (
                self._context() != context.payload()
                or self.admission.registration_fingerprint != registration
            ):
                _fail("question_registration_changed")
            old = await uow.derived_get(
                self.scope, "question_registration", registration_key(question_id)
            )
            if (
                old["generation"] if old and old.get("state") != "erased" else 0
            ) != expected_generation:
                _fail("question_registration_conflict")
            if (
                not old
                and len(await uow.derived_records(self.scope, "question_registration")) >= 128
            ):
                _fail("question_registration_capacity")
            authority = await self.registry.authority(uow, self.admission.authority_id)
            self.registry.permission(authority, readers, (self.admission.purpose,))
            if old and old.get("instance_id"):
                previous = await uow.derived_get(self.scope, "definition", old["instance_id"])
                if previous:
                    previous.update(disabled=True, dirty=True)
                    await uow.derived_put(self.scope, "definition", old["instance_id"], previous)
                    await subscriptions.install(uow, self.scope, previous)
            row = dict(
                spec=spec,
                facet_id=instance.id,
                slots=[],
                fingerprint=definition.semantic_fingerprint,
                generation=definition.generation,
                epoch=epoch,
                safety_generation=0,
                time_generation=0,
                dirty=True,
                disabled=False,
            )
            await uow.derived_put(self.scope, "definition", instance.id, row)
            await subscriptions.install(uow, self.scope, row)
            registration_row = _seal(
                dict(
                    schema="question-registration/1",
                    question_id=question_id,
                    instance_id=instance.id,
                    generation=definition.generation,
                    epoch=epoch,
                    spec_sha256=digest(spec),
                    aliases=list(aliases),
                    readers=list(instance.audience),
                    state="registered",
                )
            )
            await uow.derived_put(
                self.scope, "question_registration", registration_key(question_id), registration_row
            )
        # A crash here leaves an unavailable unconfigured definition. No alternate
        # compute path can turn that partial registration into an ungoverned job.
        await self.queue.configure(instance.id, policy)
        return deepcopy(registration_row)

    async def grant(self, grant, *, expected_version=0):
        """Host-only current processing permission with same-transaction dirty work."""
        grant = deepcopy(grant)
        if (
            type(grant) is not ProcessingGrant
            or type(expected_version) is not int
            or expected_version < 0
        ):
            _fail("invalid_question_grant")
        value = grant.payload()
        async with self.repository.unit_of_work() as uow:
            await self._open(uow)
            authority = await self.registry.authority(uow, self.admission.authority_id)
            self.registry.permission(authority, grant.readers, grant.purposes)
            old = await uow.derived_get(self.scope, "grant", grant.source_id)
            if old and old.get("authority_id") not in {None, self.admission.authority_id}:
                _fail("derived_authority_mismatch")
            if old is None and len(await uow.derived_records(self.scope, "grant")) >= 4096:
                _fail("derived_grant_capacity")
            if (old["version"] if old else 0) != expected_version:
                _fail("derived_grant_conflict")
            if await uow.derived_project_source_proof(self.scope, grant.source_id) is None:
                _fail("project_source_unavailable")
            value.update(
                version=expected_version + 1,
                authority_id=self.admission.authority_id,
                authority_version=authority["version"] if authority else None,
            )
            await uow.derived_put(self.scope, "grant", grant.source_id, value)
            from .project_index import source_changed

            await subscriptions.bump(uow, self.scope, subscriptions.SCOPE_BARRIER)
            keys = await subscriptions.source_slots(uow, self.scope, grant.source_id)
            if keys is None:
                await subscriptions.scope_fallback(
                    uow, self.scope, at=self.clock(), reason="grant", safety=True
                )
            else:
                await subscriptions.invalidate(
                    uow,
                    self.scope,
                    tuple(subscriptions.slot_key(key) for key in keys),
                    at=self.clock(),
                    reason="grant",
                    safety=True,
                )
                await source_changed(
                    uow, self.scope, grant.source_id, at=self.clock(), reason="grant", safety=True
                )
            return deepcopy(value)

    async def _definition(self, uow, instance_id):
        row = await uow.derived_get(self.scope, "definition", identity(instance_id))
        if (
            not row
            or row.get("disabled")
            or row.get("epoch") != await uow.retention_epoch(self.scope)
        ):
            _fail("question_definition_unavailable")
        spec = row.get("spec", {})
        if spec.get("schema") != REGISTRATION_SCHEMA:
            _fail("question_definition_unsupported")
        instance = QuestionInstance.from_payload(spec["instance"])
        registration = await uow.derived_get(
            self.scope, "question_registration", registration_key(spec["question_id"])
        )
        _checked(registration, "question-registration/1")
        if (
            registration.get("state") != "registered"
            or registration.get("instance_id") != instance_id
            or registration.get("spec_sha256") != digest(spec)
            or row["facet_id"] != instance.id
            or row["fingerprint"] != instance.definition.semantic_fingerprint
            or row["generation"] != instance.definition.generation
            or instance.definition.scope != self.scope
            or instance.definition.time_mode != TimeMode.CURRENT
            or instance.context.payload() != self._context()
            or spec["registration_fingerprint"] != self.admission.registration_fingerprint
            or spec["contract_fingerprint"] != self.admission.contract.fingerprint
        ):
            _fail("question_registration_changed")
        await subscriptions.subscription(uow, self.scope, row)
        return row

    async def _registration(self, uow, question_id, actor):
        registration = await uow.derived_get(
            self.scope, "question_registration", registration_key(question_id)
        )
        if registration and registration.get("state") == "erased":
            _fail("question_erased")
        _checked(registration, "question-registration/1")
        if registration.get("state") != "registered" or actor not in registration["readers"]:
            _fail("derived_read_denied")
        row = await self._definition(uow, registration["instance_id"])
        await self._authorize(uow, row, actor)
        return row

    async def _authorize(self, uow, definition, actor):
        if actor not in definition["spec"]["readers"]:
            _fail("derived_read_denied")
        authority = await self.registry.authority(uow, self.admission.authority_id)
        self.registry.permission(authority, (actor,), (self.admission.purpose,))

    async def _proof(self, uow, definition, *, at, shared=None):
        """Complete indexed metadata and original processing ACL, before any bodies."""
        from ..operations.refresh_demand import observed_clock

        await observed_clock(uow, self.scope, self.clock)
        spec = definition["spec"]
        if (
            spec["context"] != self._context()
            or spec["registration_fingerprint"] != self.admission.registration_fingerprint
        ):
            _fail("question_registration_changed")
        keys = (*query_keys(spec["contract_fingerprint"], spec["project_id"]),
                subscriptions.FALLBACK_BARRIER)
        generations = {
            key: (await uow.derived_get(self.scope, "barrier", key) or {"generation": 0})[
                "generation"
            ] for key in keys
        }
        epoch = await uow.retention_epoch(self.scope)
        if shared is None:
            headers = await uow.derived_project_candidates(
                self.scope, spec["contract_fingerprint"], spec["project_id"]
            )
            self.input_work.candidate_census_reads += 1
        else:
            headers = await shared.headers(uow, self, definition, at, generations, epoch)
        if len(headers) > MAX_CANDIDATES:
            _fail("project_candidate_capacity")
        relevant = [
            h for h in headers if h["project"]["current_project_id"] in {None, spec["project_id"]}
        ]
        ids = sorted({key for header in relevant for key in header["source_ids"]})
        if len(ids) + len(headers) > MAX_INPUTS:
            _fail("project_input_capacity")
        self.input_work.authorization_checks += 1
        grants = await self.admission._grants(uow, ids, at)
        for grant in grants.values():
            if not set(spec["readers"]) <= set(grant["readers"]):
                _fail("project_processing_denied")
        sources = []
        for source_id in ids:
            self.input_work.source_proof_reads += 1
            source = await uow.derived_project_source_proof(self.scope, source_id)
            if source is None:
                _fail("project_source_unavailable")
            grant = grants[source_id]
            sources.append(
                {
                    **source,
                    "epoch": epoch,
                    "grant_version": grant["version"],
                    "grant_sha256": digest(grant),
                    "authority_id": grant.get("authority_id"),
                    "authority_version": grant.get("authority_version"),
                }
            )
        subscription = await subscriptions.subscription(uow, self.scope, definition)
        authority = await self.registry.authority(uow, self.admission.authority_id)
        manifests = await self.admission._publication(
            uow,
            spec["instance"]["definition"]["source_basis"],
            spec["publication_request_ids"],
            relevant,
        )
        now = instant(self.clock())
        self._input_guard(spec, at)
        if authority:
            self.registry._authority_floor(authority)
            if datetime.fromisoformat(authority["spec"]["expires_at"]) <= now:
                _fail("derived_authority_expired")
        if any(g.get("expires_at") and datetime.fromisoformat(g["expires_at"]) <= now
               for g in grants.values()):
            _fail("project_processing_grant_expired")
        return dict(
            schema="question-input-proof/1",
            epoch=epoch,
            registration_fingerprint=spec["registration_fingerprint"],
            context_fingerprint=digest(spec["context"]),
            candidates=deepcopy(list(headers)),
            sources=sources,
            query_generations=generations,
            subscription_sha256=subscription["sha256"],
            authority_sha256=digest(authority),
            publication_manifests=[digest(m) for m in manifests],
        )

    async def _generation_guard(self, uow, definition, generation_proof):
        """Check original inputs even when current evidence contains equal public values.

        This method reads source/grant metadata only. A missing legacy proof may
        never be promoted to permission to load/reuse its restricted body.
        """
        if (
            not generation_proof
            or generation_proof.get("schema") != "question-input-proof/1"
            or generation_proof["epoch"] != await uow.retention_epoch(self.scope)
            or generation_proof["registration_fingerprint"]
            != self.admission.registration_fingerprint
        ):
            _fail("question_original_generation_unavailable")
        originals = {s["source_event_id"]: s for s in generation_proof["sources"]}
        self.input_work.authorization_checks += 1
        grants = await self.admission._grants(uow, originals, instant(self.clock()))
        for source_id, original in originals.items():
            if not set(definition["spec"]["readers"]) <= set(grants[source_id]["readers"]):
                _fail("project_processing_denied")
            self.input_work.source_proof_reads += 1
            current = await uow.derived_project_source_proof(self.scope, source_id)
            if current is None or any(current[k] != original.get(k) for k in current):
                _fail("question_original_generation_changed")
        authority = await self.registry.authority(uow, self.admission.authority_id)
        boundaries = [
            datetime.fromisoformat(g["expires_at"]) for g in grants.values() if g.get("expires_at")
        ]
        if authority:
            boundaries.append(datetime.fromisoformat(authority["spec"]["expires_at"]))
        # The last metadata await can cross an expiry or host-policy change.
        # Recheck synchronously before this guard permits any cached body read.
        now = instant(self.clock())
        if any(boundary <= now for boundary in boundaries):
            _fail("project_processing_grant_expired")
        if (
            self._context() != definition["spec"]["context"]
            or self.admission.registration_fingerprint
            != generation_proof["registration_fingerprint"]
        ):
            _fail("question_registration_changed")
        return min(boundaries).isoformat() if boundaries else None

    async def _cached_body(self, uow, kind, key, *, guard):
        """Every cached body crosses its own current-control boundary.

        A prior successful guard cannot authorize another body after an awaited
        read advances the clock or changes host context/registration.
        """
        await guard()
        body = await uow.derived_get(self.scope, kind, key)
        await guard()
        return body

    async def _baseline(self, uow, definition, proof, *, expected_head=None):
        header = (
            await uow.derived_get(self.scope, "question_head", definition["facet_id"])
            if expected_head is None
            else expected_head
        )
        empty = dict(
            expected_head=header,
            generation_safe=False,
            generation_until=None,
            delta_state=None,
            previous_content=None,
            change_logs={},
        )
        if not header:
            return empty
        _checked(header, "question-head-proof/1")
        try:
            generation_until = await self._generation_guard(
                uow, definition, header.get("generation_proof")
            )
        except DerivedError:
            # Independent full generation may still use the current authorized
            # census. Do not load old body/state to discover that it is unsafe.
            return empty

        async def guard():
            return await self._generation_guard(uow, definition, header["generation_proof"])

        content = await self._cached_body(
            uow, "question_content", header["head"]["content_revision_id"], guard=guard
        )
        state = await self._cached_body(
            uow, "question_delta_state", definition["facet_id"], guard=guard
        )
        if digest(content) != header["content_sha256"]:
            _fail("question_content_changed")
        if state is not None and digest(state) != header.get("delta_state_sha256"):
            # A corrupt optimization baseline is not proof. Rebuild, while the
            # independently guarded current source census remains authoritative.
            state = None
        logs = {
            key: await uow.derived_get(self.scope, "question_change_log", key)
            for key in proof["query_generations"]
        }
        await guard()
        return dict(
            expected_head=header,
            generation_safe=True,
            generation_until=generation_until,
            delta_state=state,
            previous_content=content,
            change_logs=logs,
        )

    async def model_input_header(self, uow, question_id, *, actor):
        """Guarded metadata-only union for model input, dispatch and delivery gates."""
        definition = await self._registration(uow, question_id, actor)
        header = await uow.derived_get(self.scope, "question_head", definition["facet_id"])
        if header is None:
            _fail("question_view_unavailable")
        await self._guard(uow, definition, header, actor=actor)
        sources = {
            s["source_event_id"]
            for p in (header["proof"], header["generation_proof"])
            for s in p["sources"]
        }
        return deepcopy(
            dict(
                definition=definition,
                header=header,
                original_generation_manifest=header["generation_manifest"],
                source_ids=sorted(sources),
            )
        )

    @staticmethod
    def _unit(definition, proof):
        return dict(
            schema="question-refresh-unit/1",
            facet_id=definition["facet_id"],
            epoch=definition["epoch"],
            definition_generation=definition["generation"],
            definition_fingerprint=definition["fingerprint"],
            time_generation=definition["time_generation"],
            safety_generation=definition["safety_generation"],
            proof_sha256=digest(proof),
        )

    def _input_guard(self, spec, minimum):
        if instant(self.clock()) < minimum:
            _fail("refresh_clock_discontinuity")
        if (
            self._context() != spec["context"]
            or self.admission.registration_fingerprint != spec["registration_fingerprint"]
        ):
            _fail("question_registration_changed")

    async def _census(self, uow, definition, proof, context, *, shared, minimum=None):
        """Reuse qualified bytes only behind the caller's fresh complete proof."""
        spec = definition["spec"]
        minimum = context.valid_at if minimum is None else minimum
        self._input_guard(spec, minimum)
        key = shared.census_key(self, definition, proof, context)
        census = shared.census(uow, key, context)
        if census is None:
            census = await self.admission._snapshot(
                uow, context, at=context.valid_at,
                source_basis=spec["instance"]["definition"]["source_basis"],
                publication_request_ids=tuple(spec["publication_request_ids"]),
                input_guard=lambda: self._input_guard(spec, minimum),
                candidate_headers=proof["candidates"],
            )
            shared.remember(uow, key, census)
        # A shared immutable input never extends its source/authority/time grant.
        now = instant(self.clock())
        if census.next_transition_at is not None and now >= census.next_transition_at:
            _fail("derived_time_coverage_expired")
        self._input_guard(spec, minimum)
        return census

    @staticmethod
    def _batch(values):
        values = tuple(values)
        if not 1 <= len(values) <= MAX_BATCH:
            _fail("question_batch_capacity")
        return values

    async def snapshot_many(self, tasks):
        """Capture up to eight already-leased jobs atomically; no alternate scheduler.

        Compatible views share the qualified census, with individual job tokens,
        manifests and fenced publication still owned by the usual runtime.
        """
        tasks = deepcopy(self._batch(tasks))
        if len({task.id for task in tasks}) != len(tasks):
            _fail("question_batch_duplicate")
        observed = await self._clock_barrier()
        try:
            async with self.repository.unit_of_work() as uow:
                await self._open(uow)
                shared = SharedQuestionInputs(uow, self.input_work)
                at = instant(self.clock())
                snapshots = [await self._snapshot_in_uow(uow, task, shared=shared, at=at)
                             for task in tasks]
                # Host controls and expiry may change at any awaited boundary.
                for snapshot in snapshots:
                    proof = await self._proof(uow, snapshot["definition"], at=at, shared=shared)
                    if proof != snapshot["proof"]:
                        _fail("derived_snapshot_changed")
                now = instant(self.clock())
                for snapshot in snapshots:
                    self._input_guard(snapshot["definition"]["spec"], at)
                    until = snapshot["census"].next_transition_at
                    if until is not None and now >= until:
                        _fail("derived_time_coverage_expired")
                return snapshots
        except BaseException:
            await self._failed_batch_clock(observed)
            raise

    async def _failed_batch_clock(self, observed):
        try:
            await self._clock_barrier(observed_at=max(observed, instant(self.clock())))
        except DerivedError as error:
            if error.code != "refresh_clock_discontinuity":
                raise

    async def _guard_batch(self, uow, entries, *, actor, shared, minimum):
        """One *fresh* final guard per exact common proof, never a saved decision.

        All members already passed their own body guards in this locked UoW.
        Revalidate individual registrations/heads/subscriptions, then run the
        full current source, original-lineage and authorization scan once per
        identical group. A final synchronous time/host check covers every member.
        """
        groups = {}
        for definition, header in entries:
            _checked(header, "question-head-proof/1")
            current = await self._definition(uow, definition["facet_id"])
            if (current != definition or await uow.derived_get(
                self.scope, "question_head", definition["facet_id"]
            ) != header):
                _fail("question_view_stale")
            subscription = await uow.derived_get(self.scope, "subscription", definition["facet_id"])
            if (subscription["sha256"] != header["proof"]["subscription_sha256"]
                    or self._unit(definition, header["proof"]) != header["unit"]):
                _fail("question_view_stale")
            common = {k: v for k, v in header["proof"].items() if k != "subscription_sha256"}
            original = {k: v for k, v in header["generation_proof"].items()
                        if k != "subscription_sha256"}
            key = digest([compatibility(self, definition, None), common, original])
            groups.setdefault(key, (definition, header))
        for definition, header in groups.values():
            await self._guard(uow, definition, header, actor=actor, shared=shared)
        now = instant(self.clock())
        for definition, header in entries:
            self._input_guard(definition["spec"], minimum)
            check_time(header, now)

    async def _batch_lease_deadlines(self, uow, tasks):
        """Read authenticated durable lease bounds after all batch publications.

        Heartbeat renews these rows without replacing the caller's WorkerTask.
        Its optional lease_expires_at field is therefore never lease authority.
        Completed rows still need to remain inside the publication lease until
        this transaction commits; ordinary completion reads alone skip that bound.
        """
        from ..operations.facet_refresh import stale

        deadlines = []
        for task in tasks:
            job = await self.processor.check_task(uow, task, completed=True)
            execution_id = task.payload.get("refresh_execution")
            execution = await uow.derived_get(self.scope, "refresh_execution", execution_id)
            if (
                job.get("id") != task.id
                or job.get("status") != "completed"
                or job.get("refresh_execution") != execution_id
                or not execution
                or execution.get("schema") != "refresh-execution/1"
                or execution.get("id") != execution_id
                or execution.get("status") != "completed"
                or execution.get("adapter_key") != self.processor.key
                or execution.get("unit_id") != job["id"]
                or execution.get("unit") != job["unit"]
                or execution.get("epoch") != job["unit"]["epoch"]
                or execution.get("generation") != job["generation"]
                or execution.get("fence") != job["fence"]
            ):
                raise stale()
            try:
                deadlines.extend(instant(datetime.fromisoformat(row[key]))
                                 for row in (job, execution)
                                 for key in ("lease_until", "expires_at"))
            except (KeyError, TypeError, ValueError) as error:
                raise stale() from error
        return tuple(deadlines)

    async def publish_many(self, tasks, snapshots, prepared):
        """Atomic batch over the same per-job lease, input and output CAS checks."""
        tasks, snapshots, prepared = deepcopy((self._batch(tasks), tuple(snapshots),
                                               tuple(prepared)))
        if len(snapshots) != len(tasks) or len(prepared) != len(tasks):
            _fail("question_batch_capacity")
        if len({task.id for task in tasks}) != len(tasks):
            _fail("question_batch_duplicate")
        observed = await self._clock_barrier()
        try:
            async with self.repository.unit_of_work() as uow:
                shared = SharedQuestionInputs(uow, self.input_work)
                entries = []
                results = [await self._publish_in_uow(
                    uow, task, snapshot, result, shared=shared, guarded_entries=entries
                ) for task, snapshot, result in zip(tasks, snapshots, prepared)]
                # Audiences may differ between incompatible groups. Their usual
                # processing checks still cover every registered reader.
                audiences = {}
                for entry in entries:
                    audiences.setdefault(entry[0]["spec"]["readers"][0], []).append(entry)
                for actor, values in audiences.items():
                    await self._guard_batch(uow, values, actor=actor, shared=shared, minimum=observed)
                deadlines = await self._batch_lease_deadlines(uow, tasks)
                from ..operations.refresh_demand import observed_clock

                observed = await observed_clock(uow, self.scope, self.clock)
                for definition, _ in entries:
                    self._input_guard(definition["spec"], observed)
                # Last clock sample follows every awaited read and host guard;
                # no caller-owned task timestamp can extend a durable boundary.
                now = instant(self.clock())
                if now < observed:
                    _fail("refresh_clock_discontinuity")
                if any(now >= deadline for deadline in deadlines):
                    from ..operations.facet_refresh import stale

                    raise stale()
                for _, header in entries:
                    check_time(header, now)
                return results
        except BaseException:
            await self._failed_batch_clock(observed)
            raise

    async def read_many(self, question_ids, *, actor):
        """Read a bounded compatible set with independent current delivery guards."""
        question_ids = self._batch(question_ids)
        if len(set(question_ids)) != len(question_ids):
            _fail("question_batch_duplicate")
        for question_id in question_ids:
            identity(question_id)
        identity(actor)
        observed = await self._clock_barrier()
        try:
            async with self.repository.unit_of_work() as uow:
                await self._open(uow)
                shared = SharedQuestionInputs(uow, self.input_work)
                headers = []
                results = [await self._read_in_uow(
                    uow, key, actor=actor, shared=shared, guarded_entries=headers
                ) for key in question_ids]
                await self._guard_batch(uow, headers, actor=actor, shared=shared, minimum=observed)
                return results
        except BaseException:
            await self._failed_batch_clock(observed)
            raise

    async def snapshot(self, task):
        task = deepcopy(task)
        observed = await self._clock_barrier()
        try:
            return await self._snapshot(task)
        except BaseException:
            observed = max(observed, instant(self.clock()))
            try:
                await self._clock_barrier(observed_at=observed)
            except DerivedError as error:
                if error.code != "refresh_clock_discontinuity":
                    raise
            raise

    async def _snapshot(self, task):
        async with self.repository.unit_of_work() as uow:
            return await self._snapshot_in_uow(
                uow, task, shared=SharedQuestionInputs(uow, self.input_work)
            )

    async def _snapshot_in_uow(self, uow, task, *, shared, at=None):
        await self._open(uow)
        job = await self.processor.check_task(uow, task)
        definition = await self._definition(uow, job["unit"]["facet_id"])
        at = instant(self.clock()) if at is None else at
        proof = await self._proof(uow, definition, at=at, shared=shared)
        if self._unit(definition, proof) != job["unit"]:
            _fail("derived_snapshot_changed")
        spec = deepcopy(definition["spec"])
        execution = await uow.derived_get(
            self.scope, "refresh_execution", job["refresh_execution"]
        )
        refresh_policy = RefreshPolicyRef(
            "project-refresh-policy:" + digest(execution["policy"]), 1
        ).payload()
        context = QueryContext(
            self.admission.principal,
            self.scope,
            spec["project_id"],
            spec["purpose"],
            at,
            at,
            tuple(
                ContextAttribute(name, value, self.context.issuer_id)
                for name, value in self.context.attributes.items()
            ),
            self.admission.contract.timezone,
            "question-snapshot:" + digest(job["unit"]),
        )
        census = await self._census(uow, definition, proof, context, shared=shared)
        if list(census.source_proofs) != proof["sources"] or list(
            census.candidate_versions
        ) != [(h["id"], h["version"], h["version"]) for h in proof["candidates"]]:
            _fail("derived_snapshot_changed")
        if await self._proof(uow, definition, at=at, shared=shared) != proof:
            _fail("derived_snapshot_changed")
        frozen = deepcopy(
            dict(
                instance=spec["instance"],
                refresh_policy=refresh_policy,
                definition=definition,
                proof=proof,
                census=census,
                unit=job["unit"],
                unit_id=job["id"],
                question_id=spec["question_id"],
                question=spec["question"],
                overdue_only=spec["instance"]["parameters"]["overdue_only"],
                **await self._baseline(uow, definition, proof),
            )
        )
        # Bind the complete owned snapshot to the fenced job. A caller may
        # recompute deterministic IDs after changing context or question;
        # recomputing output does not authorize those altered coordinates.
        job["snapshot_sha256"] = digest(to_jsonable(frozen))
        await uow.derived_put(self.scope, "job", job["id"], job)
        self._input_guard(spec, at)
        return frozen

    def prepare(self, snapshot):
        snapshot = deepcopy(snapshot)
        return materialize(self.admission.contract, snapshot)

    async def publish(self, task, snapshot, prepared):
        task, snapshot, prepared = deepcopy((task, snapshot, prepared))
        observed = await self._clock_barrier()
        try:
            return await self._publish(task, snapshot, prepared)
        except BaseException:
            observed = max(observed, instant(self.clock()))
            try:
                await self._clock_barrier(observed_at=observed)
            except DerivedError as error:
                if error.code != "refresh_clock_discontinuity":
                    raise
            raise

    async def _publish(self, task, snapshot, prepared):
        async with self.repository.unit_of_work() as uow:
            return await self._publish_in_uow(
                uow, task, snapshot, prepared, shared=SharedQuestionInputs(uow, self.input_work)
            )

    async def _publish_in_uow(
        self, uow, task, snapshot, prepared, *, shared, guarded_entries=None
    ):
        if prepared != self.prepare(snapshot):
            _fail("derived_output_invalid")
        content = QuestionContent.from_payload(prepared["content"])
        certificate = QuestionCertificate.from_payload(prepared["certificate"])
        epoch = await self._open(uow)
        job = await self.processor.check_task(uow, task)
        if digest(to_jsonable(snapshot)) != job.get("snapshot_sha256"):
            _fail("derived_input_changed")
        definition = await self._definition(uow, job["unit"]["facet_id"])
        now = instant(self.clock())
        proof = await self._proof(uow, definition, at=now, shared=shared)
        if (
            self._unit(definition, proof) != job["unit"]
            or proof != snapshot["proof"]
            or job["unit"] != snapshot["unit"]
            or job["id"] != snapshot["unit_id"]
            or definition["spec"] != snapshot["definition"]["spec"]
            or content.instance.payload() != definition["spec"]["instance"]
        ):
            _fail("derived_snapshot_changed")
        if (
            not certificate.time_coverage.valid_from
            <= now
            < certificate.time_coverage.valid_until
        ):
            _fail("derived_time_coverage_expired")
        if (
            await uow.derived_get(self.scope, "question_head", definition["facet_id"])
            != snapshot["expected_head"]
        ):
            _fail("derived_head_conflict")
        # Re-establish the actual host-reviewed semantic input. A caller may
        # mutate a returned snapshot, even while keeping its metadata proof.
        # Recompute under this same locked snapshot coordinate before commit.
        actual_census = await self._census(
            uow, definition, proof, snapshot["census"].snapshot.context, shared=shared,
            minimum=now,
        )
        if actual_census != snapshot["census"]:
            _fail("derived_input_changed")
        execution = await uow.derived_get(
            self.scope, "refresh_execution", job["refresh_execution"]
        )
        expected_policy = RefreshPolicyRef(
            "project-refresh-policy:" + digest(execution["policy"]), 1
        ).payload()
        if snapshot["refresh_policy"] != expected_policy:
            _fail("derived_snapshot_changed")
        baseline = await self._baseline(uow, definition, proof)
        if any(snapshot.get(k) != v for k, v in baseline.items()):
            _fail("derived_input_changed")
        certificate.validate_content_binding(content)
        if (
            not prepared["reused"]
            and len(await uow.derived_records(self.scope, "question_content")) >= 4096
        ):
            _fail("question_content_capacity")
        if len(await uow.derived_records(self.scope, "question_certificate")) >= 4096:
            _fail("question_certificate_capacity")
        head = QuestionHead(
            content.instance.id,
            self.scope,
            content.id,
            certificate.id,
            content.instance.definition.semantic_fingerprint,
            content.instance.definition.generation,
            epoch,
        )
        edges = set()
        for ref in content.generation_manifest.inputs:
            edges.add(
                (
                    "query" if ref.kind == "query" else "processing",
                    "facet:" + ref.id
                    if ref.kind == "query"
                    else "derived:" + ref.id
                    if ref.kind.startswith("derived_")
                    else ref.kind + ":" + ref.id,
                )
            )
        for ref in certificate.validation_manifest.inputs:
            edges.add(
                (
                    "query" if ref.kind == "query" else "processing",
                    "facet:" + ref.id
                    if ref.kind == "query"
                    else "derived:" + ref.id
                    if ref.kind.startswith("derived_")
                    else ref.kind + ":" + ref.id,
                )
            )
        edges.update(("support", ref.kind + ":" + ref.id) for ref in certificate.support)
        for kind, key, value in (
            ("question_content", content.id, content.payload()),
            ("question_certificate", certificate.id, certificate.payload()),
        ):
            await uow.derived_put(self.scope, kind, key, value)
            await uow.derived_edges(self.scope, key, sorted(edges))
        header = _seal(
            dict(
                schema="question-head-proof/1",
                facet_id=definition["facet_id"],
                head=head.payload(),
                proof=proof,
                unit=job["unit"],
                content_sha256=digest(content.payload()),
                certificate_sha256=digest(certificate.payload()),
                generation_manifest_sha256=content.generation_manifest_digest,
                generation_manifest=content.generation_manifest.payload(),
                generation_proof=prepared["generation_proof"],
                delta_state_sha256=digest(prepared["delta_state"]),
                digests=content_digests(content, certificate),
                compute_mode=prepared["trace"]["compute_mode"],
                compute_trace=prepared["trace"],
                result_metadata=prepared["result_metadata"],
                validated_at=certificate.validated_at.isoformat(),
                next_transition_at=prepared["next_transition_at"],
            )
        )
        await uow.derived_put(
            self.scope, "question_delta_state", definition["facet_id"], prepared["delta_state"]
        )
        await uow.derived_put(self.scope, "question_head", definition["facet_id"], header)
        await uow.derived_edges(self.scope, definition["facet_id"], sorted(edges))
        await self.pages.parent_published(uow, definition["facet_id"], header)
        definition.update(next_transition_at=prepared["next_transition_at"],
                          semantic_dirty=False, proof_dirty=False)
        await uow.derived_put(self.scope, "definition", definition["facet_id"], definition)
        outcome = "noop" if prepared["reused"] else "applied"
        token = "derived-commit:" + digest(
            [self.scope.partition_key(), job["unit"], content.id, outcome]
        )
        job.update(
            status="completed",
            outcome=outcome,
            no_outputs=False,
            revision_id=content.id,
            commit_token=token,
            completed_at=now.isoformat(),
            certificate_revision_id=certificate.id,
            certificate_sha256=digest(certificate.payload()),
            content_sha256=digest(content.payload()),
        )
        await uow.derived_put(self.scope, "job", job["id"], job)
        from ..operations.refresh_demand import publish_coverage

        await publish_coverage(
            uow,
            self,
            job,
            definition,
            manifest={
                "schema": "question-publication/1",
                "unit": job["unit"],
                "query_complete": True,
                "head_sha256": header["sha256"],
                "generation_manifest": content.generation_manifest.payload(),
            },
            now=now,
        )
        # Final clock/context/lease safety check while the same publication
        # transaction still owns all compare-and-swap coordinates.
        await self._generation_guard(uow, definition, header["generation_proof"])
        from ..operations.refresh_demand import observed_clock

        observed = await observed_clock(uow, self.scope, self.clock)
        now = instant(self.clock())
        if now < observed:
            _fail("refresh_clock_discontinuity")
        check_time(header, now)
        if any(
            now >= datetime.fromisoformat(job[key])
            for key in ("lease_until", "expires_at")
            if job.get(key)
        ):
            from ..operations.facet_refresh import stale

            raise stale()
        if (
            self._context() != definition["spec"]["context"]
            or self.admission.registration_fingerprint != proof["registration_fingerprint"]
        ):
            _fail("question_registration_changed")
        if guarded_entries is not None:
            guarded_entries.append((definition, header))
        return dict(
            outcome=outcome,
            no_outputs=False,
            revision_id=content.id,
            certificate_revision_id=certificate.id,
            commit_token=token,
        )

    async def _guard(self, uow, definition, header, *, actor, shared=None):
        _checked(header, "question-head-proof/1")
        current = await self._definition(uow, definition["facet_id"])
        if (
            current != definition
            or await uow.derived_get(self.scope, "question_head", definition["facet_id"]) != header
        ):
            _fail("question_view_stale")
        await self._authorize(uow, definition, actor)
        await self._generation_guard(uow, definition, header.get("generation_proof"))
        check_time(header, self.clock())
        proof = await self._proof(uow, definition, at=instant(self.clock()), shared=shared)
        if proof != header["proof"] or self._unit(definition, proof) != header["unit"]:
            _fail("question_view_stale")
        head = QuestionHead.from_payload(header["head"])
        if (
            head.instance_id != definition["facet_id"]
            or head.epoch != definition["epoch"]
            or head.definition_fingerprint != definition["fingerprint"]
        ):
            _fail("question_view_stale")
        from ..operations.refresh_demand import observed_clock

        observed = await observed_clock(uow, self.scope, self.clock)
        now = instant(self.clock())
        if now < observed:
            _fail("refresh_clock_discontinuity")
        check_time(header, now)
        if (
            self._context() != definition["spec"]["context"]
            or self.admission.registration_fingerprint
            != header["proof"]["registration_fingerprint"]
        ):
            _fail("question_registration_changed")
        return head

    async def read(self, question_id, *, actor, valid_at=None, known_at=None):
        if valid_at is not None or known_at is not None:
            _fail("question_historical_unsupported")
        identity(question_id)
        identity(actor)
        observed = await self._clock_barrier()
        try:
            return await self._read_current(question_id, actor=actor)
        except BaseException:
            # A time/ACL failure may have happened after loading the body. Keep
            # that observation even though the failed body UoW was rolled back.
            observed = max(observed, instant(self.clock()))
            try:
                await self._clock_barrier(observed_at=observed)
            except DerivedError as clock_error:
                if clock_error.code != "refresh_clock_discontinuity":
                    raise
            raise

    async def _read_current(self, question_id, *, actor):
        async with self.repository.unit_of_work() as uow:
            await self._open(uow)
            return await self._read_in_uow(
                uow, question_id, actor=actor, shared=SharedQuestionInputs(uow, self.input_work)
            )

    async def _read_in_uow(
        self, uow, question_id, *, actor, shared=None, guarded_entries=None
    ):
        definition = await self._registration(uow, question_id, actor)
        header = await uow.derived_get(self.scope, "question_head", definition["facet_id"])
        if header is None:
            _fail("question_view_unavailable")
        head = await self._guard(uow, definition, header, actor=actor, shared=shared)

        # No answer body is loaded until complete original-generation and
        # current-census metadata, source ACL, context and time checks pass.
        async def guard():
            return await self._guard(uow, definition, header, actor=actor, shared=shared)

        body = await self._cached_body(
            uow, "question_content", head.content_revision_id, guard=guard
        )
        cert = await self._cached_body(
            uow, "question_certificate", head.certificate_revision_id, guard=guard
        )
        if digest(body) != header["content_sha256"] or digest(cert) != header["certificate_sha256"]:
            _fail("question_content_changed")
        content, certificate = (
            QuestionContent.from_payload(body),
            QuestionCertificate.from_payload(cert),
        )
        certificate.validate_content_binding(content)
        if (
            content.id != head.content_revision_id
            or certificate.id != head.certificate_revision_id
            or content.generation_manifest_digest != header["generation_manifest_sha256"]
            or certificate.safety_fingerprint != digest(header["proof"])
        ):
            _fail("question_content_changed")
        result = response(
            content,
            certificate,
            question_id,
            metadata=header.get("result_metadata"),
            trace=header.get("compute_trace"),
        )
        from ..operations.refresh_demand import _compatibility

        demand_id = "refresh-demand:" + digest(
            [self.scope.partition_key(), definition["facet_id"], _compatibility(definition)]
        )
        demand = await uow.derived_get(self.scope, "refresh_demand", demand_id)
        result["refresh_status"] = refresh_state((demand or {}).get("status"))
        budget(result, content.instance.definition.max_output_bytes)
        await self._guard(uow, definition, header, actor=actor, shared=shared)
        from ..operations.refresh_demand import observed_clock, record_guarded_read

        await record_guarded_read(uow, self.scope, definition, {"state": "ready"}, at=self.clock())
        observed = await observed_clock(uow, self.scope, self.clock)
        now = instant(self.clock())
        if now < observed:
            _fail("refresh_clock_discontinuity")
        check_time(header, now)
        if (
            self._context() != definition["spec"]["context"]
            or self.admission.registration_fingerprint
            != header["proof"]["registration_fingerprint"]
        ):
            _fail("question_registration_changed")
        if guarded_entries is not None:
            guarded_entries.append((definition, header))
        return result

    async def request(self, question_id, *, actor, dedupe_key, deadline=None):
        async with self.repository.unit_of_work() as uow:
            await self._open(uow)
            definition = await self._registration(uow, question_id, actor)
            instance_id = definition["facet_id"]
        return await self.queue.request(
            instance_id, actor=actor, dedupe_key=dedupe_key, deadline=deadline
        )

    async def answer(self, question_id, *, actor, dedupe_key, max_steps=1):
        """Bounded direct work uses the same demand, quota, lease and publication."""
        if type(max_steps) is not int or not 0 <= max_steps <= 8:
            _fail("invalid_question_work_budget")
        try:
            return await self.read(question_id, actor=actor)
        except DerivedError as error:
            if error.code not in {
                "question_view_unavailable",
                "question_view_stale",
                "derived_time_coverage_expired",
                "project_processing_denied",
                "project_processing_grant_expired",
                "question_original_generation_unavailable",
                "question_original_generation_changed",
            }:
                raise
        receipt = await self.request(question_id, actor=actor, dedupe_key=dedupe_key)
        for _ in range(max_steps):
            lease = await self.queue.claim(
                "question-direct", lease_seconds=30, target_id=receipt["target_id"]
            )
            if lease is None:
                break
            try:
                await self.queue.apply(lease.task)
                await self.queue.complete(lease)
            except Exception as error:
                await self.queue.fail(lease, error)
                raise
            try:
                return await self.read(question_id, actor=actor)
            except DerivedError as error:
                if error.code not in {
                    "question_view_unavailable",
                    "question_view_stale",
                    "derived_time_coverage_expired",
                    "project_processing_denied",
                    "project_processing_grant_expired",
                }:
                    raise
        state = await self.queue.status(receipt["target_id"], actor=actor)
        async with self.repository.unit_of_work() as uow:
            await self._open(uow)
            definition = await self._registration(uow, question_id, actor)
            maximum = definition["spec"]["instance"]["definition"]["max_output_bytes"]
        return budget(
            dict(
                schema="question-answer/1",
                question_id=question_id,
                availability_status="stale",
                refresh_status=refresh_state(state["state"]),
                answer_status=None,
                receipt=receipt,
                model_calls=0,
            ),
            maximum,
        )
