"""Replay retained admission versions with frozen business definitions and live ACL.

History starts at an explicit opt-in registration, not a claimed past coverage.
Immutable version rows supply system-time truth; valid-time support is reevaluated.
Reads create no question/page publication and never resurrect erased evidence.
"""

from copy import deepcopy

from ..conditions import Condition, ContextAttribute, QueryContext
from ..consolidation.admission import authority_from_payload, authority_to_payload
from ..consolidation.project_admission import ProjectAdmission, ProjectMembership
from ..ontology.rules import RelationRule
from ..serialization import to_jsonable
from .model import DerivedError, digest, identity
from .project_questions import ProjectDomainContract, ProjectRiskRule, full_project_question
from .question_history import KINDS, instant
from .question_materialize import budget
from .relation_questions import DependencyRiskPlan

SCHEMA = "project-history-registration/1"


def _contract(payload):
    value = deepcopy(payload)
    value["risk_rules"] = tuple(
        ProjectRiskRule(
            **{
                **rule,
                "impact_conditions": tuple(Condition(**c) for c in rule["impact_conditions"]),
            }
        )
        for rule in value["risk_rules"]
    )
    value["relation_plans"] = tuple(
        DependencyRiskPlan(
            **{
                **plan,
                "impact_conditions": tuple(Condition(**c) for c in plan["impact_conditions"]),
                "relation_rules": tuple(RelationRule(**rule) for rule in plan["relation_rules"]),
            }
        )
        for plan in value.get("relation_plans", ())
    )
    return ProjectDomainContract(**value)


class ProjectHistory:
    """Trusted registration hooks, deterministic bitemporal read-only rebuilding."""

    def __init__(self, service):
        self.service, self.scope, self.repository = service, service.scope, service.repository

    async def register(self, uow, label, definition, *, kind="question"):
        if not self.service.history_rebuild:
            return
        from ..operations.refresh_demand import observed_clock

        observed = await observed_clock(uow, self.scope, self.service.clock)
        admission = self.service.admission
        row = dict(
            schema=SCHEMA,
            state="registered",
            identity=label,
            kind=kind,
            known_at=observed.isoformat(),
            epoch=await uow.retention_epoch(self.scope),
            definition=deepcopy(definition),
            context=deepcopy(self.service._context()),
            principal=admission.principal,
            purpose=admission.purpose,
            contract=to_jsonable(admission.contract),
            reviewer_version=admission.reviewer_version,
            authorities=[authority_to_payload(a) for a in admission.authorities.values()],
            memberships=[to_jsonable(m) for m in admission.memberships.values()],
            sources=[],
            parents=[],
        )
        row["sha256"] = digest(row)
        key = "project-history-registration:" + digest(
            [kind, label, row["known_at"], row["sha256"]]
        )
        records = await uow.derived_records(self.scope, KINDS[0])
        if len(records) >= 4096:
            raise DerivedError("question_history_capacity")
        await uow.derived_put(self.scope, KINDS[0], key, row)

    async def layout(self, uow, registration, blocks):
        """All page full/patch publications retain layout changes, not answer copies."""
        if not self.service.history_rebuild:
            return
        value = dict(
            registration_sha256=registration["sha256"],
            blocks=[
                dict(block_id=block["block_id"], question_id=block["body"]["answer"]["question_id"])
                for block in blocks
            ],
        )
        previous = [
            r["payload"]
            for r in await uow.derived_records(self.scope, KINDS[0])
            if r["payload"].get("schema") == SCHEMA
            and r["payload"].get("identity") == registration["page_id"]
            and r["payload"].get("kind") == "page_layout"
        ]
        if previous and max(previous, key=lambda r: instant(r["known_at"]))["definition"] == value:
            return
        await self.register(uow, registration["page_id"], value, kind="page_layout")

    async def _definition(self, uow, label, kind, known):
        records = [
            r["payload"]
            for r in await uow.derived_records(self.scope, KINDS[0])
            if r["payload"].get("schema") == SCHEMA
            and r["payload"].get("identity") == label
            and r["payload"].get("kind") == kind
        ]
        if not records or known < min(instant(r["known_at"]) for r in records):
            raise DerivedError("question_history_coverage_floor")
        records = [r for r in records if instant(r["known_at"]) <= known]
        # Updates sharing one timestamp are ambiguous, not an arbitrary hash sort.
        latest = max(instant(r["known_at"]) for r in records)
        choices = [r for r in records if instant(r["known_at"]) == latest]
        if len(choices) != 1:
            raise DerivedError("question_history_registration_time_ambiguous")
        row = choices[0]
        if (
            row.get("state") != "registered"
            or row["epoch"] != await uow.retention_epoch(self.scope)
            or row.get("sha256") != digest({k: v for k, v in row.items() if k != "sha256"})
        ):
            raise DerivedError("question_history_registration_unavailable")
        return row

    def _admission(self, row):
        current = self.service.admission
        if row["principal"] != current.principal or row["purpose"] != current.purpose:
            raise DerivedError("question_history_authority_changed")
        return ProjectAdmission(
            current.engine,
            self.scope,
            principal=row["principal"],
            contract=_contract(row["contract"]),
            authorities=tuple(authority_from_payload(a) for a in row["authorities"]),
            memberships=tuple(ProjectMembership(**m) for m in row["memberships"]),
            reviewer_version=row["reviewer_version"],
            purpose=row["purpose"],
            clock=current.clock,
            authority_id=current.authority_id,
            authority_min_version=current.authority_min_version,
        )

    async def read(self, label, *, actor, known_at, valid_at, kind="question"):
        if not self.service.history_rebuild:
            raise DerivedError("question_historical_unsupported")
        identity(label)
        identity(actor)
        known, valid = instant(known_at), instant(valid_at)
        observed = await self.service._clock_barrier()
        try:
            if known > observed:
                raise DerivedError("question_history_future_known_time")
            async with self.repository.unit_of_work() as uow:
                await self.service._open(uow)
                controls = self.service.history._controls()
                if kind == "question":
                    result = await self._question(uow, label, actor, known, valid, controls)
                elif kind == "page":
                    result = await self._page(uow, label, actor, known, valid, controls)
                else:
                    raise DerivedError("question_history_kind_unsupported")
                self.service._input_guard(controls, observed)
                return result
        except BaseException:
            await self.service._failed_batch_clock(observed)
            raise

    async def _question(self, uow, label, actor, known, valid, controls):
        await self.service._registration(uow, label, actor)  # Current identity and ACL.
        archive = await self._definition(uow, label, "question", known)
        definition, context = archive["definition"], archive["context"]
        if actor not in definition["spec"]["readers"]:
            raise DerivedError("derived_read_denied")
        if known >= instant(context["expires_at"]):
            raise DerivedError("question_history_context_coverage_unavailable")
        admission = self._admission(archive)
        spec = definition["spec"]
        if spec["instance"]["definition"]["source_basis"] != "admitted_l1":
            raise DerivedError("project_history_source_basis_unsupported")
        # Current indexed routes retain every prior membership and unbound route.
        # They enumerate identities only; each selected historical metadata version
        # is loaded before permission checks, then its fact body afterwards.
        live = await uow.derived_project_candidates(
            self.scope, admission.contract.fingerprint, spec["project_id"]
        )
        if len(live) > 64:
            raise DerivedError("project_candidate_capacity")
        headers = []
        for header in live:
            metadata = await uow.get_admission_record_at(
                self.scope, header["id"], known, metadata_only=True
            )
            if metadata is None:
                continue  # An identity not yet observed cannot poison the past.
            past = metadata["header"]
            project = past["project"]
            if project and project["current_project_id"] in {None, spec["project_id"]}:
                headers.append(past)
        source_ids = sorted({source_id for header in headers for source_id in header["source_ids"]})
        guard = await self.service.history._permission(
            uow, source_ids, actor, archive["purpose"], controls
        )
        query = QueryContext(
            admission.principal,
            self.scope,
            spec["project_id"],
            admission.purpose,
            valid,
            known,
            tuple(
                ContextAttribute(key, value, context["issuer_id"])
                for key, value in context["attributes"].items()
            ),
            admission.contract.timezone,
            "historical-ledger-rebuild",
        )
        census = await admission._snapshot(
            uow, query, at=valid, candidate_headers=headers, historical=True, input_guard=guard
        )
        result = full_project_question(
            admission.contract,
            census.snapshot,
            spec["question"],
            overdue_only=spec["instance"]["parameters"]["overdue_only"],
        )
        result_payload = result.payload()
        spans = {
            digest(to_jsonable(span)): to_jsonable(span)
            for row in result.rows
            for field in row.fields
            for candidate in field.candidates
            for evidence in candidate.qualification.field_evidence
            for branch in evidence.alternatives
            for span in branch
        }
        proof = dict(
            candidates=list(census.candidate_versions),
            sources=list(census.source_proofs),
            registration_sha256=archive["sha256"],
            epoch=archive["epoch"],
        )
        body = dict(
            schema="question-answer/1",
            question_id=label,
            instance_id=definition["facet_id"],
            content_revision_id="historical-content:" + digest(result_payload),
            certificate_revision_id="historical-certificate:" + digest(proof),
            answer_status=result.status.value,
            availability_status="valid",
            refresh_status="idle",
            compute_mode="historical_full",
            model_calls=0,
            runtime_current_validated=False,
            result=result_payload,
            citations=list(spans.values()),
            processing_references=to_jsonable(census.processing_references),
            generation_manifest=dict(schema="project-history-inputs/1", inputs=proof),
            validation_manifest=dict(schema="project-history-current-security/1", inputs=proof),
            digests=dict(content=digest(result_payload), certificate=digest(proof)),
            coverage=to_jsonable(census.snapshot.coverage),
            valid_until=None,
            historical=dict(
                mode="ledger_rebuild",
                known_at=known.isoformat(),
                valid_at=valid.isoformat(),
                registration_known_at=archive["known_at"],
                context_revision=context["revision"],
                current_security_checked_at=self.service.clock().isoformat(),
            ),
        )
        guard = await self.service.history._permission(
            uow, source_ids, actor, archive["purpose"], controls
        )
        await self.service.history._finish(uow, controls, guard)
        return budget(body, spec["instance"]["definition"]["max_output_bytes"])

    async def _page(self, uow, label, actor, known, valid, controls):
        current = await self.service.pages._registration(uow, label, actor)
        archive = await self._definition(uow, label, "page", known)
        definition = archive["definition"]
        if actor not in definition["readers"]:
            raise DerivedError("derived_read_denied")
        blocks = []
        layout = [
            dict(
                block_id="question-page-block:"
                + digest([definition["instance_id"], parent["question_id"]]),
                question_id=parent["question_id"],
            )
            for parent in definition["parents"]
        ]
        try:
            layout_archive = await self._definition(uow, label, "page_layout", known)
        except DerivedError as error:
            if error.code != "question_history_coverage_floor":
                raise
        else:
            if layout_archive["definition"]["registration_sha256"] == definition["sha256"]:
                layout = layout_archive["definition"]["blocks"]
        parents = {parent["question_id"]: parent for parent in definition["parents"]}
        answers = {}
        for slot in layout:
            parent = parents.get(slot["question_id"])
            if parent is None:
                raise DerivedError("question_history_page_definition_coverage_unavailable")
            parent_definition = await self._definition(
                uow, parent["question_id"], "question", known
            )
            if (
                parent_definition["definition"]["facet_id"] != parent["instance_id"]
                or digest(parent_definition["definition"]["spec"]) != parent["definition_sha256"]
            ):
                raise DerivedError("question_history_page_definition_coverage_unavailable")
            if parent["question_id"] not in answers:
                answers[parent["question_id"]] = await self._question(
                    uow, parent["question_id"], actor, known, valid, controls
                )
            answer = answers[parent["question_id"]]
            block = dict(
                block_id=slot["block_id"],
                revision_id=answer["content_revision_id"],
                answer_status=answer["answer_status"],
                body=dict(kind="project_question", answer=answer),
            )
            blocks.append(block)
        if await self.service.pages._registration(uow, label, actor) != current:
            raise DerivedError("question_page_parent_changed")
        body = dict(
            schema="question-page/1",
            page_id=label,
            template="project-question-scenario/1",
            rebuild="historical_full",
            availability_status="valid",
            blocks=blocks,
            revision_id="historical-page:" + digest(blocks),
            certificate_revision_id="historical-page-certificate:"
            + digest(
                [
                    archive["sha256"],
                    [b["body"]["answer"]["certificate_revision_id"] for b in blocks],
                ]
            ),
            model_calls=0,
            valid_until=None,
            historical=dict(
                mode="ledger_rebuild",
                known_at=known.isoformat(),
                valid_at=valid.isoformat(),
                registration_known_at=archive["known_at"],
                current_security_checked_at=self.service.clock().isoformat(),
            ),
        )
        return budget(body, definition["max_output_bytes"])
