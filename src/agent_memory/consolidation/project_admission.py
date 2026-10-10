"""Trusted project admission and a bounded, complete ledger-to-oracle bridge.

Only authenticated host setup may construct this facade. Transport callers cannot
supply ProjectQualification objects, source authority, membership, or review policy.
The facade persists the actual host review alongside the existing admission ledger;
quote matching alone never promotes a candidate. Runtime reads use this persisted
review and enumerate every candidate before deciding applicability or disposition.
"""

from contextlib import nullcontext
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..conditions import Condition, ProjectionPolicy, QueryContext, identifier, instant
from ..derived.model import DerivedError, digest
from ..derived.project_questions import (
    ProjectDomainContract,
    ProjectFact,
    ProjectInputSnapshot,
    ProjectQualification,
    ProjectSnapshotCoverage,
    QualifiedProjectFact,
    project_query_fingerprint,
)
from ..derived.registry import DerivedRegistry
from ..derived.relation_questions import DATE_PREDICATES, EDGE_PREDICATES, RELATION_PREDICATES
from ..domain import AtomDraft, MemoryEvent, MemoryScope, SourceAuthority, utc_now
from ..evidence_support import evaluate_support
from ..fact_qualification import FieldEvidence, SourceSpan
from ..operations.publication_manifest import close, valid_manifest
from ..operations.retention import DurableReceiver
from ..operations.source_revisions import source_is_current
from ..serialization import to_jsonable
from .admission import AdmissionPolicy, authority_to_payload, draft_from_payload, draft_to_payload
from .qualification import ContextualMemory, target_fingerprint

CANDIDATE_SCHEMA = "project-candidate/1"
REVIEW_SCHEMA = "project-review/1"
MAX_CANDIDATES = 64
MAX_INPUTS = 128
PROJECT_PREDICATES = frozenset(
    {
        "project.owner",
        "project.status",
        "project.phase",
        "commitment.promisor",
        "commitment.action",
        "commitment.state",
        "commitment.deadline",
        "risk.label",
        "risk.state",
        *RELATION_PREDICATES,
    }
)


def _fail(code):
    raise DerivedError(code)


def _scope(scope):
    if type(scope) is not MemoryScope:
        _fail("project_exact_scope_required")
    for name in MemoryScope.__dataclass_fields__:
        value = getattr(scope, name)
        if value is not None:
            if type(value) is not str:
                _fail("project_exact_scope_required")
            identifier(value)


@dataclass(frozen=True, slots=True)
class ProjectMembership:
    """One host registry entry, never inferred from an assertion's subject/text."""

    binding_id: str
    registry_revision: str
    project_id: str
    entity_id: str

    def __post_init__(self):
        for value in (self.binding_id, self.registry_revision, self.project_id, self.entity_id):
            identifier(value)
        if self.entity_id.startswith("rule:"):
            _fail("project_reserved_entity_identity")


@dataclass(frozen=True, slots=True)
class ProjectCensus:
    """Internal snapshot plus every processing input and its current guard proof."""

    snapshot: ProjectInputSnapshot
    candidate_versions: tuple[tuple[str, int, int], ...]
    source_ids: tuple[str, ...]
    processing_references: tuple[SourceSpan, ...]
    grants: tuple[dict, ...]
    source_proofs: tuple[dict, ...]
    registration_fingerprint: str
    next_transition_at: datetime | None
    candidate_count: int
    publication_manifests: tuple[dict, ...] = ()


class _ReviewedProjectRequired(AdmissionPolicy):
    version = "project-review-required/1"

    def evaluate(self, event, draft, authority):
        _, reasons = super().evaluate(event, draft, authority)
        return "PENDING_VERIFICATION", tuple(dict.fromkeys((*reasons, "project_review_required")))


class ProjectAdmission:
    """Host-only, exact-scope authority and review boundary for registered projects.

    The membership and source-authority registries are authenticated setup inputs.
    A call to qualify is the host's semantic review action. Its identifier, reviewer
    version, principal, policy, target, exact source revisions, and ledger version
    are persisted together; no raw qualification supplied to a read is trusted.
    """

    def __init__(
        self,
        engine,
        scope,
        *,
        principal,
        contract,
        authorities,
        memberships,
        reviewer_version,
        purpose="project_questions",
        clock=utc_now,
        authority_id=None,
        authority_min_version=None,
    ):
        _scope(scope)
        for value in (principal, reviewer_version, purpose):
            identifier(value)
        if type(contract) is not ProjectDomainContract:
            _fail("project_registered_contract_required")
        authorities, memberships = tuple(authorities), tuple(memberships)
        if not 1 <= len(authorities) <= 64 or any(
            type(a) is not SourceAuthority or a.kind not in {"document", "tool_observation"}
            for a in authorities
        ):
            _fail("project_authoritative_source_required")
        if len({a.source_id for a in authorities}) != len(authorities):
            _fail("project_duplicate_source_authority")
        if len(memberships) > 1024 or any(type(m) is not ProjectMembership for m in memberships):
            _fail("project_membership_registry_required")
        if len({m.binding_id for m in memberships}) != len(memberships):
            _fail("project_duplicate_membership_binding")
        if authority_id is not None:
            identifier(authority_id)
            if type(authority_min_version) is not int or authority_min_version < 1:
                _fail("trusted_authority_version_required")
        elif authority_min_version is not None:
            _fail("derived_authority_mismatch")
        self.engine, self.repository, self.scope = engine, engine.repository, scope
        self.principal, self.contract, self.purpose = principal, contract, purpose
        self.reviewer_version, self.clock = reviewer_version, clock
        self.authorities = {a.source_id: a for a in authorities}
        self.memberships = {m.binding_id: m for m in memberships}
        self.admission_policy = AdmissionPolicy(contract.predicate_specs)
        self.stage_policy = _ReviewedProjectRequired(contract.predicate_specs)
        self.projection_policy = ProjectionPolicy(contract.qualification_revision, purpose)
        self.policy = to_jsonable(self.admission_policy.config_payload())
        self.authority_id, self.authority_min_version = authority_id, authority_min_version
        self.registry = DerivedRegistry(self)
        self.contextual = ContextualMemory(engine, scope, principal=principal)
        self.receiver = DurableReceiver(self.repository, clock=clock)

    @property
    def registration_fingerprint(self):
        """Question definitions/certificates must pin this authenticated setup."""
        return digest(
            {
                "scope": to_jsonable(self.scope),
                "principal": self.principal,
                "purpose": self.purpose,
                "contract": self.contract.fingerprint,
                "reviewer_version": self.reviewer_version,
                "authorities": [
                    authority_to_payload(self.authorities[key]) for key in sorted(self.authorities)
                ],
                "memberships": [
                    to_jsonable(self.memberships[key]) for key in sorted(self.memberships)
                ],
                "authority_id": self.authority_id,
                "authority_min_version": self.authority_min_version,
            }
        )

    def _authority(self, key):
        authority = self.authorities.get(key)
        if authority is None:
            _fail("project_source_authority_unregistered")
        return authority

    def _membership(self, key, draft):
        if key is None:
            return None
        item = self.memberships.get(key)
        if item is None or item.entity_id != draft.subject_id:
            _fail("project_membership_unregistered")
        if draft.predicate.startswith("project.") and item.project_id != draft.subject_id:
            _fail("project_subject_binding_mismatch")
        if draft.predicate in EDGE_PREDICATES:
            if draft.value == item.project_id or not any(
                m.entity_id == draft.value and m.project_id == item.project_id
                for m in self.memberships.values()
            ):
                _fail("relation_target_membership_unregistered")
        if (draft.predicate in {"deliverable.depends_on", "deliverable.commitment_date"}
                and draft.subject_id == item.project_id):
            _fail("relation_entity_type_mismatch")
        return to_jsonable(item)

    def _draft(self, draft, *, review=False):
        if type(draft) is not AtomDraft or self.scope.project(draft.scope_level) != self.scope:
            _fail("project_exact_scope_required")
        if draft.predicate not in {p.predicate for p in self.contract.predicate_specs}:
            _fail("unregistered_project_predicate")
        if type(draft.value) is not str or not draft.value.strip() or len(draft.value) > 4096:
            _fail("project_fact_requires_bounded_string")
        enums = {
            "project.status": self.contract.project_statuses,
            "project.phase": self.contract.project_phases,
            "commitment.state": (
                *self.contract.commitment_open_states,
                *self.contract.commitment_terminal_states,
                "unknown",
            ),
            "risk.state": ("open", "closed", "unknown"),
        }
        if draft.predicate in enums and draft.value not in enums[draft.predicate]:
            _fail("unregistered_project_state")
        if draft.predicate in {"commitment.deadline", *DATE_PREDICATES}:
            try:
                instant(datetime.fromisoformat(draft.value))
            except (TypeError, ValueError):
                _fail("project_deadline_requires_absolute_instant")
        if draft.subject_id.startswith("rule:"):
            _fail("project_reserved_entity_identity")
        if review and (
            draft.valid_from is None
            or draft.negated
            or draft.kind != "fact"
            or draft.modality != "asserted"
            or draft.change_kind != "replace"
        ):
            _fail("project_explicit_reviewable_fact_required")

    async def stage_source(
        self,
        event,
        drafts,
        *,
        source_authority_id,
        request_id,
        membership_ids=None,
        base_event_id=None,
        expected_revision=None,
        _unit_of_work=None,
    ):
        """Atomically retain a source and its complete, always-pending candidates."""
        event, drafts = deepcopy((event, tuple(drafts)))
        if type(event) is not MemoryEvent or event.scope != self.scope:
            _fail("project_exact_scope_required")
        if not 1 <= len(drafts) <= MAX_CANDIDATES:
            _fail("project_candidate_capacity")
        authority = self._authority(source_authority_id)
        bindings = tuple(membership_ids) if membership_ids is not None else (None,) * len(drafts)
        if len(bindings) != len(drafts):
            _fail("project_membership_count_mismatch")
        if len({digest(draft_to_payload(d)) for d in drafts}) != len(drafts):
            _fail("project_duplicate_candidate")
        memberships = []
        for draft, key in zip(drafts, bindings, strict=True):
            self._draft(draft)
            if (
                draft.subject_id not in authority.subjects
                or draft.predicate not in authority.predicates
            ):
                _fail("project_qualification_authority_mismatch")
            memberships.append(self._membership(key, draft))
        configuration = digest(
            {
                "contract": self.contract.fingerprint,
                "principal": self.principal,
                "authority": authority_to_payload(authority),
                "purpose": self.purpose,
                "reviewer_version": self.reviewer_version,
            }
        )
        stage_sha = digest(
            {
                "drafts": [draft_to_payload(d) for d in drafts],
                "memberships": memberships,
                "configuration": configuration,
            }
        )
        transaction = (
            nullcontext(_unit_of_work)
            if _unit_of_work is not None
            else self.repository.unit_of_work()
        )
        async with transaction as uow:
            await uow.lock_admission_scope(self.scope)
            previous = await uow.retention_get(self.scope, "request", request_id)
            if (
                previous is not None
                and previous.get("project_stage", {}).get("sha256") != stage_sha
            ):
                _fail("project_stage_input_conflict")
            if base_event_id is None:
                if expected_revision is not None:
                    _fail("project_source_revision_required")
                # A completed retry is checked by the receiver, including exact source bytes.
                if previous is not None:
                    row = await uow.retention_get(self.scope, "ticket", request_id)
                    ticket = self.receiver._ticket(row)
                else:
                    ticket = await self.receiver.issue_ticket(
                        event,
                        request_id=request_id,
                        producer_id=self.principal,
                        configuration_sha256=configuration,
                        _unit_of_work=uow,
                    )
                await self.receiver.submit(
                    event,
                    ticket=ticket,
                    producer_id=self.principal,
                    configuration_sha256=configuration,
                    _unit_of_work=uow,
                )
            else:
                await self.receiver.revise(
                    event,
                    base_event_id=base_event_id,
                    expected_revision=expected_revision,
                    request_id=request_id,
                    producer_id=self.principal,
                    configuration_sha256=configuration,
                    _unit_of_work=uow,
                )
            if previous is not None:
                rows = [
                    await uow.get_admission_record(self.scope, key)
                    for key in previous["project_stage"]["candidate_ids"]
                ]
                if any(row is None for row in rows):
                    _fail("project_source_unavailable")
                return self.engine.receipt(event.id, rows, duplicate=True)
            source = await uow.get_source_event(self.scope, event.id)
            origin = source.metadata.get("lifecycle", {}).get("origin")
            if (
                origin is not None
                and origin != {"document": "host", "tool_observation": "tool"}[authority.kind]
            ):
                _fail("project_source_origin_mismatch")
            audit = {
                "input_fingerprint": stage_sha,
                "generator_version": "host-project-stage/1",
                "reviewer_version": "project-review-required/1",
                "reports": [],
            }
            receipt = await self.engine.admit(
                source,
                drafts,
                authority=authority,
                policy=self.stage_policy,
                _unit_of_work=uow,
                _retained=True,
                _extraction_audit=audit,
            )
            from .admission import candidate_id

            by_id = {
                candidate_id(source, draft): membership
                for draft, membership in zip(drafts, memberships, strict=True)
            }
            for key in receipt.candidate_ids:
                row = await uow.get_admission_record(self.scope, key)
                metadata = source.metadata["_retention"]
                row["payload"]["project_candidate"] = {
                    "schema": CANDIDATE_SCHEMA,
                    "contract_fingerprint": self.contract.fingerprint,
                    "contract_id": self.contract.id,
                    "contract_version": self.contract.version,
                    "principal": self.principal,
                    "purpose": self.purpose,
                    "authority_id": source_authority_id,
                    "membership": by_id[key],
                    "membership_history": [by_id[key]] if by_id[key] else [],
                    "was_unbound": by_id[key] is None,
                    "source": {
                        "source_event_id": source.id,
                        "sha256": source.content_hash,
                        "document_id": metadata["document_id"],
                        "revision": metadata["revision"],
                    },
                    "review": None,
                }
                await self._save(uow, row)
            request = await uow.retention_get(self.scope, "request", request_id)
            request.update(
                status="completed",
                result=to_jsonable(receipt),
                project_stage={"sha256": stage_sha, "candidate_ids": list(receipt.candidate_ids)},
            )
            close(self.scope, request, receipt)
            await uow.retention_update(self.scope, request_id, request)
            await uow.retention_head_put(
                self.scope,
                "interpretation",
                event.id,
                {
                    "active_ids": list(receipt.candidate_ids),
                    "request_id": request_id,
                    "stream": "primary",
                    "publication_closed": True,
                },
                0,
            )
            return receipt

    async def _save(self, uow, row):
        return await uow.save_admission_record(
            self.scope, row["id"], row["event_id"], row["slot_key"], row["payload"], row["version"]
        )

    def _binding(self, row):
        value = row["payload"].get("project_candidate")
        if (
            not value
            or value.get("schema") != CANDIDATE_SCHEMA
            or value.get("contract_fingerprint") != self.contract.fingerprint
            or value.get("principal") != self.principal
            or value.get("purpose") != self.purpose
            or row["scope"] != to_jsonable(self.scope)
        ):
            _fail("project_candidate_binding_mismatch")
        authority = self._authority(value["authority_id"])
        if row["payload"].get("authority") != authority_to_payload(authority):
            _fail("project_source_authority_changed")
        return value

    async def _row(self, uow, candidate_id, expected_version):
        if type(expected_version) is not int or expected_version < 1:
            _fail("project_expected_version_required")
        row = await uow.get_admission_record(self.scope, candidate_id)
        if row is None or row["version"] != expected_version:
            _fail("project_candidate_version_changed")
        self._binding(row)
        source = await uow.get_source_event(self.scope, row["event_id"])
        if source is None or not await source_is_current(uow, source):
            _fail("project_source_unavailable")
        self._source_binding(row, source)
        return row

    @staticmethod
    def _source_binding(row, source):
        binding = row["payload"]["project_candidate"]["source"]
        metadata = source.metadata.get("_retention", {})
        if binding != {
            "source_event_id": source.id,
            "sha256": source.content_hash,
            "document_id": metadata.get("document_id"),
            "revision": metadata.get("revision"),
        }:
            _fail("project_source_revision_changed")

    async def _grants(self, uow, ids, at):
        authority = await self.registry.authority(uow, self.authority_id)
        result = {}
        for key in sorted(ids):
            grant = await uow.derived_get(self.scope, "grant", key)
            self.registry.grant_binding(grant, authority)
            if (
                not grant
                or grant.get("revoked")
                or self.principal not in grant["readers"]
                or self.purpose not in grant["purposes"]
            ):
                _fail("project_processing_denied")
            if (
                grant.get("expires_at")
                and instant(datetime.fromisoformat(grant["expires_at"])) <= at
            ):
                _fail("project_processing_grant_expired")
            result[key] = grant
        return result

    def _review(self, row, review_id, disposition, *, version):
        identifier(review_id)
        binding = self._binding(row)
        return {
            "schema": REVIEW_SCHEMA,
            "id": review_id,
            "version": version,
            "reviewer_version": self.reviewer_version,
            "policy_revision": self.contract.qualification_revision,
            "principal": self.principal,
            "disposition": disposition,
            "target_sha256": digest(row["payload"]["draft"]),
            "membership_sha256": digest(binding["membership"]),
            "qualification_sha256": digest(row["payload"].get("qualification")),
        }

    async def qualify(
        self,
        candidate_id,
        *,
        expected_version,
        review_id,
        applicability_id,
        conditions=(),
        exceptions=(),
        links=(),
        field_support=(),
        _unit_of_work=None,
    ):
        """Persist the host's actual semantic review and field proofs in one transaction."""
        conditions, exceptions, links, field_support = deepcopy(
            tuple(map(tuple, (conditions, exceptions, links, field_support)))
        )
        transaction = (
            nullcontext(_unit_of_work)
            if _unit_of_work is not None
            else self.repository.unit_of_work()
        )
        async with transaction as uow:
            await uow.lock_admission_scope(self.scope)
            row = await self._row(uow, candidate_id, expected_version)
            binding = self._binding(row)
            draft = draft_from_payload(row["payload"]["draft"])
            self._draft(draft, review=True)
            membership = binding["membership"]
            if (
                membership is None
                or self._membership(membership["binding_id"], draft) != membership
            ):
                _fail("project_membership_unregistered")
            if binding.get("review") is not None:
                _fail("project_review_already_decided")
            for link in links:
                if link.authority != self._authority(link.authority.source_id):
                    _fail("project_source_authority_changed")
            await self._grants(
                uow,
                {row["event_id"], *(link.span.source_event_id for link in links)},
                instant(self.clock()),
            )
            version = await self.contextual.qualify(
                candidate_id,
                expected_version=expected_version,
                admission_policy=self.admission_policy,
                projection_policy=self.projection_policy,
                applicability_id=applicability_id,
                conditions=conditions,
                exceptions=exceptions,
                links=links,
                field_support=field_support,
                _unit_of_work=uow,
            )
            row = await uow.get_admission_record(self.scope, candidate_id)
            row["payload"]["project_candidate"]["review"] = self._review(
                row, review_id, "qualified", version=version + 1
            )
            return await self._save(uow, row)

    async def reject(
        self, candidate_id, *, expected_version, review_id, reasons, _unit_of_work=None
    ):
        return await self._dispose(
            candidate_id,
            expected_version=expected_version,
            review_id=review_id,
            reasons=reasons,
            disposition="rejected",
            _unit_of_work=_unit_of_work,
        )

    async def withdraw(self, candidate_id, *, expected_version, review_id, reasons):
        return await self._dispose(
            candidate_id,
            expected_version=expected_version,
            review_id=review_id,
            reasons=reasons,
            disposition="withdrawn",
        )

    async def _dispose(
        self, candidate_id, *, expected_version, review_id, reasons, disposition, _unit_of_work=None
    ):
        reasons = tuple(reasons)
        if not 1 <= len(reasons) <= 16:
            _fail("project_review_reason_required")
        for reason in reasons:
            identifier(reason)
        transaction = (
            nullcontext(_unit_of_work)
            if _unit_of_work is not None
            else self.repository.unit_of_work()
        )
        async with transaction as uow:
            await uow.lock_admission_scope(self.scope)
            row = await self._row(uow, candidate_id, expected_version)
            row["payload"].update(
                action="REJECT" if disposition == "rejected" else "WITHDRAWN", reasons=list(reasons)
            )
            row["payload"]["project_candidate"]["review"] = self._review(
                row, review_id, disposition, version=expected_version + 1
            )
            row["payload"]["decisions"].append(
                {
                    "action": row["payload"]["action"],
                    "reasons": list(reasons),
                    "review_id": review_id,
                    "recorded_at": instant(self.clock()).isoformat(),
                }
            )
            return await self._save(uow, row)

    async def replace_membership(self, candidate_id, *, expected_version, membership_id):
        """Move a candidate atomically; old/new project routes remain census dependencies."""
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            row = await self._row(uow, candidate_id, expected_version)
            binding = self._binding(row)
            membership = self._membership(
                membership_id, draft_from_payload(row["payload"]["draft"])
            )
            if membership == binding["membership"]:
                return expected_version
            if membership and membership not in binding["membership_history"]:
                if len(binding["membership_history"]) >= 64:
                    _fail("project_membership_history_capacity")
                binding["membership_history"].append(membership)
            binding.update(
                membership=membership,
                review=None,
                was_unbound=binding["was_unbound"] or membership is None,
            )
            row["payload"].pop("qualification", None)
            row["payload"].update(
                action="PENDING_VERIFICATION", reasons=["project_membership_changed"]
            )
            return await self._save(uow, row)

    async def snapshot(
        self,
        project_id,
        *,
        attributes=(),
        timezone=None,
        snapshot_token="host-project-snapshot",
        source_basis="admitted_l1",
        publication_request_ids=(),
    ):
        """Capture a current snapshot. Historical project queries are not enabled.

        Storage must provide derived_project_candidates(scope, contract_fingerprint,
        project_id): an indexed <=65 header census with no disposition/time filter.
        Each header's ``project`` is project_header(payload). Routing includes prior
        memberships and the unbound bucket, but moved-out headers are classified
        before loading bodies. The 65th header is an overflow sentinel, never top-k.
        """
        identifier(project_id)
        async with self.repository.unit_of_work() as uow:
            await uow.lock_admission_scope(self.scope)
            at = instant(self.clock())
            context = QueryContext(
                self.principal,
                self.scope,
                project_id,
                self.purpose,
                at,
                at,
                tuple(attributes),
                timezone or self.contract.timezone,
                snapshot_token,
            )
            return await self._snapshot(
                uow,
                context,
                at=at,
                source_basis=source_basis,
                publication_request_ids=publication_request_ids,
            )

    async def _snapshot(
        self, uow, context, *, at, source_basis="admitted_l1", publication_request_ids=(),
        input_guard=None, candidate_headers=None,
    ):
        """Transaction-internal QuestionService bridge; lock held by the caller.

        Both time coordinates must be the captured current time. This deliberately
        does not call ContextualMemory.query or the latest-boundary atom projection:
        every qualified overlapping fact survives for the full oracle to contest.
        """
        at = instant(at)
        registration = self.registration_fingerprint
        security_at = at
        if (
            type(context) is not QueryContext
            or context.scope != self.scope
            or context.principal != self.principal
            or context.purpose != self.purpose
        ):
            _fail("project_snapshot_context_mismatch")
        if context.valid_at != at or context.known_at != at:
            _fail("project_historical_snapshot_unsupported")
        reader = getattr(uow, "derived_project_candidates", None)
        if not callable(reader):
            _fail("project_census_backend_unsupported")
        if candidate_headers is None:
            headers = await reader(self.scope, self.contract.fingerprint, context.subject_id)
        else:
            from ..derived.project_index import checked_headers

            headers = checked_headers(candidate_headers, self.contract.fingerprint, context.subject_id)
        if len(headers) > MAX_CANDIDATES:
            _fail("project_candidate_capacity")
        if len({h["id"] for h in headers}) != len(headers):
            _fail("project_candidate_census_invalid")
        relevant, versions = [], []
        for header in headers:
            routing = header.get("project")
            if not routing or routing["contract_fingerprint"] not in {
                None,
                self.contract.fingerprint,
            }:
                _fail("project_candidate_census_invalid")
            versions.append((header["id"], header["version"], header["version"]))
            # A moved-out routing hit contributes a query proof only. It must not
            # demand access to, read, or disclose its new project's current body.
            if routing["current_project_id"] not in {None, context.subject_id}:
                continue
            relevant.append(header)
        ids = sorted({key for h in relevant for key in h["source_ids"]})
        if len(ids) + len(headers) > MAX_INPUTS:
            _fail("project_input_capacity")
        grants = await self._grants(uow, ids, at)
        sources, source_proofs = {}, []
        epoch = await uow.retention_epoch(self.scope)
        transitions = [
            datetime.fromisoformat(g["expires_at"]) for g in grants.values() if g.get("expires_at")
        ]
        authority = await self.registry.authority(uow, self.authority_id)
        if authority:
            transitions.append(datetime.fromisoformat(authority["spec"]["expires_at"]))

        def guard_inputs():
            nonlocal security_at
            now = instant(self.clock())
            if now < security_at:
                _fail("refresh_clock_discontinuity")
            security_at = now
            if self.registration_fingerprint != registration:
                _fail("question_registration_changed")
            if authority:
                self.registry._authority_floor(authority)
                if instant(datetime.fromisoformat(authority["spec"]["expires_at"])) <= now:
                    _fail("derived_authority_expired")
            for grant in grants.values():
                if (
                    grant.get("expires_at")
                    and instant(datetime.fromisoformat(grant["expires_at"])) <= now
                ):
                    _fail("project_processing_grant_expired")
            if input_guard is not None:
                input_guard()

        for key in ids:
            # A locked database snapshot does not freeze host controls or time.
            # Recheck immediately before each source body, not only at capture.
            guard_inputs()
            source = await uow.get_source_event(self.scope, key)
            if source is None or source.scope != self.scope:
                _fail("project_source_unavailable")
            sources[key] = source
            retained = source.metadata.get("_retention")
            head = None
            if retained:
                head = await uow.retention_head_get(self.scope, "document", retained["document_id"])
                if head is None:
                    _fail("project_source_revision_changed")
            source_proofs.append(
                {
                    "source_event_id": key,
                    "source_sha256": source.content_hash,
                    "epoch": epoch,
                    "document_id": retained["document_id"] if retained else None,
                    "revision": retained["revision"] if retained else None,
                    "document_head_generation": head["generation"] if head else None,
                    "document_head_event_id": head["payload"]["event_id"] if head else None,
                    "document_head_sha256": digest(head) if head else None,
                    "grant_version": grants[key]["version"],
                    "grant_sha256": digest(grants[key]),
                    "authority_id": grants[key].get("authority_id"),
                    "authority_version": grants[key].get("authority_version"),
                }
            )
        facts, unresolved, processing = [], [], set()
        for header in relevant:
            guard_inputs()
            row = await uow.get_admission_record(self.scope, header["id"])
            if row is None or row["version"] != header["version"]:
                _fail("project_candidate_version_changed")
            if project_header(row["payload"]) != header["project"]:
                _fail("project_candidate_census_invalid")
            if instant(datetime.fromisoformat(row["recorded_at"])) > context.known_at:
                _fail("project_snapshot_changed")
            p = row["payload"]
            # Processing provenance describes the exact retained bytes examined,
            # never an unreviewed candidate's possibly incorrect source locator.
            for key in header["source_ids"]:
                source = sources[key]
                for start in range(0, len(source.content), 16_384):
                    end = min(len(source.content), start + 16_384)
                    processing.add(SourceSpan(key, start, end, source.content[start:end]))
            if header["project"].get("unreviewed"):
                # Ordinary admission, even ACCEPT or REJECT, is not a project
                # semantic review. Raw registered predicates poison the explicit
                # unbound census rather than disappearing behind a bound filter.
                unresolved.append(row["id"])
                continue
            binding = self._binding(row)
            draft = draft_from_payload(p["draft"])
            self._draft(draft)
            source = sources[row["event_id"]]
            self._source_binding(row, source)
            review = binding.get("review")
            if review:
                self._review_valid(row)
            current = await source_is_current(uow, source)
            if p["action"] == "WITHDRAWN" and not current:
                # Source revision replacement is an independently persisted,
                # atomic withdrawal, even if the old host review was qualified.
                continue
            if not current:
                _fail("project_source_revision_changed")
            if review and review["disposition"] in {"rejected", "withdrawn"}:
                if (
                    p["action"]
                    != {"rejected": "REJECT", "withdrawn": "WITHDRAWN"}[review["disposition"]]
                ):
                    _fail("project_review_invalid")
                continue
            if not review or binding["membership"] is None:
                unresolved.append(row["id"])
                continue
            if p["action"] != "PENDING_VERIFICATION":
                _fail("project_review_invalid")
            membership = binding["membership"]
            if self._membership(membership["binding_id"], draft) != membership:
                _fail("project_membership_unregistered")
            self._draft(draft, review=True)
            if not draft.source_quote or draft.source_quote not in source.content:
                _fail("project_support_quote_missing")
            qualification = p.get("qualification")
            self._qualification_valid(qualification, draft, sources)
            for link in qualification["links"]:
                source = sources[link["span"]["source_event_id"]]
                if "_retention" in source.metadata and not await source_is_current(uow, source):
                    _fail("project_evidence_superseded")
                processing.add(SourceSpan(**link["span"]))
            ranges, _ = evaluate_support(qualification, sources)
            transitions.extend(
                bound for r in ranges for bound in (r.start, r.end) if bound and bound > at
            )
            transitions.extend(
                bound for bound in (draft.valid_from, draft.valid_to) if bound and bound > at
            )
            if not any(r.contains(at) for r in ranges):
                # An inactive assertion with proven boundaries is legitimately
                # absent; an active assertion lacking temporal support is unknown.
                if draft.valid_from <= at and (draft.valid_to is None or at < draft.valid_to):
                    unresolved.append(row["id"])
                continue
            if any(r.kind == "point" and r.contains(at) for r in ranges):
                transitions.append(at + timedelta(microseconds=1))
            fact = ProjectFact(
                row["id"],
                membership["project_id"],
                draft.subject_id,
                draft.predicate,
                draft.value,
                draft.valid_from,
                instant(datetime.fromisoformat(row["recorded_at"])),
                draft.valid_to,
                conditions=tuple(Condition(**c) for c in qualification["conditions"]),
                exceptions=tuple(Condition(**c) for c in qualification["exceptions"]),
            )
            by_id = {link["id"]: link for link in qualification["links"]}
            fields = tuple(
                FieldEvidence(
                    field["field"],
                    tuple(
                        tuple(SourceSpan(**by_id[key]["span"]) for key in group)
                        for group in field["alternatives"]
                    ),
                )
                for field in qualification["field_support"]
            )
            facts.append(
                QualifiedProjectFact(
                    fact,
                    ProjectQualification(
                        review["id"],
                        review["policy_revision"],
                        fact.fingerprint,
                        self._authority(binding["authority_id"]),
                        fields,
                    ),
                )
            )
        manifests = await self._publication(uow, source_basis, publication_request_ids, relevant)
        guard_inputs()
        coverage = ProjectSnapshotCoverage(
            project_query_fingerprint(self.contract, self.scope, context.subject_id),
            source_basis,
            True,
            len(facts),
            unqualified_candidate_ids=tuple(sorted(unresolved)),
            publication_closed=all(m["closed"] for m in manifests)
            if source_basis == "publication_manifest"
            else None,
        )
        snapshot = ProjectInputSnapshot(
            "project-snapshot:"
            + digest(
                {
                    "context": to_jsonable(context),
                    "versions": versions,
                    "grants": grants,
                    "source_proofs": source_proofs,
                    "contract": self.contract.fingerprint,
                    "registration": self.registration_fingerprint,
                }
            ),
            self.scope,
            context.subject_id,
            self.contract.fingerprint,
            context,
            coverage,
            tuple(facts),
        )
        return ProjectCensus(
            snapshot,
            tuple(versions),
            tuple(ids),
            tuple(sorted(processing, key=lambda s: (s.source_event_id, s.start, s.end, s.quote))),
            tuple(deepcopy(grants[key]) for key in ids),
            tuple(deepcopy(source_proofs)),
            self.registration_fingerprint,
            min(transitions) if transitions else None,
            len(headers),
            tuple(manifests),
        )

    @staticmethod
    def _span(span, sources):
        source = sources.get(span.source_event_id)
        if (
            source is None
            or span.end > len(source.content)
            or source.content[span.start : span.end] != span.quote
        ):
            _fail("project_support_quote_missing")

    def _review_valid(self, row):
        binding = self._binding(row)
        review = binding["review"]
        if (
            type(review.get("version")) is not int
            or not 1 <= review["version"] <= row["version"]
            or review.get("schema") != REVIEW_SCHEMA
            or review.get("principal") != self.principal
            or review.get("reviewer_version") != self.reviewer_version
            or review.get("policy_revision") != self.contract.qualification_revision
            or review.get("disposition") not in {"qualified", "rejected", "withdrawn"}
            or review.get("target_sha256") != digest(row["payload"]["draft"])
            or review.get("membership_sha256") != digest(binding["membership"])
            or review.get("qualification_sha256") != digest(row["payload"].get("qualification"))
        ):
            _fail("project_review_invalid")
        identifier(review.get("id"))

    def _qualification_valid(self, qualification, draft, sources):
        required = {"subject_id", "predicate", "value", "valid_from"}
        required.update(k for k in ("valid_to", "conditions", "exceptions") if getattr(draft, k))
        if (
            not qualification
            or qualification.get("schema") != "contextual-qualification/1"
            or qualification.get("principal") != self.principal
            or qualification.get("target_sha256") != target_fingerprint(draft)
            or qualification.get("admission_policy") != self.policy
            or qualification.get("policy_sha256") != self.projection_policy.fingerprint
            or qualification.get("policy") != to_jsonable(self.projection_policy)
            or len(qualification["conditions"]) != len(draft.conditions)
            or len(qualification["exceptions"]) != len(draft.exceptions)
            or not required <= {f["field"] for f in qualification["field_support"]}
        ):
            _fail("project_review_invalid")
        links = {link["id"]: link for link in qualification["links"]}
        if len(links) != len(qualification["links"]):
            _fail("project_review_invalid")
        for field in qualification["field_support"]:
            for group in field["alternatives"]:
                if not group or any(
                    key not in links or field["field"] not in links[key]["fields"] for key in group
                ):
                    _fail("project_review_invalid")
        for link in links.values():
            span = SourceSpan(**link["span"])
            self._span(span, sources)
            source = sources[span.source_event_id]
            authority = self._authority(link["authority"]["source_id"])
            if (
                authority_to_payload(authority) != link["authority"]
                or source.content_hash != link["source_sha256"]
                or link["target_sha256"] != target_fingerprint(draft)
                or draft.subject_id not in authority.subjects
                or draft.predicate not in authority.predicates
            ):
                _fail("project_source_authority_changed")

    async def _publication(self, uow, source_basis, request_ids, headers):
        if source_basis not in {"admitted_l1", "publication_manifest"}:
            _fail("unsupported_project_source_basis")
        request_ids = tuple(request_ids)
        if source_basis == "admitted_l1":
            if request_ids:
                _fail("unexpected_project_publication_manifest")
            return []
        if not 1 <= len(request_ids) <= MAX_CANDIDATES or len(set(request_ids)) != len(request_ids):
            _fail("project_publication_target_required")
        manifests, covered = [], set()
        for key in request_ids:
            identifier(key)
            row = await uow.retention_get(self.scope, "request", key)
            if row is None or not valid_manifest(self.scope, row):
                _fail("project_publication_manifest_unavailable")
            if row["epoch"] != await uow.retention_epoch(self.scope):
                _fail("project_publication_manifest_unavailable")
            if row.get("project_stage") is None:
                _fail("project_publication_target_mismatch")
            manifest = row["publication_manifest"]
            covered.update(d["candidate_id"] for d in manifest["dispositions"])
            manifests.append(deepcopy(manifest))
        if not {h["id"] for h in headers} <= covered:
            _fail("project_publication_target_incomplete")
        return manifests


def project_header(payload):
    """Backend header shape: current membership is separate from historical routes.

    Index by contract_fingerprint plus each project_ids entry and was_unbound.
    Old/new header unions must invalidate atomically on every candidate write,
    including initial stage, qualification, rejection, withdrawal, move and erase.
    ``was_unbound`` is a historical route, never a current unknown classification.
    A None contract is the wildcard unreviewed bucket and MUST be included for
    every contract. Legacy free-form Claims are outside this admitted-L1 census.
    """
    binding = payload.get("project_candidate")
    if binding is None:
        if payload.get("draft", {}).get("predicate") not in PROJECT_PREDICATES:
            return None
        return {
            "schema": CANDIDATE_SCHEMA,
            "contract_fingerprint": None,
            "current_project_id": None,
            "project_ids": [],
            "was_unbound": True,
            "unreviewed": True,
        }
    if binding.get("schema") != CANDIDATE_SCHEMA:
        _fail("project_candidate_schema_unsupported")
    membership = binding["membership"]
    return {
        "schema": CANDIDATE_SCHEMA,
        "contract_fingerprint": binding["contract_fingerprint"],
        "current_project_id": membership["project_id"] if membership else None,
        "project_ids": sorted({m["project_id"] for m in binding["membership_history"]}),
        "was_unbound": binding["was_unbound"],
    }
