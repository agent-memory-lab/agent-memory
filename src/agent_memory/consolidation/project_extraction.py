"""Trusted automatic transfer of reviewed raw proposals into project admission.

Extraction establishes fidelity and reuse. This bridge binds host membership and
source authority, never truth: every positive project proposal awaits independent
field qualification. Publication shares the receive lease's existing transaction.
"""

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime

from ..conditions import identifier
from ..derived.model import DerivedError, digest
from .admission import authority_to_payload, draft_from_payload, draft_to_payload
from .atom_extraction import _key
from .project_admission import CANDIDATE_SCHEMA, PROJECT_PREDICATES, ProjectAdmission


class ProjectExtractionBridge:
    """Host-only finite subject -> registered membership map.

    Missing/ambiguous mappings remain unresolved. Text, model confidence and
    request metadata cannot choose a project or grant a processing permission.
    The complete setup fingerprint is part of durable extraction configuration.
    """

    def __init__(
        self,
        admission,
        *,
        membership_ids,
        source_authority_id,
        revision,
        observation_time_predicates=(),
    ):
        if type(admission) is not ProjectAdmission or type(membership_ids) is not dict:
            raise ValueError("trusted project admission and membership mapping required")
        identifier(revision)
        identifier(source_authority_id)
        if len(membership_ids) > 1024:
            raise ValueError("bounded project membership map required")
        self.admission = admission
        self.membership_ids = dict(membership_ids)
        self.source_authority_id = source_authority_id
        self.revision = revision
        self.observation_time_predicates = tuple(observation_time_predicates)
        self.config_payload()

    def config_payload(self):
        identifier(self.revision)
        authority = self.admission._authority(self.source_authority_id)
        if authority.kind not in {"self_report", "document", "tool_observation"}:
            raise ValueError("authenticated host project capture required")
        if (
            len(self.observation_time_predicates) > 64
            or len(set(self.observation_time_predicates)) != len(self.observation_time_predicates)
            or not set(self.observation_time_predicates)
            <= {spec.predicate for spec in self.admission.contract.predicate_specs}
        ):
            raise ValueError("registered unique host observation-time predicates required")
        for subject, key in self.membership_ids.items():
            identifier(subject)
            identifier(key)
            item = self.admission.memberships.get(key)
            if item is None or item.entity_id != subject:
                raise ValueError("project extraction membership is not host registered")
        return {
            "schema": "project-extraction-bridge/1",
            "revision": self.revision,
            "registration_fingerprint": self.admission.registration_fingerprint,
            "source_authority": authority_to_payload(authority),
            "membership_ids": dict(sorted(self.membership_ids.items())),
            "observation_time_predicates": sorted(self.observation_time_predicates),
        }

    @property
    def fingerprint(self):
        return digest(self.config_payload())

    @staticmethod
    def verification_eligible(row):
        transfer = row["payload"].get("project_extraction")
        return transfer is None or transfer.get("verification_eligible") is True

    def _route(self, draft):
        try:
            self.admission._draft(draft)
            return self.admission._membership(self.membership_ids.get(draft.subject_id), draft)
        except DerivedError:
            # Keep a malformed/unregistered proposal in the unresolved census;
            # it cannot be promoted by merely finding a matching domain tool.
            return False

    async def validate_source(self, unit_of_work, source):
        """Return a local final fence: database locks alone do not freeze time."""
        registration = self.fingerprint
        at = self.admission.clock()
        grants = await self.admission._grants(unit_of_work, (source.id,), at)
        authority = await self.admission.registry.authority(
            unit_of_work, self.admission.authority_id
        )
        deadlines = [
            datetime.fromisoformat(grant["expires_at"])
            for grant in grants.values()
            if grant.get("expires_at")
        ]
        if authority:
            deadlines.append(datetime.fromisoformat(authority["spec"]["expires_at"]))

        def fence():
            now = self.admission.clock()
            if (
                now < at
                or self.fingerprint != registration
                or any(deadline <= now for deadline in deadlines)
            ):
                raise ValueError("project extraction processing permission changed or expired")

        fence()
        return fence

    def _bound_stage(self, source, prepared):
        """Pin explicit host temporal normalization while retaining raw uncertainty."""
        staged = deepcopy(prepared)
        gates, time_bindings = {}, []
        for index, raw in enumerate(prepared["drafts"]):
            original = draft_from_payload(raw)
            gate = prepared["gates"][_key(original)]
            draft = original
            if (
                original.valid_from is None
                and original.predicate in self.observation_time_predicates
            ):
                observed_at = source.occurred_at.astimezone(UTC)
                draft = replace(original, valid_from=observed_at)
                staged["drafts"][index] = draft_to_payload(draft)
                time_bindings.append(
                    {
                        "draft_index": index,
                        "original_proposal_sha256": digest(raw),
                        "original_valid_from": None,
                        "bound_valid_from": observed_at.isoformat(),
                        "reason": "host_observation_time_policy",
                        "revision": self.revision,
                    }
                )
                gate = (gate[0], (*gate[1], "host_observation_time_policy"))
                for report in staged["audit"]["reports"]:
                    if report["draft_index"] == index:
                        report["reasons"].append("host_observation_time_policy")
            key = _key(draft)
            if key in gates and gates[key] != gate:
                # Distinct raw proposals can meet at the same host-bound time.
                # Their review disagreement still requires host resolution.
                gates[key] = ("PENDING_VERIFICATION", ("duplicate_candidate_reviews_disagree",))
            else:
                gates[key] = gate
        staged["gates"] = gates
        if time_bindings:
            staged["audit"]["host_time_bindings"] = time_bindings
        return staged

    async def publish_prepared(
        self, pipeline, repository, source, prepared, *, policy, authority, unit_of_work, request
    ):
        if repository is not self.admission.repository or source.scope != self.admission.scope:
            raise ValueError("project extraction exact repository/scope required")
        if authority != self.admission._authority(self.source_authority_id):
            raise ValueError("project extraction source authority changed")
        registration = self.fingerprint
        publication_fence = await self.validate_source(unit_of_work, source)
        staged = self._bound_stage(source, prepared)
        routes, eligibility = {}, {}
        for index, payload in enumerate(staged["drafts"]):
            draft = draft_from_payload(payload)
            if draft.predicate not in PROJECT_PREDICATES:
                continue
            key = _key(draft)
            route = self._route(draft)
            reports = [r for r in staged["audit"]["reports"] if r["draft_index"] == index]
            gate, reasons = staged["gates"][key]
            base_action, base_reasons = policy.evaluate(source, draft, authority)
            faithful = bool(reports) and all(
                r["faithfulness"] == "supported"
                and r["retention"] == "durable"
                and r["source_start"] is not None
                and source.content[r["source_start"] : r["source_end"]] == draft.source_quote
                for r in reports
            )
            try:
                self.admission._draft(draft, review=True)
                reviewable = True
            except DerivedError:
                reviewable = False
            eligible = (
                route is not False
                and route is not None
                and faithful
                and reviewable
                and gate not in {"REJECT", "L0_ONLY"}
                and base_action not in {"REJECT", "L0_ONLY"}
                and "duplicate_candidate_reviews_disagree" not in reasons
            )
            if base_action in {"REJECT", "L0_ONLY"} and gate != "REJECT":
                # Source uncertainty cannot override an explicit host decision
                # that this predicate is outside the storage/reuse policy.
                staged["gates"][key] = (
                    base_action,
                    tuple(dict.fromkeys((*base_reasons, *reasons))),
                )
                for report in reports:
                    report["action"] = base_action
                    report["reasons"] = list(dict.fromkeys((*base_reasons, *report["reasons"])))
            elif gate == "ACCEPT":
                # This applies even with a precise quote and an authoritative
                # capture source: publication needs native domain field evidence.
                staged["gates"][key] = (
                    "PENDING_VERIFICATION",
                    (*reasons, "project_domain_verification_required"),
                )
                for report in reports:
                    report["action"] = "PENDING_VERIFICATION"
                    report["reasons"].append("project_domain_verification_required")
            routes[key], eligibility[key] = route, eligible
        receipt = await pipeline.publish_prepared(
            repository,
            source,
            staged,
            authority=authority,
            policy=policy,
            unit_of_work=unit_of_work,
            retained=True,
        )
        rows, project_ids = [], []
        metadata = source.metadata["_retention"]
        for key in receipt.candidate_ids:
            row = await unit_of_work.get_admission_record(source.scope, key)
            draft = draft_from_payload(row["payload"]["draft"])
            target = _key(draft)
            if target not in routes:
                rows.append(row)
                continue
            previous = row["payload"].get("project_extraction")
            if previous is not None:
                if previous["bridge_sha256"] != registration:
                    raise ValueError("project extraction publication registration changed")
                rows.append(row)
                project_ids.append(key)
                continue
            row["payload"]["project_extraction"] = {
                "schema": "project-extraction-transfer/1",
                "bridge_sha256": registration,
                "request_id": request["request_id"],
                "source_event_id": source.id,
                "source_sha256": source.content_hash,
                "verification_eligible": eligibility[target],
            }
            times = [
                binding
                for binding in staged["audit"].get("host_time_bindings", ())
                if _key(draft_from_payload(staged["drafts"][binding["draft_index"]])) == target
            ]
            if times:
                row["payload"]["project_extraction"]["host_time_bindings"] = times
                row["payload"]["valid_time_basis"] = "host_observation_time_policy"
            membership = routes[target]
            if membership is not False:
                row["payload"]["project_candidate"] = {
                    "schema": CANDIDATE_SCHEMA,
                    "contract_fingerprint": self.admission.contract.fingerprint,
                    "contract_id": self.admission.contract.id,
                    "contract_version": self.admission.contract.version,
                    "principal": self.admission.principal,
                    "purpose": self.admission.purpose,
                    "authority_id": self.source_authority_id,
                    "membership": membership,
                    "membership_history": [membership] if membership else [],
                    "was_unbound": membership is None,
                    "source": {
                        "source_event_id": source.id,
                        "sha256": source.content_hash,
                        "document_id": metadata["document_id"],
                        "revision": metadata["revision"],
                    },
                    "review": None,
                }
            version = await self.admission._save(unit_of_work, row)
            if membership is not False and row["payload"]["action"] in {"REJECT", "L0_ONLY"}:
                await self.admission.reject(
                    key,
                    expected_version=version,
                    review_id="extraction-disposition:" + digest([key, registration]),
                    reasons=("extraction_fidelity_or_retention_rejected",),
                    _unit_of_work=unit_of_work,
                )
            row = await unit_of_work.get_admission_record(source.scope, key)
            rows.append(row)
            project_ids.append(key)
        request["project_stage"] = {
            "sha256": digest([registration, source.id, source.content_hash, project_ids]),
            "bridge_sha256": registration,
            "candidate_ids": project_ids,
            "audit": {
                "schema": "project-extraction-audit/1",
                "original_proposals": [
                    {
                        "draft_index": index,
                        "sha256": digest(raw),
                        "valid_from": raw.get("valid_from"),
                    }
                    for index, raw in enumerate(prepared["drafts"])
                ],
                "host_time_bindings": staged["audit"].get("host_time_bindings", []),
            },
        }
        # Rebuild the receipt after native binding/disposition versions are saved.
        # Worker publication manifests and interpretation heads now see one result.
        if self.fingerprint != registration:
            raise ValueError("project extraction publication registration changed")
        publication_fence()
        return (
            self.admission.engine.receipt(source.id, rows, duplicate=receipt.duplicate),
            publication_fence,
        )
