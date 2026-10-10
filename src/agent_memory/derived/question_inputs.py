"""Bounded transaction-local common inputs for closed QuestionView operators.

Only immutable census work is shared. Every consumer still obtains fresh grants,
source/erasure metadata, authority, epoch, frontier and host-clock checks. Nothing
is persisted or reused across transactions, and this is not an answer cache.
"""

import json
from dataclasses import dataclass, replace
from datetime import datetime

from ..consolidation.project_admission import ProjectCensus
from ..serialization import to_jsonable
from .model import DerivedError, digest

SCHEMA = "question-shared-inputs/1"
MAX_BATCH = 8


@dataclass
class QuestionInputWork:
    """Host diagnostics, separate from semantic groups and the answer contract."""

    candidate_census_reads: int = 0
    candidate_census_reuses: int = 0
    qualified_census_builds: int = 0
    qualified_census_reuses: int = 0
    source_proof_reads: int = 0
    authorization_checks: int = 0


def compatibility(service, definition, at):
    spec = definition["spec"]
    instance = spec["instance"]
    return digest(dict(
        schema=SCHEMA,
        scope=to_jsonable(service.scope),
        project=spec["project_id"],
        contract=spec["contract_fingerprint"],
        registration=spec["registration_fingerprint"],
        context=spec["context"],
        principal=service.admission.principal,
        purpose=spec["purpose"],
        readers=spec["readers"],
        authority=service.admission.authority_id,
        authority_floor=service.admission.authority_min_version,
        admission_policy=service.admission.policy,
        projection_policy=to_jsonable(service.admission.projection_policy),
        refresh_policy=instance["definition"]["refresh_policy"],
        source_basis=instance["definition"]["source_basis"],
        publication_requests=spec["publication_request_ids"],
        time=instance["time"],
        valid_at=at.isoformat() if at is not None else None,
        known_at=at.isoformat() if at is not None else None,
    ))


@dataclass(frozen=True, slots=True)
class QualifiedInput:
    """Deeply immutable common input; mutable grant/manifest maps are JSON bytes."""

    snapshot: object
    versions: tuple
    source_ids: tuple
    references: tuple
    metadata: str

    @classmethod
    def capture(cls, census):
        return cls(
            census.snapshot, census.candidate_versions, census.source_ids,
            census.processing_references,
            json.dumps(to_jsonable(dict(
                grants=census.grants, proofs=census.source_proofs,
                registration=census.registration_fingerprint,
                transition=census.next_transition_at,
                candidate_count=census.candidate_count,
                manifests=census.publication_manifests,
            )), sort_keys=True),
        )

    def bind(self, context):
        metadata = json.loads(self.metadata)
        grants = {g["source_id"]: g for g in metadata["grants"]}
        # The per-job token stays in the individual snapshot identity. Sharing
        # must not change existing answer, certificate or provenance contracts.
        snapshot_id = "project-snapshot:" + digest(dict(
            context=to_jsonable(context), versions=self.versions, grants=grants,
            source_proofs=metadata["proofs"], contract=self.snapshot.contract_fingerprint,
            registration=metadata["registration"],
        ))
        return ProjectCensus(
            replace(self.snapshot, id=snapshot_id, context=context),
            self.versions, self.source_ids, self.references,
            tuple(metadata["grants"]), tuple(metadata["proofs"]),
            metadata["registration"],
            datetime.fromisoformat(metadata["transition"]) if metadata["transition"] else None,
            metadata["candidate_count"], tuple(metadata["manifests"]),
        )


class SharedQuestionInputs:
    """An explicit bounded owner for one locked UoW, never attached to a service."""

    def __init__(self, uow, work):
        self._uow, self.work = uow, work
        self._headers, self._qualified = {}, {}

    def check(self, uow):
        if uow is not self._uow:
            raise DerivedError("question_shared_transaction_mismatch")

    async def headers(self, uow, service, definition, at, frontier, epoch):
        self.check(uow)
        # Header enumeration is structural, independent of reader/time/policy.
        # Those bindings are checked freshly by _proof and are all retained in
        # the stricter qualified-census compatibility key below.
        spec = definition["spec"]
        key = digest([SCHEMA, to_jsonable(service.scope), spec["contract_fingerprint"],
                      spec["project_id"], frontier, epoch])
        if key in self._headers:
            self.work.candidate_census_reuses += 1
            return json.loads(self._headers[key])
        spec = definition["spec"]
        value = await uow.derived_project_candidates(
            service.scope, spec["contract_fingerprint"], spec["project_id"]
        )
        self.work.candidate_census_reads += 1
        # Bound memory even when a hostile host mutates controls between awaits.
        if len(self._headers) < MAX_BATCH:
            self._headers[key] = json.dumps(value, sort_keys=True)
        return value

    def census_key(self, service, definition, proof, context):
        common = {k: v for k, v in proof.items() if k != "subscription_sha256"}
        coordinates = to_jsonable(context)
        coordinates.pop("snapshot_token")
        return digest([compatibility(service, definition, context.valid_at), common, coordinates])

    def census(self, uow, key, context):
        self.check(uow)
        value = self._qualified.get(key)
        if value is None:
            return None
        self.work.qualified_census_reuses += 1
        return value.bind(context)

    def remember(self, uow, key, census):
        self.check(uow)
        self.work.qualified_census_builds += 1
        if len(self._qualified) < MAX_BATCH:
            self._qualified[key] = QualifiedInput.capture(census)
