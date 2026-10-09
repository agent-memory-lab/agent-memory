"""Opt-in exact registered QuestionView reuse without another retained-body cache.

QuestionService owns materialization, atomic publication, complete query
frontiers, original-generation lineage, authorization and erasure. This adapter
only returns an immutable, configuration-bound projection after revalidation.
It does not accept arbitrary MemoryBundles, cache queries, run a model, refresh
an unavailable view, or turn partial evidence into an absence proof.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from ..derived.model import DerivedError
from ..derived.question_service import QuestionService
from .model_contracts import canonical, digest


@dataclass(frozen=True, slots=True)
class EvidencePackConfiguration:
    revision: str = "certified-question-pack/1"
    max_output_bytes: int = 262_144

    def __post_init__(self):
        if self.revision != "certified-question-pack/1":
            raise ValueError("unsupported evidence-pack projection")
        if type(self.max_output_bytes) is not int or not 1 <= self.max_output_bytes <= 1_048_576:
            raise ValueError("max_output_bytes must be between 1 and 1048576")

    @property
    def fingerprint(self):
        return digest(asdict(self))


@dataclass(frozen=True, slots=True)
class QuestionEvidencePack:
    key: str
    question_id: str
    content_revision_id: str
    certificate_revision_id: str
    result_json: str
    proof_manifest_json: str
    configuration_sha256: str
    qualification: str
    materialization: str = "existing-certified-question-view"
    model_calls: int = 0


class QuestionEvidencePacks:
    """Host-only current evidence-pack reuse; disabled until explicitly enabled.

    No text or vector is retained in this adapter between calls. Every hit reads
    the current authorized QuestionView through its closed-frontier proof. Exact
    source changes, erased original inputs, processing/read revocation, context
    changes, or time expiry deny reuse even if public answer text is unchanged.
    """

    def __init__(
        self,
        questions,
        *,
        configuration=None,
        enabled=False,
        approval=None,
        verify_approval=None,
        contract_test_only=False,
    ):
        from .feature_gate import validate_feature_enablement

        if type(questions) is not QuestionService:
            raise TypeError("QuestionEvidencePacks requires the concrete QuestionService")
        self.questions = questions
        self.configuration = configuration or EvidencePackConfiguration()
        if type(self.configuration) is not EvidencePackConfiguration:
            raise TypeError("invalid evidence-pack configuration")
        self._enablement = dict(
            feature="evidence-pack-reuse",
            configuration_sha256=self.configuration.fingerprint,
            enabled=enabled,
            approval=approval,
            verify_approval=verify_approval,
            contract_test_only=contract_test_only,
        )
        self.qualification = validate_feature_enablement(**self._enablement)

    def _qualification(self, configuration):
        from .feature_gate import validate_feature_enablement

        if type(configuration) is not EvidencePackConfiguration:
            raise DerivedError("evidence_pack_configuration_changed")
        configuration.__post_init__()
        fingerprint = configuration.fingerprint
        enablement = dict(self._enablement)
        approval = enablement.get("approval")
        approval_fields = asdict(approval) if approval is not None else None
        if fingerprint != enablement["configuration_sha256"]:
            raise DerivedError("evidence_pack_configuration_changed")
        qualification = validate_feature_enablement(**enablement)
        # The synchronous verifier is host code and may take time or replace
        # configuration. Bind the exact live configuration after that callback.
        if (
            self.configuration != configuration
            or configuration.fingerprint != fingerprint
            or self._enablement != enablement
            or (asdict(approval) if approval is not None else None) != approval_fields
        ):
            raise DerivedError("evidence_pack_configuration_changed")
        return qualification

    async def read(
        self, question_id, *, actor, purpose, audience=None, valid_at=None, known_at=None
    ):
        from ..conditions import instant
        from ..derived.model import identity
        from ..derived.question_materialize import check_time

        # Revalidate host promotion/rollback authority on each use. This grants
        # no data access; the concrete storage proof below remains mandatory.
        configuration = self.configuration
        qualification = self._qualification(configuration)
        if qualification == "disabled":
            raise DerivedError("evidence_pack_reuse_disabled")
        if valid_at is not None or known_at is not None:
            raise DerivedError("question_historical_unsupported")
        identity(question_id)
        identity(actor)
        identity(purpose)
        audience = actor if audience is None else audience
        if audience != actor:
            raise DerivedError("evidence_pack_shared_audience_unsupported")
        service = self.questions
        observed = await service._clock_barrier()
        try:
            async with service.repository.unit_of_work() as uow:
                await service._open(uow)
                metadata = await service.model_input_header(uow, question_id, actor=actor)
                definition, header = metadata["definition"], metadata["header"]
                if purpose != definition["spec"]["purpose"]:
                    raise DerivedError("evidence_pack_purpose_mismatch")
                # Only the existing guarded read may load the cached body.
                result = await service._read_in_uow(uow, question_id, actor=actor)
                if result["generation_manifest"] != metadata["original_generation_manifest"]:
                    raise DerivedError("evidence_pack_original_generation_changed")
                result = {key: value for key, value in result.items() if key != "refresh_status"}
                manifest = dict(
                    schema="question-evidence-pack-proof/1",
                    scope_key=service.scope.partition_key(),
                    actor=actor,
                    purpose=purpose,
                    audience=audience,
                    configuration_sha256=configuration.fingerprint,
                    valid_at=None,
                    known_at=None,
                    definition=definition,
                    header=header,
                    original_generation_manifest=metadata["original_generation_manifest"],
                    source_ids=metadata["source_ids"],
                    result_sha256=digest(result),
                )
                result_json, manifest_json = canonical(result), canonical(manifest)
                if (
                    len(result_json.encode()) + len(manifest_json.encode())
                    > configuration.max_output_bytes
                ):
                    raise DerivedError("evidence_pack_output_capacity")
                pack = QuestionEvidencePack(
                    digest(manifest),
                    question_id,
                    result["content_revision_id"],
                    result["certificate_revision_id"],
                    result_json,
                    manifest_json,
                    manifest["configuration_sha256"],
                    qualification,
                )
                latest = await service.model_input_header(uow, question_id, actor=actor)
                if latest != metadata:
                    raise DerivedError("evidence_pack_proof_changed")
                await service._guard(uow, definition, header, actor=actor)
                guarded_at = instant(service.clock())
                if self._qualification(configuration) != qualification:
                    raise DerivedError("evidence_pack_configuration_changed")
                # No additional adapter-level body/storage read or host callback
                # follows; normal UoW completion still occurs. Authorization is
                # bound to this guarded snapshot, as in QuestionService.read.
                service._input_guard(definition["spec"], guarded_at)
                check_time(header, service.clock())
                return pack
        except BaseException:
            observed = max(observed, instant(service.clock()))
            try:
                await service._clock_barrier(observed_at=observed)
            except DerivedError as error:
                if error.code != "refresh_clock_discontinuity":
                    raise
            raise
