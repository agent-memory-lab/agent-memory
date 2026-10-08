"""Closed project-template definitions and exact, host-owned runtime bindings."""

from .model import DerivedError, digest, identity
from .project_questions import PROJECT_QUESTIONS, project_query_fingerprint
from .question_model import (
    AnswerStatus,
    ComputeMode,
    QuestionDefinition,
    RefreshPolicyRef,
    SourceBasis,
    TimeMode,
)

REGISTRATION_SCHEMA = "question-instance-registration/1"
RUNTIME_SCHEMA = "question-runtime/1"
ALGORITHM = "project-question-full/1"


def _string(*, maximum=256, enum=None):
    result = {"type": "string", "maxLength": maximum}
    if enum is not None:
        result["enum"] = list(enum)
    return result


def _object(properties):
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def project_definition(
    admission,
    question_id,
    project_id,
    question,
    *,
    readers,
    generation=1,
    overdue_only=False,
    source_basis=SourceBasis.ADMITTED_L1,
    max_output_bytes=262144,
    refresh_revision=1,
    refresh_policy_id="project-question-refresh",
):
    """Build one closed host registration, never caller-provided policy semantics."""
    identity(question_id)
    identity(project_id)
    if (
        question not in PROJECT_QUESTIONS
        or type(overdue_only) is not bool
        or (overdue_only and question != "commitments")
    ):
        raise DerivedError("unsupported_project_question")
    if type(source_basis) is not SourceBasis:
        raise DerivedError("unsupported_project_source_basis")
    if not any(m.project_id == project_id for m in admission.memberships.values()):
        raise DerivedError("project_membership_unregistered")
    contract = admission.contract
    return QuestionDefinition(
        id=question_id,
        version=ALGORITHM,
        generation=generation,
        scope=admission.scope,
        parameter_schema=_object(
            {
                "project_id": _string(enum=(project_id,)),
                "overdue_only": {"type": "boolean", "enum": [overdue_only]},
            }
        ),
        output_schema=_object(
            {
                "question": _string(enum=tuple(sorted(PROJECT_QUESTIONS))),
                "answer_status": _string(enum=tuple(s.value for s in AnswerStatus)),
                "matched_ids": {
                    "type": "array",
                    "items": _string(),
                    "maxItems": 1024,
                    "uniqueItems": True,
                },
            }
        ),
        scope_bindings={},
        query_id="project-query:" + digest([contract.fingerprint, project_id]),
        query_fingerprint=project_query_fingerprint(contract, admission.scope, project_id),
        predicate_versions={p.predicate: contract.version for p in contract.predicate_specs},
        qualification_policy_version=contract.qualification_revision,
        business_policy_version=contract.version,
        unknown_semantics="explicit-unknown-not-open/1",
        conflict_semantics="preserve-overlapping-qualified-values/1",
        empty_semantics="complete-known-scope-only/1",
        required_fields=("question", "answer_status", "matched_ids"),
        allowed_parent_kinds=("l1",),
        source_basis=source_basis,
        completeness="complete_candidates",
        time_mode=TimeMode.CURRENT,
        timezone=contract.timezone,
        calendar_version=contract.calendar_version,
        renderer_id=ALGORITHM,
        renderer_version=ALGORITHM,
        allowed_modes=(ComputeMode.FULL,),
        audiences=tuple(readers),
        purposes=(admission.purpose,),
        retention_policy_version="project-question-retention/1",
        max_output_bytes=max_output_bytes,
        max_dependencies=256,
        max_instances=128,
        configuration_version=admission.registration_fingerprint,
        refresh_policy=RefreshPolicyRef(refresh_policy_id, refresh_revision),
    )


def registration_key(question_id):
    """Public labels live only in scrub-able payloads, never durable primary keys."""
    identity(question_id)
    return "question-registration:" + digest(question_id)
