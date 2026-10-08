"""Deterministic full materialization; no models or unchecked standalone delivery."""

import json
from datetime import datetime

from ..serialization import to_jsonable
from .model import DerivedError, digest
from .project_index import project_key
from .project_questions import full_project_question
from .question_contracts import ALGORITHM
from .question_model import (
    CoverageFrontier,
    InputManifest,
    InputReference,
    QueryCoverage,
    QuestionCertificate,
    QuestionContent,
    QuestionInstance,
    RefreshPolicyRef,
    SourceBasis,
    TimeCoverage,
)


def budget(value, maximum):
    if (
        len(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
        > maximum
    ):
        raise DerivedError("question_output_capacity")
    return value


def input_manifest(proof, snapshot_id):
    inputs = []
    for item in proof["candidates"]:
        inputs.append(InputReference("atom", item["id"], str(item["version"]), digest(item)))
    for item in proof["sources"]:
        inputs.append(
            InputReference(
                "source", item["source_event_id"], str(item["revision"] or 1), item["source_sha256"]
            )
        )
    for key, generation in proof["query_generations"].items():
        inputs.append(InputReference("query", key, str(generation), digest([key, generation])))
    return InputManifest(tuple(inputs), snapshot_id, ALGORITHM)


def materialize(contract, snapshot):
    """Pure oracle consumes an owned full census; service gates persistence/delivery."""
    instance = QuestionInstance.from_payload(snapshot["instance"])
    census = snapshot["census"]
    result = full_project_question(
        contract, census.snapshot, snapshot["question"], overdue_only=snapshot["overdue_only"]
    )
    manifest = input_manifest(snapshot["proof"], census.snapshot.id)
    support_candidates = {
        candidate.fact.id: candidate
        for row in result.rows
        for field in row.fields
        for candidate in field.candidates
    }
    support_spans = sorted(
        {span for candidate in support_candidates.values() for span in candidate.source_references},
        key=lambda span: (span.source_event_id, span.start, span.end, span.quote),
    )
    content = QuestionContent(
        instance,
        result.status,
        {
            "question": result.question,
            "answer_status": result.status.value,
            "matched_ids": list(result.matched_ids),
        },
        {
            "result": json.loads(json.dumps(result.payload(), allow_nan=False)),
            "processing_references": [to_jsonable(span) for span in census.processing_references],
            "citations": [to_jsonable(span) for span in support_spans],
        },
        ALGORITHM,
        None,
        manifest,
    )
    boundaries = [instance.context.expires_at]
    boundaries.extend(t for t in (census.next_transition_at, result.next_transition_at) if t)
    until = min(boundaries)
    at = census.snapshot.context.valid_at
    if until <= at:
        raise DerivedError("derived_time_coverage_expired")
    frontier = CoverageFrontier("exact_units", {}, (snapshot["unit_id"],))
    publication = instance.definition.source_basis == SourceBasis.PUBLICATION_MANIFEST
    query = QueryCoverage(
        instance.definition.query_id,
        instance.definition.query_fingerprint,
        snapshot["proof"]["query_generations"][
            project_key(contract.fingerprint, census.snapshot.project_id)
        ],
        snapshot["proof"]["subscription_sha256"],
        contract.qualification_revision,
        instance.definition.source_basis,
        frontier,
        True,
        None,
        digest(list(census.publication_manifests)) if publication else None,
        True if publication else None,
    )
    support_ids = {span.source_event_id for span in support_spans}
    certificate = QuestionCertificate(
        content.id,
        instance.id,
        instance.definition.scope,
        instance.definition.semantic_fingerprint,
        instance.context.fingerprint,
        manifest,
        frontier,
        query,
        tuple(
            i
            for i in manifest.inputs
            if (i.kind == "source" and i.id in support_ids)
            or (i.kind == "atom" and i.id in support_candidates)
        ),
        TimeCoverage(at, until, at, until),
        digest(snapshot["proof"]),
        snapshot["proof"]["epoch"],
        ALGORITHM,
        RefreshPolicyRef.from_payload(snapshot["refresh_policy"]),
        at,
    )
    certificate.validate_content_binding(content)
    payload = {
        "content": content.payload(),
        "content_id": content.id,
        "certificate": certificate.payload(),
        "certificate_id": certificate.id,
        "next_transition_at": until.isoformat(),
    }
    response(content, certificate, snapshot["question_id"])
    return payload


def response(content, certificate, question_id):
    """Budget the complete response, including qualifiers, lineage, status and refs."""
    result = to_jsonable(content.structure["result"])
    # The protocol oracle remains explicitly known-scope. Runtime adds an actual
    # validated certificate without changing the semantic oracle's honesty flags.
    value = {
        "schema": "question-answer/1",
        "question_id": question_id,
        "instance_id": content.instance.id,
        "content_revision_id": content.id,
        "certificate_revision_id": certificate.id,
        "answer_status": content.answer_status.value,
        "availability_status": "valid",
        "refresh_status": "idle",
        "compute_mode": "full",
        "model_calls": 0,
        "runtime_current_validated": True,
        "result": result,
        "citations": to_jsonable(content.structure["citations"]),
        "processing_references": to_jsonable(content.structure["processing_references"]),
        "generation_manifest": content.generation_manifest.payload(),
        "coverage": certificate.query_coverage.payload(),
        "valid_until": certificate.time_coverage.valid_until.isoformat(),
    }
    return budget(value, content.instance.definition.max_output_bytes)


def check_time(head, at):
    start = datetime.fromisoformat(head["validated_at"])
    end = datetime.fromisoformat(head["next_transition_at"])
    if not start <= at < end:
        raise DerivedError("derived_time_coverage_expired")


def refresh_state(value):
    """Project internal scheduler outcomes onto the versioned independent axis."""
    if value in {"idle", "pending", "running", "retry", "deferred", "dead"}:
        return value
    return {"completed": "idle", "dirty": "pending", "superseded": "pending"}.get(value, "deferred")
