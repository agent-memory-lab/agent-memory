"""Deterministic full materialization; no models or unchecked standalone delivery."""

import json
from copy import deepcopy
from datetime import datetime

from ..serialization import to_jsonable
from .model import DerivedError, digest
from .project_index import project_key
from .question_contracts import ALGORITHM
from .question_delta import OPERATOR, evaluate
from .question_model import (
    AnswerStatus,
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
    result, delta_state, trace = evaluate(contract, snapshot)
    manifest = input_manifest(snapshot["proof"], census.snapshot.id)
    generation_manifest = manifest
    generation_proof = deepcopy(snapshot["proof"])
    if trace["compute_mode"] == "delta":
        old_content = QuestionContent.from_payload(snapshot["previous_content"])
        header = snapshot["expected_head"]
        references = {(r.kind, r.id, r.revision): r for r in old_content.generation_manifest.inputs}
        references.update({(r.kind, r.id, r.revision): r for r in manifest.inputs})
        for kind, key, sha in (
            ("derived_content", old_content.id, header["content_sha256"]),
            (
                "derived_certificate",
                header["head"]["certificate_revision_id"],
                header["certificate_sha256"],
            ),
        ):
            ref = InputReference(kind, key, "1", sha)
            references[(kind, key, "1")] = ref
        if len(references) > instance.definition.max_dependencies:
            result, delta_state, trace = evaluate(contract, {**snapshot, "delta_state": None})
            trace["fallback_reason"] = "generation_dependency_capacity"
        else:
            generation_manifest = InputManifest(
                tuple(references.values()), census.snapshot.id, OPERATOR
            )
            # Cached group rows were actual generation inputs. Preserve their
            # complete flattened original safety census, including removed rows.
            for key, identity in (("sources", "source_event_id"), ("candidates", "id")):
                inherited = header["generation_proof"][key]
                merged = {item[identity]: item for item in inherited}
                merged.update({item[identity]: item for item in generation_proof[key]})
                generation_proof[key] = list(merged.values())
    support_candidates = {
        candidate["fact"]["id"]: candidate
        for row in result["rows"]
        for field in row["fields"]
        for candidate in field["candidates"]
    }
    spans = {}
    for candidate in support_candidates.values():
        for evidence in candidate["qualification"]["field_evidence"]:
            for branch in evidence["alternatives"]:
                for span in branch:
                    spans[digest(span)] = span
    support_spans = sorted(
        spans.values(), key=lambda s: (s["source_event_id"], s["start"], s["end"], s["quote"])
    )
    # Clock/snapshot coordinates certify this evaluation. They are never part of
    # reusable semantic content. Actual qualifier intervals and citations remain.
    result_metadata = {
        key: result[key]
        for key in (
            "snapshot_id",
            "input_fingerprint",
            "valid_at",
            "known_at",
            "next_transition_at",
        )
    }
    stable_result = {k: v for k, v in result.items() if k not in result_metadata}
    content = QuestionContent(
        instance,
        AnswerStatus(result["status"]),
        {
            "question": result["question"],
            "answer_status": result["status"],
            "matched_ids": result["matched_ids"],
        },
        {
            "result": stable_result,
            "processing_references": [to_jsonable(span) for span in census.processing_references],
            "citations": support_spans,
        },
        ALGORITHM,
        None,
        generation_manifest,
    )
    prior = snapshot.get("previous_content")
    reused = False
    if prior and snapshot.get("generation_safe") and trace["fallback_reason"] is None:
        old = QuestionContent.from_payload(prior)
        if (
            old.instance == content.instance
            and old.value_digest == content.value_digest
            and old.structure_digest == content.structure_digest
        ):
            content, reused = old, True
            generation_proof = deepcopy(snapshot["expected_head"]["generation_proof"])
            trace["compute_mode"] = "proof_reuse"
    next_transition = result_metadata["next_transition_at"]
    boundaries = [instance.context.expires_at]
    if trace["compute_mode"] != "full" and snapshot.get("generation_until"):
        boundaries.append(datetime.fromisoformat(snapshot["generation_until"]))
    boundaries.extend(
        t
        for t in (
            census.next_transition_at,
            datetime.fromisoformat(next_transition) if next_transition else None,
        )
        if t
    )
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
    support_ids = {span["source_event_id"] for span in support_spans}
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
        "result_metadata": result_metadata,
        "delta_state": delta_state,
        "trace": trace,
        "reused": reused,
        "generation_proof": generation_proof,
    }
    response(content, certificate, snapshot["question_id"], metadata=result_metadata, trace=trace)
    return payload


def response(content, certificate, question_id, *, metadata=None, trace=None):
    """Budget the complete response, including qualifiers, lineage, status and refs."""
    result = to_jsonable(content.structure["result"])
    if metadata:
        result.update(deepcopy(metadata))
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
        "compute_mode": (trace or {}).get("compute_mode", "full"),
        "compute_trace": deepcopy(trace or {}),
        "model_calls": 0,
        "runtime_current_validated": True,
        "result": result,
        "citations": to_jsonable(content.structure["citations"]),
        "processing_references": to_jsonable(content.structure["processing_references"]),
        "generation_manifest": content.generation_manifest.payload(),
        "validation_manifest": certificate.validation_manifest.payload(),
        "digests": content_digests(content, certificate),
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


def content_digests(content, certificate):
    """Pure business projection retains conditions, time, unknown and conflicts.

    Only explicit provenance fields are removed. Source/explanation consumers
    must also compare structure/support/validation and still validate safety.
    """
    result = to_jsonable(content.structure["result"])
    for row in result["rows"]:
        for field in row["fields"]:
            field["candidates"] = [
                {
                    k: v
                    for k, v in candidate["fact"].items()
                    if k not in {"id", "known_from", "known_to"}
                }
                for candidate in field["candidates"]
            ]
    result.pop("processing_references", None)
    return dict(
        value=digest(result),
        structure=content.structure_digest,
        support=certificate.support_digest,
        generation=content.generation_manifest_digest,
        validation=certificate.validation_digest,
        safety=certificate.safety_fingerprint,
    )
