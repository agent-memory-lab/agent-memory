"""No generation/validation lineage laundering; minimal opaque erase state."""

from agent_memory.derived.project_index import query_keys
from agent_memory.derived.question_contracts import REGISTRATION_SCHEMA
from agent_memory.derived.question_erasure import erase_question_rows


def row(kind, key, value):
    return {"kind": kind, "identity": key, "payload": value}


def test_processing_manifest_erases_every_revision_and_sensitive_configuration():
    instance = "question-instance:" + "a" * 64
    private = "question-content:" + "b" * 64
    public = "question-content:" + "c" * 64
    rows = [
        row(
            "question_content",
            private,
            {
                "instance_id": instance,
                "generation_manifest": {"inputs": [{"kind": "source", "id": "private-input"}]},
                "secret": "private-content",
            },
        ),
        row(
            "question_content",
            public,
            {
                "instance_id": instance,
                "generation_manifest": {"inputs": [{"kind": "source", "id": "public-input"}]},
                "secret": "public-but-equal-content",
            },
        ),
        row(
            "question_certificate",
            "certificate",
            {
                "instance_id": instance,
                "validation_manifest": {"inputs": [{"kind": "source", "id": "public-input"}]},
                "support": [],
            },
        ),
        row(
            "question_registration",
            "opaque-registration",
            {
                "instance_id": instance,
                "aliases": ["sensitive-alias"],
                "context": {"private-context": True},
            },
        ),
        row(
            "definition",
            instance,
            {
                "facet_id": instance,
                "spec": {
                    "schema": REGISTRATION_SCHEMA,
                    "contract_fingerprint": "contract",
                    "project_id": "sensitive-project",
                    "context": {"secret": True},
                },
            },
        ),
        row("job", "opaque-job", {"unit": {"facet_id": instance}, "status": "running"}),
    ]
    changes, affected, owners = erase_question_rows(rows, {"source:private-input"}, False)
    assert affected == {instance}
    assert len(changes) == len(rows)
    assert {private, public, instance} <= owners
    encoded = repr(changes)
    for secret in (
        "private-input",
        "private-content",
        "public-but-equal-content",
        "sensitive-alias",
        "private-context",
        "sensitive-project",
    ):
        assert secret not in encoded
    assert rows[0]["payload"]["secret"] == "private-content"  # input is never mutated


def test_empty_and_unbuilt_queries_follow_exact_predelete_project_routes():
    instance = "question-instance:" + "a" * 64
    spec = {
        "schema": REGISTRATION_SCHEMA,
        "contract_fingerprint": "contract",
        "project_id": "project",
    }
    rows = [row("definition", instance, {"facet_id": instance, "spec": spec})]
    assert not erase_question_rows(rows, {"source:irrelevant"}, False)[0]
    routes = query_keys("contract", "project")
    changes, affected, owners = erase_question_rows(rows, set(), False, project_routes=routes)
    assert affected == {instance}
    assert changes[0][2]["disabled"] is True
    assert changes[0][2]["spec"] == {"id": instance, "schema": "question-erased/1"}


def test_original_candidate_and_rejected_source_refs_are_erasure_dependencies():
    instance = "question-instance:" + "a" * 64
    rows = [
        row(
            "question_head",
            instance,
            {
                "head": {"instance_id": instance},
                "proof": {
                    "sources": [{"source_event_id": "rejected"}],
                    "candidates": [{"id": "pending", "source_ids": ["unreviewed"]}],
                },
            },
        )
    ]
    for erased in ("source:rejected", "source:unreviewed", "atom:pending"):
        assert erase_question_rows(rows, {erased}, False)[1] == {instance}


def test_explicit_legacy_derived_erase_never_rewrites_observation_definitions():
    rows = [
        row(
            "definition",
            "language",
            {
                "facet_id": "language",
                "spec": {"template_version": "locale/1"},
                "slots": ["slot"],
            },
        )
    ]
    assert erase_question_rows(rows, {"derived:language"}, False) == ([], set(), set())
    assert erase_question_rows(rows, set(), True) == ([], set(), set())
