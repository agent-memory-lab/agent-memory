from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_memory import OntologyClass, OntologyProperty, OntologySchema
from agent_memory.ontology_schema import (
    ONTOLOGY_SCHEMA_FORMAT,
    OntologyChangeSeverity,
    deserialize_ontology_schema,
    diff_ontology_schemas,
    ontology_schema_digest,
    plan_ontology_migration,
    serialize_ontology_schema,
)


NOW = datetime(2026, 9, 23, tzinfo=UTC)


def _schema(version: str = "1.0.0") -> OntologySchema:
    return OntologySchema(
        ontology_id="agent.knowledge",
        version=version,
        classes=(OntologyClass("person", "Person"),),
        properties=(OntologyProperty("display_name", "Display name", "person"),),
        created_at=NOW,
    )


def test_schema_json_round_trip_and_digest_are_deterministic() -> None:
    schema = _schema()
    payload = serialize_ontology_schema(schema)
    loaded = deserialize_ontology_schema(payload)

    assert loaded == schema
    assert ONTOLOGY_SCHEMA_FORMAT in payload
    assert ontology_schema_digest(loaded) == ontology_schema_digest(schema)
    assert len(ontology_schema_digest(schema)) == 64


def test_additive_schema_change_is_automatic_with_minor_version() -> None:
    previous = _schema()
    current = OntologySchema(
        ontology_id=previous.ontology_id,
        version="1.1.0",
        classes=(*previous.classes, OntologyClass("project", "Project")),
        properties=previous.properties,
        created_at=NOW,
    )

    diff = diff_ontology_schemas(previous, current)
    plan = plan_ontology_migration(previous, current)

    assert diff.compatible is True
    assert diff.highest_severity is OntologyChangeSeverity.ADDITIVE
    assert plan.ready is True
    assert plan.required_version_bump == "minor"
    assert plan.actions == ("register_schema_version", "rebuild_ontology_index")


def test_breaking_schema_change_requires_major_version_and_host_approval() -> None:
    previous = _schema()
    previous = replace(
        previous,
        properties=(
            *previous.properties,
            OntologyProperty("nickname", "Nickname", "person"),
        ),
    )
    invalid_version = OntologySchema(
        ontology_id=previous.ontology_id,
        version="1.1.0",
        classes=previous.classes,
        properties=previous.properties[1:],
        created_at=NOW,
    )
    valid_version = replace(invalid_version, version="2.0.0")

    invalid_plan = plan_ontology_migration(previous, invalid_version)
    valid_plan = plan_ontology_migration(previous, valid_version)

    assert invalid_plan.automatic is False
    assert invalid_plan.version_policy_valid is False
    assert valid_plan.automatic is False
    assert valid_plan.version_policy_valid is True
    assert valid_plan.ready is False
    assert valid_plan.actions[0] == "require_host_approval"


def test_metadata_change_only_requires_patch_version() -> None:
    previous = _schema()
    current = OntologySchema(
        ontology_id=previous.ontology_id,
        version="1.0.1",
        classes=(OntologyClass("person", "Human"),),
        properties=previous.properties,
        created_at=NOW,
    )

    plan = plan_ontology_migration(previous, current)

    assert plan.ready is True
    assert plan.required_version_bump == "patch"
    assert plan.diff.changes[0].severity is OntologyChangeSeverity.METADATA


def test_schema_import_rejects_unknown_fields_and_version_downgrade() -> None:
    document = {
        "format": ONTOLOGY_SCHEMA_FORMAT,
        "ontology_id": "agent.knowledge",
        "version": "1.0.0",
        "created_at": "2026-09-23T00:00:00Z",
        "classes": [],
        "properties": [],
        "unexpected": True,
    }
    with pytest.raises(ValueError, match="unknown unexpected"):
        deserialize_ontology_schema(document)
    with pytest.raises(ValueError, match="must be newer"):
        diff_ontology_schemas(_schema("1.1.0"), _schema("1.0.0"))
