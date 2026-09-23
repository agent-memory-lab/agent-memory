"""Deterministic schema exchange and governed Ontology Memory evolution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
import json
import re
from types import MappingProxyType
from typing import Any

from .ontology_memory import OntologyClass, OntologyProperty, OntologySchema


ONTOLOGY_SCHEMA_FORMAT = "agent-memory.ontology-schema/v1"
_MAX_SCHEMA_BYTES = 1_048_576
_SEMVER = re.compile(
    r"(?P<major>0|[1-9]\d*)\."
    r"(?P<minor>0|[1-9]\d*)\."
    r"(?P<patch>0|[1-9]\d*)"
    r"(?:-[0-9A-Za-z.-]+)?\Z"
)


class OntologyChangeSeverity(StrEnum):
    METADATA = "metadata"
    ADDITIVE = "additive"
    BREAKING = "breaking"


@dataclass(frozen=True, slots=True)
class OntologySchemaChange:
    path: str
    operation: str
    severity: OntologyChangeSeverity
    reason: str
    before: Any = None
    after: Any = None

    def __post_init__(self) -> None:
        if not self.path or not self.operation or not self.reason:
            raise ValueError("schema change fields must be non-empty")
        object.__setattr__(self, "severity", OntologyChangeSeverity(self.severity))


@dataclass(frozen=True, slots=True)
class OntologySchemaDiff:
    ontology_id: str
    from_version: str
    to_version: str
    changes: tuple[OntologySchemaChange, ...]

    @property
    def compatible(self) -> bool:
        return all(
            change.severity is not OntologyChangeSeverity.BREAKING
            for change in self.changes
        )

    @property
    def requires_approval(self) -> bool:
        return not self.compatible

    @property
    def highest_severity(self) -> OntologyChangeSeverity:
        if any(
            change.severity is OntologyChangeSeverity.BREAKING
            for change in self.changes
        ):
            return OntologyChangeSeverity.BREAKING
        if any(
            change.severity is OntologyChangeSeverity.ADDITIVE
            for change in self.changes
        ):
            return OntologyChangeSeverity.ADDITIVE
        return OntologyChangeSeverity.METADATA


@dataclass(frozen=True, slots=True)
class OntologyMigrationPlan:
    diff: OntologySchemaDiff
    automatic: bool
    version_policy_valid: bool
    required_version_bump: str
    actions: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.automatic and self.version_policy_valid


def serialize_ontology_schema(schema: OntologySchema) -> str:
    """Return stable JSON suitable for source control, signing, and hashing."""
    if not isinstance(schema, OntologySchema):
        raise TypeError("schema must be an OntologySchema")
    return json.dumps(
        dict(ontology_schema_document(schema)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def ontology_schema_document(schema: OntologySchema) -> Mapping[str, Any]:
    if not isinstance(schema, OntologySchema):
        raise TypeError("schema must be an OntologySchema")
    created_at = schema.created_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    document = {
        "format": ONTOLOGY_SCHEMA_FORMAT,
        "ontology_id": schema.ontology_id,
        "version": schema.version,
        "created_at": created_at,
        "classes": [
            {
                "id": value.class_id,
                "label": value.label,
                "parents": sorted(value.parent_ids),
                "description": value.description,
            }
            for value in sorted(schema.classes, key=lambda item: item.class_id)
        ],
        "properties": [
            {
                "id": value.property_id,
                "label": value.label,
                "domain": value.domain_class,
                "range": value.range_class,
                "description": value.description,
                "functional": value.functional,
            }
            for value in sorted(schema.properties, key=lambda item: item.property_id)
        ],
    }
    return MappingProxyType(document)


def ontology_schema_digest(schema: OntologySchema) -> str:
    return sha256(serialize_ontology_schema(schema).encode("utf-8")).hexdigest()


def deserialize_ontology_schema(payload: str | bytes | Mapping[str, Any]) -> OntologySchema:
    if isinstance(payload, bytes):
        if len(payload) > _MAX_SCHEMA_BYTES:
            raise ValueError("ontology schema exceeds the size limit")
        try:
            raw: Any = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("ontology schema is not valid UTF-8 JSON") from error
    elif isinstance(payload, str):
        if len(payload.encode("utf-8")) > _MAX_SCHEMA_BYTES:
            raise ValueError("ontology schema exceeds the size limit")
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as error:
            raise ValueError("ontology schema is not valid JSON") from error
    elif isinstance(payload, Mapping):
        raw = dict(payload)
    else:
        raise TypeError("payload must be JSON text, bytes, or an object")
    if not isinstance(raw, dict):
        raise ValueError("ontology schema document must be an object")
    _require_keys(
        raw,
        {"format", "ontology_id", "version", "created_at", "classes", "properties"},
        "schema",
    )
    if raw["format"] != ONTOLOGY_SCHEMA_FORMAT:
        raise ValueError("unsupported ontology schema format")
    created_at = _parse_datetime(raw["created_at"])
    classes_raw = _object_list(raw["classes"], "classes")
    properties_raw = _object_list(raw["properties"], "properties")
    classes = tuple(_load_class(value, index) for index, value in enumerate(classes_raw))
    properties = tuple(
        _load_property(value, index) for index, value in enumerate(properties_raw)
    )
    return OntologySchema(
        ontology_id=_text(raw["ontology_id"], "ontology_id"),
        version=_text(raw["version"], "version"),
        classes=classes,
        properties=properties,
        created_at=created_at,
    )


def diff_ontology_schemas(
    previous: OntologySchema,
    current: OntologySchema,
) -> OntologySchemaDiff:
    if not isinstance(previous, OntologySchema) or not isinstance(current, OntologySchema):
        raise TypeError("previous and current must be OntologySchema values")
    if previous.ontology_id != current.ontology_id:
        raise ValueError("cannot compare schemas from different ontologies")
    if _version(current.version) <= _version(previous.version):
        raise ValueError("current ontology version must be newer than previous version")

    changes: list[OntologySchemaChange] = []
    old_classes = {value.class_id: value for value in previous.classes}
    new_classes = {value.class_id: value for value in current.classes}
    for class_id in sorted(old_classes.keys() - new_classes.keys()):
        changes.append(
            _change(
                f"classes.{class_id}",
                "remove",
                OntologyChangeSeverity.BREAKING,
                "removing a class can orphan existing entities",
                old_classes[class_id],
                None,
            )
        )
    for class_id in sorted(new_classes.keys() - old_classes.keys()):
        changes.append(
            _change(
                f"classes.{class_id}",
                "add",
                OntologyChangeSeverity.ADDITIVE,
                "new classes do not invalidate existing entities",
                None,
                new_classes[class_id],
            )
        )
    for class_id in sorted(old_classes.keys() & new_classes.keys()):
        old = old_classes[class_id]
        new = new_classes[class_id]
        if old.parent_ids != new.parent_ids:
            changes.append(
                _change(
                    f"classes.{class_id}.parents",
                    "replace",
                    OntologyChangeSeverity.BREAKING,
                    "changing inheritance can alter entity validation",
                    old.parent_ids,
                    new.parent_ids,
                )
            )
        _metadata_changes(changes, f"classes.{class_id}", old, new)

    old_properties = {value.property_id: value for value in previous.properties}
    new_properties = {value.property_id: value for value in current.properties}
    for property_id in sorted(old_properties.keys() - new_properties.keys()):
        changes.append(
            _change(
                f"properties.{property_id}",
                "remove",
                OntologyChangeSeverity.BREAKING,
                "removing a property can invalidate existing assertions",
                old_properties[property_id],
                None,
            )
        )
    for property_id in sorted(new_properties.keys() - old_properties.keys()):
        changes.append(
            _change(
                f"properties.{property_id}",
                "add",
                OntologyChangeSeverity.ADDITIVE,
                "new properties do not invalidate existing assertions",
                None,
                new_properties[property_id],
            )
        )
    for property_id in sorted(old_properties.keys() & new_properties.keys()):
        old = old_properties[property_id]
        new = new_properties[property_id]
        for field_name, reason in (
            ("domain_class", "changing a property domain can invalidate subjects"),
            ("range_class", "changing a property range can invalidate objects"),
            ("functional", "changing cardinality can create assertion conflicts"),
        ):
            old_value = getattr(old, field_name)
            new_value = getattr(new, field_name)
            if old_value != new_value:
                changes.append(
                    _change(
                        f"properties.{property_id}.{field_name}",
                        "replace",
                        OntologyChangeSeverity.BREAKING,
                        reason,
                        old_value,
                        new_value,
                    )
                )
        _metadata_changes(changes, f"properties.{property_id}", old, new)

    changes.sort(key=lambda value: (value.path, value.operation, value.severity.value))
    return OntologySchemaDiff(
        ontology_id=previous.ontology_id,
        from_version=previous.version,
        to_version=current.version,
        changes=tuple(changes),
    )


def plan_ontology_migration(
    previous: OntologySchema,
    current: OntologySchema,
) -> OntologyMigrationPlan:
    diff = diff_ontology_schemas(previous, current)
    old_version = _version(previous.version)
    new_version = _version(current.version)
    severity = diff.highest_severity
    if severity is OntologyChangeSeverity.BREAKING:
        required = "major"
        policy_valid = new_version[0] > old_version[0]
        actions = (
            "require_host_approval",
            "register_schema_version",
            "revalidate_existing_projections",
            "rebuild_ontology_index",
            "retain_previous_version_for_rollback",
        )
    elif severity is OntologyChangeSeverity.ADDITIVE:
        required = "minor"
        policy_valid = new_version[0] > old_version[0] or (
            new_version[0] == old_version[0] and new_version[1] > old_version[1]
        )
        actions = ("register_schema_version", "rebuild_ontology_index")
    else:
        required = "patch"
        policy_valid = new_version > old_version
        actions = ("register_schema_version",)
    return OntologyMigrationPlan(
        diff=diff,
        automatic=diff.compatible,
        version_policy_valid=policy_valid,
        required_version_bump=required,
        actions=actions,
    )


def _load_class(value: Mapping[str, Any], index: int) -> OntologyClass:
    _require_keys(value, {"id", "label", "parents", "description"}, f"classes[{index}]")
    parents = value["parents"]
    if not isinstance(parents, list) or any(not isinstance(item, str) for item in parents):
        raise ValueError(f"classes[{index}].parents must be a string array")
    return OntologyClass(
        class_id=_text(value["id"], f"classes[{index}].id"),
        label=_text(value["label"], f"classes[{index}].label"),
        parent_ids=tuple(parents),
        description=_string(value["description"], f"classes[{index}].description"),
    )


def _load_property(value: Mapping[str, Any], index: int) -> OntologyProperty:
    _require_keys(
        value,
        {"id", "label", "domain", "range", "description", "functional"},
        f"properties[{index}]",
    )
    range_class = value["range"]
    if range_class is not None and not isinstance(range_class, str):
        raise ValueError(f"properties[{index}].range must be a string or null")
    functional = value["functional"]
    if type(functional) is not bool:
        raise ValueError(f"properties[{index}].functional must be a boolean")
    return OntologyProperty(
        property_id=_text(value["id"], f"properties[{index}].id"),
        label=_text(value["label"], f"properties[{index}].label"),
        domain_class=_text(value["domain"], f"properties[{index}].domain"),
        range_class=range_class,
        description=_string(value["description"], f"properties[{index}].description"),
        functional=functional,
    )


def _metadata_changes(changes: list[OntologySchemaChange], prefix: str, old: Any, new: Any) -> None:
    for field_name in ("label", "description"):
        old_value = getattr(old, field_name)
        new_value = getattr(new, field_name)
        if old_value != new_value:
            changes.append(
                _change(
                    f"{prefix}.{field_name}",
                    "replace",
                    OntologyChangeSeverity.METADATA,
                    "descriptive metadata does not alter stored knowledge semantics",
                    old_value,
                    new_value,
                )
            )


def _change(
    path: str,
    operation: str,
    severity: OntologyChangeSeverity,
    reason: str,
    before: Any,
    after: Any,
) -> OntologySchemaChange:
    return OntologySchemaChange(path, operation, severity, reason, before, after)


def _version(value: str) -> tuple[int, int, int]:
    matched = _SEMVER.fullmatch(value)
    if matched is None:
        raise ValueError(f"ontology version {value!r} must be semantic x.y.z")
    return tuple(int(matched.group(name)) for name in ("major", "minor", "patch"))


def _parse_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("created_at must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("created_at must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("created_at must include a timezone")
    return parsed


def _object_list(value: Any, field: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ValueError(f"{field} must be an object array")
    return value


def _require_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unknown:
            details.append("unknown " + ", ".join(unknown))
        raise ValueError(f"{field} has invalid fields: {'; '.join(details)}")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    return value
