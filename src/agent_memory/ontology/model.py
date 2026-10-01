"""Ontology values, validation, projection rules and store contracts."""

from __future__ import annotations

import math
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
from typing import Any, Protocol
from uuid import uuid4

from ..domain import (
    Claim,
    ClaimStatus,
    MemoryItem,
    MemoryScope,
    canonical_json,
    utc_now,
)
from ..serialization import to_jsonable

_IDENTIFIER = re.compile(r"[a-z][a-z0-9_.:-]{0,127}\Z")

_VERSION = re.compile(r"[0-9A-Za-z][0-9A-Za-z._-]{0,63}\Z")


class OntologyAssertionStatus(StrEnum):
    ACTIVE = "active"
    CONFLICT = "conflict"
    SUPERSEDED = "superseded"
    ARCHIVED = "archived"


class OntologyValidationError(ValueError):
    pass


def _identifier(value: object, name: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise OntologyValidationError(
            f"{name} must be a lowercase ontology identifier"
        )
    return value


def _non_empty(value: object, name: str, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise OntologyValidationError(f"{name} must contain 1 to {maximum} characters")
    return value.strip()


def _evidence(values: Sequence[str]) -> tuple[str, ...]:
    result = tuple(dict.fromkeys(values))
    if not result or len(result) > 128 or any(
        not isinstance(value, str) or not value.strip() for value in result
    ):
        raise OntologyValidationError(
            "ontology knowledge requires 1 to 128 source event IDs"
        )
    return result


@dataclass(frozen=True, slots=True)
class OntologyClass:
    class_id: str
    label: str
    parent_ids: tuple[str, ...] = ()
    description: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "class_id", _identifier(self.class_id, "class_id"))
        object.__setattr__(self, "label", _non_empty(self.label, "class label", 256))
        parents = tuple(_identifier(value, "parent_id") for value in self.parent_ids)
        if len(set(parents)) != len(parents) or self.class_id in parents:
            raise OntologyValidationError("class parents must be unique and non-recursive")
        object.__setattr__(self, "parent_ids", parents)
        if len(self.description) > 2048:
            raise OntologyValidationError("class description exceeds 2048 characters")


@dataclass(frozen=True, slots=True)
class OntologyProperty:
    property_id: str
    label: str
    domain_class: str
    range_class: str | None = None
    description: str = ""
    functional: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "property_id", _identifier(self.property_id, "property_id"))
        object.__setattr__(self, "label", _non_empty(self.label, "property label", 256))
        object.__setattr__(self, "domain_class", _identifier(self.domain_class, "domain_class"))
        if self.range_class is not None:
            object.__setattr__(
                self, "range_class", _identifier(self.range_class, "range_class")
            )
        if len(self.description) > 2048:
            raise OntologyValidationError("property description exceeds 2048 characters")

    @property
    def literal_range(self) -> bool:
        return self.range_class is None


@dataclass(frozen=True, slots=True)
class OntologySchema:
    ontology_id: str
    version: str
    classes: tuple[OntologyClass, ...]
    properties: tuple[OntologyProperty, ...]
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ontology_id", _identifier(self.ontology_id, "ontology_id"))
        if not isinstance(self.version, str) or not _VERSION.fullmatch(self.version):
            raise OntologyValidationError("ontology version is invalid")
        classes = tuple(self.classes)
        properties = tuple(self.properties)
        if not classes or not properties:
            raise OntologyValidationError("ontology requires classes and properties")
        class_ids = {item.class_id for item in classes}
        property_ids = {item.property_id for item in properties}
        if len(class_ids) != len(classes) or len(property_ids) != len(properties):
            raise OntologyValidationError("ontology class and property IDs must be unique")
        for item in classes:
            if any(parent not in class_ids for parent in item.parent_ids):
                raise OntologyValidationError("ontology class references an unknown parent")
        for item in properties:
            if item.domain_class not in class_ids or (
                item.range_class is not None and item.range_class not in class_ids
            ):
                raise OntologyValidationError("ontology property references an unknown class")
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(class_id: str) -> None:
            if class_id in visiting:
                raise OntologyValidationError("ontology class hierarchy contains a cycle")
            if class_id in visited:
                return
            visiting.add(class_id)
            for parent_id in next(
                item.parent_ids for item in classes if item.class_id == class_id
            ):
                visit(parent_id)
            visiting.remove(class_id)
            visited.add(class_id)

        for class_id in class_ids:
            visit(class_id)
        if self.created_at.tzinfo is None:
            raise OntologyValidationError("ontology created_at must be timezone-aware")
        object.__setattr__(self, "classes", classes)
        object.__setattr__(self, "properties", properties)

    def class_by_id(self, class_id: str) -> OntologyClass:
        for item in self.classes:
            if item.class_id == class_id:
                return item
        raise OntologyValidationError(f"unknown ontology class: {class_id}")

    def property_by_id(self, property_id: str) -> OntologyProperty:
        for item in self.properties:
            if item.property_id == property_id:
                return item
        raise OntologyValidationError(f"unknown ontology property: {property_id}")

    def is_a(self, child_id: str, parent_id: str) -> bool:
        self.class_by_id(child_id)
        self.class_by_id(parent_id)
        pending = [child_id]
        visited: set[str] = set()
        while pending:
            current = pending.pop()
            if current == parent_id:
                return True
            if current in visited:
                continue
            visited.add(current)
            pending.extend(self.class_by_id(current).parent_ids)
        return False


@dataclass(frozen=True, slots=True)
class OntologyEntity:
    scope: MemoryScope
    entity_id: str
    class_id: str
    label: str
    source_event_ids: tuple[str, ...]
    aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "entity_id", _identifier(self.entity_id, "entity_id"))
        object.__setattr__(self, "class_id", _identifier(self.class_id, "class_id"))
        object.__setattr__(self, "label", _non_empty(self.label, "entity label", 256))
        aliases = tuple(dict.fromkeys(_non_empty(value, "entity alias", 256) for value in self.aliases))
        if len(aliases) > 32:
            raise OntologyValidationError("entity aliases exceed 32 values")
        object.__setattr__(self, "aliases", aliases)
        object.__setattr__(self, "source_event_ids", _evidence(self.source_event_ids))


@dataclass(frozen=True, slots=True)
class OntologyAssertion:
    assertion_id: str
    scope: MemoryScope
    ontology_id: str
    ontology_version: str
    subject_entity_id: str
    predicate_id: str
    source_event_ids: tuple[str, ...]
    text: str
    confidence: float
    valid_from: datetime
    object_entity_id: str | None = None
    literal_value: Any | None = None
    valid_to: datetime | None = None
    status: OntologyAssertionStatus = OntologyAssertionStatus.ACTIVE
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.assertion_id.strip():
            raise OntologyValidationError("assertion_id must not be empty")
        object.__setattr__(self, "ontology_id", _identifier(self.ontology_id, "ontology_id"))
        if not _VERSION.fullmatch(self.ontology_version):
            raise OntologyValidationError("ontology_version is invalid")
        object.__setattr__(
            self, "subject_entity_id", _identifier(self.subject_entity_id, "subject_entity_id")
        )
        object.__setattr__(self, "predicate_id", _identifier(self.predicate_id, "predicate_id"))
        if (self.object_entity_id is None) == (self.literal_value is None):
            raise OntologyValidationError(
                "assertion requires exactly one entity object or non-null literal"
            )
        if self.object_entity_id is not None:
            object.__setattr__(
                self, "object_entity_id", _identifier(self.object_entity_id, "object_entity_id")
            )
        elif len(canonical_json(self.literal_value)) > 4096:
            raise OntologyValidationError("assertion literal exceeds 4096 characters")
        object.__setattr__(self, "source_event_ids", _evidence(self.source_event_ids))
        object.__setattr__(self, "text", _non_empty(self.text, "assertion text", 4096))
        if isinstance(self.confidence, bool) or not isinstance(self.confidence, (int, float)):
            raise OntologyValidationError("assertion confidence must be numeric")
        if not math.isfinite(float(self.confidence)) or not 0 <= self.confidence <= 1:
            raise OntologyValidationError("assertion confidence must be between 0 and 1")
        if self.valid_from.tzinfo is None or (
            self.valid_to is not None and self.valid_to.tzinfo is None
        ):
            raise OntologyValidationError("assertion validity must be timezone-aware")
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise OntologyValidationError("assertion valid_to precedes valid_from")
        if self.created_at.tzinfo is None:
            raise OntologyValidationError("assertion created_at must be timezone-aware")
        object.__setattr__(self, "status", OntologyAssertionStatus(self.status))


@dataclass(frozen=True, slots=True)
class OntologyProjection:
    schema: OntologySchema
    entities: tuple[OntologyEntity, ...]
    assertion: OntologyAssertion


@dataclass(frozen=True, slots=True)
class OntologyConflictResolution:
    scope: MemoryScope
    ontology_id: str
    ontology_version: str
    subject_entity_id: str
    predicate_id: str
    winner_assertion_id: str
    conflict_assertion_ids: tuple[str, ...]
    reason: str
    approved_by: str
    resolution_id: str = field(default_factory=lambda: str(uuid4()))
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ontology_id", _identifier(self.ontology_id, "ontology_id"))
        if not _VERSION.fullmatch(self.ontology_version):
            raise OntologyValidationError("ontology_version is invalid")
        object.__setattr__(
            self, "subject_entity_id", _identifier(self.subject_entity_id, "subject_entity_id")
        )
        object.__setattr__(self, "predicate_id", _identifier(self.predicate_id, "predicate_id"))
        conflict_ids = tuple(dict.fromkeys(self.conflict_assertion_ids))
        if (
            len(conflict_ids) < 2
            or self.winner_assertion_id not in conflict_ids
            or any(not isinstance(value, str) or not value.strip() for value in conflict_ids)
        ):
            raise OntologyValidationError(
                "resolution requires a winner inside at least two conflict assertions"
            )
        object.__setattr__(self, "conflict_assertion_ids", conflict_ids)
        object.__setattr__(self, "reason", _non_empty(self.reason, "resolution reason", 2048))
        object.__setattr__(
            self, "approved_by", _non_empty(self.approved_by, "resolution approver", 256)
        )
        if not self.resolution_id.strip() or self.created_at.tzinfo is None:
            raise OntologyValidationError("resolution identity and timestamp are required")


@dataclass(frozen=True, slots=True)
class OntologyMatch:
    item: MemoryItem
    source_event_ids: tuple[str, ...]
    score: float


class OntologyStore(Protocol):
    async def initialize(self) -> None: ...

    async def index_identity(self) -> str: ...

    async def register_schema(self, schema: OntologySchema) -> None: ...

    async def get_assertions(self, scope, assertion_ids, *, ontology_id, ontology_version, at_time): ...

    async def neighbors(self, scope, entity_ids, *, ontology_id, ontology_version, at_time,
                        predicates=(), direction="outgoing", limit=100): ...

    async def upsert_projection(self, projection: OntologyProjection) -> None: ...

    async def invalidate_sources(
        self,
        scope: MemoryScope,
        source_event_ids: Sequence[str],
        *,
        max_rows: int,
    ) -> int: ...

    async def search(
        self,
        text: str,
        scope: MemoryScope,
        *,
        ontology_id: str,
        ontology_version: str,
        at_time: datetime,
        limit: int,
        max_scan: int,
    ) -> tuple[OntologyMatch, ...]: ...

    async def list_conflicts(
        self,
        scope: MemoryScope,
        *,
        ontology_id: str,
        ontology_version: str,
        limit: int,
    ) -> tuple[OntologyAssertion, ...]: ...

    async def resolve_conflict(
        self, resolution: OntologyConflictResolution
    ) -> OntologyAssertion: ...


def project_claim_to_ontology(
    claim: Claim,
    schema: OntologySchema,
) -> OntologyProjection | None:
    """Project an active structured Claim into a validated ontology assertion."""

    if claim.status is not ClaimStatus.ACTIVE:
        return None
    if not isinstance(claim.value, Mapping) or "$ontology" not in claim.value:
        return None
    payload = claim.value["$ontology"]
    if not isinstance(payload, Mapping):
        raise OntologyValidationError("$ontology must be an object")
    subject_raw = payload.get("subject")
    object_raw = payload.get("object")
    predicate_id = _identifier(payload.get("predicate"), "predicate")
    if not isinstance(subject_raw, Mapping) or not isinstance(object_raw, Mapping):
        raise OntologyValidationError("ontology subject and object must be objects")
    sources = _evidence(claim.provenance.source_event_ids)
    subject = _entity_from_mapping(claim.scope, subject_raw, sources, "subject")
    property_definition = schema.property_by_id(predicate_id)
    if not schema.is_a(subject.class_id, property_definition.domain_class):
        raise OntologyValidationError("subject class is outside the property domain")

    entities = [subject]
    object_entity: OntologyEntity | None = None
    literal: Any | None = None
    if "literal" in object_raw:
        if not property_definition.literal_range:
            raise OntologyValidationError("property requires an entity object")
        literal = object_raw["literal"]
        if literal is None:
            raise OntologyValidationError("ontology literal must not be null")
    else:
        if property_definition.range_class is None:
            raise OntologyValidationError("property requires a literal object")
        object_entity = _entity_from_mapping(claim.scope, object_raw, sources, "object")
        if not schema.is_a(object_entity.class_id, property_definition.range_class):
            raise OntologyValidationError("object class is outside the property range")
        entities.append(object_entity)

    assertion_key = canonical_json(
        {
            "partition": claim.scope.partition_key(),
            "ontology": schema.ontology_id,
            "version": schema.version,
            "subject": subject.entity_id,
            "predicate": predicate_id,
            "object": object_entity.entity_id if object_entity else literal,
            "valid_from": claim.valid_from.isoformat(),
        }
    )
    assertion = OntologyAssertion(
        assertion_id=f"ontology-{sha256(assertion_key.encode('utf-8')).hexdigest()[:32]}",
        scope=claim.scope,
        ontology_id=schema.ontology_id,
        ontology_version=schema.version,
        subject_entity_id=subject.entity_id,
        predicate_id=predicate_id,
        object_entity_id=object_entity.entity_id if object_entity else None,
        literal_value=literal,
        source_event_ids=sources,
        text=claim.text,
        confidence=claim.confidence,
        valid_from=claim.valid_from,
        valid_to=claim.valid_to,
        created_at=claim.created_at,
    )
    return OntologyProjection(schema, tuple(entities), assertion)


def _schema_payload(schema: OntologySchema) -> str:
    """Canonical semantic identity; registration time is intentionally excluded."""

    return canonical_json(
        {
            "ontology_id": schema.ontology_id,
            "version": schema.version,
            "classes": to_jsonable(schema.classes),
            "properties": to_jsonable(schema.properties),
        }
    )


def _validate_projection(projection: OntologyProjection) -> None:
    schema = projection.schema
    assertion = projection.assertion
    if (
        assertion.ontology_id != schema.ontology_id
        or assertion.ontology_version != schema.version
    ):
        raise OntologyValidationError("projection schema identity does not match assertion")
    if assertion.status is not OntologyAssertionStatus.ACTIVE:
        raise OntologyValidationError("new projections must contain active assertions")
    entities = {entity.entity_id: entity for entity in projection.entities}
    if len(entities) != len(projection.entities):
        raise OntologyValidationError("projection contains duplicate entity IDs")
    if assertion.subject_entity_id not in entities:
        raise OntologyValidationError("projection does not contain its subject entity")
    if assertion.object_entity_id is not None and assertion.object_entity_id not in entities:
        raise OntologyValidationError("projection does not contain its object entity")
    if assertion.scope != entities[assertion.subject_entity_id].scope:
        raise OntologyValidationError("projection subject scope does not match assertion")
    if any(entity.scope != assertion.scope for entity in entities.values()):
        raise OntologyValidationError("projection entities span multiple scopes")
    property_definition = schema.property_by_id(assertion.predicate_id)
    subject = entities[assertion.subject_entity_id]
    if not schema.is_a(subject.class_id, property_definition.domain_class):
        raise OntologyValidationError("projection subject violates property domain")
    if assertion.object_entity_id is None:
        if not property_definition.literal_range:
            raise OntologyValidationError("projection property requires an entity object")
    else:
        if property_definition.range_class is None:
            raise OntologyValidationError("projection property requires a literal object")
        object_entity = entities[assertion.object_entity_id]
        if not schema.is_a(object_entity.class_id, property_definition.range_class):
            raise OntologyValidationError("projection object violates property range")
    assertion_sources = set(assertion.source_event_ids)
    if any(
        not assertion_sources.issubset(entity.source_event_ids)
        for entity in entities.values()
    ):
        raise OntologyValidationError("projection entity evidence does not cover assertion")


class OntologyEvidenceVerifier(Protocol):
    async def verify(
        self, scope: MemoryScope, source_event_ids: Sequence[str]
    ) -> bool: ...


class CallableOntologyEvidenceVerifier:
    """Adapter for a host's authoritative scoped event-existence check."""

    def __init__(
        self,
        callback: Callable[[MemoryScope, Sequence[str]], Awaitable[bool]],
    ) -> None:
        if not callable(callback):
            raise TypeError("evidence verifier callback must be callable")
        self._callback = callback

    async def verify(
        self, scope: MemoryScope, source_event_ids: Sequence[str]
    ) -> bool:
        result = await self._callback(scope, tuple(source_event_ids))
        if type(result) is not bool:
            raise OntologyValidationError("evidence verifier must return a boolean")
        return result


def _entity_from_mapping(
    scope: MemoryScope,
    value: Mapping[str, Any],
    source_event_ids: tuple[str, ...],
    prefix: str,
) -> OntologyEntity:
    aliases_raw = value.get("aliases", ())
    if not isinstance(aliases_raw, Sequence) or isinstance(aliases_raw, (str, bytes)):
        raise OntologyValidationError(f"{prefix} aliases must be an array")
    return OntologyEntity(
        scope=scope,
        entity_id=_identifier(value.get("id"), f"{prefix}.id"),
        class_id=_identifier(value.get("class"), f"{prefix}.class"),
        label=_non_empty(value.get("label", value.get("id")), f"{prefix}.label", 256),
        aliases=tuple(aliases_raw),
        source_event_ids=source_event_ids,
    )
