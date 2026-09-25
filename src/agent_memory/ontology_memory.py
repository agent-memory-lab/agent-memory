"""Versioned, evidence-backed ontology memory as optional plugins."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
import json
import math
from pathlib import Path
import re
import sqlite3
from types import MappingProxyType
from typing import Any, Protocol
from uuid import uuid4

from .domain import (
    Claim,
    ClaimStatus,
    MemoryChannel,
    MemoryItem,
    MemoryKind,
    MemoryQuery,
    MemoryScope,
    ScopeLevel,
    canonical_json,
    utc_now,
)
from .plugin_protocol import (
    ConsolidationRequest,
    ConsolidationResult,
    PluginContext,
    PluginHealth,
    PluginHealthStatus,
    RetrievalCandidate,
)
from .plugins import (
    PluginError,
    PluginErrorCode,
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)
from .serialization import to_jsonable


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


from .ontology_queries import SQLOntologyQueries


class SQLiteOntologyStore(SQLOntologyQueries):
    """Optional local ontology index with no dependency beyond Python stdlib."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    async def index_identity(self) -> str:
        def read():
            connection = self._connect()
            try:
                return connection.execute("SELECT index_id FROM ontology_index_identity WHERE singleton=1").fetchone()["index_id"]
            finally:
                connection.close()
        return await asyncio.to_thread(read)

    async def register_schema(self, schema: OntologySchema) -> None:
        await asyncio.to_thread(self._register_schema_sync, schema)

    async def upsert_projection(self, projection: OntologyProjection) -> None:
        await asyncio.to_thread(self._upsert_projection_sync, projection)

    async def invalidate_sources(
        self,
        scope: MemoryScope,
        source_event_ids: Sequence[str],
        *,
        max_rows: int = 10_000,
    ) -> int:
        source_ids = tuple(dict.fromkeys(source_event_ids))
        if any(not isinstance(value, str) or not value.strip() for value in source_ids):
            raise OntologyValidationError("deleted source IDs must not be empty")
        if not source_ids:
            return 0
        if type(max_rows) is not int or not 1 <= max_rows <= 100_000:
            raise OntologyValidationError("max_rows must be between 1 and 100000")
        return await asyncio.to_thread(
            self._invalidate_sources_sync,
            scope.partition_key(),
            set(source_ids),
            max_rows,
        )

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
    ) -> tuple[OntologyMatch, ...]:
        if not text.strip():
            return ()
        if not 1 <= limit <= 100 or not 1 <= max_scan <= 10_000:
            raise OntologyValidationError("ontology search limits are invalid")
        return await asyncio.to_thread(
            self._search_sync,
            text,
            scope,
            ontology_id,
            ontology_version,
            at_time,
            limit,
            max_scan,
        )

    async def list_conflicts(
        self,
        scope: MemoryScope,
        *,
        ontology_id: str,
        ontology_version: str,
        limit: int = 100,
    ) -> tuple[OntologyAssertion, ...]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise OntologyValidationError("conflict limit must be between 1 and 1000")
        return await asyncio.to_thread(
            self._list_conflicts_sync,
            scope,
            ontology_id,
            ontology_version,
            limit,
        )

    async def resolve_conflict(
        self, resolution: OntologyConflictResolution
    ) -> OntologyAssertion:
        return await asyncio.to_thread(self._resolve_conflict_sync, resolution)

    def _connect(self) -> sqlite3.Connection:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _initialize_sync(self) -> None:
        connection = self._connect()
        try:
            with connection:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS ontology_index_identity (
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        index_id TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS ontology_schemas (
                        ontology_id TEXT NOT NULL,
                        version TEXT NOT NULL,
                        schema_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (ontology_id, version)
                    );
                    CREATE TABLE IF NOT EXISTS ontology_entities (
                        partition_key TEXT NOT NULL,
                        ontology_id TEXT NOT NULL,
                        ontology_version TEXT NOT NULL,
                        entity_id TEXT NOT NULL,
                        class_id TEXT NOT NULL,
                        label TEXT NOT NULL,
                        aliases_json TEXT NOT NULL,
                        source_event_ids_json TEXT NOT NULL,
                        archived_at TEXT,
                        PRIMARY KEY (
                            partition_key, ontology_id, ontology_version, entity_id
                        )
                    );
                    CREATE TABLE IF NOT EXISTS ontology_assertions (
                        assertion_id TEXT PRIMARY KEY,
                        partition_key TEXT NOT NULL,
                        ontology_id TEXT NOT NULL,
                        ontology_version TEXT NOT NULL,
                        subject_entity_id TEXT NOT NULL,
                        predicate_id TEXT NOT NULL,
                        object_entity_id TEXT,
                        literal_json TEXT,
                        text TEXT NOT NULL,
                        confidence REAL NOT NULL,
                        source_event_ids_json TEXT NOT NULL,
                        valid_from TEXT NOT NULL,
                        valid_to TEXT,
                        created_at TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'active',
                        superseded_by TEXT,
                        archived_at TEXT
                    );
                    CREATE TABLE IF NOT EXISTS ontology_entity_sources (
                        partition_key TEXT NOT NULL,
                        ontology_id TEXT NOT NULL,
                        ontology_version TEXT NOT NULL,
                        entity_id TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        PRIMARY KEY (
                            partition_key, ontology_id, ontology_version,
                            entity_id, event_id
                        )
                    );
                    CREATE INDEX IF NOT EXISTS ontology_entity_sources_event_idx
                    ON ontology_entity_sources(partition_key, event_id);
                    CREATE TABLE IF NOT EXISTS ontology_assertion_sources (
                        partition_key TEXT NOT NULL,
                        assertion_id TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        PRIMARY KEY (partition_key, assertion_id, event_id)
                    );
                    CREATE INDEX IF NOT EXISTS ontology_assertion_sources_event_idx
                    ON ontology_assertion_sources(partition_key, event_id);
                    CREATE INDEX IF NOT EXISTS ontology_assertions_scope_idx
                    ON ontology_assertions(
                        partition_key, ontology_id, ontology_version, archived_at
                    );
                    CREATE INDEX IF NOT EXISTS ontology_assertions_relation_idx
                    ON ontology_assertions(
                        partition_key, subject_entity_id, predicate_id, object_entity_id
                    );
                    CREATE TABLE IF NOT EXISTS ontology_conflict_resolutions (
                        resolution_id TEXT PRIMARY KEY,
                        partition_key TEXT NOT NULL,
                        ontology_id TEXT NOT NULL,
                        ontology_version TEXT NOT NULL,
                        subject_entity_id TEXT NOT NULL,
                        predicate_id TEXT NOT NULL,
                        winner_assertion_id TEXT NOT NULL,
                        conflict_assertion_ids_json TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        approved_by TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    """
                )
                connection.execute(
                    "INSERT OR IGNORE INTO ontology_index_identity VALUES (1, ?)", (str(uuid4()),)
                )
                assertion_columns = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(ontology_assertions)"
                    ).fetchall()
                }
                if "status" not in assertion_columns:
                    connection.execute(
                        "ALTER TABLE ontology_assertions "
                        "ADD COLUMN status TEXT NOT NULL DEFAULT 'active'"
                    )
                if "superseded_by" not in assertion_columns:
                    connection.execute(
                        "ALTER TABLE ontology_assertions ADD COLUMN superseded_by TEXT"
                    )
        finally:
            connection.close()

    def _register_schema_sync(self, schema: OntologySchema) -> None:
        self._initialize_sync()
        payload = _schema_payload(schema)
        connection = self._connect()
        try:
            with connection:
                existing = connection.execute(
                    "SELECT schema_json FROM ontology_schemas "
                    "WHERE ontology_id=? AND version=?",
                    (schema.ontology_id, schema.version),
                ).fetchone()
                if existing is not None and existing["schema_json"] != payload:
                    raise OntologyValidationError(
                        "an ontology version cannot be redefined"
                    )
                connection.execute(
                    "INSERT OR IGNORE INTO ontology_schemas "
                    "(ontology_id, version, schema_json, created_at) VALUES (?, ?, ?, ?)",
                    (schema.ontology_id, schema.version, payload, schema.created_at.isoformat()),
                )
        finally:
            connection.close()

    def _upsert_projection_sync(self, projection: OntologyProjection) -> None:
        _validate_projection(projection)
        connection = self._connect()
        try:
            with connection:
                schema_row = connection.execute(
                    "SELECT 1 FROM ontology_schemas WHERE ontology_id=? AND version=?",
                    (projection.schema.ontology_id, projection.schema.version),
                ).fetchone()
                if schema_row is None:
                    raise OntologyValidationError("ontology schema is not registered")
                for entity in projection.entities:
                    self._upsert_entity(connection, projection.schema, entity)
                assertion = projection.assertion
                property_definition = projection.schema.property_by_id(
                    assertion.predicate_id
                )
                existing = connection.execute(
                    "SELECT source_event_ids_json, status, superseded_by "
                    "FROM ontology_assertions "
                    "WHERE assertion_id=?",
                    (assertion.assertion_id,),
                ).fetchone()
                sources = assertion.source_event_ids
                assertion_status = (
                    OntologyAssertionStatus(existing["status"])
                    if existing is not None
                    else OntologyAssertionStatus.ACTIVE
                )
                if existing is not None:
                    sources = tuple(
                        dict.fromkeys(
                            (*json.loads(existing["source_event_ids_json"]), *sources)
                        )
                    )
                if (
                    property_definition.functional
                    and assertion_status is not OntologyAssertionStatus.SUPERSEDED
                ):
                    relation_rows = connection.execute(
                        """
                        SELECT assertion_id, object_entity_id, literal_json,
                               valid_from, valid_to
                        FROM ontology_assertions
                        WHERE partition_key=? AND ontology_id=? AND ontology_version=?
                          AND subject_entity_id=? AND predicate_id=?
                          AND status IN ('active', 'conflict') AND archived_at IS NULL
                          AND assertion_id<>?
                        """,
                        (
                            assertion.scope.partition_key(),
                            assertion.ontology_id,
                            assertion.ontology_version,
                            assertion.subject_entity_id,
                            assertion.predicate_id,
                            assertion.assertion_id,
                        ),
                    ).fetchall()
                    new_object = (
                        ("entity", assertion.object_entity_id)
                        if assertion.object_entity_id is not None
                        else ("literal", canonical_json(assertion.literal_value))
                    )
                    conflicting_ids = [
                        row["assertion_id"]
                        for row in relation_rows
                        if _stored_object(row) != new_object
                        and _intervals_overlap(
                            assertion.valid_from,
                            assertion.valid_to,
                            datetime.fromisoformat(row["valid_from"]),
                            (
                                datetime.fromisoformat(row["valid_to"])
                                if row["valid_to"]
                                else None
                            ),
                        )
                    ]
                    if conflicting_ids:
                        assertion_status = OntologyAssertionStatus.CONFLICT
                        placeholders = ",".join("?" for _ in conflicting_ids)
                        connection.execute(
                            f"UPDATE ontology_assertions SET status='conflict', "
                            f"superseded_by=NULL WHERE assertion_id IN ({placeholders})",
                            tuple(conflicting_ids),
                        )
                values = (
                    assertion.assertion_id,
                    assertion.scope.partition_key(),
                    assertion.ontology_id,
                    assertion.ontology_version,
                    assertion.subject_entity_id,
                    assertion.predicate_id,
                    assertion.object_entity_id,
                    (
                        canonical_json(assertion.literal_value)
                        if assertion.literal_value is not None
                        else None
                    ),
                    assertion.text,
                    float(assertion.confidence),
                    canonical_json(sources),
                    assertion.valid_from.isoformat(),
                    assertion.valid_to.isoformat() if assertion.valid_to else None,
                    assertion.created_at.isoformat(),
                    assertion_status.value,
                )
                connection.execute(
                    """
                    INSERT INTO ontology_assertions (
                        assertion_id, partition_key, ontology_id, ontology_version,
                        subject_entity_id, predicate_id, object_entity_id, literal_json,
                        text, confidence, source_event_ids_json, valid_from, valid_to,
                        created_at, status, superseded_by, archived_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                    ON CONFLICT(assertion_id) DO UPDATE SET
                        source_event_ids_json=excluded.source_event_ids_json,
                        confidence=MAX(ontology_assertions.confidence, excluded.confidence),
                        archived_at=NULL
                    """,
                    values,
                )
                for source_id in sources:
                    connection.execute(
                        "INSERT OR IGNORE INTO ontology_assertion_sources "
                        "(partition_key, assertion_id, event_id) VALUES (?, ?, ?)",
                        (
                            assertion.scope.partition_key(),
                            assertion.assertion_id,
                            source_id,
                        ),
                    )
        finally:
            connection.close()

    @staticmethod
    def _upsert_entity(
        connection: sqlite3.Connection,
        schema: OntologySchema,
        entity: OntologyEntity,
    ) -> None:
        existing = connection.execute(
            """
            SELECT class_id, label, aliases_json, source_event_ids_json
            FROM ontology_entities
            WHERE partition_key=? AND ontology_id=? AND ontology_version=? AND entity_id=?
            """,
            (
                entity.scope.partition_key(),
                schema.ontology_id,
                schema.version,
                entity.entity_id,
            ),
        ).fetchone()
        aliases = entity.aliases
        sources = entity.source_event_ids
        if existing is not None:
            if existing["class_id"] != entity.class_id or existing["label"] != entity.label:
                raise OntologyValidationError(
                    "entity identity conflicts within one ontology version"
                )
            aliases = tuple(dict.fromkeys((*json.loads(existing["aliases_json"]), *aliases)))
            sources = tuple(
                dict.fromkeys((*json.loads(existing["source_event_ids_json"]), *sources))
            )
        connection.execute(
            """
            INSERT INTO ontology_entities (
                partition_key, ontology_id, ontology_version, entity_id, class_id,
                label, aliases_json, source_event_ids_json, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(partition_key, ontology_id, ontology_version, entity_id)
            DO UPDATE SET aliases_json=excluded.aliases_json,
                          source_event_ids_json=excluded.source_event_ids_json,
                          archived_at=NULL
            """,
            (
                entity.scope.partition_key(),
                schema.ontology_id,
                schema.version,
                entity.entity_id,
                entity.class_id,
                entity.label,
                canonical_json(aliases),
                canonical_json(sources),
            ),
        )
        for source_id in sources:
            connection.execute(
                """
                INSERT OR IGNORE INTO ontology_entity_sources (
                    partition_key, ontology_id, ontology_version, entity_id, event_id
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    entity.scope.partition_key(),
                    schema.ontology_id,
                    schema.version,
                    entity.entity_id,
                    source_id,
                ),
            )

    def _invalidate_sources_sync(
        self,
        partition: str,
        deleted: set[str],
        max_rows: int,
    ) -> int:
        connection = self._connect()
        try:
            with connection:
                now = utc_now().isoformat()
                placeholders = ",".join("?" for _ in deleted)
                params = (partition, *sorted(deleted), max_rows + 1)
                assertion_rows = connection.execute(
                    f"""
                    SELECT DISTINCT assertion_id
                    FROM ontology_assertion_sources
                    WHERE partition_key=? AND event_id IN ({placeholders})
                    ORDER BY assertion_id LIMIT ?
                    """,
                    params,
                ).fetchall()
                entity_rows = connection.execute(
                    f"""
                    SELECT DISTINCT ontology_id, ontology_version, entity_id
                    FROM ontology_entity_sources
                    WHERE partition_key=? AND event_id IN ({placeholders})
                    ORDER BY ontology_id, ontology_version, entity_id LIMIT ?
                    """,
                    params,
                ).fetchall()
                if len(assertion_rows) > max_rows or len(entity_rows) > max_rows:
                    raise OntologyValidationError(
                        "ontology source invalidation exceeds max_rows"
                    )
                connection.execute(
                    f"DELETE FROM ontology_assertion_sources "
                    f"WHERE partition_key=? AND event_id IN ({placeholders})",
                    (partition, *sorted(deleted)),
                )
                connection.execute(
                    f"DELETE FROM ontology_entity_sources "
                    f"WHERE partition_key=? AND event_id IN ({placeholders})",
                    (partition, *sorted(deleted)),
                )
                for row in assertion_rows:
                    remaining = tuple(
                        value["event_id"]
                        for value in connection.execute(
                            "SELECT event_id FROM ontology_assertion_sources "
                            "WHERE partition_key=? AND assertion_id=? ORDER BY event_id",
                            (partition, row["assertion_id"]),
                        ).fetchall()
                    )
                    connection.execute(
                        "UPDATE ontology_assertions SET source_event_ids_json=?, "
                        "archived_at=? WHERE partition_key=? AND assertion_id=?",
                        (
                            canonical_json(remaining),
                            None if remaining else now,
                            partition,
                            row["assertion_id"],
                        ),
                    )
                for row in entity_rows:
                    remaining = tuple(
                        value["event_id"]
                        for value in connection.execute(
                            """
                            SELECT event_id FROM ontology_entity_sources
                            WHERE partition_key=? AND ontology_id=?
                              AND ontology_version=? AND entity_id=?
                            ORDER BY event_id
                            """,
                            (
                                partition,
                                row["ontology_id"],
                                row["ontology_version"],
                                row["entity_id"],
                            ),
                        ).fetchall()
                    )
                    connection.execute(
                        """
                        UPDATE ontology_entities
                        SET source_event_ids_json=?, archived_at=?
                        WHERE partition_key=? AND ontology_id=?
                          AND ontology_version=? AND entity_id=?
                        """,
                        (
                            canonical_json(remaining),
                            None if remaining else now,
                            partition,
                            row["ontology_id"],
                            row["ontology_version"],
                            row["entity_id"],
                        ),
                    )
            return len(assertion_rows) + len(entity_rows)
        finally:
            connection.close()

    def _search_sync(
        self,
        text: str,
        scope: MemoryScope,
        ontology_id: str,
        ontology_version: str,
        at_time: datetime,
        limit: int,
        max_scan: int,
    ) -> tuple[OntologyMatch, ...]:
        connection = self._connect()
        try:
            visible_scopes = _visible_scope_partitions(scope)
            partitions = tuple(value for _, value in visible_scopes)
            placeholders = ",".join("?" for _ in partitions)
            rows = connection.execute(
                f"""
                SELECT a.*, subject.label AS subject_label,
                       object.label AS object_label
                FROM ontology_assertions a
                JOIN ontology_entities subject
                  ON subject.partition_key=a.partition_key
                 AND subject.ontology_id=a.ontology_id
                 AND subject.ontology_version=a.ontology_version
                 AND subject.entity_id=a.subject_entity_id
                 AND subject.archived_at IS NULL
                LEFT JOIN ontology_entities object
                  ON object.partition_key=a.partition_key
                 AND object.ontology_id=a.ontology_id
                 AND object.ontology_version=a.ontology_version
                 AND object.entity_id=a.object_entity_id
                 AND object.archived_at IS NULL
                WHERE a.partition_key IN ({placeholders}) AND a.ontology_id=?
                  AND a.ontology_version=? AND a.status='active'
                  AND a.archived_at IS NULL
                  AND a.valid_from<=?
                  AND (a.valid_to IS NULL OR a.valid_to>=?)
                ORDER BY a.valid_from DESC, a.assertion_id
                LIMIT ?
                """,
                (
                    *partitions,
                    ontology_id,
                    ontology_version,
                    at_time.isoformat(),
                    at_time.isoformat(),
                    max_scan,
                ),
            ).fetchall()
            tokens = tuple(dict.fromkeys(value.casefold() for value in text.split() if value))[:32]
            matches: list[OntologyMatch] = []
            for row in rows:
                literal = json.loads(row["literal_json"]) if row["literal_json"] else None
                haystack = " ".join(
                    str(value)
                    for value in (
                        row["subject_entity_id"],
                        row["subject_label"],
                        row["predicate_id"],
                        row["object_entity_id"],
                        row["object_label"],
                        literal,
                        row["text"],
                    )
                    if value is not None
                ).casefold()
                hits = sum(token in haystack for token in tokens)
                if not hits:
                    continue
                score = 0.75 * hits / max(1, len(tokens)) + 0.25 * float(row["confidence"])
                scope_level = next(
                    level for level, key in visible_scopes if key == row["partition_key"]
                )
                specificity = next(
                    index
                    for index, (_, key) in enumerate(visible_scopes)
                    if key == row["partition_key"]
                )
                score = min(
                    1.0,
                    score + 0.05 * specificity / max(1, len(visible_scopes) - 1),
                )
                sources = tuple(json.loads(row["source_event_ids_json"]))
                item = MemoryItem(
                    id=row["assertion_id"],
                    kind=MemoryKind.CLAIM,
                    text=row["text"],
                    score=score,
                    occurred_at=datetime.fromisoformat(row["valid_from"]),
                    metadata={
                        "ontology_id": row["ontology_id"],
                        "ontology_version": row["ontology_version"],
                        "subject_entity_id": row["subject_entity_id"],
                        "predicate_id": row["predicate_id"],
                        "object_entity_id": row["object_entity_id"],
                        "literal_value": literal,
                        "valid_to": row["valid_to"],
                        "scope_level": scope_level.value,
                    },
                )
                matches.append(OntologyMatch(item, sources, score))
            matches.sort(key=lambda value: (-value.score, value.item.id))
            return tuple(matches[:limit])
        finally:
            connection.close()

    def _list_conflicts_sync(
        self,
        scope: MemoryScope,
        ontology_id: str,
        ontology_version: str,
        limit: int,
    ) -> tuple[OntologyAssertion, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM ontology_assertions
                WHERE partition_key=? AND ontology_id=? AND ontology_version=?
                  AND status='conflict' AND archived_at IS NULL
                ORDER BY subject_entity_id, predicate_id, valid_from, assertion_id
                LIMIT ?
                """,
                (scope.partition_key(), ontology_id, ontology_version, limit),
            ).fetchall()
            return tuple(_assertion_from_row(row, scope) for row in rows)
        finally:
            connection.close()

    def _resolve_conflict_sync(
        self, resolution: OntologyConflictResolution
    ) -> OntologyAssertion:
        connection = self._connect()
        try:
            with connection:
                rows = connection.execute(
                    """
                    SELECT * FROM ontology_assertions
                    WHERE partition_key=? AND ontology_id=? AND ontology_version=?
                      AND subject_entity_id=? AND predicate_id=?
                      AND status='conflict' AND archived_at IS NULL
                    ORDER BY assertion_id
                    """,
                    (
                        resolution.scope.partition_key(),
                        resolution.ontology_id,
                        resolution.ontology_version,
                        resolution.subject_entity_id,
                        resolution.predicate_id,
                    ),
                ).fetchall()
                stored_ids = {row["assertion_id"] for row in rows}
                if stored_ids != set(resolution.conflict_assertion_ids):
                    raise OntologyValidationError(
                        "resolution must include the complete current conflict set"
                    )
                winner = next(
                    row
                    for row in rows
                    if row["assertion_id"] == resolution.winner_assertion_id
                )
                losers = stored_ids - {resolution.winner_assertion_id}
                connection.execute(
                    "UPDATE ontology_assertions SET status='active', superseded_by=NULL "
                    "WHERE assertion_id=?",
                    (resolution.winner_assertion_id,),
                )
                placeholders = ",".join("?" for _ in losers)
                connection.execute(
                    f"UPDATE ontology_assertions SET status='superseded', "
                    f"superseded_by=? WHERE assertion_id IN ({placeholders})",
                    (resolution.winner_assertion_id, *sorted(losers)),
                )
                connection.execute(
                    """
                    INSERT INTO ontology_conflict_resolutions (
                        resolution_id, partition_key, ontology_id, ontology_version,
                        subject_entity_id, predicate_id, winner_assertion_id,
                        conflict_assertion_ids_json, reason, approved_by, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        resolution.resolution_id,
                        resolution.scope.partition_key(),
                        resolution.ontology_id,
                        resolution.ontology_version,
                        resolution.subject_entity_id,
                        resolution.predicate_id,
                        resolution.winner_assertion_id,
                        canonical_json(sorted(resolution.conflict_assertion_ids)),
                        resolution.reason,
                        resolution.approved_by,
                        resolution.created_at.isoformat(),
                    ),
                )
                return _assertion_from_row(
                    {**dict(winner), "status": "active"}, resolution.scope
                )
        finally:
            connection.close()


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


def _stored_object(row: Mapping[str, Any]) -> tuple[str, Any]:
    if row["object_entity_id"] is not None:
        return ("entity", row["object_entity_id"])
    return ("literal", row["literal_json"])


def _intervals_overlap(
    left_from: datetime,
    left_to: datetime | None,
    right_from: datetime,
    right_to: datetime | None,
) -> bool:
    return (left_to is None or right_from <= left_to) and (
        right_to is None or left_from <= right_to
    )


def _visible_scope_partitions(scope: MemoryScope) -> tuple[tuple[ScopeLevel, str], ...]:
    values: list[tuple[ScopeLevel, str]] = []
    seen: set[str] = set()
    for level in ScopeLevel:
        try:
            partition = scope.project(level).partition_key()
        except ValueError:
            continue
        if partition not in seen:
            seen.add(partition)
            values.append((level, partition))
    return tuple(values)


def _assertion_from_row(
    row: Mapping[str, Any], scope: MemoryScope
) -> OntologyAssertion:
    return OntologyAssertion(
        assertion_id=row["assertion_id"],
        scope=scope,
        ontology_id=row["ontology_id"],
        ontology_version=row["ontology_version"],
        subject_entity_id=row["subject_entity_id"],
        predicate_id=row["predicate_id"],
        object_entity_id=row["object_entity_id"],
        literal_value=json.loads(row["literal_json"]) if row["literal_json"] else None,
        source_event_ids=tuple(json.loads(row["source_event_ids_json"])),
        text=row["text"],
        confidence=float(row["confidence"]),
        valid_from=datetime.fromisoformat(row["valid_from"]),
        valid_to=datetime.fromisoformat(row["valid_to"]) if row["valid_to"] else None,
        status=OntologyAssertionStatus(row["status"]),
        created_at=datetime.fromisoformat(row["created_at"]),
    )


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


class _OntologyPlugin:
    def __init__(
        self,
        store: OntologyStore,
        schema: OntologySchema,
        manifest: PluginManifest,
    ) -> None:
        self._store = store
        self._schema = schema
        self._manifest = manifest
        self._context: PluginContext | None = None

    def plugin_manifest(self) -> PluginManifest:
        return self._manifest

    async def initialize(self, context: PluginContext) -> None:
        if self._context is not None:
            raise PluginError(
                "ontology plugin is already initialized",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        await self._store.initialize()
        await self._store.register_schema(self._schema)
        self._context = context

    async def health(self) -> PluginHealth:
        if self._context is None:
            return PluginHealth(PluginHealthStatus.UNAVAILABLE, "ontology plugin is inactive")
        return PluginHealth(
            PluginHealthStatus.READY,
            details={
                "ontology_id": self._schema.ontology_id,
                "ontology_version": self._schema.version,
            },
        )

    async def close(self) -> None:
        self._context = None

    def _require_context(self, context: PluginContext) -> None:
        if context is not self._context or context.cancelled or context.expired:
            raise PluginError(
                "ontology plugin context is not active",
                code=PluginErrorCode.PLUGIN_LOAD_FAILED,
            )


class OntologyProjectionConsolidatorPlugin(_OntologyPlugin):
    """Materialize accepted Claims into a rebuildable ontology index."""

    def __init__(
        self,
        store: OntologyStore,
        schema: OntologySchema,
        evidence_verifier: OntologyEvidenceVerifier,
    ) -> None:
        super().__init__(
            store,
            schema,
            PluginManifest(
                name="ontology-projection",
                version="0.1.0",
                kind=PluginKind.CONSOLIDATOR,
                capabilities=("ontology.project", "ontology.evidence.invalidate"),
                requires={"core": ">=0.1,<1.0"},
                config_schema={"type": "object", "additionalProperties": False},
                resource_limits=PluginResourceLimits(
                    timeout_ms=2_000,
                    max_candidates=100,
                    max_batch_size=256,
                    max_concurrency=1,
                ),
                failure_mode=PluginFailureMode.FALLBACK,
            ),
        )
        self._evidence_verifier = evidence_verifier

    async def consolidate(
        self,
        request: ConsolidationRequest,
        context: PluginContext,
    ) -> ConsolidationResult:
        self._require_context(context)
        if request.scope != context.scope:
            raise PluginError(
                "ontology consolidation is outside the trusted scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        if len(request.claims) > context.resource_limits.max_batch_size:
            raise PluginError(
                "ontology projection exceeded the claim batch limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        deleted = set(request.deleted_event_ids)
        for claim in request.claims:
            if claim.scope != request.scope:
                raise PluginError(
                    "ontology claim is outside the trusted scope",
                    code=PluginErrorCode.INVALID_IMPLEMENTATION,
                    field="scope",
                )
            projection = project_claim_to_ontology(claim, self._schema)
            if projection is not None:
                if deleted.intersection(projection.assertion.source_event_ids):
                    continue
                verified = await self._evidence_verifier.verify(
                    claim.scope, projection.assertion.source_event_ids
                )
                if not verified:
                    raise OntologyValidationError(
                        "ontology projection evidence does not exist in the trusted scope"
                    )
                await self._store.upsert_projection(projection)
        if deleted:
            await self._store.invalidate_sources(
                request.scope,
                tuple(deleted),
                max_rows=context.resource_limits.max_batch_size,
            )
        return ConsolidationResult()


class OntologyRetrieverPlugin(_OntologyPlugin):
    """Return bounded, evidence-cited candidates from one ontology version."""

    def __init__(self, store: OntologyStore, schema: OntologySchema) -> None:
        super().__init__(
            store,
            schema,
            PluginManifest(
                name="ontology-retriever",
                version="0.1.0",
                kind=PluginKind.RETRIEVER,
                capabilities=("ontology.search", "ontology.versioned"),
                requires={"core": ">=0.1,<1.0"},
                config_schema={"type": "object", "additionalProperties": False},
                resource_limits=PluginResourceLimits(
                    timeout_ms=1_000,
                    max_candidates=8,
                    max_batch_size=512,
                    max_concurrency=2,
                ),
                failure_mode=PluginFailureMode.FALLBACK,
            ),
        )

    async def retrieve(
        self,
        query: MemoryQuery,
        context: PluginContext,
    ) -> tuple[RetrievalCandidate, ...]:
        self._require_context(context)
        if query.scope != context.scope:
            raise PluginError(
                "ontology query is outside the trusted scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        limit = min(query.limit, context.resource_limits.max_candidates)
        matches = await self._store.search(
            query.text,
            query.scope,
            ontology_id=self._schema.ontology_id,
            ontology_version=self._schema.version,
            at_time=context.clock.now(),
            limit=limit,
            max_scan=context.resource_limits.max_batch_size,
        )
        if len(matches) > limit:
            raise PluginError(
                "ontology store exceeded the candidate limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        return tuple(
            RetrievalCandidate(
                item=match.item,
                channel=MemoryChannel.SEMANTIC,
                rank=rank,
                source_event_ids=match.source_event_ids,
                retriever="ontology-retriever",
                retrieval_method="ontology",
                metadata={
                    "ontology_id": self._schema.ontology_id,
                    "ontology_version": self._schema.version,
                    "ontology_score": match.score,
                },
            )
            for rank, match in enumerate(matches, start=1)
        )
