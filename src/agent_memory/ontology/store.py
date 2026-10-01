"""Transactional SQL persistence for the local ontology index."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..domain import (
    MemoryItem,
    MemoryKind,
    MemoryScope,
    ScopeLevel,
    canonical_json,
    utc_now,
)
from .model import (
    OntologyAssertion,
    OntologyAssertionStatus,
    OntologyConflictResolution,
    OntologyEntity,
    OntologyMatch,
    OntologyProjection,
    OntologySchema,
    OntologyValidationError,
    _schema_payload,
    _validate_projection,
)
from .queries import SQLOntologyQueries, _assertion_from_row, ontology_instant


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
        connection.create_function("ontology_instant", 1, ontology_instant, deterministic=True)
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
        if not isinstance(at_time, datetime) or at_time.utcoffset() is None:
            raise ValueError("at_time must be timezone aware")
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
                  AND {self._validity_sql()}
                ORDER BY {self._timestamp_sql("a.valid_from")} DESC, a.assertion_id
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
