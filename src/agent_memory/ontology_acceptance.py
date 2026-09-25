"""Independent, streaming completeness checks for a SQLite ontology index."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json

from .ontology_memory import project_claim_to_ontology
from .ontology_source import SQLiteOntologySnapshot, database
from .sqlite import SQLiteMemoryRepository


@dataclass(frozen=True, slots=True)
class OntologyIndexAcceptance:
    ready: bool
    expected_assertions: int
    actual_assertions: int
    problems: tuple[str, ...]
    source_revision: int


async def validate_ontology_index(snapshot: SQLiteOntologySnapshot, schema, index_path):
    """Recompute expected assertions from the snapshot, without using text search.

    Uses disk-backed temporary tables and bounded reads. This verifies the given
    snapshot; callers must compare its revision to the live source before use.
    """
    return await asyncio.to_thread(_validate, snapshot, schema, index_path)


def _signature(subject, predicate, object_id, literal, valid_from, valid_to):
    return json.dumps([subject, predicate, object_id, literal, valid_from, valid_to], sort_keys=True)


def _validate(snapshot, schema, index_path):
    problems = []

    def problem(message):
        if message not in problems and len(problems) < 20:
            problems.append(message)

    codec = SQLiteMemoryRepository(snapshot.path)
    with database(snapshot.path, readonly=True) as source, database(index_path, readonly=True) as index:
        index.execute("PRAGMA temp_store=FILE")
        index.execute("CREATE TEMP TABLE expected (id TEXT PRIMARY KEY, signature TEXT, text TEXT, confidence REAL)")
        index.execute("CREATE TEMP TABLE expected_sources (id TEXT, event TEXT, PRIMARY KEY(id,event))")
        index.execute("CREATE TEMP TABLE expected_entities (id TEXT PRIMARY KEY, class TEXT, label TEXT)")
        claims = source.execute("SELECT * FROM claims ORDER BY id")
        while rows := claims.fetchmany(128):
            for row in rows:
                projection = project_claim_to_ontology(codec._claim_from_row(row), schema)
                if projection is None:
                    continue
                assertion = projection.assertion
                ids = tuple(set(assertion.source_event_ids))
                if not ids or source.execute(
                    "SELECT COUNT(*) FROM evidence WHERE id IN (" + ",".join("?" for _ in ids) + ")", ids,
                ).fetchone()[0] != len(ids):
                    problem("missing source evidence")
                signature = _signature(
                    assertion.subject_entity_id, assertion.predicate_id, assertion.object_entity_id,
                    assertion.literal_value, assertion.valid_from.isoformat(),
                    assertion.valid_to.isoformat() if assertion.valid_to else None,
                )
                index.execute(
                    "INSERT INTO expected VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET confidence=MAX(confidence,excluded.confidence)",
                    (assertion.assertion_id, signature, assertion.text, assertion.confidence),
                )
                index.executemany("INSERT OR IGNORE INTO expected_sources VALUES (?, ?)", ((assertion.assertion_id, event) for event in ids))
                for entity in projection.entities:
                    existing = index.execute("SELECT class,label FROM expected_entities WHERE id=?", (entity.entity_id,)).fetchone()
                    if existing and tuple(existing) != (entity.class_id, entity.label):
                        problem("conflicting entity identity")
                    index.execute("INSERT OR IGNORE INTO expected_entities VALUES (?, ?, ?)", (entity.entity_id, entity.class_id, entity.label))
        expected = index.execute("SELECT COUNT(*) FROM expected").fetchone()[0]
        actual = 0
        records = index.execute(
            "SELECT a.*, e.signature expected_signature, e.text expected_text, e.confidence expected_confidence "
            "FROM ontology_assertions a LEFT JOIN expected e ON e.id=a.assertion_id "
            "WHERE a.partition_key=? AND a.ontology_id=? AND a.ontology_version=?",
            (snapshot.scope.partition_key(), schema.ontology_id, schema.version),
        )
        while rows := records.fetchmany(128):
            for row in rows:
                actual += 1
                if row["expected_signature"] is None:
                    problem("unexpected assertion")
                    continue
                if row["archived_at"] is not None or row["status"] != "active":
                    problem("inactive or conflicting assertion")
                signature = _signature(
                    row["subject_entity_id"], row["predicate_id"], row["object_entity_id"],
                    json.loads(row["literal_json"]) if row["literal_json"] else None,
                    row["valid_from"], row["valid_to"],
                )
                if signature != row["expected_signature"] or row["text"] != row["expected_text"] or row["confidence"] != row["expected_confidence"]:
                    problem("assertion content mismatch")
                sources = {value[0] for value in index.execute("SELECT event FROM expected_sources WHERE id=?", (row["assertion_id"],))}
                if sources != set(json.loads(row["source_event_ids_json"])):
                    problem("assertion evidence mismatch")
        if index.execute(
            "SELECT 1 FROM expected e LEFT JOIN ontology_assertions a ON a.assertion_id=e.id "
            "WHERE a.assertion_id IS NULL LIMIT 1"
        ).fetchone():
            problem("missing assertion")
        if actual != expected:
            problem("assertion count mismatch")
        if index.execute(
            "SELECT 1 FROM expected_entities e LEFT JOIN ontology_entities a "
            "ON a.entity_id=e.id AND a.partition_key=? AND a.ontology_id=? AND a.ontology_version=? "
            "WHERE a.entity_id IS NULL OR a.archived_at IS NOT NULL OR a.class_id!=e.class OR a.label!=e.label LIMIT 1",
            (snapshot.scope.partition_key(), schema.ontology_id, schema.version),
        ).fetchone():
            problem("missing or inconsistent entity")
        return OntologyIndexAcceptance(not problems, expected, actual, tuple(problems), snapshot.revision)
