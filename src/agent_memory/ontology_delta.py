"""Disk-backed snapshot differences for opt-in local shadow index updates.

Only impacted connected components are reprojected. Immutable snapshots and a
shadow copy are still required; this is not log-based CDC or an in-place index.
"""
from __future__ import annotations

import asyncio
from hashlib import sha256
import json
from pathlib import Path
from uuid import uuid4

from .ontology_memory import project_claim_to_ontology
from .ontology_source import SQLiteOntologySnapshot, database
from .sqlite import SQLiteMemoryRepository


async def prepare_delta(snapshot, previous_snapshot, schema, previous_index, target_index):
    """Clone a validated index, remove affected components, select new inputs.

    Caller owns the workspace lock, must supply the same source and schema, and
    must not publish until full snapshot acceptance succeeds. Any failure leaves
    only an unbound shadow generation, which the workspace can discard safely.
    """
    if snapshot.scope != previous_snapshot.scope:
        raise ValueError("delta snapshots must have the same scope")
    if snapshot.revision < previous_snapshot.revision:
        raise ValueError("delta source revision moved backwards")
    await asyncio.to_thread(_prepare, snapshot, previous_snapshot, schema,
                            Path(previous_index), Path(target_index))


def _prepare(snapshot, previous, schema, previous_index, target_index):
    codec = SQLiteMemoryRepository(snapshot.path)
    with database(snapshot.path) as current:
        current.execute("PRAGMA temp_store=FILE")
        current.execute("""CREATE TEMP TABLE projections (
            side INTEGER, claim_id TEXT, fingerprint TEXT, subject TEXT, object TEXT,
            PRIMARY KEY(side, claim_id))""")
        for side, path in ((0, previous.path), (1, snapshot.path)):
            with database(path, readonly=True) as source:
                rows = source.execute("SELECT * FROM claims ORDER BY id")
                while page := rows.fetchmany(128):
                    for row in page:
                        projection = project_claim_to_ontology(codec._claim_from_row(row), schema)
                        if projection is None:
                            continue
                        fingerprint = sha256(json.dumps(dict(row), sort_keys=True,
                            default=str, ensure_ascii=False).encode()).hexdigest()
                        assertion = projection.assertion
                        current.execute("INSERT INTO projections VALUES (?, ?, ?, ?, ?)",
                            (side, row["id"], fingerprint,
                             assertion.subject_entity_id, assertion.object_entity_id))
        current.execute("CREATE TEMP TABLE changed (claim_id TEXT PRIMARY KEY)")
        current.execute("""INSERT INTO changed
            SELECT p.claim_id FROM projections p LEFT JOIN projections other
            ON other.claim_id=p.claim_id AND other.side=1-p.side
            WHERE other.claim_id IS NULL OR other.fingerprint!=p.fingerprint
            GROUP BY p.claim_id""")
        # A shared entity can carry evidence from multiple assertions. Rebuild
        # its whole component to avoid stale evidence, labels or conflict state.
        current.execute("CREATE TABLE ontology_delta_entities (id TEXT PRIMARY KEY)")
        current.execute("""INSERT INTO ontology_delta_entities
            WITH RECURSIVE links(a,b) AS (
                SELECT subject,object FROM projections WHERE object IS NOT NULL
                UNION SELECT object,subject FROM projections WHERE object IS NOT NULL
            ), seeds(id) AS (
                SELECT subject FROM projections JOIN changed USING(claim_id)
                UNION SELECT object FROM projections JOIN changed USING(claim_id)
                WHERE object IS NOT NULL
            ), affected(id) AS (
                SELECT id FROM seeds
                UNION SELECT links.b FROM links JOIN affected ON links.a=affected.id
            ) SELECT id FROM affected""")
        current.execute("CREATE TABLE ontology_delta_claims (id TEXT PRIMARY KEY)")
        current.execute("""INSERT INTO ontology_delta_claims
            SELECT claim_id FROM projections WHERE side=1 AND (
                subject IN (SELECT id FROM ontology_delta_entities)
                OR object IN (SELECT id FROM ontology_delta_entities))""")
    with target_index.open("xb"):
        pass
    with database(previous_index, readonly=True) as source, database(target_index) as target:
        source.backup(target, pages=128)
        target.execute("ATTACH DATABASE ? AS delta", (str(snapshot.path),))
        target.execute("BEGIN IMMEDIATE")
        target.execute("UPDATE ontology_index_identity SET index_id=? WHERE singleton=1", (str(uuid4()),))
        affected = "SELECT id FROM delta.ontology_delta_entities"
        assertions = ("SELECT assertion_id FROM ontology_assertions WHERE "
                      f"subject_entity_id IN ({affected}) OR object_entity_id IN ({affected})")
        target.execute(f"DELETE FROM ontology_assertion_sources WHERE assertion_id IN ({assertions})")
        target.execute(f"DELETE FROM ontology_conflict_resolutions WHERE subject_entity_id IN ({affected})")
        target.execute(f"DELETE FROM ontology_assertions WHERE assertion_id IN ({assertions})")
        target.execute(f"DELETE FROM ontology_entity_sources WHERE entity_id IN ({affected})")
        target.execute(f"DELETE FROM ontology_entities WHERE entity_id IN ({affected})")


class _DeltaPageSource:
    def __init__(self, snapshot):
        self.snapshot = snapshot

    async def read_page(self, scope, snapshot_id, *, cursor, limit):
        from .ontology_backfill import OntologyClaimPage
        if scope != self.snapshot.scope or snapshot_id != self.snapshot.snapshot_id:
            raise ValueError("delta snapshot identity or scope mismatch")
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("delta page limit must be between 1 and 256")

        def read():
            with database(self.snapshot.path, readonly=True) as connection:
                rows = connection.execute("""SELECT c.* FROM claims c
                    JOIN ontology_delta_claims selected ON selected.id=c.id
                    WHERE c.id>? ORDER BY c.id LIMIT ?""", (cursor or "", limit + 1)).fetchall()
            codec = SQLiteMemoryRepository(self.snapshot.path)
            claims = tuple(codec._claim_from_row(row) for row in rows[:limit])
            return OntologyClaimPage(claims, claims[-1].id if len(rows) > limit else None)
        return await asyncio.to_thread(read)


async def projection_source(snapshot: SQLiteOntologySnapshot):
    """Recover delta selection from the snapshot, including after restart."""
    def selected():
        with database(snapshot.path, readonly=True) as connection:
            return connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ontology_delta_claims'").fetchone() is not None
    return _DeltaPageSource(snapshot) if await asyncio.to_thread(selected) else snapshot
