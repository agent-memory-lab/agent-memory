"""SQLite final-delivery snapshot; no model or trace await follows this boundary."""

import json

from ..domain import MemoryKind
from .atom_state import project_records, select_record_snapshots
from .delivery_guard import (
    bundle_values,
    diagnostic_ids,
    delivery_evidence_key,
    filter_delivery,
    requested_claim_ids,
    selected_artifact_ids,
    selected_source_ids,
    snapshot_delivery_bundle,
)
from .pipeline_delivery import external_candidates, pipeline_reference_ids
from .temporal_history import SQLiteClaimHistory


def guard_delivery(repository, query, bundle, *, pipeline=False):
    with repository._connection() as connection:
        connection.execute("BEGIN")
        return guard_delivery_snapshot(repository, connection, query, bundle, pipeline=pipeline)


def guard_delivery_snapshot(repository, connection, query, bundle, *, pipeline=False):
    # Use the adapter clock so controlled-time contracts and native current
    # reads resolve omitted axes identically.
    from ..sqlite import utc_now

    bundle = snapshot_delivery_bundle(bundle)
    now = utc_now()
    valid_at, known_at = query.valid_at or now, query.known_at or now
    claims = SQLiteClaimHistory(repository).read(connection, query.scope, valid_at, known_at)
    snapshot = repository._admission_snapshot(connection, query.scope)
    atoms, info = project_records(select_record_snapshots(snapshot, known_at), valid_at)
    protected = repository._admission_protected_sources(connection, query.scope)
    where, params = repository._visible_scope_clause(query.scope)
    identities = selected_source_ids(bundle) | diagnostic_ids(bundle, (MemoryKind.EVENT,))
    claim_ids = requested_claim_ids(bundle)
    claim_sources = {source for claim in (*claims, *atoms) if claim.id in claim_ids
                     for source in claim.provenance.source_event_ids}
    identities.update(claim_sources)
    claim_source_ids_live = set()
    for start in range(0, len(claim_sources), 256):
        batch = sorted(claim_sources)[start:start + 256]
        marks = ",".join("?" for _ in batch)
        claim_source_ids_live.update(row["id"] for row in connection.execute(
            "SELECT id FROM events WHERE tenant_id=? AND namespace=? "
            f"AND archived_at IS NULL AND id IN ({marks})",
            (query.scope.tenant_id, query.scope.namespace, *batch),
        ))
    events = {}
    for start in range(0, len(identities), 256):
        batch = sorted(identities)[start:start + 256]
        marks = ",".join("?" for _ in batch)
        rows = connection.execute(
            f"SELECT id,event_type FROM events WHERE {where} AND archived_at IS NULL "
            f"AND id IN ({marks})", (*params, *batch),
        ).fetchall()
        events.update((row["id"], {**dict(row), "verified_evidence_keys": set()})
                      for row in rows)
    for item in bundle_values(bundle):
        if getattr(item, "kind", None) == MemoryKind.EVENT and item.id in events:
            key = delivery_evidence_key(item)
            if key is not None and _event_matches(connection, item):
                events[item.id]["verified_evidence_keys"].add(key)
    atom_ids = {claim.id for claim in atoms}
    live_atoms = set()
    for start in range(0, len(atom_ids), 256):
        batch = sorted(atom_ids)[start:start + 256]
        marks = ",".join("?" for _ in batch)
        live_atoms.update(row["id"] for row in connection.execute(
            f"SELECT id FROM claims WHERE {where} AND archived_at IS NULL "
            f"AND id IN ({marks})", (*params, *batch),
        ))
    atoms = tuple(claim for claim in atoms if claim.id in live_atoms)
    identities = sorted(selected_artifact_ids(bundle))
    artifacts = {}
    for start in range(0, len(identities), 256):
        batch = identities[start:start + 256]
        marks = ",".join("?" for _ in batch)
        rows = connection.execute(
            f"SELECT * FROM artifacts WHERE {where} AND id IN ({marks})",
            (*params, *batch),
        ).fetchall()
        for row in repository._live_artifact_rows(connection, query.scope, rows):
            artifacts[row["id"]] = {**dict(row), "source_event_ids":
                json.loads(row["provenance_json"])["source_event_ids"]}
    external = external_candidates(
        bundle, _owned_ids(connection, pipeline_reference_ids(bundle)),
        set(events), protected,
    ) if pipeline else ()
    return filter_delivery(
        bundle, query=query, valid_at=valid_at, known_at=known_at, claims=claims,
        atoms=atoms, atom_info=info, protected_sources=protected,
        events=events, artifacts=artifacts, claim_source_ids_live=claim_source_ids_live,
        external_evidence_keys=external, pipeline=pipeline,
    )


def _event_matches(connection, item):
    """Check indexed revision and exact selected text without hydrating a source.

    Every source UPDATE deletes its old lexical headers transactionally, so a
    surviving revision proves even text outside the returned slice unchanged.
    Dirty/unindexed replacements fail closed; this guard never rebuilds indexes.
    """
    metadata = item.metadata
    span = metadata.get("source_span")
    if span is None:
        return connection.execute(
            "SELECT 1 FROM events WHERE id=? AND content=?", (item.id, item.text),
        ).fetchone() is not None
    if not isinstance(span, dict) or span.get("unit") != "characters":
        return False
    start, end = span.get("start"), span.get("end")
    if type(start) is not int or type(end) is not int or not 0 <= start < end:
        return False
    row = connection.execute(
        "SELECT 1 FROM events e WHERE e.id=? AND "
        "substr(e.content,?,?)=? AND EXISTS (SELECT 1 FROM lexical_chunks c "
        "WHERE c.source_table='events' AND c.source_id=e.id AND c.source_revision=? "
        "AND c.source_chars=? AND c.source_chars>=?)",
        (item.id, start + 1, end - start, item.text, metadata.get("source_revision"),
         metadata.get("source_chars"), end),
    ).fetchone()
    return row is not None


def _owned_ids(connection, identities):
    owned = set()
    identities = sorted(identities)
    for start in range(0, len(identities), 256):
        batch = identities[start:start + 256]
        placeholders = ",".join("?" for _ in batch)
        for table in ("events", "claims", "artifacts", "memory_tombstones"):
            owned.update(row["id"] for row in connection.execute(
                f"SELECT id FROM {table} WHERE id IN ({placeholders})", batch,
            ))
        owned.update(row["event_id"] for row in connection.execute(
            f"SELECT event_id FROM admission_tombstones WHERE event_id IN ({placeholders})", batch,
        ))
    return owned
