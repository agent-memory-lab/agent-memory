"""One PostgreSQL repeatable-read snapshot for final native evidence delivery."""

from agent_memory.domain import MemoryKind
from agent_memory.retrieval.atom_state import project_records, select_record_snapshots
from agent_memory.retrieval.delivery_guard import (
    bundle_values,
    diagnostic_ids,
    delivery_evidence_key,
    filter_delivery,
    requested_claim_ids,
    selected_artifact_ids,
    selected_source_ids,
    snapshot_delivery_bundle,
)
from agent_memory.retrieval.pipeline_delivery import external_candidates, pipeline_reference_ids

from . import admission
from .temporal_history import PostgresClaimHistory


async def guard_delivery(repository, query, bundle, *, pipeline=False):
    from .repository import utc_now

    bundle = snapshot_delivery_bundle(bundle)
    now = utc_now()
    valid_at, known_at = query.valid_at or now, query.known_at or now
    async with repository.pool.connection() as connection:
        async with connection.transaction():
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            claims = await PostgresClaimHistory(repository).read(
                connection, query.scope, valid_at, known_at,
            )
            snapshot = await admission.read_snapshot(connection, query.scope)
            atoms, info = project_records(select_record_snapshots(snapshot, known_at), valid_at)
            protected = await admission.protected_sources(connection, query.scope)
            where, params = repository._visible_scope_clause(query.scope)
            identities = selected_source_ids(bundle) | diagnostic_ids(bundle, (MemoryKind.EVENT,))
            claim_ids = requested_claim_ids(bundle)
            claim_sources = {source for claim in (*claims, *atoms) if claim.id in claim_ids
                             for source in claim.provenance.source_event_ids}
            identities.update(claim_sources)
            cursor = await connection.execute(
                "SELECT id FROM agent_memory_events WHERE tenant_id=%s AND namespace=%s "
                "AND archived_at IS NULL AND id = ANY(%s)",
                (query.scope.tenant_id, query.scope.namespace, sorted(claim_sources)),
            )
            claim_source_ids_live = {row["id"] for row in await cursor.fetchall()}
            cursor = await connection.execute(
                f"SELECT id,event_type FROM agent_memory_events WHERE {where} "
                "AND archived_at IS NULL "
                "AND id = ANY(%s)", (*params, sorted(identities)),
            )
            events = {row["id"]: {**row, "verified_evidence_keys": set()}
                      for row in await cursor.fetchall()}
            for item in bundle_values(bundle):
                if getattr(item, "kind", None) == MemoryKind.EVENT and item.id in events:
                    key = delivery_evidence_key(item)
                    if key is not None and await _event_matches(connection, item):
                        events[item.id]["verified_evidence_keys"].add(key)
            cursor = await connection.execute(
                f"SELECT id FROM agent_memory_claims WHERE {where} AND archived_at IS NULL "
                "AND id = ANY(%s)", (*params, [claim.id for claim in atoms]),
            )
            live_atoms = {row["id"] for row in await cursor.fetchall()}
            atoms = tuple(claim for claim in atoms if claim.id in live_atoms)
            cursor = await connection.execute(
                f"SELECT * FROM agent_memory_artifacts WHERE {where} AND id = ANY(%s)",
                (*params, sorted(selected_artifact_ids(bundle))),
            )
            artifacts = {row["id"]: {**row, "source_event_ids":
                         row["provenance_json"]["source_event_ids"]}
                         for row in await repository._live_artifact_rows(
                             connection, query.scope, await cursor.fetchall(),
                         )}
            external = external_candidates(
                bundle, await _owned_ids(connection, pipeline_reference_ids(bundle)),
                set(events), protected,
            ) if pipeline else ()
            return filter_delivery(
                bundle, query=query, valid_at=valid_at, known_at=known_at, claims=claims,
                atoms=atoms, atom_info=info, protected_sources=protected,
                events=events, artifacts=artifacts, claim_source_ids_live=claim_source_ids_live,
                external_evidence_keys=external, pipeline=pipeline,
            )


async def _owned_ids(connection, identities):
    owned = set()
    for table in ("events", "claims", "artifacts", "memory_tombstones"):
        cursor = await connection.execute(
            f"SELECT id FROM agent_memory_{table} WHERE id = ANY(%s)", (sorted(identities),),
        )
        owned.update(row["id"] for row in await cursor.fetchall())
    cursor = await connection.execute(
        "SELECT event_id FROM agent_memory_admission_tombstones WHERE event_id = ANY(%s)",
        (sorted(identities),),
    )
    owned.update(row["event_id"] for row in await cursor.fetchall())
    return owned


async def _event_matches(connection, item):
    """Validate revision and selected substring server-side, returning only a flag."""
    metadata = item.metadata
    span = metadata.get("source_span")
    if span is None:
        cursor = await connection.execute(
            "SELECT 1 FROM agent_memory_events WHERE id=%s AND content=%s", (item.id, item.text),
        )
    else:
        if not isinstance(span, dict) or span.get("unit") != "characters":
            return False
        start, end = span.get("start"), span.get("end")
        if type(start) is not int or type(end) is not int or not 0 <= start < end:
            return False
        cursor = await connection.execute(
            "SELECT 1 FROM agent_memory_events WHERE id=%s AND "
            "substring(content FROM %s FOR %s)=%s AND char_length(content)=%s "
            "AND encode(sha256(convert_to(content,'UTF8')),'hex')=%s",
            (item.id, start + 1, end - start, item.text, metadata.get("source_chars"),
             metadata.get("source_revision")),
        )
    return await cursor.fetchone() is not None
