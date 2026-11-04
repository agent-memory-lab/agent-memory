"""Bitemporal Claim revisions and bounded valid-time interval materialization.

Input observations are preserved independently of the legacy current-state rows.
Each write closes one system-time snapshot and publishes the next atomically.
All intervals are half-open. System timestamps come from the trusted writer.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from ..domain import (
    Claim,
    ClaimStatus,
    MemoryChannel,
    MemoryItem,
    MemoryKind,
    MemoryScope,
    Provenance,
    utc_now,
)
from ..serialization import to_jsonable
from .analyzer import lexical_terms

MAX_OBSERVATIONS_PER_KEY = 512


class TemporalHistoryUnavailable(ValueError):
    """An old database cannot reconstruct knowledge predating temporal migration."""


def aware(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(UTC)


def dump_claim(claim: Claim) -> str:
    return json.dumps(to_jsonable(claim), sort_keys=True)


def load_claim(payload: str | dict) -> Claim:
    data = json.loads(payload) if isinstance(payload, str) else dict(payload)
    data["scope"] = MemoryScope(**data["scope"])
    provenance = dict(data["provenance"])
    provenance["source_event_ids"] = tuple(provenance["source_event_ids"])
    provenance["created_at"] = datetime.fromisoformat(provenance["created_at"])
    data["provenance"] = Provenance(**provenance)
    data["status"] = ClaimStatus(data["status"])
    for field in ("valid_from", "valid_to", "created_at", "system_from", "system_to"):
        if data.get(field) is not None:
            data[field] = datetime.fromisoformat(data[field])
    return Claim(**data)


def materialize(claims: list[Claim]) -> tuple[Claim, ...]:
    """Resolve valid intervals by effective time, never by arrival order.

    Newer effective starts take precedence. Equal starts use observation order.
    A bounded interval restores the previous fact when it ends. Explicit
    corrections are removed by the store before this function is called.
    """
    if len(claims) > MAX_OBSERVATIONS_PER_KEY:
        raise ValueError("temporal observation limit reached for this key")
    boundaries: set[datetime] = set()
    for claim in claims:
        start = aware(claim.valid_from, "valid_from")
        end = aware(claim.valid_to, "valid_to") if claim.valid_to else None
        if end is not None and end <= start:
            raise ValueError("valid_to must be after valid_from")
        boundaries.add(start)
        if end is not None:
            boundaries.add(end)
    segments: list[Claim] = []
    points = sorted(boundaries)
    for index, start in enumerate(points):
        end = points[index + 1] if index + 1 < len(points) else None
        visible = [
            c
            for c in claims
            if c.valid_from <= start and (c.valid_to is None or start < c.valid_to)
        ]
        if not visible:
            continue
        winner = max(visible, key=lambda c: (c.valid_from, c.system_from or c.created_at, c.id))
        if segments and segments[-1].id == winner.id and segments[-1].valid_to == start:
            segments[-1] = replace(segments[-1], valid_to=end)
        else:
            segments.append(
                replace(winner, valid_from=start, valid_to=end, status=ClaimStatus.ACTIVE)
            )
    return tuple(segments)


def temporal_candidates(claims, text: str, limit: int) -> tuple[MemoryItem, ...]:
    terms = set(lexical_terms(text))
    items = []
    for claim in claims:
        document_terms = set(lexical_terms(" ".join((
            claim.key, claim.text, json.dumps(to_jsonable(claim.value), ensure_ascii=False),
        ))))
        match = len(terms & document_terms)
        items.append(
            MemoryItem(
                claim.id,
                MemoryKind.CLAIM,
                claim.text,
                (match / max(1, len(terms))) * 0.55
                + claim.importance * 0.25
                + claim.confidence * 0.2,
                claim.created_at,
                {
                    "channel": MemoryChannel.SEMANTIC,
                    "key": claim.key,
                    "version": claim.version,
                    "source_event_ids": claim.provenance.source_event_ids,
                    "valid_from": claim.valid_from.isoformat(),
                    "valid_to": claim.valid_to.isoformat() if claim.valid_to else None,
                    "system_from": claim.system_from.isoformat() if claim.system_from else None,
                    "system_to": claim.system_to.isoformat() if claim.system_to else None,
                },
            )
        )
    return tuple(sorted(items, key=lambda item: (-item.score, item.id))[:limit])


class SQLiteClaimHistory:
    def __init__(self, repository):
        self.repository = repository

    def initialize(self, connection) -> None:
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS claim_observations (
                claim_id TEXT PRIMARY KEY REFERENCES claims(id) ON DELETE CASCADE,
                partition_key TEXT NOT NULL, claim_key TEXT NOT NULL,
                observed_at TEXT NOT NULL, payload_json TEXT NOT NULL,
                retracted INTEGER NOT NULL DEFAULT 0,
                legacy INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS claim_observations_key_idx
            ON claim_observations(partition_key, claim_key);
            CREATE TABLE IF NOT EXISTS claim_versions (
                revision_id TEXT PRIMARY KEY,
                claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
                partition_key TEXT NOT NULL, claim_key TEXT NOT NULL,
                valid_from TEXT NOT NULL, valid_to TEXT,
                system_from TEXT NOT NULL, system_to TEXT, payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS claim_versions_lookup_idx
            ON claim_versions(partition_key, system_from, system_to, valid_from);
        """)
        # Migration publishes only the surviving state; previous overwritten
        # interpretations are unknowable and must not be represented as history.
        floor = utc_now()
        keys = set()
        for row in connection.execute("""
            SELECT * FROM claims WHERE archived_at IS NULL AND status IN ('active', 'superseded')
            AND id NOT IN (SELECT claim_id FROM claim_observations)
        """).fetchall():
            claim = self.repository._claim_from_row(row)
            if claim.valid_to is not None and claim.valid_to <= claim.valid_from:
                continue
            connection.execute(
                "INSERT INTO claim_observations VALUES (?, ?, ?, ?, ?, 0, 1)",
                (claim.id, row["partition_key"], claim.key, floor.isoformat(), dump_claim(claim)),
            )
            keys.add((claim.scope.partition_key(), claim.key))
        for partition, key in keys:
            self.publish(connection, partition, key, floor)

    def record(self, connection, claim: Claim) -> None:
        start = aware(claim.valid_from, "valid_from")
        claim = replace(
            claim,
            valid_from=start,
            valid_to=aware(claim.valid_to, "valid_to") if claim.valid_to else None,
        )
        if claim.valid_to is not None and aware(claim.valid_to, "valid_to") <= start:
            raise ValueError("valid_to must be after valid_from")
        partition = claim.scope.partition_key()
        if claim.corrects_id is not None:
            cursor = connection.execute(
                """
                UPDATE claim_observations SET retracted = 1
                WHERE claim_id = ? AND partition_key = ? AND claim_key = ? AND retracted = 0
            """,
                (claim.corrects_id, partition, claim.key),
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    "correction target is missing, already corrected, or outside the key/scope"
                )
        now = self.next_time(connection, partition, claim.key)
        connection.execute(
            "INSERT INTO claim_observations VALUES (?, ?, ?, ?, ?, 0, 0)",
            (claim.id, partition, claim.key, now.isoformat(), dump_claim(claim)),
        )
        self.publish(connection, partition, claim.key, now)

    def next_time(self, connection, partition, key):
        row = connection.execute(
            "SELECT MAX(system_from) AS last FROM claim_versions "
            "WHERE partition_key = ? AND claim_key = ?",
            (partition, key),
        ).fetchone()
        now = utc_now()
        if row["last"]:
            now = max(now, datetime.fromisoformat(row["last"]) + timedelta(microseconds=1))
        return now

    def refresh_evidence(self, connection, claim_id):
        row = connection.execute(
            "SELECT partition_key, claim_key FROM claim_observations WHERE claim_id = ?",
            (claim_id,),
        ).fetchone()
        if row:
            now = self.next_time(connection, row["partition_key"], row["claim_key"])
            self.publish(connection, row["partition_key"], row["claim_key"], now)

    def publish(self, connection, partition, key, now):
        rows = connection.execute(
            """
            SELECT o.*, c.provenance_json FROM claim_observations o
            JOIN claims c ON c.id = o.claim_id
            WHERE o.partition_key = ? AND o.claim_key = ?
            AND o.retracted = 0 AND c.archived_at IS NULL
            ORDER BY o.observed_at, o.claim_id LIMIT ?
        """,
            (partition, key, MAX_OBSERVATIONS_PER_KEY + 1),
        ).fetchall()
        claims = []
        for row in rows:
            original = load_claim(row["payload_json"])
            current = json.loads(row["provenance_json"])
            current["source_event_ids"] = tuple(current["source_event_ids"])
            current["created_at"] = datetime.fromisoformat(current["created_at"])
            claims.append(
                replace(
                    original,
                    provenance=Provenance(**current),
                    system_from=datetime.fromisoformat(row["observed_at"]),
                )
            )
        segments = materialize(claims)
        connection.execute(
            "UPDATE claim_versions SET system_to = ? "
            "WHERE partition_key = ? AND claim_key = ? AND system_to IS NULL",
            (now.isoformat(), partition, key),
        )
        for claim in segments:
            connection.execute(
                "INSERT INTO claim_versions VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)",
                (
                    str(uuid4()),
                    claim.id,
                    partition,
                    key,
                    claim.valid_from.isoformat(),
                    claim.valid_to.isoformat() if claim.valid_to else None,
                    now.isoformat(),
                    dump_claim(claim),
                ),
            )

    def read(
        self, connection, scope, valid_at, known_at, *, limit=None,
        legacy_only=False, protected_sources=(), claim_ids=None,
    ):
        if limit is not None and (type(limit) is not int or limit < 1):
            raise ValueError("state limit must be a positive integer")
        valid_at, known_at = aware(valid_at, "valid_at"), aware(known_at, "known_at")
        where, params = self.repository._visible_scope_clause(scope)
        authorized = f"SELECT id FROM claims WHERE {where} AND archived_at IS NULL"
        legacy = connection.execute(
            "SELECT MIN(observed_at) AS floor FROM claim_observations "
            f"WHERE legacy = 1 AND claim_id IN ({authorized})",
            params,
        ).fetchone()
        if legacy["floor"] and known_at < datetime.fromisoformat(legacy["floor"]):
            raise TemporalHistoryUnavailable(
                "knowledge time predates the available temporal history"
            )
        # Validate sources and authorization before applying the output bound.
        # The JSON aggregate preserves provenance order, including repeated IDs,
        # without issuing a source SELECT for every returned claim.
        live_sources = (
            "SELECT evidence.value FROM json_each(v.payload_json, "
            "'$.provenance.source_event_ids') evidence "
            "JOIN events e ON e.id=evidence.value WHERE e.archived_at IS NULL"
        )
        eligibility, extra = "", ()
        if legacy_only:
            eligibility = (
                "AND substr(v.claim_key, 1, 5) != 'atom:' "
                "AND substr(v.claim_id, 1, 11) != 'atom-claim:' "
                f"AND NOT EXISTS ({live_sources} AND evidence.value IN "
                "(SELECT value FROM json_each(?))) "
            )
            extra = (json.dumps(tuple(protected_sources)),)
        if claim_ids is not None:
            eligibility += "AND v.claim_id IN (SELECT value FROM json_each(?)) "
            extra += (json.dumps(tuple(claim_ids)),)
        bound = " LIMIT ?" if limit is not None else ""
        rows = connection.execute(
            f"""
            SELECT v.*, (SELECT json_group_array(value) FROM (
                {live_sources} ORDER BY CAST(evidence.key AS INTEGER)
            )) AS live_sources_json
            FROM claim_versions v WHERE claim_id IN ({authorized})
            AND system_from <= ? AND (system_to IS NULL OR ? < system_to)
            AND valid_from <= ? AND (valid_to IS NULL OR ? < valid_to)
            AND EXISTS ({live_sources}) {eligibility}
            ORDER BY json_extract(v.payload_json, '$.importance') DESC,
                     json_extract(v.payload_json, '$.confidence') DESC,
                     v.claim_id COLLATE BINARY, system_from DESC, revision_id{bound}
        """,
            (*params, known_at.isoformat(), known_at.isoformat(),
             valid_at.isoformat(), valid_at.isoformat(), *extra,
             *((limit,) if limit is not None else ())),
        ).fetchall()
        claims = []
        for row in rows:
            claim = load_claim(row["payload_json"])
            sources = tuple(json.loads(row["live_sources_json"]))
            claims.append(
                replace(
                    claim,
                    provenance=replace(claim.provenance, source_event_ids=sources),
                    system_from=datetime.fromisoformat(row["system_from"]),
                    system_to=datetime.fromisoformat(row["system_to"])
                    if row["system_to"]
                    else None,
                )
            )
        return tuple(sorted(claims, key=lambda c: (-c.importance, -c.confidence, c.id)))

    def scrub_sources(self, connection, claim_id, removed):
        """Erasure applies to historical evidence as well as current provenance."""
        for table, identity in (
            ("claim_observations", "claim_id"),
            ("claim_versions", "revision_id"),
        ):
            rows = connection.execute(
                f"SELECT {identity}, payload_json FROM {table} WHERE claim_id = ?", (claim_id,)
            ).fetchall()
            for row in rows:
                claim = load_claim(row["payload_json"])
                sources = tuple(
                    source for source in claim.provenance.source_event_ids if source not in removed
                )
                payload = dump_claim(
                    replace(claim, provenance=replace(claim.provenance, source_event_ids=sources))
                )
                connection.execute(
                    f"UPDATE {table} SET payload_json = ? WHERE {identity} = ?",
                    (payload, row[identity]),
                )
