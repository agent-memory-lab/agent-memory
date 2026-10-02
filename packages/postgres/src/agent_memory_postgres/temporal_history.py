"""PostgreSQL orchestration for the shared Claim interval materializer."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

from agent_memory.domain import utc_now
from agent_memory.retrieval.temporal_history import (
    MAX_OBSERVATIONS_PER_KEY,
    TemporalHistoryUnavailable,
    aware,
    dump_claim,
    load_claim,
    materialize,
)


class PostgresClaimHistory:
    def __init__(self, repository):
        self.repository = repository

    async def initialize(self, connection):
        # Concurrent initialization must not race while bootstrapping legacy rows.
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            ("agent-memory:claim-history-migration",),
        )
        cursor = await connection.execute("""
            SELECT * FROM agent_memory_claims WHERE archived_at IS NULL
            AND status IN ('active', 'superseded') AND id NOT IN
            (SELECT claim_id FROM agent_memory_claim_observations)
        """)
        floor, keys = utc_now(), set()
        for row in await cursor.fetchall():
            claim = self.repository._claim_from_row(row)
            if claim.valid_to is not None and claim.valid_to <= claim.valid_from:
                continue
            await connection.execute(
                """INSERT INTO agent_memory_claim_observations
                VALUES (%s, %s, %s, %s, %s::jsonb, false, true)""",
                (claim.id, row["partition_key"], claim.key, floor, dump_claim(claim)),
            )
            keys.add((row["partition_key"], claim.key))
        for partition, key in sorted(keys):
            await self.publish(
                connection, partition, key, await self.next_time(connection, partition, key)
            )

    async def record(self, connection, claim):
        start = aware(claim.valid_from, "valid_from")
        claim = replace(
            claim,
            valid_from=start,
            valid_to=aware(claim.valid_to, "valid_to") if claim.valid_to else None,
        )
        if claim.valid_to is not None and claim.valid_to <= start:
            raise ValueError("valid_to must be after valid_from")
        partition = claim.scope.partition_key()
        if claim.corrects_id is not None:
            cursor = await connection.execute(
                """
                UPDATE agent_memory_claim_observations SET retracted = true
                WHERE claim_id = %s AND partition_key = %s AND claim_key = %s AND NOT retracted
            """,
                (claim.corrects_id, partition, claim.key),
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    "correction target is missing, already corrected, or outside the key/scope"
                )
        now = await self.next_time(connection, partition, claim.key)
        await connection.execute(
            """INSERT INTO agent_memory_claim_observations
            VALUES (%s, %s, %s, %s, %s::jsonb, false, false)""",
            (claim.id, partition, claim.key, now, dump_claim(claim)),
        )
        await self.publish(connection, partition, claim.key, now)

    async def next_time(self, connection, partition, key):
        cursor = await connection.execute(
            """SELECT MAX(system_from) AS last
            FROM agent_memory_claim_versions WHERE partition_key = %s AND claim_key = %s""",
            (partition, key),
        )
        row = await cursor.fetchone()
        now = utc_now()
        return max(now, row["last"] + timedelta(microseconds=1)) if row["last"] else now

    async def refresh_evidence(self, connection, claim_id):
        cursor = await connection.execute(
            "SELECT partition_key, claim_key FROM agent_memory_claim_observations "
            "WHERE claim_id = %s",
            (claim_id,),
        )
        row = await cursor.fetchone()
        if row:
            now = await self.next_time(connection, row["partition_key"], row["claim_key"])
            await self.publish(connection, row["partition_key"], row["claim_key"], now)

    async def publish(self, connection, partition, key, now):
        cursor = await connection.execute(
            """
            SELECT o.*, c.provenance_json FROM agent_memory_claim_observations o
            JOIN agent_memory_claims c ON c.id = o.claim_id
            WHERE o.partition_key = %s AND o.claim_key = %s AND NOT o.retracted
            AND c.archived_at IS NULL ORDER BY o.observed_at, o.claim_id LIMIT %s
        """,
            (partition, key, MAX_OBSERVATIONS_PER_KEY + 1),
        )
        claims = []
        for row in await cursor.fetchall():
            original = load_claim(row["payload_json"])
            data = dict(row["payload_json"])
            data["provenance"] = row["provenance_json"]
            claims.append(
                replace(
                    original, provenance=load_claim(data).provenance, system_from=row["observed_at"]
                )
            )
        segments = materialize(claims)
        await connection.execute(
            """UPDATE agent_memory_claim_versions SET system_to = %s
            WHERE partition_key = %s AND claim_key = %s AND system_to IS NULL""",
            (now, partition, key),
        )
        for claim in segments:
            await connection.execute(
                """INSERT INTO agent_memory_claim_versions
                VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, %s::jsonb)""",
                (
                    str(uuid4()),
                    claim.id,
                    partition,
                    key,
                    claim.valid_from,
                    claim.valid_to,
                    now,
                    dump_claim(claim),
                ),
            )

    async def read(self, connection, scope, valid_at, known_at):
        valid_at, known_at = aware(valid_at, "valid_at"), aware(known_at, "known_at")
        where, params = self.repository._visible_scope_clause(scope)
        authorized = f"SELECT id FROM agent_memory_claims WHERE {where} AND archived_at IS NULL"
        cursor = await connection.execute(
            "SELECT MIN(observed_at) AS floor FROM agent_memory_claim_observations "
            f"WHERE legacy AND claim_id IN ({authorized})",
            params,
        )
        row = await cursor.fetchone()
        if row["floor"] and known_at < row["floor"]:
            raise TemporalHistoryUnavailable(
                "knowledge time predates the available temporal history"
            )
        cursor = await connection.execute(
            f"""
            SELECT * FROM agent_memory_claim_versions WHERE claim_id IN ({authorized})
            AND system_from <= %s AND (system_to IS NULL OR %s < system_to)
            AND valid_from <= %s AND (valid_to IS NULL OR %s < valid_to)
            ORDER BY system_from DESC, revision_id
        """,
            (*params, known_at, known_at, valid_at, valid_at),
        )
        claims = []
        for row in await cursor.fetchall():
            claim = load_claim(row["payload_json"])
            cursor = await connection.execute(
                "SELECT id FROM agent_memory_events WHERE id = ANY(%s) AND archived_at IS NULL",
                (list(claim.provenance.source_event_ids),),
            )
            live = {event["id"] for event in await cursor.fetchall()}
            sources = tuple(
                source for source in claim.provenance.source_event_ids if source in live
            )
            if sources:
                claims.append(
                    replace(
                        claim,
                        provenance=replace(claim.provenance, source_event_ids=sources),
                        system_from=row["system_from"],
                        system_to=row["system_to"],
                    )
                )
        return tuple(
            sorted(claims, key=lambda claim: (-claim.importance, -claim.confidence, claim.id))
        )

    async def scrub_sources(self, connection, claim_id, removed):
        for table, identity in (
            ("agent_memory_claim_observations", "claim_id"),
            ("agent_memory_claim_versions", "revision_id"),
        ):
            cursor = await connection.execute(
                f"SELECT {identity}, payload_json FROM {table} WHERE claim_id = %s", (claim_id,)
            )
            for row in await cursor.fetchall():
                claim = load_claim(row["payload_json"])
                sources = tuple(
                    source for source in claim.provenance.source_event_ids if source not in removed
                )
                payload = dump_claim(
                    replace(claim, provenance=replace(claim.provenance, source_event_ids=sources))
                )
                await connection.execute(
                    f"UPDATE {table} SET payload_json = %s::jsonb WHERE {identity} = %s",
                    (payload, row[identity]),
                )
