"""Transactional admission snapshots and deletion fencing for PostgreSQL."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import asdict
from datetime import datetime, timedelta
from itertools import product
from typing import Any

from agent_memory.domain import ForgetMode, MemoryScope, ScopeLevel, utc_now
from agent_memory.serialization import to_jsonable


def _json(value: Any) -> str:
    return json.dumps(to_jsonable(value), ensure_ascii=False, separators=(",", ":"))


async def lock_scope(connection: Any, scope: MemoryScope) -> None:
    # One namespace lock also fences sources promoted out of their session scope.
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (_json(["agent-memory-admission", scope.tenant_id, scope.namespace]),),
    )


async def publication_time(connection: Any, scope: MemoryScope) -> datetime:
    """Allocate one boundary after every committed publication in this namespace."""
    await lock_scope(connection, scope)
    cursor = await connection.execute(
        """SELECT MAX(recorded_at) AS last FROM agent_memory_admission_records
           WHERE scope_json ->> 'tenant_id' = %s AND scope_json ->> 'namespace' = %s""",
        (scope.tenant_id, scope.namespace),
    )
    last = (await cursor.fetchone())["last"]
    return max(utc_now(), last + timedelta(microseconds=1)) if last is not None else utc_now()


async def check_event_identity(
    connection: Any, scope: MemoryScope, event_id: str | None, idempotency_key: str | None
) -> None:
    cursor = await connection.execute(
        """SELECT 1 FROM agent_memory_admission_tombstones
           WHERE event_id = %s OR
           (partition_key = %s AND idempotency_key = %s) LIMIT 1""",
        (event_id, scope.partition_key(), idempotency_key),
    )
    if await cursor.fetchone():
        raise ValueError("deleted source identity cannot be reused")


def _scope_clause(scope: MemoryScope, *, visible: bool, alias: str = "r"):
    if not visible:
        return f"{alias}.partition_key = %s", (scope.partition_key(),)
    fields = asdict(scope)
    clauses = [
        f"{alias}.scope_json ->> 'tenant_id' = %s",
        f"{alias}.scope_json ->> 'namespace' = %s",
    ]
    params: list[Any] = [scope.tenant_id, scope.namespace]
    for key in ("user_id", "agent_id", "workspace_id", "session_id"):
        clauses.append(
            f"({alias}.scope_json ->> '{key}' IS NULL OR {alias}.scope_json ->> '{key}' = %s)"
        )
        params.append(fields[key])
    return " AND ".join(clauses), tuple(params)


def _record(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["record_id"],
        "event_id": row["event_id"],
        "slot_key": row["slot_key"],
        "scope": row["scope_json"],
        "payload": row["payload_json"],
        "version": row["version"],
        "recorded_at": row["recorded_at"].isoformat(),
    }


async def read_records(
    connection: Any,
    scope: MemoryScope,
    *,
    visible: bool,
    record_id: str | None = None,
    slot_key: str | None = None,
    barriers: bool = False,
):
    where, params = _scope_clause(scope, visible=visible)
    for column, value in (("record_id", record_id), ("slot_key", slot_key)):
        if value is not None:
            where += f" AND r.{column} = %s"
            params += (value,)
    visibility = "NOT r.payload_json @> '{\"deleted\": true}'::jsonb"
    if barriers:
        visibility += " OR r.payload_json ? 'contribution_barrier'"
    cursor = await connection.execute(
        f"""SELECT r.* FROM agent_memory_admission_records r WHERE {where}
            AND ({visibility})
            ORDER BY r.record_id LIMIT 1025""",
        params,
    )
    rows = await cursor.fetchall()
    if len(rows) > 1024:
        raise ValueError("admission record limit exceeded")
    return tuple(_record(row) for row in rows)


async def read_versions(connection: Any, scope: MemoryScope, record_id: str):
    where, params = _scope_clause(scope, visible=True)
    cursor = await connection.execute(
        f"""SELECT v.version, v.payload_json, v.recorded_at
            FROM agent_memory_admission_versions v
            JOIN agent_memory_admission_records r USING(record_id)
            WHERE {where} AND r.record_id = %s
              AND NOT r.payload_json @> '{{"deleted": true}}'::jsonb
            ORDER BY v.version""",
        (*params, record_id),
    )
    rows = await cursor.fetchall()
    return tuple(
        {
            "version": row["version"],
            "payload": row["payload_json"],
            "recorded_at": row["recorded_at"].isoformat(),
        }
        for row in rows
    )


async def read_snapshot(connection: Any, scope: MemoryScope) -> tuple[dict[str, Any], ...]:
    """Caller holds a REPEATABLE READ transaction for all current and historical rows."""
    records = await read_records(connection, scope, visible=True, barriers=True)
    if not records:
        return ()
    ids = [record["id"] for record in records]
    cursor = await connection.execute(
        """SELECT record_id, version, payload_json, recorded_at
           FROM agent_memory_admission_versions WHERE record_id = ANY(%s)
           ORDER BY record_id, version""",
        (ids,),
    )
    by_id: dict[str, list[dict[str, Any]]] = {identity: [] for identity in ids}
    for row in await cursor.fetchall():
        by_id[row["record_id"]].append({
            "version": row["version"],
            "payload": row["payload_json"],
            "recorded_at": row["recorded_at"].isoformat(),
        })
    return tuple({**record, "versions": by_id[record["id"]]} for record in records)


async def protected_sources(connection: Any, scope: MemoryScope) -> tuple[str, ...]:
    """ID-only guard input with no truncation, retaining withdrawn source identities."""
    choices = [(None,) if value is None else (None, value) for value in (
        scope.user_id, scope.agent_id, scope.workspace_id, scope.session_id
    )]
    partitions = sorted({
        MemoryScope(scope.tenant_id, scope.namespace, *values).partition_key()
        for values in product(*choices)
    })
    cursor = await connection.execute(
        """SELECT id FROM agent_memory_events
           WHERE partition_key = ANY(%s)
             AND event_type IN ('memory.atom', 'memory.atom.verification')
           UNION SELECT event_id AS id FROM agent_memory_admission_tombstones
           WHERE partition_key = ANY(%s)
           UNION SELECT event_id AS id FROM agent_memory_admission_records
           WHERE partition_key = ANY(%s)""",
        (partitions, partitions, partitions),
    )
    protected = {row["id"] for row in await cursor.fetchall()}
    cursor = await connection.execute(
        "SELECT payload_json FROM agent_memory_admission_records WHERE partition_key = ANY(%s)",
        (partitions,),
    )
    for row in await cursor.fetchall():
        protected.update(_source_ids(row["payload_json"]))
    cursor = await connection.execute(
        """SELECT id, metadata_json FROM agent_memory_events
           WHERE partition_key = ANY(%s)
             AND jsonb_typeof(metadata_json -> 'source_event_ids') = 'array'""",
        (partitions,),
    )
    dependents: dict[str, set[str]] = {}
    for row in await cursor.fetchall():
        for source_id in row["metadata_json"]["source_event_ids"]:
            if isinstance(source_id, str):
                dependents.setdefault(source_id, set()).add(row["id"])
    pending = deque(protected)
    while pending:
        for dependent in dependents.get(pending.popleft(), ()):
            if dependent not in protected:
                protected.add(dependent)
                pending.append(dependent)
    return tuple(sorted(protected))


def _source_ids(payload: dict[str, Any]) -> set[str]:
    ids = payload.get("source_event_ids", [])
    evidence = payload.get("evidence", [])
    if not isinstance(ids, (list, tuple)) or not isinstance(evidence, (list, tuple)):
        raise ValueError("admission sources and evidence must be sequences")
    if any(not isinstance(item, str) or not item for item in ids):
        raise ValueError("admission source IDs must be nonempty strings")
    result = set(ids)
    for item in evidence:
        if not isinstance(item, dict):
            raise ValueError("admission evidence must be objects")
        event_id = item.get("source_event_id")
        if event_id is not None:
            if not isinstance(event_id, str) or not event_id:
                raise ValueError("admission evidence source must be a nonempty string")
            result.add(event_id)
    for transition in payload.get("transitions", []):
        for role in ("end_support", "start_support"):
            support = transition.get(role)
            if support:
                result.add(support["source_event_id"])
        if transition.get("correction"):
            result.add(transition["correction"]["evidence"]["source_event_id"])
    return result


def _projected_source(source: MemoryScope, target: MemoryScope) -> bool:
    if source == target:
        return True
    for level in ScopeLevel:
        try:
            if source.project(level) == target:
                return True
        except ValueError:
            continue
    return False


async def save_record(
    connection: Any,
    scope: MemoryScope,
    record_id: str,
    event_id: str,
    slot_key: str,
    payload: dict[str, Any],
    expected_version: int,
    *,
    recorded_at: datetime | None = None,
) -> int:
    if type(expected_version) is not int or expected_version < 0:
        raise ValueError("expected version must be a nonnegative integer")
    if any(not isinstance(value, str) or not value for value in (record_id, event_id, slot_key)):
        raise ValueError("admission record, event and slot IDs are required")
    if not isinstance(payload, dict) or payload.get("deleted"):
        raise ValueError("invalid admission payload")
    await lock_scope(connection, scope)
    cursor = await connection.execute(
        "SELECT * FROM agent_memory_admission_records WHERE record_id = %s FOR UPDATE",
        (record_id,),
    )
    old = await cursor.fetchone()
    if old and (
        old["partition_key"] != scope.partition_key()
        or old["event_id"] != event_id
        or old["slot_key"] != slot_key
        or old["payload_json"].get("deleted")
    ):
        raise ValueError("admission record identity is unavailable")
    if (old["version"] if old else 0) != expected_version:
        raise ValueError("admission record version conflict")
    source_ids = _source_ids(payload) | {event_id}
    if len(source_ids) > 1024:
        raise ValueError("admission source limit exceeded")
    cursor = await connection.execute(
        "SELECT * FROM agent_memory_events WHERE id = ANY(%s) AND archived_at IS NULL",
        (sorted(source_ids),),
    )
    sources = await cursor.fetchall()
    if len(sources) != len(source_ids):
        raise ValueError("source evidence is missing or archived")
    scope_fields = tuple(asdict(scope))
    for row in sources:
        source_scope = MemoryScope(**{key: row[key] for key in scope_fields})
        if not _projected_source(source_scope, scope):
            raise ValueError("source evidence is outside the authorized scope")
        await check_event_identity(connection, source_scope, row["id"], row["idempotency_key"])
    claim_id = payload.get("claim_id")
    if claim_id is not None:
        if not isinstance(claim_id, str) or not claim_id:
            raise ValueError("admission Claim identity must be a nonempty string")
        cursor = await connection.execute(
            """SELECT 1 FROM agent_memory_claims
               WHERE id=%s AND partition_key=%s AND archived_at IS NULL""",
            (claim_id, scope.partition_key()),
        )
        if not await cursor.fetchone():
            raise ValueError("admission Claim is missing or outside the authorized scope")
    now = recorded_at if recorded_at is not None else await publication_time(connection, scope)
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError("admission publication time must include a timezone")
    if old is not None and now < old["recorded_at"]:
        raise ValueError("admission publication time precedes an existing version")
    version = expected_version + 1
    encoded = _json(payload)
    if old:
        cursor = await connection.execute(
            """UPDATE agent_memory_admission_records SET payload_json=%s::jsonb,
                version=%s, recorded_at=%s
                WHERE record_id=%s AND partition_key=%s AND version=%s""",
            (encoded, version, now, record_id, scope.partition_key(), expected_version),
        )
    else:
        cursor = await connection.execute(
            """INSERT INTO agent_memory_admission_records
                (record_id, partition_key, event_id, slot_key, scope_json,
                 payload_json, version, recorded_at)
                VALUES (%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s)
                ON CONFLICT(record_id) DO NOTHING""",
            (
                record_id,
                scope.partition_key(),
                event_id,
                slot_key,
                _json(scope),
                encoded,
                version,
                now,
            ),
        )
    if cursor.rowcount != 1:
        raise ValueError("admission record version conflict")
    await connection.execute(
        """INSERT INTO agent_memory_admission_versions
            (record_id, version, partition_key, payload_json, recorded_at)
            VALUES (%s,%s,%s,%s::jsonb,%s)""",
        (record_id, version, scope.partition_key(), encoded, now),
    )
    return version


async def forget_records(connection: Any, request: Any) -> tuple[set[str], int]:
    """Fence old source identities and clear all matching admission snapshots.

    Returns dependent Claim IDs and their count when outside the direct request.
    Source dependencies may cross upward scope projections within this namespace.
    """
    await lock_scope(connection, request.scope)
    direct_where = "partition_key = %s"
    direct_params: tuple[Any, ...] = (request.scope.partition_key(),)
    if not request.all_in_scope:
        direct_where += " AND id = ANY(%s)"
        direct_params += (list(request.memory_ids),)
    cursor = await connection.execute(
        f"SELECT id, idempotency_key FROM agent_memory_events WHERE {direct_where}",
        direct_params,
    )
    events = await cursor.fetchall()
    source_ids = {row["id"] for row in events}
    for row in events:
        await connection.execute(
            """INSERT INTO agent_memory_admission_tombstones
                (partition_key, event_id, idempotency_key) VALUES (%s,%s,%s)
                ON CONFLICT(partition_key,event_id) DO NOTHING""",
            (request.scope.partition_key(), row["id"], row["idempotency_key"]),
        )
    cursor = await connection.execute(
        f"SELECT id FROM agent_memory_claims WHERE {direct_where}",
        direct_params,
    )
    direct_claim_ids = {row["id"] for row in await cursor.fetchall()}
    dependent_claim_ids: set[str] = set()
    withdrawn_slots: set[tuple[str, str]] = set()
    extra_claim_count = 0
    after_id = ""
    scan_changed = False
    deleted_at: datetime | None = None
    while True:
        cursor = await connection.execute(
            """SELECT * FROM agent_memory_admission_records
               WHERE scope_json ->> 'tenant_id' = %s AND scope_json ->> 'namespace' = %s
                 AND record_id > %s AND NOT payload_json @> '{"deleted": true}'::jsonb
               ORDER BY record_id LIMIT 128 FOR UPDATE""",
            (request.scope.tenant_id, request.scope.namespace, after_id),
        )
        rows = await cursor.fetchall()
        if not rows:
            if scan_changed:
                after_id, scan_changed = "", False
                continue
            break
        for row in rows:
            cursor = await connection.execute(
                "SELECT version,payload_json FROM agent_memory_admission_versions WHERE record_id = %s",
                (row["record_id"],),
            )
            versions = await cursor.fetchall()
            from agent_memory.contribution_state import erased_payload, scrub_transitions
            from agent_memory.evidence_support import scrub_support

            def scrub(payload):
                first = scrub_support(payload, source_ids)
                return scrub_transitions(payload, source_ids) or first

            if (
                row["payload_json"].get("qualification") or row["payload_json"].get("transitions")
            ) and row["event_id"] not in source_ids:
                changed = scrub(row["payload_json"])
                for version in versions:
                    if scrub(version["payload_json"]):
                        changed = True
                        await connection.execute(
                            "UPDATE agent_memory_admission_versions SET payload_json=%s::jsonb "
                            "WHERE record_id=%s AND version=%s",
                            (_json(version["payload_json"]), row["record_id"], version["version"]),
                        )
                if changed:
                    deleted_at = deleted_at or await publication_time(connection, request.scope)
                    row["version"] += 1
                    row["recorded_at"] = deleted_at
                    await connection.execute(
                        "UPDATE agent_memory_admission_records "
                        "SET payload_json=%s::jsonb,version=%s,recorded_at=%s WHERE record_id=%s",
                        (_json(row["payload_json"]), row["version"], deleted_at, row["record_id"]),
                    )
                    await connection.execute(
                        "INSERT INTO agent_memory_admission_versions "
                        "(record_id,version,partition_key,payload_json,recorded_at) "
                        "VALUES (%s,%s,%s,%s::jsonb,%s)",
                        (
                            row["record_id"],
                            row["version"],
                            row["partition_key"],
                            _json(row["payload_json"]),
                            deleted_at,
                        ),
                    )
            payloads = [row["payload_json"], *(item["payload_json"] for item in versions)]
            refs = {row["event_id"]}
            claim_ids: set[str] = set()
            for payload in payloads:
                refs.update(_source_ids(payload))
                if payload.get("claim_id"):
                    claim_ids.add(payload["claim_id"])
            directly_selected = row["partition_key"] == request.scope.partition_key() and (
                request.all_in_scope or row["record_id"] in request.memory_ids
            )
            slot = (row["partition_key"], row["slot_key"])
            if not (
                directly_selected
                or refs & source_ids
                or claim_ids & (direct_claim_ids | dependent_claim_ids)
                or slot in withdrawn_slots
            ):
                continue
            scan_changed = True
            # Withdrawing a newer assertion must not resurrect an older value.
            # Until a fresh source rebuilds it, withdraw the whole typed timeline.
            if not row["payload_json"].get("contribution"):
                withdrawn_slots.add(slot)
            dependent_claim_ids.update(claim_ids)
            if deleted_at is None:
                deleted_at = await publication_time(connection, request.scope)
            await connection.execute(
                "DELETE FROM agent_memory_admission_versions WHERE record_id = %s",
                (row["record_id"],),
            )
            await connection.execute(
                """UPDATE agent_memory_admission_records
                    SET payload_json=%s::jsonb, version=version+1,
                        recorded_at=%s
                    WHERE record_id=%s""",
                (_json(erased_payload(row["payload_json"])), deleted_at, row["record_id"]),
            )
            from . import index

            await index.invalidate_records(connection, (row["record_id"],))
        after_id = rows[-1]["record_id"]
    extra_ids = dependent_claim_ids - direct_claim_ids
    if extra_ids:
        cursor = await connection.execute(
            "SELECT id, partition_key FROM agent_memory_claims WHERE id=ANY(%s) FOR UPDATE",
            (sorted(extra_ids),),
        )
        extra_rows = await cursor.fetchall()
        extra_claim_count = len(extra_rows)
        if request.mode == ForgetMode.ERASE:
            await connection.execute(
                "DELETE FROM agent_memory_claims WHERE id=ANY(%s)", (sorted(extra_ids),)
            )
        else:
            await connection.execute(
                """UPDATE agent_memory_claims SET status='archived', archived_at=now()
                    WHERE id=ANY(%s)""",
                (sorted(extra_ids),),
            )
    return dependent_claim_ids, extra_claim_count
