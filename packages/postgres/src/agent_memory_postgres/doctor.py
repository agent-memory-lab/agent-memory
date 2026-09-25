"""Bounded, read-only diagnostics for the optional PostgreSQL provider."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from hashlib import sha256
from typing import Any

from agent_memory import (
    SCHEMA_VERSION,
    MemoryDoctorCode,
    MemoryDoctorFinding,
    MemoryDoctorLimits,
    MemoryDoctorReport,
    MemoryDoctorSeverity,
    MemoryDoctorStatus,
    MemoryScope,
)
from agent_memory.domain import utc_now


class PostgresMemoryDoctor:
    """Inspect a provider pool without changing memory or queue state."""

    def __init__(
        self,
        pool: Any,
        *,
        limits: MemoryDoctorLimits | None = None,
        statement_timeout_ms: int = 5_000,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if pool is None or not hasattr(pool, "connection"):
            raise TypeError("pool must provide connection()")
        if type(statement_timeout_ms) is not int or not 100 <= statement_timeout_ms <= 60_000:
            raise ValueError("statement_timeout_ms must be between 100 and 60000")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._pool = pool
        self._limits = limits or MemoryDoctorLimits()
        self._statement_timeout_ms = statement_timeout_ms
        self._clock = clock

    async def inspect(self, scope: MemoryScope | None = None) -> MemoryDoctorReport:
        if scope is not None and not isinstance(scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope or None")
        checked_at = self._clock()
        if checked_at.tzinfo is None:
            raise ValueError("doctor clock must return a timezone-aware datetime")
        partition = scope.partition_key() if scope is not None else None
        findings: list[MemoryDoctorFinding] = []
        counts: dict[str, int] = {}
        schema_version: int | None = None
        database_bytes = 0
        database_identity = "postgresql"

        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute("SET TRANSACTION READ ONLY")
                await connection.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (f"{self._statement_timeout_ms}ms",),
                )
                identity = await _one(
                    connection,
                    "SELECT current_database() AS database_name, current_schema() AS schema_name, "
                    "pg_database_size(current_database()) AS database_bytes",
                )
                database_identity = f"{identity['database_name']}:{identity['schema_name']}"
                database_bytes = int(identity["database_bytes"])
                if database_bytes > self._limits.max_database_bytes:
                    findings.append(
                        _finding(
                            MemoryDoctorCode.DATABASE_CAPACITY,
                            MemoryDoctorSeverity.WARNING,
                            1,
                            "database size exceeds the configured diagnostic budget",
                            "Archive data using the host retention policy.",
                        )
                    )

                tables = {
                    row["table_name"]
                    for row in await _all(
                        connection,
                        "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema=current_schema() AND table_name LIKE %s",
                    ("agent_memory_%",),
                    )
                }
                required = {
                    "agent_memory_schema",
                    "agent_memory_events",
                    "agent_memory_claims",
                    "agent_memory_state_deltas",
                    "agent_memory_artifacts",
                    "agent_memory_evolution_records",
                    "agent_memory_proposals",
                    "agent_memory_consolidation_jobs",
                }
                missing = sorted(required - tables)
                if missing:
                    findings.append(
                        _finding(
                            MemoryDoctorCode.SCHEMA_VERSION,
                            MemoryDoctorSeverity.CRITICAL,
                            len(missing),
                            "required PostgreSQL memory tables are missing",
                            "Run the supported provider migrations before use.",
                        )
                    )
                else:
                    row = await _one(
                        connection,
                        "SELECT max(schema_version) AS version FROM agent_memory_schema",
                    )
                    schema_version = int(row["version"]) if row["version"] is not None else None
                    if schema_version != SCHEMA_VERSION:
                        findings.append(
                            _finding(
                                MemoryDoctorCode.SCHEMA_VERSION,
                                MemoryDoctorSeverity.CRITICAL,
                                1,
                                "database schema version does not match the running core",
                                "Back up and migrate with the matching provider version.",
                            )
                        )
                    await self._inspect_core(connection, partition, counts, findings, checked_at)

        findings.sort(key=lambda item: (-_severity_rank(item.severity), item.code.value))
        status = MemoryDoctorStatus.HEALTHY
        if any(item.severity is MemoryDoctorSeverity.CRITICAL for item in findings):
            status = MemoryDoctorStatus.CRITICAL
        elif findings:
            status = MemoryDoctorStatus.DEGRADED
        return MemoryDoctorReport(
            checked_at=checked_at,
            status=status,
            database_fingerprint=_fingerprint(database_identity),
            scope_fingerprint=_fingerprint(partition) if partition is not None else None,
            schema_version=schema_version,
            database_bytes=database_bytes,
            counts=counts,
            findings=tuple(findings),
        )

    async def _inspect_core(
        self,
        connection: Any,
        partition: str | None,
        counts: dict[str, int],
        findings: list[MemoryDoctorFinding],
        checked_at: datetime,
    ) -> None:
        where, params = _scope_clause(partition)
        for name in ("events", "claims", "state_deltas", "artifacts", "evolution_records", "proposals"):
            row = await _one(
                connection,
                f"SELECT count(*) AS count FROM agent_memory_{name} {where}",
                params,
            )
            counts[name] = int(row["count"])

        index = await _one(
            connection,
            "SELECT to_regclass('agent_memory_claims_one_active_idx') AS index_name",
        )
        if index["index_name"] is None:
            findings.append(
                _finding(
                    MemoryDoctorCode.SCHEMA_INDEX_MISSING,
                    MemoryDoctorSeverity.CRITICAL,
                    1,
                    "the unique current-claim index is missing",
                    "Back up and run the supported provider migration.",
                )
            )

        duplicate_where = "WHERE status='active' AND archived_at IS NULL"
        duplicate_params: tuple[Any, ...] = ()
        if partition is not None:
            duplicate_where += " AND partition_key=%s"
            duplicate_params = (partition,)
        duplicates = await _all(
            connection,
            f"SELECT partition_key, claim_key FROM agent_memory_claims {duplicate_where} "
            "GROUP BY partition_key, claim_key HAVING count(*) > 1 LIMIT %s",
            (*duplicate_params, self._limits.max_rows + 1),
        )
        if duplicates:
            findings.append(
                _finding(
                    MemoryDoctorCode.DUPLICATE_ACTIVE_CLAIM,
                    MemoryDoctorSeverity.CRITICAL,
                    len(duplicates),
                    "multiple active claims exist for the same scoped key",
                    "Resolve the winning version and restore the unique index.",
                    _samples(
                        (f"{row['partition_key']}:{row['claim_key']}" for row in duplicates),
                        self._limits.max_samples,
                    ),
                )
            )

        invalid = await _all(
            connection,
            "SELECT c.id FROM agent_memory_claims c "
            "LEFT JOIN agent_memory_claims successor ON successor.id=c.superseded_by "
            "LEFT JOIN agent_memory_claims predecessor ON predecessor.id=c.supersedes "
            "WHERE ((c.status='superseded' AND (c.superseded_by IS NULL OR successor.id IS NULL)) "
            "OR (c.status='active' AND c.superseded_by IS NOT NULL) "
            "OR (c.supersedes IS NOT NULL AND predecessor.id IS NULL)) "
            + ("AND c.partition_key=%s " if partition is not None else "")
            + "LIMIT %s",
            ((*params, self._limits.max_rows + 1) if partition is not None else (self._limits.max_rows + 1,)),
        )
        if invalid:
            findings.append(
                _finding(
                    MemoryDoctorCode.CLAIM_SUPERSESSION_INVALID,
                    MemoryDoctorSeverity.ERROR,
                    len(invalid),
                    "claim supersession links or statuses are inconsistent",
                    "Reconstruct claim lineage from state deltas and evidence.",
                    _samples((row["id"] for row in invalid), self._limits.max_samples),
                )
            )

        await self._inspect_provenance(connection, partition, findings)
        await self._inspect_jobs(connection, partition, findings, counts, checked_at)

    async def _inspect_provenance(
        self,
        connection: Any,
        partition: str | None,
        findings: list[MemoryDoctorFinding],
    ) -> None:
        where, params = _scope_clause(partition)
        claim_rows = await _all(
            connection,
            f"SELECT id, partition_key, provenance_json FROM agent_memory_claims {where} LIMIT %s",
            (*params, self._limits.max_rows + 1),
        )
        artifact_rows = await _all(
            connection,
            f"SELECT id, partition_key, provenance_json FROM agent_memory_artifacts {where} LIMIT %s",
            (*params, self._limits.max_rows + 1),
        )
        if len(claim_rows) > self._limits.max_rows or len(artifact_rows) > self._limits.max_rows:
            findings.append(
                _finding(
                    MemoryDoctorCode.INSPECTION_TRUNCATED,
                    MemoryDoctorSeverity.WARNING,
                    1,
                    "provenance inspection reached its configured row limit",
                    "Run partitioned diagnostics or raise the explicit row budget.",
                )
            )

        malformed: list[str] = []
        unsupported_claims: list[str] = []
        unverifiable_artifacts: list[str] = []
        candidates: list[tuple[str, str, tuple[str, ...], bool]] = []
        for row, artifact in (
            *((row, False) for row in claim_rows[: self._limits.max_rows]),
            *((row, True) for row in artifact_rows[: self._limits.max_rows]),
        ):
            sources = _source_ids(row["provenance_json"])
            if sources is None:
                malformed.append(row["id"])
            elif not sources:
                (unverifiable_artifacts if artifact else unsupported_claims).append(row["id"])
            else:
                candidates.append((row["id"], row["partition_key"], sources, artifact))

        source_pairs = {
            (partition_key, source_id)
            for _, partition_key, sources, _ in candidates
            for source_id in sources
        }
        existing: set[tuple[str, str]] = set()
        for partition_key, event_id in source_pairs:
            row = await _maybe_one(
                connection,
                "SELECT partition_key, id FROM agent_memory_events "
                "WHERE partition_key=%s AND id=%s",
                (partition_key, event_id),
            )
            if row is not None:
                existing.add((row["partition_key"], row["id"]))
        for item_id, partition_key, sources, artifact in candidates:
            if any((partition_key, source_id) not in existing for source_id in sources):
                (unverifiable_artifacts if artifact else unsupported_claims).append(item_id)

        if malformed:
            findings.append(
                _finding(
                    MemoryDoctorCode.MALFORMED_PROVENANCE,
                    MemoryDoctorSeverity.ERROR,
                    len(malformed),
                    "stored provenance is not valid structured JSON",
                    "Restore malformed rows from a verified backup.",
                    _samples(malformed, self._limits.max_samples),
                )
            )
        if unsupported_claims:
            findings.append(
                _finding(
                    MemoryDoctorCode.CLAIM_SOURCE_MISSING,
                    MemoryDoctorSeverity.ERROR,
                    len(unsupported_claims),
                    "claims exist without retained source evidence",
                    "Archive unsupported claims after host review.",
                    _samples(unsupported_claims, self._limits.max_samples),
                    repairable=True,
                )
            )
        if unverifiable_artifacts:
            findings.append(
                _finding(
                    MemoryDoctorCode.ARTIFACT_SOURCE_MISSING,
                    MemoryDoctorSeverity.ERROR,
                    len(unverifiable_artifacts),
                    "derived artifacts have missing or empty source evidence",
                    "Archive unverifiable artifacts and regenerate from retained evidence.",
                    _samples(unverifiable_artifacts, self._limits.max_samples),
                    repairable=True,
                )
            )

    async def _inspect_jobs(
        self,
        connection: Any,
        partition: str | None,
        findings: list[MemoryDoctorFinding],
        counts: dict[str, int],
        checked_at: datetime,
    ) -> None:
        where, params = _scope_clause(partition)
        rows = await _all(
            connection,
            f"SELECT id, partition_key, status, payload_json, lease_expires_at "
            f"FROM agent_memory_consolidation_jobs {where} LIMIT %s",
            (*params, self._limits.max_rows + 1),
        )
        for status in ("pending", "running", "completed", "dead"):
            status_where = f"{where} {'AND' if where else 'WHERE'} status=%s"
            row = await _one(
                connection,
                f"SELECT count(*) AS count FROM agent_memory_consolidation_jobs {status_where}",
                (*params, status),
            )
            counts[f"worker_{status}"] = int(row["count"])
        if len(rows) > self._limits.max_rows:
            findings.append(
                _finding(
                    MemoryDoctorCode.INSPECTION_TRUNCATED,
                    MemoryDoctorSeverity.WARNING,
                    1,
                    "worker inspection reached its configured row limit",
                    "Run partitioned diagnostics or raise the explicit row budget.",
                )
            )
        selected = rows[: self._limits.max_rows]
        dead = [row["id"] for row in selected if row["status"] == "dead"]
        stale = [
            row["id"]
            for row in selected
            if row["status"] == "running"
            and row["lease_expires_at"] is not None
            and row["lease_expires_at"] <= checked_at
        ]
        malformed: list[str] = []
        missing: list[str] = []
        for row in selected:
            if row["status"] not in {"pending", "running"}:
                continue
            payload = row["payload_json"]
            if not isinstance(payload, Mapping):
                malformed.append(row["id"])
                continue
            event_id = payload.get("event_id")
            if event_id is not None and (not isinstance(event_id, str) or not event_id):
                malformed.append(row["id"])
            elif event_id and await _maybe_one(
                connection,
                "SELECT id FROM agent_memory_events WHERE partition_key=%s AND id=%s",
                (row["partition_key"], event_id),
            ) is None:
                missing.append(row["id"])
        for values, code, severity, message, recommendation, repairable in (
            (dead, MemoryDoctorCode.DEAD_WORKER_TASK, MemoryDoctorSeverity.WARNING,
             "worker tasks exhausted their retry budget",
             "Review failures before retrying or archiving dead tasks.", True),
            (stale, MemoryDoctorCode.STALE_WORKER_LEASE, MemoryDoctorSeverity.WARNING,
             "worker leases expired without completion",
             "Release expired leases through the queue recovery path.", True),
            (malformed, MemoryDoctorCode.WORKER_PAYLOAD_INVALID, MemoryDoctorSeverity.ERROR,
             "active worker tasks contain malformed payloads",
             "Cancel malformed tasks after preserving queue evidence.", False),
            (missing, MemoryDoctorCode.WORKER_SOURCE_MISSING, MemoryDoctorSeverity.ERROR,
             "active worker tasks reference deleted events",
             "Cancel source-less tasks to prevent memory regeneration.", True),
        ):
            if values:
                findings.append(
                    _finding(
                        code, severity, len(values), message, recommendation,
                        _samples(values, self._limits.max_samples), repairable=repairable,
                    )
                )
        active = counts["worker_pending"] + counts["worker_running"]
        terminal = counts["worker_completed"] + counts["worker_dead"]
        if active > self._limits.max_pending_tasks or terminal > self._limits.max_terminal_tasks:
            findings.append(
                _finding(
                    MemoryDoctorCode.WORKER_CAPACITY,
                    MemoryDoctorSeverity.WARNING,
                    1,
                    "worker queue exceeds configured diagnostic capacity",
                    "Apply bounded retention and host-controlled backpressure.",
                )
            )


async def _all(connection: Any, query: str, params: tuple[Any, ...] = ()) -> list[Mapping[str, Any]]:
    cursor = await connection.execute(query, params)
    return list(await cursor.fetchall())


async def _one(connection: Any, query: str, params: tuple[Any, ...] = ()) -> Mapping[str, Any]:
    row = await _maybe_one(connection, query, params)
    if row is None:
        raise RuntimeError("diagnostic query returned no row")
    return row


async def _maybe_one(
    connection: Any, query: str, params: tuple[Any, ...] = ()
) -> Mapping[str, Any] | None:
    cursor = await connection.execute(query, params)
    return await cursor.fetchone()


def _scope_clause(partition: str | None) -> tuple[str, tuple[Any, ...]]:
    return ("WHERE partition_key=%s", (partition,)) if partition is not None else ("", ())


def _source_ids(value: Any) -> tuple[str, ...] | None:
    if not isinstance(value, Mapping):
        return None
    sources = value.get("source_event_ids")
    if not isinstance(sources, list) or any(
        not isinstance(item, str) or not item for item in sources
    ):
        return None
    return tuple(sources)


def _finding(
    code: MemoryDoctorCode,
    severity: MemoryDoctorSeverity,
    count: int,
    message: str,
    recommendation: str,
    samples: tuple[str, ...] = (),
    *,
    repairable: bool = False,
) -> MemoryDoctorFinding:
    return MemoryDoctorFinding(
        code, severity, count, message, recommendation, samples, repairable
    )


def _fingerprint(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:12]


def _samples(values: Any, maximum: int) -> tuple[str, ...]:
    return tuple(_fingerprint(str(value)) for value in list(values)[:maximum])


def _severity_rank(severity: MemoryDoctorSeverity) -> int:
    return {
        MemoryDoctorSeverity.WARNING: 1,
        MemoryDoctorSeverity.ERROR: 2,
        MemoryDoctorSeverity.CRITICAL: 3,
    }[severity]
