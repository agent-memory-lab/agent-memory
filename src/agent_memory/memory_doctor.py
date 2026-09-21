"""Read-only, privacy-safe diagnostics for the default SQLite memory store."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from .domain import SCHEMA_VERSION, MemoryScope, utc_now
from .serialization import to_jsonable


class MemoryDoctorSeverity(StrEnum):
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class MemoryDoctorStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"


class MemoryDoctorCode(StrEnum):
    DATABASE_INTEGRITY = "database-integrity"
    SCHEMA_VERSION = "schema-version"
    SCHEMA_INDEX_MISSING = "schema-index-missing"
    DATABASE_CAPACITY = "database-capacity"
    DUPLICATE_ACTIVE_CLAIM = "duplicate-active-claim"
    ORPHAN_CLAIM_SOURCE = "orphan-claim-source"
    CLAIM_SOURCE_MISSING = "claim-source-missing"
    CLAIM_SOURCE_MISMATCH = "claim-source-mismatch"
    CLAIM_SUPERSESSION_INVALID = "claim-supersession-invalid"
    ARTIFACT_SOURCE_MISSING = "artifact-source-missing"
    MALFORMED_PROVENANCE = "malformed-provenance"
    INSPECTION_TRUNCATED = "inspection-truncated"
    DEAD_WORKER_TASK = "dead-worker-task"
    STALE_WORKER_LEASE = "stale-worker-lease"
    WORKER_SOURCE_MISSING = "worker-source-missing"
    WORKER_PAYLOAD_INVALID = "worker-payload-invalid"
    WORKER_CAPACITY = "worker-capacity"


@dataclass(frozen=True, slots=True)
class MemoryDoctorLimits:
    max_rows: int = 10_000
    max_samples: int = 8
    max_database_bytes: int = 512 * 1024 * 1024
    max_pending_tasks: int = 1_000
    max_terminal_tasks: int = 10_000

    def __post_init__(self) -> None:
        for name, value, maximum in (
            ("max_rows", self.max_rows, 1_000_000),
            ("max_samples", self.max_samples, 100),
            ("max_database_bytes", self.max_database_bytes, 10**13),
            ("max_pending_tasks", self.max_pending_tasks, 10_000_000),
            ("max_terminal_tasks", self.max_terminal_tasks, 100_000_000),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{name} must be between 1 and {maximum}")


@dataclass(frozen=True, slots=True)
class MemoryDoctorFinding:
    code: MemoryDoctorCode
    severity: MemoryDoctorSeverity
    count: int
    message: str
    recommendation: str
    sample_fingerprints: tuple[str, ...] = ()
    repairable: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", MemoryDoctorCode(self.code))
        object.__setattr__(self, "severity", MemoryDoctorSeverity(self.severity))
        if type(self.count) is not int or self.count < 1:
            raise ValueError("finding count must be positive")
        if not self.message.strip() or not self.recommendation.strip():
            raise ValueError("finding message and recommendation must not be empty")
        samples = tuple(self.sample_fingerprints)
        if any(len(value) != 12 for value in samples):
            raise ValueError("sample fingerprints must contain 12 characters")
        object.__setattr__(self, "sample_fingerprints", samples)


@dataclass(frozen=True, slots=True)
class MemoryDoctorReport:
    checked_at: datetime
    status: MemoryDoctorStatus
    database_fingerprint: str
    scope_fingerprint: str | None
    schema_version: int | None
    database_bytes: int
    counts: Mapping[str, int]
    findings: tuple[MemoryDoctorFinding, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", MemoryDoctorStatus(self.status))
        object.__setattr__(self, "counts", MappingProxyType(dict(self.counts)))
        object.__setattr__(self, "findings", tuple(self.findings))

    def to_json(self) -> str:
        return json.dumps(
            to_jsonable(self),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def to_markdown(self) -> str:
        lines = [
            "# Agent Memory Doctor",
            "",
            f"Status: **{self.status.value.upper()}**",
            f"Schema: {self.schema_version if self.schema_version is not None else 'unknown'}",
            f"Database bytes: {self.database_bytes}",
            "",
        ]
        if not self.findings:
            lines.append("No findings.")
        else:
            lines.extend(
                f"- [{item.severity.value}] {item.code.value}: {item.message} "
                f"(count={item.count})"
                for item in self.findings
            )
        return "\n".join(lines) + "\n"


@dataclass(frozen=True, slots=True)
class MemoryRepairAction:
    action_id: str
    source_code: MemoryDoctorCode
    description: str
    affected_count: int
    requires_backup: bool = True
    requires_human_approval: bool = True


@dataclass(frozen=True, slots=True)
class MemoryRepairPlan:
    generated_at: datetime
    database_fingerprint: str
    actions: tuple[MemoryRepairAction, ...]


@runtime_checkable
class MemoryDoctorProvider(Protocol):
    """Optional, storage-neutral boundary for scoped read-only diagnostics."""

    async def inspect(self, scope: MemoryScope | None = None) -> MemoryDoctorReport: ...


_REPAIR_DESCRIPTIONS = {
    MemoryDoctorCode.ORPHAN_CLAIM_SOURCE: "Remove orphan claim-source links after backup.",
    MemoryDoctorCode.CLAIM_SOURCE_MISSING: "Archive unsupported claims after host review.",
    MemoryDoctorCode.CLAIM_SOURCE_MISMATCH: "Rebuild claim provenance from evidence links.",
    MemoryDoctorCode.ARTIFACT_SOURCE_MISSING: "Archive derived artifacts with missing evidence.",
    MemoryDoctorCode.DEAD_WORKER_TASK: "Archive or retry dead tasks using host policy.",
    MemoryDoctorCode.STALE_WORKER_LEASE: "Release expired leases back to the pending queue.",
    MemoryDoctorCode.WORKER_SOURCE_MISSING: "Cancel tasks whose source evidence no longer exists.",
}


def build_memory_repair_plan(report: MemoryDoctorReport) -> MemoryRepairPlan:
    if not isinstance(report, MemoryDoctorReport):
        raise TypeError("report must be a MemoryDoctorReport")
    actions = tuple(
        MemoryRepairAction(
            action_id=f"repair-{finding.code.value}",
            source_code=finding.code,
            description=_REPAIR_DESCRIPTIONS[finding.code],
            affected_count=finding.count,
        )
        for finding in report.findings
        if finding.repairable and finding.code in _REPAIR_DESCRIPTIONS
    )
    return MemoryRepairPlan(report.checked_at, report.database_fingerprint, actions)


class SQLiteMemoryDoctor:
    """Inspect SQLite state without acquiring write locks or changing data."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        worker_path: str | Path | None = None,
        limits: MemoryDoctorLimits | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._database_path = Path(database_path)
        self._worker_path = Path(worker_path) if worker_path is not None else None
        self._limits = limits or MemoryDoctorLimits()
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._clock = clock

    async def inspect(self, scope: MemoryScope | None = None) -> MemoryDoctorReport:
        if scope is not None and not isinstance(scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope or None")
        return await asyncio.to_thread(self._inspect_sync, scope)

    def _inspect_sync(self, scope: MemoryScope | None) -> MemoryDoctorReport:
        checked_at = self._clock()
        if checked_at.tzinfo is None:
            raise ValueError("doctor clock must return a timezone-aware datetime")
        if not self._database_path.is_file():
            raise FileNotFoundError(self._database_path)
        partition = scope.partition_key() if scope is not None else None
        findings: list[MemoryDoctorFinding] = []
        counts: dict[str, int] = {}
        schema_version: int | None = None
        database_bytes = _database_bytes(self._database_path)
        if database_bytes > self._limits.max_database_bytes:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.DATABASE_CAPACITY,
                    MemoryDoctorSeverity.WARNING,
                    1,
                    "database size exceeds the configured diagnostic budget",
                    "Archive or compact data using host retention policy.",
                )
            )

        with _read_only_connection(self._database_path) as connection:
            findings.extend(self._integrity_findings(connection))
            tables = _table_names(connection)
            required = {
                "memory_schema",
                "events",
                "claims",
                "claim_sources",
                "state_deltas",
                "artifacts",
                "evolution_records",
                "proposals",
            }
            missing_tables = sorted(required - tables)
            if missing_tables:
                findings.append(
                    MemoryDoctorFinding(
                        MemoryDoctorCode.SCHEMA_VERSION,
                        MemoryDoctorSeverity.CRITICAL,
                        len(missing_tables),
                        "required memory tables are missing",
                        "Run the supported schema migration before using this database.",
                    )
                )
            else:
                row = connection.execute(
                    "SELECT MAX(schema_version) AS version FROM memory_schema"
                ).fetchone()
                schema_version = int(row["version"]) if row and row["version"] is not None else None
                if schema_version != SCHEMA_VERSION:
                    findings.append(
                        MemoryDoctorFinding(
                            MemoryDoctorCode.SCHEMA_VERSION,
                            MemoryDoctorSeverity.CRITICAL,
                            1,
                            "database schema version does not match the running core",
                            "Back up and migrate the database with the matching core version.",
                        )
                    )
                for table in (
                    "events",
                    "claims",
                    "state_deltas",
                    "artifacts",
                    "evolution_records",
                    "proposals",
                ):
                    counts[table] = _scoped_count(connection, table, partition)
                findings.extend(self._core_findings(connection, partition))

        if self._worker_path is not None and self._worker_path.is_file():
            with _read_only_connection(self._worker_path) as worker_connection:
                if "agent_memory_worker_tasks" in _table_names(worker_connection):
                    worker_findings, worker_counts = self._worker_findings(
                        worker_connection,
                        partition,
                        checked_at,
                    )
                    findings.extend(worker_findings)
                    counts.update(worker_counts)

        findings.sort(key=lambda item: (-_severity_rank(item.severity), item.code.value))
        status = MemoryDoctorStatus.HEALTHY
        if any(item.severity is MemoryDoctorSeverity.CRITICAL for item in findings):
            status = MemoryDoctorStatus.CRITICAL
        elif findings:
            status = MemoryDoctorStatus.DEGRADED
        return MemoryDoctorReport(
            checked_at=checked_at,
            status=status,
            database_fingerprint=_fingerprint(str(self._database_path.resolve())),
            scope_fingerprint=_fingerprint(partition) if partition is not None else None,
            schema_version=schema_version,
            database_bytes=database_bytes,
            counts=counts,
            findings=tuple(findings),
        )

    def _integrity_findings(
        self,
        connection: sqlite3.Connection,
    ) -> list[MemoryDoctorFinding]:
        findings: list[MemoryDoctorFinding] = []
        quick = connection.execute("PRAGMA quick_check(1)").fetchone()
        if quick is None or quick[0] != "ok":
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.DATABASE_INTEGRITY,
                    MemoryDoctorSeverity.CRITICAL,
                    1,
                    "SQLite quick_check failed",
                    "Stop writes, back up the file, and restore from a verified copy.",
                )
            )
        return findings

    def _core_findings(
        self,
        connection: sqlite3.Connection,
        partition: str | None,
    ) -> list[MemoryDoctorFinding]:
        findings: list[MemoryDoctorFinding] = []
        index = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='claims_current_idx'"
        ).fetchone()
        if index is None:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.SCHEMA_INDEX_MISSING,
                    MemoryDoctorSeverity.CRITICAL,
                    1,
                    "the unique current-claim index is missing",
                    "Back up and run the supported schema migration.",
                )
            )

        params: tuple[Any, ...] = (partition,) if partition is not None else ()
        partition_where = "AND partition_key=?" if partition is not None else ""
        duplicate_rows = connection.execute(
            f"""
            SELECT partition_key, claim_key, COUNT(*) AS count
            FROM claims WHERE status='active' {partition_where}
            GROUP BY partition_key, claim_key HAVING COUNT(*) > 1
            """,
            params,
        ).fetchall()
        if duplicate_rows:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.DUPLICATE_ACTIVE_CLAIM,
                    MemoryDoctorSeverity.CRITICAL,
                    len(duplicate_rows),
                    "multiple active claims exist for the same scoped key",
                    "Resolve the winning version and restore the unique index.",
                    _samples(
                        (f"{row['partition_key']}:{row['claim_key']}" for row in duplicate_rows),
                        self._limits.max_samples,
                    ),
                )
            )

        orphan_sql = """
            SELECT cs.claim_id, cs.event_id
            FROM claim_sources cs
            LEFT JOIN claims c ON c.id=cs.claim_id
            LEFT JOIN events e ON e.id=cs.event_id
            WHERE (c.id IS NULL OR e.id IS NULL)
        """
        orphan_params: tuple[Any, ...] = ()
        if partition is not None:
            orphan_sql += " AND (c.partition_key=? OR e.partition_key=?)"
            orphan_params = (partition, partition)
        orphan_rows = connection.execute(orphan_sql, orphan_params).fetchall()
        if orphan_rows:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.ORPHAN_CLAIM_SOURCE,
                    MemoryDoctorSeverity.ERROR,
                    len(orphan_rows),
                    "claim-source links reference missing evidence or claims",
                    "Remove orphan links only after preserving an audit backup.",
                    _samples(
                        (f"{row['claim_id']}:{row['event_id']}" for row in orphan_rows),
                        self._limits.max_samples,
                    ),
                    repairable=True,
                )
            )

        source_missing_rows = connection.execute(
            f"""
            SELECT c.id FROM claims c
            LEFT JOIN claim_sources cs ON cs.claim_id=c.id
            WHERE c.status='active' {('AND c.partition_key=?' if partition is not None else '')}
            GROUP BY c.id HAVING COUNT(cs.event_id)=0
            """,
            params,
        ).fetchall()
        if source_missing_rows:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.CLAIM_SOURCE_MISSING,
                    MemoryDoctorSeverity.ERROR,
                    len(source_missing_rows),
                    "active claims exist without linked source evidence",
                    "Archive unsupported claims after host review.",
                    _samples(
                        (row["id"] for row in source_missing_rows),
                        self._limits.max_samples,
                    ),
                    repairable=True,
                )
            )

        invalid_supersession = connection.execute(
            f"""
            SELECT c.id FROM claims c
            LEFT JOIN claims successor ON successor.id=c.superseded_by
            LEFT JOIN claims predecessor ON predecessor.id=c.supersedes
            WHERE (
                (c.status='superseded' AND (c.superseded_by IS NULL OR successor.id IS NULL))
                OR (c.status='active' AND c.superseded_by IS NOT NULL)
                OR (c.supersedes IS NOT NULL AND predecessor.id IS NULL)
            ) {('AND c.partition_key=?' if partition is not None else '')}
            """,
            params,
        ).fetchall()
        if invalid_supersession:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.CLAIM_SUPERSESSION_INVALID,
                    MemoryDoctorSeverity.ERROR,
                    len(invalid_supersession),
                    "claim supersession links or statuses are inconsistent",
                    "Reconstruct the claim lineage from state deltas and evidence.",
                    _samples(
                        (row["id"] for row in invalid_supersession),
                        self._limits.max_samples,
                    ),
                )
            )

        findings.extend(self._provenance_findings(connection, partition))
        return findings

    def _provenance_findings(
        self,
        connection: sqlite3.Connection,
        partition: str | None,
    ) -> list[MemoryDoctorFinding]:
        findings: list[MemoryDoctorFinding] = []
        where = "WHERE partition_key=?" if partition is not None else ""
        params: tuple[Any, ...] = (partition,) if partition is not None else ()
        artifact_rows = connection.execute(
            f"SELECT id, partition_key, provenance_json FROM artifacts {where} LIMIT ?",
            (*params, self._limits.max_rows + 1),
        ).fetchall()
        claim_rows = connection.execute(
            f"SELECT id, provenance_json FROM claims {where} LIMIT ?",
            (*params, self._limits.max_rows + 1),
        ).fetchall()
        if len(artifact_rows) > self._limits.max_rows or len(claim_rows) > self._limits.max_rows:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.INSPECTION_TRUNCATED,
                    MemoryDoctorSeverity.WARNING,
                    1,
                    "provenance inspection reached its configured row limit",
                    "Run partitioned diagnostics or raise the explicit row budget.",
                )
            )
        malformed: list[str] = []
        missing_artifacts: list[str] = []
        for row in artifact_rows[: self._limits.max_rows]:
            sources = _source_ids(row["provenance_json"])
            if sources is None:
                malformed.append(row["id"])
                continue
            if not sources or any(
                connection.execute(
                    "SELECT 1 FROM events WHERE id=? AND partition_key=?",
                    (source_id, row["partition_key"]),
                ).fetchone()
                is None
                for source_id in sources
            ):
                missing_artifacts.append(row["id"])
        if missing_artifacts:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.ARTIFACT_SOURCE_MISSING,
                    MemoryDoctorSeverity.ERROR,
                    len(missing_artifacts),
                    "derived artifacts have missing or empty source evidence",
                    "Archive unverifiable artifacts and regenerate only from retained evidence.",
                    _samples(missing_artifacts, self._limits.max_samples),
                    repairable=True,
                )
            )

        mismatched_claims: list[str] = []
        for row in claim_rows[: self._limits.max_rows]:
            sources = _source_ids(row["provenance_json"])
            if sources is None:
                malformed.append(row["id"])
                continue
            linked = {
                value["event_id"]
                for value in connection.execute(
                    "SELECT event_id FROM claim_sources WHERE claim_id=?",
                    (row["id"],),
                ).fetchall()
            }
            if set(sources) != linked:
                mismatched_claims.append(row["id"])
        if mismatched_claims:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.CLAIM_SOURCE_MISMATCH,
                    MemoryDoctorSeverity.ERROR,
                    len(mismatched_claims),
                    "claim provenance JSON differs from normalized source links",
                    "Rebuild provenance JSON from the normalized evidence links.",
                    _samples(mismatched_claims, self._limits.max_samples),
                    repairable=True,
                )
            )
        if malformed:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.MALFORMED_PROVENANCE,
                    MemoryDoctorSeverity.ERROR,
                    len(malformed),
                    "stored provenance is not valid structured JSON",
                    "Restore malformed rows from a verified backup.",
                    _samples(malformed, self._limits.max_samples),
                )
            )
        return findings

    def _worker_findings(
        self,
        connection: sqlite3.Connection,
        partition: str | None,
        checked_at: datetime,
    ) -> tuple[list[MemoryDoctorFinding], dict[str, int]]:
        findings: list[MemoryDoctorFinding] = []
        where = "WHERE partition_key=?" if partition is not None else ""
        params: tuple[Any, ...] = (partition,) if partition is not None else ()
        rows = connection.execute(
            f"""
            SELECT id, partition_key, status, payload_json, lease_expires_at
            FROM agent_memory_worker_tasks {where}
            LIMIT ?
            """,
            (*params, self._limits.max_rows + 1),
        ).fetchall()
        counts = {
            f"worker_{status}": int(
                connection.execute(
                    f"SELECT COUNT(*) FROM agent_memory_worker_tasks {where} "
                    f"{'AND' if where else 'WHERE'} status=?",
                    (*params, status),
                ).fetchone()[0]
            )
            for status in ("pending", "leased", "completed", "dead", "cancelled")
        }
        if len(rows) > self._limits.max_rows:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.INSPECTION_TRUNCATED,
                    MemoryDoctorSeverity.WARNING,
                    1,
                    "worker inspection reached its configured row limit",
                    "Run partitioned diagnostics or raise the explicit row budget.",
                )
            )
        selected = rows[: self._limits.max_rows]
        dead = [row["id"] for row in selected if row["status"] == "dead"]
        if dead:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.DEAD_WORKER_TASK,
                    MemoryDoctorSeverity.WARNING,
                    len(dead),
                    "worker tasks exhausted their retry budget",
                    "Review failure codes before retrying or archiving dead tasks.",
                    _samples(dead, self._limits.max_samples),
                    repairable=True,
                )
            )
        stale = [
            row["id"]
            for row in selected
            if row["status"] == "leased"
            and row["lease_expires_at"]
            and datetime.fromisoformat(row["lease_expires_at"]) <= checked_at
        ]
        if stale:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.STALE_WORKER_LEASE,
                    MemoryDoctorSeverity.WARNING,
                    len(stale),
                    "worker leases expired without completion",
                    "Release expired leases through the queue recovery path.",
                    _samples(stale, self._limits.max_samples),
                    repairable=True,
                )
            )
        malformed: list[str] = []
        missing_sources: list[str] = []
        with _read_only_connection(self._database_path) as memory_connection:
            for row in selected:
                if row["status"] not in {"pending", "leased"}:
                    continue
                try:
                    payload = json.loads(row["payload_json"])
                except (TypeError, json.JSONDecodeError):
                    malformed.append(row["id"])
                    continue
                event_id = payload.get("event_id")
                if event_id and memory_connection.execute(
                    "SELECT 1 FROM events WHERE id=? AND partition_key=?",
                    (event_id, row["partition_key"]),
                ).fetchone() is None:
                    missing_sources.append(row["id"])
        if malformed:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.WORKER_PAYLOAD_INVALID,
                    MemoryDoctorSeverity.ERROR,
                    len(malformed),
                    "active worker tasks contain malformed payloads",
                    "Cancel malformed tasks after preserving queue evidence.",
                    _samples(malformed, self._limits.max_samples),
                )
            )
        if missing_sources:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.WORKER_SOURCE_MISSING,
                    MemoryDoctorSeverity.ERROR,
                    len(missing_sources),
                    "active worker tasks reference deleted or unauthorized events",
                    "Cancel source-less tasks to prevent memory regeneration.",
                    _samples(missing_sources, self._limits.max_samples),
                    repairable=True,
                )
            )
        pending_active = counts["worker_pending"] + counts["worker_leased"]
        terminal = counts["worker_completed"] + counts["worker_dead"] + counts["worker_cancelled"]
        if pending_active > self._limits.max_pending_tasks or terminal > self._limits.max_terminal_tasks:
            findings.append(
                MemoryDoctorFinding(
                    MemoryDoctorCode.WORKER_CAPACITY,
                    MemoryDoctorSeverity.WARNING,
                    1,
                    "worker queue exceeds configured diagnostic capacity",
                    "Apply bounded retention and host-controlled backpressure.",
                )
            )
        return findings, counts


class _ReadOnlyConnection:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._connection: sqlite3.Connection | None = None

    def __enter__(self) -> sqlite3.Connection:
        uri = self._path.resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA foreign_keys=ON")
        self._connection = connection
        return connection

    def __exit__(self, exc_type, exc, traceback) -> None:
        assert self._connection is not None
        self._connection.close()


def _read_only_connection(path: Path) -> _ReadOnlyConnection:
    return _ReadOnlyConnection(path)


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _scoped_count(
    connection: sqlite3.Connection,
    table: str,
    partition: str | None,
) -> int:
    if partition is None:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return int(
        connection.execute(
            f"SELECT COUNT(*) FROM {table} WHERE partition_key=?",
            (partition,),
        ).fetchone()[0]
    )


def _source_ids(value: str) -> tuple[str, ...] | None:
    try:
        payload = json.loads(value)
        sources = payload.get("source_event_ids", ())
        if not isinstance(sources, list):
            return None
        if any(not isinstance(item, str) or not item for item in sources):
            return None
        return tuple(sources)
    except (TypeError, json.JSONDecodeError):
        return None


def _database_bytes(path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in path.parent.glob(path.name + "*")
        if candidate.is_file()
    )


def _fingerprint(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:12]


def _samples(values, maximum: int) -> tuple[str, ...]:
    return tuple(_fingerprint(value) for value in list(values)[:maximum])


def _severity_rank(severity: MemoryDoctorSeverity) -> int:
    return {
        MemoryDoctorSeverity.WARNING: 1,
        MemoryDoctorSeverity.ERROR: 2,
        MemoryDoctorSeverity.CRITICAL: 3,
    }[severity]
