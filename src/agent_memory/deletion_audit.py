"""Privacy-safe, tamper-evident audit receipts for memory deletion operations."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
import hmac
import json
from pathlib import Path
import sqlite3
from types import MappingProxyType
from typing import Protocol
from uuid import uuid4

from .domain import ForgetMode, ForgetRequest, MemoryScope, utc_now
from .ports import MemoryProvider


class DeletionAuditStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class DeletionAuditError(RuntimeError):
    def __init__(self, message: str, *, code: str, operation_id: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.operation_id = operation_id


@dataclass(frozen=True, slots=True)
class DeletionAuditReceipt:
    operation_id: str
    request_fingerprint: str
    scope_fingerprint: str
    actor_fingerprint: str
    mode: ForgetMode
    status: DeletionAuditStatus
    target_count: int
    all_in_scope: bool
    requested_at: datetime
    completed_at: datetime | None = None
    affected_events: int = 0
    affected_claims: int = 0
    affected_artifacts: int = 0
    failure_code: str | None = None
    integrity_digest: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "mode", ForgetMode(self.mode))
        object.__setattr__(self, "status", DeletionAuditStatus(self.status))
        if not self.operation_id.strip():
            raise ValueError("operation_id must not be empty")
        for name in ("request_fingerprint", "scope_fingerprint", "actor_fingerprint"):
            if len(getattr(self, name)) != 64:
                raise ValueError(f"{name} must be a SHA-256 HMAC digest")
        if self.integrity_digest and len(self.integrity_digest) != 64:
            raise ValueError("integrity_digest must be a SHA-256 HMAC digest")
        for name in (
            "target_count",
            "affected_events",
            "affected_claims",
            "affected_artifacts",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.requested_at.tzinfo is None:
            raise ValueError("requested_at must be timezone-aware")
        if self.completed_at is not None and self.completed_at.tzinfo is None:
            raise ValueError("completed_at must be timezone-aware")
        if self.status is DeletionAuditStatus.PENDING and self.completed_at is not None:
            raise ValueError("pending receipt cannot have completed_at")
        if self.status is not DeletionAuditStatus.PENDING and self.completed_at is None:
            raise ValueError("terminal receipt requires completed_at")
        if self.status is DeletionAuditStatus.FAILED and not self.failure_code:
            raise ValueError("failed receipt requires failure_code")
        if self.status is not DeletionAuditStatus.FAILED and self.failure_code is not None:
            raise ValueError("only failed receipt may contain failure_code")


@dataclass(frozen=True, slots=True)
class DeletionAuditReport:
    generated_at: datetime
    scope_fingerprint: str
    receipts: tuple[DeletionAuditReceipt, ...]
    status_counts: Mapping[str, int]
    integrity_verified: bool

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must be timezone-aware")
        object.__setattr__(self, "receipts", tuple(self.receipts))
        object.__setattr__(self, "status_counts", MappingProxyType(dict(self.status_counts)))


class DeletionAuditSink(Protocol):
    async def record(self, receipt: DeletionAuditReceipt) -> None: ...

    async def list_recent(
        self, scope_fingerprint: str, *, limit: int
    ) -> tuple[DeletionAuditReceipt, ...]: ...


class SQLiteDeletionAuditSink:
    """Small audit store kept separate from memory content."""

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    async def record(self, receipt: DeletionAuditReceipt) -> None:
        await asyncio.to_thread(self._record_sync, receipt)

    async def list_recent(
        self, scope_fingerprint: str, *, limit: int
    ) -> tuple[DeletionAuditReceipt, ...]:
        if len(scope_fingerprint) != 64:
            raise ValueError("scope_fingerprint must be a SHA-256 HMAC digest")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        return await asyncio.to_thread(self._list_recent_sync, scope_fingerprint, limit)

    def _connect(self) -> sqlite3.Connection:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS deletion_audit (
                operation_id TEXT PRIMARY KEY,
                request_fingerprint TEXT NOT NULL,
                scope_fingerprint TEXT NOT NULL,
                actor_fingerprint TEXT NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                target_count INTEGER NOT NULL,
                all_in_scope INTEGER NOT NULL,
                requested_at TEXT NOT NULL,
                completed_at TEXT,
                affected_events INTEGER NOT NULL,
                affected_claims INTEGER NOT NULL,
                affected_artifacts INTEGER NOT NULL,
                failure_code TEXT,
                integrity_digest TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS deletion_audit_scope_idx "
            "ON deletion_audit(scope_fingerprint, requested_at DESC, operation_id DESC)"
        )
        return connection

    def _record_sync(self, receipt: DeletionAuditReceipt) -> None:
        connection = self._connect()
        try:
            with connection:
                existing = connection.execute(
                    "SELECT * FROM deletion_audit WHERE operation_id=?",
                    (receipt.operation_id,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO deletion_audit (
                            operation_id, request_fingerprint, scope_fingerprint,
                            actor_fingerprint, mode, status, target_count, all_in_scope,
                            requested_at, completed_at, affected_events, affected_claims,
                            affected_artifacts, failure_code, integrity_digest
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        _receipt_values(receipt),
                    )
                    return
                immutable = (
                    existing["request_fingerprint"],
                    existing["scope_fingerprint"],
                    existing["actor_fingerprint"],
                    existing["mode"],
                    existing["target_count"],
                    bool(existing["all_in_scope"]),
                    existing["requested_at"],
                )
                expected = (
                    receipt.request_fingerprint,
                    receipt.scope_fingerprint,
                    receipt.actor_fingerprint,
                    receipt.mode.value,
                    receipt.target_count,
                    receipt.all_in_scope,
                    receipt.requested_at.isoformat(),
                )
                if immutable != expected:
                    raise DeletionAuditError(
                        "audit operation identity conflict",
                        code="audit_conflict",
                        operation_id=receipt.operation_id,
                    )
                if existing["status"] != DeletionAuditStatus.PENDING.value:
                    if _receipt_from_row(existing) != receipt:
                        raise DeletionAuditError(
                            "terminal audit receipt cannot be changed",
                            code="audit_conflict",
                            operation_id=receipt.operation_id,
                        )
                    return
                connection.execute(
                    """
                    UPDATE deletion_audit
                    SET status=?, completed_at=?, affected_events=?, affected_claims=?,
                        affected_artifacts=?, failure_code=?, integrity_digest=?
                    WHERE operation_id=?
                    """,
                    _terminal_values(receipt),
                )
        finally:
            connection.close()

    def _list_recent_sync(
        self, scope_fingerprint: str, limit: int
    ) -> tuple[DeletionAuditReceipt, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM deletion_audit WHERE scope_fingerprint=? "
                "ORDER BY requested_at DESC, operation_id DESC LIMIT ?",
                (scope_fingerprint, limit),
            ).fetchall()
            return tuple(_receipt_from_row(row) for row in rows)
        finally:
            connection.close()


class DeletionAuditService:
    """Execute and report deletion with a host-owned HMAC integrity key."""

    def __init__(self, sink: DeletionAuditSink, *, secret: bytes) -> None:
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError("audit secret must contain at least 32 bytes")
        self._sink = sink
        self._secret = secret

    async def forget(
        self,
        provider: MemoryProvider,
        request: ForgetRequest,
        *,
        actor: str,
    ) -> DeletionAuditReceipt:
        if not actor.strip():
            raise ValueError("actor must not be empty")
        receipt = self._sign(
            DeletionAuditReceipt(
                operation_id=str(uuid4()),
                request_fingerprint=self._request_fingerprint(request),
                scope_fingerprint=self.scope_fingerprint(request.scope),
                actor_fingerprint=self._digest(actor),
                mode=request.mode,
                status=DeletionAuditStatus.PENDING,
                target_count=len(set(request.memory_ids)),
                all_in_scope=request.all_in_scope,
                requested_at=utc_now(),
            )
        )
        try:
            await self._sink.record(receipt)
        except DeletionAuditError:
            raise
        except Exception as error:
            raise DeletionAuditError(
                "deletion was not started because audit admission failed",
                code="audit_unavailable",
                operation_id=receipt.operation_id,
            ) from error
        try:
            result = await provider.forget(request)
        except Exception:
            failed = self._sign(
                replace(
                    receipt,
                    status=DeletionAuditStatus.FAILED,
                    completed_at=utc_now(),
                    failure_code="provider_error",
                    integrity_digest="",
                )
            )
            try:
                await self._sink.record(failed)
            except Exception:
                pass
            raise
        completed = self._sign(
            replace(
                receipt,
                status=DeletionAuditStatus.SUCCEEDED,
                completed_at=utc_now(),
                affected_events=result.affected_events,
                affected_claims=result.affected_claims,
                affected_artifacts=result.affected_artifacts,
                integrity_digest="",
            )
        )
        try:
            await self._sink.record(completed)
        except Exception as error:
            raise DeletionAuditError(
                "deletion completed but audit finalization failed",
                code="audit_incomplete",
                operation_id=receipt.operation_id,
            ) from error
        return completed

    async def report(self, scope: MemoryScope, *, limit: int = 100) -> DeletionAuditReport:
        fingerprint = self.scope_fingerprint(scope)
        receipts = await self._sink.list_recent(fingerprint, limit=limit)
        counts = {status.value: 0 for status in DeletionAuditStatus}
        for receipt in receipts:
            counts[receipt.status.value] += 1
        return DeletionAuditReport(
            generated_at=utc_now(),
            scope_fingerprint=fingerprint,
            receipts=receipts,
            status_counts=counts,
            integrity_verified=all(self.verify(receipt) for receipt in receipts),
        )

    def scope_fingerprint(self, scope: MemoryScope) -> str:
        return self._digest(f"scope:{scope.partition_key()}")

    def verify(self, receipt: DeletionAuditReceipt) -> bool:
        expected = self._receipt_digest(replace(receipt, integrity_digest=""))
        return hmac.compare_digest(receipt.integrity_digest, expected)

    def _sign(self, receipt: DeletionAuditReceipt) -> DeletionAuditReceipt:
        return replace(receipt, integrity_digest=self._receipt_digest(receipt))

    def _request_fingerprint(self, request: ForgetRequest) -> str:
        return self._digest(
            _canonical(
                {
                    "scope": request.scope.partition_key(),
                    "memory_ids": sorted(set(request.memory_ids)),
                    "all_in_scope": request.all_in_scope,
                    "mode": request.mode.value,
                }
            )
        )

    def _receipt_digest(self, receipt: DeletionAuditReceipt) -> str:
        return self._digest(
            _canonical(
                {
                    "operation_id": receipt.operation_id,
                    "request_fingerprint": receipt.request_fingerprint,
                    "scope_fingerprint": receipt.scope_fingerprint,
                    "actor_fingerprint": receipt.actor_fingerprint,
                    "mode": receipt.mode.value,
                    "status": receipt.status.value,
                    "target_count": receipt.target_count,
                    "all_in_scope": receipt.all_in_scope,
                    "requested_at": receipt.requested_at.isoformat(),
                    "completed_at": (
                        receipt.completed_at.isoformat() if receipt.completed_at else None
                    ),
                    "affected_events": receipt.affected_events,
                    "affected_claims": receipt.affected_claims,
                    "affected_artifacts": receipt.affected_artifacts,
                    "failure_code": receipt.failure_code,
                }
            )
        )

    def _digest(self, value: str) -> str:
        return hmac.new(self._secret, value.encode("utf-8"), sha256).hexdigest()


def _canonical(value: Mapping[str, object]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _receipt_values(receipt: DeletionAuditReceipt) -> tuple[object, ...]:
    return (
        receipt.operation_id,
        receipt.request_fingerprint,
        receipt.scope_fingerprint,
        receipt.actor_fingerprint,
        receipt.mode.value,
        receipt.status.value,
        receipt.target_count,
        int(receipt.all_in_scope),
        receipt.requested_at.isoformat(),
        receipt.completed_at.isoformat() if receipt.completed_at else None,
        receipt.affected_events,
        receipt.affected_claims,
        receipt.affected_artifacts,
        receipt.failure_code,
        receipt.integrity_digest,
    )


def _terminal_values(receipt: DeletionAuditReceipt) -> tuple[object, ...]:
    if receipt.status is DeletionAuditStatus.PENDING:
        raise ValueError("pending receipt cannot replace an existing receipt")
    return (
        receipt.status.value,
        receipt.completed_at.isoformat() if receipt.completed_at else None,
        receipt.affected_events,
        receipt.affected_claims,
        receipt.affected_artifacts,
        receipt.failure_code,
        receipt.integrity_digest,
        receipt.operation_id,
    )


def _receipt_from_row(row: sqlite3.Row) -> DeletionAuditReceipt:
    return DeletionAuditReceipt(
        operation_id=row["operation_id"],
        request_fingerprint=row["request_fingerprint"],
        scope_fingerprint=row["scope_fingerprint"],
        actor_fingerprint=row["actor_fingerprint"],
        mode=ForgetMode(row["mode"]),
        status=DeletionAuditStatus(row["status"]),
        target_count=int(row["target_count"]),
        all_in_scope=bool(row["all_in_scope"]),
        requested_at=datetime.fromisoformat(row["requested_at"]),
        completed_at=(datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None),
        affected_events=int(row["affected_events"]),
        affected_claims=int(row["affected_claims"]),
        affected_artifacts=int(row["affected_artifacts"]),
        failure_code=row["failure_code"],
        integrity_digest=row["integrity_digest"],
    )
