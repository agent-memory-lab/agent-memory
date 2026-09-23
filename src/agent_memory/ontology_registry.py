"""Optional scoped schema catalog, activation, and audited rollback."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import sqlite3
from typing import Protocol

from .domain import MemoryScope
from .ontology_memory import OntologySchema
from .ontology_schema import (
    deserialize_ontology_schema,
    ontology_schema_digest,
    plan_ontology_migration,
    serialize_ontology_schema,
)


class OntologyRegistryConflict(ValueError):
    """The expected activation generation no longer matches storage."""


@dataclass(frozen=True, slots=True)
class OntologyActivation:
    ontology_id: str
    version: str
    digest: str
    generation: int


@dataclass(frozen=True, slots=True)
class OntologySwitchRequest:
    scope: MemoryScope
    previous: OntologyActivation | None
    target: OntologySchema
    target_digest: str
    action: str
    reason: str


class OntologySwitchAuthorizer(Protocol):
    """Trusted host checks approval and target-index readiness for this scope.

    Activation selects a schema; the host must prepare and validate its index
    before approving. Rollback also requires a usable retained target index.
    """

    async def authorize(self, request: OntologySwitchRequest) -> bool: ...


@dataclass(frozen=True, slots=True)
class OntologySwitchAudit:
    generation: int
    action: str
    from_version: str | None
    to_version: str
    target_digest: str
    reason: str
    created_at: str


class SQLiteOntologyRegistry:
    """Explicitly enabled catalog; does not switch already loaded plugins.

    Hosts resolve active_schema() when building a new plugin instance. Running
    instances remain pinned to their original schema until the host reloads.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    @contextmanager
    def _connection(self):
        connection = sqlite3.connect(self._path, timeout=5)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize)

    def _initialize(self):
        with self._connection() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS ontology_catalog (
                    scope TEXT NOT NULL, ontology TEXT NOT NULL,
                    version TEXT NOT NULL, digest TEXT NOT NULL, document TEXT NOT NULL,
                    PRIMARY KEY (scope, ontology, version)
                );
                CREATE TABLE IF NOT EXISTS ontology_activation (
                    scope TEXT NOT NULL, ontology TEXT NOT NULL,
                    version TEXT NOT NULL, digest TEXT NOT NULL, generation INTEGER NOT NULL,
                    PRIMARY KEY (scope, ontology)
                );
                CREATE TABLE IF NOT EXISTS ontology_switch_audit (
                    scope TEXT NOT NULL, ontology TEXT NOT NULL, generation INTEGER NOT NULL,
                    action TEXT NOT NULL, from_version TEXT, to_version TEXT NOT NULL,
                    target_digest TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY (scope, ontology, generation)
                );
            """)

    async def register(self, scope: MemoryScope, schema: OntologySchema) -> None:
        """Register an immutable document without activating it."""
        await asyncio.to_thread(self._register, scope.partition_key(), schema)

    def _register(self, scope, schema):
        document = serialize_ontology_schema(schema)
        digest = ontology_schema_digest(schema)
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute(
                "SELECT digest FROM ontology_catalog WHERE scope=? AND ontology=? AND version=?",
                (scope, schema.ontology_id, schema.version),
            ).fetchone()
            if previous is not None and previous["digest"] != digest:
                raise OntologyRegistryConflict("schema version already has a different document")
            db.execute(
                "INSERT OR IGNORE INTO ontology_catalog VALUES (?, ?, ?, ?, ?)",
                (scope, schema.ontology_id, schema.version, digest, document),
            )

    async def get(self, scope: MemoryScope, ontology_id: str, version: str) -> OntologySchema:
        return await asyncio.to_thread(self._get, scope.partition_key(), ontology_id, version)

    def _get(self, scope, ontology, version):
        with self._connection() as db:
            row = db.execute(
                "SELECT document FROM ontology_catalog WHERE scope=? AND ontology=? AND version=?",
                (scope, ontology, version),
            ).fetchone()
        if row is None:
            raise KeyError("schema version is not registered in this scope")
        return deserialize_ontology_schema(row["document"])

    async def versions(
        self, scope: MemoryScope, ontology_id: str, *, limit: int = 50, after: str = ""
    ) -> tuple[str, ...]:
        """Bounded lexical pagination; version ordering is not SemVer precedence."""
        _limit(limit)
        return await asyncio.to_thread(self._versions, scope.partition_key(), ontology_id, limit, after)

    def _versions(self, scope, ontology, limit, after):
        with self._connection() as db:
            return tuple(row[0] for row in db.execute(
                "SELECT version FROM ontology_catalog WHERE scope=? AND ontology=? "
                "AND version>? ORDER BY version LIMIT ?", (scope, ontology, after, limit),
            ))

    async def active(self, scope: MemoryScope, ontology_id: str) -> OntologyActivation | None:
        return await asyncio.to_thread(self._active, scope.partition_key(), ontology_id)

    def _active(self, scope, ontology):
        with self._connection() as db:
            row = db.execute(
                "SELECT * FROM ontology_activation WHERE scope=? AND ontology=?", (scope, ontology),
            ).fetchone()
        return _activation(row) if row is not None else None

    async def active_schema(self, scope: MemoryScope, ontology_id: str) -> OntologySchema | None:
        active = await self.active(scope, ontology_id)
        return None if active is None else await self.get(scope, ontology_id, active.version)

    async def activate(
        self, scope: MemoryScope, ontology_id: str, version: str, *,
        expected_generation: int, reason: str, authorizer: OntologySwitchAuthorizer,
    ) -> OntologyActivation:
        return await self._switch(
            scope, ontology_id, version, expected_generation, reason, authorizer, "activate"
        )

    async def rollback(
        self, scope: MemoryScope, ontology_id: str, version: str, *,
        expected_generation: int, reason: str, authorizer: OntologySwitchAuthorizer,
    ) -> OntologyActivation:
        return await self._switch(
            scope, ontology_id, version, expected_generation, reason, authorizer, "rollback"
        )

    async def _switch(self, scope, ontology, version, generation, reason, authorizer, action):
        if type(generation) is not int or generation < 0:
            raise ValueError("expected_generation must be a nonnegative integer")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 2_000:
            raise ValueError("reason must contain 1 to 2000 characters")
        previous = await self.active(scope, ontology)
        if (previous.generation if previous else 0) != generation:
            raise OntologyRegistryConflict("activation generation changed")
        target = await self.get(scope, ontology, version)
        if previous is not None and previous.version == version:
            raise ValueError("target schema is already active")
        if action == "activate" and previous is not None:
            baseline = await self.get(scope, ontology, previous.version)
            plan = plan_ontology_migration(baseline, target)
            if not plan.version_policy_valid:
                raise ValueError(f"activation requires a {plan.required_version_bump} version bump")
        if action == "rollback" and previous is None:
            raise ValueError("rollback requires an active schema")
        request = OntologySwitchRequest(
            scope, previous, target, ontology_schema_digest(target), action, reason,
        )
        if await asyncio.wait_for(authorizer.authorize(request), timeout=2) is not True:
            raise PermissionError("host did not authorize schema switch")
        return await asyncio.to_thread(self._commit_switch, request, generation)

    def _commit_switch(self, request, generation):
        scope = request.scope.partition_key()
        ontology = request.target.ontology_id
        version = request.target.version
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute(
                "SELECT generation FROM ontology_activation WHERE scope=? AND ontology=?",
                (scope, ontology),
            ).fetchone()
            if (current[0] if current else 0) != generation:
                raise OntologyRegistryConflict("activation generation changed during approval")
            if request.action == "rollback":
                seen = db.execute(
                    "SELECT 1 FROM ontology_switch_audit WHERE scope=? AND ontology=? "
                    "AND to_version=? LIMIT 1", (scope, ontology, version),
                ).fetchone()
                if seen is None:
                    raise ValueError("rollback target has never been active")
            db.execute(
                "INSERT INTO ontology_activation VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(scope, ontology) DO UPDATE SET version=excluded.version, "
                "digest=excluded.digest, generation=excluded.generation",
                (scope, ontology, version, request.target_digest, generation + 1),
            )
            db.execute(
                "INSERT INTO ontology_switch_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (scope, ontology, generation + 1, request.action,
                 request.previous.version if request.previous else None,
                 version, request.target_digest, request.reason, datetime.now(UTC).isoformat()),
            )
        return OntologyActivation(ontology, version, request.target_digest, generation + 1)

    async def history(
        self, scope: MemoryScope, ontology_id: str, *, after_generation: int = 0, limit: int = 50,
    ) -> tuple[OntologySwitchAudit, ...]:
        _limit(limit)
        if type(after_generation) is not int or after_generation < 0:
            raise ValueError("after_generation must be a nonnegative integer")
        return await asyncio.to_thread(
            self._history, scope.partition_key(), ontology_id, after_generation, limit,
        )

    def _history(self, scope, ontology, after, limit):
        with self._connection() as db:
            rows = db.execute(
                "SELECT generation, action, from_version, to_version, target_digest, reason, "
                "created_at FROM ontology_switch_audit WHERE scope=? AND ontology=? "
                "AND generation>? ORDER BY generation LIMIT ?", (scope, ontology, after, limit),
            ).fetchall()
        return tuple(OntologySwitchAudit(**dict(row)) for row in rows)


def _activation(row):
    return OntologyActivation(row["ontology"], row["version"], row["digest"], row["generation"])


def _limit(value):
    if type(value) is not int or not 1 <= value <= 100:
        raise ValueError("limit must be between 1 and 100")
