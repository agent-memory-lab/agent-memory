from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from agent_memory import MemoryScope

from .repository import PostgresMemoryRepository


@dataclass(frozen=True, slots=True)
class HealthCapacityPolicy:
    max_active_events: int | None = None
    max_active_claims: int | None = None
    max_active_blocks: int | None = None
    max_orphan_vectors: int | None = None
    max_queue_dead: int | None = None
    max_expired_leases: int | None = None


@dataclass(frozen=True, slots=True)
class MemoryQueueHealth:
    pending: int
    running: int
    completed: int
    dead: int
    expired_leases: int
    oldest_pending_age_seconds: float | None
    oldest_dead_age_seconds: float | None


@dataclass(frozen=True, slots=True)
class MemoryStorageHealth:
    events_active: int
    events_archived: int
    claims_active: int
    claims_archived: int
    blocks_active: int
    blocks_archived: int
    episodes_active: int
    episodes_archived: int
    procedures_active: int
    procedures_archived: int


@dataclass(frozen=True, slots=True)
class VectorIntegrityHealth:
    vector_rows: int
    orphan_vectors: int
    vectors_for_archived_blocks: int


@dataclass(frozen=True, slots=True)
class DeadQueueJob:
    id: str
    job_key: str
    attempts: int
    max_attempts: int
    last_error: str | None


@dataclass(frozen=True, slots=True)
class MemoryHealthReport:
    checked_at: datetime
    scope: MemoryScope | None
    storage: MemoryStorageHealth
    queue: MemoryQueueHealth
    vectors: VectorIntegrityHealth
    dead_jobs: tuple[DeadQueueJob, ...]
    capacity_violations: tuple[str, ...]


def _scope_clause(scope: MemoryScope | None, *, prefix: str = "") -> tuple[str, tuple[Any, ...]]:
    if scope is None:
        return "TRUE", ()

    sep = f"{prefix}." if prefix else ""
    clauses = [f"{sep}tenant_id = %s", f"{sep}namespace = %s"]
    params: list[Any] = [scope.tenant_id, scope.namespace]
    for column, value in (
        ("user_id", scope.user_id),
        ("agent_id", scope.agent_id),
        ("workspace_id", scope.workspace_id),
        ("session_id", scope.session_id),
    ):
        clauses.append(f"({sep}{column} IS NULL OR {sep}{column} = %s)")
        params.append(value)
    return " AND ".join(clauses), tuple(params)


def _int(value: Any) -> int:
    return int(value or 0)


def _float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _capacity_violations(
    report: MemoryHealthReport,
    policy: HealthCapacityPolicy | None,
) -> tuple[str, ...]:
    if policy is None:
        return ()
    reasons: list[str] = []
    if (
        policy.max_active_events is not None
        and report.storage.events_active > policy.max_active_events
    ):
        reasons.append(
            f"active events {report.storage.events_active} exceeds "
            f"max_active_events={policy.max_active_events}"
        )
    if (
        policy.max_active_claims is not None
        and report.storage.claims_active > policy.max_active_claims
    ):
        reasons.append(
            f"active claims {report.storage.claims_active} exceeds "
            f"max_active_claims={policy.max_active_claims}"
        )
    if (
        policy.max_active_blocks is not None
        and report.storage.blocks_active > policy.max_active_blocks
    ):
        reasons.append(
            f"active blocks {report.storage.blocks_active} exceeds "
            f"max_active_blocks={policy.max_active_blocks}"
        )
    if (
        policy.max_orphan_vectors is not None
        and report.vectors.orphan_vectors > policy.max_orphan_vectors
    ):
        reasons.append(
            f"orphan vectors {report.vectors.orphan_vectors} exceeds "
            f"max_orphan_vectors={policy.max_orphan_vectors}"
        )
    if policy.max_queue_dead is not None and report.queue.dead > policy.max_queue_dead:
        reasons.append(
            f"dead consolidation jobs {report.queue.dead} exceeds "
            f"max_queue_dead={policy.max_queue_dead}"
        )
    if (
        policy.max_expired_leases is not None
        and report.queue.expired_leases > policy.max_expired_leases
    ):
        reasons.append(
            f"expired queue leases {report.queue.expired_leases} exceeds "
            f"max_expired_leases={policy.max_expired_leases}"
        )
    return tuple(reasons)


def _evaluate(
    policy: HealthCapacityPolicy | None,
    checked_at: datetime,
    scope: MemoryScope | None,
    storage: MemoryStorageHealth,
    queue: MemoryQueueHealth,
    vectors: VectorIntegrityHealth,
    dead_jobs: tuple[DeadQueueJob, ...],
) -> MemoryHealthReport:
    report = MemoryHealthReport(
        checked_at=checked_at,
        scope=scope,
        storage=storage,
        queue=queue,
        vectors=vectors,
        dead_jobs=dead_jobs,
        capacity_violations=(),
    )
    return MemoryHealthReport(
        checked_at=report.checked_at,
        scope=report.scope,
        storage=report.storage,
        queue=report.queue,
        vectors=report.vectors,
        dead_jobs=report.dead_jobs,
        capacity_violations=_capacity_violations(report, policy),
    )


async def collect_memory_health(
    repository: PostgresMemoryRepository,
    scope: MemoryScope | None = None,
    policy: HealthCapacityPolicy | None = None,
) -> MemoryHealthReport:
    where_scope, params = _scope_clause(scope)
    queue_where, queue_params = _scope_clause(scope, prefix="j")
    vector_where, vector_params = _scope_clause(scope, prefix="v")
    artifact_where, artifact_params = _scope_clause(scope)

    async with repository.pool.connection() as connection:
        events_cursor = await connection.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE archived_at IS NULL) AS active_events,
                COUNT(*) FILTER (WHERE archived_at IS NOT NULL) AS archived_events
            FROM agent_memory_events
            WHERE {where_scope}
            """,
            params,
        )
        events = await events_cursor.fetchone()

        claims_cursor = await connection.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE status = 'active' AND archived_at IS NULL) AS active_claims,
                COUNT(*) FILTER (WHERE archived_at IS NOT NULL) AS archived_claims
            FROM agent_memory_claims
            WHERE {where_scope}
            """,
            params,
        )
        claims = await claims_cursor.fetchone()

        artifact_cursor = await connection.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE kind = 'block' AND archived_at IS NULL) AS active_blocks,
                COUNT(*) FILTER (WHERE kind = 'block' AND archived_at IS NOT NULL)
                    AS archived_blocks,
                COUNT(*) FILTER (WHERE kind = 'episode' AND archived_at IS NULL) AS active_episodes,
                COUNT(*) FILTER (WHERE kind = 'episode' AND archived_at IS NOT NULL)
                    AS archived_episodes,
                COUNT(*) FILTER (WHERE kind = 'procedure' AND archived_at IS NULL)
                    AS active_procedures,
                COUNT(*) FILTER (WHERE kind = 'procedure' AND archived_at IS NOT NULL)
                    AS archived_procedures
            FROM agent_memory_artifacts
            WHERE {artifact_where}
            """,
            artifact_params,
        )
        artifacts = await artifact_cursor.fetchone()

        queue_cursor = await connection.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                COUNT(*) FILTER (WHERE status = 'running') AS running,
                COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                COUNT(*) FILTER (WHERE status = 'dead') AS dead,
                COUNT(*) FILTER (
                    WHERE status = 'running'
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at < now()
                ) AS expired_leases,
                MAX(EXTRACT(EPOCH FROM (now() - next_attempt_at))) FILTER (WHERE status = 'pending')
                    AS oldest_pending_age_seconds,
                MAX(EXTRACT(EPOCH FROM (now() - updated_at))) FILTER (WHERE status = 'dead')
                    AS oldest_dead_age_seconds
            FROM agent_memory_consolidation_jobs AS j
            WHERE {queue_where}
            """,
            queue_params,
        )
        queue_row = await queue_cursor.fetchone()

        dead_job_cursor = await connection.execute(
            f"""
            SELECT id, job_key, attempts, max_attempts, last_error
            FROM agent_memory_consolidation_jobs
            WHERE {queue_where} AND status = 'dead'
            ORDER BY updated_at DESC
            LIMIT 10
            """,
            queue_params,
        )
        dead_job_rows = await dead_job_cursor.fetchall()

        vector_total_cursor = await connection.execute(
            f"""
            SELECT COUNT(*) AS vector_rows
            FROM agent_memory_vectors AS v
            WHERE {vector_where}
            """,
            vector_params,
        )
        vector_total = await vector_total_cursor.fetchone()

        orphan_cursor = await connection.execute(
            f"""
            SELECT COUNT(*) AS orphan_vectors
            FROM agent_memory_vectors AS v
            LEFT JOIN agent_memory_artifacts AS a
              ON a.id = v.memory_id
             AND a.kind = 'block'
             AND a.archived_at IS NULL
            WHERE {vector_where}
              AND a.id IS NULL
            """,
            vector_params,
        )
        orphan = await orphan_cursor.fetchone()

        archived_vector_cursor = await connection.execute(
            f"""
            SELECT COUNT(*) AS vectors_for_archived_blocks
            FROM agent_memory_vectors AS v
            JOIN agent_memory_artifacts AS a
              ON a.id = v.memory_id
             AND a.kind = 'block'
            WHERE {vector_where}
              AND a.archived_at IS NOT NULL
            """,
            vector_params,
        )
        archived_vector = await archived_vector_cursor.fetchone()

    storage = MemoryStorageHealth(
        events_active=_int(events["active_events"]),
        events_archived=_int(events["archived_events"]),
        claims_active=_int(claims["active_claims"]),
        claims_archived=_int(claims["archived_claims"]),
        blocks_active=_int(artifacts["active_blocks"]),
        blocks_archived=_int(artifacts["archived_blocks"]),
        episodes_active=_int(artifacts["active_episodes"]),
        episodes_archived=_int(artifacts["archived_episodes"]),
        procedures_active=_int(artifacts["active_procedures"]),
        procedures_archived=_int(artifacts["archived_procedures"]),
    )

    queue = MemoryQueueHealth(
        pending=_int(queue_row["pending"]),
        running=_int(queue_row["running"]),
        completed=_int(queue_row["completed"]),
        dead=_int(queue_row["dead"]),
        expired_leases=_int(queue_row["expired_leases"]),
        oldest_pending_age_seconds=_float(queue_row["oldest_pending_age_seconds"]),
        oldest_dead_age_seconds=_float(queue_row["oldest_dead_age_seconds"]),
    )

    vectors = VectorIntegrityHealth(
        vector_rows=_int(vector_total["vector_rows"]),
        orphan_vectors=_int(orphan["orphan_vectors"]),
        vectors_for_archived_blocks=_int(archived_vector["vectors_for_archived_blocks"]),
    )

    dead_jobs = tuple(
        DeadQueueJob(
            id=row["id"],
            job_key=row["job_key"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            last_error=row["last_error"],
        )
        for row in dead_job_rows
    )

    return _evaluate(
        policy=policy,
        checked_at=datetime.now(),
        scope=scope,
        storage=storage,
        queue=queue,
        vectors=vectors,
        dead_jobs=dead_jobs,
    )


async def run_health_scan(
    dsn: str,
    scope: MemoryScope | None = None,
    policy: HealthCapacityPolicy | None = None,
) -> MemoryHealthReport:
    repository = PostgresMemoryRepository.from_dsn(dsn)
    await repository.pool.open()
    try:
        return await collect_memory_health(repository, scope=scope, policy=policy)
    finally:
        await repository.close()


def _format_report(report: MemoryHealthReport, *, include_dead_jobs: bool = True) -> str:
    lines = [
        f"checked_at: {report.checked_at.isoformat()}",
        f"scope: {'global' if report.scope is None else _scope_dict(report.scope)}",
        f"events: active={report.storage.events_active} archived={report.storage.events_archived}",
        f"claims: active={report.storage.claims_active} archived={report.storage.claims_archived}",
        (
            "artifacts: "
            f"blocks={report.storage.blocks_active}/{report.storage.blocks_archived} "
            f"episodes={report.storage.episodes_active}/{report.storage.episodes_archived} "
            f"procedures={report.storage.procedures_active}/{report.storage.procedures_archived}"
        ),
        (
            "consolidation queue: "
            f"pending={report.queue.pending} running={report.queue.running} "
            f"completed={report.queue.completed} dead={report.queue.dead} "
            f"expired_leases={report.queue.expired_leases}"
        ),
        (
            f"vectors: total={report.vectors.vector_rows} "
            f"orphan={report.vectors.orphan_vectors} "
            f"archived={report.vectors.vectors_for_archived_blocks}"
        ),
    ]
    if include_dead_jobs and report.dead_jobs:
        lines.append("dead jobs:")
        for job in report.dead_jobs:
            lines.append(
                f"  - {job.job_key} attempts={job.attempts}/{job.max_attempts} "
                f"last_error={job.last_error or '(none)'}"
            )
    if report.capacity_violations:
        lines.append("policy violations:")
        lines.extend(f"  - {message}" for message in report.capacity_violations)
    else:
        lines.append("policy violations: none")
    return "\n".join(lines)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Agent Memory PostgreSQL health scan.")
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN; required unless AGENT_MEMORY_POSTGRES_DSN is set.",
    )
    parser.add_argument(
        "--all-scopes",
        action="store_true",
        help="Scan all tenant/namespace scopes (no scope filter).",
    )
    parser.add_argument("--tenant-id", default=None, help="Tenant scope filter.")
    parser.add_argument("--namespace", default=None, help="Namespace scope filter.")
    parser.add_argument("--user-id", default=None)
    parser.add_argument("--agent-id", default=None)
    parser.add_argument("--workspace-id", default=None)
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--max-active-events", type=int, dest="max_active_events")
    parser.add_argument("--max-active-claims", type=int, dest="max_active_claims")
    parser.add_argument("--max-active-blocks", type=int, dest="max_active_blocks")
    parser.add_argument("--max-orphan-vectors", type=int, dest="max_orphan_vectors")
    parser.add_argument("--max-dead-jobs", type=int, dest="max_queue_dead")
    parser.add_argument("--max-expired-leases", type=int, dest="max_expired_leases")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON report instead of human readable summary.",
    )
    parser.add_argument(
        "--fail-on-violation",
        action="store_true",
        help="Exit non-zero when policy violations are reported.",
    )
    args = parser.parse_args(argv)
    if args.all_scopes:
        return args
    if args.tenant_id is None or args.namespace is None:
        parser.error("--tenant-id and --namespace are required unless --all-scopes is set.")
    return args


def _build_scope(args: argparse.Namespace) -> MemoryScope | None:
    if args.all_scopes:
        return None
    return MemoryScope(
        tenant_id=args.tenant_id,
        namespace=args.namespace,
        user_id=args.user_id,
        agent_id=args.agent_id,
        workspace_id=args.workspace_id,
        session_id=args.session_id,
    )


def _build_policy(args: argparse.Namespace) -> HealthCapacityPolicy | None:
    if (
        args.max_active_events is None
        and args.max_active_claims is None
        and args.max_active_blocks is None
        and args.max_orphan_vectors is None
        and args.max_queue_dead is None
        and args.max_expired_leases is None
    ):
        return None
    return HealthCapacityPolicy(
        max_active_events=args.max_active_events,
        max_active_claims=args.max_active_claims,
        max_active_blocks=args.max_active_blocks,
        max_orphan_vectors=args.max_orphan_vectors,
        max_queue_dead=args.max_queue_dead,
        max_expired_leases=args.max_expired_leases,
    )


def _scope_dict(scope: MemoryScope | None) -> dict[str, str | None] | None:
    if scope is None:
        return None
    return {
        "tenant_id": scope.tenant_id,
        "namespace": scope.namespace,
        "user_id": scope.user_id,
        "agent_id": scope.agent_id,
        "workspace_id": scope.workspace_id,
        "session_id": scope.session_id,
    }


def _serialize_report(report: MemoryHealthReport) -> str:
    return json.dumps(
        {
            "checked_at": report.checked_at.isoformat(),
            "scope": _scope_dict(report.scope),
            "storage": {
                "events_active": report.storage.events_active,
                "events_archived": report.storage.events_archived,
                "claims_active": report.storage.claims_active,
                "claims_archived": report.storage.claims_archived,
                "blocks_active": report.storage.blocks_active,
                "blocks_archived": report.storage.blocks_archived,
                "episodes_active": report.storage.episodes_active,
                "episodes_archived": report.storage.episodes_archived,
                "procedures_active": report.storage.procedures_active,
                "procedures_archived": report.storage.procedures_archived,
            },
            "queue": {
                "pending": report.queue.pending,
                "running": report.queue.running,
                "completed": report.queue.completed,
                "dead": report.queue.dead,
                "expired_leases": report.queue.expired_leases,
                "oldest_pending_age_seconds": report.queue.oldest_pending_age_seconds,
                "oldest_dead_age_seconds": report.queue.oldest_dead_age_seconds,
            },
            "vectors": {
                "vector_rows": report.vectors.vector_rows,
                "orphan_vectors": report.vectors.orphan_vectors,
                "vectors_for_archived_blocks": report.vectors.vectors_for_archived_blocks,
            },
            "dead_jobs": [
                {
                    "id": job.id,
                    "job_key": job.job_key,
                    "attempts": job.attempts,
                    "max_attempts": job.max_attempts,
                    "last_error": job.last_error,
                }
                for job in report.dead_jobs
            ],
            "capacity_violations": list(report.capacity_violations),
            "is_healthy": not report.capacity_violations,
        },
        ensure_ascii=False,
        indent=2,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    dsn = args.dsn or os.getenv("AGENT_MEMORY_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("AGENT_MEMORY_POSTGRES_DSN is required when --dsn is not set.")
    scope = _build_scope(args)
    policy = _build_policy(args)
    report = asyncio.run(run_health_scan(dsn, scope=scope, policy=policy))
    if args.json:
        print(_serialize_report(report))
    else:
        print(_format_report(report))
    if args.fail_on_violation and report.capacity_violations:
        return 2
    return 0
