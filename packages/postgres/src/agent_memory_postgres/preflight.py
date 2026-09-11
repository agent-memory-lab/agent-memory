from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass

from agent_memory import MemoryScope

from .health import HealthCapacityPolicy, MemoryHealthReport, run_health_scan
from .release_scan import ReleaseScanPolicy, ReleaseScanReport, scan_release_paths
from .sensitivity import SensitiveScanPolicy, SensitiveScanReport, run_sensitive_scan


@dataclass(frozen=True, slots=True)
class BuildResult:
    path: str
    command: tuple[str, ...]
    returncode: int
    elapsed_seconds: float
    stdout: str
    stderr: str


def _scope_clause(scope: MemoryScope | None) -> dict[str, str | None] | None:
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


def _scope_from_args(args: argparse.Namespace) -> MemoryScope | None:
    if args.all_scopes or (args.skip_health and args.skip_sensitive_scan):
        return None
    return MemoryScope(
        tenant_id=args.tenant_id,
        namespace=args.namespace,
        user_id=args.user_id,
        agent_id=args.agent_id,
        workspace_id=args.workspace_id,
        session_id=args.session_id,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run memory preflight: health check, sensitive scan, and package build."
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help=(
            "PostgreSQL DSN; required for health/sensitive checks unless "
            "AGENT_MEMORY_POSTGRES_DSN is set."
        ),
    )

    parser.add_argument(
        "--all-scopes",
        action="store_true",
        help="Run checks for all tenant/namespace scopes (no scope filter).",
    )
    parser.add_argument("--tenant-id", default=None, help="Tenant scope filter.")
    parser.add_argument("--namespace", default=None, help="Namespace scope filter.")
    parser.add_argument("--user-id", default=None)
    parser.add_argument("--agent-id", default=None)
    parser.add_argument("--workspace-id", default=None)
    parser.add_argument("--session-id", default=None)

    parser.add_argument(
        "--include-archived",
        action="store_true",
        help="Include archived rows in scans.",
    )
    parser.add_argument(
        "--max-active-events",
        type=int,
        help="Fail if active events exceed this value.",
    )
    parser.add_argument(
        "--max-active-claims",
        type=int,
        help="Fail if active claims exceed this value.",
    )
    parser.add_argument(
        "--max-active-blocks",
        type=int,
        help="Fail if active blocks exceed this value.",
    )
    parser.add_argument(
        "--max-orphan-vectors",
        type=int,
        help="Fail if orphan vectors exceed this value.",
    )
    parser.add_argument(
        "--max-dead-jobs",
        type=int,
        help="Fail if dead consolidation jobs exceed this value.",
    )
    parser.add_argument(
        "--max-expired-leases",
        type=int,
        help="Fail if expired queue leases exceed this value.",
    )

    parser.add_argument("--max-hits", type=int, default=None)
    parser.add_argument("--max-critical", type=int, default=0)
    parser.add_argument("--max-high", type=int, default=0)
    parser.add_argument("--max-medium", type=int, default=None)
    parser.add_argument("--max-low", type=int, default=None)
    parser.add_argument("--max-per-pattern", type=int, default=25)

    parser.add_argument(
        "--build-path",
        action="append",
        dest="build_paths",
        default=None,
        help="Package directory to run `python -m build`; can be repeated.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip package build verification.",
    )
    parser.add_argument(
        "--skip-health",
        action="store_true",
        help="Skip PostgreSQL release health check.",
    )
    parser.add_argument(
        "--skip-sensitive-scan",
        action="store_true",
        help="Skip sensitive information scan.",
    )
    parser.add_argument(
        "--release-scan-path",
        action="append",
        dest="release_scan_paths",
        default=None,
        help="Source directory or package archive to scan; can be repeated.",
    )
    parser.add_argument(
        "--skip-release-scan",
        action="store_true",
        help="Skip source and built-package secret scanning.",
    )
    parser.add_argument("--max-release-findings", type=int, default=0)
    parser.add_argument("--max-release-report-hits", type=int, default=100)
    parser.add_argument("--max-release-archive-members", type=int, default=10_000)
    parser.add_argument(
        "--build-python",
        default=sys.executable,
        help="Python executable used for build steps.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON result.",
    )
    parser.add_argument(
        "--no-fail-on-violation",
        action="store_true",
        help="Do not exit non-zero when violations are found.",
    )

    args = parser.parse_args(argv)
    database_checks_enabled = not args.skip_health or not args.skip_sensitive_scan
    if (
        database_checks_enabled
        and not args.all_scopes
        and (args.tenant_id is None or args.namespace is None)
    ):
        parser.error("--tenant-id and --namespace are required unless --all-scopes is set.")
    return args


def _build_health_policy(args: argparse.Namespace) -> HealthCapacityPolicy | None:
    if (
        args.max_active_events is None
        and args.max_active_claims is None
        and args.max_active_blocks is None
        and args.max_orphan_vectors is None
        and args.max_dead_jobs is None
        and args.max_expired_leases is None
    ):
        return None
    return HealthCapacityPolicy(
        max_active_events=args.max_active_events,
        max_active_claims=args.max_active_claims,
        max_active_blocks=args.max_active_blocks,
        max_orphan_vectors=args.max_orphan_vectors,
        max_queue_dead=args.max_dead_jobs,
        max_expired_leases=args.max_expired_leases,
    )


def _build_sensitive_policy(args: argparse.Namespace) -> SensitiveScanPolicy:
    return SensitiveScanPolicy(
        max_hits=args.max_hits,
        max_critical=args.max_critical,
        max_high=args.max_high,
        max_medium=args.max_medium,
        max_low=args.max_low,
        max_per_pattern=args.max_per_pattern,
    )


def _run_build(path: str, python_executable: str) -> BuildResult:
    command: tuple[str, ...] = (python_executable, "-m", "build", path)
    start = time.perf_counter()
    process = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = time.perf_counter() - start
    return BuildResult(
        path=path,
        command=command,
        returncode=process.returncode,
        elapsed_seconds=elapsed,
        stdout=process.stdout.strip(),
        stderr=process.stderr.strip(),
    )


async def _run_checks(
    dsn: str | None,
    scope: MemoryScope | None,
    args: argparse.Namespace,
) -> tuple[
    MemoryHealthReport | None,
    SensitiveScanReport | None,
    tuple[BuildResult, ...],
    ReleaseScanReport | None,
]:
    health_report = None
    sensitive_report = None
    build_results: list[BuildResult] = []
    if not args.skip_health:
        assert dsn is not None
        health_report = await run_health_scan(
            dsn,
            scope=scope,
            policy=_build_health_policy(args),
        )
    if not args.skip_sensitive_scan:
        assert dsn is not None
        sensitive_report = await run_sensitive_scan(
            dsn,
            scope=scope,
            policy=_build_sensitive_policy(args),
            include_archived=args.include_archived,
        )
    if not args.skip_build:
        for raw_path in args.build_paths or ["packages/postgres"]:
            build_results.append(_run_build(raw_path, args.build_python))
    release_report = None
    if not args.skip_release_scan:
        release_report = scan_release_paths(
            args.release_scan_paths or ["."],
            policy=ReleaseScanPolicy(
                max_findings=args.max_release_findings,
                max_reported_hits=args.max_release_report_hits,
                max_archive_members=args.max_release_archive_members,
            ),
        )
    return health_report, sensitive_report, tuple(build_results), release_report


def _print_human(
    scope: MemoryScope | None,
    health: MemoryHealthReport | None,
    sensitive: SensitiveScanReport | None,
    build_results: tuple[BuildResult, ...],
    release: ReleaseScanReport | None,
) -> None:
    print(f"preflight scope: {'global' if scope is None else _scope_clause(scope)}")
    if health is None:
        print("health: skipped")
    else:
        print(
            "health: "
            + ("ok" if not health.capacity_violations else "violations detected")
            + f" ({health.storage.events_active}/{health.storage.events_archived} events, "
            f"{health.storage.claims_active}/{health.storage.claims_archived} claims)"
        )
        if health.capacity_violations:
            for item in health.capacity_violations:
                print(f"  - {item}")

    if sensitive is None:
        print("sensitive scan: skipped")
    else:
        total_hits = sum(count for _, count in sensitive.by_severity)
        print(
            "sensitive scan: "
            + ("ok" if not sensitive.violations else "violations detected")
            + f" ({total_hits} matches)"
        )
        if sensitive.violations:
            for item in sensitive.violations:
                print(f"  - {item}")

    if not build_results:
        print("build: skipped")
    else:
        for result in build_results:
            status = "ok" if result.returncode == 0 else "failed"
            print(f"build[{result.path}]: {status} in {result.elapsed_seconds:.2f}s")
            if result.returncode != 0:
                if result.stdout:
                    print(f"  stdout: {result.stdout}")
                if result.stderr:
                    print(f"  stderr: {result.stderr}")

    if release is None:
        print("release scan: skipped")
    else:
        status = "ok" if not release.violations else "violations detected"
        print(
            f"release scan: {status} "
            f"({release.total_findings} findings, "
            f"{release.scanned_files} files, {release.scanned_archives} archives, "
            f"complete={release.scan_complete})"
        )
        for item in release.violations:
            print(f"  - {item}")


def _serialize(
    scope: MemoryScope | None,
    health: MemoryHealthReport | None,
    sensitive: SensitiveScanReport | None,
    build_results: tuple[BuildResult, ...],
    release: ReleaseScanReport | None,
) -> str:
    return json.dumps(
        {
            "scope": _scope_clause(scope),
            "health": None
            if health is None
            else {
                "checked_at": health.checked_at.isoformat(),
                "storage": {
                    "events_active": health.storage.events_active,
                    "events_archived": health.storage.events_archived,
                    "claims_active": health.storage.claims_active,
                    "claims_archived": health.storage.claims_archived,
                    "blocks_active": health.storage.blocks_active,
                    "blocks_archived": health.storage.blocks_archived,
                    "episodes_active": health.storage.episodes_active,
                    "episodes_archived": health.storage.episodes_archived,
                    "procedures_active": health.storage.procedures_active,
                    "procedures_archived": health.storage.procedures_archived,
                },
                "queue": {
                    "pending": health.queue.pending,
                    "running": health.queue.running,
                    "completed": health.queue.completed,
                    "dead": health.queue.dead,
                    "expired_leases": health.queue.expired_leases,
                    "oldest_pending_age_seconds": health.queue.oldest_pending_age_seconds,
                    "oldest_dead_age_seconds": health.queue.oldest_dead_age_seconds,
                },
                "vectors": {
                    "vector_rows": health.vectors.vector_rows,
                    "orphan_vectors": health.vectors.orphan_vectors,
                    "vectors_for_archived_blocks": health.vectors.vectors_for_archived_blocks,
                },
                "dead_jobs": [
                    {
                        "id": job.id,
                        "job_key": job.job_key,
                        "attempts": job.attempts,
                        "max_attempts": job.max_attempts,
                        "last_error": job.last_error,
                    }
                    for job in health.dead_jobs
                ],
                "capacity_violations": list(health.capacity_violations),
                "is_healthy": not health.capacity_violations,
            },
            "sensitive_scan": None
            if sensitive is None
            else {
                "checked_at": sensitive.checked_at.isoformat(),
                "patterns": list(sensitive.patterns),
                "by_severity": dict(sensitive.by_severity),
                "violations": list(sensitive.violations),
                "is_healthy": not sensitive.violations,
                "total_hits": sum(count for _, count in sensitive.by_severity),
            },
            "build": [
                {
                    "path": item.path,
                    "command": item.command,
                    "returncode": item.returncode,
                    "elapsed_seconds": round(item.elapsed_seconds, 2),
                    "stdout": item.stdout,
                    "stderr": item.stderr,
                }
                for item in build_results
            ],
            "release_scan": None
            if release is None
            else {
                "roots": list(release.roots),
                "scanned_files": release.scanned_files,
                "scanned_archives": release.scanned_archives,
                "skipped_large_files": release.skipped_large_files,
                "total_findings": release.total_findings,
                "scan_complete": release.scan_complete,
                "hits": [
                    {
                        "path": hit.path,
                        "member": hit.member,
                        "line": hit.line,
                        "pattern": hit.pattern,
                        "severity": hit.severity,
                        "matched_text": hit.matched_text,
                    }
                    for hit in release.hits
                ],
                "violations": list(release.violations),
                "is_healthy": not release.violations,
            },
        },
        ensure_ascii=False,
        indent=2,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    dsn = args.dsn or os.getenv("AGENT_MEMORY_POSTGRES_DSN")
    if (not args.skip_health or not args.skip_sensitive_scan) and not dsn:
        raise SystemExit("AGENT_MEMORY_POSTGRES_DSN is required when --dsn is not set.")
    if args.skip_health and args.skip_sensitive_scan:
        dsn = None

    scope = _scope_from_args(args)
    health_report, sensitive_report, build_results, release_report = asyncio.run(
        _run_checks(dsn, scope, args)
    )

    failed = False
    if health_report is not None and health_report.capacity_violations:
        failed = True
    if sensitive_report is not None and sensitive_report.violations:
        failed = True
    if any(item.returncode != 0 for item in build_results):
        failed = True
    if release_report is not None and release_report.violations:
        failed = True

    if args.json:
        print(
            _serialize(
                scope,
                health_report,
                sensitive_report,
                build_results,
                release_report,
            )
        )
    else:
        _print_human(
            scope,
            health_report,
            sensitive_report,
            build_results,
            release_report,
        )

    if failed and not args.no_fail_on_violation:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
