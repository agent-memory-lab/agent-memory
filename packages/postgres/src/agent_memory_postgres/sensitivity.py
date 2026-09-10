from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from agent_memory import MemoryScope

from .repository import PostgresMemoryRepository


@dataclass(frozen=True, slots=True)
class SensitivePattern:
    key: str
    regex: str
    severity: str
    description: str


@dataclass(frozen=True, slots=True)
class SensitiveScanPolicy:
    max_hits: int | None = None
    max_critical: int | None = 0
    max_high: int | None = 0
    max_medium: int | None = None
    max_low: int | None = None
    max_per_pattern: int | None = 25


@dataclass(frozen=True, slots=True)
class SensitiveScanHit:
    table: str
    row_id: str
    column: str
    pattern: str
    severity: str
    matched_text: str
    tenant_id: str
    namespace: str
    user_id: str | None
    agent_id: str | None
    workspace_id: str | None
    session_id: str | None
    observed_at: datetime | None


@dataclass(frozen=True, slots=True)
class SensitiveScanReport:
    checked_at: datetime
    scope: MemoryScope | None
    patterns: tuple[str, ...]
    policy: SensitiveScanPolicy
    hits: tuple[SensitiveScanHit, ...]
    by_severity: tuple[tuple[str, int], ...]
    violations: tuple[str, ...]


DEFAULT_PATTERNS: tuple[SensitivePattern, ...] = (
    SensitivePattern(
        key="aws_access_key",
        regex=r"AKIA[0-9A-Z]{16}",
        severity="critical",
        description="AWS access key id",
    ),
    SensitivePattern(
        key="aws_secret_key",
        regex=r"ASIA[0-9A-Z]{16}",
        severity="critical",
        description="AWS session token key",
    ),
    SensitivePattern(
        key="openai_api_key",
        regex=r"sk-[A-Za-z0-9_-]{20,}",
        severity="critical",
        description="OpenAI-style API secret",
    ),
    SensitivePattern(
        key="github_pat",
        regex=r"(gh[pousr]_[A-Za-z0-9_]{30,255}|github_pat_[A-Za-z0-9_]{20,255})",
        severity="high",
        description="GitHub personal token",
    ),
    SensitivePattern(
        key="bearer_token",
        regex=r"bearer[[:space:]]+[A-Za-z0-9_.-]{8,}",
        severity="high",
        description="Bearer token",
    ),
    SensitivePattern(
        key="generic_api_key",
        regex=(
            r"(api[_-]?(secret|key)|secret[_-]?(token|key)|"
            r"access[_-]?(key|token))[[:space:]]*[:=][[:space:]]*"
            r"[\"']?[A-Za-z0-9_-]{16,}"
        ),
        severity="high",
        description="Generic API key assignment",
    ),
    SensitivePattern(
        key="password_like",
        regex=r"password[[:space:]]*[:=][[:space:]]*[\"'][^\"']{6,}",
        severity="medium",
        description="Potential password literal",
    ),
    SensitivePattern(
        key="email",
        regex=r"[[:alnum:]_.+%-]+@[[:alnum:].-]+\.[A-Za-z]{2,}",
        severity="low",
        description="Email-like token",
    ),
    SensitivePattern(
        key="china_mobile",
        regex=r"(^|[^0-9])1[3-9][0-9]{9}([^0-9]|$)",
        severity="low",
        description="Likely mobile phone number",
    ),
)


_SEVERITY_ORDER = ("critical", "high", "medium", "low")
_VALID_SEVERITY = frozenset(_SEVERITY_ORDER)


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


def _normalize_severity(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in _VALID_SEVERITY:
        raise ValueError(f"unsupported severity: {value}")
    return normalized


async def _scan_table_column(
    connection: Any,
    table: str,
    id_column: str,
    tenant_column: str,
    namespace_column: str,
    user_column: str,
    agent_column: str,
    workspace_column: str,
    session_column: str,
    observed_at_column: str,
    text_expression: str,
    scope_where: str,
    scope_params: tuple[Any, ...],
    pattern: SensitivePattern,
    *,
    include_archived: bool,
    limit: int,
) -> tuple[list[tuple[Any, ...]], int]:
    archived_filter = "TRUE" if include_archived else "archived_at IS NULL"
    cursor = await connection.execute(
        f"""
        SELECT
            {id_column} AS row_id,
            {tenant_column} AS tenant_id,
            {namespace_column} AS namespace,
            {user_column} AS user_id,
            {agent_column} AS agent_id,
            {workspace_column} AS workspace_id,
            {session_column} AS session_id,
            {observed_at_column} AS observed_at,
            substring(({text_expression}) FROM %s) AS matched_text,
            COUNT(*) OVER() AS total_matches
        FROM {table}
        WHERE ({scope_where})
          AND ({archived_filter})
          AND ({text_expression} ~* %s)
        ORDER BY {observed_at_column} DESC NULLS LAST
        LIMIT %s
        """,
        (pattern.regex, *scope_params, pattern.regex, max(limit, 1)),
    )
    rows = list(await cursor.fetchall())
    total_matches = int(rows[0]["total_matches"]) if rows else 0
    return rows, total_matches


def _policy_violations(hits: dict[str, int], policy: SensitiveScanPolicy) -> tuple[str, ...]:
    reasons: list[str] = []
    total = sum(hits.values())
    if policy.max_hits is not None and total > policy.max_hits:
        reasons.append(f"total sensitive matches {total} > max_hits={policy.max_hits}")
    severities = {
        "critical": policy.max_critical,
        "high": policy.max_high,
        "medium": policy.max_medium,
        "low": policy.max_low,
    }
    for severity in _SEVERITY_ORDER:
        limit = severities[severity]
        if limit is None:
            continue
        if hits.get(severity, 0) > limit:
            reasons.append(f"{severity} matches {hits.get(severity, 0)} > max_{severity}={limit}")
    return tuple(reasons)


async def collect_sensitive_scan(
    repository: PostgresMemoryRepository,
    scope: MemoryScope | None = None,
    *,
    patterns: Sequence[SensitivePattern] | None = None,
    policy: SensitiveScanPolicy | None = None,
    include_archived: bool = False,
) -> SensitiveScanReport:
    policy = policy or SensitiveScanPolicy()
    chosen_patterns = tuple(patterns or DEFAULT_PATTERNS)
    scope_where, scope_params = _scope_clause(scope)

    targets = (
        (
            "agent_memory_events",
            "id",
            "tenant_id",
            "namespace",
            "user_id",
            "agent_id",
            "workspace_id",
            "session_id",
            "occurred_at",
            "content",
        ),
        (
            "agent_memory_events",
            "id",
            "tenant_id",
            "namespace",
            "user_id",
            "agent_id",
            "workspace_id",
            "session_id",
            "occurred_at",
            "metadata_json::text",
        ),
        (
            "agent_memory_claims",
            "id",
            "tenant_id",
            "namespace",
            "user_id",
            "agent_id",
            "workspace_id",
            "session_id",
            "created_at",
            "text",
        ),
        (
            "agent_memory_claims",
            "id",
            "tenant_id",
            "namespace",
            "user_id",
            "agent_id",
            "workspace_id",
            "session_id",
            "created_at",
            "value_json::text",
        ),
        (
            "agent_memory_artifacts",
            "id",
            "tenant_id",
            "namespace",
            "user_id",
            "agent_id",
            "workspace_id",
            "session_id",
            "occurred_at",
            "text",
        ),
        (
            "agent_memory_artifacts",
            "id",
            "tenant_id",
            "namespace",
            "user_id",
            "agent_id",
            "workspace_id",
            "session_id",
            "occurred_at",
            "payload_json::text",
        ),
    )

    hits: list[SensitiveScanHit] = []
    counts = {severity: 0 for severity in _SEVERITY_ORDER}
    async with repository.pool.connection() as connection:
        for pattern in chosen_patterns:
            severity = _normalize_severity(pattern.severity)
            sample_remaining = max(policy.max_per_pattern or 0, 0)
            for (
                table,
                id_column,
                tenant_column,
                namespace_column,
                user_column,
                agent_column,
                workspace_column,
                session_column,
                observed_at_column,
                text_expression,
            ) in targets:
                rows, total_matches = await _scan_table_column(
                    connection=connection,
                    table=table,
                    id_column=id_column,
                    tenant_column=tenant_column,
                    namespace_column=namespace_column,
                    user_column=user_column,
                    agent_column=agent_column,
                    workspace_column=workspace_column,
                    session_column=session_column,
                    observed_at_column=observed_at_column,
                    text_expression=text_expression,
                    scope_where=scope_where,
                    scope_params=scope_params,
                    pattern=pattern,
                    include_archived=include_archived,
                    limit=max(sample_remaining, 1),
                )
                counts[severity] += total_matches
                for row in rows[:sample_remaining]:
                    matched_text = row["matched_text"]
                    if matched_text is None:
                        continue
                    hits.append(
                        SensitiveScanHit(
                            table=table,
                            row_id=row["row_id"],
                            column=text_expression,
                            pattern=pattern.key,
                            severity=severity,
                            matched_text="[REDACTED]",
                            tenant_id=row["tenant_id"],
                            namespace=row["namespace"],
                            user_id=row["user_id"],
                            agent_id=row["agent_id"],
                            workspace_id=row["workspace_id"],
                            session_id=row["session_id"],
                            observed_at=row["observed_at"],
                        )
                    )
                    sample_remaining -= 1

    return SensitiveScanReport(
        checked_at=datetime.now(UTC),
        scope=scope,
        patterns=tuple(pattern.key for pattern in chosen_patterns),
        policy=policy,
        hits=tuple(hits),
        by_severity=tuple((severity, counts[severity]) for severity in _SEVERITY_ORDER),
        violations=_policy_violations(counts, policy),
    )


async def run_sensitive_scan(
    dsn: str,
    scope: MemoryScope | None = None,
    *,
    patterns: Sequence[SensitivePattern] | None = None,
    policy: SensitiveScanPolicy | None = None,
    include_archived: bool = False,
) -> SensitiveScanReport:
    repository = PostgresMemoryRepository.from_dsn(dsn)
    await repository.pool.open()
    try:
        return await collect_sensitive_scan(
            repository,
            scope=scope,
            patterns=patterns,
            policy=policy,
            include_archived=include_archived,
        )
    finally:
        await repository.close()


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


def _format_report(report: SensitiveScanReport, *, include_hits: bool = True) -> str:
    lines = [
        f"checked_at: {report.checked_at.isoformat()}",
        f"scope: {'global' if report.scope is None else _scope_dict(report.scope)}",
        f"patterns: {', '.join(report.patterns)}",
    ]
    total = sum(count for _, count in report.by_severity)
    lines.append(f"total_matches: {total}")
    lines.append("matches by severity:")
    for severity, count in report.by_severity:
        lines.append(f"  {severity}: {count}")
    if report.violations:
        lines.append("policy violations:")
        lines.extend(f"  - {item}" for item in report.violations)
    else:
        lines.append("policy violations: none")
    if include_hits:
        lines.append("matches:")
        if not report.hits:
            lines.append("  none")
        else:
            for hit in report.hits:
                lines.append(
                    f"  - [{hit.severity}] {hit.table}:{hit.row_id} "
                    f"{hit.pattern}({hit.column}) match={hit.matched_text!r}"
                )
    return "\n".join(lines)


def _serialize_report(report: SensitiveScanReport) -> str:
    return json.dumps(
        {
            "checked_at": report.checked_at.isoformat(),
            "scope": _scope_dict(report.scope),
            "patterns": list(report.patterns),
            "policy": {
                "max_hits": report.policy.max_hits,
                "max_critical": report.policy.max_critical,
                "max_high": report.policy.max_high,
                "max_medium": report.policy.max_medium,
                "max_low": report.policy.max_low,
                "max_per_pattern": report.policy.max_per_pattern,
            },
            "by_severity": dict(report.by_severity),
            "hits": [
                {
                    "table": hit.table,
                    "row_id": hit.row_id,
                    "column": hit.column,
                    "pattern": hit.pattern,
                    "severity": hit.severity,
                    "matched_text": hit.matched_text,
                    "scope": {
                        "tenant_id": hit.tenant_id,
                        "namespace": hit.namespace,
                        "user_id": hit.user_id,
                        "agent_id": hit.agent_id,
                        "workspace_id": hit.workspace_id,
                        "session_id": hit.session_id,
                    },
                    "observed_at": hit.observed_at.isoformat() if hit.observed_at else None,
                }
                for hit in report.hits
            ],
            "violations": list(report.violations),
            "is_healthy": not report.violations,
        },
        ensure_ascii=False,
        indent=2,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan PostgreSQL memory tables for sensitive patterns."
    )
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
    parser.add_argument("--include-archived", action="store_true", help="Include archived rows.")
    parser.add_argument("--max-hits", type=int, default=None)
    parser.add_argument("--max-critical", type=int, default=0)
    parser.add_argument("--max-high", type=int, default=0)
    parser.add_argument("--max-medium", type=int, default=None)
    parser.add_argument("--max-low", type=int, default=None)
    parser.add_argument("--max-per-pattern", type=int, default=25)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON summary.",
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


def _build_policy(args: argparse.Namespace) -> SensitiveScanPolicy:
    return SensitiveScanPolicy(
        max_hits=args.max_hits,
        max_critical=args.max_critical,
        max_high=args.max_high,
        max_medium=args.max_medium,
        max_low=args.max_low,
        max_per_pattern=args.max_per_pattern,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    dsn = args.dsn or os.getenv("AGENT_MEMORY_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("AGENT_MEMORY_POSTGRES_DSN is required when --dsn is not set.")

    scope = _build_scope(args)
    policy = _build_policy(args)
    report = asyncio.run(
        run_sensitive_scan(
            dsn,
            scope=scope,
            policy=policy,
            include_archived=args.include_archived,
        )
    )
    if args.json:
        print(_serialize_report(report))
    else:
        print(_format_report(report))
    if args.fail_on_violation and report.violations:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
