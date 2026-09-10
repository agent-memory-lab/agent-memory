import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from agent_memory_postgres.sensitivity import (
    SensitivePattern,
    SensitiveScanHit,
    SensitiveScanPolicy,
    SensitiveScanReport,
    _format_report,
    _scan_table_column,
    _serialize_report,
    collect_sensitive_scan,
)


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    async def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def execute(self, query, params):
        self.calls.append((query, params))
        return _Cursor(self.rows)


class _ConnectionContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return False


def _row(total_matches=1, matched_text="secret-value"):
    return {
        "row_id": "event-1",
        "tenant_id": "tenant-a",
        "namespace": "namespace-a",
        "user_id": None,
        "agent_id": None,
        "workspace_id": None,
        "session_id": None,
        "observed_at": datetime.now(UTC),
        "matched_text": matched_text,
        "total_matches": total_matches,
    }


def test_scoped_query_binds_regex_before_scope_parameters():
    connection = _Connection([_row()])
    pattern = SensitivePattern("test", "SECRET", "high", "test")

    rows, total = asyncio.run(
        _scan_table_column(
            connection,
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
            "tenant_id = %s AND namespace = %s",
            ("tenant-a", "namespace-a"),
            pattern,
            include_archived=False,
            limit=25,
        )
    )

    assert rows
    assert total == 1
    assert connection.calls[0][1] == (
        "SECRET",
        "tenant-a",
        "namespace-a",
        "SECRET",
        25,
    )


def test_zero_hit_threshold_still_scans_and_reports_violation():
    connection = _Connection([_row(total_matches=1)])
    repository = SimpleNamespace(
        pool=SimpleNamespace(connection=lambda: _ConnectionContext(connection))
    )

    report = asyncio.run(
        collect_sensitive_scan(
            repository,
            patterns=(SensitivePattern("test", "SECRET", "high", "test"),),
            policy=SensitiveScanPolicy(max_hits=0, max_high=0, max_per_pattern=1),
        )
    )

    assert len(connection.calls) == 6
    assert dict(report.by_severity)["high"] == 6
    assert report.violations
    assert all(hit.matched_text == "[REDACTED]" for hit in report.hits)


def test_reports_never_include_detected_secret():
    secret = "Bearer " + "private-token-value"
    hit = SensitiveScanHit(
        table="agent_memory_events",
        row_id="event-1",
        column="content",
        pattern="bearer_token",
        severity="high",
        matched_text="[REDACTED]",
        tenant_id="tenant-a",
        namespace="namespace-a",
        user_id=None,
        agent_id=None,
        workspace_id=None,
        session_id=None,
        observed_at=None,
    )
    report = SensitiveScanReport(
        checked_at=datetime.now(UTC),
        scope=None,
        patterns=("bearer_token",),
        policy=SensitiveScanPolicy(),
        hits=(hit,),
        by_severity=(("critical", 0), ("high", 1), ("medium", 0), ("low", 0)),
        violations=("high matches 1 > max_high=0",),
    )

    assert secret not in _format_report(report)
    assert secret not in _serialize_report(report)
    assert "[REDACTED]" in _format_report(report)
    assert "[REDACTED]" in _serialize_report(report)
