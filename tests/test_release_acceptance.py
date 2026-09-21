"""T43 fail-closed release acceptance tests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import zipfile

from agent_memory import (
    ReleaseAcceptancePlan,
    ReleaseCheckKind,
    ReleaseCheckOutcome,
    ReleaseCheckStatus,
    ReleaseClaim,
    ReleaseAcceptanceStatus,
    evaluate_release_acceptance,
    run_documentation_consistency_check,
    run_release_checks,
    run_sensitive_information_check,
)


NOW = datetime(2026, 9, 21, 11, 0, tzinfo=timezone.utc)


def _plan() -> ReleaseAcceptancePlan:
    return ReleaseAcceptancePlan(
        release_version="0.1.0",
        fixed_clock=NOW,
        python_version="3.13",
        required_packages=(
            "agent-memory",
            "agent-memory-sdk",
            "agent-memory-mcp",
            "agent-memory-evolution",
            "agent-memory-postgres",
        ),
        claims=(
            ReleaseClaim(
                "sqlite-ready",
                "SQLite provider passes its real-backend contract",
                (ReleaseCheckKind.SQLITE,),
            ),
            ReleaseClaim(
                "plugin-api-ready",
                "Published API and capability matrix match implementation",
                (ReleaseCheckKind.DOCUMENTATION_API,),
            ),
        ),
    )


def _passing_outcomes():
    outcomes = []
    for kind in ReleaseCheckKind:
        subjects = _plan().required_packages if kind is ReleaseCheckKind.BUILD_INSTALL else ()
        outcomes.append(
            ReleaseCheckOutcome(
                check_id=f"check-{kind.value}",
                kind=kind,
                status=ReleaseCheckStatus.PASS,
                summary="verified",
                subjects=subjects,
            )
        )
    return tuple(outcomes)


def test_release_requires_every_check_zero_skips_and_claim_evidence():
    report = evaluate_release_acceptance(_plan(), _passing_outcomes())
    assert report.status is ReleaseAcceptanceStatus.READY
    assert report.reasons == ()
    assert {item.claim_id for item in report.claims} == {
        "sqlite-ready",
        "plugin-api-ready",
    }
    assert all(item.supported for item in report.claims)

    skipped = tuple(
        ReleaseCheckOutcome(
            item.check_id,
            item.kind,
            ReleaseCheckStatus.SKIPPED,
            "service unavailable",
            skipped=1,
            subjects=item.subjects,
        )
        if item.kind is ReleaseCheckKind.POSTGRESQL
        else item
        for item in _passing_outcomes()
    )
    blocked = evaluate_release_acceptance(_plan(), skipped)
    assert blocked.status is ReleaseAcceptanceStatus.BLOCKED
    assert any("postgresql" in reason and "skipped" in reason for reason in blocked.reasons)

    missing = evaluate_release_acceptance(
        _plan(),
        tuple(item for item in _passing_outcomes() if item.kind is not ReleaseCheckKind.FAULT),
    )
    assert missing.status is ReleaseAcceptanceStatus.BLOCKED
    assert "fault check is unmeasured" in missing.reasons


def test_build_coverage_and_unsupported_claims_block_release():
    incomplete = tuple(
        ReleaseCheckOutcome(
            item.check_id,
            item.kind,
            item.status,
            item.summary,
            subjects=("agent-memory",) if item.kind is ReleaseCheckKind.BUILD_INSTALL else (),
        )
        for item in _passing_outcomes()
    )
    report = evaluate_release_acceptance(_plan(), incomplete)
    assert report.status is ReleaseAcceptanceStatus.BLOCKED
    assert any("build/install evidence is missing" in reason for reason in report.reasons)

    failed_sqlite = tuple(
        ReleaseCheckOutcome(
            item.check_id,
            item.kind,
            ReleaseCheckStatus.FAIL if item.kind is ReleaseCheckKind.SQLITE else item.status,
            "failed" if item.kind is ReleaseCheckKind.SQLITE else item.summary,
            failures=1 if item.kind is ReleaseCheckKind.SQLITE else 0,
            subjects=item.subjects,
        )
        for item in _passing_outcomes()
    )
    report = evaluate_release_acceptance(_plan(), failed_sqlite)
    sqlite_claim = next(item for item in report.claims if item.claim_id == "sqlite-ready")
    assert not sqlite_claim.supported
    assert ReleaseCheckKind.SQLITE in sqlite_claim.missing_or_failed_checks


def test_sensitive_scan_redacts_values_and_scans_build_artifacts(tmp_path):
    source = tmp_path / "config.py"
    github_fixture = "ghp_" + "abcdefghijklmnopqrstuvwxyz1234567890"
    source.write_text(f'TOKEN = "{github_fixture}"\n', encoding="utf-8")
    wheel = tmp_path / "package.whl"
    openai_fixture = "sk-" + "abcdefghijklmnopqrstuvwxyz1234567890"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "agent_memory/config.txt",
            f"OPENAI_API_KEY={openai_fixture}",
        )

    outcome = run_sensitive_information_check((tmp_path,))
    assert outcome.status is ReleaseCheckStatus.FAIL
    assert outcome.findings == 2
    assert len(outcome.sensitive_findings) == 2
    assert {item.rule for item in outcome.sensitive_findings} == {
        "github-token",
        "openai-api-key",
    }
    assert all(len(item.fingerprint) == 12 for item in outcome.sensitive_findings)
    assert "ghp_" not in outcome.summary and "sk-" not in outcome.summary

    allowed = run_sensitive_information_check(
        (tmp_path,),
        allow_fingerprints=tuple(item.fingerprint for item in outcome.sensitive_findings),
    )
    assert allowed.status is ReleaseCheckStatus.PASS
    assert allowed.findings == 0


def test_documentation_api_check_and_runner_fail_closed(tmp_path):
    matching = run_documentation_consistency_check(
        declared_capabilities={"memory_blocks": True, "graph_memory": False},
        actual_capabilities={"memory_blocks": True, "graph_memory": False},
        required_api_symbols=("MemoryKernel", "MemoryEvent"),
        public_namespace={"MemoryKernel": object(), "MemoryEvent": object()},
    )
    assert matching.status is ReleaseCheckStatus.PASS

    mismatch = run_documentation_consistency_check(
        declared_capabilities={"graph_memory": True},
        actual_capabilities={"graph_memory": False},
        required_api_symbols=("MissingApi",),
        public_namespace={},
    )
    assert mismatch.status is ReleaseCheckStatus.FAIL
    assert mismatch.failures == 2

    async def scenario():
        async def passed():
            return ReleaseCheckOutcome(
                "unit",
                ReleaseCheckKind.UNIT,
                ReleaseCheckStatus.PASS,
                "ok",
            )

        async def crashed():
            raise RuntimeError("secret internal output must not leak")

        outcomes = await run_release_checks(
            {
                ReleaseCheckKind.UNIT: passed,
                ReleaseCheckKind.CONTRACT: crashed,
            }
        )
        assert outcomes[0].status is ReleaseCheckStatus.PASS
        assert outcomes[1].status is ReleaseCheckStatus.FAIL
        assert outcomes[1].summary == "runner raised RuntimeError"

    asyncio.run(scenario())


def test_reports_are_machine_and_human_readable():
    report = evaluate_release_acceptance(_plan(), _passing_outcomes())
    assert '"status":"ready"' in report.to_json()
    markdown = report.to_markdown()
    assert "# Agent Memory Release Acceptance" in markdown
    assert "READY" in markdown
    assert "agent-memory-postgres" in markdown
