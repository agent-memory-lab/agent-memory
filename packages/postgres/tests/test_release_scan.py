import zipfile
from pathlib import Path

from agent_memory_postgres.release_scan import ReleaseScanPolicy, scan_release_paths


def _token() -> str:
    return "sk-" + ("A" * 24)


def test_release_scan_detects_and_redacts_source_secret(tmp_path: Path):
    source = tmp_path / "settings.py"
    source.write_text(f'API_KEY = "{_token()}"', encoding="utf-8")

    report = scan_release_paths([tmp_path])

    assert report.total_findings >= 1
    assert report.violations
    assert all(hit.matched_text == "[REDACTED]" for hit in report.hits)
    assert _token() not in repr(report)


def test_release_scan_checks_package_archives(tmp_path: Path):
    dist = tmp_path / "dist"
    dist.mkdir()
    archive_path = dist / "example.whl"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("example/config.py", f'TOKEN = "{_token()}"')

    report = scan_release_paths([tmp_path])

    assert report.scanned_archives == 1
    assert report.total_findings >= 1
    assert report.hits[0].member == "example/config.py"


def test_release_scan_counts_all_findings_but_bounds_report_memory(tmp_path: Path):
    source = tmp_path / "settings.txt"
    source.write_text("\n".join([_token()] * 5), encoding="utf-8")

    report = scan_release_paths(
        [tmp_path],
        policy=ReleaseScanPolicy(max_findings=0, max_reported_hits=2),
    )

    assert report.total_findings == 5
    assert len(report.hits) == 2
    assert report.violations
