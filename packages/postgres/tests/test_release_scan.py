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


def test_release_scan_skips_archive_directory_members(tmp_path: Path):
    dist = tmp_path / "dist"
    dist.mkdir()
    archive_path = dist / "pkg.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("dir/", "")
        archive.writestr("dir/credentials.py", f'TOKEN = "{_token()}"')

    report = scan_release_paths([tmp_path], policy=ReleaseScanPolicy(max_findings=0))

    assert report.total_findings >= 1
    assert report.hits
    assert report.hits[0].member == "dir/credentials.py"
    assert report.violations


def test_release_scan_reports_archived_scan_errors(tmp_path: Path):
    bad_archive = tmp_path / "bad.whl"
    bad_archive.write_bytes(b"not-a-archive")

    report = scan_release_paths([tmp_path], policy=ReleaseScanPolicy(max_findings=10))

    assert report.scanned_archives == 1
    assert any("cannot scan archive" in issue for issue in report.violations)


def test_release_scan_reports_missing_path(tmp_path: Path):
    missing_path = tmp_path / "missing_root"
    report = scan_release_paths([missing_path], policy=ReleaseScanPolicy(max_findings=10))

    assert any("scan path missing" in issue for issue in report.violations)
    assert not report.total_findings
    assert report.scanned_files == 0


def test_release_scan_reports_archive_member_cap(tmp_path: Path):
    dist = tmp_path / "dist"
    dist.mkdir()
    archive_path = dist / "pkg.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a.py", _token())
        archive.writestr("b.py", _token())

    report = scan_release_paths(
        [tmp_path],
        policy=ReleaseScanPolicy(max_findings=10, max_archive_members=1),
    )

    assert report.scanned_archives == 1
    assert any("archive member cap reached" in issue for issue in report.violations)


def test_release_scan_rejects_negative_limits(tmp_path: Path):
    source = tmp_path / "settings.py"
    source.write_text("abc", encoding="utf-8")

    report = scan_release_paths(
        [tmp_path],
        policy=ReleaseScanPolicy(max_findings=-1, max_reported_hits=-2),
    )

    assert any("invalid max_findings" in issue for issue in report.violations)
    assert any("invalid max_reported_hits" in issue for issue in report.violations)


def test_release_scan_rejects_negative_archive_member_limit(tmp_path: Path):
    source = tmp_path / "settings.py"
    source.write_text("abc", encoding="utf-8")

    report = scan_release_paths(
        [tmp_path],
        policy=ReleaseScanPolicy(max_findings=10, max_archive_members=-1),
    )

    assert any("invalid max_archive_members" in issue for issue in report.violations)


def test_release_scan_complete_when_no_io_errors(tmp_path: Path):
    source = tmp_path / "ok.py"
    source.write_text(f'TOKEN = "{_token()}"', encoding="utf-8")

    report = scan_release_paths([tmp_path], policy=ReleaseScanPolicy(max_findings=10))

    assert report.scan_complete is True


def test_release_scan_incomplete_when_archive_read_error(tmp_path: Path):
    bad_archive = tmp_path / "bad.whl"
    bad_archive.write_bytes(b"not-a-archive")

    report = scan_release_paths([tmp_path], policy=ReleaseScanPolicy(max_findings=10))

    assert report.scan_complete is False
