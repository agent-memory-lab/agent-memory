import io
import json
import struct
import tarfile
import zipfile
from pathlib import Path

import pytest
from agent_memory_postgres.release_scan import ReleaseScanPolicy, main, scan_release_paths


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


@pytest.mark.parametrize("failure, error", [
    ("crc", zipfile.BadZipFile),
    ("compression", NotImplementedError),
    ("encrypted", RuntimeError),
])
def test_unreadable_zip_member_fails_release_gate(tmp_path, capsys, failure, error):
    archive_path = tmp_path / "package.whl"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("config.py", _token())
        member = archive.getinfo("config.py")
    assert scan_release_paths([archive_path]).total_findings == 1
    content = bytearray(archive_path.read_bytes())
    local = member.header_offset
    central = content.index(b"PK\x01\x02")
    if failure == "crc":
        payload = local + 30 + len(member.filename.encode()) + len(member.extra)
        content[payload] ^= 1
    elif failure == "compression":
        # Keep valid ZIP headers while declaring an unsupported compression method.
        struct.pack_into("<H", content, local + 8, 99)
        struct.pack_into("<H", content, central + 10, 99)
    else:
        struct.pack_into("<H", content, local + 6, 1)
        struct.pack_into("<H", content, central + 8, 1)
    archive_path.write_bytes(content)
    with zipfile.ZipFile(archive_path) as archive, pytest.raises(error):
        archive.read("config.py")

    assert main(["--json", "--fail-on-violation", str(archive_path)]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result["scanned_archives"] == 1
    assert result["scan_complete"] is False
    assert result["is_healthy"] is False
    assert result["violations"]
    assert _token() not in json.dumps(result)


def test_zip_member_io_error_is_incomplete_and_other_roots_are_scanned(tmp_path, monkeypatch):
    archive_path = tmp_path / "package.whl"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("config.py", _token())
    other_source = tmp_path / "other.py"
    other_source.write_text(_token(), encoding="utf-8")

    def unreadable(*args, **kwargs):
        raise OSError(f"synthetic read failure: {_token()}")

    monkeypatch.setattr(zipfile.ZipFile, "open", unreadable)
    report = scan_release_paths([archive_path, other_source])

    assert report.scan_complete is False
    assert any("cannot scan archive" in issue for issue in report.violations)
    assert report.total_findings == 1
    assert report.hits[0].path == str(other_source)
    assert _token() not in repr(report)


def test_unexpected_runtime_error_is_not_hidden_as_an_archive_read_error(tmp_path, monkeypatch):
    archive_path = tmp_path / "package.whl"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("config.py", "content")

    def programming_error(*args, **kwargs):
        raise RuntimeError("unexpected scanner failure")

    monkeypatch.setattr(zipfile.ZipFile, "open", programming_error)
    with pytest.raises(RuntimeError, match="unexpected scanner failure"):
        scan_release_paths([archive_path])


@pytest.mark.parametrize("archive_type", ["zip", "tar"])
def test_zero_archive_member_limit_scans_all_members(tmp_path, archive_type):
    payload = _token().encode()
    if archive_type == "zip":
        archive_path = tmp_path / "package.whl"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("first.py", payload)
            archive.writestr("second.py", payload)
    else:
        archive_path = tmp_path / "package.tar.gz"
        with tarfile.open(archive_path, "w:gz") as archive:
            for name in ("first.py", "second.py"):
                member = tarfile.TarInfo(name)
                member.size = len(payload)
                archive.addfile(member, io.BytesIO(payload))

    report = scan_release_paths([archive_path], policy=ReleaseScanPolicy(max_archive_members=0))

    assert report.scan_complete is True
    assert report.total_findings == 2
    assert {hit.member for hit in report.hits} == {"first.py", "second.py"}
    assert report.violations


@pytest.mark.parametrize("kind", ["source", "zip", "tar"])
def test_oversize_files_cannot_pass_release_gate(tmp_path, capsys, kind):
    payload = b"padding" * 8 + _token().encode()
    if kind == "source":
        path = tmp_path / "config.py"
        path.write_bytes(payload)
    elif kind == "zip":
        path = tmp_path / "package.whl"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("config.py", payload)
    else:
        path = tmp_path / "package.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            member = tarfile.TarInfo("config.py")
            member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))

    assert main(["--json", "--fail-on-violation", "--max-file-bytes", "8", str(path)]) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["scan_complete"] is False
    assert report["is_healthy"] is False
    assert report["skipped_large_files"] == 1
    assert report["total_findings"] == 0
    assert any("exceeds byte cap" in violation for violation in report["violations"])
    assert _token() not in json.dumps(report)


def test_source_that_grows_after_stat_is_still_bounded(tmp_path, monkeypatch):
    path = tmp_path / "config.py"
    path.write_text("small", encoding="utf-8")
    original_open = Path.open

    def growing_file(self, *args, **kwargs):
        if self == path and args == ("rb",):
            with original_open(self, "wb") as output:
                output.write(b"padding" * 8 + _token().encode())
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", growing_file)
    report = scan_release_paths([path], policy=ReleaseScanPolicy(max_file_bytes=8))

    assert report.scan_complete is False
    assert report.skipped_large_files == 1
    assert report.violations
