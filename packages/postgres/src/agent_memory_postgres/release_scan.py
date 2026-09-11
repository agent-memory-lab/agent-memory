from __future__ import annotations

import argparse
import json
import os
import re
import tarfile
import zipfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


@dataclass(frozen=True, slots=True)
class ReleasePattern:
    key: str
    regex: re.Pattern[str]
    severity: str


@dataclass(frozen=True, slots=True)
class ReleaseScanPolicy:
    max_findings: int | None = 0
    max_reported_hits: int = 100
    max_file_bytes: int = 2 * 1024 * 1024
    max_archive_members: int = 10_000


@dataclass(frozen=True, slots=True)
class ReleaseScanHit:
    path: str
    member: str | None
    line: int
    pattern: str
    severity: str
    matched_text: str = "[REDACTED]"


@dataclass(frozen=True, slots=True)
class ReleaseScanReport:
    roots: tuple[str, ...]
    scanned_files: int
    scanned_archives: int
    skipped_large_files: int
    total_findings: int
    hits: tuple[ReleaseScanHit, ...]
    scan_complete: bool
    violations: tuple[str, ...]


DEFAULT_RELEASE_PATTERNS: tuple[ReleasePattern, ...] = (
    ReleasePattern("aws_access_key", re.compile(r"AKIA[0-9A-Z]{16}"), "critical"),
    ReleasePattern("aws_session_key", re.compile(r"ASIA[0-9A-Z]{16}"), "critical"),
    ReleasePattern(
        "openai_api_key",
        re.compile(r"sk-[A-Za-z0-9_-]{20,}"),
        "critical",
    ),
    ReleasePattern(
        "github_token",
        re.compile(
            r"(?:gh[pousr]_[A-Za-z0-9_]{30,255}|"
            r"github_pat_[A-Za-z0-9_]{20,255})"
        ),
        "critical",
    ),
    ReleasePattern(
        "private_key",
        re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
        "critical",
    ),
    ReleasePattern(
        "bearer_token",
        re.compile(r"\bbearer\s+[A-Za-z0-9_.-]{8,}", re.IGNORECASE),
        "high",
    ),
    ReleasePattern(
        "credential_assignment",
        re.compile(
            r"\b(?:api[_-]?(?:secret|key)|secret[_-]?(?:token|key)|"
            r"access[_-]?(?:key|token)|password)\s*[:=]\s*[\"']?"
            r"[A-Za-z0-9_./+=-]{12,}",
            re.IGNORECASE,
        ),
        "high",
    ),
)


_EXCLUDED_DIRECTORIES = frozenset(
    {
        ".git",
        ".hg",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".svn",
        ".tox",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "node_modules",
        "venv",
    }
)
_ARCHIVE_SUFFIXES = (".tar.gz", ".tgz", ".whl", ".zip")


def _is_archive(path: Path) -> bool:
    lowered = path.name.lower()
    return any(lowered.endswith(suffix) for suffix in _ARCHIVE_SUFFIXES)


def _iter_targets(root: Path) -> Iterable[tuple[str, Path]]:
    if root.is_file():
        yield ("archive" if _is_archive(root) else "file"), root
        return
    if not root.is_dir():
        return

    for current, directory_names, file_names in os.walk(root):
        current_path = Path(current)
        excluded = [name for name in directory_names if name in _EXCLUDED_DIRECTORIES]
        directory_names[:] = [name for name in directory_names if name not in _EXCLUDED_DIRECTORIES]
        for directory_name in excluded:
            if directory_name != "dist":
                continue
            dist_path = current_path / directory_name
            for archive in dist_path.glob("*"):
                if archive.is_file() and _is_archive(archive):
                    yield "archive", archive
        for file_name in file_names:
            path = current_path / file_name
            if path.is_symlink():
                continue
            yield ("archive" if _is_archive(path) else "file"), path


def _scan_text(
    text: str,
    *,
    path: str,
    member: str | None,
    patterns: Sequence[ReleasePattern],
) -> Iterable[ReleaseScanHit]:
    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern in patterns:
            for _ in pattern.regex.finditer(line):
                yield ReleaseScanHit(
                    path=path,
                    member=member,
                    line=line_number,
                    pattern=pattern.key,
                    severity=pattern.severity,
                )


def _decode_text(payload: bytes) -> str | None:
    if b"\x00" in payload:
        return None
    return payload.decode("utf-8", errors="replace")


def _read_limited(stream: BinaryIO, size: int, max_file_bytes: int) -> bytes | None:
    if size > max_file_bytes:
        return None
    return stream.read(max_file_bytes + 1)


def _archive_members(
    path: Path,
    policy: ReleaseScanPolicy,
) -> Iterable[tuple[str, bytes | None]]:
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            file_infos = [info for info in infos if not info.is_dir()]
            if policy.max_archive_members > 0 and len(file_infos) > policy.max_archive_members:
                raise ValueError(f"archive member cap reached: {path}")
            for index, info in enumerate(file_infos):
                if index >= policy.max_archive_members:
                    break
                if info.file_size > policy.max_file_bytes:
                    yield info.filename, None
                    continue
                try:
                    with archive.open(info) as stream:
                        yield (
                            info.filename,
                            _read_limited(
                                stream,
                                info.file_size,
                                policy.max_file_bytes,
                            ),
                        )
                except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile):
                    continue
        return

    with tarfile.open(path, mode="r:*") as archive:
        members = list(archive)
        if policy.max_archive_members > 0 and len(members) > policy.max_archive_members:
            raise ValueError(f"archive member cap reached: {path}")
        for index, info in enumerate(members):
            if index >= policy.max_archive_members:
                break
            if not info.isfile():
                continue
            if info.size > policy.max_file_bytes:
                yield info.name, None
                continue
            stream = archive.extractfile(info)
            if stream is None:
                continue
            with stream:
                yield (
                    info.name,
                    _read_limited(
                        stream,
                        info.size,
                        policy.max_file_bytes,
                    ),
                )


def scan_release_paths(
    paths: Sequence[str | Path],
    *,
    policy: ReleaseScanPolicy | None = None,
    patterns: Sequence[ReleasePattern] | None = None,
) -> ReleaseScanReport:
    selected_policy = policy or ReleaseScanPolicy()
    selected_patterns = tuple(patterns or DEFAULT_RELEASE_PATTERNS)
    roots = tuple(str(Path(path)) for path in paths)
    hits: list[ReleaseScanHit] = []
    scanned_files = 0
    scanned_archives = 0
    skipped_large_files = 0
    total_findings = 0
    violations: list[str] = []
    scan_complete = True
    if selected_policy.max_findings is not None and selected_policy.max_findings < 0:
        violations.append(f"invalid max_findings={selected_policy.max_findings}; must be >= 0")
    if selected_policy.max_reported_hits < 0:
        violations.append(
            f"invalid max_reported_hits={selected_policy.max_reported_hits}; must be >= 0"
        )
    if selected_policy.max_file_bytes < 0:
        violations.append(
            f"invalid max_file_bytes={selected_policy.max_file_bytes}; must be >= 0"
        )
    if selected_policy.max_archive_members < 0:
        violations.append(
            f"invalid max_archive_members={selected_policy.max_archive_members}; must be >= 0"
        )
    if violations:
        return ReleaseScanReport(
            roots=roots,
            scanned_files=scanned_files,
            scanned_archives=scanned_archives,
            skipped_large_files=skipped_large_files,
            total_findings=total_findings,
            hits=tuple(hits),
            scan_complete=False,
            violations=tuple(violations),
        )

    def record(found: Iterable[ReleaseScanHit]) -> None:
        nonlocal total_findings
        for hit in found:
            total_findings += 1
            if len(hits) < max(selected_policy.max_reported_hits, 0):
                hits.append(hit)

    seen: set[Path] = set()
    for raw_root in paths:
        root = Path(raw_root).resolve()
        if not root.exists():
            violations.append(f"scan path missing: {raw_root}")
            scan_complete = False
            continue
        for target_kind, target in _iter_targets(root):
            resolved = target.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            if target_kind == "archive":
                scanned_archives += 1
                try:
                    for member_name, payload in _archive_members(target, selected_policy):
                        if payload is None:
                            skipped_large_files += 1
                            continue
                        text = _decode_text(payload)
                        if text is not None:
                            record(
                                _scan_text(
                                    text,
                                    path=str(target),
                                    member=member_name,
                                    patterns=selected_patterns,
                                )
                            )
                except (OSError, tarfile.TarError, zipfile.BadZipFile):
                    violations.append(f"cannot scan archive: {target}")
                    scan_complete = False
                    continue
                except ValueError as exc:
                    violations.append(str(exc))
                    scan_complete = False
                continue

            try:
                size = target.stat().st_size
                if size > selected_policy.max_file_bytes:
                    skipped_large_files += 1
                    continue
                payload = target.read_bytes()
            except OSError:
                violations.append(f"cannot read file: {target}")
                scan_complete = False
                continue
            scanned_files += 1
            text = _decode_text(payload)
            if text is not None:
                record(
                    _scan_text(
                        text,
                        path=str(target),
                        member=None,
                        patterns=selected_patterns,
                    )
                )

    if selected_policy.max_findings is not None and total_findings > selected_policy.max_findings:
        violations.append(
            f"release scan findings {total_findings} > max_findings={selected_policy.max_findings}",
        )

    return ReleaseScanReport(
        roots=roots,
        scanned_files=scanned_files,
        scanned_archives=scanned_archives,
        skipped_large_files=skipped_large_files,
        total_findings=total_findings,
        hits=tuple(hits),
        scan_complete=scan_complete,
        violations=tuple(violations),
    )


def _serialize_report(report: ReleaseScanReport) -> str:
    return json.dumps(
        {
            "roots": list(report.roots),
            "scanned_files": report.scanned_files,
            "scanned_archives": report.scanned_archives,
            "skipped_large_files": report.skipped_large_files,
            "total_findings": report.total_findings,
            "scan_complete": report.scan_complete,
            "hits": [
                {
                    "path": hit.path,
                    "member": hit.member,
                    "line": hit.line,
                    "pattern": hit.pattern,
                    "severity": hit.severity,
                    "matched_text": hit.matched_text,
                }
                for hit in report.hits
            ],
            "violations": list(report.violations),
            "is_healthy": not report.violations,
        },
        ensure_ascii=False,
        indent=2,
    )


def _format_report(report: ReleaseScanReport) -> str:
    lines = [
        f"roots: {', '.join(report.roots)}",
        f"scanned_files: {report.scanned_files}",
        f"scanned_archives: {report.scanned_archives}",
        f"skipped_large_files: {report.skipped_large_files}",
        f"total_findings: {report.total_findings}",
        f"scan_complete: {report.scan_complete}",
    ]
    if report.violations:
        lines.append("policy violations:")
        lines.extend(f"  - {violation}" for violation in report.violations)
    else:
        lines.append("policy violations: none")
    if report.hits:
        lines.append("findings:")
        for hit in report.hits:
            location = hit.path if hit.member is None else f"{hit.path}:{hit.member}"
            lines.append(
                f"  - [{hit.severity}] {location}:{hit.line} {hit.pattern}={hit.matched_text}"
            )
    return "\n".join(lines)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan release source files and package archives for embedded secrets."
    )
    parser.add_argument("paths", nargs="*", default=["."])
    parser.add_argument("--max-findings", type=int, default=0)
    parser.add_argument("--max-reported-hits", type=int, default=100)
    parser.add_argument("--max-file-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--max-archive-members", type=int, default=10_000)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--fail-on-violation", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    report = scan_release_paths(
        args.paths,
        policy=ReleaseScanPolicy(
            max_findings=args.max_findings,
            max_reported_hits=args.max_reported_hits,
            max_file_bytes=args.max_file_bytes,
            max_archive_members=args.max_archive_members,
        ),
    )
    print(_serialize_report(report) if args.json else _format_report(report))
    if args.fail_on_violation and report.violations:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
