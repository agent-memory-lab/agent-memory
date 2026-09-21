"""Fail-closed V4 release evidence, scanning, and acceptance reports."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
import inspect
import json
from pathlib import Path
import re
import tarfile
from typing import Protocol
import zipfile

from .serialization import to_jsonable


class ReleaseCheckKind(StrEnum):
    UNIT = "unit"
    CONTRACT = "contract"
    FAULT = "fault"
    INTEGRATION = "integration"
    REPLAY = "replay"
    RESOURCE = "resource"
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    BUILD_INSTALL = "build-install"
    SENSITIVE_INFORMATION = "sensitive-information"
    DOCUMENTATION_API = "documentation-api"


class ReleaseCheckStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    SKIPPED = "skipped"
    UNMEASURED = "unmeasured"


class ReleaseAcceptanceStatus(StrEnum):
    READY = "ready"
    BLOCKED = "blocked"


def _identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 160:
        raise ValueError(f"{field_name} must contain 1 to 160 characters")
    return value


def _identifiers(values: Sequence[str], field_name: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise TypeError(f"{field_name} must be a sequence")
    normalized = tuple(_identifier(value, field_name) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} must be unique")
    return normalized


@dataclass(frozen=True, slots=True)
class SensitiveInformationFinding:
    location: str
    line: int
    rule: str
    fingerprint: str

    def __post_init__(self) -> None:
        if not self.location:
            raise ValueError("finding location must not be empty")
        if type(self.line) is not int or self.line < 1:
            raise ValueError("finding line must be positive")
        _identifier(self.rule, "finding rule")
        if len(self.fingerprint) != 12:
            raise ValueError("finding fingerprint must contain 12 characters")


@dataclass(frozen=True, slots=True)
class ReleaseCheckOutcome:
    check_id: str
    kind: ReleaseCheckKind
    status: ReleaseCheckStatus
    summary: str
    failures: int = 0
    skipped: int = 0
    findings: int = 0
    subjects: tuple[str, ...] = ()
    sensitive_findings: tuple[SensitiveInformationFinding, ...] = ()
    artifact_uri: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.check_id, "check_id")
        object.__setattr__(self, "kind", ReleaseCheckKind(self.kind))
        object.__setattr__(self, "status", ReleaseCheckStatus(self.status))
        if not isinstance(self.summary, str) or not self.summary.strip():
            raise ValueError("summary must not be empty")
        for name in ("failures", "skipped", "findings"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        subjects = _identifiers(self.subjects, "subjects")
        sensitive = tuple(self.sensitive_findings)
        if any(not isinstance(item, SensitiveInformationFinding) for item in sensitive):
            raise TypeError("sensitive_findings contains an invalid value")
        if len(sensitive) > self.findings:
            raise ValueError("sensitive_findings cannot exceed findings")
        object.__setattr__(self, "subjects", subjects)
        object.__setattr__(self, "sensitive_findings", sensitive)


@dataclass(frozen=True, slots=True)
class ReleaseClaim:
    claim_id: str
    text: str
    required_checks: tuple[ReleaseCheckKind, ...]

    def __post_init__(self) -> None:
        _identifier(self.claim_id, "claim_id")
        if not isinstance(self.text, str) or not self.text.strip():
            raise ValueError("claim text must not be empty")
        checks = tuple(ReleaseCheckKind(item) for item in self.required_checks)
        if not checks or len(checks) != len(set(checks)):
            raise ValueError("required_checks must be non-empty and unique")
        object.__setattr__(self, "required_checks", checks)


@dataclass(frozen=True, slots=True)
class ReleaseAcceptancePlan:
    release_version: str
    fixed_clock: datetime
    python_version: str
    required_packages: tuple[str, ...]
    claims: tuple[ReleaseClaim, ...]

    def __post_init__(self) -> None:
        _identifier(self.release_version, "release_version")
        if self.fixed_clock.tzinfo is None:
            raise ValueError("fixed_clock must be timezone-aware")
        if self.python_version != "3.13":
            raise ValueError("release acceptance requires Python 3.13")
        packages = _identifiers(self.required_packages, "required_packages")
        if not packages:
            raise ValueError("required_packages must not be empty")
        claims = tuple(self.claims)
        if any(not isinstance(item, ReleaseClaim) for item in claims):
            raise TypeError("claims contains an invalid value")
        if len({item.claim_id for item in claims}) != len(claims):
            raise ValueError("claim IDs must be unique")
        object.__setattr__(self, "required_packages", packages)
        object.__setattr__(self, "claims", claims)


@dataclass(frozen=True, slots=True)
class ReleaseClaimResult:
    claim_id: str
    supported: bool
    evidence_check_ids: tuple[str, ...]
    missing_or_failed_checks: tuple[ReleaseCheckKind, ...]


@dataclass(frozen=True, slots=True)
class ReleaseAcceptanceReport:
    release_version: str
    fixed_clock: datetime
    python_version: str
    required_packages: tuple[str, ...]
    status: ReleaseAcceptanceStatus
    checks: tuple[ReleaseCheckOutcome, ...]
    claims: tuple[ReleaseClaimResult, ...]
    reasons: tuple[str, ...]

    def to_json(self) -> str:
        return json.dumps(
            to_jsonable(self),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def to_markdown(self) -> str:
        lines = [
            "# Agent Memory Release Acceptance",
            "",
            f"Status: **{self.status.value.upper()}**",
            f"Release: {self.release_version}",
            f"Python: {self.python_version}",
            f"Packages: {', '.join(self.required_packages)}",
            "",
            "| Check | Status | Failures | Skipped | Findings |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
        for check in self.checks:
            lines.append(
                f"| {check.kind.value} | {check.status.value} | {check.failures} | "
                f"{check.skipped} | {check.findings} |"
            )
        if self.reasons:
            lines.extend(["", "## Blocking reasons"])
            lines.extend(f"- {reason}" for reason in self.reasons)
        return "\n".join(lines) + "\n"


def evaluate_release_acceptance(
    plan: ReleaseAcceptancePlan,
    outcomes: Sequence[ReleaseCheckOutcome],
) -> ReleaseAcceptanceReport:
    if not isinstance(plan, ReleaseAcceptancePlan):
        raise TypeError("plan must be a ReleaseAcceptancePlan")
    checked = tuple(outcomes)
    if any(not isinstance(item, ReleaseCheckOutcome) for item in checked):
        raise TypeError("outcomes contains an invalid value")
    by_kind: dict[ReleaseCheckKind, ReleaseCheckOutcome] = {}
    duplicate_kinds: set[ReleaseCheckKind] = set()
    for outcome in checked:
        if outcome.kind in by_kind:
            duplicate_kinds.add(outcome.kind)
        else:
            by_kind[outcome.kind] = outcome
    reasons: list[str] = []
    for kind in ReleaseCheckKind:
        outcome = by_kind.get(kind)
        if outcome is None:
            reasons.append(f"{kind.value} check is unmeasured")
            continue
        if kind in duplicate_kinds:
            reasons.append(f"{kind.value} check has duplicate evidence")
        if outcome.status is not ReleaseCheckStatus.PASS:
            reasons.append(f"{kind.value} check is {outcome.status.value}")
        if outcome.failures:
            reasons.append(f"{kind.value} check reports {outcome.failures} failures")
        if outcome.skipped:
            reasons.append(f"{kind.value} check reports {outcome.skipped} skipped")
        if outcome.findings:
            reasons.append(f"{kind.value} check reports {outcome.findings} findings")

    build = by_kind.get(ReleaseCheckKind.BUILD_INSTALL)
    if build is not None:
        missing_packages = sorted(set(plan.required_packages) - set(build.subjects))
        if missing_packages:
            reasons.append(
                "build/install evidence is missing packages: " + ", ".join(missing_packages)
            )

    claim_results: list[ReleaseClaimResult] = []
    for claim in plan.claims:
        missing_or_failed = tuple(
            kind
            for kind in claim.required_checks
            if kind not in by_kind or not _outcome_passes(by_kind[kind])
        )
        evidence_ids = tuple(
            by_kind[kind].check_id for kind in claim.required_checks if kind in by_kind
        )
        claim_results.append(
            ReleaseClaimResult(
                claim_id=claim.claim_id,
                supported=not missing_or_failed,
                evidence_check_ids=evidence_ids,
                missing_or_failed_checks=missing_or_failed,
            )
        )
        if missing_or_failed:
            reasons.append(f"release claim {claim.claim_id} lacks passing evidence")

    unique_reasons = tuple(dict.fromkeys(reasons))
    return ReleaseAcceptanceReport(
        release_version=plan.release_version,
        fixed_clock=plan.fixed_clock,
        python_version=plan.python_version,
        required_packages=plan.required_packages,
        status=(
            ReleaseAcceptanceStatus.BLOCKED
            if unique_reasons
            else ReleaseAcceptanceStatus.READY
        ),
        checks=tuple(sorted(checked, key=lambda item: item.kind.value)),
        claims=tuple(claim_results),
        reasons=unique_reasons,
    )


def _outcome_passes(outcome: ReleaseCheckOutcome) -> bool:
    return (
        outcome.status is ReleaseCheckStatus.PASS
        and outcome.failures == 0
        and outcome.skipped == 0
        and outcome.findings == 0
    )


ReleaseCheckRunner = Callable[
    [], ReleaseCheckOutcome | Awaitable[ReleaseCheckOutcome]
]


async def run_release_checks(
    runners: Mapping[ReleaseCheckKind, ReleaseCheckRunner],
) -> tuple[ReleaseCheckOutcome, ...]:
    if not isinstance(runners, Mapping):
        raise TypeError("runners must be a mapping")
    outcomes: list[ReleaseCheckOutcome] = []
    for kind in ReleaseCheckKind:
        runner = runners.get(kind)
        if runner is None:
            continue
        try:
            result = runner()
            if inspect.isawaitable(result):
                result = await result
            if not isinstance(result, ReleaseCheckOutcome):
                raise TypeError("runner returned an invalid outcome")
            if result.kind is not kind:
                raise ValueError("runner returned evidence for a different check kind")
            outcomes.append(result)
        except Exception as error:
            outcomes.append(
                ReleaseCheckOutcome(
                    check_id=f"runner-{kind.value}",
                    kind=kind,
                    status=ReleaseCheckStatus.FAIL,
                    summary=f"runner raised {type(error).__name__}",
                    failures=1,
                )
            )
    return tuple(outcomes)


_SECRET_PATTERNS = (
    ("private-key", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("aws-access-key", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("github-token", re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{30,}\b")),
    ("github-pat", re.compile(r"\bgithub_pat_[A-Za-z0-9_]{40,}\b")),
    ("openai-api-key", re.compile(r"\bsk-[A-Za-z0-9_-]{32,}\b")),
    ("slack-token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
)
_TEXT_SUFFIXES = {
    ".cfg",
    ".conf",
    ".css",
    ".env",
    ".ini",
    ".json",
    ".md",
    ".py",
    ".rst",
    ".sh",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}


def run_sensitive_information_check(
    paths: Sequence[str | Path],
    *,
    allow_fingerprints: Sequence[str] = (),
    max_member_bytes: int = 2_000_000,
) -> ReleaseCheckOutcome:
    allowed = set(allow_fingerprints)
    findings: list[SensitiveInformationFinding] = []
    for path in _iter_files(paths):
        if zipfile.is_zipfile(path):
            findings.extend(_scan_zip(path, max_member_bytes, allowed))
        elif tarfile.is_tarfile(path):
            findings.extend(_scan_tar(path, max_member_bytes, allowed))
        elif path.suffix.lower() in _TEXT_SUFFIXES and path.stat().st_size <= max_member_bytes:
            findings.extend(_scan_bytes(str(path), path.read_bytes(), allowed))
    findings.sort(key=lambda item: (item.location, item.line, item.rule))
    return ReleaseCheckOutcome(
        check_id="sensitive-information-scan",
        kind=ReleaseCheckKind.SENSITIVE_INFORMATION,
        status=(ReleaseCheckStatus.FAIL if findings else ReleaseCheckStatus.PASS),
        summary=f"{len(findings)} sensitive findings",
        findings=len(findings),
        sensitive_findings=tuple(findings),
        subjects=tuple(str(Path(path)) for path in paths),
    )


def _iter_files(paths: Sequence[str | Path]):
    files: set[Path] = set()
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_file() and not path.is_symlink():
            files.add(path)
        elif path.is_dir():
            files.update(
                item
                for item in path.rglob("*")
                if item.is_file() and not item.is_symlink()
            )
    return iter(sorted(files))


def _scan_zip(
    path: Path,
    max_member_bytes: int,
    allowed: set[str],
) -> list[SensitiveInformationFinding]:
    findings: list[SensitiveInformationFinding] = []
    with zipfile.ZipFile(path) as archive:
        for member in sorted(archive.infolist(), key=lambda item: item.filename):
            suffix = Path(member.filename).suffix.lower()
            if member.is_dir() or member.file_size > max_member_bytes or suffix not in _TEXT_SUFFIXES:
                continue
            findings.extend(
                _scan_bytes(f"{path}!{member.filename}", archive.read(member), allowed)
            )
    return findings


def _scan_tar(
    path: Path,
    max_member_bytes: int,
    allowed: set[str],
) -> list[SensitiveInformationFinding]:
    findings: list[SensitiveInformationFinding] = []
    with tarfile.open(path) as archive:
        for member in sorted(archive.getmembers(), key=lambda item: item.name):
            suffix = Path(member.name).suffix.lower()
            if not member.isfile() or member.size > max_member_bytes or suffix not in _TEXT_SUFFIXES:
                continue
            extracted = archive.extractfile(member)
            if extracted is not None:
                findings.extend(
                    _scan_bytes(f"{path}!{member.name}", extracted.read(), allowed)
                )
    return findings


def _scan_bytes(
    location: str,
    content: bytes,
    allowed: set[str],
) -> list[SensitiveInformationFinding]:
    if b"\x00" in content:
        return []
    text = content.decode("utf-8", errors="replace")
    findings: list[SensitiveInformationFinding] = []
    for rule, pattern in _SECRET_PATTERNS:
        for match in pattern.finditer(text):
            fingerprint = sha256(match.group(0).encode("utf-8")).hexdigest()[:12]
            if fingerprint in allowed:
                continue
            findings.append(
                SensitiveInformationFinding(
                    location=location,
                    line=text.count("\n", 0, match.start()) + 1,
                    rule=rule,
                    fingerprint=fingerprint,
                )
            )
    return findings


def run_documentation_consistency_check(
    *,
    declared_capabilities: Mapping[str, bool],
    actual_capabilities: Mapping[str, bool],
    required_api_symbols: Sequence[str],
    public_namespace: Mapping[str, object],
) -> ReleaseCheckOutcome:
    if not all(isinstance(value, bool) for value in declared_capabilities.values()):
        raise TypeError("declared capabilities must contain booleans")
    if not all(isinstance(value, bool) for value in actual_capabilities.values()):
        raise TypeError("actual capabilities must contain booleans")
    capability_names = sorted(set(declared_capabilities) | set(actual_capabilities))
    mismatches = [
        name
        for name in capability_names
        if declared_capabilities.get(name) != actual_capabilities.get(name)
    ]
    required = _identifiers(required_api_symbols, "required_api_symbols")
    missing_api = [name for name in required if name not in public_namespace]
    failures = len(mismatches) + len(missing_api)
    subjects = tuple(capability_names) + required
    return ReleaseCheckOutcome(
        check_id="documentation-api-consistency",
        kind=ReleaseCheckKind.DOCUMENTATION_API,
        status=ReleaseCheckStatus.FAIL if failures else ReleaseCheckStatus.PASS,
        summary=(
            f"{len(mismatches)} capability mismatches and {len(missing_api)} missing API symbols"
        ),
        failures=failures,
        subjects=subjects,
    )
