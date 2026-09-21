"""Versioned, streaming event snapshots and deterministic replay."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
import json
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

from .domain import (
    SCHEMA_VERSION,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryScope,
)
from .serialization import to_jsonable


SNAPSHOT_FORMAT_VERSION = 1


class SnapshotReplayError(RuntimeError):
    """Raised before replay when snapshot integrity or versions are invalid."""


class SnapshotSplit(StrEnum):
    TRAIN = "train"
    EVAL = "eval"
    TEST = "test"


class SnapshotDeletionState(StrEnum):
    CLEAN = "clean"
    AFFECTED = "affected"


def _identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValueError(f"{field_name} must contain 1 to 128 characters")
    return value


def _versions(value: Mapping[str, str]) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError("plugin_versions must be a non-empty mapping")
    normalized: dict[str, str] = {}
    for name, version in sorted(value.items()):
        normalized[_identifier(name, "plugin name")] = _identifier(
            version, "plugin version"
        )
    return MappingProxyType(normalized)


def _canonical_json(value: object) -> str:
    return json.dumps(
        to_jsonable(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


@dataclass(frozen=True, slots=True)
class SnapshotDataLicense:
    id: str
    name: str
    usage: str
    source_uri: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.id, "license id")
        _identifier(self.name, "license name")
        _identifier(self.usage, "license usage")
        if self.source_uri is not None and not self.source_uri.strip():
            raise ValueError("license source_uri must not be empty")


@dataclass(frozen=True, slots=True)
class SnapshotRecord:
    event: MemoryEvent
    split: SnapshotSplit
    license_id: str
    artifact_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.event, MemoryEvent):
            raise TypeError("snapshot record event must be a MemoryEvent")
        object.__setattr__(self, "split", SnapshotSplit(self.split))
        _identifier(self.license_id, "license_id")
        references = tuple(self.artifact_refs)
        if len(references) > 128:
            raise ValueError("artifact_refs exceeds its capacity")
        if any(not isinstance(value, str) or not value.strip() for value in references):
            raise ValueError("artifact_refs must contain non-empty strings")
        if len(references) != len(set(references)):
            raise ValueError("artifact_refs must not contain duplicates")
        object.__setattr__(self, "artifact_refs", references)


@dataclass(frozen=True, slots=True)
class SnapshotExportSpec:
    snapshot_id: str
    version: str
    licenses: tuple[SnapshotDataLicense, ...]
    plugin_versions: Mapping[str, str]
    created_at: datetime
    max_inline_event_bytes: int = 65_536

    def __post_init__(self) -> None:
        _identifier(self.snapshot_id, "snapshot_id")
        _identifier(self.version, "snapshot version")
        licenses = tuple(self.licenses)
        if not licenses or any(not isinstance(item, SnapshotDataLicense) for item in licenses):
            raise ValueError("licenses must contain at least one SnapshotDataLicense")
        if len({item.id for item in licenses}) != len(licenses):
            raise ValueError("license IDs must be unique")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        if type(self.max_inline_event_bytes) is not int or not (
            256 <= self.max_inline_event_bytes <= 4_194_304
        ):
            raise ValueError("max_inline_event_bytes must be between 256 and 4194304")
        object.__setattr__(self, "licenses", licenses)
        object.__setattr__(self, "plugin_versions", _versions(self.plugin_versions))


@dataclass(frozen=True, slots=True)
class SnapshotDeletionImpact:
    scope_partition: str
    mode: ForgetMode
    record_ids: tuple[str, ...]
    affected_at: datetime

    def __post_init__(self) -> None:
        if not self.scope_partition:
            raise ValueError("scope_partition must not be empty")
        object.__setattr__(self, "mode", ForgetMode(self.mode))
        record_ids = tuple(sorted(set(self.record_ids)))
        if not record_ids:
            raise ValueError("deletion impact must contain record IDs")
        if self.affected_at.tzinfo is None:
            raise ValueError("affected_at must be timezone-aware")
        object.__setattr__(self, "record_ids", record_ids)


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    snapshot_id: str
    version: str
    format_version: int
    schema_version: int
    created_at: datetime
    time_start: datetime
    time_end: datetime
    licenses: tuple[SnapshotDataLicense, ...]
    plugin_versions: Mapping[str, str]
    record_count: int
    split_counts: Mapping[str, int]
    data_sha256: str
    deletion_state: SnapshotDeletionState = SnapshotDeletionState.CLEAN
    deletions: tuple[SnapshotDeletionImpact, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.snapshot_id, "snapshot_id")
        _identifier(self.version, "snapshot version")
        if self.format_version != SNAPSHOT_FORMAT_VERSION:
            raise ValueError("unsupported snapshot format version")
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        if self.created_at.tzinfo is None or self.time_start.tzinfo is None or self.time_end.tzinfo is None:
            raise ValueError("snapshot times must be timezone-aware")
        if self.time_start > self.time_end:
            raise ValueError("snapshot time range is invalid")
        if type(self.record_count) is not int or self.record_count < 1:
            raise ValueError("record_count must be positive")
        counts = {
            split.value: int(self.split_counts.get(split.value, 0))
            for split in SnapshotSplit
        }
        if any(value < 0 for value in counts.values()) or sum(counts.values()) != self.record_count:
            raise ValueError("split_counts must equal record_count")
        if len(self.data_sha256) != 64:
            raise ValueError("data_sha256 must be a SHA-256 digest")
        licenses = tuple(self.licenses)
        if not licenses:
            raise ValueError("snapshot licenses must not be empty")
        object.__setattr__(self, "licenses", licenses)
        object.__setattr__(self, "plugin_versions", _versions(self.plugin_versions))
        object.__setattr__(self, "split_counts", MappingProxyType(counts))
        object.__setattr__(self, "deletion_state", SnapshotDeletionState(self.deletion_state))
        object.__setattr__(self, "deletions", tuple(self.deletions))

    @property
    def affected_record_ids(self) -> tuple[str, ...]:
        return tuple(sorted({item for impact in self.deletions for item in impact.record_ids}))


class SnapshotReplayProvider(Protocol):
    async def ingest_event(self, event: MemoryEvent) -> object: ...


@dataclass(frozen=True, slots=True)
class SnapshotReplayConfig:
    fixed_clock: datetime
    run_seed: str
    plugin_versions: Mapping[str, str]
    splits: tuple[SnapshotSplit, ...] = tuple(SnapshotSplit)
    page_size: int = 100

    def __post_init__(self) -> None:
        if self.fixed_clock.tzinfo is None:
            raise ValueError("fixed_clock must be timezone-aware")
        _identifier(self.run_seed, "run_seed")
        splits = tuple(SnapshotSplit(value) for value in self.splits)
        if not splits or len(splits) != len(set(splits)):
            raise ValueError("splits must be non-empty and unique")
        if type(self.page_size) is not int or not 1 <= self.page_size <= 10_000:
            raise ValueError("page_size must be between 1 and 10000")
        object.__setattr__(self, "plugin_versions", _versions(self.plugin_versions))
        object.__setattr__(self, "splits", splits)


@dataclass(frozen=True, slots=True)
class SnapshotReplayReport:
    replay_id: str
    snapshot_id: str
    snapshot_version: str
    fixed_clock: datetime
    plugin_versions: Mapping[str, str]
    splits: tuple[SnapshotSplit, ...]
    replayed_event_ids: tuple[str, ...]
    skipped_deleted_event_ids: tuple[str, ...]
    result_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "plugin_versions", _versions(self.plugin_versions))


def export_snapshot(
    data_path: str | Path,
    records: Iterable[SnapshotRecord],
    spec: SnapshotExportSpec,
) -> SnapshotManifest:
    """Write records incrementally and return a separate immutable inventory."""
    if not isinstance(spec, SnapshotExportSpec):
        raise TypeError("spec must be a SnapshotExportSpec")
    path = Path(data_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    license_ids = {item.id for item in spec.licenses}
    digest = sha256()
    seen: set[str] = set()
    counts = {split.value: 0 for split in SnapshotSplit}
    first_time: datetime | None = None
    last_time: datetime | None = None
    previous_key: tuple[datetime, str] | None = None
    try:
        with temporary.open("wb") as stream:
            for record in records:
                if not isinstance(record, SnapshotRecord):
                    raise TypeError("records must contain SnapshotRecord values")
                if record.license_id not in license_ids:
                    raise ValueError("snapshot record references an undeclared license")
                if record.event.id in seen:
                    raise ValueError("snapshot event IDs must be unique")
                key = (record.event.occurred_at, record.event.id)
                if previous_key is not None and key < previous_key:
                    raise ValueError("snapshot records must use deterministic event order")
                line = (_canonical_json(record) + "\n").encode("utf-8")
                if len(line) > spec.max_inline_event_bytes:
                    raise ValueError(
                        "inline event payload exceeds max_inline_event_bytes; use artifact_refs"
                    )
                stream.write(line)
                digest.update(line)
                seen.add(record.event.id)
                counts[record.split.value] += 1
                first_time = first_time or record.event.occurred_at
                last_time = record.event.occurred_at
                previous_key = key
        if not seen or first_time is None or last_time is None:
            raise ValueError("snapshot must contain at least one record")
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return SnapshotManifest(
        snapshot_id=spec.snapshot_id,
        version=spec.version,
        format_version=SNAPSHOT_FORMAT_VERSION,
        schema_version=SCHEMA_VERSION,
        created_at=spec.created_at,
        time_start=first_time,
        time_end=last_time,
        licenses=spec.licenses,
        plugin_versions=spec.plugin_versions,
        record_count=len(seen),
        split_counts=counts,
        data_sha256=digest.hexdigest(),
    )


def write_snapshot_manifest(path: str | Path, manifest: SnapshotManifest) -> None:
    if not isinstance(manifest, SnapshotManifest):
        raise TypeError("manifest must be a SnapshotManifest")
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    try:
        temporary.write_text(_canonical_json(manifest) + "\n", encoding="utf-8")
        temporary.replace(destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def read_snapshot_manifest(path: str | Path) -> SnapshotManifest:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return _manifest_from_payload(payload)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise SnapshotReplayError("snapshot manifest is invalid") from error


def iter_snapshot_pages(
    data_path: str | Path,
    manifest: SnapshotManifest,
    *,
    page_size: int = 100,
) -> Iterator[tuple[SnapshotRecord, ...]]:
    if type(page_size) is not int or not 1 <= page_size <= 10_000:
        raise ValueError("page_size must be between 1 and 10000")
    _validate_snapshot_data(data_path, manifest)
    page: list[SnapshotRecord] = []
    with Path(data_path).open("r", encoding="utf-8") as stream:
        for line in stream:
            try:
                page.append(_record_from_payload(json.loads(line)))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise SnapshotReplayError("snapshot record is invalid") from error
            if len(page) == page_size:
                yield tuple(page)
                page.clear()
    if page:
        yield tuple(page)


def mark_snapshot_affected(
    data_path: str | Path,
    manifest: SnapshotManifest,
    request: ForgetRequest,
    *,
    affected_at: datetime,
) -> SnapshotManifest:
    if not isinstance(request, ForgetRequest):
        raise TypeError("request must be a ForgetRequest")
    if affected_at.tzinfo is None:
        raise ValueError("affected_at must be timezone-aware")
    already_affected = set(manifest.affected_record_ids)
    requested_ids = set(request.memory_ids)
    matching: set[str] = set()
    for page in iter_snapshot_pages(data_path, manifest):
        for record in page:
            if record.event.scope.partition_key() != request.scope.partition_key():
                continue
            if request.all_in_scope or record.event.id in requested_ids:
                matching.add(record.event.id)
    new_ids = tuple(sorted(matching - already_affected))
    if not new_ids:
        return manifest
    impact = SnapshotDeletionImpact(
        scope_partition=request.scope.partition_key(),
        mode=request.mode,
        record_ids=new_ids,
        affected_at=affected_at,
    )
    return replace(
        manifest,
        deletion_state=SnapshotDeletionState.AFFECTED,
        deletions=(*manifest.deletions, impact),
    )


async def replay_snapshot(
    data_path: str | Path,
    manifest: SnapshotManifest,
    provider: SnapshotReplayProvider,
    config: SnapshotReplayConfig,
) -> SnapshotReplayReport:
    if dict(config.plugin_versions) != dict(manifest.plugin_versions):
        raise SnapshotReplayError("replay plugin versions do not match snapshot plugin versions")
    if not callable(getattr(provider, "ingest_event", None)):
        raise TypeError("provider must implement ingest_event")
    affected_ids = set(manifest.affected_record_ids)
    selected = set(config.splits)
    replayed: list[str] = []
    skipped_deleted: list[str] = []
    result_digest = sha256()
    seed_payload = {
        "snapshot_id": manifest.snapshot_id,
        "snapshot_version": manifest.version,
        "data_sha256": manifest.data_sha256,
        "fixed_clock": config.fixed_clock,
        "run_seed": config.run_seed,
        "plugin_versions": config.plugin_versions,
        "splits": tuple(split.value for split in config.splits),
        "deleted": manifest.affected_record_ids,
    }
    replay_id = str(uuid5(NAMESPACE_URL, _canonical_json(seed_payload)))
    for page in iter_snapshot_pages(data_path, manifest, page_size=config.page_size):
        for record in page:
            if record.split not in selected:
                continue
            if record.event.id in affected_ids:
                skipped_deleted.append(record.event.id)
                continue
            result = await provider.ingest_event(record.event)
            replayed.append(record.event.id)
            result_digest.update((_canonical_json(result) + "\n").encode("utf-8"))
    return SnapshotReplayReport(
        replay_id=replay_id,
        snapshot_id=manifest.snapshot_id,
        snapshot_version=manifest.version,
        fixed_clock=config.fixed_clock,
        plugin_versions=config.plugin_versions,
        splits=config.splits,
        replayed_event_ids=tuple(replayed),
        skipped_deleted_event_ids=tuple(skipped_deleted),
        result_sha256=result_digest.hexdigest(),
    )


def _validate_snapshot_data(data_path: str | Path, manifest: SnapshotManifest) -> None:
    digest = sha256()
    count = 0
    with Path(data_path).open("rb") as stream:
        for line in stream:
            digest.update(line)
            count += 1
    if digest.hexdigest() != manifest.data_sha256:
        raise SnapshotReplayError("snapshot data digest does not match manifest")
    if count != manifest.record_count:
        raise SnapshotReplayError("snapshot record count does not match manifest")


def _record_from_payload(payload: Mapping[str, Any]) -> SnapshotRecord:
    event_payload = payload["event"]
    scope_payload = event_payload["scope"]
    scope = MemoryScope(
        scope_payload["tenant_id"],
        namespace=scope_payload.get("namespace", "default"),
        user_id=scope_payload.get("user_id"),
        agent_id=scope_payload.get("agent_id"),
        workspace_id=scope_payload.get("workspace_id"),
        session_id=scope_payload.get("session_id"),
    )
    event = MemoryEvent(
        scope=scope,
        event_type=event_payload["event_type"],
        content=event_payload["content"],
        id=event_payload["id"],
        metadata=event_payload.get("metadata", {}),
        occurred_at=datetime.fromisoformat(event_payload["occurred_at"]),
        ingested_at=datetime.fromisoformat(event_payload["ingested_at"]),
        idempotency_key=event_payload.get("idempotency_key"),
        actor=event_payload.get("actor", "agent"),
        source_uri=event_payload.get("source_uri"),
        sensitivity=event_payload.get("sensitivity", "internal"),
        retention_class=event_payload.get("retention_class", "standard"),
        schema_version=int(event_payload.get("schema_version", SCHEMA_VERSION)),
        content_hash=event_payload["content_hash"],
    )
    return SnapshotRecord(
        event=event,
        split=SnapshotSplit(payload["split"]),
        license_id=payload["license_id"],
        artifact_refs=tuple(payload.get("artifact_refs", ())),
    )


def _manifest_from_payload(payload: Mapping[str, Any]) -> SnapshotManifest:
    licenses = tuple(SnapshotDataLicense(**item) for item in payload["licenses"])
    deletions = tuple(
        SnapshotDeletionImpact(
            scope_partition=item["scope_partition"],
            mode=ForgetMode(item["mode"]),
            record_ids=tuple(item["record_ids"]),
            affected_at=datetime.fromisoformat(item["affected_at"]),
        )
        for item in payload.get("deletions", ())
    )
    return SnapshotManifest(
        snapshot_id=payload["snapshot_id"],
        version=payload["version"],
        format_version=int(payload["format_version"]),
        schema_version=int(payload["schema_version"]),
        created_at=datetime.fromisoformat(payload["created_at"]),
        time_start=datetime.fromisoformat(payload["time_start"]),
        time_end=datetime.fromisoformat(payload["time_end"]),
        licenses=licenses,
        plugin_versions=payload["plugin_versions"],
        record_count=int(payload["record_count"]),
        split_counts=payload["split_counts"],
        data_sha256=payload["data_sha256"],
        deletion_state=SnapshotDeletionState(payload.get("deletion_state", "clean")),
        deletions=deletions,
    )
