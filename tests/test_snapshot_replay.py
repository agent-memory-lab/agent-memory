"""T40 versioned snapshot inventory and deterministic replay acceptance tests."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from agent_memory import (
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryScope,
    SnapshotDataLicense,
    SnapshotDeletionState,
    SnapshotExportSpec,
    SnapshotRecord,
    SnapshotReplayConfig,
    SnapshotReplayError,
    SnapshotSplit,
    build_local_kernel,
    export_snapshot,
    iter_snapshot_pages,
    mark_snapshot_affected,
    read_snapshot_manifest,
    replay_snapshot,
    write_snapshot_manifest,
)


NOW = datetime(2026, 9, 21, 8, 0, tzinfo=timezone.utc)
SCOPE = MemoryScope("tenant", session_id="session")
LICENSE = SnapshotDataLicense(
    id="internal-eval",
    name="Internal evaluation data",
    usage="evaluation-only",
    source_uri="https://example.invalid/license",
)


def _event(index: int, *, scope: MemoryScope = SCOPE) -> MemoryEvent:
    occurred_at = NOW + timedelta(seconds=index)
    return MemoryEvent(
        scope,
        "tool.completed",
        f"event-{index}",
        id=f"event-{index}",
        metadata={"index": index},
        occurred_at=occurred_at,
        ingested_at=occurred_at,
    )


def _spec(*, max_inline_event_bytes: int = 65_536) -> SnapshotExportSpec:
    return SnapshotExportSpec(
        snapshot_id="snapshot-1",
        version="1.0.0",
        licenses=(LICENSE,),
        plugin_versions={"core": "0.3.0", "extractor": "deterministic-1"},
        created_at=NOW,
        max_inline_event_bytes=max_inline_event_bytes,
    )


def test_snapshot_inventory_round_trip_and_streaming_pages(tmp_path):
    data_path = tmp_path / "snapshot.jsonl"
    manifest_path = tmp_path / "snapshot.manifest.json"
    records = tuple(
        SnapshotRecord(
            _event(index),
            SnapshotSplit.TRAIN if index == 0 else SnapshotSplit.EVAL,
            LICENSE.id,
        )
        for index in range(3)
    )

    manifest = export_snapshot(data_path, records, _spec())
    write_snapshot_manifest(manifest_path, manifest)
    restored = read_snapshot_manifest(manifest_path)
    pages = tuple(iter_snapshot_pages(data_path, restored, page_size=2))

    assert restored == manifest
    assert manifest.record_count == 3
    assert manifest.split_counts == {"train": 1, "eval": 2, "test": 0}
    assert manifest.time_start == NOW
    assert manifest.time_end == NOW + timedelta(seconds=2)
    assert manifest.deletion_state is SnapshotDeletionState.CLEAN
    assert tuple(len(page) for page in pages) == (2, 1)
    assert tuple(record.event.id for page in pages for record in page) == (
        "event-0",
        "event-1",
        "event-2",
    )


def test_snapshot_replay_is_deterministic_with_fixed_versions_clock_and_ids(tmp_path):
    async def scenario():
        data_path = tmp_path / "snapshot.jsonl"
        records = (
            SnapshotRecord(_event(0), SnapshotSplit.TRAIN, LICENSE.id),
            SnapshotRecord(_event(1), SnapshotSplit.EVAL, LICENSE.id),
            SnapshotRecord(_event(2), SnapshotSplit.TEST, LICENSE.id),
        )
        manifest = export_snapshot(data_path, records, _spec())
        config = SnapshotReplayConfig(
            fixed_clock=NOW,
            run_seed="release-gate",
            plugin_versions=manifest.plugin_versions,
            splits=(SnapshotSplit.EVAL, SnapshotSplit.TEST),
        )
        first = build_local_kernel(tmp_path / "first.db")
        second = build_local_kernel(tmp_path / "second.db")
        await first.initialize()
        await second.initialize()

        first_report = await replay_snapshot(data_path, manifest, first, config)
        second_report = await replay_snapshot(data_path, manifest, second, config)

        assert first_report == second_report
        assert first_report.replayed_event_ids == ("event-1", "event-2")
        assert len(first_report.result_sha256) == 64
        incompatible = replace(config, plugin_versions={"core": "9.0.0"})
        with pytest.raises(SnapshotReplayError, match="plugin versions"):
            await replay_snapshot(data_path, manifest, first, incompatible)

    asyncio.run(scenario())


def test_forget_marks_snapshot_affected_and_replay_skips_deleted_sources(tmp_path):
    async def scenario():
        data_path = tmp_path / "snapshot.jsonl"
        other = MemoryScope("tenant", session_id="other")
        records = (
            SnapshotRecord(_event(0), SnapshotSplit.TRAIN, LICENSE.id),
            SnapshotRecord(_event(1, scope=other), SnapshotSplit.EVAL, LICENSE.id),
        )
        manifest = export_snapshot(data_path, records, _spec())
        unaffected = mark_snapshot_affected(
            data_path,
            manifest,
            ForgetRequest(SCOPE, memory_ids=("event-1",), mode=ForgetMode.ERASE),
            affected_at=NOW,
        )
        assert unaffected == manifest

        affected = mark_snapshot_affected(
            data_path,
            manifest,
            ForgetRequest(SCOPE, memory_ids=("event-0",), mode=ForgetMode.ERASE),
            affected_at=NOW,
        )
        assert affected.deletion_state is SnapshotDeletionState.AFFECTED
        assert affected.deletions[0].record_ids == ("event-0",)

        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        report = await replay_snapshot(
            data_path,
            affected,
            kernel,
            SnapshotReplayConfig(
                fixed_clock=NOW,
                run_seed="deletion-test",
                plugin_versions=affected.plugin_versions,
            ),
        )
        assert report.replayed_event_ids == ("event-1",)
        assert report.skipped_deleted_event_ids == ("event-0",)

    asyncio.run(scenario())


def test_replay_validates_complete_stream_before_mutating_provider(tmp_path):
    class RecordingProvider:
        def __init__(self):
            self.events = []

        async def ingest_event(self, event):
            self.events.append(event)
            raise AssertionError("tampered snapshots must not reach the provider")

    async def scenario():
        data_path = tmp_path / "snapshot.jsonl"
        manifest = export_snapshot(
            data_path,
            (SnapshotRecord(_event(0), SnapshotSplit.EVAL, LICENSE.id),),
            _spec(),
        )
        data_path.write_text(
            data_path.read_text(encoding="utf-8").replace("event-0", "tampered", 1),
            encoding="utf-8",
        )
        provider = RecordingProvider()
        with pytest.raises(SnapshotReplayError, match="digest"):
            await replay_snapshot(
                data_path,
                manifest,
                provider,
                SnapshotReplayConfig(
                    fixed_clock=NOW,
                    run_seed="tamper-test",
                    plugin_versions=manifest.plugin_versions,
                ),
            )
        assert provider.events == []

    asyncio.run(scenario())


def test_large_artifacts_are_referenced_not_copied_and_inline_payload_is_bounded(tmp_path):
    artifact = tmp_path / "large.bin"
    artifact.write_bytes(b"secret-binary-payload" * 10_000)
    event = MemoryEvent(
        SCOPE,
        "artifact.created",
        "external artifact summary",
        id="artifact-event",
        metadata={"artifact_uri": artifact.as_uri()},
        occurred_at=NOW,
        ingested_at=NOW,
    )
    record = SnapshotRecord(
        event,
        SnapshotSplit.TEST,
        LICENSE.id,
        artifact_refs=(artifact.as_uri(),),
    )
    data_path = tmp_path / "snapshot.jsonl"
    export_snapshot(data_path, (record,), _spec(max_inline_event_bytes=1_024))
    serialized = data_path.read_text(encoding="utf-8")
    assert artifact.as_uri() in serialized
    assert "secret-binary-payload" not in serialized

    oversized = SnapshotRecord(
        replace(event, id="oversized", content="x" * 2_000),
        SnapshotSplit.TEST,
        LICENSE.id,
    )
    with pytest.raises(ValueError, match="inline event payload"):
        export_snapshot(tmp_path / "oversized.jsonl", (oversized,), _spec(max_inline_event_bytes=256))
