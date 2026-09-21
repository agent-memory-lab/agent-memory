"""Read-only, privacy-safe Memory Doctor acceptance tests."""

from __future__ import annotations

import asyncio
from contextlib import closing
from datetime import datetime, timedelta, timezone
import sqlite3

from agent_memory import (
    Episode,
    MemoryDoctorCode,
    MemoryDoctorLimits,
    MemoryDoctorSeverity,
    MemoryDoctorStatus,
    MemoryEvent,
    MemoryScope,
    Provenance,
    SQLiteMemoryDoctor,
    build_local_kernel,
    build_memory_repair_plan,
)
from agent_memory.sqlite_worker_queue import SQLiteWorkerQueue


NOW = datetime(2026, 9, 21, 13, 0, tzinfo=timezone.utc)
SCOPE = MemoryScope("tenant", session_id="doctor")


def test_healthy_database_and_worker_queue_report_counts_without_findings(tmp_path):
    async def scenario():
        memory_path = tmp_path / "memory.db"
        worker_path = tmp_path / "worker.db"
        kernel = build_local_kernel(memory_path)
        queue = SQLiteWorkerQueue(worker_path)
        await kernel.initialize()
        await queue.initialize()
        await kernel.ingest_event(MemoryEvent(SCOPE, "message", "healthy", id="event-1"))

        report = await SQLiteMemoryDoctor(
            memory_path,
            worker_path=worker_path,
            clock=lambda: NOW,
        ).inspect(SCOPE)

        assert report.status is MemoryDoctorStatus.HEALTHY
        assert report.findings == ()
        assert report.counts["events"] == 1
        assert report.counts["claims"] == report.counts["artifacts"] == 0
        assert report.schema_version == 2
        assert report.database_bytes > 0

    asyncio.run(scenario())


def test_missing_artifact_evidence_is_detected_without_exposing_raw_ids(tmp_path):
    async def scenario():
        memory_path = tmp_path / "memory.db"
        kernel = build_local_kernel(memory_path)
        await kernel.initialize()
        event = MemoryEvent(SCOPE, "tool.completed", "source", id="private-event-id")
        await kernel.ingest_event(event)
        await kernel.record_episode(
            Episode(
                scope=SCOPE,
                observation="observed",
                action="acted",
                outcome="completed",
                lesson="retain evidence",
                id="private-episode-id",
                provenance=Provenance(source_event_ids=(event.id,)),
            )
        )
        with closing(sqlite3.connect(memory_path)) as connection, connection:
            connection.execute("PRAGMA foreign_keys=OFF")
            connection.execute("DELETE FROM events WHERE id=?", (event.id,))

        report = await SQLiteMemoryDoctor(memory_path, clock=lambda: NOW).inspect(SCOPE)
        finding = next(
            item for item in report.findings
            if item.code is MemoryDoctorCode.ARTIFACT_SOURCE_MISSING
        )
        serialized = report.to_json()

        assert report.status is MemoryDoctorStatus.DEGRADED
        assert finding.severity is MemoryDoctorSeverity.ERROR
        assert finding.count == 1
        assert all(len(value) == 12 for value in finding.sample_fingerprints)
        assert "private-event-id" not in serialized
        assert "private-episode-id" not in serialized

    asyncio.run(scenario())


def test_duplicate_active_claim_and_orphan_source_are_critical(tmp_path):
    async def scenario():
        memory_path = tmp_path / "memory.db"
        kernel = build_local_kernel(memory_path)
        await kernel.initialize()
        partition = SCOPE.partition_key()
        with closing(sqlite3.connect(memory_path)) as connection, connection:
            connection.execute("PRAGMA foreign_keys=OFF")
            connection.execute("DROP INDEX claims_current_idx")
            values = (
                partition, "tenant", "default", None, None, None, "doctor",
                "preference.color", '"blue"', "color is blue", 1.0, 0.5,
                "active", '{"source_event_ids":[]}', NOW.isoformat(), NOW.isoformat(), 1,
            )
            connection.execute(
                """
                INSERT INTO claims (
                    id, partition_key, tenant_id, namespace, user_id, agent_id,
                    workspace_id, session_id, claim_key, value_json, text,
                    confidence, importance, status, provenance_json, valid_from,
                    created_at, version
                ) VALUES ('claim-a',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                values,
            )
            connection.execute(
                """
                INSERT INTO claims (
                    id, partition_key, tenant_id, namespace, user_id, agent_id,
                    workspace_id, session_id, claim_key, value_json, text,
                    confidence, importance, status, provenance_json, valid_from,
                    created_at, version
                ) SELECT 'claim-b', partition_key, tenant_id, namespace, user_id,
                    agent_id, workspace_id, session_id, claim_key, value_json,
                    text, confidence, importance, status, provenance_json,
                    valid_from, created_at, version FROM claims WHERE id='claim-a'
                """
            )
            connection.execute(
                "INSERT INTO claim_sources(claim_id,event_id) VALUES ('claim-a','missing-event')"
            )

        report = await SQLiteMemoryDoctor(memory_path, clock=lambda: NOW).inspect(SCOPE)
        codes = {item.code for item in report.findings}
        assert report.status is MemoryDoctorStatus.CRITICAL
        assert MemoryDoctorCode.DUPLICATE_ACTIVE_CLAIM in codes
        assert MemoryDoctorCode.ORPHAN_CLAIM_SOURCE in codes

    asyncio.run(scenario())


def test_worker_dead_stale_and_missing_evidence_tasks_are_reported(tmp_path):
    async def scenario():
        memory_path = tmp_path / "memory.db"
        worker_path = tmp_path / "worker.db"
        kernel = build_local_kernel(memory_path)
        queue = SQLiteWorkerQueue(worker_path)
        await kernel.initialize()
        await queue.initialize()
        await queue.enqueue("dead", SCOPE, "memory.consolidate", {"event_id": "missing-dead"})
        await queue.enqueue("stale", SCOPE, "memory.consolidate", {"event_id": "missing-stale"})
        with closing(sqlite3.connect(worker_path)) as connection, connection:
            connection.execute(
                "UPDATE agent_memory_worker_tasks SET status='dead' WHERE task_key='dead'"
            )
            connection.execute(
                """
                UPDATE agent_memory_worker_tasks SET status='leased', leased_by='worker',
                    lease_token='token', lease_expires_at=? WHERE task_key='stale'
                """,
                ((NOW - timedelta(minutes=5)).isoformat(),),
            )

        report = await SQLiteMemoryDoctor(
            memory_path,
            worker_path=worker_path,
            clock=lambda: NOW,
        ).inspect(SCOPE)
        codes = {item.code for item in report.findings}
        assert MemoryDoctorCode.DEAD_WORKER_TASK in codes
        assert MemoryDoctorCode.STALE_WORKER_LEASE in codes
        assert MemoryDoctorCode.WORKER_SOURCE_MISSING in codes
        assert report.counts["worker_dead"] == 1
        assert report.counts["worker_leased"] == 1

        plan = build_memory_repair_plan(report)
        assert plan.actions
        assert all(item.requires_human_approval for item in plan.actions)
        repeated = await SQLiteMemoryDoctor(
            memory_path,
            worker_path=worker_path,
            clock=lambda: NOW,
        ).inspect(SCOPE)
        assert repeated.findings == report.findings

    asyncio.run(scenario())


def test_scope_filter_and_capacity_limits_do_not_expand_visibility(tmp_path):
    async def scenario():
        memory_path = tmp_path / "memory.db"
        kernel = build_local_kernel(memory_path)
        await kernel.initialize()
        left = MemoryScope("tenant", session_id="left")
        right = MemoryScope("tenant", session_id="right")
        await kernel.ingest_event(MemoryEvent(left, "event", "left", id="left-id"))
        await kernel.ingest_event(MemoryEvent(right, "event", "right", id="right-id"))
        with closing(sqlite3.connect(memory_path)) as connection, connection:
            connection.execute("PRAGMA foreign_keys=OFF")
            connection.execute(
                """
                INSERT INTO artifacts (
                    id, partition_key, tenant_id, namespace, session_id, kind,
                    text, payload_json, status, version, quality,
                    provenance_json, occurred_at
                ) VALUES (?,?,?,?,?,'episode','broken','{}','candidate',1,0.5,?,?)
                """,
                (
                    "right-private-artifact", right.partition_key(), "tenant", "default", "right",
                    '{"source_event_ids":["missing-right"]}', NOW.isoformat(),
                ),
            )

        doctor = SQLiteMemoryDoctor(
            memory_path,
            limits=MemoryDoctorLimits(max_database_bytes=1),
            clock=lambda: NOW,
        )
        left_report = await doctor.inspect(left)
        right_report = await doctor.inspect(right)

        assert MemoryDoctorCode.ARTIFACT_SOURCE_MISSING not in {
            item.code for item in left_report.findings
        }
        assert MemoryDoctorCode.ARTIFACT_SOURCE_MISSING in {
            item.code for item in right_report.findings
        }
        assert MemoryDoctorCode.DATABASE_CAPACITY in {
            item.code for item in left_report.findings
        }
        assert "right-private-artifact" not in left_report.to_json()

    asyncio.run(scenario())
