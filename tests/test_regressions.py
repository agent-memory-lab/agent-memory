from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing

from agent_memory import (
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryScope,
    ScopeLevel,
    build_local_kernel,
)


def run(coroutine):
    return asyncio.run(coroutine)


def test_duplicate_event_claim_keys_are_deduplicated(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", user_id="user-a", session_id="session-a")

        event = MemoryEvent(
            scope,
            "agent.message",
            "Duplicate claim key extraction payload.",
            metadata={
                "claims": [
                    {
                        "key": "contact.preference",
                        "value": "email",
                        "text": "Use email",
                        "scope": "user",
                    },
                    {
                        "key": "contact.preference",
                        "value": "sms",
                        "text": "Use SMS",
                        "scope": "user",
                    },
                    {"key": "other.key", "value": "ok", "text": "Different key", "scope": "user"},
                ]
            },
        )
        result = await kernel.ingest_event(event)

        assert len(result.claim_ids) == 2
        state = await kernel.get_state(scope)
        assert len(state) == 2
        assert {claim.key for claim in state} == {"contact.preference", "other.key"}
        assert next(claim for claim in state if claim.key == "contact.preference").value == "email"

    run(scenario())


def test_forget_targeted_event_partial_source_removal_does_not_delete_claim(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", user_id="user-a", session_id="session-a")

        first = MemoryEvent(
            scope,
            "agent.message",
            "Preference is email.",
            metadata={
                "claims": [
                    {
                        "key": "contact.preference",
                        "value": "email",
                        "text": "Use email.",
                        "scope": "user",
                    }
                ]
            },
        )
        second = MemoryEvent(
            scope,
            "agent.message",
            "Preference is still email.",
            metadata={
                "claims": [
                    {
                        "key": "contact.preference",
                        "value": "email",
                        "text": "Use email.",
                        "scope": "user",
                    }
                ]
            },
        )
        await kernel.ingest_event(first)
        await kernel.ingest_event(second)

        before = await kernel.get_state(scope)
        assert len(before) == 1
        assert before[0].provenance.source_event_ids == (first.id, second.id)

        result = await kernel.forget(
            ForgetRequest(scope=scope, memory_ids=(first.id,), mode=ForgetMode.ERASE)
        )
        assert result.affected_events == 1
        assert result.affected_claims == 0

        after = await kernel.get_state(scope)
        assert len(after) == 1
        assert after[0].provenance.source_event_ids == (second.id,)

    run(scenario())


def test_forget_targeted_event_removes_orphan_claim(tmp_path) -> None:
    async def scenario() -> None:
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant-a", user_id="user-a", session_id="session-a")

        event = MemoryEvent(
            scope,
            "agent.message",
            "Single-source claim.",
            metadata={
                "claims": [
                    {
                        "key": "contact.preference",
                        "value": "sms",
                        "text": "Use SMS.",
                        "scope": "user",
                    }
                ]
            },
        )
        await kernel.ingest_event(event)
        assert len(await kernel.get_state(scope)) == 1

        result = await kernel.forget(
            ForgetRequest(scope=scope, memory_ids=(event.id,), mode=ForgetMode.ERASE)
        )
        assert result.affected_events == 1
        assert result.affected_claims == 1
        assert not await kernel.get_state(scope)

    run(scenario())


def test_multiple_kernels_can_ingest_same_key_without_multiple_active_claims(tmp_path) -> None:
    async def scenario() -> None:
        db_path = tmp_path / "memory.db"
        left = build_local_kernel(db_path)
        right = build_local_kernel(db_path)
        await left.initialize()
        await right.initialize()

        scope = MemoryScope("tenant-a", user_id="user-a", session_id="session-a")

        async def ingest_with(kernel, value: str) -> None:
            await kernel.ingest_event(
                MemoryEvent(
                    scope,
                    "agent.message",
                    f"Value: {value}",
                    metadata={
                        "claims": [
                            {
                                "key": "contact.preference",
                                "value": value,
                                "text": f"Use {value}.",
                                "scope": "user",
                            }
                        ]
                    },
                )
            )

        await asyncio.gather(
            *(ingest_with(left, str(index)) for index in range(1, 4)),
            *(ingest_with(right, str(index)) for index in range(4, 7)),
        )

        state = await left.get_state(scope)
        assert len(state) == 1
        assert state[0].version == 6

    run(scenario())


def test_initialize_migrates_duplicate_active_claim_rows_and_repairs_uniqueness(tmp_path) -> None:
    async def scenario() -> None:
        db_path = tmp_path / "memory.db"
        base = build_local_kernel(db_path)
        await base.initialize()
        scope = MemoryScope("tenant-a", user_id="user-a", session_id="session-a")

        claim_event = MemoryEvent(
            scope,
            "agent.message",
            "Preference is email.",
            metadata={
                "claims": [
                    {
                        "key": "contact.preference",
                        "value": "email",
                        "text": "Use email.",
                        "scope": "user",
                    }
                ]
            },
        )
        await base.ingest_event(claim_event)
        base_scope = scope.project(ScopeLevel.USER).partition_key()

        with closing(sqlite3.connect(db_path)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("DROP INDEX IF EXISTS claims_current_idx")
            row = connection.execute(
                """
                INSERT INTO claims (
                    id, partition_key, tenant_id, namespace, user_id, agent_id, workspace_id,
                    session_id, claim_key, value_json, text, confidence, importance,
                    status, provenance_json, valid_from, valid_to, created_at, version,
                    supersedes, superseded_by, archived_at
                )
                SELECT
                    ?, partition_key, tenant_id, namespace, user_id, agent_id, workspace_id,
                    session_id, claim_key, value_json, text, confidence, importance,
                    status, provenance_json, valid_from, valid_to, created_at, version,
                    supersedes, superseded_by, archived_at
                FROM claims
                WHERE id = (SELECT id FROM claims LIMIT 1)
                """,
                (str(claim_event.id) + "-duplicate",),
            ).rowcount
            if row == 0:
                raise AssertionError("missing base claim row for duplicate injection")

        reloaded = build_local_kernel(db_path)
        await reloaded.initialize()
        claims = await reloaded.get_state(scope)
        assert len(claims) == 1
        with closing(sqlite3.connect(db_path)) as connection:
            connection.row_factory = sqlite3.Row
            count = connection.execute(
                """
                SELECT COUNT(*) AS total
                FROM claims
                WHERE partition_key = ? AND claim_key = ? AND status = 'active'
                """,
                (base_scope, "contact.preference"),
            ).fetchone()["total"]
            assert count == 1

    run(scenario())
