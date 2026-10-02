"""Behavioral contract shared by SQLite and a real, isolated PostgreSQL database."""

import asyncio
import os
import sqlite3
from contextlib import asynccontextmanager, closing
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from agent_memory import (
    AgentMemory,
    ForgetMode,
    ForgetRequest,
    MCPMemoryTools,
    MCPRequestContext,
    MemoryEvent,
    MemoryKind,
    MemoryQuery,
    MemoryScope,
    TemporalHistoryUnavailable,
)


def at(day, hour=0):
    return datetime(2026, 9, day, hour, tzinfo=UTC)


@pytest.fixture(params=["sqlite", "postgres"])
def store(request, tmp_path, monkeypatch):
    import agent_memory.domain as domain
    import agent_memory.kernel as kernel
    import agent_memory.retrieval.temporal_history as history
    import agent_memory.sqlite as local

    if request.param == "postgres":
        pytest.importorskip("agent_memory_postgres.repository")
        pytest.importorskip("agent_memory_postgres.temporal_history")

    clock = [at(1)]
    for module in (domain, kernel, history, local):
        monkeypatch.setattr(module, "utc_now", lambda: clock[0])
    scope = MemoryScope(f"temporal-{uuid4().hex}", session_id="session")
    path = tmp_path / "memory.db"
    if request.param == "postgres":
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN")
        if not dsn:
            pytest.skip("real PostgreSQL temporal tests require a test DSN")
        from urllib.parse import urlsplit

        if "test" not in urlsplit(dsn).path.casefold():
            pytest.fail("temporal PostgreSQL tests require a database named test")
        pg = pytest.importorskip("agent_memory_postgres.repository")
        ph = pytest.importorskip("agent_memory_postgres.temporal_history")
        from agent_memory_postgres import build_postgres_kernel

        for module in (pg, ph):
            monkeypatch.setattr(module, "utc_now", lambda: clock[0])
        memory = AgentMemory(build_postgres_kernel(dsn), scope)
    else:
        memory = AgentMemory.local(path, scope=scope)

    @asynccontextmanager
    async def open_store():
        await memory.initialize()
        try:
            yield memory, clock
        finally:
            await memory.__aexit__(None, None, None)

    return open_store


async def assert_fact(
    memory,
    clock,
    value,
    valid,
    known,
    *,
    end=None,
    corrects=None,
    key="city",
    scope=None,
    explicit=True,
    idempotency=None,
):
    clock[0] = at(known)
    claim = {"key": key, "value": value, "text": f"{key}: {value}"}
    if explicit:
        claim["valid_from"] = at(valid).isoformat()
    if end:
        claim["valid_to"] = at(end).isoformat()
    if corrects:
        claim["corrects_id"] = corrects
    event = MemoryEvent(
        scope=scope or memory.scope,
        event_type="fact",
        content=claim["text"],
        occurred_at=at(valid),
        metadata={"claims": [claim]},
        idempotency_key=idempotency,
    )
    return await memory.provider.ingest_event(event)


async def state(memory, valid, known):
    return await memory.provider.get_state_at(memory.scope, valid_at=at(valid), known_at=at(known))


def test_change_and_retroactive_correction_preserve_past_knowledge(store):
    async def scenario():
        async with store() as (memory, clock):
            await assert_fact(memory, clock, "Shanghai", 1, 1)
            moved = await assert_fact(memory, clock, "Hangzhou", 5, 10)
            correction = await assert_fact(
                memory, clock, "Hangzhou", 8, 20, corrects=moved.claim_ids[0]
            )
            assert [c.value for c in await state(memory, 6, 2)] == ["Shanghai"]
            assert [c.value for c in await state(memory, 6, 15)] == ["Hangzhou"]
            assert [c.value for c in await state(memory, 6, 25)] == ["Shanghai"]
            corrected = (await state(memory, 8, 20))[0]
            assert corrected.value == "Hangzhou"
            assert corrected.corrects_id == moved.claim_ids[0]
            assert corrected.id == correction.claim_ids[0]
            assert corrected.system_from == at(20)
            old = (await state(memory, 6, 15))[0]
            assert old.system_from == at(10) and old.system_to == at(20)
            # Transaction and validity boundaries are both [start, end).
            assert (await state(memory, 5, 10))[0].value == "Hangzhou"
            assert (await state(memory, 7, 20))[0].value == "Shanghai"
            assert memory.provider.manifest().capabilities.bitemporal_claims

    asyncio.run(scenario())


def test_late_old_observation_does_not_replace_newer_effective_fact(store):
    async def scenario():
        async with store() as (memory, clock):
            await assert_fact(memory, clock, "A", 1, 1)
            await assert_fact(memory, clock, "B", 10, 10)
            await assert_fact(memory, clock, "C", 3, 20)
            assert (await state(memory, 4, 21))[0].value == "C"
            assert (await state(memory, 12, 21))[0].value == "B"
            assert (await memory.provider.get_state(memory.scope))[0].value == "B"
            bundle = await memory.provider.retrieve(
                MemoryQuery(memory.scope, "city", include_current_state=False)
            )
            assert {
                item.text for item in bundle.relevant_memories if item.kind == MemoryKind.CLAIM
            } == {"city: B"}

    asyncio.run(scenario())


def test_finite_interval_restores_previous_value(store):
    async def scenario():
        async with store() as (memory, clock):
            await assert_fact(memory, clock, "A", 1, 1)
            await assert_fact(memory, clock, "B", 5, 10, end=8)
            assert (await state(memory, 5, 11))[0].value == "B"
            assert (await state(memory, 7, 11))[0].valid_to == at(8)
            assert (await state(memory, 8, 11))[0].value == "A"
            assert (await memory.provider.get_state(memory.scope))[0].value == "A"

    asyncio.run(scenario())


def test_future_effective_fact_is_not_current_state(store):
    async def scenario():
        async with store() as (memory, clock):
            await assert_fact(memory, clock, "A", 1, 1)
            await assert_fact(memory, clock, "B", 20, 10)
            assert (await memory.provider.get_state(memory.scope))[0].value == "A"
            assert (await state(memory, 20, 11))[0].value == "B"
            assert (await state(memory, 20, 2))[0].value == "A"

    asyncio.run(scenario())


def test_failed_correction_is_atomic_and_scope_checked(store):
    async def scenario():
        async with store() as (memory, clock):
            original = await assert_fact(memory, clock, "A", 1, 1)
            other = MemoryScope("other-tenant", session_id="session")
            with pytest.raises(ValueError, match="correction target"):
                await assert_fact(
                    memory, clock, "B", 1, 10, corrects=original.claim_ids[0], scope=other
                )
            assert await memory.provider.get_state_at(other, valid_at=at(2), known_at=at(11)) == ()
            with pytest.raises(ValueError, match="correction target"):
                await assert_fact(
                    memory, clock, "B", 1, 10, corrects=original.claim_ids[0], key="name"
                )
            assert (await state(memory, 2, 11))[0].value == "A"
            await assert_fact(memory, clock, "B", 1, 20, corrects=original.claim_ids[0])
            with pytest.raises(ValueError, match="correction target"):
                await assert_fact(memory, clock, "C", 1, 25, corrects=original.claim_ids[0])
            assert (await state(memory, 2, 26))[0].value == "B"

    asyncio.run(scenario())


def test_corroboration_does_not_add_future_evidence_to_old_snapshot(store):
    async def scenario():
        async with store() as (memory, clock):
            first = await assert_fact(memory, clock, "A", 1, 1, explicit=False)
            second = await assert_fact(memory, clock, "A", 5, 10, explicit=False)
            assert first.claim_ids == second.claim_ids
            assert (await state(memory, 6, 2))[0].provenance.source_event_ids == (first.event_id,)
            assert set((await state(memory, 6, 11))[0].provenance.source_event_ids) == {
                first.event_id,
                second.event_id,
            }
            await memory.provider.forget(
                ForgetRequest(memory.scope, (first.event_id,), mode=ForgetMode.ERASE)
            )
            assert await state(memory, 6, 2) == ()
            assert (await state(memory, 6, 11))[0].provenance.source_event_ids == (second.event_id,)

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_forgotten_history_is_not_retrievable(store, mode):
    async def scenario():
        async with store() as (memory, clock):
            first = await assert_fact(memory, clock, "A", 1, 1)
            await memory.provider.forget(ForgetRequest(memory.scope, (first.event_id,), mode=mode))
            assert await state(memory, 2, 2) == ()
            assert (
                await memory.recall("city", valid_at=at(2), known_at=at(2))
            ).relevant_memories == ()

    asyncio.run(scenario())


def test_idempotent_event_does_not_create_another_system_version(store):
    async def scenario():
        async with store() as (memory, clock):
            first = await assert_fact(memory, clock, "A", 1, 1, idempotency="one")
            again = await assert_fact(memory, clock, "A", 1, 10, idempotency="one")
            assert again.duplicate and again.claim_ids == first.claim_ids
            assert (await state(memory, 2, 11))[0].system_from == at(1)

    asyncio.run(scenario())


def test_historical_retrieval_excludes_current_events_and_derived_blocks(store):
    async def scenario():
        async with store() as (memory, clock):
            await assert_fact(memory, clock, "A", 1, 1)
            await assert_fact(memory, clock, "B", 5, 10)
            bundle = await memory.recall("city", valid_at=at(6), known_at=at(2))
            assert [claim.value for claim in bundle.current_state] == ["A"]
            assert all(item.kind == MemoryKind.CLAIM for item in bundle.relevant_memories)
            bundle = await memory.provider.retrieve(
                MemoryQuery(
                    memory.scope,
                    "city",
                    valid_at=at(6),
                    known_at=at(2),
                    include_current_state=False,
                )
            )
            assert [item.text for item in bundle.relevant_memories] == ["city: A"]

    asyncio.run(scenario())


def test_mcp_and_sdk_forward_temporal_query_and_reject_naive_time(store):
    async def scenario():
        async with store() as (memory, clock):
            await assert_fact(memory, clock, "A", 1, 1)
            await assert_fact(memory, clock, "B", 5, 10)
            from agent_memory_sdk import EmbeddedMemoryClient, MemoryClientError

            client = EmbeddedMemoryClient(memory.provider, MCPRequestContext(memory.scope))
            result = await client.retrieve(
                "city", valid_at=at(6).isoformat(), known_at=at(2).isoformat()
            )
            assert result["current_state"][0]["value"] == "A"
            with pytest.raises(MemoryClientError, match="timezone"):
                await client.retrieve("city", valid_at="2026-09-06T00:00:00")
            schemas = MCPMemoryTools(memory.provider).list_tools()
            retrieve = next(tool for tool in schemas if tool["name"] == "memory_retrieve")
            assert {"valid_at", "known_at"} <= retrieve["inputSchema"]["properties"].keys()

    asyncio.run(scenario())


def test_timezone_offsets_are_normalized_and_query_times_require_timezone(store):
    async def scenario():
        async with store() as (memory, clock):
            clock[0] = at(10)
            result = await memory.remember(
                "city A",
                claims=[
                    {
                        "key": "city",
                        "value": "A",
                        "text": "city A",
                        "valid_from": "2026-09-05T08:00:00+08:00",
                        "valid_to": "2026-09-06T08:00:00+08:00",
                    }
                ],
            )
            assert result.claim_ids
            assert (await state(memory, 5, 11))[0].valid_from == at(5)
            assert await state(memory, 6, 11) == ()
            with pytest.raises(ValueError, match="timezone"):
                MemoryQuery(memory.scope, "city", valid_at=datetime(2026, 9, 5))

    asyncio.run(scenario())


def test_observation_budget_failure_rolls_back_previous_revision(store, monkeypatch):
    async def scenario():
        async with store() as (memory, clock):
            import agent_memory.retrieval.temporal_history as history

            monkeypatch.setattr(history, "MAX_OBSERVATIONS_PER_KEY", 2)
            await assert_fact(memory, clock, "A", 1, 1)
            await assert_fact(memory, clock, "B", 5, 10)
            with pytest.raises(ValueError, match="observation limit"):
                await assert_fact(memory, clock, "C", 8, 20)
            assert (await state(memory, 9, 21))[0].value == "B"
            assert (await state(memory, 2, 21))[0].value == "A"

    asyncio.run(scenario())


def test_old_sqlite_migration_exposes_a_truthful_history_floor(tmp_path):
    async def scenario():
        path = tmp_path / "legacy.db"
        memory = AgentMemory.local(path)
        await memory.initialize()
        result = await memory.remember(
            "city A",
            claims=[
                {"key": "city", "value": "A", "text": "city A", "valid_from": at(1).isoformat()}
            ],
        )
        await memory.__aexit__(None, None, None)
        with closing(sqlite3.connect(path)) as db:
            db.execute("DROP TABLE claim_versions")
            db.execute("DROP TABLE claim_observations")
        memory = AgentMemory.local(path)
        await memory.initialize()
        try:
            with pytest.raises(TemporalHistoryUnavailable, match="predates"):
                await memory.recall("city", valid_at=at(2), known_at=at(2))
            current = await memory.provider.get_state(memory.scope)
            assert current[0].id == result.claim_ids[0]
            floor = current[0].system_from
            await memory.provider.initialize()
            assert (await memory.provider.get_state(memory.scope))[0].system_from == floor
        finally:
            await memory.__aexit__(None, None, None)

    asyncio.run(scenario())


def test_late_observation_corroboration_checks_effective_state(store):
    async def scenario():
        async with store() as (memory, clock):
            await assert_fact(memory, clock, "A", 1, 1)
            await assert_fact(memory, clock, "B", 10, 10)
            late = await assert_fact(memory, clock, "C", 3, 20)
            fresh = await assert_fact(memory, clock, "C", 21, 21, explicit=False)
            assert fresh.claim_ids != late.claim_ids
            assert (await state(memory, 12, 22))[0].value == "B"
            assert (await state(memory, 21, 22))[0].value == "C"

    asyncio.run(scenario())


def test_history_migration_is_idempotent_in_both_backends(store):
    async def scenario():
        async with store() as (memory, clock):
            first = await assert_fact(memory, clock, "A", 1, 1)
            repo = memory.provider._repository
            from agent_memory.sqlite import SQLiteMemoryRepository

            if isinstance(repo, SQLiteMemoryRepository):
                with repo._connection() as db:
                    db.execute(
                        "DELETE FROM claim_versions WHERE claim_id = ?", (first.claim_ids[0],)
                    )
                    db.execute(
                        "DELETE FROM claim_observations WHERE claim_id = ?", (first.claim_ids[0],)
                    )
            else:
                async with repo.pool.connection() as db:
                    await db.execute(
                        "DELETE FROM agent_memory_claim_versions WHERE claim_id = %s",
                        (first.claim_ids[0],),
                    )
                    await db.execute(
                        "DELETE FROM agent_memory_claim_observations WHERE claim_id = %s",
                        (first.claim_ids[0],),
                    )
            clock[0] = at(10)
            await repo.initialize()
            with pytest.raises(TemporalHistoryUnavailable):
                await state(memory, 2, 2)
            assert (await state(memory, 2, 11))[0].system_from == at(10)
            clock[0] = at(20)
            await repo.initialize()
            assert (await state(memory, 2, 21))[0].system_from == at(10)

    asyncio.run(scenario())


def test_system_revisions_are_monotonic_when_writer_clock_moves_back(store):
    async def scenario():
        async with store() as (memory, clock):
            await assert_fact(memory, clock, "A", 1, 10)
            await assert_fact(memory, clock, "B", 1, 2)
            assert (await state(memory, 2, 10))[0].value == "A"
            assert (await state(memory, 2, 11))[0].value == "B"
            assert (await state(memory, 2, 11))[0].system_from > at(10)

    asyncio.run(scenario())


def test_concurrent_writers_leave_one_effective_value_and_preserve_revisions(store):
    async def scenario():
        async with store() as (memory, clock):
            await asyncio.gather(
                *(assert_fact(memory, clock, value, 1, 10) for value in ("A", "B", "C"))
            )
            current = await state(memory, 2, 11)
            assert len(current) == 1
            first = await state(memory, 2, 10)
            assert len(first) == 1
            assert first[0].id != current[0].id

    asyncio.run(scenario())


def test_unified_facade_forwards_both_time_axes(tmp_path, monkeypatch):
    async def scenario():
        import agent_memory.retrieval.temporal_history as history
        from agent_memory.unified_memory import UnifiedMemory

        clock = [at(1)]
        monkeypatch.setattr(history, "utc_now", lambda: clock[0])

        class Generator:
            async def generate_claims(self, event):
                return [{"key": "city", "value": event.content, "text": event.content}]

        memory = UnifiedMemory.local(
            tmp_path / "unified.db",
            MemoryScope("unified", session_id="session"),
            generator=Generator(),
        )
        await memory.initialize()
        try:
            await memory.capture(
                event_id="first", role="user", content="A", run_id="run", occurred_at=at(1)
            )
            clock[0] = at(10)
            await memory.capture(
                event_id="second", role="user", content="B", run_id="run", occurred_at=at(5)
            )
            result = await memory.recall("city", valid_at=at(6), known_at=at(2))
            assert [claim.value for claim in result.current_state] == ["A"]
            assert result.retrieval_metadata["known_at"] == at(2).isoformat()
        finally:
            await memory.close()

    asyncio.run(scenario())


def test_postgres_retrieval_works_with_a_single_connection_pool():
    async def scenario():
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN")
        if not dsn:
            pytest.skip("single-connection PostgreSQL test requires a test DSN")
        from urllib.parse import urlsplit

        if "test" not in urlsplit(dsn).path.casefold():
            pytest.fail("PostgreSQL tests require a database named test")
        from agent_memory_postgres import build_postgres_kernel

        memory = AgentMemory(
            build_postgres_kernel(dsn, min_pool_size=1, max_pool_size=1),
            MemoryScope(f"one-pool-{uuid4().hex}", session_id="s"),
        )
        await memory.initialize()
        try:
            await memory.remember(
                "city A", claims=[{"key": "city", "value": "A", "text": "city A"}]
            )
            result = await asyncio.wait_for(memory.recall("city"), timeout=5)
            assert result.current_state[0].value == "A"
        finally:
            await memory.__aexit__(None, None, None)

    asyncio.run(scenario())


def test_mcp_does_not_advertise_or_silently_ignore_unsupported_temporal_query(tmp_path):
    async def scenario():
        from dataclasses import replace

        from agent_memory import MCPToolError

        memory = AgentMemory.local(tmp_path / "no-temporal.db")
        await memory.initialize()
        try:

            class NonTemporalProvider:
                def manifest(self):
                    manifest = memory.provider.manifest()
                    return replace(
                        manifest,
                        capabilities=replace(manifest.capabilities, bitemporal_claims=False),
                    )

                async def retrieve(self, query):
                    pytest.fail(
                        "unsupported query must be rejected before retrieving current state"
                    )

            tools = MCPMemoryTools(NonTemporalProvider())
            schema = next(tool for tool in tools.list_tools() if tool["name"] == "memory_retrieve")
            assert "known_at" not in schema["inputSchema"]["properties"]
            with pytest.raises(MCPToolError) as error:
                await tools.call_tool(
                    "memory_retrieve",
                    {"text": "city", "known_at": at(2).isoformat()},
                    MCPRequestContext(memory.scope),
                )
            assert error.value.code == "unsupported_capability"
        finally:
            await memory.__aexit__(None, None, None)

    asyncio.run(scenario())
