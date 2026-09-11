import asyncio
import sqlite3

import pytest

from agent_memory import AgentMemory, DecisionRecord, MemoryScope, MemoryUsage
from agent_memory.sqlite import SQLiteMemoryRepository


def test_feedback_history_is_stable_and_paginated(tmp_path) -> None:
    async def scenario() -> None:
        async with AgentMemory.local(tmp_path / "memory.db") as memory:
            for index in range(5):
                await memory.record_decision(
                    f"action-{index}",
                    memory_usage=MemoryUsage.NONE,
                    record_id=f"decision-{index}",
                )
            first = await memory.feedback_history("decision", limit=2)
            second = await memory.feedback_history(
                "decision", limit=2, cursor=first.next_cursor
            )
            third = await memory.feedback_history(
                "decision", limit=2, cursor=second.next_cursor
            )

            ids = [item.record_id for page in (first, second, third) for item in page.items]
            assert len(ids) == 5
            assert len(set(ids)) == 5
            assert first.next_cursor is not None
            assert second.next_cursor is not None
            assert third.next_cursor is None

            with pytest.raises(ValueError, match="cursor is invalid"):
                await memory.feedback_history("decision", cursor="not-visible")

    asyncio.run(scenario())


def test_feedback_survives_provider_restart(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as first:
            decision = await first.record_decision(
                "answer", memory_usage=MemoryUsage.NONE
            )

        async with AgentMemory.local(database) as restarted:
            receipt = await restarted.feedback_status(decision.id, "decision")
            assert receipt is not None
            assert receipt.record_id == decision.id

    asyncio.run(scenario())


def test_feedback_unit_of_work_rolls_back_atomically(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        repository = SQLiteMemoryRepository(database)
        await repository.initialize()
        decision = DecisionRecord(
            scope=MemoryScope("tenant", session_id="session"),
            action="must rollback",
            memory_ids=(),
            memory_usage=MemoryUsage.NONE,
        )
        with pytest.raises(RuntimeError, match="force rollback"):
            async with repository.unit_of_work() as uow:
                await uow.save_decision(decision)
                raise RuntimeError("force rollback")

        with sqlite3.connect(database) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM evolution_records WHERE id = ?", (decision.id,)
            ).fetchone()[0]
        assert count == 0

    asyncio.run(scenario())


def test_multiple_sqlite_providers_share_feedback_idempotency(tmp_path) -> None:
    database = tmp_path / "memory.db"

    async def write() -> str:
        async with AgentMemory.local(database) as memory:
            decision = await memory.record_decision(
                "same action",
                memory_usage=MemoryUsage.NONE,
                idempotency_key="concurrent-request",
            )
            return decision.id

    async def scenario() -> None:
        ids = await asyncio.gather(
            asyncio.to_thread(lambda: asyncio.run(write())),
            asyncio.to_thread(lambda: asyncio.run(write())),
        )
        assert ids[0] == ids[1]
        with sqlite3.connect(database) as connection:
            count = connection.execute(
                """
                SELECT COUNT(*) FROM evolution_records
                WHERE record_type = 'decision' AND idempotency_key = 'concurrent-request'
                """
            ).fetchone()[0]
        assert count == 1

    asyncio.run(scenario())
