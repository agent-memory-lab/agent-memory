import asyncio
import json
import sqlite3

import pytest

from agent_memory import AgentMemory, MemoryQuery, MemoryUsage


def test_retrieval_trace_links_confirmed_usage_without_copying_content(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            await memory.remember(
                "A private preference payload that must not be copied into traces.",
                claims=(
                    {
                        "key": "style",
                        "value": "concise",
                        "text": "Keep answers concise",
                        "scope_level": "session",
                    },
                ),
            )
            bundle = await memory.recall("concise")
            used = tuple(citation.memory_id for citation in bundle.citations)
            decision = await memory.record_decision(
                "answer",
                memory_ids=used,
                memory_usage=MemoryUsage.CONFIRMED,
                bundle_id=bundle.bundle_id,
            )

        with sqlite3.connect(database) as connection:
            row = connection.execute(
                "SELECT payload_json FROM evolution_records WHERE id = ?",
                (bundle.bundle_id,),
            ).fetchone()
        payload = json.loads(row[0])
        assert payload["request_id"] == bundle.request_id
        assert set(payload["returned_memory_ids"]) == set(used)
        assert "private preference payload" not in row[0].lower()
        assert decision.bundle_id == bundle.bundle_id

    asyncio.run(scenario())


def test_decision_rejects_missing_bundle_and_unreturned_memory(tmp_path) -> None:
    async def scenario() -> None:
        async with AgentMemory.local(tmp_path / "memory.db") as memory:
            with pytest.raises(ValueError, match="retrieval bundle"):
                await memory.record_decision(
                    "answer",
                    memory_ids=("unknown",),
                    memory_usage=MemoryUsage.CONFIRMED,
                    bundle_id="missing",
                )

            await memory.remember("Remember this event")
            bundle = await memory.recall("remember")
            with pytest.raises(ValueError, match="not present in the bundle"):
                await memory.record_decision(
                    "answer",
                    memory_ids=("not-returned",),
                    memory_usage=MemoryUsage.CONFIRMED,
                    bundle_id=bundle.bundle_id,
                )

    asyncio.run(scenario())


def test_retrieval_tracing_can_be_disabled(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            bundle = await memory.provider.retrieve(
                MemoryQuery(scope=memory.scope, text="anything", trace_enabled=False)
            )
        with sqlite3.connect(database) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM evolution_records WHERE record_type = 'retrieval'"
            ).fetchone()[0]
        assert bundle.bundle_id
        assert count == 0

    asyncio.run(scenario())
