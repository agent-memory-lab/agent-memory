"""Acceptance tests for the opt-in scoped lexical candidate path."""

import asyncio
import threading
from datetime import datetime, timezone

import pytest

from agent_memory import AgentMemory, MemoryScope
from agent_memory.domain import MemoryChannel, MemoryItem, MemoryKind
from agent_memory.lexical_plugin import LexicalCandidatePlugin
from agent_memory.lexical_retrieval import EvidenceItem
from agent_memory.scoped_lexical_retrieval import (
    ScopeIsolationError,
    ScopedEvidenceItem,
)
from agent_memory.sqlite import SQLiteMemoryRepository
from agent_memory.sqlite_evidence_source import SQLiteRecentEventEvidenceSource


class StaticSource:
    def __init__(self, records):
        self.records = records
        self.thread_id = None

    def load(self, scope, *, limit):
        self.thread_id = threading.get_ident()
        return self.records


def _record(scope: MemoryScope) -> ScopedEvidenceItem:
    item = MemoryItem(
        "event-1",
        MemoryKind.EVENT,
        "private migration note",
        0.5,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    return ScopedEvidenceItem(
        scope,
        EvidenceItem(item, MemoryChannel.SEMANTIC, (item.id,)),
    )


def test_sqlite_events_are_scoped_and_keep_their_source_id(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            await memory.remember("violet migration checkpoint")
            repository = SQLiteMemoryRepository(database)
            source = SQLiteRecentEventEvidenceSource(repository)
            records = source.load(memory.scope, limit=8)
            assert len(records) == 1
            assert records[0].scope == memory.scope

            plugin = LexicalCandidatePlugin.from_sqlite(repository)
            result = await plugin.candidates("VIOLET migration", memory.scope)
            assert len(result.candidates) == 1
            candidate = result.candidates[0]
            assert candidate.item.id == records[0].evidence.item.id
            assert candidate.source_event_ids == (candidate.item.id,)
            assert candidate.channel == MemoryChannel.EPISODIC
            assert candidate.retrieval_method == "lexical"

            foreign = await plugin.candidates("violet", MemoryScope("another-tenant"))
            assert not foreign.candidates

    asyncio.run(scenario())


def test_sqlite_source_has_a_hard_event_window(tmp_path) -> None:
    async def scenario() -> None:
        database = tmp_path / "memory.db"
        async with AgentMemory.local(database) as memory:
            for index in range(3):
                await memory.remember(f"migration checkpoint {index}")
            repository = SQLiteMemoryRepository(database)
            plugin = LexicalCandidatePlugin.from_sqlite(repository, limit=2, max_items=2)
            result = await plugin.candidates("migration", memory.scope)
            assert len(result.candidates) == 2
            assert result.trace.input_count == 2

            with pytest.raises(ValueError):
                SQLiteRecentEventEvidenceSource(repository).load(memory.scope, limit=0)

    asyncio.run(scenario())


def test_mixed_scope_batch_is_rejected_instead_of_partially_returned() -> None:
    requested = MemoryScope("tenant-a")
    foreign = MemoryScope("tenant-b")
    source = StaticSource((_record(requested), _record(foreign)))

    with pytest.raises(ScopeIsolationError, match="another scope"):
        asyncio.run(LexicalCandidatePlugin(source).candidates("migration", requested))


def test_source_cannot_exceed_requested_capacity() -> None:
    scope = MemoryScope("tenant-a")
    source = StaticSource((_record(scope), _record(scope)))

    with pytest.raises(ScopeIsolationError, match="exceeded"):
        asyncio.run(
            LexicalCandidatePlugin(source, max_items=1).candidates("migration", scope)
        )


def test_async_plugin_runs_blocking_source_off_the_event_loop() -> None:
    async def scenario() -> None:
        source = StaticSource(())
        calling_thread = threading.get_ident()
        result = await LexicalCandidatePlugin(source).candidates(
            "migration", MemoryScope("tenant-a")
        )
        assert source.thread_id is not None
        assert source.thread_id != calling_thread
        assert not result.candidates

    asyncio.run(scenario())
