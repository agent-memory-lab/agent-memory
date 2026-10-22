import asyncio
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_memory.context.compression import CompressionPlan, ContextSegment
from agent_memory.context.compression_strategy import (
    CompressionStrategy,
    CompressionStrategyRegistry,
    StrategyApproval,
    StrategyBinding,
)
from agent_memory.context.recovery import RecoveryState
from agent_memory.context.source_recovery import RepositorySourceRecovery, SourceRecoveryReference
from agent_memory.domain import MemoryScope
from agent_memory.sqlite import SQLiteMemoryRepository
from agent_memory.unified_memory import UnifiedMemory

SCOPE = MemoryScope("source-recovery", session_id="session")
CONTENT = "Owner: Ada. Deadline: October 30. " + "Source details. " * 200


def memory_at(path, **kwargs):
    return UnifiedMemory.local(
        path / "memory.db",
        SCOPE,
        recovery_path=path / "recovery.db",
        recoverable_compression=True,
        **kwargs,
    )


async def prepare(memory, *, text=CONTENT):
    receipt = await memory.capture_with_receipt(
        event_id="event",
        run_id="run",
        role="user",
        content=CONTENT,
        occurred_at=datetime(2026, 10, 22, tzinfo=UTC),
    )
    source_id = receipt.provider_event_id
    await memory.save_recovery(RecoveryState("run", 1, "Find owner", (source_id,)))
    plan = CompressionPlan("run", 1, (ContextSegment(source_id, text),), token_budget=1800)
    return source_id, plan


def test_omitted_original_can_be_recovered_after_restart(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            source_id, plan = await prepare(memory)
            result = await memory.propose_compression(plan)
            assert result.accepted, result.reason
            replacement = json.loads(result.replacement)
            assert replacement["format"] == "agent-memory-context-v2"
            assert replacement["summary"] == ""
            assert len(replacement["recoverable_sources"]) == 1
            assert await memory.recover_compressed_source(result.summary_id, source_id) == CONTENT
        finally:
            await memory.close()
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            assert await memory.recover_compressed_source(result.summary_id, source_id) == CONTENT
            assert await memory.recover_compressed_source(result.summary_id, "foreign") is None
            await memory.forget_sources((source_id,))
            assert await memory.load_compression(result.summary_id) is None
            assert await memory.recover_compressed_source(result.summary_id, source_id) is None
        finally:
            await memory.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("text", ["Made-up source", "Source details."])
def test_nonexistent_or_ambiguous_excerpts_cannot_get_recovery_references(tmp_path, text):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            _, plan = await prepare(memory, text=text)
            result = await memory.propose_compression(plan)
            assert not result.accepted and result.summary_id is None
        finally:
            await memory.close()

    asyncio.run(scenario())


def test_changed_or_cross_scope_sources_fail_closed(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            source_id, plan = await prepare(memory, text="Owner: Ada.")
            result = await memory.propose_compression(plan)
            assert result.accepted, result.reason
            reference = json.loads(result.replacement)["recoverable_sources"][0]
            recovery = RepositorySourceRecovery(SQLiteMemoryRepository(tmp_path / "memory.db"))
            assert await recovery.restore(MemoryScope("foreign"), reference) is None
            assert await recovery.restore(SCOPE, {**reference, "excerpt_sha256": "0" * 64}) is None
            with closing(sqlite3.connect(tmp_path / "memory.db")) as db, db:
                db.execute("UPDATE events SET content=? WHERE id=?", ("Owner: Bob.", source_id))
            assert await memory.load_compression(result.summary_id) is None
            assert await memory.recover_compressed_source(result.summary_id, source_id) is None
        finally:
            await memory.close()

    asyncio.run(scenario())


def test_state_changes_and_archives_invalidate_recovery(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            source_id, plan = await prepare(memory)
            result = await memory.propose_compression(plan)
            assert result.accepted, result.reason
            state = await memory.load_recovery("run")
            await memory.save_recovery(replace(state, version=2), expected_version=1)
            assert await memory.recover_compressed_source(result.summary_id, source_id) is None
            result = await memory.propose_compression(replace(plan, recovery_version=2))
            await memory.forget_sources((source_id,), erase=False)
            assert await memory.recover_compressed_source(result.summary_id, source_id) is None
        finally:
            await memory.close()

    asyncio.run(scenario())


def test_recovery_metadata_counts_toward_budget(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            _, plan = await prepare(memory)
            result = await memory.propose_compression(replace(plan, token_budget=100))
            assert not result.accepted
            assert result.reason == "required_state_exceeds_budget"
        finally:
            await memory.close()

    asyncio.run(scenario())


def test_recoverable_compression_requires_a_persistent_recovery_host(tmp_path):
    with pytest.raises(ValueError, match="recovery"):
        UnifiedMemory.local(tmp_path / "memory.db", SCOPE, recoverable_compression=True)


def test_reference_bounds_are_strict():
    with pytest.raises(ValueError, match="bounds"):
        SourceRecoveryReference("id", "a" * 64, "b" * 64, True, 5, "c" * 64)


def during_recovery(memory, stage, mutation):
    """Place an independently committed write inside an asynchronous boundary."""
    source_recovery = memory._compression.source_recovery
    original = getattr(source_recovery, stage)
    calls = 0

    async def hooked(*args):
        nonlocal calls
        result = await original(*args)
        calls += 1
        # First validate belongs to initial load; second is after restoration.
        if calls == (2 if stage == "validate" else 1):
            await mutation()
        return result

    setattr(source_recovery, stage, hooked)


@pytest.mark.parametrize("stage", ["restore", "validate"])
def test_state_committed_by_other_instance_during_recovery_is_rejected(tmp_path, stage):
    async def scenario():
        memory, other = memory_at(tmp_path), memory_at(tmp_path)
        await memory.initialize()
        await other.initialize()
        try:
            source_id, plan = await prepare(memory)
            proposal = await memory.propose_compression(plan)

            async def advance():
                state = await other.load_recovery("run")
                await other.save_recovery(replace(state, version=2), expected_version=1)

            during_recovery(memory, stage, advance)
            assert await memory.recover_compressed_source(proposal.summary_id, source_id) is None
            assert (await other.load_recovery("run")).version == 2
        finally:
            await other.close()
            await memory.close()

    asyncio.run(scenario())


def test_state_committed_during_proposal_source_validation_is_rejected(tmp_path):
    async def scenario():
        memory, other = memory_at(tmp_path), memory_at(tmp_path)
        await memory.initialize()
        await other.initialize()
        try:
            _, plan = await prepare(memory)
            source_recovery = memory._compression.source_recovery
            original = source_recovery.validate

            async def advancing_validate(*args):
                valid = await original(*args)
                state = await other.load_recovery("run")
                await other.save_recovery(replace(state, version=2), expected_version=1)
                return valid

            source_recovery.validate = advancing_validate
            proposal = await memory.propose_compression(plan)
            assert not proposal.accepted and proposal.reason == "recovery_changed"
            assert proposal.summary_id is None and proposal.replacement is None
        finally:
            await other.close()
            await memory.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["input_digest", "replacement", "source_event_ids"])
def test_summary_replaced_during_final_validation_is_rejected(tmp_path, change):
    async def scenario():
        memory, other = memory_at(tmp_path), memory_at(tmp_path)
        await memory.initialize()
        await other.initialize()
        try:
            source_id, plan = await prepare(memory)
            proposal = await memory.propose_compression(plan)

            async def replace_summary():
                store = other._recovery.store
                row = await store.read(SCOPE, "summary", proposal.summary_id)
                payload, refs = dict(row.payload), row.source_event_ids
                if change == "source_event_ids":
                    refs = (*refs, "unexpected-source")
                else:
                    payload[change] = "changed"
                await store.write(SCOPE, "summary", proposal.summary_id, payload, refs,
                                  expected_revision=row.revision)

            during_recovery(memory, "validate", replace_summary)
            assert await memory.recover_compressed_source(proposal.summary_id, source_id) is None
        finally:
            await other.close()
            await memory.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["content", "erase"])
def test_source_change_after_restore_does_not_deliver_copied_text(tmp_path, change):
    async def scenario():
        memory, other = memory_at(tmp_path), memory_at(tmp_path)
        await memory.initialize()
        await other.initialize()
        try:
            source_id, plan = await prepare(memory)
            proposal = await memory.propose_compression(plan)

            async def mutate_source():
                if change == "erase":
                    await other.forget_sources((source_id,))
                else:
                    with closing(sqlite3.connect(tmp_path / "memory.db")) as db, db:
                        db.execute("UPDATE events SET content=? WHERE id=?", ("Changed", source_id))

            during_recovery(memory, "restore", mutate_source)
            assert await memory.recover_compressed_source(proposal.summary_id, source_id) is None
        finally:
            await other.close()
            await memory.close()

    asyncio.run(scenario())


def test_strategy_changed_during_final_validation_is_rejected(tmp_path):
    from agent_memory.context.compression import ExtractiveContextCompressor, UTF8ByteCounter

    class Approve:
        async def authorize(self, *args):
            return True

    async def scenario():
        bindings = [
            StrategyBinding(
                CompressionStrategy("extractive", version, "extractive-v1",
                                    "builtin-extractive-v1", "utf8-bytes-v1"),
                ExtractiveContextCompressor(), None, UTF8ByteCounter(),
            ) for version in ("1", "2")
        ]
        registry = CompressionStrategyRegistry(tmp_path / "strategy.db", SCOPE,
                                               bindings=bindings, authorizer=Approve())
        memory = memory_at(tmp_path, strategy_registry=registry)
        await memory.initialize()
        try:
            await registry.switch("extractive", "1", expected_generation=0,
                                  approval=StrategyApproval("first", "host", "Baseline"))
            source_id, plan = await prepare(memory)
            proposal = await memory.propose_compression(plan)

            async def switch():
                await registry.switch("extractive", "2", expected_generation=1,
                                      approval=StrategyApproval("second", "host", "Changed"))

            during_recovery(memory, "validate", switch)
            assert await memory.recover_compressed_source(proposal.summary_id, source_id) is None
        finally:
            await memory.close()

    asyncio.run(scenario())
