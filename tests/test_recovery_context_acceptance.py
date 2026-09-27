import asyncio
from dataclasses import asdict, replace
from datetime import UTC, datetime
import json

import pytest

from agent_memory.domain import ForgetRequest, MemoryScope
from agent_memory.context_compression import CompressionPlan, ContextSegment
from agent_memory.recovery import RecoveryState, ToolExecutionState
from agent_memory.recovery_store import RecoveryConflict, SQLiteRecoveryStore
from agent_memory.unified_memory import PendingMemoryDeletion, UnifiedMemory


SCOPE = MemoryScope("recovery-test", session_id="session-1")
AT = datetime(2026, 9, 27, tzinfo=UTC)
CONTENT = "Compare both reports. Do not publish the result."


def memory_at(tmp_path, **kwargs):
    return UnifiedMemory.local(tmp_path / "memory.db", SCOPE,
        recovery_path=tmp_path / "recovery.db", **kwargs)


async def capture(memory, event_id="event-1", run_id="run-1"):
    return await memory.capture_with_receipt(event_id=event_id, run_id=run_id,
        role="user", content=CONTENT, occurred_at=AT)


async def prepare(memory):
    receipt = await capture(memory)
    state = RecoveryState("run-1", 1, "Compare both reports",
        (receipt.provider_event_id,), constraints=("Do not publish the result",),
        pending_items=("Read the second report",),
        tools=(ToolExecutionState("call-1", "read_report", "running",
                                 source_event_ids=(receipt.provider_event_id,)),))
    await memory.save_recovery(state)
    plan = CompressionPlan("run-1", 1,
        (ContextSegment(receipt.provider_event_id, CONTENT),))
    return receipt, state, plan


def test_receipt_restart_duplicate_and_conflicting_identity(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            first = await capture(memory)
            assert first.persisted and first.stage == "persisted"
            assert not first.extracted and not first.retrievable
            again = await capture(memory)
            assert again.provider_event_id == first.provider_event_id
            with pytest.raises(RecoveryConflict):
                await memory.capture_with_receipt(event_id="event-1", run_id="run-1",
                    role="user", content="Different payload", occurred_at=AT)
        finally:
            await memory.close()
        restarted = memory_at(tmp_path)
        await restarted.initialize()
        try:
            assert await restarted.capture_receipt("event-1") == first
        finally:
            await restarted.close()
    asyncio.run(scenario())


def test_extraction_and_readiness_are_independent(tmp_path):
    class Generator:
        async def generate_claims(self, event):
            return []

    class Probe:
        ready_now = False

        async def ready(self, scope, ids):
            assert scope == SCOPE and len(ids) == 1
            return self.ready_now

    async def scenario():
        probe = Probe()
        memory = memory_at(tmp_path, generator=Generator(), retrieval_probe=probe)
        await memory.initialize()
        try:
            receipt = await capture(memory)
            assert receipt.persisted and receipt.extracted
            assert receipt.stage == "extracted" and not receipt.retrievable
            probe.ready_now = True
            assert (await memory.capture_receipt("event-1")).stage == "retrievable"
            probe.ready_now = False
            assert not (await memory.capture_receipt("event-1")).retrievable
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_failed_extraction_does_not_acknowledge_persistence(tmp_path):
    class Generator:
        async def generate_claims(self, event):
            raise RuntimeError("injected extraction failure")

    async def scenario():
        memory = memory_at(tmp_path, generator=Generator())
        await memory.initialize()
        try:
            with pytest.raises(Exception):
                await capture(memory)
            receipt = await memory.capture_receipt("event-1")
            assert receipt.stage == "received"
            assert not receipt.persisted and not receipt.extracted
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_recovery_restart_and_version_conflict(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            _, state, _ = await prepare(memory)
            with pytest.raises(RecoveryConflict):
                await memory.save_recovery(state)
            updated = replace(state, version=2, pending_items=("Review differences",))
            await memory.save_recovery(updated, expected_version=1)
        finally:
            await memory.close()
        restarted = memory_at(tmp_path)
        await restarted.initialize()
        try:
            restored = await restarted.load_recovery("run-1")
            assert restored == updated
            assert restored.tools[0].needs_reconciliation
        finally:
            await restarted.close()
    asyncio.run(scenario())


def test_cross_run_and_scope_evidence_is_rejected(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            _, state, _ = await prepare(memory)
            with pytest.raises(ValueError):
                await memory.save_recovery(replace(state, run_id="other-run"))
            other = UnifiedMemory.local(tmp_path / "memory.db",
                MemoryScope("other-tenant", session_id="session-1"),
                recovery_path=tmp_path / "recovery.db")
            await other.initialize()
            try:
                assert await other.load_recovery("run-1") is None
                assert await other.capture_receipt("event-1") is None
                with pytest.raises(ValueError):
                    await other.save_recovery(state)
            finally:
                await other.close()
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_compression_keeps_exact_state_and_rejects_stale_version(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            _, state, plan = await prepare(memory)
            result = await memory.propose_compression(plan)
            assert result.accepted, result.reason
            payload = json.loads(result.replacement)
            assert payload["recovery"] == json.loads(json.dumps(asdict(state)))
            assert payload["summary"] == CONTENT
            assert result.input_digest == plan.digest
            assert result.budget_used <= plan.token_budget
            assert await memory.load_compression(result.summary_id) == result
            await memory.save_recovery(replace(state, version=2), expected_version=1)
            assert await memory.load_compression(result.summary_id) is None
            rejected = await memory.propose_compression(plan)
            assert not rejected.accepted and rejected.replacement is None
        finally:
            await memory.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["no-validator", "reject", "raise", "timeout", "overflow"])
def test_custom_compression_fails_closed(tmp_path, mode):
    class Compressor:
        async def compress(self, plan, *, summary_budget):
            if mode == "raise":
                raise RuntimeError("sensitive provider error")
            if mode == "timeout":
                await asyncio.sleep(1)
            return "x" * 10000 if mode == "overflow" else "A summary"

    class Validator:
        async def validate(self, plan, state, summary):
            return mode != "reject"

    async def scenario():
        memory = memory_at(tmp_path, compressor=Compressor(),
            compression_validator=None if mode == "no-validator" else Validator())
        await memory.initialize()
        try:
            _, _, plan = await prepare(memory)
            if mode == "timeout":
                memory._compression.timeout_seconds = 0.01
            result = await memory.propose_compression(plan)
            assert not result.accepted
            assert result.replacement is None and result.summary_id is None
            assert "sensitive" not in result.reason
            assert plan.segments[0].text == CONTENT
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_required_state_budget_rejection(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            _, _, plan = await prepare(memory)
            result = await memory.propose_compression(replace(plan, token_budget=1))
            assert not result.accepted
            assert result.reason == "required_state_exceeds_budget"
        finally:
            await memory.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("erase", [True, False])
def test_forget_invalidates_receipt_state_and_summary(tmp_path, erase):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            receipt, state, plan = await prepare(memory)
            summary = await memory.propose_compression(plan)
            assert summary.accepted
            await memory.forget_sources((receipt.provider_event_id,), erase=erase)
            assert await memory.capture_receipt("event-1") is None
            assert await memory.load_recovery("run-1") is None
            assert await memory.load_compression(summary.summary_id) is None
            with pytest.raises((ValueError, RecoveryConflict)):
                await memory.save_recovery(state)
            with pytest.raises(RecoveryConflict):
                await capture(memory)
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_interrupted_deletion_blocks_reads_until_resumed(tmp_path):
    class Target:
        fail = True

        async def forget_sources(self, request):
            if self.fail:
                raise RuntimeError("injected target failure")

    async def scenario():
        target = Target()
        memory = memory_at(tmp_path, targets={"later-target": target})
        await memory.initialize()
        try:
            receipt, _, _ = await prepare(memory)
            with pytest.raises(RuntimeError):
                await memory.forget_sources((receipt.provider_event_id,))
            with pytest.raises(PendingMemoryDeletion):
                await memory.load_recovery("run-1")
        finally:
            await memory.close()
        target.fail = False
        restarted = memory_at(tmp_path, targets={"later-target": target})
        await restarted.initialize()
        try:
            with pytest.raises(PendingMemoryDeletion):
                await restarted.capture_receipt("event-1")
            await restarted.resume_deletion()
            assert await restarted.load_recovery("run-1") is None
        finally:
            await restarted.close()
    asyncio.run(scenario())


def test_store_capacity_and_terminal_tombstone(tmp_path):
    async def scenario():
        store = SQLiteRecoveryStore(tmp_path / "bounded.db", max_records=1)
        await store.initialize()
        await store.write(SCOPE, "receipt", "one", {"ok": True}, ("source",), expected_revision=0)
        with pytest.raises(ValueError, match="capacity"):
            await store.write(SCOPE, "receipt", "two", {}, (), expected_revision=0)
        await store.forget_sources(ForgetRequest(SCOPE, ("source",)))
        assert await store.read(SCOPE, "receipt", "one") is None
        with pytest.raises(RecoveryConflict):
            await store.write(SCOPE, "receipt", "one", {}, (), expected_revision=2)
    asyncio.run(scenario())


def test_sensitive_and_oversized_state_rejected():
    with pytest.raises(ValueError):
        RecoveryState("run", 1, "password=abcdefghijklmnop", ("source",))
    with pytest.raises(ValueError):
        RecoveryState("run", 1, "x" * 4097, ("source",))
    with pytest.raises(ValueError):
        ToolExecutionState("call", "tool", "running")
