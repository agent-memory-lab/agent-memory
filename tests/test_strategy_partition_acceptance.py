import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from agent_memory.compression_strategy import (
    CompressionStrategy, CompressionStrategyRegistry, StrategyApproval, StrategyBinding,
)
from agent_memory.context_compression import (
    CompressionPlan, ContextSegment, ExtractiveContextCompressor, UTF8ByteCounter,
)
from agent_memory.model_token_counter import ModelTokenCounter
from agent_memory.recovery import RecoveryState
from agent_memory.recovery_partitions import PartitionedRecoveryStore
from agent_memory.recovery_store import RecoveryConflict
from agent_memory.unified_memory import UnifiedMemory
from test_recovery_context_acceptance import AT, CONTENT, SCOPE, prepare


class Approve:
    async def authorize(self, *args):
        return True


class Deny:
    async def authorize(self, *args):
        return False


def registry_at(path, authorizer=None):
    bindings = [StrategyBinding(
        CompressionStrategy("extractive", version, "extractive-v1", "builtin-extractive-v1", "utf8-bytes-v1"),
        ExtractiveContextCompressor(), None, UTF8ByteCounter()) for version in ("1", "2")]
    return CompressionStrategyRegistry(path, SCOPE, bindings=bindings, authorizer=authorizer or Approve())


def test_token_count_includes_rendering_and_reservations():
    counter = ModelTokenCounter(lambda text: list(text), model_id="fixture-model",
        tokenizer_version="characters-v1", template_version="brackets-v1",
        render=lambda text: "[" + text + "]", framing_tokens=3, reserve_tokens=5)
    measured = counter.measure("abc")
    assert measured["prompt_tokens"] == 5
    assert counter.count("abc") == 13
    changed = ModelTokenCounter(lambda text: list(text), model_id="fixture-model",
        tokenizer_version="characters-v2", template_version="brackets-v1")
    assert changed.counter_id != counter.counter_id
    with pytest.raises(ValueError):
        counter.count("x" * 1048577)


def test_strategy_switch_rollback_and_stale_proposal(tmp_path):
    async def scenario():
        registry = registry_at(tmp_path / "strategies.db")
        memory = UnifiedMemory.local(tmp_path / "memory.db", SCOPE,
            recovery_path=tmp_path / "recovery.db", strategy_registry=registry)
        await memory.initialize()
        try:
            _, _, plan = await prepare(memory)
            assert not (await memory.propose_compression(plan)).accepted
            await registry.switch("extractive", "1", expected_generation=0,
                approval=StrategyApproval("a1", "host", "Baseline"))
            first = await memory.propose_compression(plan)
            assert first.accepted
            assert (await memory.load_compression(first.summary_id)).accepted
            with pytest.raises(RecoveryConflict):
                await registry.switch("extractive", "2", expected_generation=0,
                    approval=StrategyApproval("stale", "host", "Stale request"))
            await registry.switch("extractive", "2", expected_generation=1,
                approval=StrategyApproval("a2", "host", "Evaluate new strategy"))
            assert await memory.load_compression(first.summary_id) is None
            await registry.switch("extractive", "1", expected_generation=2, rollback=True,
                approval=StrategyApproval("a3", "host", "Rollback"))
            assert await memory.load_compression(first.summary_id) is None
            assert (await memory.propose_compression(plan)).accepted
        finally:
            await memory.close()
        reopened = registry_at(tmp_path / "strategies.db")
        await reopened.initialize()
        snapshot, _ = await reopened.resolve()
        assert snapshot["version"] == "1" and snapshot["generation"] == 3
    asyncio.run(scenario())


def test_strategy_denial_and_offline_comparison(tmp_path):
    async def scenario():
        registry = registry_at(tmp_path / "strategies.db", Deny())
        await registry.initialize()
        with pytest.raises(PermissionError):
            await registry.switch("extractive", "1", expected_generation=0,
                approval=StrategyApproval("a1", "host", "Not approved"))
        protocol = dict(dataset_digest="heldout-v1", model_id="model-v1", evaluator_id="eval-v1", sample_count=20)
        await registry.record_evaluation("base", "extractive", "1", **protocol, metrics={"quality": 0.8})
        await registry.record_evaluation("new", "extractive", "2", **protocol, metrics={"quality": 0.9})
        comparison = await registry.compare("base", "new")
        assert comparison["candidate_minus_baseline"]["quality"] == pytest.approx(0.1)
        assert not comparison["automatic_promotion"]
        await registry.record_evaluation("other", "extractive", "2",
            **{**protocol, "dataset_digest": "different"}, metrics={"quality": 0.99})
        with pytest.raises(ValueError):
            await registry.compare("base", "other")
    asyncio.run(scenario())


def test_model_counter_budget_and_identity(tmp_path):
    def counter(version, reserve=0):
        return ModelTokenCounter(lambda text: list(text), model_id="fixture-model",
            tokenizer_version=version, template_version="plain-v1", reserve_tokens=reserve)

    async def scenario():
        memory = UnifiedMemory.local(tmp_path / "memory.db", SCOPE,
            recovery_path=tmp_path / "recovery.db", token_counter=counter("v1"))
        await memory.initialize()
        try:
            _, _, plan = await prepare(memory)
            result = await memory.propose_compression(plan)
            assert result.accepted
            assert (await memory.load_compression(result.summary_id)).accepted
        finally:
            await memory.close()
        changed = UnifiedMemory.local(tmp_path / "memory.db", SCOPE,
            recovery_path=tmp_path / "recovery.db", token_counter=counter("v2", 5000))
        await changed.initialize()
        try:
            assert await changed.load_compression(result.summary_id) is None
            rejected = await changed.propose_compression(plan)
            assert not rejected.accepted and rejected.reason == "required_state_exceeds_budget"
        finally:
            await changed.close()
    asyncio.run(scenario())


def test_partition_rotation_retirement_and_old_identity_rejection(tmp_path):
    async def scenario():
        store = PartitionedRecoveryStore(tmp_path / "partitions", SCOPE, authorizer=Approve())
        memory = UnifiedMemory.local(tmp_path / "memory.db", SCOPE, recovery_store=store)
        await memory.initialize()
        try:
            prefix = (await memory.recovery_partition_info())["required_prefix"]
            event = dict(event_id=prefix + "event", run_id=prefix + "run", role="user", content=CONTENT, occurred_at=AT)
            receipt = await memory.capture_with_receipt(**event)
            await memory.save_recovery(RecoveryState(event["run_id"], 1, "Compare reports", (receipt.provider_event_id,)))
            with pytest.raises(RecoveryConflict):
                await memory.rotate_recovery_partition(expected_generation=1, approval="host")
            await memory.complete_recovery(event["run_id"], expected_version=1)
            next_partition = await memory.rotate_recovery_partition(expected_generation=1, approval="host")
            assert next_partition["generation"] == 2
            with pytest.raises(RecoveryConflict):
                await memory.capture_with_receipt(**event)
            with pytest.raises(RecoveryConflict):
                await memory.retire_recovery_partition(2, approval="host")
            await memory.retire_recovery_partition(1, approval="host")
            assert not (tmp_path / "partitions" / "recovery-0000000000000001.db").exists()
        finally:
            await memory.close()
        reopened = PartitionedRecoveryStore(tmp_path / "partitions", SCOPE, authorizer=Approve())
        await reopened.initialize()
        info = await reopened.partition_info()
        assert info["generation"] == 2 and info["retired_through"] == 1
        with pytest.raises(RecoveryConflict):
            await reopened.ensure_run(SCOPE, prefix + "run")
    asyncio.run(scenario())


def test_forget_reaches_closed_retained_partition(tmp_path):
    async def scenario():
        store = PartitionedRecoveryStore(tmp_path / "partitions", SCOPE, authorizer=Approve())
        memory = UnifiedMemory.local(tmp_path / "memory.db", SCOPE, recovery_store=store)
        await memory.initialize()
        try:
            prefix = (await memory.recovery_partition_info())["required_prefix"]
            receipt = await memory.capture_with_receipt(event_id=prefix + "event", run_id=prefix + "run",
                role="user", content=CONTENT, occurred_at=AT)
            await memory.set_recovery_expiry(prefix + "run", expires_at=datetime.now(UTC) - timedelta(seconds=1))
            await memory.rotate_recovery_partition(expected_generation=1, approval="host")
            await memory.forget_sources((receipt.provider_event_id,))
            assert (await store._store(1).stats(SCOPE))["retained_payloads"] == 0
        finally:
            await memory.close()
    asyncio.run(scenario())
