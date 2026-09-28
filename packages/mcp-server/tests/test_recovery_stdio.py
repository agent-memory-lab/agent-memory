import asyncio
from pathlib import Path
import sys

import pytest

from agent_memory.compression_feedback import CompressionFeedback
from agent_memory.context_compression import CompressionPlan, ContextSegment
from agent_memory.recovery import RecoveryState
from agent_memory_sdk import MCPMemoryClient, MemoryClientError


FIXTURE = str(Path(__file__).with_name("recovery_stdio_fixture.py"))
EVENT = dict(event_id="source", run_id="run", role="user", content="Compare both reports",
             occurred_at="2026-09-28T00:00:00+00:00")


def client_at(path, mode="normal"):
    return MCPMemoryClient.from_stdio(sys.executable, args=[FIXTURE, str(path), mode])


def test_stdio_recovery_feedback_and_cancellation(tmp_path):
    async def scenario():
        async with client_at(tmp_path) as client:
            receipt = (await client.capture_confirmed(**EVENT))["result"]
            source = receipt["provider_event_id"]
            state = RecoveryState("run", 1, "Compare both reports", (source,))
            await client.save_recovery(state)
            plan = CompressionPlan("run", 1, (ContextSegment(source, EVENT["content"]),))
            summary = (await client.propose_compression(plan))["result"]
            assert summary["accepted"]
            assert (await client.validate_compression(summary["summary_id"], plan))["result"]["accepted"]
            feedback = CompressionFeedback("feedback-1", summary["summary_id"], "host-eval-v1",
                "succeeded", 2000, 1000, "tokens", "host-counter-v1")
            first = await client.record_compression_feedback(feedback)
            assert first["result"]["saved_units"] == 1000
            assert await client.record_compression_feedback(feedback) == first
            assert await client.compression_feedback("feedback-1") == first
            await client.enqueue_capture(**{**EVENT, "event_id": "cancel-me"})
            cancelled = (await client.cancel_capture("cancel-me"))["result"]
            assert cancelled["cancelled"] and not cancelled["source_may_be_persisted"]
            assert (await client.capture_queue_status("cancel-me"))["result"]["status"] == "cancelled"
            assert (await client.process_next_capture())["result"] is None
            with pytest.raises(MemoryClientError):
                await client.enqueue_capture(**{**EVENT, "event_id": "cancel-me"})
            with pytest.raises(MemoryClientError):
                await client.recovery_call("load", {"run_id": "run", "scope": {"tenant_id": "foreign"}})
            await client.forget_sources((source,))
            assert (await client.compression_feedback("feedback-1"))["result"] is None
        # A fresh process cannot silently resurrect the cancelled identity.
        async with client_at(tmp_path) as client:
            with pytest.raises(MemoryClientError):
                await client.capture_confirmed(**{**EVENT, "event_id": "cancel-me"})
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["denied", "foreign"])
def test_stdio_denies_unauthorized_identity(tmp_path, mode):
    async def scenario():
        async with client_at(tmp_path, mode) as client:
            with pytest.raises(MemoryClientError) as caught:
                await client.recovery_stats()
            assert caught.value.code == "forbidden"
    asyncio.run(scenario())


def test_stdio_erase_permission(tmp_path):
    async def scenario():
        async with client_at(tmp_path, "no-erase") as client:
            with pytest.raises(MemoryClientError) as caught:
                await client.forget_sources(all_in_scope=True)
            assert caught.value.code == "forbidden"
    asyncio.run(scenario())


def test_stdio_error_does_not_leak_diagnostics(tmp_path):
    async def scenario():
        async with client_at(tmp_path, "error") as client:
            with pytest.raises(MemoryClientError) as caught:
                await client.recovery_stats()
            assert caught.value.code == "recovery_operation_failed"
            assert "private-provider" not in str(caught.value)
    asyncio.run(scenario())


def test_stdio_client_timeout_does_not_break_session(tmp_path):
    async def scenario():
        async with client_at(tmp_path, "slow") as client:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(client.recovery_stats(), timeout=0.02)
            assert (await client.list_recovery_runs())["result"]["items"] == []
    asyncio.run(scenario())
