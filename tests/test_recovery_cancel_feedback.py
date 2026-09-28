import asyncio
from dataclasses import replace

import pytest

from agent_memory.compression_feedback import CompressionFeedback
from agent_memory.recovery_store import RecoveryConflict
from test_recovery_context_acceptance import AT, CONTENT, memory_at, prepare


def test_cancel_scrubs_job_and_never_rolls_back_completed_capture(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            event = dict(event_id="queued", run_id="run", role="user", content=CONTENT, occurred_at=AT)
            await memory.enqueue_capture(**event)
            result = await memory.cancel_capture("queued")
            assert result["cancelled"] and result["source_rollback"] is False
            assert await memory.cancel_capture("queued") == result
            job = await memory._recovery.store.read(memory.scope, "job", "queued")
            assert "event" not in job.payload and CONTENT not in str(job.payload)
            assert (await memory.capture_receipt("queued")).queue_status == "cancelled"
            assert await memory.process_next_capture() is None
            with pytest.raises(RecoveryConflict):
                await memory.retry_capture("queued")
            await memory.enqueue_capture(**{**event, "event_id": "done"})
            receipt = await memory.process_next_capture()
            assert not (await memory.cancel_capture("done"))["cancelled"]
            assert (await memory.capture_receipt("done")).provider_event_id == receipt.provider_event_id
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_feedback_identity_and_source_invalidation(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            receipt, _, plan = await prepare(memory)
            summary = await memory.propose_compression(plan)
            feedback = CompressionFeedback("f1", summary.summary_id, "host-v1", "failed",
                100, 150, "utf8_bytes", "utf8-v1", "Required detail omitted")
            result = await memory.record_compression_feedback(feedback)
            assert result["saved_units"] == -50
            assert result["measurement_source"] == "host_reported"
            assert await memory.record_compression_feedback(feedback) == result
            with pytest.raises(RecoveryConflict):
                await memory.record_compression_feedback(replace(feedback, after_units=99))
            await memory.forget_sources((receipt.provider_event_id,), erase=False)
            assert await memory.compression_feedback("f1") is None
        finally:
            await memory.close()
    asyncio.run(scenario())
