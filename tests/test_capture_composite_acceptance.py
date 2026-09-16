"""T28: capture admission, feedback, failure, replay and disabled-path gates."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from time import time as walltime

import pytest

from agent_memory.capture_policy import CaptureSanitizer
from agent_memory.capture_queue import CaptureQueueError, SQLiteCaptureQueue
from agent_memory.capture_sink import QueuedCaptureSink
from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryScope
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory_sdk import EmbeddedMemoryClient


_WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _event(
    scope: MemoryScope,
    event_id: str,
    kind: LifecycleEventType = LifecycleEventType.TOOL_COMPLETED,
    origin: LifecycleOrigin = LifecycleOrigin.TOOL,
) -> LifecycleEvent:
    return LifecycleEvent(
        scope=scope,
        event_id=event_id,
        event_type=kind,
        origin=origin,
        occurred_at=_WHEN,
        run_id="composite-run",
        content=f"{kind.value} completed.",
        payload={"result": "safe output", "password": "secret-value-12345678"},
    )


def test_complete_turn_reaches_host_corrected_feedback(tmp_path):
    async def scenario():
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        scope = MemoryScope("tenant", user_id="user", session_id="turn")
        queue = SQLiteCaptureQueue(tmp_path / "capture.db", sanitizer=CaptureSanitizer())
        client = EmbeddedMemoryClient(
            provider, MCPRequestContext(scope, actor="trusted-host"),
            capture_sink=QueuedCaptureSink(queue),
        )
        events = (
            _event(scope, "started", LifecycleEventType.TURN_STARTED, LifecycleOrigin.HOST),
            _event(scope, "input", LifecycleEventType.MESSAGE_RECEIVED, LifecycleOrigin.USER),
            _event(scope, "tool"),
            _event(scope, "finished", LifecycleEventType.TURN_COMPLETED, LifecycleOrigin.HOST),
        )
        for event in events:
            assert (await client.capture(event.to_dict()))["status"] == "pending"
        processed = []
        while (receipt := await queue.process_one(provider, scope=scope)) is not None:
            processed.append(receipt.event_id)
        assert processed == [event.event_id for event in events]
        assert all(queue.status(scope, event.event_id).status == "done" for event in events)

        decision = await client.record_decision(
            "answer", memory_usage="none", idempotency_key="decision-composite"
        )
        first = await client.record_outcome(
            decision["record_id"], "incorrect", False, idempotency_key="outcome-composite"
        )
        corrected = await client.record_outcome(
            decision["record_id"], "accepted", True, corrects_id=first["record_id"]
        )
        evaluation = await client.record_evaluation(
            corrected["record_id"],
            evaluator_id="host", evaluator_version="1",
            rubric_id="quality", rubric_version="1",
            metrics={"quality": 1.0}, evidence_digest="sha256:composite",
        )
        reward = await client.record_reward(
            corrected["record_id"], 1.0, "formula-1",
            evaluation_id=evaluation["record_id"], reward_definition_id="task-success",
        )
        receipt = await client.feedback_status(reward["record_id"], "reward")
        assert receipt["receipt"]["status"] == "accepted"
        assert queue.status(scope, "tool").status == "done"

    asyncio.run(scenario())


def test_failed_capture_does_not_change_manual_ingest_or_feedback(tmp_path):
    async def scenario():
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        scope = MemoryScope("tenant", session_id="manual")
        context = MCPRequestContext(scope)

        class BrokenSink:
            async def submit(self, event):
                raise RuntimeError("sink unavailable")

        disabled = EmbeddedMemoryClient(provider, context)
        broken = EmbeddedMemoryClient(provider, context, capture_sink=BrokenSink())
        payload = _event(scope, "ignored").to_dict()
        assert (await disabled.capture(payload))["status"] == "disabled"
        assert (await broken.try_capture(payload))["status"] == "skipped"
        manual = await disabled.ingest(
            "user.message", "Remember the manual path.", idempotency_key="manual-ingest"
        )
        assert manual["event_id"]
        decision = await disabled.record_decision("answer", memory_usage="none")
        outcome = await disabled.record_outcome(decision["record_id"], "accepted", True)
        assert (await disabled.feedback_status(outcome["record_id"], "outcome"))[
            "receipt"
        ]["status"] == "accepted"

    asyncio.run(scenario())


def test_capture_timeout_is_isolated_from_agent_turn(tmp_path):
    async def scenario():
        provider = build_local_kernel(tmp_path / "memory.db")
        scope = MemoryScope("tenant", session_id="timeout")

        class HangingSink:
            async def submit(self, event):
                await asyncio.Event().wait()

        client = EmbeddedMemoryClient(
            provider, MCPRequestContext(scope), capture_sink=HangingSink(),
            capture_timeout_seconds=0.02,
        )
        result = await asyncio.wait_for(
            client.try_capture(_event(scope, "timeout").to_dict()), timeout=0.05
        )
        assert result == {"status": "skipped", "reason": "capture_timeout"}

    asyncio.run(scenario())


def test_concurrent_duplicates_out_of_order_restart_and_postcommit_failure(tmp_path, monkeypatch):
    from agent_memory import capture_queue as queue_module

    async def scenario():
        clock = [walltime()]
        monkeypatch.setattr(queue_module.time, "time", lambda: clock[0])
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        scope = MemoryScope("tenant", user_id="user", session_id="replay")
        path = tmp_path / "capture.db"
        sanitizer = CaptureSanitizer()
        first_queue = SQLiteCaptureQueue(path, sanitizer=sanitizer)
        latest = _event(scope, "latest")
        earlier = replace(_event(scope, "earlier"), occurred_at=_WHEN - timedelta(days=1))
        duplicate_receipts = await asyncio.gather(
            *(first_queue.enqueue(latest) for _ in range(12))
        )
        assert {receipt.queue_id for receipt in duplicate_receipts} == {
            duplicate_receipts[0].queue_id
        }
        assert sum(not receipt.duplicate for receipt in duplicate_receipts) == 1
        await first_queue.enqueue(earlier)
        with pytest.raises(CaptureQueueError) as conflict:
            await first_queue.enqueue(replace(latest, content="conflicting correction"))
        assert conflict.value.code == "event_conflict"

        class CommitThenRaise:
            async def ingest_event(self, event):
                await provider.ingest_event(event)
                raise RuntimeError("worker interrupted after commit")

        with pytest.raises(RuntimeError, match="after commit"):
            await first_queue.process_one(CommitThenRaise(), scope=scope)
        assert first_queue.status(scope, "latest").status == "pending"
        clock[0] += 3
        restarted_queue = SQLiteCaptureQueue(path, sanitizer=CaptureSanitizer())
        replay = await restarted_queue.process_one(provider, scope=scope)
        assert replay.event_id == "latest" and replay.duplicate
        old = await restarted_queue.process_one(provider, scope=scope)
        assert old.event_id == "earlier" and old.status == "done"
        assert restarted_queue.status(scope, "latest").status == "done"
        assert restarted_queue.status(scope, "earlier").status == "done"
        assert await restarted_queue.process_one(provider, scope=scope) is None

    asyncio.run(scenario())
