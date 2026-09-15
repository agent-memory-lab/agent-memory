import asyncio
from datetime import datetime, timezone

import pytest

from agent_memory.capture_api import submit_capture
from agent_memory.capture_policy import CaptureSanitizer
from agent_memory.capture_queue import SQLiteCaptureQueue
from agent_memory.capture_sink import CaptureError, CaptureSubmission, QueuedCaptureSink
from agent_memory.composition import build_local_kernel
from agent_memory.domain import MemoryScope
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory_sdk import EmbeddedMemoryClient, LangGraphCaptureAdapter, MemoryClientError


def _event(scope, event_id="capture-1"):
    return LifecycleEvent(
        scope=scope,
        event_id=event_id,
        event_type=LifecycleEventType.TOOL_COMPLETED,
        origin=LifecycleOrigin.TOOL,
        occurred_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        run_id="run-1",
        content="Tool completed.",
        payload={"api_key": "private-secret-12345678", "result": "result"},
    )


def test_embedded_capture_is_optional_scoped_and_durable(tmp_path):
    async def scenario():
        provider = build_local_kernel(tmp_path / "memory.db")
        await provider.initialize()
        scope = MemoryScope(tenant_id="tenant", user_id="user-a", session_id="session")
        context = MCPRequestContext(scope=scope, actor="trusted-host")
        queue = SQLiteCaptureQueue(tmp_path / "capture.db", sanitizer=CaptureSanitizer())
        disabled = EmbeddedMemoryClient(provider, context)
        assert await disabled.capture(_event(scope).to_dict()) == {"status": "disabled"}
        client = EmbeddedMemoryClient(provider, context, capture_sink=QueuedCaptureSink(queue))
        payload = _event(scope).to_dict()
        payload["actor"] = "untrusted"
        first = await client.capture(payload)
        assert first["status"] == "pending" and not first["duplicate"]
        assert (await client.capture(payload))["duplicate"]
        lease = await queue.claim(scope=scope)
        assert lease.event.actor == "trusted-host"
        assert lease.event.payload["api_key"] == "[REDACTED]"
        assert queue.status(MemoryScope(tenant_id="other"), "capture-1") is None
        result = await provider.ingest_event(lease.event.to_memory_event())
        await queue.ack(lease, result)
        assert queue.status(scope, "capture-1").status == "done"
        malicious = {**payload, "scope": {"tenant_id": "other"}}
        with pytest.raises(MemoryClientError) as invalid:
            await client.capture(malicious)
        assert invalid.value.code == "capture_invalid_event"
        assert (await client.try_capture(malicious))["status"] == "skipped"

    asyncio.run(scenario())


def test_custom_sink_receipt_contract_and_no_feedback_spoofing():
    async def scenario():
        scope = MemoryScope(tenant_id="tenant")
        seen = []

        class CustomSink:
            async def submit(self, event):
                seen.append(event)
                return CaptureSubmission(event_id=event.event_id, status="pending")

        payload = _event(scope).to_dict()
        receipt = await submit_capture(payload, sink=CustomSink(), scope=scope, actor="host")
        assert receipt.status == "pending" and seen[0].scope == scope
        assert seen[0].actor == "host"
        payload["event_type"] = "reward.received"
        payload["origin"] = "host"
        with pytest.raises(ValueError):
            await submit_capture(payload, sink=CustomSink(), scope=scope, actor="host")

        class BadSink:
            async def submit(self, event):
                return CaptureSubmission(event_id="another-event", status="done")

        with pytest.raises(CaptureError) as contract:
            await submit_capture(_event(scope).to_dict(), sink=BadSink(), scope=scope, actor="host")
        assert contract.value.code == "capture_sink_contract"

    asyncio.run(scenario())


def test_langgraph_adapter_ignores_replayed_history_and_yields_original_updates():
    async def scenario():
        observed = []

        class Capture:
            async def try_capture(self, event):
                observed.append(event)
                return {"event_id": event["event_id"], "status": "pending"}

        model = {"type": "ai", "id": "m-1", "content": "answer"}
        parts = [
            {"type": "updates", "ns": (), "data": {"model": {"messages": [model]}}},
            {"type": "updates", "ns": (), "data": {"replay": {"messages": [model]}}},
            {"type": "updates", "ns": (), "data": {"history": {"messages": [
                {"type": "human", "content": "old input"}, model,
            ]}}},
            {"type": "updates", "ns": (), "data": {"tools": {"messages": [
                {"type": "tool", "id": "t-1", "content": "tool output"},
            ]}}},
        ]

        class Graph:
            async def astream(self, inputs, **options):
                assert options["version"] == "v2" and options["stream_mode"] == "updates"
                for part in parts:
                    yield part

        adapter = LangGraphCaptureAdapter(Capture(), run_id="stable-run", started_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
        returned = [part async for part in adapter.astream_updates(Graph(), {})]
        assert returned == parts
        assert [event["event_type"] for event in observed] == [
            "turn.started", "message.received", "tool.completed", "turn.completed",
        ]
        assert len({event["event_id"] for event in observed}) == 4

    asyncio.run(scenario())
