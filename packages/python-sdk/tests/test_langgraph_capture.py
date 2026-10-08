import asyncio
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from agent_memory_sdk.langgraph import LangGraphCaptureAdapter

from agent_memory.capture.api import submit_capture
from agent_memory.capture.policy import CaptureSanitizer
from agent_memory.capture.queue import SQLiteCaptureQueue
from agent_memory.capture.sink import QueuedCaptureSink
from agent_memory.domain import MemoryScope


class RecordingCapture:
    def __init__(self):
        self.events = []

    async def try_capture(self, event):
        self.events.append(event)
        return {"event_id": event["event_id"], "status": "pending"}


def _adapter(capture):
    return LangGraphCaptureAdapter(
        capture,
        run_id="run",
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _update(messages, node="tools"):
    return {"type": "updates", "ns": ["subgraph"], "data": {node: {"messages": messages}}}


def _tool(message_id, content):
    return {"type": "tool", "id": message_id, "content": content}


def test_capture_all_identified_messages_in_batched_tool_update():
    async def scenario():
        capture = RecordingCapture()
        adapter = _adapter(capture)
        messages = [
            _tool("tool-A", "Result A"),
            {"type": "tool", "tool_call_id": "call-B", "content": "Result B"},
            SimpleNamespace(type="tool", id="tool-C", content=[{"text": "Result C"}]),
        ]
        receipts = await adapter.observe(_update(messages))
        assert len(receipts) == 3
        assert [event["payload"]["result"] for event in capture.events] == [
            "Result A",
            "Result B",
            "Result C",
        ]
        assert [event["event_id"] for event in capture.events] == [
            "run:update:1",
            "run:update:2",
            "run:update:3",
        ]
        assert all(event["payload"]["namespace"] == ["subgraph"] for event in capture.events)
        assert await adapter.observe(_update(messages)) == ()

    asyncio.run(scenario())


def test_history_replay_rollback_and_appends_capture_only_unseen_messages():
    async def scenario():
        capture = RecordingCapture()
        adapter = _adapter(capture)
        user = {"role": "user", "id": "input", "content": "Question"}
        model = {"role": "assistant", "id": "model", "content": "Answer"}
        first = _tool("first", "Repeated output")
        second = _tool("second", "Repeated output")
        assert len(await adapter.observe(_update([user, model, first, first]))) == 3
        assert await adapter.observe(_update([user], node="rollback")) == ()
        assert (
            len(await adapter.observe(_update([user, model, first, second], node="history"))) == 1
        )
        assert await adapter.observe(_update([second, first, user, model])) == ()
        assert [event["origin"] for event in capture.events] == ["user", "model", "tool", "tool"]
        assert len({event["event_id"] for event in capture.events}) == 4

    asyncio.run(scenario())


def test_unidentified_or_malformed_messages_do_not_hide_identified_neighbors():
    class BrokenMessage:
        @property
        def type(self):
            raise ValueError("malformed message")

    async def scenario():
        capture = RecordingCapture()
        adapter = _adapter(capture)
        messages = [
            {"type": "human", "content": "ambiguous history"},
            _tool("first", "First"),
            BrokenMessage(),
            {"type": "system", "id": "system", "content": "not evidence"},
            {"type": "ai", "id": "empty", "content": ""},
            _tool("second", "Second"),
            {"type": "ai", "content": "ambiguous history tail"},
        ]
        assert len(await adapter.observe(_update(messages))) == 2
        assert [event["payload"]["result"] for event in capture.events] == ["First", "Second"]
        assert (
            len(await adapter.observe(_update([{"type": "ai", "content": "single update"}]))) == 1
        )
        assert await adapter.observe(_update([{"type": "human", "content": "no identity"}])) == ()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["raise", "skipped", "disabled", "cancelled"])
def test_capture_failure_retries_same_event_without_replaying_successes(failure):
    async def scenario():
        class IntermittentCapture(RecordingCapture):
            async def try_capture(self, event):
                self.events.append(event)
                if event["payload"].get("result") == "Second" and len(self.events) == 2:
                    if failure == "raise":
                        raise TimeoutError("admission may have succeeded")
                    if failure == "cancelled":
                        raise asyncio.CancelledError()
                    return {"status": failure, "reason": "temporarily unavailable"}
                return {"event_id": event["event_id"], "status": "pending"}

        capture = IntermittentCapture()
        adapter = _adapter(capture)
        messages = [_tool("first", "First"), _tool("second", "Second"), _tool("third", "Third")]
        if failure == "cancelled":
            with pytest.raises(asyncio.CancelledError):
                await adapter.observe(_update(messages))
        else:
            receipts = await adapter.observe(_update(messages))
            assert len(receipts) == 3
            assert receipts[1]["status"] in {"skipped", "disabled"}
        receipts = await adapter.observe(_update(messages, node="replayed-history"))
        assert len(receipts) == (2 if failure == "cancelled" else 1)
        firsts = [event for event in capture.events if event["payload"].get("result") == "First"]
        seconds = [event for event in capture.events if event["payload"].get("result") == "Second"]
        thirds = [event for event in capture.events if event["payload"].get("result") == "Third"]
        assert len(firsts) == len(thirds) == 1
        assert len(seconds) == 2 and seconds[0] == seconds[1]
        assert seconds[0]["event_id"] == "run:update:2"
        assert thirds[0]["event_id"] == "run:update:3"
        assert await adapter.observe(_update(messages)) == ()

    asyncio.run(scenario())


def test_changed_message_content_preserves_revision_deduplication():
    async def scenario():
        capture = RecordingCapture()
        adapter = _adapter(capture)
        original = _tool("same-id", "Original result")
        revised = _tool("same-id", "Revised result")
        assert len(await adapter.observe(_update([original]))) == 1
        assert len(await adapter.observe(_update([revised]))) == 1
        assert await adapter.observe(_update([original, revised])) == ()

    asyncio.run(scenario())


def test_retry_after_uncertain_admission_is_idempotent_in_durable_queue(tmp_path):
    async def scenario():
        scope = MemoryScope("tenant", session_id="session")
        queue = SQLiteCaptureQueue(tmp_path / "capture.db", sanitizer=CaptureSanitizer())
        sink = QueuedCaptureSink(queue)

        class UncertainCapture:
            attempts = 0

            async def try_capture(self, event):
                receipt = await submit_capture(event, sink=sink, scope=scope, actor="host")
                self.attempts += 1
                if self.attempts == 1:
                    raise TimeoutError("receipt lost after durable admission")
                return asdict(receipt)

        adapter = _adapter(UncertainCapture())
        message = _tool("tool", "Saved result")
        assert (await adapter.observe(_update([message])))[0]["status"] == "skipped"
        retry = await adapter.observe(_update([message], node="replayed-history"))
        assert len(retry) == 1 and retry[0]["duplicate"]
        assert retry[0]["event_id"] == "run:update:1"
        assert await adapter.observe(_update([message])) == ()
        lease = await queue.claim(scope=scope)
        assert lease.event.payload["node"] == "tools"
        assert tuple(lease.event.payload["namespace"]) == ("subgraph",)
        assert lease.event.payload["result"] == "Saved result"
        assert await queue.claim(scope=scope) is None

    asyncio.run(scenario())


def test_history_limit_stays_bounded_and_still_allows_failed_admission_retry():
    async def scenario():
        class InitiallyFailingCapture(RecordingCapture):
            async def try_capture(self, event):
                self.events.append(event)
                if len(self.events) == 1:
                    return {"status": "skipped", "reason": "queue_full"}
                return {"event_id": event["event_id"], "status": "done"}

        capture = InitiallyFailingCapture()
        adapter = _adapter(capture)
        messages = [_tool(f"tool-{number}", f"Result {number}") for number in range(2049)]
        receipts = await adapter.observe(_update(messages))
        assert len(capture.events) == 2048
        assert receipts[-1] == {"status": "skipped", "reason": "capture_history_limit"}
        retry = await adapter.observe(_update(messages[:1]))
        assert retry == ({"event_id": "run:update:1", "status": "done"},)
        assert capture.events[0] == capture.events[-1]
        assert await adapter.observe(_update(messages[:2048])) == ()

    asyncio.run(scenario())
