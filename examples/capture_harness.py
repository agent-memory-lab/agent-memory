"""Minimal custom-agent capture hook; the host supplies identity and evaluation.

Construct a MemoryProvider, an MCPRequestContext from authenticated identity,
and a SQLiteCaptureQueue with a CaptureSanitizer before calling capture_turn.
No framework dependency or background service is required.
"""

from datetime import datetime, timezone

from agent_memory.capture_queue import SQLiteCaptureQueue
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin
from agent_memory.mcp import MCPRequestContext
from agent_memory.ports import MemoryProvider
from agent_memory_sdk import EmbeddedMemoryClient, QueuedCaptureSink


async def capture_turn(
    provider: MemoryProvider,
    context: MCPRequestContext,
    queue: SQLiteCaptureQueue,
    *,
    run_id: str,
    tool_output: str,
) -> list[dict]:
    client = EmbeddedMemoryClient(provider, context, capture_sink=QueuedCaptureSink(queue))
    await client.initialize()
    now = datetime.now(timezone.utc)
    events = (
        LifecycleEvent(
            scope=context.scope,
            event_id=f"{run_id}:start",
            event_type=LifecycleEventType.TURN_STARTED,
            origin=LifecycleOrigin.HOST,
            occurred_at=now,
            run_id=run_id,
            content="Turn started.",
        ),
        LifecycleEvent(
            scope=context.scope,
            event_id=f"{run_id}:tool",
            event_type=LifecycleEventType.TOOL_COMPLETED,
            origin=LifecycleOrigin.TOOL,
            occurred_at=now,
            run_id=run_id,
            content="Tool completed.",
            payload={"result": tool_output},
        ),
    )
    receipts = [await client.try_capture(event.to_dict()) for event in events]
    return receipts


async def drain_capture_queue(
    provider: MemoryProvider,
    context: MCPRequestContext,
    queue: SQLiteCaptureQueue,
    *,
    max_events: int = 32,
) -> list[dict]:
    """Call from a separate worker after the Agent's turn, never in its hot path."""

    processed: list[dict] = []
    for _ in range(max_events):
        receipt = await queue.process_one(provider, scope=context.scope)
        if receipt is None:
            break
        processed.append(
            {
                "event_id": receipt.event_id,
                "status": receipt.status,
                "duplicate": receipt.duplicate,
            }
        )
    return processed
