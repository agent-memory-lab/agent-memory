"""Optional LangGraph stream-v2 capture bridge with no LangGraph import.

Only completed messages are mapped; graph state is not treated as outcome,
evaluation, reward, or permission-bearing evidence. The authenticated SDK
client provides Trusted Scope. Persist run_id and started_at when replaying
a stream so event IDs and content hashes remain stable.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

from agent_memory.lifecycle import LIFECYCLE_SCHEMA_VERSION

from .client import CaptureClient


def _role(message: Any) -> str | None:
    if isinstance(message, Mapping):
        value = message.get("type", message.get("role"))
    else:
        value = getattr(message, "type", None)
    return {
        "human": "user",
        "user": "user",
        "ai": "model",
        "assistant": "model",
        "tool": "tool",
    }.get(value) if isinstance(value, str) else None


def _content(message: Any) -> str:
    raw = message.get("content") if isinstance(message, Mapping) else getattr(message, "content", None)
    if isinstance(raw, str):
        return raw
    if isinstance(raw, (list, tuple)) and len(raw) <= 32:
        blocks = [part.get("text") for part in raw if isinstance(part, Mapping)]
        if all(isinstance(text, str) for text in blocks) and len(blocks) == len(raw):
            return "\n".join(blocks)
    return ""


def _message_id(message: Any) -> str | None:
    if isinstance(message, Mapping):
        value = message.get("id") or message.get("tool_call_id")
    else:
        value = getattr(message, "id", None) or getattr(message, "tool_call_id", None)
    return value if isinstance(value, str) and value.strip() else None


class LangGraphCaptureAdapter:
    """Observe v2 update chunks while leaving the graph output unchanged."""

    def __init__(
        self,
        capture: CaptureClient,
        *,
        run_id: str,
        started_at: datetime | None = None,
    ) -> None:
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id must be a stable, nonempty host identifier")
        when = started_at or datetime.now(timezone.utc)
        if not isinstance(when, datetime) or when.utcoffset() is None:
            raise ValueError("started_at must include a timezone")
        self._capture = capture
        self._run_id = run_id
        self._occurred_at = when.astimezone(timezone.utc).isoformat()
        self._sequence = 0
        self._seen: set[str] = set()

    def _event(
        self,
        suffix: str,
        event_type: str,
        origin: str,
        content: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "event_id": f"{self._run_id}:{suffix}",
            "event_type": event_type,
            "origin": origin,
            "occurred_at": self._occurred_at,
            "run_id": self._run_id,
            "content": content,
            "payload": dict(payload or {}),
        }

    async def _safe_capture(self, event: Mapping[str, Any]) -> dict[str, Any]:
        try:
            return await self._capture.try_capture(event)
        except Exception:
            return {"status": "skipped", "reason": "capture_adapter_failed"}

    async def observe(self, part: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
        """Map message updates only; never infer feedback from a graph state."""

        if not isinstance(part, Mapping) or part.get("type") != "updates":
            return ()
        updates = part.get("data")
        if not isinstance(updates, Mapping):
            return ()
        namespace = part.get("ns", ())
        if not isinstance(namespace, (tuple, list)) or any(not isinstance(name, str) for name in namespace):
            namespace = ()
        receipts: list[dict[str, Any]] = []
        for node, update in updates.items():
            if not isinstance(node, str) or not isinstance(update, Mapping):
                continue
            messages = update.get("messages")
            if not isinstance(messages, (list, tuple)) or not messages:
                continue
            try:
                message = messages[-1]
                role = _role(message)
                text = _content(message)
                message_id = _message_id(message)
            except Exception:
                continue
            if role is None or (role != "tool" and not text):
                continue
            if message_id is None and (len(messages) > 1 or role == "user"):
                continue  # Ambiguous history without an identity is not new evidence.
            key = sha256(
                f"{role}\x00{message_id or 'unidentified'}\x00{text}".encode("utf-8")
            ).hexdigest()
            if key in self._seen:
                continue
            if len(self._seen) >= 2_048:
                receipts.append({"status": "skipped", "reason": "capture_history_limit"})
                continue
            self._seen.add(key)
            self._sequence += 1
            payload = {"node": node, "namespace": list(namespace)}
            if role == "tool":
                payload["result"] = text
                event_type, content = "tool.completed", "Tool completed."
            else:
                event_type, content = "message.received", text
            receipt = await self._safe_capture(
                self._event(
                    f"update:{self._sequence}", event_type, role, content, payload
                )
            )
            receipts.append(receipt)
        return tuple(receipts)

    async def astream_updates(
        self,
        graph: Any,
        inputs: Any,
        *,
        config: Any = None,
        subgraphs: bool = False,
    ) -> AsyncIterator[Mapping[str, Any]]:
        """Yield the official v2 stream unchanged; capture is best-effort."""

        await self._safe_capture(self._event("start", "turn.started", "host", "Turn started."))
        options: dict[str, Any] = {
            "stream_mode": "updates",
            "version": "v2",
            "subgraphs": subgraphs,
        }
        if config is not None:
            options["config"] = config
        async for part in graph.astream(inputs, **options):
            await self.observe(part)
            yield part
        await self._safe_capture(self._event("done", "turn.completed", "host", "Turn completed."))
