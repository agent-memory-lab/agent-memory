from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, Self

from agent_memory.capture.api import submit_capture
from agent_memory.capture.sink import CaptureError, CaptureSink
from agent_memory.lifecycle import LifecycleEventError
from agent_memory.mcp import MCPMemoryTools, MCPRequestContext, MCPToolError, decode_mcp_error
from agent_memory.operations.deletion_audit import DeletionAuditService
from agent_memory.operations.retention import RetentionError
from agent_memory.ports import MemoryProvider
from agent_memory.serialization import to_jsonable

from .recovery import RecoveryClientOperations

if TYPE_CHECKING:
    from mcp import Client


class MemoryClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "memory_client_error",
        field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.field = field

    def to_dict(self) -> dict[str, str]:
        payload = {"code": self.code, "message": str(self)}
        if self.field is not None:
            payload["field"] = self.field
        return payload


def _capture_deadline(value: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 < value <= 30
    ):
        raise MemoryClientError(
            "capture_timeout_seconds must be between 0 and 30",
            code="capture_configuration",
            field="capture_timeout_seconds",
        )
    return float(value)


class MemoryClient(Protocol):
    async def ontology_status(self) -> dict[str, Any]: ...
    async def ontology_search(self, text: str, *, limit: int = 8) -> dict[str, Any]: ...
    async def ontology_assertions(self, ids: list[str]) -> dict[str, Any]: ...
    async def ontology_graph(self, start_entity: str, **options: Any) -> dict[str, Any]: ...
    async def ontology_switch(self, version: str, *, expected_generation: int,
        reason: str, action: str = "activate") -> dict[str, Any]: ...
    async def ingest(self, event_type: str, content: str, **options: Any) -> dict[str, Any]: ...
    async def retrieve(self, text: str, **options: Any) -> dict[str, Any]: ...
    async def get_state(self) -> dict[str, Any]: ...
    async def propose(self, **proposal: Any) -> dict[str, Any]: ...
    async def forget(self, **request: Any) -> dict[str, Any]: ...
    async def read_block(self, block_id: str) -> dict[str, Any]: ...
    async def write_block(self, **block: Any) -> dict[str, Any]: ...
    async def search_blocks(self, text: str, **options: Any) -> dict[str, Any]: ...
    async def forget_block(self, block_id: str, **options: Any) -> dict[str, Any]: ...
    async def capabilities(self) -> dict[str, Any]: ...
    async def record_decision(self, action: str, **options: Any) -> dict[str, Any]: ...
    async def record_outcome(
        self, decision_id: str, outcome: str, success: bool, **options: Any
    ) -> dict[str, Any]: ...
    async def record_evaluation(self, outcome_id: str, **evaluation: Any) -> dict[str, Any]: ...
    async def record_reward(
        self, outcome_id: str, value: float, formula_version: str, **options: Any
    ) -> dict[str, Any]: ...
    async def feedback_status(self, record_id: str, record_type: str) -> dict[str, Any]: ...
    async def deletion_audit(self, *, limit: int = 100) -> dict[str, Any]: ...


class CaptureClient(Protocol):
    async def capture(self, event: Mapping[str, Any]) -> dict[str, Any]: ...
    async def try_capture(self, event: Mapping[str, Any]) -> dict[str, Any]: ...


class _Operations(RecoveryClientOperations):
    async def durable_append(self, event, session, sequence):
        return await self._call("memory_durable", {"operation": "append", "payload": {
            "event": to_jsonable(event), "session": to_jsonable(session), "sequence": sequence}})

    async def durable_revise(self, event, session, sequence, *, base_event_id, expected_revision):
        return await self._call(
            "memory_durable",
            {
                "operation": "revise",
                "payload": {
                    "event": to_jsonable(event),
                    "session": to_jsonable(session),
                    "sequence": sequence,
                    "base_event_id": base_event_id,
                    "expected_revision": expected_revision,
                },
            },
        )

    async def durable_status(self, session, sequence):
        return await self._call("memory_durable", {"operation": "status", "payload": {
            "session": to_jsonable(session), "sequence": sequence}})

    async def durable_purge_sync(self, session, *, after=0, limit=128):
        return await self._call("memory_durable", {"operation": "purge_sync", "payload": {
            "session": to_jsonable(session), "after": after, "limit": limit}})

    async def durable_purge_ack(self, session, *, through):
        return await self._call("memory_durable", {"operation": "purge_ack", "payload": {
            "session": to_jsonable(session), "through": through}})

    async def durable_contracts(self, session):
        return await self._call("memory_durable", {"operation": "contracts", "payload": {
            "session": to_jsonable(session)}})

    async def durable_cancel_sequence(self, session, sequence, source_event_id):
        return await self._call("memory_durable", {"operation": "cancel_sequence", "payload": {
            "session": to_jsonable(session), "sequence": sequence, "source_event_id": source_event_id}})

    async def durable_freeze_target(self, session, sequences):
        return await self._call("memory_durable", {"operation": "freeze_target", "payload": {
            "session": to_jsonable(session), "sequences": list(sequences)}})

    async def durable_freeze_reprocessing_target(self, session, request_ids):
        """Freeze existing host-owned reprocessing requests; this does not submit work."""
        return await self._call("memory_durable", {
            "operation": "freeze_reprocessing_target",
            "payload": {"session": to_jsonable(session), "request_ids": list(request_ids)},
        })

    async def durable_readiness(self, session, target_id, *, stage="l1_decided"):
        return await self._call("memory_durable", {"operation": "readiness", "payload": {
            "session": to_jsonable(session), "target_id": target_id, "stage": stage}})

    async def durable_wait_until(self, session, target_id, *, stage="l1_decided", timeout=30, poll_interval=0.1):
        from .durable_readiness import wait_until

        return await wait_until(self, session, target_id, stage=stage, timeout=timeout, poll_interval=poll_interval)

    async def durable_cursor(self, session):
        return await self._call("memory_durable", {"operation": "cursor", "payload": {
            "session": to_jsonable(session)}})

    async def ontology_status(self):
        return await self._call("memory_ontology_status", {})

    async def ontology_search(self, text: str, *, limit=8):
        return await self._call("memory_ontology_search", dict(text=text, limit=limit))

    async def ontology_assertions(self, ids):
        return await self._call("memory_ontology_assertions", dict(ids=list(ids)))

    async def ontology_graph(self, start_entity: str, **options):
        return await self._call("memory_ontology_graph", dict(start_entity=start_entity, **options))

    async def ontology_switch(self, version: str, *, expected_generation: int, reason: str, action="activate"):
        return await self._call("memory_ontology_switch", dict(version=version,
            expected_generation=expected_generation, reason=reason, action=action))

    async def _call(self, name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    async def ingest(
        self,
        event_type: str,
        content: str,
        *,
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        source_uri: str | None = None,
    ) -> dict[str, Any]:
        return await self._call(
            "memory_ingest",
            {
                "event_type": event_type,
                "content": content,
                "metadata": dict(metadata or {}),
                "idempotency_key": idempotency_key,
                "source_uri": source_uri,
            },
        )

    async def retrieve(
        self,
        text: str,
        *,
        limit: int = 8,
        token_budget: int = 1200,
        channels: list[str] | None = None,
        valid_at: str | None = None,
        known_at: str | None = None,
    ) -> dict[str, Any]:
        arguments: dict[str, Any] = {"text": text, "limit": limit, "token_budget": token_budget}
        if channels is not None:
            arguments["channels"] = channels
        for name, value in (("valid_at", valid_at), ("known_at", known_at)):
            if value is not None:
                arguments[name] = value
        return await self._call("memory_retrieve", arguments)

    async def get_state(self) -> dict[str, Any]:
        return await self._call("memory_get_state", {})

    async def record_decision(self, action: str, **options: Any) -> dict[str, Any]:
        return await self._call("memory_record_decision", {"action": action, **options})

    async def record_outcome(
        self,
        decision_id: str,
        outcome: str,
        success: bool,
        **options: Any,
    ) -> dict[str, Any]:
        return await self._call(
            "memory_record_outcome",
            {
                "decision_id": decision_id,
                "outcome": outcome,
                "success": success,
                **options,
            },
        )

    async def record_evaluation(
        self, outcome_id: str, **evaluation: Any
    ) -> dict[str, Any]:
        return await self._call(
            "memory_record_evaluation", {"outcome_id": outcome_id, **evaluation}
        )

    async def record_reward(
        self,
        outcome_id: str,
        value: float,
        formula_version: str,
        **options: Any,
    ) -> dict[str, Any]:
        return await self._call(
            "memory_record_reward",
            {
                "outcome_id": outcome_id,
                "value": value,
                "formula_version": formula_version,
                **options,
            },
        )

    async def feedback_status(self, record_id: str, record_type: str) -> dict[str, Any]:
        return await self._call(
            "memory_feedback_status",
            {"record_id": record_id, "record_type": record_type},
        )

    async def propose(
        self,
        *,
        key: str,
        value: Any,
        text: str,
        source_event_ids: list[str],
        expected_version: int,
        scope_level: str = "session",
        confidence: float = 1.0,
        importance: float = 0.5,
    ) -> dict[str, Any]:
        return await self._call(
            "memory_propose",
            {
                "key": key,
                "value": value,
                "text": text,
                "source_event_ids": source_event_ids,
                "expected_version": expected_version,
                "scope_level": scope_level,
                "confidence": confidence,
                "importance": importance,
            },
        )

    async def forget(
        self,
        *,
        memory_ids: list[str] | None = None,
        all_in_scope: bool = False,
        mode: str = "archive",
    ) -> dict[str, Any]:
        return await self._call(
            "memory_forget",
            {
                "memory_ids": memory_ids or [],
                "all_in_scope": all_in_scope,
                "mode": mode,
            },
        )

    async def read_block(self, block_id: str) -> dict[str, Any]:
        return await self._call("memory_block_read", {"block_id": block_id})

    async def write_block(
        self,
        *,
        title: str,
        content: str,
        event_ids: list[str],
        block_id: str | None = None,
        channel: str = "semantic",
        scope_level: str = "session",
        token_budget: int = 256,
        expected_version: int = 0,
        status: str = "active",
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "title": title,
            "content": content,
            "event_ids": event_ids,
            "channel": channel,
            "scope_level": scope_level,
            "token_budget": token_budget,
            "expected_version": expected_version,
            "status": status,
            "metadata": dict(metadata or {}),
        }
        if block_id is not None:
            arguments["block_id"] = block_id
        return await self._call("memory_block_write", arguments)

    async def search_blocks(
        self,
        text: str,
        *,
        limit: int = 8,
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        arguments: dict[str, Any] = {"text": text, "limit": limit}
        if channels is not None:
            arguments["channels"] = channels
        return await self._call("memory_block_search", arguments)

    async def forget_block(self, block_id: str, *, mode: str = "archive") -> dict[str, Any]:
        return await self._call(
            "memory_block_forget",
            {"block_id": block_id, "mode": mode},
        )

    async def capabilities(self) -> dict[str, Any]:
        return await self._call("memory_capabilities", {})

    async def derived_capabilities(self) -> dict[str, Any]:
        return await self._call("memory_derived", {"operation": "capabilities", "payload": {}})

    async def question_capabilities(self) -> dict[str, Any]:
        return await self._call("memory_question", {"operation": "capabilities", "payload": {}})

    async def question_read(self, question_id: str, *, valid_at=None, known_at=None):
        payload = {"question_id": question_id}
        if valid_at is not None or known_at is not None:
            payload.update(valid_at=valid_at, known_at=known_at)
        return await self._call("memory_question", {"operation": "read", "payload": payload})

    async def question_route(self, query: str, *, parameters=None):
        payload = {"query": query}
        if parameters is not None:
            payload["parameters"] = parameters
        return await self._call("memory_question", {"operation": "route", "payload": payload})

    async def question_model_answer(self, question_id: str):
        """Explicit opt-in buffered explanation; never enabled by ordinary answer()."""
        return await self._call("memory_question", {
            "operation": "model_answer", "payload": {"question_id": question_id},
        })

    async def question_answer(self, query: str, *, dedupe_key: str, parameters=None, max_steps=1):
        payload = {"query": query, "dedupe_key": dedupe_key, "max_steps": max_steps}
        if parameters is not None:
            payload["parameters"] = parameters
        return await self._call("memory_question", {"operation": "answer", "payload": payload})

    async def question_request(self, question_id: str, *, dedupe_key: str):
        return await self._call("memory_question", {"operation": "request", "payload": {
            "question_id": question_id, "dedupe_key": dedupe_key,
        }})

    async def question_status(self, target_id: str):
        return await self._call("memory_question", {"operation": "status", "payload": {
            "target_id": target_id,
        }})

    async def question_page_read(self, page_id: str, *, valid_at=None, known_at=None):
        payload = {"page_id": page_id}
        if valid_at is not None or known_at is not None:
            payload.update(valid_at=valid_at, known_at=known_at)
        return await self._call("memory_question", {"operation": "page_read", "payload": payload})

    async def page_capabilities(self) -> dict[str, Any]:
        return await self._call("memory_derived", {"operation": "page_capabilities", "payload": {}})

    async def page_read(self, page_id: str, *, purpose: str = "agent_context") -> dict[str, Any]:
        return await self._call("memory_derived", {"operation": "page_read", "payload": {
            "page_id": page_id, "purpose": purpose,
        }})

    async def page_context(self, page_id: str, *, purpose: str = "agent_context") -> dict[str, Any]:
        return await self._call("memory_derived", {"operation": "page_context", "payload": {
            "page_id": page_id, "purpose": purpose,
        }})

    async def page_status(
        self, target_id: str, *, purpose: str = "agent_context"
    ) -> dict[str, Any]:
        return await self._call("memory_derived", {"operation": "page_status", "payload": {
            "target_id": target_id, "purpose": purpose,
        }})

    async def derived_read(
        self, facet_id: str, *, purpose: str = "agent_context", known_at=None, valid_at=None
    ) -> dict[str, Any]:
        return await self._call(
            "memory_derived",
            {
                "operation": "read",
                "payload": {
                    "facet_id": facet_id,
                    "purpose": purpose,
                    "known_at": known_at,
                    "valid_at": valid_at,
                },
            },
        )

    async def derived_status(self, target_id: str) -> dict[str, Any]:
        return await self._call(
            "memory_derived", {"operation": "status", "payload": {"target_id": target_id}}
        )

    async def derived_context(
        self, facet_id: str, *, purpose: str = "agent_context", known_at=None, valid_at=None
    ) -> dict[str, Any]:
        payload = {"facet_id": facet_id, "purpose": purpose}
        if known_at is not None or valid_at is not None:
            payload.update(known_at=known_at, valid_at=valid_at)
        return await self._call(
            "memory_derived",
            {"operation": "derived_context", "payload": payload},
        )

    async def derived_history_points(
        self, facet_id: str, *, purpose: str = "agent_context"
    ) -> dict[str, Any]:
        return await self._call(
            "memory_derived",
            {
                "operation": "history_points",
                "payload": {"facet_id": facet_id, "purpose": purpose},
            },
        )

    async def deletion_audit(self, *, limit: int = 100) -> dict[str, Any]:
        return await self._call("memory_deletion_audit", {"limit": limit})


class EmbeddedMemoryClient(_Operations):
    """Run the exact MCP contract in-process without a protocol hop."""

    def __init__(
        self,
        provider: MemoryProvider,
        context: MCPRequestContext,
        *,
        capture_sink: CaptureSink | None = None,
        capture_timeout_seconds: float = 0.25,
        deletion_auditor: DeletionAuditService | None = None,
        ontology=None,
        recovery_tools=None,
        durable_capture=None,
        derived=None,
        questions=None,
    ) -> None:
        if recovery_tools is not None and (
            capture_sink is not None
            or durable_capture is not None
            or derived is not None
            or questions is not None
            or recovery_tools.memory.provider is not provider
        ):
            raise ValueError("recovery requires its own provider and capture gate")
        self._durable_capture = durable_capture
        self._recovery_tools = recovery_tools
        self._provider = provider
        self._tools = MCPMemoryTools(
            provider, deletion_auditor=deletion_auditor, ontology=ontology,
            derived=derived, questions=questions,
        )
        self._context = context
        self._capture_sink = capture_sink
        self._capture_timeout_seconds = _capture_deadline(capture_timeout_seconds)

    async def initialize(self) -> None:
        if self._recovery_tools is not None:
            await self._recovery_tools.memory.initialize()
        else:
            await self._provider.initialize()

    async def _call(self, name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        try:
            if name == "memory_durable":
                if self._durable_capture is None:
                    raise MCPToolError("durable capture is not enabled", code="durable_disabled")
                try:
                    return await self._durable_capture.call(arguments["operation"],
                        arguments["payload"], self._context)
                except RetentionError as error:
                    raise MCPToolError(error.code, code=error.code) from None
            if name == "memory_recovery":
                if self._recovery_tools is None:
                    raise MCPToolError("recovery is not enabled", code="recovery_disabled")
                return await self._recovery_tools.call(arguments["operation"],
                    arguments.get("payload", {}), self._context)
            if self._recovery_tools is not None:
                return await self._recovery_tools.legacy(self._tools, name, arguments, self._context)
            return await self._tools.call_tool(name, arguments, self._context)
        except MCPToolError as error:
            raise MemoryClientError(
                str(error),
                code=error.code,
                field=error.field,
            ) from error

    async def capture(self, event: Mapping[str, Any]) -> dict[str, Any]:
        """Append a host-scoped lifecycle event; disabled unless explicitly configured.

        Queue receipts confirm durable admission, not completion of provider ingest.
        Direct capture does not offer queue-backed crash recovery.
        """

        if self._capture_sink is None:
            return {"status": "disabled"}
        try:
            return to_jsonable(
                await asyncio.wait_for(
                    submit_capture(
                        event,
                        sink=self._capture_sink,
                        scope=self._context.scope,
                        actor=self._context.actor,
                    ),
                    timeout=self._capture_timeout_seconds,
                )
            )
        except TimeoutError as error:
            raise MemoryClientError("capture deadline exceeded", code="capture_timeout") from error
        except LifecycleEventError as error:
            raise MemoryClientError(
                str(error), code="capture_invalid_event", field=error.field
            ) from error
        except CaptureError as error:
            raise MemoryClientError(str(error), code=error.code, field=error.field) from error
        except Exception as error:
            raise MemoryClientError(
                "capture storage failed", code="capture_storage_failed"
            ) from error

    async def try_capture(self, event: Mapping[str, Any]) -> dict[str, Any]:
        """Fail open on async errors and deadlines; sinks must bound synchronous I/O."""

        try:
            return await self.capture(event)
        except MemoryClientError as error:
            return {"status": "skipped", "reason": error.code}


class MCPMemoryClient(_Operations):
    """High-level client backed by the official MCP v2 Client."""

    def __init__(self, source: Any, *, capture_timeout_seconds: float = 0.25) -> None:
        try:
            from mcp import Client
        except ImportError as error:
            raise MemoryClientError(
                "MCPMemoryClient requires the optional 'mcp' dependency; "
                "install agent-memory-sdk[mcp]"
            ) from error
        self._client: Client = Client(source)
        self._capture_timeout_seconds = _capture_deadline(capture_timeout_seconds)

    @classmethod
    def from_http(
        cls,
        url: str,
        *,
        http_client: Any | None = None,
        capture_timeout_seconds: float = 0.25,
    ) -> MCPMemoryClient:
        try:
            from mcp.client.streamable_http import streamable_http_client
        except ImportError as error:
            raise MemoryClientError(
                "MCPMemoryClient requires the optional 'mcp' dependency; "
                "install agent-memory-sdk[mcp]"
            ) from error
        if http_client is None:
            return cls(url, capture_timeout_seconds=capture_timeout_seconds)
        return cls(
            streamable_http_client(url, http_client=http_client),
            capture_timeout_seconds=capture_timeout_seconds,
        )

    @classmethod
    def from_stdio(
        cls,
        command: str,
        *,
        args: list[str] | None = None,
        env: Mapping[str, str] | None = None,
        capture_timeout_seconds: float = 0.25,
    ) -> MCPMemoryClient:
        try:
            from mcp import StdioServerParameters
        except ImportError as error:
            raise MemoryClientError(
                "MCPMemoryClient requires the optional 'mcp' dependency; "
                "install agent-memory-sdk[mcp]"
            ) from error
        return cls(
            StdioServerParameters(
                command=command,
                args=args or [],
                env=dict(env) if env is not None else None,
            ),
            capture_timeout_seconds=capture_timeout_seconds,
        )

    async def __aenter__(self) -> Self:
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._client.__aexit__(exc_type, exc, traceback)

    async def _call(self, name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        result = await self._client.call_tool(name, dict(arguments))
        if result.is_error:
            messages = [
                str(getattr(block, "text", ""))
                for block in result.content
                if getattr(block, "text", None)
            ]
            for message in messages:
                payload = decode_mcp_error(message)
                if payload is not None:
                    raise MemoryClientError(
                        payload["message"],
                        code=payload["code"],
                        field=payload.get("field"),
                    )
            raise MemoryClientError("; ".join(messages) or f"{name} failed")
        if not isinstance(result.structured_content, dict):
            raise MemoryClientError(f"{name} returned no structured content")
        return result.structured_content

    async def capture(self, event: Mapping[str, Any]) -> dict[str, Any]:
        """Invoke an MCP Capture tool only when the server explicitly enables it."""

        if not isinstance(event, Mapping):
            raise MemoryClientError("capture event must be an object", code="capture_invalid_event")
        try:
            return await asyncio.wait_for(
                self._call("memory_capture", {"event": dict(event)}),
                timeout=self._capture_timeout_seconds,
            )
        except TimeoutError as error:
            raise MemoryClientError("capture deadline exceeded", code="capture_timeout") from error

    async def try_capture(self, event: Mapping[str, Any]) -> dict[str, Any]:
        try:
            return await self.capture(event)
        except MemoryClientError as error:
            return {"status": "skipped", "reason": error.code}
