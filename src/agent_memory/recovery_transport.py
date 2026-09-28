"""Explicitly authorized, shared embedded/MCP recovery transport contract."""
from __future__ import annotations

from datetime import datetime

from .context_compression import CompressionPlan, ContextSegment
from .mcp import MCPToolError
from .recovery import CaptureReceipt, RecoveryState
from .serialization import to_jsonable


def _plan(data):
    data = dict(data)
    data["segments"] = tuple(ContextSegment(**s) for s in data["segments"])
    return CompressionPlan(**data)


def _event(data):
    data = dict(data)
    if isinstance(data.get("occurred_at"), str):
        data["occurred_at"] = datetime.fromisoformat(data["occurred_at"].replace("Z", "+00:00"))
    return data


class RecoveryTransport:
    """Host must inject an async authorizer.authorize(context, operation).

    A scope match alone does not grant recovery-write, worker, cleanup or
    compression authority. Never derive the authorizer from tool arguments.
    """
    OPERATIONS = frozenset(("capture", "receipt", "save", "load", "compress",
        "load_compression", "validate_compression", "enqueue", "process_one",
        "retry", "complete", "expire", "cleanup", "stats", "forget", "resume_deletion"))

    def __init__(self, memory, authorizer):
        memory._require_recovery()
        if not callable(getattr(authorizer, "authorize", None)):
            raise TypeError("recovery transport requires a host authorizer")
        self.memory, self.authorizer = memory, authorizer

    async def _authorize(self, context, operation):
        if context.scope != self.memory.scope:
            raise MCPToolError("recovery scope mismatch", code="forbidden")
        if await self.authorizer.authorize(context, operation) is not True:
            raise MCPToolError("recovery operation is not authorized", code="forbidden")

    async def legacy(self, tools, name, arguments, context):
        # Do not let a co-located legacy endpoint bypass the deletion gate.
        reads = {"memory_retrieve", "memory_get_state", "memory_capabilities",
                 "memory_block_read", "memory_block_search", "memory_feedback_status",
                 "memory_deletion_audit", "memory_ontology_status", "memory_ontology_search",
                 "memory_ontology_assertions", "memory_ontology_graph", "memory_doctor",
                 "memory_repair_plan"}
        await self._authorize(context, name)
        if name not in reads:
            raise MCPToolError("use the recovery-enabled capture/forget interface for writes",
                               code="recovery_gate_required")
        async with self.memory._lock:
            await self.memory._ready()
            result = await tools.call_tool(name, arguments, context)
            await self.memory._ready()
            return result

    async def call(self, operation, payload, context):
        if operation not in self.OPERATIONS or not isinstance(payload, dict):
            raise MCPToolError("invalid recovery operation or payload")
        try:
            await self._authorize(context, operation)
            p = dict(payload)
            memory = self.memory
            if operation == "capture":
                result = await memory.capture_with_receipt(**_event(p))
            elif operation == "enqueue":
                result = await memory.enqueue_capture(**_event(p))
            elif operation == "receipt":
                result = await memory.capture_receipt(**p)
            elif operation == "save":
                state = RecoveryState.from_dict(p.pop("state"))
                result = await memory.save_recovery(state, **p)
            elif operation == "load":
                result = await memory.load_recovery(**p)
            elif operation == "compress":
                result = await memory.propose_compression(_plan(p))
            elif operation == "load_compression":
                result = await memory.load_compression(**p)
            elif operation == "validate_compression":
                plan = _plan(p.pop("current_plan"))
                result = await memory.validate_compression(current_plan=plan, **p)
            elif operation == "process_one":
                result = await memory.process_next_capture(**p)
            elif operation == "retry":
                result = await memory.retry_capture(**p)
            elif operation == "complete":
                result = await memory.complete_recovery(**p)
            elif operation == "expire":
                p["expires_at"] = datetime.fromisoformat(p["expires_at"].replace("Z", "+00:00"))
                result = await memory.set_recovery_expiry(**p)
            elif operation == "cleanup":
                result = await memory.cleanup_recovery(**p)
            elif operation == "stats":
                result = await memory.recovery_stats(**p)
            elif operation == "forget":
                if type(p.get("erase", True)) is not bool or type(p.get("all_in_scope", False)) is not bool:
                    raise ValueError("deletion flags must be booleans")
                if p.get("erase", True) and not context.can_erase:
                    raise MCPToolError("erase is not authorized", code="forbidden")
                result = await memory.forget_sources(**p)
            else:
                # Resume can finish a previously authorized erasure.
                if not context.can_erase:
                    raise MCPToolError("deletion resumption requires erase authority", code="forbidden")
                result = await memory.resume_deletion(**p)
            value = to_jsonable(result)
            if isinstance(result, CaptureReceipt):
                value["stage"] = result.stage
            return {"result": value}
        except MCPToolError:
            raise
        except (ValueError, TypeError, KeyError):
            raise MCPToolError("invalid recovery input or unavailable state", code="recovery_invalid_request") from None
        except Exception:
            raise MCPToolError("recovery operation failed", code="recovery_operation_failed") from None
