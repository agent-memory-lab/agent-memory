"""Optional proposal-only compression; the host owns context replacement."""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from hashlib import sha256
import json
import math
from typing import Protocol
from uuid import uuid4

from .recovery import RecoveryState, _text
from .store import _refs


@dataclass(frozen=True, slots=True)
class ContextSegment:
    source_event_id: str
    text: str

    def __post_init__(self):
        _refs((self.source_event_id,))
        _text(self.text, limit=16384)


@dataclass(frozen=True, slots=True)
class CompressionPlan:
    run_id: str
    recovery_version: int
    segments: tuple[ContextSegment, ...]
    token_budget: int = 4096

    def __post_init__(self):
        _text(self.run_id, limit=256)
        if type(self.recovery_version) is not int or self.recovery_version < 1:
            raise ValueError("invalid recovery version")
        if not isinstance(self.segments, tuple) or not 1 <= len(self.segments) <= 128:
            raise ValueError("provide one to 128 immutable context segments")
        if any(not isinstance(s, ContextSegment) for s in self.segments):
            raise ValueError("invalid context segment")
        _refs(tuple(s.source_event_id for s in self.segments))
        if sum(len(s.text.encode("utf-8")) for s in self.segments) > 262144:
            raise ValueError("compression input exceeds byte budget")
        if type(self.token_budget) is not int or not 1 <= self.token_budget <= 32768:
            raise ValueError("invalid compression budget")

    @property
    def digest(self):
        return sha256(_json(asdict(self)).encode("utf-8")).hexdigest()


class ContextCompressor(Protocol):
    async def compress(self, plan: CompressionPlan, *, summary_budget: int) -> str: ...


class CompressionValidator(Protocol):
    """Host assessment of summary fidelity, not a model's self-certification."""
    async def validate(self, plan: CompressionPlan, state: RecoveryState, summary: str) -> bool: ...


class TokenCounter(Protocol):
    def count(self, text: str) -> int: ...


class UTF8ByteCounter:
    """Dependency-free byte budget, NOT an exact tokenizer for arbitrary models.

    Inject the target model's counter for a model-token guarantee. Framing,
    system instructions and other host context are outside this local budget.
    """
    counter_id = "utf8-bytes-v1"

    def count(self, text):
        return len(text.encode("utf-8"))


class ExtractiveContextCompressor:
    """Keeps complete original segments, newest first in the selection pass.

    No model calls. Omission remains lossy; exact recovery state is attached
    separately by the coordinator. Text order in the result stays chronological.
    """
    compressor_id = "extractive-v1"

    async def compress(self, plan, *, summary_budget):
        selected, used = [], 0
        for segment in reversed(plan.segments):
            size = len(segment.text.encode("utf-8")) + (2 if selected else 0)
            if used + size <= summary_budget:
                selected.append(segment.text)
                used += size
        return "\n\n".join(reversed(selected))


@dataclass(frozen=True, slots=True)
class CompressionResult:
    accepted: bool
    reason: str
    summary_id: str | None = None
    replacement: str | None = None
    input_digest: str | None = None
    budget_used: int = 0


def _json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True,
                      allow_nan=False, separators=(",", ":"))


class CompressionCoordinator:
    def __init__(self, recovery, *, compressor=None, validator=None, counter=None, timeout_seconds=30,
                 strategy_registry=None):
        if not isinstance(timeout_seconds, (int, float)) or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 120:
            raise ValueError("compression timeout must be between zero and 120 seconds")
        self.recovery = recovery
        self.compressor = compressor if compressor is not None else ExtractiveContextCompressor()
        self.validator, self.counter = validator, counter or UTF8ByteCounter()
        self.timeout_seconds = timeout_seconds
        self.strategy_registry = strategy_registry
        if strategy_registry is not None and strategy_registry.scope != recovery.scope:
            raise ValueError("compression strategy registry scope mismatch")

    @staticmethod
    def _counter_id(counter):
        return getattr(counter, "counter_id", type(counter).__module__ + "." + type(counter).__qualname__)

    def _count(self, text):
        count = self.counter.count(text)
        if type(count) is not int or count < 0:
            raise ValueError("token counter returned an invalid count")
        return count

    async def propose(self, plan):
        if not isinstance(plan, CompressionPlan):
            raise TypeError("compression requires CompressionPlan")
        try:
            async with asyncio.timeout(self.timeout_seconds):
                if self.strategy_registry is not None:
                    snapshot, binding = await self.strategy_registry.resolve()
                    worker = CompressionCoordinator(self.recovery, compressor=binding.compressor,
                        validator=binding.validator, counter=binding.counter, timeout_seconds=self.timeout_seconds)
                    return await worker._propose(plan, strategy_snapshot=snapshot,
                                                  registry=self.strategy_registry)
                return await self._propose(plan)
        except TimeoutError:
            return CompressionResult(False, "compression_timeout", input_digest=plan.digest)
        except Exception:
            # Do not persist or echo model errors, prompts or sensitive values.
            # Cancellation (BaseException) still propagates to the host.
            return CompressionResult(False, "compression_or_validation_failed", input_digest=plan.digest)

    async def _propose(self, plan, *, strategy_snapshot=None, registry=None):
        state = await self.recovery.load(plan.run_id)
        if state is None or state.version != plan.recovery_version:
            return CompressionResult(False, "recovery_version_unavailable", input_digest=plan.digest)
        refs = tuple(dict.fromkeys((*state.source_event_ids,
                                   *(s.source_event_id for s in plan.segments))))
        _refs(refs)
        await self.recovery.require_sources(plan.run_id, refs)
        # The compressor cannot alter this required state or the evidence set.
        base = {"format": "agent-memory-context-v1", "run_id": plan.run_id,
                "recovery": asdict(state), "source_event_ids": refs, "summary": ""}
        available = plan.token_budget - self._count(_json(base))
        if available <= 0:
            return CompressionResult(False, "required_state_exceeds_budget", input_digest=plan.digest)
        if type(self.compressor) is not ExtractiveContextCompressor and self.validator is None:
            return CompressionResult(False, "custom_compressor_requires_validator", input_digest=plan.digest)
        summary = await self.compressor.compress(plan, summary_budget=available)
        _text(summary, limit=65536)
        if self.validator is not None and await self.validator.validate(plan, state, summary) is not True:
            return CompressionResult(False, "host_validation_rejected", input_digest=plan.digest)
        base["summary"] = summary
        replacement = _json(base)
        used = self._count(replacement)
        if used > plan.token_budget:
            return CompressionResult(False, "compressed_context_exceeds_budget", input_digest=plan.digest)
        # Reject stale state/evidence after slow or externally hosted generation.
        if await self.recovery.load(plan.run_id) != state:
            return CompressionResult(False, "recovery_changed", input_digest=plan.digest)
        await self.recovery.require_sources(plan.run_id, refs)
        identity = str(uuid4())
        payload = {"run_id": plan.run_id, "recovery_version": state.version,
                   "input_digest": plan.digest, "replacement": replacement,
                   "budget_used": used, "token_budget": plan.token_budget,
                   "counter_id": self._counter_id(self.counter), "strategy": strategy_snapshot}
        if registry is not None and not await registry.is_current(strategy_snapshot):
            return CompressionResult(False, "compression_strategy_changed", input_digest=plan.digest)
        await self.recovery.store.write(self.recovery.scope, "summary", identity,
                                       payload, refs, expected_revision=0)
        return CompressionResult(True, "host_approval_required", identity, replacement, plan.digest, used)

    async def load(self, summary_id):
        row = await self.recovery.store.read(self.recovery.scope, "summary", summary_id)
        if row is None:
            return None
        payload = row.payload
        counter = self.counter
        if self.strategy_registry is not None:
            snapshot, binding = await self.strategy_registry.resolve()
            if payload.get("strategy") != snapshot:
                return None
            counter = binding.counter
        elif payload.get("strategy") is not None:
            return None
        if payload.get("counter_id", self._counter_id(counter)) != self._counter_id(counter):
            return None
        state = await self.recovery.load(payload["run_id"])
        if state is None or state.version != payload["recovery_version"]:
            return None
        await self.recovery.require_sources(payload["run_id"], row.source_event_ids)
        replacement = payload["replacement"]
        used = counter.count(replacement)
        if type(used) is not int or used < 0:
            raise ValueError("token counter returned an invalid count")
        if used > payload["token_budget"]:
            return None
        return CompressionResult(True, "host_approval_required", summary_id,
                                 replacement, payload["input_digest"], used)
