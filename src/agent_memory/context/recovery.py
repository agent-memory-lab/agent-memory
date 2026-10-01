"""Host-owned recovery, conservative durable receipts, and live evidence checks."""
from __future__ import annotations

from contextvars import ContextVar
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Protocol

from ..capture.policy import DefaultCaptureRedactor
from ..capture.sink import CaptureSubmission
from ..domain import ForgetMode, ForgetRequest
from .store import RecoveryConflict, RecoveryStore, _refs


_extracted_event = ContextVar("recovery_extracted_event", default=None)


def _text(value, *, limit=4096, empty=False):
    if not isinstance(value, str) or (not empty and not value.strip()) or len(value.encode("utf-8")) > limit:
        raise ValueError("invalid or oversized recovery text")
    if DefaultCaptureRedactor().redact(value) != value:
        raise ValueError("recovery input requires host redaction before persistence")
    return value


def _items(values):
    if not isinstance(values, tuple) or len(values) > 64:
        raise ValueError("use at most 64 immutable recovery items")
    for value in values:
        _text(value)


@dataclass(frozen=True, slots=True)
class ToolExecutionState:
    """Observation only. No arguments, tool executor or automatic replay hook."""
    call_id: str
    tool_name: str
    status: str
    side_effecting: bool = True
    source_event_ids: tuple[str, ...] = ()

    def __post_init__(self):
        _text(self.call_id, limit=256)
        _text(self.tool_name, limit=256)
        if self.status not in ("planned", "running", "succeeded", "failed", "unknown"):
            raise ValueError("invalid tool execution status")
        if type(self.side_effecting) is not bool:
            raise ValueError("side_effecting must be a boolean")
        _refs(self.source_event_ids)

    @property
    def needs_reconciliation(self):
        return self.side_effecting and self.status in ("running", "unknown")


@dataclass(frozen=True, slots=True)
class RecoveryState:
    run_id: str
    version: int
    goal: str
    source_event_ids: tuple[str, ...]
    constraints: tuple[str, ...] = ()
    pending_items: tuple[str, ...] = ()
    tools: tuple[ToolExecutionState, ...] = ()

    def __post_init__(self):
        _text(self.run_id, limit=256)
        _text(self.goal)
        if type(self.version) is not int or self.version < 1:
            raise ValueError("recovery version must be positive")
        if not isinstance(self.source_event_ids, tuple):
            raise ValueError("source IDs must be immutable")
        _refs(self.source_event_ids)
        _items(self.constraints)
        _items(self.pending_items)
        if not isinstance(self.tools, tuple) or len(self.tools) > 64:
            raise ValueError("use at most 64 immutable tool states")
        seen = set()
        for tool in self.tools:
            if not isinstance(tool, ToolExecutionState) or tool.call_id in seen:
                raise ValueError("tool call IDs must be unique")
            if not set(tool.source_event_ids) <= set(self.source_event_ids):
                raise ValueError("tool evidence must be included in recovery evidence")
            seen.add(tool.call_id)

    @classmethod
    def from_dict(cls, payload):
        data = dict(payload)
        for key in ("source_event_ids", "constraints", "pending_items"):
            data[key] = tuple(data[key])
        data["tools"] = tuple(ToolExecutionState(**{**tool,
            "source_event_ids": tuple(tool["source_event_ids"])}) for tool in data["tools"])
        return cls(**data)


@dataclass(frozen=True, slots=True)
class CaptureReceipt:
    event_id: str
    run_id: str
    content_hash: str
    received_at: str
    provider_event_id: str | None = None
    persisted: bool = False
    extracted: bool = False
    retrievable: bool = False
    raw_readable: bool = False
    facts_retrievable: bool = False
    queue_status: str = "none"
    attempts: int = 0
    error_code: str | None = None

    @property
    def stage(self):
        if self.retrievable:
            return "retrievable"
        if self.extracted:
            return "extracted"
        return "persisted" if self.persisted else "received"


class EvidenceVerifier(Protocol):
    """Return true only for live authorized evidence; exceptions are not absence."""
    async def exists(self, scope, source_event_ids) -> bool: ...


class RetrievalReadinessProbe(Protocol):
    """Host/backend check for the required retrieval path, not task correctness."""
    async def ready(self, scope, source_event_ids) -> bool: ...


class RepositoryEvidenceVerifier:
    def __init__(self, repository):
        self.repository = repository

    async def exists(self, scope, source_event_ids):
        _refs(source_event_ids)
        async with self.repository.unit_of_work() as uow:
            return bool(await uow.events_exist(scope, source_event_ids))


class RecoveryMemory:
    """Internal companion to UnifiedMemory's exclusive scope gate.

    These methods do not add a distributed lock. Do not expose this instance
    or its store as an alternate public write/read path around UnifiedMemory.
    All evidence must have an exact-scope receipt from this companion.
    """
    def __init__(self, scope, store: RecoveryStore, evidence: EvidenceVerifier, *, retrieval_probe=None):
        self.scope, self.store, self.evidence = scope, store, evidence
        self.retrieval_probe = retrieval_probe

    async def initialize(self):
        await self.store.initialize()

    async def forget_sources(self, request):
        if request.scope != self.scope:
            raise ValueError("recovery deletion scope mismatch")
        return await self.store.forget_sources(request)

    async def begin(self, safe):
        _text(safe.event_id, limit=256)
        _text(safe.run_id, limit=256)
        await self.store.ensure_run(self.scope, safe.run_id)
        job = await self.store.read(self.scope, "job", safe.event_id)
        if job is not None and job.payload["status"] == "cancelled":
            raise RecoveryConflict("cancelled capture identity cannot be reused")
        old = await self.store.read(self.scope, "receipt", safe.event_id)
        if old:
            receipt = CaptureReceipt(**old.payload)
            if receipt.content_hash != safe.content_hash or receipt.run_id != safe.run_id:
                raise RecoveryConflict("capture event ID has a different payload")
            return old
        from datetime import UTC
        receipt = CaptureReceipt(safe.event_id, safe.run_id, safe.content_hash,
                                 datetime.now(UTC).isoformat())
        await self.store.write(self.scope, "receipt", safe.event_id, asdict(receipt), (), expected_revision=0)
        return await self.store.read(self.scope, "receipt", safe.event_id)

    async def finish(self, old, provider_event_id, *, extracted):
        from dataclasses import replace
        ids = (provider_event_id,)
        if not await self.evidence.exists(self.scope, ids):
            raise ValueError("capture evidence is not durable and live")
        receipt = CaptureReceipt(**old.payload)
        if receipt.provider_event_id not in (None, provider_event_id):
            raise RecoveryConflict("capture provider identity changed")
        # A successful synchronous extraction can be recorded even if it
        # produced no claims. It does not imply successful semantic recall.
        current = replace(receipt, provider_event_id=provider_event_id,
                          persisted=True, extracted=receipt.extracted or extracted,
                          retrievable=False, raw_readable=True, facts_retrievable=False)
        await self.store.write(self.scope, "receipt", receipt.event_id,
            asdict(current), ids, expected_revision=old.revision)

    async def receipt(self, event_id):
        old = await self.store.read(self.scope, "receipt", event_id)
        if old is None:
            return None
        receipt = CaptureReceipt(**old.payload)
        if not receipt.persisted:
            return receipt
        ids = (receipt.provider_event_id,)
        if not await self.evidence.exists(self.scope, ids):
            await self.forget_sources(ForgetRequest(self.scope, ids, mode=ForgetMode.ARCHIVE))
            return None
        from dataclasses import replace
        facts = False
        if self.retrieval_probe is not None and hasattr(self.retrieval_probe, "inspect"):
            readiness = await self.retrieval_probe.inspect(self.scope, ids)
            ready, facts = readiness.facts_retrievable, readiness.facts_retrievable
        else:
            ready = bool(self.retrieval_probe and await self.retrieval_probe.ready(self.scope, ids))
        if ready != receipt.retrievable or facts != receipt.facts_retrievable or not receipt.raw_readable:
            receipt = replace(receipt, retrievable=ready, raw_readable=True, facts_retrievable=facts)
            await self.store.write(self.scope, "receipt", event_id, asdict(receipt), ids,
                                   expected_revision=old.revision)
        return receipt

    async def require_sources(self, run_id, source_event_ids):
        refs = _refs(source_event_ids)
        rows = await self.store.sources(self.scope, refs)
        captured = set()
        for row in rows:
            receipt = CaptureReceipt(**row.payload)
            if receipt.persisted and receipt.run_id == run_id:
                captured.add(receipt.provider_event_id)
        if not set(refs) <= captured:
            raise ValueError("evidence requires live capture receipts from this run and scope")
        if not await self.evidence.exists(self.scope, refs):
            # Conservative invalidation of this dependency set. No content
            # is served after even one reference becomes unavailable.
            await self.forget_sources(ForgetRequest(self.scope, refs, mode=ForgetMode.ARCHIVE))
            raise ValueError("recovery evidence is no longer available")

    async def save(self, state, *, expected_version):
        if not isinstance(state, RecoveryState):
            raise TypeError("save requires RecoveryState")
        if type(expected_version) is not int or expected_version < 0 or state.version != expected_version + 1:
            raise RecoveryConflict("recovery version must advance by exactly one")
        await self.store.ensure_run(self.scope, state.run_id)
        await self.require_sources(state.run_id, state.source_event_ids)
        await self.store.write(self.scope, "state", state.run_id, asdict(state),
            state.source_event_ids, expected_revision=expected_version)
        return state

    async def load(self, run_id):
        row = await self.store.read(self.scope, "state", run_id)
        if row is None:
            return None
        state = RecoveryState.from_dict(row.payload)
        if state.run_id != run_id or state.version != row.revision:
            raise RecoveryConflict("stored recovery identity or version is invalid")
        await self.require_sources(run_id, state.source_event_ids)
        return state


class RecoveryCaptureSink:
    """Prepare once, persist admission metadata, then ingest synchronously.

    A crash after source commit but before finish leaves a conservative
    received receipt. Retry the same envelope; never infer extraction success
    from an event merely existing. No raw text is stored in the receipt.
    """
    def __init__(self, provider, sanitizer, recovery):
        self.provider, self.sanitizer, self.recovery = provider, sanitizer, recovery

    async def submit(self, event):
        safe = await self.sanitizer.prepare(event)
        return await self.submit_prepared(safe)

    async def submit_prepared(self, safe):
        old = await self.recovery.begin(safe)
        token = _extracted_event.set(None)
        try:
            result = await self.provider.ingest_event(safe.to_memory_event())
            await self.recovery.finish(old, result.event_id,
                extracted=_extracted_event.get() == result.event_id)
        finally:
            _extracted_event.reset(token)
        return CaptureSubmission(event_id=safe.event_id, status="done",
            duplicate=result.duplicate, provider_event_id=result.event_id)
