"""Bounded, resumable projection of host-owned immutable Claim snapshots."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol

from ..domain import Claim, MemoryScope
from .model import OntologyEvidenceVerifier, OntologySchema, OntologyStore
from .plugins import OntologyProjectionConsolidatorPlugin
from .schema import ontology_schema_digest
from ..extensions.protocol import ConsolidationRequest, PluginContext


@dataclass(frozen=True, slots=True)
class OntologyClaimPage:
    claims: tuple[Claim, ...]
    next_cursor: str | None


class OntologyClaimSnapshot(Protocol):
    """Host supplies stable pagination over an immutable, scope-filtered snapshot."""

    async def read_page(
        self, scope: MemoryScope, snapshot_id: str, *, cursor: str | None, limit: int,
    ) -> OntologyClaimPage: ...


@dataclass(frozen=True, slots=True)
class OntologyBackfillCheckpoint:
    scope: MemoryScope
    snapshot_id: str
    schema_digest: str
    cursor: str | None = None
    processed_claims: int = 0
    completed: bool = False
    target_index_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope")
        _identifier(self.snapshot_id, "snapshot_id")
        _identifier(self.schema_digest, "schema_digest")
        if self.target_index_id is not None:
            _identifier(self.target_index_id, "target_index_id")
        if self.cursor is not None:
            _identifier(self.cursor, "cursor")
        if type(self.processed_claims) is not int or self.processed_claims < 0:
            raise ValueError("processed_claims must be nonnegative")
        if type(self.completed) is not bool:
            raise TypeError("completed must be a boolean")
        if self.completed and self.cursor is not None:
            raise ValueError("completed checkpoint cannot have a next cursor")
        if not self.completed and self.processed_claims and self.cursor is None:
            raise ValueError("unfinished checkpoint requires a continuation cursor")


class OntologyCheckpointSink(Protocol):
    """Persist a checkpoint durably; raise on failure. Host owns access control."""

    async def save(self, checkpoint: OntologyBackfillCheckpoint) -> None: ...


async def backfill_ontology_memory(
    source: OntologyClaimSnapshot,
    snapshot_id: str,
    schema: OntologySchema,
    store: OntologyStore,
    evidence_verifier: OntologyEvidenceVerifier,
    context: PluginContext,
    *,
    checkpoint_sink: OntologyCheckpointSink,
    resume: OntologyBackfillCheckpoint | None = None,
    batch_size: int = 32,
    max_batches: int = 8,
    target_index_id: str | None = None,
    phase_timeout_ms: int | None = None,
) -> OntologyBackfillCheckpoint:
    """Backfill at most max_batches; checkpoint only after successful projection.

    This is at-least-once replay: storage may contain part of a failed batch.
    Reusing its prior checkpoint safely replays deterministic projections. Use
    a single worker per job and a stable source snapshot. processed_claims counts
    consumed inputs, including claims with no ontology payload, not assertions.

    Completion means source exhaustion, not activation readiness. This operation
    does not remove old projections, rebuild from deleted evidence, or activate
    a schema. Full rebuilds require a fresh target index managed by the host.

    phase_timeout_ms configures this job's projection plugin budget, still
    capped by the host context timeout. None retains the 2-second plugin
    default. This is a per-phase budget, not an overall job deadline.
    """
    _identifier(snapshot_id, "snapshot_id")
    if type(batch_size) is not int or not 1 <= batch_size <= 256:
        raise ValueError("batch_size must be between 1 and 256")
    if type(max_batches) is not int or not 1 <= max_batches <= 128:
        raise ValueError("max_batches must be between 1 and 128")
    if phase_timeout_ms is not None and (
        type(phase_timeout_ms) is not int or not 1 <= phase_timeout_ms <= 300_000
    ):
        raise ValueError("phase_timeout_ms must be between 1 and 300000")
    current = resume or OntologyBackfillCheckpoint(
        context.scope, snapshot_id, ontology_schema_digest(schema), target_index_id=target_index_id,
    )
    if (
        current.scope != context.scope
        or current.snapshot_id != snapshot_id
        or current.schema_digest != ontology_schema_digest(schema)
        or current.target_index_id != target_index_id
    ):
        raise ValueError("checkpoint does not match scope, snapshot, or target schema")
    _live(context)
    if target_index_id is not None:
        if await store.index_identity() != target_index_id:
            raise ValueError("target index identity changed; refusing checkpoint resume")
    if current.completed:
        return current
    plugin = OntologyProjectionConsolidatorPlugin(
        store, schema, evidence_verifier,
        timeout_ms=2_000 if phase_timeout_ms is None else phase_timeout_ms,
    )
    limits = plugin.plugin_manifest().resource_limits
    limit = min(batch_size, context.resource_limits.max_batch_size, limits.max_batch_size)
    timeout = min(context.resource_limits.timeout_ms, limits.timeout_ms) / 1_000
    visited = {current.cursor}

    async def run_phase(name, operation):
        try:
            return await asyncio.wait_for(operation, timeout=timeout)
        except TimeoutError as exc:
            # Do not include tenant identities, opaque cursors or claim data.
            exc.add_note(
                f"ontology backfill phase={name}; timeout_ms={int(timeout * 1000)}; "
                f"last_acknowledged_processed_claims={current.processed_claims}; "
                "resume from the last durable checkpoint; writes may have completed"
            )
            raise

    try:
        await run_phase("initialize", plugin.initialize(context))
        for _ in range(max_batches):
            _live(context)
            page = await run_phase(
                "read_page",
                source.read_page(context.scope, snapshot_id, cursor=current.cursor, limit=limit),
            )
            if not isinstance(page, OntologyClaimPage) or not isinstance(page.claims, tuple):
                raise TypeError("source must return an OntologyClaimPage with tuple claims")
            if len(page.claims) > limit:
                raise ValueError("source exceeded the requested batch size")
            if any(not isinstance(claim, Claim) or claim.scope != context.scope for claim in page.claims):
                raise ValueError("source returned invalid or cross-scope claims")
            if page.next_cursor is not None:
                _identifier(page.next_cursor, "next_cursor")
                if page.next_cursor in visited or not page.claims:
                    raise ValueError("source pagination did not make progress")
            _live(context)
            await run_phase(
                "consolidate",
                plugin.consolidate(ConsolidationRequest(context.scope, claims=page.claims), context),
            )
            next_checkpoint = OntologyBackfillCheckpoint(
                context.scope, snapshot_id, current.schema_digest,
                cursor=page.next_cursor,
                processed_claims=current.processed_claims + len(page.claims),
                completed=page.next_cursor is None,
                target_index_id=target_index_id,
            )
            await run_phase("save_checkpoint", checkpoint_sink.save(next_checkpoint))
            current = next_checkpoint
            if current.completed:
                break
            visited.add(current.cursor)
        return current
    finally:
        await plugin.close()


def _identifier(value, name):
    if not isinstance(value, str) or not value.strip() or len(value) > 4_096:
        raise ValueError(f"{name} must contain 1 to 4096 characters")


def _live(context):
    if context.cancelled or context.expired:
        raise RuntimeError("backfill context is cancelled or expired")
