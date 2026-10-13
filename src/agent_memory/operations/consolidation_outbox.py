"""Recoverable handoff from committed generic ingestion to a host scheduler.

The source transaction stores an intent, not a second copy of source content.
Delivery is at least once: schedulers must deduplicate the stable
``event:{event.id}:consolidate`` key. Consumers must still validate live evidence
when committing derived memory; an outbox cannot atomically fence a remote queue.
"""

from dataclasses import dataclass
from datetime import datetime

from ..domain import ForgetMode, ForgetRequest, IngestResult, MemoryEvent, MemoryScope
from ..extraction_result import ExtractionDiagnostic


@dataclass(frozen=True, slots=True)
class ConsolidationDelivery:
    event: MemoryEvent
    result: IngestResult
    token: str
    lease_expires_at: datetime
    attempts: int = 1

    @property
    def idempotency_key(self):
        return f"event:{self.event.id}:consolidate"


@dataclass(frozen=True, slots=True)
class ConsolidationCancellation:
    scope: MemoryScope
    event_id: str
    generation: int
    attempts: int = 0


def retry_delay_seconds(attempts: int) -> int:
    """Bound exponential backoff; keep failed intents available for repair/retry."""
    return min(300, 2 ** min(9, max(0, attempts - 1)))


def restore_ingest_result(payload):
    """Restore immutable diagnostics as well as the original ingestion IDs."""
    result = dict(payload)
    for name in ("claim_ids", "state_delta_ids", "superseded_claim_ids"):
        result[name] = tuple(result[name])
    if result.get("extraction") is not None:
        result["extraction"] = ExtractionDiagnostic.from_dict(result["extraction"])
    return IngestResult(**result)


async def _cancel_unavailable_source(repository, scheduler, delivery):
    if not await repository.consolidation_source_is_live(delivery):
        cancellation = await repository.request_consolidation_cancellation(
            delivery.event.scope, delivery.event.id
        )
        await scheduler.cancel_forget(
            ForgetRequest(delivery.event.scope, (delivery.event.id,), mode=ForgetMode.ERASE)
        )
        if cancellation is not None:
            await repository.ack_consolidation_cancellation(cancellation)


async def drain_consolidation_cancellations(repository, scheduler, *, scope=None, limit=64):
    """Retry ID-only queue cancellations without dispatching any new work.

    Cancellation is idempotent, so concurrent hosts may repeat it. A generation
    fence prevents an older acknowledgement from clearing a late enqueue's new
    cancellation request. No source body is read by this path.
    """
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ValueError("outbox cancellation batch limit must be between 1 and 256")
    records = await repository.pending_consolidation_cancellations(scope=scope, limit=limit)
    totals = dict(claimed=len(records), cancelled=0, failed=0, skipped=0)
    for record in records:
        try:
            await scheduler.cancel_forget(
                ForgetRequest(record.scope, (record.event_id,), mode=ForgetMode.ERASE)
            )
            acknowledged = await repository.ack_consolidation_cancellation(record)
        except Exception:
            await repository.fail_consolidation_cancellation(record)
            totals["failed"] += 1
            continue
        totals["cancelled" if acknowledged else "skipped"] += 1
    return totals


async def drain_consolidation_outbox(
    repository, scheduler, *, scope=None, event_id=None, limit=64, lease_seconds=60
):
    """Deliver a bounded batch; failures retain durable intents for later retry.

    No scheduler exception turns committed ingestion into a misleading failed
    write. Cancellation remains cancellation; its lease expires for recovery.
    Sources are hydrated only after claiming and checked again at delivery.
    A failing intent is delayed without blocking unrelated scopes. Each intent
    is attempted at most once per batch, even when processing outlasts backoff.
    An explicit event_id is an immediate retry, still bounded to one attempt.
    """
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ValueError("outbox batch limit must be between 1 and 256")
    required = (
        "claim_consolidation_intent",
        "consolidation_delivery_is_current",
        "consolidation_source_is_live",
        "complete_consolidation_intent",
        "fail_consolidation_intent",
    )
    if not all(callable(getattr(repository, name, None)) for name in required):
        raise NotImplementedError("repository lacks transactional consolidation outbox")
    totals = dict(claimed=0, delivered=0, failed=0, skipped=0)
    attempted = []
    for _ in range(limit):
        delivery = await repository.claim_consolidation_intent(
            scope=scope, event_id=event_id, lease_seconds=lease_seconds,
            exclude_event_ids=tuple(attempted),
        )
        if delivery is None:
            break
        totals["claimed"] += 1
        attempted.append(delivery.event.id)
        try:
            if not await repository.consolidation_delivery_is_current(delivery):
                totals["skipped"] += 1
                continue
            await scheduler.enqueue_event(delivery.event, delivery.result)
            completed = await repository.complete_consolidation_intent(delivery)
        except Exception as error:
            await repository.fail_consolidation_intent(delivery, type(error).__name__)
            totals["failed"] += 1
            # Enqueue may commit and then fail (for example a lost response).
            # If deletion raced that write, clean up the late task exactly as
            # for an acknowledged enqueue whose publication fence went stale.
            try:
                await _cancel_unavailable_source(repository, scheduler, delivery)
            except Exception:
                # The primary failure is already counted; live-evidence guards
                # still prevent publication if queue cancellation also fails.
                pass
            continue
        if completed:
            totals["delivered"] += 1
        else:
            totals["skipped"] += 1
            # Deletion can race a foreign queue's enqueue. Cancellation is an
            # additional cleanup; source validation on derived writes remains
            # mandatory, including when this host crashes before cleanup.
            try:
                await _cancel_unavailable_source(repository, scheduler, delivery)
            except Exception:
                # The invalidated source cannot publish derived memory. Expose
                # the cleanup failure without resurrecting its erased intent.
                totals["failed"] += 1
    return totals
