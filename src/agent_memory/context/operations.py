"""Optional readiness, queued capture, preflight and lifecycle operations.

One UnifiedMemory instance exclusively owns each scope, including its workers.
No distributed lease, background scheduler or external service is introduced.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
import json

from .compression import CompressionPlan, CompressionResult
from ..domain import MemoryQuery
from ..lifecycle import LifecycleEvent
from .recovery import CaptureReceipt
from .store import RecoveryConflict, _refs
from ..serialization import to_jsonable


@dataclass(frozen=True, slots=True)
class RetrievalReadiness:
    raw_readable: bool
    facts_retrievable: bool
    checked_sources: int
    reason: str


class LocalRetrievalReadinessProbe:
    """Check live evidence and actual returned claims, not index existence.

    At most eight source checks and retrievals per probe. Results measure the
    configured provider's recall of a claim-text query, not arbitrary questions.
    """
    def __init__(self, repository, provider, evidence):
        self.repository, self.provider, self.evidence = repository, provider, evidence

    async def inspect(self, scope, source_event_ids):
        refs = _refs(source_event_ids)
        raw = bool(await self.evidence.exists(scope, refs))
        if not raw:
            return RetrievalReadiness(False, False, 0, "evidence_unavailable")
        if len(refs) > 8:
            return RetrievalReadiness(True, False, 0, "probe_limit_exceeded")
        claims = await self.repository.current_claims(scope)
        checked = 0
        for source_id in refs:
            claim = next((c for c in claims if c.scope == scope and
                          source_id in c.provenance.source_event_ids), None)
            if claim is None:
                return RetrievalReadiness(True, False, checked, "no_current_claim")
            bundle = to_jsonable(await self.provider.retrieve(MemoryQuery(scope, claim.text, token_budget=4096)))
            # Require both the claim identity and its evidence in the returned
            # bundle. This is conservative for providers that omit provenance.
            ids, evidence_ids = set(), set()
            stack = [bundle]
            visited = 0
            while stack:
                visited += 1
                if visited > 20000:
                    return RetrievalReadiness(True, False, checked, "bundle_limit_exceeded")
                item = stack.pop()
                if isinstance(item, dict):
                    for key, value in item.items():
                        if key in ("id", "memory_id", "claim_id") and isinstance(value, str):
                            ids.add(value)
                        if key in ("source_event_ids", "event_ids") and isinstance(value, list):
                            evidence_ids.update(v for v in value if isinstance(v, str))
                        stack.append(value)
                elif isinstance(item, list):
                    stack.extend(item)
            checked += 1
            if claim.id not in ids or source_id not in evidence_ids:
                return RetrievalReadiness(True, False, checked, "claim_not_returned_with_evidence")
        return RetrievalReadiness(True, True, checked, "claim_recall_confirmed")

    async def ready(self, scope, source_event_ids):
        return (await self.inspect(scope, source_event_ids)).facts_retrievable


class RecoveryOperations:
    """Mixin: all operations enter the owning UnifiedMemory deletion gate."""
    async def retry_retired_cleanup(self, *, limit=8):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            return await self._recovery.store.retry_retired_cleanup(limit=limit)

    async def recovery_history(self, run_id, *, limit=20, before=None):
        from .recovery import RecoveryState
        self._require_recovery()
        async with self._lock:
            await self._ready()
            page = await self._recovery.store.history(self.scope, run_id, limit=limit, before=before)
            states = []
            for row in page["items"]:
                await self._recovery.require_sources(run_id, row.source_event_ids)
                states.append(to_jsonable(RecoveryState.from_dict(row.payload)))
            await self._ready()
            return dict(items=states, next_cursor=page["next_cursor"])

    async def restore_recovery(self, run_id, version, *, expected_version):
        from .recovery import RecoveryState
        self._require_recovery()
        async with self._lock:
            await self._ready()
            current = await self._recovery.load(run_id)
            if current is None or current.version != expected_version:
                raise RecoveryConflict("current recovery version changed")
            old = await self._recovery.store.historical_state(self.scope, run_id, version)
            if old is None:
                raise ValueError("historical recovery version is unavailable")
            state = RecoveryState.from_dict(old.payload)
            # Historic side-effect status is not current execution evidence.
            tools = tuple(replace(tool, status="unknown") if tool.side_effecting else tool for tool in state.tools)
            restored = replace(state, version=expected_version+1, tools=tools)
            result = await self._recovery.save(restored, expected_version=expected_version)
            await self._ready()
            return result

    async def compression_feedback_report(self, *, limit=100, after=None):
        """Bounded page aggregates; never mixes units/counters/evaluators."""
        from hashlib import sha256
        self._require_recovery()
        async with self._lock:
            await self._ready()
            page = await self._recovery.store.feedback_page(self.scope, limit=limit, after=after)
            groups, identities = {}, []
            for row in page["items"]:
                payload = row.payload
                await self._recovery.require_sources(payload["run_id"], row.source_event_ids)
                identity = dict(unit=payload["measurement_unit"], counter_id=payload["counter_id"],
                    evaluator_id=payload["evaluator_id"], strategy=payload.get("strategy"),
                    proposal_counter_id=payload.get("proposal_counter_id"))
                key = json.dumps(identity, sort_keys=True)
                item = groups.setdefault(key, dict(**identity, samples=0, before_units=0, after_units=0,
                    succeeded=0, failed=0, unknown=0))
                item["samples"] += 1
                item["before_units"] += payload["before_units"]
                item["after_units"] += payload["after_units"]
                item[payload["outcome"]] += 1
                identities.append(payload["feedback_id"])
            for item in groups.values():
                item["saved_units"] = item["before_units"]-item["after_units"]
            await self._ready()
            return dict(groups=list(groups.values()), scanned=len(identities),
                next_cursor=page["next_cursor"], page_only=True, source="host_reported",
                feedback_snapshot_digest=sha256(json.dumps(identities).encode()).hexdigest(),
                automatic_promotion=False)

    async def recovery_partition_info(self):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            return await self._recovery.store.partition_info()

    async def rotate_recovery_partition(self, *, expected_generation, approval):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            return await self._recovery.store.rotate(expected_generation=expected_generation, approval=approval)

    async def retire_recovery_partition(self, generation, *, approval):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            return await self._recovery.store.retire(generation, approval=approval)

    async def cancel_capture(self, event_id):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            result = await self._recovery.store.cancel_job(self.scope, event_id)
            await self._ready()
            return result

    async def record_compression_feedback(self, feedback):
        from .compression_feedback import CompressionFeedback
        if not isinstance(feedback, CompressionFeedback):
            raise TypeError("feedback requires CompressionFeedback")
        self._require_recovery()
        async with self._lock:
            await self._ready()
            store = self._recovery.store
            summary = await store.read(self.scope, "summary", feedback.summary_id)
            if summary is None:
                raise ValueError("compression proposal is unavailable")
            run_id = summary.payload["run_id"]
            refs = tuple(dict.fromkeys((*summary.source_event_ids, *feedback.outcome_event_ids)))
            await self._recovery.require_sources(run_id, refs)
            payload = to_jsonable(feedback)
            payload.update(run_id=run_id, input_digest=summary.payload["input_digest"],
                           recovery_version=summary.payload["recovery_version"],
                           strategy=summary.payload.get("strategy"),
                           proposal_counter_id=summary.payload.get("counter_id"),
                           saved_units=feedback.before_units - feedback.after_units,
                           measurement_source="host_reported")
            old = await store.read(self.scope, "feedback", feedback.feedback_id)
            if old is not None:
                if old.payload != payload or old.source_event_ids != refs:
                    raise RecoveryConflict("feedback identity already contains different measurements")
            else:
                await store.write(self.scope, "feedback", feedback.feedback_id, payload,
                                  refs, expected_revision=0)
            await self._ready()
            return payload

    async def compression_feedback(self, feedback_id):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            row = await self._recovery.store.read(self.scope, "feedback", feedback_id)
            if row is None:
                return None
            await self._recovery.require_sources(row.payload["run_id"], row.source_event_ids)
            await self._ready()
            return row.payload

    async def _queue_mark(self, event_id, status, attempts, error_code=None):
        row = await self._recovery.store.read(self.scope, "receipt", event_id)
        if row is None:
            raise RecoveryConflict("capture receipt is unavailable")
        receipt = replace(CaptureReceipt(**row.payload), queue_status=status,
                          attempts=attempts, error_code=error_code)
        await self._recovery.store.write(self.scope, "receipt", event_id,
            asdict(receipt), row.source_event_ids, expected_revision=row.revision)
        return receipt

    async def enqueue_capture(self, *, event_id, role, content, run_id, occurred_at=None):
        self._require_recovery()
        if role not in ("user", "assistant", "tool"):
            raise ValueError("unsupported capture role")
        at = occurred_at or datetime.now(UTC)
        event = LifecycleEvent.from_dict(dict(schema_version=1, event_id=event_id,
            event_type="tool.completed" if role == "tool" else "message.received",
            origin={"user": "user", "assistant": "model", "tool": "tool"}[role],
            occurred_at=at.isoformat(), run_id=run_id, content=content, payload={},
            actor="host-adapter"), trusted_scope=self.scope)
        async with self._lock:
            await self._ready()
            safe = await self.sink.sanitizer.prepare(event)
            receipt_row = await self._recovery.begin(safe)
            old_receipt = CaptureReceipt(**receipt_row.payload)
            if old_receipt.persisted:
                return await self._recovery.receipt(event_id)
            store = self._recovery.store
            job = await store.read(self.scope, "job", event_id)
            if job is None:
                payload = dict(run_id=run_id, event=safe.to_dict(), status="queued", attempts=0)
                await store.write(self.scope, "job", event_id, payload, (), expected_revision=0)
                receipt = await self._queue_mark(event_id, "queued", 0)
            else:
                receipt = await self._queue_mark(event_id, job.payload["status"], job.payload["attempts"])
            await self._ready()
            return receipt

    async def process_next_capture(self):
        """Process one durable job; the host schedules calls, never the plugin.

        Processing records are replayable after a crash, with at most five
        attempts. Provider idempotency and stable envelopes prevent duplicate
        source ingestion. No tool action is executed by this worker.
        """
        self._require_recovery()
        async with self._lock:
            await self._ready()
            store = self._recovery.store
            found = await store.next_job(self.scope)
            if found is None:
                return None
            event_id, job = found
            payload = dict(job.payload)
            attempts = payload["attempts"]
            if attempts >= 5:
                payload["status"] = "failed"
                await store.write(self.scope, "job", event_id, payload,
                    job.source_event_ids, expected_revision=job.revision)
                return await self._queue_mark(event_id, "failed", attempts, "attempt_limit")
            payload.update(status="processing", attempts=attempts + 1)
            revision = await store.write(self.scope, "job", event_id, payload,
                job.source_event_ids, expected_revision=job.revision)
            await self._queue_mark(event_id, "processing", attempts + 1)
            try:
                safe = LifecycleEvent.from_dict(payload["event"], trusted_scope=self.scope)
                async with asyncio.timeout(30):
                    submission = await self.sink.submit_prepared(safe)
                await store.write(self.scope, "job", event_id,
                    dict(run_id=payload["run_id"], status="done", attempts=attempts + 1),
                    (submission.provider_event_id,), expected_revision=revision)
            except Exception:
                # Preserve the sanitized envelope for explicit host retry.
                payload["status"] = "failed"
                await store.write(self.scope, "job", event_id, payload,
                    job.source_event_ids, expected_revision=revision)
                return await self._queue_mark(event_id, "failed", attempts + 1, "capture_processing_failed")
            result = await self._queue_mark(event_id, "done", attempts + 1)
            await self._ready()
            return result

    async def retry_capture(self, event_id):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            store = self._recovery.store
            job = await store.read(self.scope, "job", event_id)
            if job is None or job.payload["status"] != "failed" or job.payload["attempts"] >= 5:
                raise RecoveryConflict("capture is not retryable")
            payload = {**job.payload, "status": "queued"}
            await store.write(self.scope, "job", event_id, payload,
                job.source_event_ids, expected_revision=job.revision)
            return await self._queue_mark(event_id, "queued", payload["attempts"])

    async def validate_compression(self, summary_id, current_plan):
        """Point-in-time application preflight, not a reusable permission token."""
        self._require_recovery()
        if not isinstance(current_plan, CompressionPlan):
            raise TypeError("current_plan must be a CompressionPlan")
        async with self._lock:
            await self._ready()
            result = await self._compression.load(summary_id)
            if result is None:
                return CompressionResult(False, "proposal_unavailable", input_digest=current_plan.digest)
            if result.input_digest != current_plan.digest:
                return CompressionResult(False, "host_context_changed", input_digest=current_plan.digest)
            payload = json.loads(result.replacement)
            if payload["run_id"] != current_plan.run_id or payload["recovery"]["version"] != current_plan.recovery_version:
                return CompressionResult(False, "recovery_version_changed", input_digest=current_plan.digest)
            await self._ready()
            return replace(result, reason="preflight_passed_host_approval_required")

    async def complete_recovery(self, run_id, *, expected_version):
        self._require_recovery()
        if type(expected_version) is not int or expected_version < 1:
            raise ValueError("expected_version must be positive")
        async with self._lock:
            await self._ready()
            state = await self._recovery.load(run_id)
            if state is None or state.version != expected_version:
                raise RecoveryConflict("recovery version is unavailable or changed")
            await self._recovery.store.configure_run(self.scope, run_id, completed=True)
            return dict(run_id=run_id, status="completed")

    async def set_recovery_expiry(self, run_id, *, expires_at):
        self._require_recovery()
        if not isinstance(expires_at, datetime) or expires_at.utcoffset() is None:
            raise ValueError("expires_at must include a timezone")
        async with self._lock:
            await self._ready()
            await self._recovery.store.configure_run(self.scope, run_id, expires_at=expires_at)
            return dict(run_id=run_id, expires_at=expires_at.astimezone(UTC).isoformat())

    async def cleanup_recovery(self, *, limit=100):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            count = await self._recovery.store.cleanup(self.scope, limit=limit)
            return dict(removed_records=count)

    async def recovery_stats(self):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            return await self._recovery.store.stats(self.scope)

    async def list_recovery_runs(self, *, limit=20, after=None):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            result = await self._recovery.store.list_runs(self.scope, limit=limit, after=after)
            await self._ready()
            return result

    async def recovery_run_status(self, run_id):
        self._require_recovery()
        async with self._lock:
            await self._ready()
            result = await self._recovery.store.run_status(self.scope, run_id)
            await self._ready()
            return result

    async def capture_queue_status(self, event_id):
        """No raw envelope, provider error detail, retrieval call or retry."""
        self._require_recovery()
        async with self._lock:
            await self._ready()
            job = await self._recovery.store.read(self.scope, "job", event_id)
            if job is None:
                return None
            receipt = await self._recovery.store.read(self.scope, "receipt", event_id)
            payload = job.payload
            # The durable job is authoritative: a crash may leave the receipt's
            # queue acknowledgement behind the job transition.
            acknowledged = receipt.payload if receipt else {}
            status = payload["status"]
            attempts = payload["attempts"]
            matches = acknowledged.get("queue_status") == status and acknowledged.get("attempts") == attempts
            result = dict(event_id=event_id, run_id=payload["run_id"], status=status,
                          attempts=attempts, receipt_synchronized=matches,
                          error_code=acknowledged.get("error_code") if matches else None,
                          retryable=status == "failed" and attempts < 5,
                          resumable=status == "processing" and attempts < 5)
            await self._ready()
            return result
