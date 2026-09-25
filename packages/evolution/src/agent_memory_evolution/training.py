"""Authorized dataset export and host-owned training/orchestration handoff."""
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
import inspect
import json
import math
from pathlib import Path

from agent_memory.serialization import to_jsonable
from .feedback import FeedbackTrajectory


@dataclass(frozen=True, slots=True)
class TrainingBatch:
    batch_id: str
    scope: object
    payload: bytes
    record_count: int
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TrainingJob:
    batch_id: str
    scope: object
    backend_job_id: str


class MemoryTrainingBridge:
    """No trainer, shell, networking, reward definition or auto-promotion inside.

    Required host ports: authorize(scope, action, subject), is_current(trajectory),
    redact(row), evidence.verify(scope, ids). Optional trainer provides async
    submit(payload, idempotency_key), status(job_id), cancel(job_id). It must
    honor idempotency keys across process restarts and uncertain responses.
    """

    def __init__(self, *, authorize, is_current, redact, evidence, trainer=None,
                 max_records=128, max_bytes=1048576):
        if any(not callable(value) for value in (authorize, is_current, redact)):
            raise TypeError("authorization, freshness and redaction callbacks are required")
        if type(max_records) is not int or not 1 <= max_records <= 256:
            raise ValueError("max_records must be between 1 and 256")
        if type(max_bytes) is not int or not 1024 <= max_bytes <= 8388608:
            raise ValueError("max_bytes must be between 1024 and 8388608")
        self.authorize, self.is_current, self.redact = authorize, is_current, redact
        self.evidence, self.trainer = evidence, trainer
        self.max_records, self.max_bytes = max_records, max_bytes

    async def _permit(self, scope, action, subject):
        if await self.authorize(scope, action, subject) is not True:
            raise PermissionError("host denied " + action)

    async def prepare(self, scope, trajectories):
        if not isinstance(trajectories, (tuple, list)) or not 1 <= len(trajectories) <= self.max_records:
            raise ValueError("training input must be a bounded nonempty sequence")
        await self._permit(scope, "training.prepare", str(len(trajectories)))
        rows, sources, seen, size = [], set(), set(), 0
        for trajectory in trajectories:
            if not isinstance(trajectory, FeedbackTrajectory) or trajectory.scope != scope:
                raise ValueError("training trajectories must share the authorized exact scope")
            if trajectory.decision.id in seen:
                raise ValueError("duplicate training decision")
            seen.add(trajectory.decision.id)
            if not math.isfinite(trajectory.reward.value):
                raise ValueError("training reward must be finite")
            now = datetime.now(UTC)
            for record in (trajectory.outcome, trajectory.evaluation, trajectory.reward):
                if record.expires_at is not None and record.expires_at <= now:
                    raise ValueError("expired feedback cannot be exported")
            if await self.is_current(trajectory) is not True:
                raise ValueError("feedback was corrected, revoked or invalidated")
            if len(trajectory.source_event_ids) > 256 or not await self.evidence.verify(scope, trajectory.source_event_ids):
                raise ValueError("training evidence unavailable or exceeds bounds")
            sources.update(trajectory.source_event_ids)
            row = dict(format_version="agent-memory-feedback-v1", decision=to_jsonable(trajectory.decision),
                outcome=to_jsonable(trajectory.outcome), evaluation=to_jsonable(trajectory.evaluation),
                reward=to_jsonable(trajectory.reward), source_event_ids=list(trajectory.source_event_ids))
            redacted = self.redact(row)
            if inspect.isawaitable(redacted):
                redacted = await redacted
            if not isinstance(redacted, dict):
                raise TypeError("host redactor must return a JSON object")
            encoded = (json.dumps(redacted, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n").encode()
            size += len(encoded)
            if size > self.max_bytes:
                raise ValueError("training payload exceeds byte budget")
            rows.append(encoded)
        for trajectory in trajectories:
            if await self.is_current(trajectory) is not True:
                raise ValueError("feedback changed during export preparation")
            if not await self.evidence.verify(scope, trajectory.source_event_ids):
                raise ValueError("training evidence changed during export preparation")
        payload = b"".join(rows)
        return TrainingBatch(sha256(scope.partition_key().encode() + payload).hexdigest(),
            scope, payload, len(rows), tuple(sorted(sources)))

    async def export(self, scope, trajectories, destination):
        path = Path(destination)
        await self._permit(scope, "training.export", str(path.resolve()))
        batch = await self.prepare(scope, trajectories)
        # Exclusive creation never overwrites an existing dataset.
        with path.open("xb") as output:
            output.write(batch.payload)
        return batch

    async def submit(self, scope, trajectories):
        if self.trainer is None:
            raise RuntimeError("no host training adapter configured")
        batch = await self.prepare(scope, trajectories)
        await self._permit(scope, "training.submit", batch.batch_id)
        job_id = await self.trainer.submit(batch.payload, idempotency_key=batch.batch_id)
        if not isinstance(job_id, str) or not 1 <= len(job_id) <= 1024:
            raise ValueError("host trainer returned an invalid job ID")
        return TrainingJob(batch.batch_id, scope, job_id)

    async def status(self, job):
        if self.trainer is None:
            raise RuntimeError("no host training adapter configured")
        await self._permit(job.scope, "training.status", job.backend_job_id)
        return await self.trainer.status(job.backend_job_id)

    async def cancel(self, job):
        if self.trainer is None:
            raise RuntimeError("no host training adapter configured")
        await self._permit(job.scope, "training.cancel", job.backend_job_id)
        return await self.trainer.cancel(job.backend_job_id)
