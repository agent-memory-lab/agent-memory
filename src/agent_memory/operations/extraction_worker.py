"""Durable extraction on the receive ledger, using the existing bounded runner.

Sources stay immutable. Stage data and publication receipts belong to requests.
All writes and source/lease fences share the repository's transaction connection.
This adapter supports host-approved local generators and exact-scope outputs.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from hashlib import sha256
from secrets import token_urlsafe

from ..consolidation.admission import authority_to_payload
from ..domain import utc_now
from ..serialization import to_jsonable
from .retention import DurableReceiver, RetentionError, _identity, _time
from .worker_tasks import WorkerLease, WorkerQueueError, WorkerTask, WorkerTaskStatus


def processing_configuration_sha256(pipeline, policy, authority):
    return sha256(
        json.dumps(
            {
                "schema": "durable-extraction/1",
                "pipeline": pipeline.config_payload(),
                "policy": policy.config_payload(),
                "authority": authority_to_payload(authority),
                "execution": "local-exact-scope",
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


def stale():
    return WorkerQueueError("source or lease is no longer available", code="stale_lease")


class ExtractionQueue:
    """Scope/configuration-bound adapter over the transactional request outbox."""

    def __init__(
        self,
        repository,
        scope,
        configuration_sha256,
        *,
        clock=utc_now,
        max_attempts=3,
        retry_seconds=2,
    ):
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be between 1 and 100")
        if type(retry_seconds) is not int or not 0 <= retry_seconds <= 3600:
            raise ValueError("retry_seconds must be between 0 and 3600")
        self.repository, self.scope = repository, scope
        self.configuration_sha256 = configuration_sha256
        self.clock, self.max_attempts, self.retry_seconds = clock, max_attempts, retry_seconds

    async def initialize(self):
        # The owning repository initializes its schema; never open another DB.
        async with self.repository.unit_of_work() as uow:
            await DurableReceiver._check_support(uow, self.scope)

    async def enqueue(self, *args, **kwargs):
        raise NotImplementedError("submit sources through DurableReceiver")

    async def _live(self, uow, row):
        return (
            row is not None
            and row["status"] != "cancelled"
            and row["epoch"] == await uow.retention_epoch(self.scope)
            and await uow.events_exist(self.scope, (row["event_id"],))
        )

    async def checked(self, uow, request_id, token, *, completed=False):
        await DurableReceiver._check_support(uow, self.scope)
        row = await uow.retention_get(self.scope, "request", request_id)
        if not await self._live(uow, row) or row.get("lease_token") != token:
            raise stale()
        if row["configuration_sha256"] != self.configuration_sha256:
            raise stale()
        if completed and row["status"] == "completed":
            return row
        if row["status"] != "running" or datetime.fromisoformat(row["lease_until"]) <= _time(
            self.clock()
        ):
            raise stale()
        return row

    async def claim(self, worker_id, *, lease_seconds):
        _identity(worker_id)
        if type(lease_seconds) is not int or not 5 <= lease_seconds <= 86400:
            raise ValueError("invalid lease duration")
        async with self.repository.unit_of_work() as uow:
            await DurableReceiver._check_support(uow, self.scope)
            now = _time(self.clock())
            for row in await uow.retention_active(self.scope):
                if row["configuration_sha256"] != self.configuration_sha256:
                    continue
                if not await self._live(uow, row):
                    row = {
                        k: v
                        for k, v in row.items()
                        if k not in {"prepared", "result", "input_manifest"}
                    }
                    row["status"] = "cancelled"
                    await uow.retention_update(self.scope, row["request_id"], row)
                    continue
                if row["status"] == "running" and datetime.fromisoformat(row["lease_until"]) > now:
                    continue
                if (
                    row.get("next_attempt_at")
                    and datetime.fromisoformat(row["next_attempt_at"]) > now
                ):
                    continue
                if row.get("attempts", 0) >= self.max_attempts:
                    row["status"] = "dead"
                    await uow.retention_update(self.scope, row["request_id"], row)
                    continue
                until = now + timedelta(seconds=lease_seconds)
                row.update(
                    status="running",
                    attempts=row.get("attempts", 0) + 1,
                    lease_token=token_urlsafe(32),
                    lease_until=until.isoformat(),
                    worker_id=worker_id,
                )
                await uow.retention_update(self.scope, row["request_id"], row)
                task = WorkerTask(
                    row["request_id"],
                    row["request_id"],
                    self.scope,
                    "memory.extract",
                    {"fence": row["lease_token"]},
                    WorkerTaskStatus.LEASED,
                    row["attempts"],
                    self.max_attempts,
                    now,
                    datetime.fromisoformat(row["received_at"]),
                    now,
                    worker_id,
                    until,
                )
                return WorkerLease(task, row["lease_token"])
        return None

    async def checkpoint(self, lease, value):
        encoded = json.dumps(value, allow_nan=False, separators=(",", ":"))
        if len(encoded.encode()) > 160_000 or set(value) != {"prepared", "input_manifest"}:
            raise ValueError("invalid or oversized extraction checkpoint")
        async with self.repository.unit_of_work() as uow:
            row = await self.checked(uow, lease.task.id, lease.token)
            row.update(json.loads(encoded))
            await uow.retention_update(self.scope, lease.task.id, row)

    async def complete(self, lease):
        async with self.repository.unit_of_work() as uow:
            row = await self.checked(uow, lease.task.id, lease.token, completed=True)
            if row["status"] != "completed":
                raise WorkerQueueError("publication did not commit", code="publication_missing")

    async def fail(self, lease, error):
        async with self.repository.unit_of_work() as uow:
            row = await self.checked(uow, lease.task.id, lease.token, completed=True)
            if row["status"] == "completed":
                return
            row.update(
                status="dead" if row["attempts"] >= self.max_attempts else "retry_wait",
                next_attempt_at=(
                    _time(self.clock()) + timedelta(seconds=self.retry_seconds)
                ).isoformat(),
                last_error_code="processing_failed",
            )
            await uow.retention_update(self.scope, lease.task.id, row)

    async def status(self, request_id):
        async with self.repository.unit_of_work() as uow:
            await DurableReceiver._check_support(uow, self.scope)
            row = await uow.retention_get(self.scope, "request", request_id)
            if row is None:
                return None
            if not await self._live(uow, row):
                return {"request_id": request_id, "status": "cancelled"}
            return {
                "request_id": request_id,
                "source_event_id": row["event_id"],
                "status": row["status"],
                "source_persisted": True,
                "l1_decided": row["status"] == "completed",
                "index_visible": "unsupported",
                "attempts": row.get("attempts", 0),
                "result": row.get("result"),
            }


class DurableAtomHandler:
    def __init__(self, queue, pipeline, policy, authority, *, local_only):
        if local_only is not True:
            raise NotImplementedError("external dispatch authorization is not enabled")
        if queue.scope.project(pipeline.scope_level) != queue.scope:
            raise ValueError("durable extraction cannot broaden source audience")
        self.queue, self.pipeline, self.policy, self.authority = queue, pipeline, policy, authority
        self._check_config()

    def _check_config(self):
        if (
            processing_configuration_sha256(self.pipeline, self.policy, self.authority)
            != self.queue.configuration_sha256
        ):
            raise RetentionError("processing_configuration_changed")

    async def __call__(self, task, checkpoint):
        if task.scope != self.queue.scope:
            raise stale()
        self._check_config()
        async with self.queue.repository.unit_of_work() as uow:
            row = await self.queue.checked(uow, task.id, task.payload["fence"])
            ticket = await uow.retention_get(task.scope, "ticket", task.id)
            source = await uow.find_event_by_idempotency(task.scope, ticket["idempotency_key"])
            if source is None or source.id != row["event_id"]:
                raise stale()
            if source.metadata.get("lifecycle", {}).get("origin") == "model":
                raise RetentionError("model_output_is_not_independent_evidence")
            origin = source.metadata.get("lifecycle", {}).get("origin")
            expected_origin = {
                "self_report": "user",
                "tool_observation": "tool",
                "document": "host",
            }
            if origin is not None and origin != expected_origin.get(self.authority.kind):
                raise RetentionError("source_origin_authority_mismatch")
            prepared = row.get("prepared")
            manifest = {
                "source_event_id": source.id,
                "source_sha256": source.content_hash,
                "scope": to_jsonable(source.scope),
                "configuration_sha256": self.queue.configuration_sha256,
            }
            if prepared is not None and row.get("input_manifest") != manifest:
                raise ValueError("saved stage input changed")
        if prepared is None:
            prepared = await self.pipeline.prepare(
                source, authority=self.authority, policy=self.policy
            )
            if prepared["audit"]["processing_state"] != "completed":
                raise ValueError("extraction stage failed")
            await checkpoint({"prepared": prepared, "input_manifest": manifest})
        self._check_config()
        async with self.queue.repository.unit_of_work() as uow:
            row = await self.queue.checked(uow, task.id, task.payload["fence"])
            current = await uow.find_event_by_idempotency(task.scope, ticket["idempotency_key"])
            if current is None or current.content_hash != source.content_hash:
                raise stale()
            receipt = await self.pipeline.publish_prepared(
                self.queue.repository,
                source,
                prepared,
                authority=self.authority,
                policy=self.policy,
                unit_of_work=uow,
                retained=True,
            )
            row.update(
                status="completed",
                result={**to_jsonable(receipt), "extraction": prepared["audit"]},
                publication_id="publication:" + task.id,
                completed_at=_time(self.queue.clock()).isoformat(),
            )
            # The stage is no longer needed. Identity-only provenance remains.
            row.pop("prepared", None)
            await uow.retention_update(task.scope, task.id, row)
