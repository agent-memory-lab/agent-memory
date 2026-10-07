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
from .reprocessing import checked_records
from .retention import DurableReceiver, RetentionError, _identity, _time
from .source_revisions import source_is_current
from .worker_tasks import WorkerLease, WorkerQueueError, WorkerTask, WorkerTaskStatus


def processing_configuration_sha256(
    pipeline, policy, authority, *, index_channel=None, publication_policy=None
):
    return sha256(
        json.dumps(
            {
                "schema": "durable-extraction/1",
                "pipeline": pipeline.config_payload(),
                "policy": policy.config_payload(),
                "authority": authority_to_payload(authority),
                "execution": "local-exact-scope",
                **({"index_channel": index_channel.payload()} if index_channel else {}),
                **(
                    {"publication_policy": publication_policy.payload()}
                    if publication_policy
                    else {}
                ),
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
        if (
            row is None
            or row["status"] == "cancelled"
            or row["epoch"] != await uow.retention_epoch(self.scope)
        ):
            return False
        source = await uow.get_source_event(self.scope, row["event_id"])
        return source is not None and await source_is_current(uow, source)

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
                        if k not in {"prepared", "result", "input_manifest", "publication_manifest"}
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
            code = error.code if isinstance(error, RetentionError) else "processing_failed"
            conflicts = {
                "interpretation_head_changed",
                "interpretation_contribution_changed",
                "source_revision_changed",
            }
            resolution = {
                "reprocessing_needs_resolution",
                "reprocessing_incomplete",
                "reprocessing_review_incomplete",
                "reprocessing_review_inconsistent",
                "interpretation_capability_unsupported",
                "interpretation_capacity",
            }
            status = (
                "conflict"
                if code in conflicts
                else "needs_resolution"
                if code in resolution
                else "dead"
                if row["attempts"] >= self.max_attempts
                else "retry_wait"
            )
            row.update(
                status=status,
                next_attempt_at=(
                    _time(self.clock()) + timedelta(seconds=self.retry_seconds)
                ).isoformat(),
                last_error_code=code,
            )
            await uow.retention_update(self.scope, lease.task.id, row)

    async def status(self, request_id):
        async with self.repository.unit_of_work() as uow:
            await DurableReceiver._check_support(uow, self.scope)
            row = await uow.retention_get(self.scope, "request", request_id)
            if row is None:
                return None
            if not await self._live(uow, row):
                source = await uow.get_source_event(self.scope, row["event_id"])
                status = (
                    "superseded"
                    if source is not None and row["epoch"] == await uow.retention_epoch(self.scope)
                    else "cancelled"
                )
                return {"request_id": request_id, "status": status}
            head = await uow.retention_head_get(self.scope, "interpretation", row["event_id"])
            return {
                "request_id": request_id,
                "source_event_id": row["event_id"],
                "status": row["status"],
                "source_persisted": True,
                "l1_decided": row["status"] == "completed",
                "interpretation_current": head is not None
                and head["payload"]["request_id"] == request_id,
                "index_visible": "unsupported",
                "attempts": row.get("attempts", 0),
                "last_error_code": row.get("last_error_code"),
                "result": row.get("result"),
            }

    async def resume(self, request_id, *, expected_manifest_version, actor, reason):
        """Explicit host recovery of a dead partial publication; completed work stays fixed."""
        from .publication_manifest import valid_manifest

        _identity(request_id)
        _identity(actor)
        _identity(reason)
        if type(expected_manifest_version) is not int:
            raise RetentionError("invalid_manifest_version")
        async with self.repository.unit_of_work() as uow:
            await DurableReceiver._check_support(uow, self.scope)
            row = await uow.retention_get(self.scope, "request", request_id)
            if not await self._live(uow, row):
                raise RetentionError("source_unavailable")
            if row["configuration_sha256"] != self.configuration_sha256:
                raise RetentionError("processing_configuration_changed")
            manifest = row.get("publication_manifest", {})
            if (
                row["status"] != "dead"
                or manifest.get("schema") != "publication-manifest/2"
                or not valid_manifest(self.scope, row)
                or manifest["closed"]
                or not manifest["publications"]
                or "prepared" not in row
            ):
                raise RetentionError("publication_resume_unavailable")
            if manifest["version"] != expected_manifest_version:
                raise RetentionError("publication_manifest_changed")
            resumptions = manifest.setdefault("resumptions", [])
            if len(resumptions) >= 32:
                raise RetentionError("publication_resume_capacity")
            resumptions.append(
                {
                    "actor": actor,
                    "reason": reason,
                    "attempts": row["attempts"],
                    "manifest_version": expected_manifest_version,
                    "recorded_at": _time(self.clock()).isoformat(),
                }
            )
            row.update(status="queued", attempts=0)
            for field in ("lease_token", "lease_until", "next_attempt_at", "last_error_code"):
                row.pop(field, None)
            if not valid_manifest(self.scope, row):
                raise RetentionError("publication_resume_capacity")
            await uow.retention_update(self.scope, request_id, row)
            return {
                "request_id": request_id,
                "status": "queued",
                "manifest_version": manifest["version"],
            }


class DurableAtomHandler:
    def __init__(
        self,
        queue,
        pipeline,
        policy,
        authority,
        *,
        local_only,
        index_channel=None,
        publication_policy=None,
    ):
        if local_only is not True:
            raise NotImplementedError("external dispatch authorization is not enabled")
        if queue.scope.project(pipeline.scope_level) != queue.scope:
            raise ValueError("durable extraction cannot broaden source audience")
        self.queue, self.pipeline, self.policy, self.authority = queue, pipeline, policy, authority
        if index_channel is not None:
            from .indexing import CandidateIndexChannel

            if not isinstance(index_channel, CandidateIndexChannel):
                raise TypeError("expected CandidateIndexChannel")
        self.index_channel = index_channel
        if publication_policy is not None:
            from .publication_batches import PublicationPolicy

            if not isinstance(publication_policy, PublicationPolicy):
                raise TypeError("expected PublicationPolicy")
        self.publication_policy = publication_policy
        self._check_config()

    def _check_config(self):
        if (
            processing_configuration_sha256(
                self.pipeline,
                self.policy,
                self.authority,
                index_channel=self.index_channel,
                publication_policy=self.publication_policy,
            )
            != self.queue.configuration_sha256
        ):
            raise RetentionError("processing_configuration_changed")

    async def __call__(self, task, checkpoint):
        if task.scope != self.queue.scope:
            raise stale()
        self._check_config()
        async with self.queue.repository.unit_of_work() as uow:
            row = await self.queue.checked(uow, task.id, task.payload["fence"])
            source = await uow.get_source_event(task.scope, row["event_id"])
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
            reprocessing = row.get("reprocessing")
            records = (
                (
                    await checked_records(
                        uow,
                        task.scope,
                        source.id,
                        reprocessing["expected_head_generation"],
                        row["base_versions"],
                    )
                )
                if reprocessing
                else ()
            )
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
            if reprocessing:
                from ..consolidation.interpretation import prepare_reconciliation

                prepared = await prepare_reconciliation(
                    self.pipeline,
                    source,
                    prepared,
                    records,
                    reprocessing,
                    self.policy,
                    self.authority,
                )
            await checkpoint({"prepared": prepared, "input_manifest": manifest})
        self._check_config()
        if self.publication_policy is not None and not reprocessing:
            from .publication_batches import initial

            await initial(self, task, source, prepared)
            return
        async with self.queue.repository.unit_of_work() as uow:
            row = await self.queue.checked(uow, task.id, task.payload["fence"])
            current = await uow.get_source_event(task.scope, source.id)
            if current is None or current.content_hash != source.content_hash:
                raise stale()
            interpretation = None
            if reprocessing:
                from ..consolidation.interpretation import activate

                receipt, interpretation = await activate(
                    uow,
                    self.queue.repository,
                    source,
                    row,
                    prepared,
                    self.pipeline,
                    self.policy,
                    self.authority,
                )
            else:
                receipt = await self.pipeline.publish_prepared(
                    self.queue.repository,
                    source,
                    prepared,
                    authority=self.authority,
                    policy=self.policy,
                    unit_of_work=uow,
                    retained=True,
                )
                active_ids = [
                    d.candidate_id
                    for d in receipt.decisions
                    if d.action not in {"REJECT", "L0_ONLY"}
                ]
                await uow.retention_head_put(
                    task.scope,
                    "interpretation",
                    source.id,
                    {"active_ids": active_ids, "request_id": task.id, "stream": "primary"},
                    0,
                )
            row.update(
                status="completed",
                result={
                    **to_jsonable(receipt),
                    "extraction": prepared["audit"],
                    **({"interpretation": interpretation} if interpretation else {}),
                },
                publication_id="publication:" + task.id,
                completed_at=_time(self.queue.clock()).isoformat(),
            )
            from .readiness import close

            close(task.scope, row, receipt, interpretation)
            if self.publication_policy is not None:
                from .publication_batches import atomic_manifest

                await atomic_manifest(
                    uow, task.scope, row, receipt, interpretation, prepared, self.publication_policy
                )
            if self.index_channel is not None:
                from .indexing import enqueue

                await enqueue(uow, task.scope, row, self.index_channel)
            # The stage is no longer needed. Identity-only provenance remains.
            row.pop("prepared", None)
            await uow.retention_update(task.scope, task.id, row)
