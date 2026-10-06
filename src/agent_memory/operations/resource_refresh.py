"""Host-owned resource maintenance with finite unit receipts and fenced execution.

Units are immutable host work identities with retained-source dependencies. This
queue schedules local handlers; it does not grant model, view or Claim authority.
"""

import json
from collections.abc import Mapping
from datetime import datetime, timedelta
from hashlib import sha256
from secrets import token_urlsafe

from ..domain import MemoryScope, utc_now
from .retention import DurableReceiver, RetentionError, _hash, _identity, _time
from .source_revisions import source_is_current
from .worker_tasks import (
    WorkerFailureDisposition,
    WorkerLease,
    WorkerQueueError,
    WorkerTask,
    WorkerTaskStatus,
)


def identity(prefix, value):
    return prefix + ":" + sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def stale():
    return WorkerQueueError("refresh lease is stale", code="stale_lease")


def commit_token(scope, row, generation, units):
    coordinates = [
        scope.partition_key(),
        row["epoch"],
        row["resource_id"],
        row["definition_sha256"],
        generation,
        units,
    ]
    return {
        "kind": "refresh",
        "id": identity("refresh-commit", coordinates),
        "generation": generation,
        "units": units,
    }


class RefreshDeferred(Exception):
    """Quota/backpressure deferral, distinct from processing failures."""

    def __init__(self, until, reason="backpressure"):
        self.until, self.reason = _time(until), _identity(reason)
        super().__init__(reason)


class ResourceRefreshQueue:
    """Exact-scope optional queue adapted to the existing BoundedWorker."""

    def __init__(
        self,
        repository,
        scope,
        *,
        clock=utc_now,
        max_attempts=3,
        retry_seconds=2,
        max_active=128,
        max_age_seconds=86400,
        no_progress_seconds=3600,
    ):
        if not isinstance(scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope")
        for value, low, high in [
            (max_attempts, 1, 100),
            (retry_seconds, 1, 3600),
            (max_active, 1, 1000),
            (max_age_seconds, 5, 86400),
            (no_progress_seconds, 5, 86400),
        ]:
            if type(value) is not int or not low <= value <= high:
                raise ValueError("invalid refresh limits")
        self.repository, self.scope, self.clock = repository, scope, clock
        self.max_attempts, self.retry_seconds, self.max_active = (
            max_attempts,
            retry_seconds,
            max_active,
        )
        self.max_age_seconds, self.no_progress_seconds = max_age_seconds, no_progress_seconds

    async def _open(self, uow):
        await DurableReceiver._check_support(uow, self.scope)
        if not callable(getattr(uow, "refresh_get", None)):
            raise RetentionError("resource_refresh_unsupported")
        return await uow.retention_epoch(self.scope)

    async def _sources(self, uow, units):
        for source_id in sorted({s for values in units.values() for s in values}):
            source = await uow.get_source_event(self.scope, source_id)
            if source is None:
                raise RetentionError("source_unavailable")
            if not await source_is_current(uow, source):
                raise RetentionError("source_revision_changed")

    @staticmethod
    def _units(units):
        if not isinstance(units, Mapping) or not 1 <= len(units) <= 128:
            raise RetentionError("invalid_refresh_units")
        result = {}
        for unit, sources in units.items():
            _identity(unit)
            if (
                not isinstance(sources, (list, tuple))
                or not 1 <= len(sources) <= 16
                or any(not isinstance(s, str) for s in sources)
                or len(set(sources)) != len(sources)
            ):
                raise RetentionError("invalid_refresh_units")
            result[unit] = sorted(_identity(s) for s in sources)
        if len({s for values in result.values() for s in values}) > 256:
            raise RetentionError("refresh_unit_capacity")
        if len(json.dumps(result).encode()) > 128000:
            raise RetentionError("refresh_unit_capacity")
        return dict(sorted(result.items()))

    async def submit(self, *, dedupe_key, serialization_key, definition_sha256, units):
        """Persist a finite submission; new units merge without changing a live claim."""
        _identity(dedupe_key)
        _identity(serialization_key)
        _hash(definition_sha256)
        units = self._units(units)
        async with self.repository.unit_of_work() as uow:
            epoch = await self._open(uow)
            resource_id = identity(
                "refresh-resource", [self.scope.partition_key(), epoch, serialization_key]
            )
            request_id = identity(
                "refresh-request", [self.scope.partition_key(), epoch, dedupe_key]
            )
            receipt = {
                "schema": "resource-refresh-request/1",
                "request_id": request_id,
                "resource_id": resource_id,
                "epoch": epoch,
                "dedupe_key": dedupe_key,
                "definition_sha256": definition_sha256,
                "units": units,
            }
            receipt["fingerprint"] = identity("refresh-contract", receipt)
            old = await uow.refresh_get(self.scope, "request", request_id)
            if old is not None:
                if old != receipt:
                    raise RetentionError("refresh_idempotency_conflict")
                await self._sources(uow, units)
                return old
            await self._sources(uow, units)
            resources = await uow.refresh_records(self.scope, "resource")
            if len(await uow.refresh_records(self.scope, "request")) >= 4096:
                raise RetentionError("refresh_request_capacity")
            row = await uow.refresh_get(self.scope, "resource", resource_id)
            now = _time(self.clock()).isoformat()
            if row is None:
                if len(resources) >= 1000:
                    raise RetentionError("refresh_resource_capacity")
                row = {
                    "schema": "resource-refresh/1",
                    "resource_id": resource_id,
                    "serialization_key": serialization_key,
                    "definition_sha256": definition_sha256,
                    "epoch": epoch,
                    "units": {},
                    "requested_through": [],
                    "claimed_through": [],
                    "completed_through": [],
                    "commits": {},
                    "unit_commits": {},
                    "generation": 0,
                    "attempts": 0,
                    "status": "completed",
                    "created_at": now,
                    "checkpoint": {},
                    "last_committed_progress_at": None,
                    "next_attempt_at": now,
                }
            elif row["definition_sha256"] != definition_sha256:
                raise RetentionError("refresh_definition_conflict")
            elif row["status"] in {"dead", "cancelled"}:
                raise RetentionError("refresh_resource_stopped")
            for unit, sources in units.items():
                if unit in row["units"] and row["units"][unit] != sources:
                    raise RetentionError("refresh_unit_conflict")
            merged = {**row["units"], **units}
            self._units(merged)
            # Current completed inputs are dependencies too; never merge onto an invalid head.
            await self._sources(uow, merged)
            if row["status"] == "completed" and set(merged) - set(row["completed_through"]):
                active = sum(
                    r["epoch"] == epoch
                    and r["status"] in {"pending", "running", "retry_wait", "deferred"}
                    for r in resources
                )
                if active >= self.max_active:
                    raise RetentionError("refresh_active_capacity")
                row.update(status="pending", next_attempt_at=now, work_started_at=now)
            row.update(units=merged, requested_through=sorted(merged))
            await uow.refresh_put(self.scope, "resource", resource_id, row)
            await uow.refresh_put(self.scope, "request", request_id, receipt)
            return receipt

    def _expired(self, row):
        now = _time(self.clock())
        start = datetime.fromisoformat(row.get("work_started_at", row["created_at"]))
        progress = datetime.fromisoformat(row["last_committed_progress_at"] or start.isoformat())
        # An idle resource can begin another bounded cycle after its previous completion.
        progress = max(progress, start)
        return (now - start).total_seconds() >= self.max_age_seconds or (
            now - progress
        ).total_seconds() >= self.no_progress_seconds

    async def claim(self, worker_id, *, lease_seconds):
        _identity(worker_id)
        if type(lease_seconds) is not int or not 5 <= lease_seconds <= 86400:
            raise ValueError("invalid lease duration")
        async with self.repository.unit_of_work() as uow:
            epoch = await self._open(uow)
            rows = await uow.refresh_records(self.scope, "resource")
            for row in sorted(rows, key=lambda r: (r["next_attempt_at"], r["resource_id"])):
                if row["epoch"] != epoch or row["status"] not in {
                    "pending",
                    "running",
                    "retry_wait",
                    "deferred",
                }:
                    continue
                now = _time(self.clock())
                if self._expired(row):
                    row.update(status="dead", reason="refresh_age_exceeded")
                    row.pop("lease_token", None)
                    await uow.refresh_put(self.scope, "resource", row["resource_id"], row)
                    continue
                if row["status"] == "running" and datetime.fromisoformat(row["lease_until"]) > now:
                    continue
                try:
                    await self._sources(uow, row["units"])
                except RetentionError as error:
                    row.update(status="cancelled", reason=error.code)
                    row.pop("lease_token", None)
                    await uow.refresh_put(self.scope, "resource", row["resource_id"], row)
                    continue
                if row["attempts"] >= self.max_attempts:
                    row.update(
                        status="dead",
                        reason="refresh_attempts_exhausted",
                    )
                    row.pop("lease_token", None)
                    await uow.refresh_put(self.scope, "resource", row["resource_id"], row)
                    continue
                if datetime.fromisoformat(row["next_attempt_at"]) > now:
                    continue
                # Retry the original set. Newly submitted work waits for a successor claim.
                units = row["claimed_through"] or sorted(
                    set(row["requested_through"]) - set(row["completed_through"])
                )
                row.update(
                    status="running",
                    claimed_through=units,
                    generation=row["generation"] + 1,
                    attempts=row["attempts"] + 1,
                    lease_token=token_urlsafe(32),
                    lease_until=(now + timedelta(seconds=lease_seconds)).isoformat(),
                )
                await uow.refresh_put(self.scope, "resource", row["resource_id"], row)
                task = WorkerTask(
                    row["resource_id"],
                    row["serialization_key"],
                    self.scope,
                    "memory.refresh",
                    {
                        "epoch": epoch,
                        "generation": row["generation"],
                        "fence": row["lease_token"],
                        "definition_sha256": row["definition_sha256"],
                        "claimed_through": units,
                        "units": {key: row["units"][key] for key in units},
                    },
                    WorkerTaskStatus.LEASED,
                    row["attempts"],
                    self.max_attempts,
                    now,
                    datetime.fromisoformat(row["created_at"]),
                    now,
                    worker_id,
                    datetime.fromisoformat(row["lease_until"]),
                    row["checkpoint"],
                )
                return WorkerLease(task, row["lease_token"])
        return None

    async def _checked(self, uow, task, *, acknowledge=False):
        epoch = await self._open(uow)
        if task.scope != self.scope or task.task_type != "memory.refresh":
            raise stale()
        row = await uow.refresh_get(self.scope, "resource", task.id)
        if not row or row["epoch"] != epoch or task.payload.get("epoch") != epoch:
            raise stale()
        if acknowledge and (
            row.get("last_completed_generation") == task.payload.get("generation")
            and row.get("last_completed_fence") == task.payload.get("fence")
        ):
            return row, True
        if (
            row["status"] != "running"
            or row["generation"] != task.payload.get("generation")
            or row.get("lease_token") != task.payload.get("fence")
            or row["definition_sha256"] != task.payload.get("definition_sha256")
            or row["claimed_through"] != task.payload.get("claimed_through")
            or {key: row["units"][key] for key in row["claimed_through"]}
            != task.payload.get("units")
            or datetime.fromisoformat(row["lease_until"]) <= _time(self.clock())
            or self._expired(row)
        ):
            raise stale()
        try:
            await self._sources(uow, row["units"])
        except RetentionError as error:
            raise stale() from error
        return row, False

    async def commit(self, task, write):
        """Write local outputs and cover exactly the claim in one scope-locked UoW.

        write(uow, task) must return the exact committed unit IDs. The host owns
        output CAS/qualification and all actual-input tracking. No network work
        belongs in this callback; prepare outside, then fence and publish here.
        """
        if not callable(write):
            raise TypeError("write must be callable")
        async with self.repository.unit_of_work() as uow:
            row, already = await self._checked(uow, task, acknowledge=True)
            if already:
                return
            committed = await write(uow, task)
            if (
                not isinstance(committed, (list, tuple))
                or list(committed) != row["claimed_through"]
            ):
                raise RetentionError("refresh_commit_range_mismatch")
            # Recheck after the callback, including lease expiry and any in-transaction mutation.
            row, _ = await self._checked(uow, task)
            token = commit_token(self.scope, row, row["generation"], row["claimed_through"])
            row["commits"][token["id"]] = token
            row["unit_commits"].update({key: token["id"] for key in row["claimed_through"]})
            done = sorted(set(row["completed_through"]) | set(row["claimed_through"]))
            remaining = set(row["requested_through"]) - set(done)
            row.update(
                completed_through=done,
                claimed_through=[],
                checkpoint={},
                attempts=0,
                status="pending" if remaining else "completed",
                next_attempt_at=_time(self.clock()).isoformat(),
                last_committed_progress_at=_time(self.clock()).isoformat(),
                last_completed_generation=row["generation"],
                last_completed_fence=task.payload["fence"],
            )
            row.pop("lease_token", None)
            await uow.refresh_put(self.scope, "resource", row["resource_id"], row)

    async def complete(self, lease):
        async with self.repository.unit_of_work() as uow:
            _, committed = await self._checked(uow, lease.task, acknowledge=True)
            if not committed:
                raise RetentionError("refresh_commit_missing")

    async def fail(self, lease, error):
        async with self.repository.unit_of_work() as uow:
            row, committed = await self._checked(uow, lease.task, acknowledge=True)
            if committed:
                return
            now = _time(self.clock())
            disposition = None
            if isinstance(error, RefreshDeferred) and now < error.until <= now + timedelta(
                seconds=3600
            ):
                disposition = WorkerFailureDisposition.DEFERRED
                row.update(
                    status="deferred",
                    attempts=row["attempts"] - 1,
                    next_attempt_at=error.until.isoformat(),
                    deferred_reason=error.reason,
                )
            else:
                row.update(
                    status="dead" if row["attempts"] >= self.max_attempts else "retry_wait",
                    next_attempt_at=(
                        now
                        + timedelta(
                            seconds=min(3600, self.retry_seconds * 2 ** (row["attempts"] - 1))
                        )
                    ).isoformat(),
                    reason="refresh_processing_failed",
                )
            row.pop("lease_token", None)
            await uow.refresh_put(self.scope, "resource", row["resource_id"], row)
            return disposition

    async def heartbeat(self, lease, *, lease_seconds=60):
        if type(lease_seconds) is not int or not 5 <= lease_seconds <= 86400:
            raise ValueError("invalid lease duration")
        async with self.repository.unit_of_work() as uow:
            row, _ = await self._checked(uow, lease.task)
            row["lease_until"] = (
                _time(self.clock()) + timedelta(seconds=lease_seconds)
            ).isoformat()
            await uow.refresh_put(self.scope, "resource", row["resource_id"], row)

    async def checkpoint(self, lease, value):
        if not isinstance(value, Mapping):
            raise ValueError("refresh checkpoint must be a mapping")
        value = json.loads(json.dumps(dict(value), allow_nan=False))
        if len(json.dumps(value).encode()) > 8192:
            raise ValueError("refresh checkpoint capacity exceeded")
        async with self.repository.unit_of_work() as uow:
            row, _ = await self._checked(uow, lease.task)
            row["checkpoint"] = value
            await uow.refresh_put(self.scope, "resource", row["resource_id"], row)

    async def cancel(self, resource_id):
        _identity(resource_id)
        async with self.repository.unit_of_work() as uow:
            epoch = await self._open(uow)
            row = await uow.refresh_get(self.scope, "resource", resource_id)
            if row is None or row["epoch"] != epoch:
                raise RetentionError("invalid_refresh_resource")
            row.update(status="cancelled", reason="refresh_cancelled", checkpoint={})
            row.pop("lease_token", None)
            await uow.refresh_put(self.scope, "resource", resource_id, row)

    async def status(self, request_id):
        _identity(request_id)
        async with self.repository.unit_of_work() as uow:
            epoch = await self._open(uow)
            receipt = await uow.refresh_get(self.scope, "request", request_id)
            if receipt is None:
                raise RetentionError("invalid_refresh_request")
            base = {"schema": "resource-refresh-readiness/1", "request_id": request_id}
            fingerprint = receipt.get("fingerprint")
            payload = {k: v for k, v in receipt.items() if k != "fingerprint"}
            if receipt["epoch"] != epoch or receipt.get("invalidated"):
                return {**base, "state": "blocked", "reason": "source_unavailable"}
            if fingerprint != identity("refresh-contract", payload):
                return {**base, "state": "blocked", "reason": "refresh_history_unavailable"}
            row = await uow.refresh_get(self.scope, "resource", receipt["resource_id"])
            if (
                row is None
                or row["definition_sha256"] != receipt["definition_sha256"]
                or any(row["units"].get(k) != v for k, v in receipt["units"].items())
            ):
                return {**base, "state": "blocked", "reason": "refresh_history_unavailable"}
            try:
                await self._sources(uow, row["units"])
            except RetentionError as error:
                return {**base, "state": "blocked", "reason": error.code}
            done = set(row["completed_through"]) & set(receipt["units"])
            tokens = []
            for unit in sorted(done):
                token = row["commits"].get(row["unit_commits"].get(unit))
                if (
                    not isinstance(token, dict)
                    or type(token.get("generation")) is not int
                    or not 1 <= token["generation"] <= row["generation"]
                    or not isinstance(token.get("units"), list)
                    or unit not in token["units"]
                    or token != commit_token(self.scope, row, token["generation"], token["units"])
                ):
                    return {**base, "state": "blocked", "reason": "refresh_history_unavailable"}
                if token not in tokens:
                    tokens.append(token)
            remaining = sorted(set(receipt["units"]) - done)
            return {
                **base,
                "state": "reached"
                if not remaining
                else "failed"
                if row["status"] == "dead"
                or (
                    row["status"] in {"pending", "running", "retry_wait", "deferred"}
                    and self._expired(row)
                )
                else "blocked"
                if row["status"] == "cancelled"
                else "processing",
                "resource_id": row["resource_id"],
                "definition_sha256": row["definition_sha256"],
                "completed_units": sorted(done),
                "remaining_units": remaining,
                "commit_tokens": tokens,
                "generation": row["generation"],
                "resource_status": row["status"],
                "last_committed_progress_at": row["last_committed_progress_at"],
            }
