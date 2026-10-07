"""Typed facet work units with finite receipts and BoundedWorker lease compatibility.

Separate ledger kinds avoid interpreting an empty facet as a legacy source unit.
Dirty definitions are the transactional outbox; scheduling never loses dirty work
under backpressure and never enlarges a previously claimed target.
"""

from datetime import datetime, timedelta
from secrets import token_urlsafe

from ..derived.model import DerivedError, digest, identity, timestamp
from ..derived.service import open_derived
from .worker_tasks import WorkerLease, WorkerQueueError, WorkerTask, WorkerTaskStatus


def stale():
    return WorkerQueueError("facet lease is stale", code="stale_lease")


def valid_completion(scope, row):
    if row.get("status") != "completed" or row.get("outcome") not in {"applied", "noop"}:
        return False
    if type(row.get("no_outputs")) is not bool or bool(row.get("revision_id")) == row["no_outputs"]:
        return False
    expected = "derived-commit:" + digest(
        [scope.partition_key(), row["unit"], row.get("revision_id"), row["outcome"]]
    )
    return row.get("commit_token") == expected


async def checked_job(service, uow, task, *, completed=False):
    if task.scope != service.scope or task.task_type != "memory.facet_refresh":
        raise stale()
    row = await uow.derived_get(service.scope, "job", task.id)
    if (
        not row
        or row.get("fence") != task.payload.get("fence")
        or row.get("generation") != task.payload.get("generation")
    ):
        raise stale()
    if row.get("unit") != task.payload.get("unit") or row["unit"][
        "epoch"
    ] != await uow.retention_epoch(service.scope):
        raise stale()
    if row.get("expires_at") and datetime.fromisoformat(row["expires_at"]) <= service.clock():
        raise stale()
    if completed and row["status"] == "completed" and valid_completion(service.scope, row):
        return row
    if row["status"] != "running" or datetime.fromisoformat(row["lease_until"]) <= service.clock():
        raise stale()
    return row


class FacetRefreshQueue:
    def __init__(self, service, *, max_attempts=3, max_active=128, max_age_seconds=86400):
        for value, low, high in (
            (max_attempts, 1, 100),
            (max_active, 1, 128),
            (max_age_seconds, 5, 86400),
        ):
            if type(value) is not int or not low <= value <= high:
                raise ValueError("invalid facet queue limits")
        self.service, self.max_attempts, self.max_active, self.max_age_seconds = (
            service,
            max_attempts,
            max_active,
            max_age_seconds,
        )
        self.repository, self.scope = service.repository, service.scope

    async def _job(self, uow, definition):
        unit = await self.service._unit(uow, definition)
        key = unit.id
        row = await uow.derived_get(self.scope, "job", key)
        if row:
            definition["dirty"] = False
            await uow.derived_put(self.scope, "definition", definition["facet_id"], definition)
            return row
        jobs = await uow.derived_records(self.scope, "job")
        if (
            len(jobs) >= 4096
            or sum(r["payload"]["status"] in {"pending", "running", "retry"} for r in jobs)
            >= self.max_active
        ):
            raise DerivedError("derived_refresh_backpressure")
        now = timestamp(self.service.clock()).isoformat()
        row = dict(
            id=key,
            unit=unit.payload(),
            status="pending",
            attempts=0,
            generation=0,
            expires_at=(self.service.clock() + timedelta(seconds=self.max_age_seconds)).isoformat(),
            created_at=now,
            next_attempt_at=now,
        )
        await uow.derived_put(self.scope, "job", key, row)
        definition["dirty"] = False
        await uow.derived_put(self.scope, "definition", definition["facet_id"], definition)
        return row

    async def request(self, facet_id, *, dedupe_key, force=False):
        identity(dedupe_key)
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            target_id = "facet-target:" + digest([self.scope.partition_key(), epoch, dedupe_key])
            definition = await self.service._definition(uow, facet_id)
            old = await uow.derived_get(self.scope, "request", target_id)
            if old:
                if old.get("facet_id") != facet_id or old.get("force", False) != force:
                    raise DerivedError("derived_target_conflict")
                return old
            if len(await uow.derived_records(self.scope, "request")) >= 4096:
                raise DerivedError("derived_target_capacity")
            await self._time_transition(uow, definition)
            if type(force) is not bool:
                raise DerivedError("invalid_derived_request")
            if force:
                definition.update(time_generation=definition["time_generation"] + 1, dirty=True)
            job = await self._job(uow, definition)
            receipt = dict(
                target_id=target_id,
                force=force,
                facet_id=facet_id,
                unit_id=job["id"],
                unit=job["unit"],
                readers=definition["spec"]["readers"],
                epoch=job["unit"]["epoch"],
            )
            if definition["spec"].get("context"):
                receipt["context_token"] = self.service.context_token
            await uow.derived_put(self.scope, "request", target_id, receipt)
            return receipt

    async def _time_transition(self, uow, definition):
        boundary = definition.get("next_transition_at")
        if boundary and datetime.fromisoformat(boundary) <= self.service.clock():
            definition.update(
                time_generation=definition["time_generation"] + 1,
                dirty=True,
                next_transition_at=None,
            )
            await uow.derived_put(self.scope, "definition", definition["facet_id"], definition)

    async def claim(self, worker_id, *, lease_seconds):
        identity(worker_id)
        if type(lease_seconds) is not int or not 5 <= lease_seconds <= 86400:
            raise ValueError("invalid facet lease duration")
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            definitions = await uow.derived_records(self.scope, "definition")
            owned = {
                item["identity"]
                for item in definitions
                if self.service.accepts_definition(item["payload"])
            }
            for item in definitions:
                definition = item["payload"]
                if (
                    item["identity"] not in owned
                    or definition.get("disabled")
                    or definition["epoch"] != epoch
                ):
                    continue
                await self._time_transition(uow, definition)
                if definition["dirty"]:
                    try:
                        await self._job(uow, definition)
                    except DerivedError as error:
                        if error.code != "derived_refresh_backpressure":
                            raise
            rows = [item["payload"] for item in await uow.derived_records(self.scope, "job")]
            now = self.service.clock()
            live_facets = {
                r["unit"]["facet_id"]
                for r in rows
                if r["status"] == "running" and datetime.fromisoformat(r["lease_until"]) > now
            }
            for row in sorted(rows, key=lambda r: (r.get("created_at", ""), r["id"])):
                if row.get("unit", {}).get("facet_id") not in owned:
                    continue
                if row["status"] not in {"pending", "retry", "running"}:
                    continue
                if row["unit"]["epoch"] != epoch:
                    row.update(status="cancelled", reason="epoch_changed")
                elif (
                    row["status"] == "running" and datetime.fromisoformat(row["lease_until"]) > now
                ):
                    continue
                elif row["unit"]["facet_id"] in live_facets:
                    continue
                elif datetime.fromisoformat(row["next_attempt_at"]) > now:
                    continue
                elif row["attempts"] >= self.max_attempts or now - datetime.fromisoformat(
                    row["created_at"]
                ) > timedelta(seconds=self.max_age_seconds):
                    row.update(status="dead", reason="refresh_budget_exhausted")
                else:
                    try:
                        await self.service._check_unit(uow, row["unit"])
                    except DerivedError as error:
                        row.update(status="superseded", reason=error.code)
                    else:
                        row.update(
                            status="running",
                            attempts=row["attempts"] + 1,
                            generation=row["generation"] + 1,
                            fence=token_urlsafe(24),
                            lease_until=(now + timedelta(seconds=lease_seconds)).isoformat(),
                        )
                        await uow.derived_put(self.scope, "job", row["id"], row)
                        task = WorkerTask(
                            row["id"],
                            row["id"],
                            self.scope,
                            "memory.facet_refresh",
                            dict(
                                unit=row["unit"], generation=row["generation"], fence=row["fence"]
                            ),
                            WorkerTaskStatus.LEASED,
                            row["attempts"],
                            self.max_attempts,
                            now,
                            datetime.fromisoformat(row["created_at"]),
                            now,
                            worker_id,
                            datetime.fromisoformat(row["lease_until"]),
                        )
                        return WorkerLease(task, row["fence"])
                await uow.derived_put(self.scope, "job", row["id"], row)
            return None

    async def checkpoint(self, lease, value):
        # Derived preparation is bounded and deterministic, regenerated on retry.
        # Never put source bodies into the legacy 8KiB worker checkpoint.
        if value:
            raise DerivedError("derived_checkpoint_unsupported")
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope)
            await checked_job(self.service, uow, lease.task)

    async def complete(self, lease):
        if lease.token != lease.task.payload.get("fence"):
            raise stale()
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope)
            row = await checked_job(self.service, uow, lease.task, completed=True)
            if not valid_completion(self.scope, row):
                raise DerivedError("derived_output_not_committed")

    async def fail(self, lease, error):
        async with self.repository.unit_of_work() as uow:
            await open_derived(uow, self.scope)
            row = await checked_job(self.service, uow, lease.task)
            code = getattr(error, "code", "derived_processing_failed")
            row.update(
                status="dead" if row["attempts"] >= self.max_attempts else "retry",
                reason=code,
                next_attempt_at=(
                    self.service.clock() + timedelta(seconds=2 ** row["attempts"])
                ).isoformat(),
            )
            await uow.derived_put(self.scope, "job", row["id"], row)

    async def status(self, target_id, *, actor):
        async with self.repository.unit_of_work() as uow:
            epoch = await open_derived(uow, self.scope)
            receipt = await uow.derived_get(self.scope, "request", identity(target_id))
            if not receipt or receipt.get("invalidated") or receipt.get("epoch") != epoch:
                raise DerivedError("derived_target_unavailable")
            if actor not in receipt["readers"]:
                raise DerivedError("derived_read_denied")
            if (
                receipt.get("context_token") is not None
                and receipt["context_token"] != self.service.context_token
            ):
                raise DerivedError("derived_context_mismatch")
            row = await uow.derived_get(self.scope, "job", receipt["unit_id"])
            complete = bool(
                row
                and row["status"] == "completed"
                and valid_completion(self.scope, row)
                and row.get("unit") == receipt["unit"]
            )
            return dict(
                target_id=target_id,
                state=row["status"] if row else "missing",
                complete=complete,
                outcome=row.get("outcome") if row else None,
                no_outputs=row.get("no_outputs") if row else None,
                commit_token=row.get("commit_token") if complete else None,
            )
