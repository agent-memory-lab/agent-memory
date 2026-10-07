"""Host-selected candidate locator, atomic publication outbox and exact finite coverage.

This channel indexes identities, not text or embeddings. It cannot authorize facts.
Every mutation and current-source check uses the admission namespace transaction.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from secrets import token_urlsafe

from ..domain import canonical_json, utc_now
from .extraction_worker import stale
from .publication_manifest import token_dispositions, token_time
from .retention import DurableReceiver, RetentionError, _identity, _time
from .source_revisions import source_is_current
from .worker_tasks import WorkerLease, WorkerTask, WorkerTaskStatus


def digest(value):
    return sha256(canonical_json(value).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CandidateIndexChannel:
    name: str

    def __post_init__(self):
        if not isinstance(self.name, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", self.name):
            raise ValueError("invalid candidate index channel name")

    def payload(self):
        return {"schema": "candidate-locator/1", "name": self.name, "version": 1}

    @property
    def key(self):
        return "index:" + digest(self.payload())


async def enqueue(uow, scope, row, channel):
    """Called inside each publication transaction, including open batch manifests."""
    if not callable(getattr(uow, "index_job_put", None)):
        raise RetentionError("index_storage_unsupported")
    from .index_recovery import active_stream, physical_channel

    stream = await active_stream(uow, scope, channel)
    key = physical_channel(channel, stream)
    manifest = row["publication_manifest"]
    manifest["index_channel"] = channel.payload()
    jobs = await uow.index_jobs(scope, key, row["epoch"])
    for token in manifest["publication_commit_tokens"]:
        existing = await uow.index_job_get(scope, key, row["epoch"], token["id"])
        if existing is not None:
            if existing["token"] != token or existing["dispositions"] != token_dispositions(
                row, token
            ):
                raise RetentionError("index_publication_conflict")
            continue
        if (
            len(jobs) >= 100000
            or sum(j["status"] in {"pending", "running", "retry_wait"} for j in jobs) >= 128
        ):
            raise RetentionError("index_outbox_capacity")
        job = {
            "schema": "index-publication/1",
            "channel": key,
            "epoch": row["epoch"],
            "token": token,
            "sequence": max((j["sequence"] for j in jobs), default=0) + 1,
            "request_id": row["request_id"],
            "event_id": row["event_id"],
            "dispositions": token_dispositions(row, token),
            "status": "pending",
            "attempts": 0,
            "created_at": token_time(row, token),
        }
        await uow.index_job_put(scope, job)
        jobs = (*jobs, job)


async def current_document(uow, scope, candidate_id):
    record = await uow.get_admission_record(scope, candidate_id)
    if (
        record is None
        or record["payload"].get("deleted")
        or record["payload"]["action"] in {"WITHDRAWN", "REJECT", "L0_ONLY"}
    ):
        return None
    source = await uow.get_source_event(scope, record["event_id"])
    if source is None or not await source_is_current(uow, source):
        return None
    return {
        "candidate_id": record["id"],
        "event_id": record["event_id"],
        "slot_key": record["slot_key"],
        "record_version": record["version"],
    }


def proof(job, applied):
    return digest(
        {
            "token": job["token"],
            "channel": job["channel"],
            "sequence": job["sequence"],
            "dispositions": job["dispositions"],
            "applied": applied,
        }
    )


def receipt_valid(job):
    if job["status"] != "completed" or not isinstance(job.get("applied"), list):
        return False
    applied = job["applied"]
    if any(not isinstance(i, dict) or set(i) != {"candidate_id", "document"} for i in applied):
        return False
    if [i.get("candidate_id") for i in applied] != [d["candidate_id"] for d in job["dispositions"]]:
        return False
    if job.get("proof") != proof(job, applied):
        return False
    return True


async def stale_candidates(uow, scope, job):
    stale_ids = set()
    for item in job["applied"]:
        candidate_id = item["candidate_id"]
        expected = await current_document(uow, scope, candidate_id)
        actual = await uow.index_document_get(scope, job["channel"], candidate_id)
        if actual != expected:
            stale_ids.add(candidate_id)
    return stale_ids


async def verified(uow, scope, job):
    return receipt_valid(job) and not await stale_candidates(uow, scope, job)


async def coverage(uow, scope, rows, channel, index_stream=None):
    from .readiness import project

    result = project(rows, "l1_decided")
    result["index_channel"] = channel
    if channel is None or not callable(getattr(uow, "index_job_get", None)):
        return {**result, "state": "unsupported", "index_visible": "unsupported"}
    try:
        selected = CandidateIndexChannel(channel["name"])
    except (KeyError, TypeError, ValueError):
        return {**result, "state": "blocked", "reason": "index_channel_invalid"}
    if selected.payload() != channel:
        return {**result, "state": "unsupported", "index_visible": "unsupported"}
    if result["state"] != "reached":
        return {**result, "index_visible": False}
    if any(r["publication_manifest"].get("index_channel") != channel for r in rows):
        return {
            **result,
            "state": "unsupported",
            "index_visible": "unsupported",
            "reason": "index_history_unavailable",
        }
    from .index_recovery import frozen_stream, physical_channel

    try:
        stream, is_current = await frozen_stream(
            uow, scope, selected, rows[0]["epoch"], index_stream
        )
    except RetentionError as error:
        return {**result, "state": "blocked", "index_visible": False, "reason": error.code}
    key = physical_channel(selected, stream)
    tokens = result["publication_commit_tokens"]
    missing, failed, invalid, applied = [], [], [], []
    coordinates, versions = {}, []
    jobs = await uow.index_jobs(scope, key, rows[0]["epoch"])
    token_jobs = {j["token"]["id"]: j for j in jobs}
    bound = max(
        (token_jobs[t["id"]]["sequence"] for t in tokens if t["id"] in token_jobs), default=0
    )
    repair_ids = {
        d["candidate_id"]
        for j in jobs
        if j["sequence"] <= bound and j["status"] in {"pending", "running", "retry_wait"}
        for d in j["dispositions"]
    }

    async def repairable(job):
        if not receipt_valid(job):
            return False
        stale_ids = await stale_candidates(uow, scope, job)
        return bool(stale_ids) and stale_ids <= repair_ids

    by_request = {r["request_id"]: r for r in rows}
    for token in tokens:
        request = by_request[token["generation"]]
        job = await uow.index_job_get(scope, key, token["epoch"], token["id"])
        if job is not None:
            versions.append(
                {
                    "publication_id": token["id"],
                    "sequence": job["sequence"],
                    "status": job["status"],
                    "attempts": job["attempts"],
                }
            )
        if (
            job is None
            or job["token"] != token
            or job["channel"] != key
            or job["epoch"] != token["epoch"]
            or job["request_id"] != token["generation"]
            or job["event_id"] != request["event_id"]
            or job["dispositions"] != token_dispositions(request, job["token"])
        ):
            invalid.append(token["id"])
        elif job["status"] in {"dead", "cancelled"}:
            coordinates[token["id"]] = job["sequence"]
            failed.append(token["id"])
        elif job["status"] != "completed":
            coordinates[token["id"]] = job["sequence"]
            missing.append(token["id"])
        elif not await verified(uow, scope, job):
            if await repairable(job):
                coordinates[token["id"]] = job["sequence"]
                missing.append(token["id"])
            else:
                invalid.append(token["id"])
        else:
            applied.append(token["id"])
            coordinates[token["id"]] = job["sequence"]
    # Continuous visibility is reported independently of the target's exact token set.
    through, blocker = 0, None
    for job in jobs:
        if job["sequence"] != through + 1 or not await verified(uow, scope, job):
            blocker = job
            break
        through += 1
    covered = [identity for identity in applied if coordinates[identity] <= through]
    prefix_waiting = [identity for identity in applied if identity not in covered]
    target_through = max(coordinates.values(), default=0)
    prefix_failed = (
        blocker is not None
        and blocker["sequence"] <= target_through
        and blocker["status"] in {"dead", "cancelled"}
    )
    prefix_invalid = (
        blocker is not None
        and blocker["sequence"] <= target_through
        and blocker["status"] == "completed"
        and not await repairable(blocker)
    )
    state = (
        "blocked"
        if invalid or prefix_invalid
        else "failed"
        if failed or prefix_failed
        else "processing"
        if missing or prefix_waiting
        else "reached"
    )
    retired = not is_current and state != "reached"
    if retired:
        state = "blocked"
    return {
        **result,
        "index_stream": stream,
        "index_stream_current": is_current,
        "state": state,
        "index_visible": state == "reached",
        "coverage_mode": "continuous_index",
        "index_status_version": versions,
        "prefix_blocked_at": blocker["sequence"] if blocker else None,
        "applied_publication_ids": applied,
        "covered_publication_ids": covered,
        "uncovered_publication_ids": [*missing, *failed, *invalid, *prefix_waiting],
        "target_visible_through": target_through,
        "continuous_visible_through": through,
        **(
            {"reason": "index_stream_retired"}
            if retired
            else {"reason": "index_proof_unavailable"}
            if invalid or prefix_invalid
            else {}
        ),
    }


class CandidateIndexQueue:
    """Adapter to BoundedWorker; no additional scheduler or source body checkpoint."""

    def __init__(
        self, repository, scope, channel, *, clock=utc_now, max_attempts=3, retry_seconds=2
    ):
        if not isinstance(channel, CandidateIndexChannel):
            raise TypeError("expected CandidateIndexChannel")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("invalid max_attempts")
        if type(retry_seconds) is not int or not 0 <= retry_seconds <= 3600:
            raise ValueError("invalid retry_seconds")
        self.repository, self.scope, self.channel = repository, scope, channel
        self.clock, self.max_attempts, self.retry_seconds = clock, max_attempts, retry_seconds

    async def active_key(self, uow):
        from .index_recovery import active_stream, physical_channel

        return physical_channel(self.channel, await active_stream(uow, self.scope, self.channel))

    async def live(self, uow, job):
        if job is None or job["epoch"] != await uow.retention_epoch(self.scope):
            return False
        source = await uow.get_source_event(self.scope, job["event_id"])
        return source is not None and await source_is_current(uow, source)

    async def checked(self, uow, identity, token, *, completed=False):
        await DurableReceiver._check_support(uow, self.scope)
        epoch = await uow.retention_epoch(self.scope)
        job = await uow.index_job_get(self.scope, await self.active_key(uow), epoch, identity)
        if (
            not await self.live(uow, job)
            or job.get("lease_token") != token
            or job["status"] not in ({"running", "completed"} if completed else {"running"})
        ):
            raise stale()
        if job["status"] == "running" and datetime.fromisoformat(job["lease_until"]) <= _time(
            self.clock()
        ):
            raise stale()
        return job

    async def claim(self, worker_id, *, lease_seconds):
        _identity(worker_id)
        if type(lease_seconds) is not int or not 5 <= lease_seconds <= 86400:
            raise ValueError("invalid lease duration")
        async with self.repository.unit_of_work() as uow:
            await DurableReceiver._check_support(uow, self.scope)
            epoch = await uow.retention_epoch(self.scope)
            for job in await uow.index_jobs(self.scope, await self.active_key(uow), epoch):
                if job["status"] not in {"pending", "running", "retry_wait"}:
                    continue
                if not await self.live(uow, job):
                    job.update(status="cancelled")
                    job.pop("lease_token", None)
                    await uow.index_job_put(self.scope, job)
                    continue
                now = _time(self.clock())
                if (
                    job["status"] == "running"
                    and datetime.fromisoformat(job["lease_until"]) > now
                    or job.get("next_attempt_at")
                    and datetime.fromisoformat(job["next_attempt_at"]) > now
                ):
                    continue
                if job["attempts"] >= self.max_attempts:
                    job["status"] = "dead"
                    await uow.index_job_put(self.scope, job)
                    continue
                until = now + timedelta(seconds=lease_seconds)
                job.update(
                    status="running",
                    attempts=job["attempts"] + 1,
                    lease_token=token_urlsafe(32),
                    lease_until=until.isoformat(),
                )
                await uow.index_job_put(self.scope, job)
                task = WorkerTask(
                    job["token"]["id"],
                    job["token"]["id"],
                    self.scope,
                    "memory.index",
                    {"fence": job["lease_token"]},
                    WorkerTaskStatus.LEASED,
                    job["attempts"],
                    self.max_attempts,
                    now,
                    datetime.fromisoformat(job["created_at"]),
                    now,
                    worker_id,
                    until,
                )
                return WorkerLease(task, job["lease_token"])
        return None

    async def checkpoint(self, lease, value):
        raise ValueError("candidate locator does not accept checkpoints")

    async def complete(self, lease):
        async with self.repository.unit_of_work() as uow:
            job = await self.checked(uow, lease.task.id, lease.token, completed=True)
            if not receipt_valid(job):
                raise RetentionError("index_publication_missing")

    async def fail(self, lease, error):
        async with self.repository.unit_of_work() as uow:
            job = await self.checked(uow, lease.task.id, lease.token, completed=True)
            if job["status"] == "completed":
                return
            job.update(
                status="dead" if job["attempts"] >= self.max_attempts else "retry_wait",
                next_attempt_at=(
                    _time(self.clock()) + timedelta(seconds=self.retry_seconds)
                ).isoformat(),
                last_error_code="index_processing_failed",
            )
            await uow.index_job_put(self.scope, job)

    async def apply(self, task, checkpoint):
        if task.scope != self.scope:
            raise stale()
        async with self.repository.unit_of_work() as uow:
            job = await self.checked(uow, task.id, task.payload["fence"])
            request = await uow.retention_get(self.scope, "request", job["request_id"])
            from .readiness import valid_manifest

            if (
                request is None
                or not valid_manifest(self.scope, request)
                or job["token"] not in request["publication_manifest"]["publication_commit_tokens"]
                or request["publication_manifest"].get("index_channel") != self.channel.payload()
                or job["dispositions"] != token_dispositions(request, job["token"])
            ):
                raise RetentionError("index_publication_conflict")
            applied = []
            for disposition in job["dispositions"]:
                identity = disposition["candidate_id"]
                document = await current_document(uow, self.scope, identity)
                await uow.index_document_put(self.scope, job["channel"], identity, document)
                applied.append({"candidate_id": identity, "document": document})
            job.update(status="completed", applied=applied, proof=proof(job, applied))
            await uow.index_job_put(self.scope, job)

    async def lookup(self, slot_key, *, limit=128):
        """Identity candidates only; callers still use qualified temporal projection."""
        _identity(slot_key)
        if type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError("invalid index lookup limit")
        async with self.repository.unit_of_work() as uow:
            await DurableReceiver._check_support(uow, self.scope)
            documents = await uow.index_lookup(
                self.scope, await self.active_key(uow), slot_key, limit + 1
            )
            if len(documents) > limit:
                raise RetentionError("index_lookup_capacity")
            return tuple(
                [
                    d
                    for d in documents
                    if d == await current_document(uow, self.scope, d["candidate_id"])
                ]
            )
