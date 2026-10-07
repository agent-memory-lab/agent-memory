"""Explicit local index repair and atomic stream rollover, never capture epoch replay."""

from ..domain import MemoryScope, utc_now
from .indexing import CandidateIndexChannel, current_document, digest, proof, verified
from .publication_manifest import token_dispositions, token_time
from .readiness import valid_manifest
from .retention import DurableReceiver, RetentionError, _hash, _identity, _time
from .source_revisions import source_is_current


def descriptor(scope, channel, epoch, generation):
    return {
        "schema": "candidate-index-stream/1",
        "scope_key": scope.partition_key(),
        "channel": channel.key,
        "epoch": epoch,
        "generation": generation,
        "id": "index-stream:" + digest([scope.partition_key(), channel.key, epoch, generation]),
    }


def physical_channel(channel, stream):
    return channel.key if stream["generation"] == 0 else channel.key + ":" + stream["id"]


async def active_stream(uow, scope, channel):
    epoch = await uow.retention_epoch(scope)
    if not callable(getattr(uow, "index_recovery_get", None)):
        return descriptor(scope, channel, epoch, 0)
    head = await uow.index_recovery_get(scope, channel.key, epoch, "head", "head")
    if head is None:
        return descriptor(scope, channel, epoch, 0)
    stream = head.get("stream")
    if (
        not isinstance(stream, dict)
        or type(stream.get("generation")) is not int
        or not 1 <= stream["generation"] <= 32
        or stream != descriptor(scope, channel, epoch, stream["generation"])
    ):
        raise RetentionError("index_stream_history_unavailable")
    record = await uow.index_recovery_get(scope, channel.key, epoch, "stream", stream["id"])
    if not record or record.get("stream") != stream or record.get("status") != "active":
        raise RetentionError("index_stream_history_unavailable")
    return stream


async def frozen_stream(uow, scope, channel, epoch, selected):
    """Legacy absent binding always means stream 0, regardless of the current head."""
    stream = selected if selected is not None else descriptor(scope, channel, epoch, 0)
    if (
        not isinstance(stream, dict)
        or type(stream.get("generation")) is not int
        or not 0 <= stream["generation"] <= 32
        or stream != descriptor(scope, channel, epoch, stream["generation"])
    ):
        raise RetentionError("index_stream_invalid")
    current = await active_stream(uow, scope, channel)
    if stream["generation"]:
        if not callable(getattr(uow, "index_recovery_get", None)):
            raise RetentionError("index_stream_history_unavailable")
        record = await uow.index_recovery_get(scope, channel.key, epoch, "stream", stream["id"])
        if (
            not record
            or record.get("stream") != stream
            or record.get("status") not in {"active", "retired"}
        ):
            raise RetentionError("index_stream_history_unavailable")
    return stream, stream == current


async def published_request(uow, scope, channel, token):
    if not isinstance(token, dict) or not isinstance(token.get("generation"), str):
        raise RetentionError("index_publication_conflict")
    row = await uow.retention_get(scope, "request", token["generation"])
    if (
        not row
        or not valid_manifest(scope, row)
        or token not in row["publication_manifest"]["publication_commit_tokens"]
        or row["publication_manifest"].get("index_channel") != channel.payload()
    ):
        raise RetentionError("index_publication_conflict")
    source = await uow.get_source_event(scope, row["event_id"])
    if source is None:
        raise RetentionError("source_unavailable")
    if not await source_is_current(uow, source):
        raise RetentionError("source_revision_changed")
    return row


async def write_projection(uow, scope, channel_key, dispositions):
    applied = []
    for disposition in dispositions:
        candidate_id = disposition["candidate_id"]
        document = await current_document(uow, scope, candidate_id)
        await uow.index_document_put(scope, channel_key, candidate_id, document)
        applied.append({"candidate_id": candidate_id, "document": document})
    return applied


class CandidateIndexRecovery:
    """Host-bound operator authority; not exposed as a model SDK/MCP operation."""

    def __init__(self, repository, scope, channel, *, actor, clock=utc_now):
        if not isinstance(scope, MemoryScope) or not isinstance(channel, CandidateIndexChannel):
            raise TypeError("expected exact scope and candidate channel")
        self.repository, self.scope, self.channel = repository, scope, channel
        self.actor, self.clock = _identity(actor), clock

    async def _open(self, uow):
        await DurableReceiver._check_support(uow, self.scope)
        if any(
            not callable(getattr(uow, name, None))
            for name in (
                "index_recovery_get",
                "index_recovery_put",
                "index_recovery_count",
                "index_job_position",
                "retention_requests",
            )
        ):
            raise RetentionError("index_recovery_unsupported")
        return await active_stream(uow, self.scope, self.channel)

    async def inspect(self, publication_id):
        _identity(publication_id)
        async with self.repository.unit_of_work() as uow:
            stream = await self._open(uow)
            job = await uow.index_job_get(
                self.scope, physical_channel(self.channel, stream), stream["epoch"], publication_id
            )
            if job is None:
                raise RetentionError("index_publication_missing")
            return {
                "stream": stream,
                "publication_id": publication_id,
                "sequence": job["sequence"],
                "status": job["status"],
                "job_sha256": digest(job),
            }

    async def repair(self, publication_id, *, recovery_id, expected_job_sha256, stream, reason):
        _identity(publication_id)
        _identity(recovery_id)
        _identity(reason)
        _hash(expected_job_sha256)
        async with self.repository.unit_of_work() as uow:
            current = await self._open(uow)
            if stream != current:
                raise RetentionError("index_stream_retired")
            key, epoch = physical_channel(self.channel, current), current["epoch"]
            job = await uow.index_job_get(self.scope, key, epoch, publication_id)
            if job is None:
                raise RetentionError("index_publication_missing")
            row = await published_request(uow, self.scope, self.channel, job["token"])
            if (
                job["channel"] != key
                or job["epoch"] != epoch
                or job["token"]["id"] != publication_id
                or job["request_id"] != row["request_id"]
                or job["event_id"] != row["event_id"]
            ):
                raise RetentionError("index_publication_conflict")
            if job["sequence"] != await uow.index_job_position(
                self.scope, key, epoch, publication_id
            ):
                raise RetentionError("index_sequence_conflict")
            operation = "index-repair:" + digest(
                [self.scope.partition_key(), self.channel.key, epoch, recovery_id]
            )
            contract = digest([stream, publication_id, expected_job_sha256, reason, self.actor])
            existing = await uow.index_recovery_get(
                self.scope, self.channel.key, epoch, "repair", operation
            )
            if existing is not None:
                if existing["contract_sha256"] != contract:
                    raise RetentionError("index_recovery_idempotency_conflict")
                return existing
            if digest(job) != expected_job_sha256:
                raise RetentionError("index_repair_head_changed")
            if job["status"] not in {"dead", "cancelled", "completed"}:
                raise RetentionError("index_repair_active_job")
            if job["status"] == "completed" and await verified(uow, self.scope, job):
                raise RetentionError("index_repair_not_needed")
            if (
                await uow.index_recovery_count(self.scope, self.channel.key, epoch, "repair")
                >= 1000
            ):
                raise RetentionError("index_repair_capacity")
            job["dispositions"] = token_dispositions(row, job["token"])
            applied = await write_projection(uow, self.scope, key, job["dispositions"])
            job.update(status="completed", applied=applied, proof=proof(job, applied))
            for field in ("lease_token", "lease_until", "next_attempt_at", "last_error_code"):
                job.pop(field, None)
            job["repair_id"] = operation
            receipt = {
                "schema": "candidate-index-repair/1",
                "id": operation,
                "stream": stream,
                "publication_id": publication_id,
                "sequence": job["sequence"],
                "before_sha256": expected_job_sha256,
                "after_sha256": digest(job),
                "contract_sha256": contract,
                "actor": self.actor,
                "reason": reason,
                "committed_at": _time(self.clock()).isoformat(),
            }
            await uow.index_job_put(self.scope, job)
            await uow.index_recovery_put(
                self.scope, self.channel.key, epoch, "repair", operation, receipt
            )
            return receipt

    async def rollover(self, *, recovery_id, expected_generation, reason):
        _identity(recovery_id)
        _identity(reason)
        if type(expected_generation) is not int or not 0 <= expected_generation < 32:
            raise RetentionError("invalid_index_stream_generation")
        async with self.repository.unit_of_work() as uow:
            parent = await self._open(uow)
            epoch = parent["epoch"]
            operation = "index-rollover:" + digest(
                [self.scope.partition_key(), self.channel.key, epoch, recovery_id]
            )
            contract = digest([expected_generation, reason, self.actor])
            existing = await uow.index_recovery_get(
                self.scope, self.channel.key, epoch, "rollover", operation
            )
            if existing is not None:
                if existing["contract_sha256"] != contract:
                    raise RetentionError("index_recovery_idempotency_conflict")
                return existing
            if parent["generation"] != expected_generation:
                raise RetentionError("index_stream_head_changed")
            stream = descriptor(self.scope, self.channel, epoch, expected_generation + 1)
            key = physical_channel(self.channel, stream)
            baseline = []
            try:
                requests = await uow.retention_requests(self.scope)
            except ValueError as error:
                raise RetentionError("index_rollover_capacity") from error
            for row in requests:
                if row["epoch"] != epoch:
                    continue
                manifest = row.get("publication_manifest", {})
                if manifest.get("index_channel") != self.channel.payload():
                    continue
                source = await uow.get_source_event(self.scope, row["event_id"])
                if source is None or not await source_is_current(uow, source):
                    continue
                if not valid_manifest(self.scope, row):
                    raise RetentionError("index_rollover_history_unavailable")
                for token in manifest["publication_commit_tokens"]:
                    baseline.append((row, token))
            if (
                len(baseline) > 256
                or len(
                    {
                        d["candidate_id"]
                        for r, token in baseline
                        for d in token_dispositions(r, token)
                    }
                )
                > 4096
            ):
                raise RetentionError("index_rollover_capacity")
            baseline.sort(
                key=lambda item: (
                    token_time(*item),
                    item[0]["request_id"],
                    item[1].get("batch_index", 0),
                    item[1]["id"],
                )
            )
            identities = []
            for sequence, (row, token) in enumerate(baseline, start=1):
                dispositions = token_dispositions(row, token)
                applied = await write_projection(uow, self.scope, key, dispositions)
                job = {
                    "schema": "index-publication/1",
                    "channel": key,
                    "epoch": epoch,
                    "token": token,
                    "sequence": sequence,
                    "request_id": row["request_id"],
                    "event_id": row["event_id"],
                    "dispositions": dispositions,
                    "status": "completed",
                    "attempts": 0,
                    "applied": applied,
                    "created_at": _time(self.clock()).isoformat(),
                    "rollover_id": operation,
                }
                job["proof"] = proof(job, applied)
                await uow.index_job_put(self.scope, job)
                identities.append(
                    {
                        "token": token,
                        "request_id": row["request_id"],
                        "event_id": row["event_id"],
                        "dispositions": dispositions,
                    }
                )
            receipt = {
                "schema": "candidate-index-rollover/1",
                "id": operation,
                "stream": stream,
                "parent": parent,
                "baseline_count": len(baseline),
                "baseline_sha256": digest(identities),
                "contract_sha256": contract,
                "actor": self.actor,
                "reason": reason,
                "committed_at": _time(self.clock()).isoformat(),
            }
            parent_record = await uow.index_recovery_get(
                self.scope, self.channel.key, epoch, "stream", parent["id"]
            )
            parent_record = {**(parent_record or {"stream": parent}), "status": "retired"}
            await uow.index_recovery_put(
                self.scope, self.channel.key, epoch, "stream", parent["id"], parent_record
            )
            await uow.index_recovery_put(
                self.scope,
                self.channel.key,
                epoch,
                "stream",
                stream["id"],
                {"stream": stream, "status": "active", "receipt": receipt},
            )
            await uow.index_recovery_put(
                self.scope, self.channel.key, epoch, "rollover", operation, receipt
            )
            # Activate only after every actual document and proof is committed by this transaction.
            await uow.index_recovery_put(
                self.scope, self.channel.key, epoch, "head", "head", {"stream": stream}
            )
            return receipt
