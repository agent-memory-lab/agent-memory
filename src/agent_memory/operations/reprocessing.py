"""Host-owned, explicit same-source processing requests with immutable head expectations."""

import json
from hashlib import sha256

from .retention import RetentionError, _hash, _identity, _time
from .source_revisions import document_head, interpretation_head, source_is_current


async def checked_records(uow, scope, source_id, expected_generation, versions=None):
    head = await uow.retention_head_get(scope, "interpretation", source_id)
    if head is None or head["generation"] != expected_generation:
        raise RetentionError("interpretation_head_changed")
    ids = head["payload"]["active_ids"]
    if len(ids) > 64:
        raise RetentionError("interpretation_capacity")
    records = []
    for identity in ids:
        row = await uow.get_admission_record(scope, identity)
        if row is None or row["payload"].get("deleted"):
            raise RetentionError("interpretation_contribution_changed")
        if versions is not None and versions.get(identity) != row["version"]:
            raise RetentionError("interpretation_contribution_changed")
        payload = row["payload"]
        if (
            payload["source_event_ids"] != [source_id]
            or payload["draft"]["change_kind"] != "replace"
            or payload.get("termination")
        ):
            raise RetentionError("interpretation_capability_unsupported")
        records.append(row)
    if versions is not None and set(versions) != set(ids):
        raise RetentionError("interpretation_contribution_changed")
    return records


class ReprocessingService:
    """Host registration binds owner and the single production stream out of band.

    Whole immutable source coverage is supported. Partial semantic units, shadow
    streams and multi-source contributions require their own activation contract.
    """

    def __init__(self, receiver, *, producer_id, actor, stream="primary"):
        _identity(producer_id)
        _identity(actor)
        if stream != "primary":
            raise RetentionError("interpretation_stream_unsupported")
        self.receiver, self.producer_id, self.actor = receiver, producer_id, actor

    async def _source(self, uow, scope, source_id):
        await self.receiver._check_support(uow, scope)
        source = await uow.get_source_event(scope, source_id)
        if source is None:
            raise RetentionError("source_unavailable")
        _, document = await document_head(uow, source)
        if document["payload"]["producer_id"] != self.producer_id or source.actor != self.actor:
            raise RetentionError("source_owner_mismatch")
        if not await source_is_current(uow, source):
            raise RetentionError("source_revision_changed")
        return source

    async def snapshot(self, scope, source_event_id):
        async with self.receiver.repository.unit_of_work() as uow:
            source = await self._source(uow, scope, source_event_id)
            head = await interpretation_head(uow, source)
            return {
                "source_event_id": source.id,
                "source_sha256": source.content_hash,
                "stream": "primary",
                "generation": head["generation"],
                "active_candidate_ids": head["payload"]["active_ids"],
                "coverage": {
                    "start": 0,
                    "end": len(source.content),
                    "source_sha256": source.content_hash,
                },
            }

    async def submit(
        self,
        scope,
        *,
        source_event_id,
        request_id,
        mode,
        configuration_sha256,
        expected_head_generation,
        coverage=None,
        allow_pending=False,
    ):
        _identity(request_id)
        _identity(source_event_id)
        _hash(configuration_sha256)
        if mode not in {"additive", "replace_interpretation"}:
            raise RetentionError("invalid_reprocessing_mode")
        if type(expected_head_generation) is not int or expected_head_generation < 1:
            raise RetentionError("invalid_interpretation_generation")
        if type(allow_pending) is not bool:
            raise RetentionError("invalid_pending_policy")
        async with self.receiver.repository.unit_of_work() as uow:
            source = await self._source(uow, scope, source_event_id)
            complete = {
                "start": 0,
                "end": len(source.content),
                "source_sha256": source.content_hash,
            }
            if coverage is not None and (
                coverage != complete
                or type(coverage.get("start")) is not int
                or type(coverage.get("end")) is not int
            ):
                raise RetentionError("partial_coverage_unsupported")
            contract = {
                "source_event_id": source.id,
                "mode": mode,
                "coverage": complete,
                "stream": "primary",
                "configuration_sha256": configuration_sha256,
                "expected_head_generation": expected_head_generation,
                "allow_pending": allow_pending,
                "producer_id": self.producer_id,
                "actor": self.actor,
            }
            fingerprint = sha256(
                json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            prior = await uow.retention_get(scope, "request", request_id)
            if prior is not None:
                if prior.get("reprocessing_fingerprint") != fingerprint:
                    raise RetentionError("request_input_conflict")
                if prior["epoch"] != await uow.retention_epoch(scope):
                    raise RetentionError("source_unavailable")
                return self.receiver._receipt(prior, duplicate=True)
            # Request IDs are global within a scope, including unsubmitted tickets.
            if await uow.retention_get(scope, "ticket", request_id):
                raise RetentionError("request_identity_reserved")
            await interpretation_head(uow, source)
            records = await checked_records(uow, scope, source.id, expected_head_generation)
            if await uow.retention_count(scope, "request") >= self.receiver.max_pending:
                raise RetentionError("pending_capacity")
            row = {
                "request_id": request_id,
                "event_id": source.id,
                "idempotency_key": source.idempotency_key,
                "epoch": await uow.retention_epoch(scope),
                "configuration_sha256": configuration_sha256,
                "status": "queued",
                "received_at": _time(self.receiver.clock()).isoformat(),
                "operation": "reprocess",
                "reprocessing": contract,
                "reprocessing_fingerprint": fingerprint,
                "base_versions": {r["id"]: r["version"] for r in records},
            }
            await uow.retention_insert(scope, "request", request_id, row)
            return self.receiver._receipt(row)
