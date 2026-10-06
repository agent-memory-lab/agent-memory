"""Opt-in durable L0 reception, without running an extractor or publishing facts.

The request ledger is a transactional outbox for a future execution adapter,
not a second worker scheduler. Trusted hosts issue tickets before reception;
neither a ticket nor a queued receipt grants model-dispatch authority.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from secrets import token_urlsafe

from ..domain import MemoryEvent, MemoryScope, utc_now
from ..lifecycle import is_memory_context
from ..ports import RetentionRepository
from ..serialization import to_jsonable

RETAIN_SCHEMA = "durable-receive/1"
_RESERVED = "_retention"


class RetentionError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _identity(value: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 256 or not value.strip():
        raise RetentionError("invalid_identity")
    return value


def _hash(value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise RetentionError("invalid_configuration_hash")
    return value


def _time(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise RetentionError("timezone_required")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class AdmissionTicket:
    request_id: str
    token: str
    epoch: int
    expires_at: datetime

    def __post_init__(self):
        _identity(self.request_id)
        _identity(self.token)
        if type(self.epoch) is not int or self.epoch < 0:
            raise RetentionError("invalid_epoch")
        object.__setattr__(self, "expires_at", _time(self.expires_at))


@dataclass(frozen=True, slots=True)
class RetentionReceipt:
    request_id: str
    source_event_id: str
    epoch: int
    status: str
    received_at: datetime
    duplicate: bool = False
    schema: str = RETAIN_SCHEMA

    def __post_init__(self):
        _identity(self.request_id)
        _identity(self.source_event_id)
        if type(self.epoch) is not int or self.epoch < 0:
            raise RetentionError("invalid_epoch")
        if (
            self.status
            not in {
                "queued",
                "running",
                "retry_wait",
                "completed",
                "dead",
                "cancelled",
                "conflict",
                "needs_resolution",
                "superseded",
            }
            or self.schema != RETAIN_SCHEMA
        ):
            raise RetentionError("unsupported_receive_state")
        if type(self.duplicate) is not bool:
            raise RetentionError("invalid_duplicate_flag")
        object.__setattr__(self, "received_at", _time(self.received_at))


class DurableReceiver:
    """Host-only source/request acceptance. It does not implement a worker.

    Inputs must already be sanitized by the trusted capture adapter. One request
    reserves one immutable event identity and an exact processing configuration.
    Source revisions use revise(); same-source interpretation requests use ReprocessingService.
    """

    def __init__(
        self,
        repository: RetentionRepository,
        *,
        max_pending: int = 1000,
        max_tickets: int = 10_000,
        ticket_ttl_seconds: int = 300,
        clock: Callable[[], datetime] = utc_now,
    ):
        for value, maximum in (
            (max_pending, 100_000),
            (max_tickets, 1_000_000),
            (ticket_ttl_seconds, 3600),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                raise RetentionError("invalid_retention_limits")
        self.repository = repository
        self.max_pending, self.max_tickets = max_pending, max_tickets
        self.ticket_ttl_seconds, self.clock = ticket_ttl_seconds, clock

    @staticmethod
    def _input(event: MemoryEvent, producer_id: str, configuration_sha256: str):
        if not isinstance(event, MemoryEvent) or not isinstance(event.scope, MemoryScope):
            raise RetentionError("invalid_source")
        if is_memory_context(event):
            raise RetentionError("context_is_not_independent_evidence")
        if _RESERVED in event.metadata or any(
            str(key).startswith("atom_") for key in event.metadata
        ):
            raise RetentionError("reserved_retention_metadata")
        if event.event_type in {"memory.atom", "memory.atom.verification"}:
            raise RetentionError("source_is_already_an_admission_event")
        _identity(producer_id)
        _identity(event.id)
        key = _identity(event.idempotency_key or event.id)
        _hash(configuration_sha256)
        if not isinstance(event.content, str) or len(event.content) > 32_000:
            raise RetentionError("source_content_limit")
        # Freeze nested metadata before the first await. ingested_at is server
        # bookkeeping and must not make a genuine network retry a new input.
        try:
            metadata = json.loads(json.dumps(event.metadata, allow_nan=False))
        except (TypeError, ValueError) as error:
            raise RetentionError("source_metadata_must_be_json") from error
        snapshot = replace(
            event,
            metadata=metadata,
            occurred_at=_time(event.occurred_at),
            idempotency_key=key,
            content_hash="",
        )
        document = to_jsonable(snapshot)
        document.pop("ingested_at")
        encoded = json.dumps(
            {
                "source": document,
                "producer_id": producer_id,
                "configuration_sha256": configuration_sha256,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        if len(encoded.encode()) > 128_000:
            raise RetentionError("source_envelope_limit")
        return snapshot, sha256(encoded.encode()).hexdigest()

    @staticmethod
    async def _check_support(uow, scope):
        if not callable(getattr(uow, "retention_epoch", None)):
            raise NotImplementedError("provider does not support durable source reception")
        await uow.lock_admission_scope(scope)

    @staticmethod
    def _ticket(row):
        return AdmissionTicket(
            row["request_id"], row["token"], row["epoch"], datetime.fromisoformat(row["expires_at"])
        )

    @staticmethod
    def _receipt(row, duplicate=False):
        return RetentionReceipt(
            row["request_id"],
            row["event_id"],
            row["epoch"],
            row["status"],
            datetime.fromisoformat(row["received_at"]),
            duplicate,
        )

    async def issue_ticket(
        self,
        event: MemoryEvent,
        *,
        request_id: str,
        producer_id: str,
        configuration_sha256: str,
        _unit_of_work=None,
    ) -> AdmissionTicket:
        """Persist identity/hash only; never store source text before acceptance."""
        _identity(request_id)
        source, fingerprint = self._input(event, producer_id, configuration_sha256)
        context = (
            nullcontext(_unit_of_work)
            if _unit_of_work is not None
            else self.repository.unit_of_work()
        )
        async with context as uow:
            await self._check_support(uow, source.scope)
            epoch = await uow.retention_epoch(source.scope)
            row = await uow.retention_get(source.scope, "ticket", request_id)
            now = _time(self.clock())
            if row:
                if row["input_sha256"] != fingerprint:
                    raise RetentionError("request_input_conflict")
                if row["epoch"] != epoch or row["revoked"]:
                    raise RetentionError("ticket_revoked")
                # An expired identity cannot silently receive a new epoch or TTL.
                if datetime.fromisoformat(row["expires_at"]) <= now:
                    raise RetentionError("ticket_expired")
                return self._ticket(row)
            if await uow.retention_get(source.scope, "request", request_id):
                raise RetentionError("request_identity_reserved")
            if await uow.retention_identity_owner(source.scope, source.id, source.idempotency_key):
                raise RetentionError("source_identity_already_reserved")
            if await uow.retention_count(source.scope, "ticket") >= self.max_tickets:
                raise RetentionError("ticket_capacity")
            erased = getattr(uow, "source_erased", None)
            if callable(erased) and await erased(source.scope, source.id):
                raise RetentionError("source_identity_erased")
            # The existing idempotency and deletion tombstones remain authoritative.
            if await uow.find_event_by_idempotency(source.scope, source.idempotency_key):
                raise RetentionError("source_already_exists")
            row = dict(
                request_id=request_id,
                token=token_urlsafe(32),
                epoch=epoch,
                event_id=source.id,
                idempotency_key=source.idempotency_key,
                input_sha256=fingerprint,
                configuration_sha256=configuration_sha256,
                producer_id=producer_id,
                revoked=False,
                expires_at=(now + timedelta(seconds=self.ticket_ttl_seconds)).isoformat(),
            )
            await uow.retention_insert(source.scope, "ticket", request_id, row)
            return self._ticket(row)

    async def submit(
        self,
        event: MemoryEvent,
        *,
        ticket: AdmissionTicket,
        producer_id: str,
        configuration_sha256: str,
        _unit_of_work=None,
        _revision=None,
    ) -> RetentionReceipt:
        source, fingerprint = self._input(event, producer_id, configuration_sha256)
        if not isinstance(ticket, AdmissionTicket):
            raise RetentionError("invalid_ticket")
        context = (
            nullcontext(_unit_of_work)
            if _unit_of_work is not None
            else self.repository.unit_of_work()
        )
        async with context as uow:
            await self._check_support(uow, source.scope)
            row = await uow.retention_get(source.scope, "ticket", ticket.request_id)
            if row is None or self._ticket(row) != ticket:
                raise RetentionError("invalid_ticket")
            if row["revoked"] or row["epoch"] != await uow.retention_epoch(source.scope):
                raise RetentionError("ticket_revoked")
            if row["input_sha256"] != fingerprint:
                raise RetentionError("request_input_conflict")
            previous = await uow.retention_get(source.scope, "request", ticket.request_id)
            if previous:
                if previous["status"] == "cancelled" or not await uow.events_exist(
                    source.scope, (previous["event_id"],)
                ):
                    raise RetentionError("source_unavailable")
                # A committed result survives ticket TTL; never dispatch or write again.
                return self._receipt(previous, duplicate=True)
            now = _time(self.clock())
            if now >= ticket.expires_at:
                raise RetentionError("ticket_expired")
            if await uow.retention_count(source.scope, "request") >= self.max_pending:
                raise RetentionError("pending_capacity")
            if await uow.find_event_by_idempotency(source.scope, source.idempotency_key):
                raise RetentionError("source_already_exists")
            # memory.atom is protected from legacy recall/consolidation promotion.
            # Original content, occurrence time and capture annotations stay intact.
            retained = replace(
                source,
                event_type="memory.atom",
                ingested_at=now,
                metadata={
                    **source.metadata,
                    _RESERVED: {
                        "schema": RETAIN_SCHEMA,
                        "request_id": ticket.request_id,
                        "original_event_type": source.event_type,
                        "input_sha256": fingerprint,
                        "producer_id": producer_id,
                        **(_revision or {"document_id": source.id, "revision": 1}),
                    },
                },
                content_hash="",
            )
            await uow.append_event(retained)
            request = dict(
                request_id=ticket.request_id,
                event_id=source.id,
                epoch=row["epoch"],
                configuration_sha256=configuration_sha256,
                idempotency_key=source.idempotency_key,
                status="queued",
                received_at=now.isoformat(),
                schema=RETAIN_SCHEMA,
            )
            from .readiness import begin

            begin(source.scope, request)
            await uow.retention_insert(source.scope, "request", ticket.request_id, request)
            if _revision is None:
                from .source_revisions import document_head

                await document_head(uow, retained)
            return self._receipt(request)

    async def revise(self, event, **options):
        from .source_revisions import revise

        return await revise(self, event, **options)

    async def status(self, scope: MemoryScope, request_id: str) -> RetentionReceipt | None:
        _identity(request_id)
        async with self.repository.unit_of_work() as uow:
            await self._check_support(uow, scope)
            row = await uow.retention_get(scope, "request", request_id)
            if row is None:
                return None
            if row["status"] != "cancelled" and (
                row["epoch"] != await uow.retention_epoch(scope)
                or not await uow.events_exist(scope, (row["event_id"],))
            ):
                row = {**row, "status": "cancelled"}
            return self._receipt(row)
