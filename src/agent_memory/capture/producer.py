"""Host-owned producer sessions and contiguous acknowledgments for durable append."""

import json
from dataclasses import dataclass
from hashlib import sha256
from secrets import token_urlsafe

from ..operations.retention import DurableReceiver, RetentionError, _hash, _identity
from ..serialization import to_jsonable


@dataclass(frozen=True, slots=True)
class ProducerSession:
    producer_id: str
    epoch: int
    token: str
    configuration_sha256: str

    def __post_init__(self):
        _identity(self.producer_id)
        _identity(self.token)
        _hash(self.configuration_sha256)
        if type(self.epoch) is not int or self.epoch < 0:
            raise RetentionError("invalid_epoch")


class DurableProducer:
    def __init__(self, receiver: DurableReceiver, *, max_gap=1024):
        if type(max_gap) is not int or not 1 <= max_gap <= 10_000:
            raise ValueError("max_gap must be between 1 and 10000")
        self.receiver, self.max_gap = receiver, max_gap

    async def open(self, scope, *, producer_id, actor, configuration_sha256):
        """Trusted host setup, never a model-callable registration operation."""
        _identity(producer_id)
        _identity(actor)
        _hash(configuration_sha256)
        async with self.receiver.repository.unit_of_work() as uow:
            await self.receiver._check_support(uow, scope)
            epoch = await uow.retention_epoch(scope)
            row = await uow.producer_get(scope, producer_id)
            if row is None:
                row = dict(
                    producer_id=producer_id,
                    epoch=epoch,
                    token=token_urlsafe(32),
                    configuration_sha256=configuration_sha256,
                    actor=actor,
                    acked_through=0,
                    received=[],
                )
                await uow.producer_put(scope, producer_id, row)
            if row["epoch"] != epoch:
                raise RetentionError("producer_revoked")
            if row["actor"] != actor or row["configuration_sha256"] != configuration_sha256:
                raise RetentionError("producer_configuration_conflict")
            return ProducerSession(
                **{name: row[name] for name in ProducerSession.__dataclass_fields__}
            )

    async def _check(self, uow, scope, session, actor):
        if not isinstance(session, ProducerSession):
            raise RetentionError("invalid_producer")
        await self.receiver._check_support(uow, scope)
        row = await uow.producer_get(scope, session.producer_id)
        if (
            row is None
            or any(row[k] != v for k, v in to_jsonable(session).items())
            or row["actor"] != actor
        ):
            raise RetentionError("invalid_producer")
        if row["epoch"] != await uow.retention_epoch(scope):
            raise RetentionError("producer_revoked")
        return row

    async def append(self, event, session, *, sequence, actor):
        if type(sequence) is not int or not 1 <= sequence <= 2**53:
            raise RetentionError("invalid_producer_sequence")
        if not isinstance(session, ProducerSession):
            raise RetentionError("invalid_producer")
        if event.actor != actor:
            raise RetentionError("producer_actor_mismatch")
        key = self._request_key(event.scope, session, sequence)
        async with self.receiver.repository.unit_of_work() as uow:
            row = await self._check(uow, event.scope, session, actor)
            if sequence > row["acked_through"] + self.max_gap:
                raise RetentionError("producer_gap_limit")
            saved = await uow.retention_get(event.scope, "ticket", key)
            ticket = (
                self.receiver._ticket(saved)
                if saved
                else await self.receiver.issue_ticket(
                    event,
                    request_id=key,
                    producer_id=session.producer_id,
                    configuration_sha256=session.configuration_sha256,
                    _unit_of_work=uow,
                )
            )
            receipt = await self.receiver.submit(
                event,
                ticket=ticket,
                producer_id=session.producer_id,
                configuration_sha256=session.configuration_sha256,
                _unit_of_work=uow,
            )
            received = set(row["received"])
            if sequence > row["acked_through"]:
                received.add(sequence)
            while row["acked_through"] + 1 in received:
                row["acked_through"] += 1
                received.remove(row["acked_through"])
            row["received"] = sorted(received)
            await uow.producer_put(event.scope, session.producer_id, row)
            return {
                "receipt": to_jsonable(receipt),
                "sequence": sequence,
                "acked_through": row["acked_through"],
                "received_after_gap": row["received"],
            }

    async def cursor(self, scope, session, *, actor):
        async with self.receiver.repository.unit_of_work() as uow:
            row = await self._check(uow, scope, session, actor)
            return {"acked_through": row["acked_through"], "received_after_gap": row["received"]}

    @staticmethod
    def _request_key(scope, session, sequence):
        if type(sequence) is not int or not 1 <= sequence <= 2**53:
            raise RetentionError("invalid_producer_sequence")
        return (
            "append:"
            + sha256(
                json.dumps(
                    [
                        scope.partition_key(),
                        session.producer_id,
                        session.epoch,
                        sequence,
                    ],
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
        )

    async def status(self, scope, session, *, sequence, actor):
        async with self.receiver.repository.unit_of_work() as uow:
            await self._check(uow, scope, session, actor)
            key = self._request_key(scope, session, sequence)
            row = await uow.retention_get(scope, "request", key)
            if row is None:
                return {"sequence": sequence, "status": "not_received"}
            if row["status"] == "cancelled" or not await uow.events_exist(
                scope, (row["event_id"],)
            ):
                return {"sequence": sequence, "status": "cancelled"}
            return {
                "sequence": sequence,
                "receipt": to_jsonable(self.receiver._receipt(row)),
                "source_persisted": True,
                "l1_decided": row["status"] == "completed",
                "index_visible": "unsupported",
                "attempts": row.get("attempts", 0),
                "result": row.get("result"),
            }
