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
    def __init__(self, receiver: DurableReceiver, *, max_gap=1024, index_channel=None):
        if type(max_gap) is not int or not 1 <= max_gap <= 10_000:
            raise ValueError("max_gap must be between 1 and 10000")
        if index_channel is not None:
            from ..operations.indexing import CandidateIndexChannel

            if not isinstance(index_channel, CandidateIndexChannel):
                raise TypeError("expected CandidateIndexChannel")
        self.index_channel = index_channel
        self.receiver, self.max_gap = receiver, max_gap

    async def open(
        self,
        scope,
        *,
        producer_id,
        actor,
        configuration_sha256,
        sync_purges=False,
        sequence_dispositions=False,
    ):
        """Trusted host setup, never a model-callable registration operation."""
        if type(sync_purges) is not bool or type(sequence_dispositions) is not bool:
            raise ValueError("producer options must be boolean")
        if sequence_dispositions and not sync_purges:
            raise ValueError("sequence dispositions require purge synchronization")
        _identity(producer_id)
        _identity(actor)
        _hash(configuration_sha256)
        async with self.receiver.repository.unit_of_work() as uow:
            await self.receiver._check_support(uow, scope)
            epoch = await uow.retention_epoch(scope)
            row = await uow.producer_get(scope, producer_id)
            if row is None:
                purge_head = getattr(uow, "purge_head", None)
                if sync_purges and not callable(purge_head):
                    raise NotImplementedError(
                        "producer purge synchronization requires purge storage"
                    )
                if sequence_dispositions and not callable(getattr(uow, "delivery_get", None)):
                    raise RetentionError("sequence_dispositions_unsupported")
                row = dict(
                    producer_id=producer_id,
                    epoch=epoch,
                    token=token_urlsafe(32),
                    configuration_sha256=configuration_sha256,
                    actor=actor,
                    acked_through=0,
                    received=[],
                    sync_purges=sync_purges,
                    sequence_dispositions=sequence_dispositions,
                    purged_through=await purge_head(scope) if callable(purge_head) else 0,
                )
                await uow.producer_put(scope, producer_id, row)
            if row["epoch"] != epoch:
                raise RetentionError("producer_revoked")
            if (
                row["actor"] != actor
                or row["configuration_sha256"] != configuration_sha256
                or row.get("sync_purges", False) != sync_purges
                or row.get("sequence_dispositions", False) != sequence_dispositions
            ):
                raise RetentionError("producer_configuration_conflict")
            return ProducerSession(
                **{name: row[name] for name in ProducerSession.__dataclass_fields__}
            )

    async def _check(self, uow, scope, session, actor, *, allow_revoked=False):
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
        if not allow_revoked and row["epoch"] != await uow.retention_epoch(scope):
            raise RetentionError("producer_revoked")
        return row

    async def append(self, event, session, *, sequence, actor, _revision=None):
        if type(sequence) is not int or not 1 <= sequence <= 2**53:
            raise RetentionError("invalid_producer_sequence")
        if not isinstance(session, ProducerSession):
            raise RetentionError("invalid_producer")
        if event.actor != actor:
            raise RetentionError("producer_actor_mismatch")
        key = self._request_key(event.scope, session, sequence)
        async with self.receiver.repository.unit_of_work() as uow:
            row = await self._check(uow, event.scope, session, actor)
            if row.get("sync_purges"):
                head = await uow.purge_head(event.scope)
                if row.get("purged_through", 0) > head:
                    raise RetentionError("purge_history_unavailable")
                if row.get("purged_through", 0) < head:
                    raise RetentionError("producer_purge_required")
            floor = (
                row.get("settled_through", 0)
                if row.get("sequence_dispositions")
                else row["acked_through"]
            )
            if sequence > floor + self.max_gap:
                raise RetentionError("producer_gap_limit")
            if row.get("sequence_dispositions"):
                disposition = await uow.delivery_get(event.scope, "sequence", key)
                if disposition and disposition["disposition"] == "cancelled":
                    raise RetentionError("sequence_cancelled")
                if disposition is None and sequence <= floor:
                    raise RetentionError("sequence_history_unavailable")
            if _revision is not None:
                receipt = await self.receiver.revise(
                    event,
                    **_revision,
                    request_id=key,
                    producer_id=session.producer_id,
                    configuration_sha256=session.configuration_sha256,
                    _unit_of_work=uow,
                )
            else:
                previous = await uow.retention_get(event.scope, "request", key)
                if previous is not None and previous.get("revision_input") is not None:
                    raise RetentionError("request_input_conflict")
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
            if row.get("sequence_dispositions"):
                from .dispositions import cursor, record

                await record(self, uow, event.scope, session, row, sequence, event.id, "received")
                return {
                    "receipt": to_jsonable(receipt),
                    "sequence": sequence,
                    "cursor": cursor(row),
                }
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

    async def revise(self, event, session, *, sequence, actor, base_event_id, expected_revision):
        return await self.append(
            event,
            session,
            sequence=sequence,
            actor=actor,
            _revision={"base_event_id": base_event_id, "expected_revision": expected_revision},
        )

    async def cursor(self, scope, session, *, actor):
        async with self.receiver.repository.unit_of_work() as uow:
            row = await self._check(uow, scope, session, actor)
            if row.get("sequence_dispositions"):
                from .dispositions import cursor

                return cursor(row)
            return {"acked_through": row["acked_through"], "received_after_gap": row["received"]}

    async def contracts(self, scope, session, *, actor):
        async with self.receiver.repository.unit_of_work() as uow:
            row = await self._check(uow, scope, session, actor)
            supported = callable(getattr(uow, "delivery_get", None))
            indexed = self.index_channel is not None and callable(getattr(uow, "index_job_get", None))
            return {
                "schema": "durable-contracts/1",
                "staged_readiness": supported,
                "reprocessing_targets": supported,
                "target_schemas": ["durable-target/1", "durable-target/2"] if supported else [],
                "producer_id": session.producer_id,
                "epoch": session.epoch,
                "scope_key": scope.partition_key(),
                "supported_stages": (
                    ["source_persisted", "l1_decided"] + (["index_visible"] if indexed else [])
                ) if supported else [],
                "index_visible": "candidate-locator/1" if indexed else "unsupported",
                **({"index_channel": self.index_channel.payload()} if indexed else {}),
                "target_limit": 128,
                "sync_purges": row.get("sync_purges", False),
                "sequence_dispositions": row.get("sequence_dispositions", False),
                "cursor_schema": "producer-disposition/1"
                if row.get("sequence_dispositions")
                else "legacy",
            }

    async def cancel_sequence(self, scope, session, *, sequence, source_event_id, actor):
        from .dispositions import cancel

        return await cancel(
            self, scope, session, sequence=sequence, source_event_id=source_event_id, actor=actor
        )

    async def freeze_target(self, scope, session, *, sequences, actor):
        from ..operations.readiness import DurableReadiness

        return await DurableReadiness(self).freeze(scope, session, sequences=sequences, actor=actor)

    async def freeze_reprocessing_target(self, scope, session, *, request_ids, actor):
        from ..operations.readiness import DurableReadiness

        return await DurableReadiness(self).freeze_reprocessing(
            scope, session, request_ids=request_ids, actor=actor
        )

    async def readiness(self, scope, session, *, target_id, stage, actor):
        from ..operations.readiness import DurableReadiness

        return await DurableReadiness(self).status(
            scope, session, target_id=target_id, stage=stage, actor=actor
        )

    async def purge_sync(self, scope, session, *, actor, after=0, limit=128):
        from .purge import synchronize

        return await synchronize(self, scope, session, actor=actor, after=after, limit=limit)

    async def purge_ack(self, scope, session, *, actor, through):
        from .purge import acknowledge

        return await acknowledge(self, scope, session, actor=actor, through=through)

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
            from ..operations.source_revisions import source_is_current

            source = await uow.get_source_event(scope, row["event_id"])
            current = source is not None and await source_is_current(uow, source)
            head = await uow.retention_head_get(scope, "interpretation", row["event_id"])
            interpretation_current = (
                current and head is not None and head["payload"]["request_id"] == key
            )
            return {
                "sequence": sequence,
                "source_current": current,
                "interpretation_current": interpretation_current,
                "receipt": to_jsonable(self.receiver._receipt(row)),
                "source_persisted": True,
                "l1_decided": row["status"] == "completed",
                "index_visible": "unsupported",
                "attempts": row.get("attempts", 0),
                "result": row.get("result") if interpretation_current else None,
            }
