"""Negotiated sequence disposition: cancellation resolves a gap without receiving a source."""

from ..operations.retention import RetentionError, _identity


def sequence_key(producer, scope, session, sequence):
    return producer._request_key(scope, session, sequence)


def cursor(row):
    return {
        "schema": "producer-disposition/1",
        "received_through": row.get("received_through", 0),
        "settled_through": row.get("settled_through", 0),
        "settled_after_gap": row.get("settled_after_gap", []),
        "received_count": row.get("received_count", 0),
        "cancelled_count": row.get("cancelled_count", 0),
    }


async def record(producer, uow, scope, session, row, sequence, source_id, disposition):
    key = sequence_key(producer, scope, session, sequence)
    previous = await uow.delivery_get(scope, "sequence", key)
    if previous:
        if previous["source_event_id"] != source_id:
            raise RetentionError("sequence_input_conflict")
        if previous["disposition"] == "cancelled" and disposition == "received":
            raise RetentionError("sequence_cancelled")
        return previous
    if await uow.delivery_count(scope, "sequence") >= 100_000:
        raise RetentionError("sequence_capacity")
    value = {"sequence": sequence, "source_event_id": source_id, "disposition": disposition}
    await uow.delivery_insert(scope, "sequence", key, value)
    field = disposition + "_count"
    row[field] = row.get(field, 0) + 1
    pending = set(row.get("settled_after_gap", []))
    pending.add(sequence)
    through = row.get("settled_through", 0)
    while through + 1 in pending:
        through += 1
        pending.remove(through)
    row.update(settled_through=through, settled_after_gap=sorted(pending))
    received = row.get("received_through", 0)
    while received < through:
        next_row = await uow.delivery_get(
            scope, "sequence", sequence_key(producer, scope, session, received + 1)
        )
        if next_row is None:
            raise RetentionError("sequence_history_unavailable")
        if next_row["disposition"] != "received":
            break
        received += 1
    row["received_through"] = received
    await uow.producer_put(scope, session.producer_id, row)
    return value


async def cancel(producer, scope, session, *, sequence, source_event_id, actor):
    _identity(source_event_id)
    key = sequence_key(producer, scope, session, sequence)
    async with producer.receiver.repository.unit_of_work() as uow:
        row = await producer._check(uow, scope, session, actor)
        if not row.get("sequence_dispositions"):
            raise RetentionError("sequence_dispositions_unsupported")
        head = await uow.purge_head(scope)
        if row.get("purged_through", 0) > head:
            raise RetentionError("purge_history_unavailable")
        if row.get("purged_through", 0) < head:
            raise RetentionError("producer_purge_required")
        if sequence > row.get("settled_through", 0) + producer.max_gap:
            raise RetentionError("producer_gap_limit")
        if not await uow.source_erased(scope, source_event_id):
            raise RetentionError("sequence_cancellation_unproven")
        previous = await uow.delivery_get(scope, "sequence", key)
        if previous is None and sequence <= row.get("settled_through", 0):
            raise RetentionError("sequence_history_unavailable")
        value = await record(
            producer, uow, scope, session, row, sequence, source_event_id, "cancelled"
        )
        # An already received source stays received even if its processing was later erased.
        return {
            "schema": "producer-cancellation/1",
            "producer_id": session.producer_id,
            "epoch": session.epoch,
            "scope_key": scope.partition_key(),
            **value,
            "cursor": cursor(row),
        }
