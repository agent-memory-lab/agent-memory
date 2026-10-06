"""Host-scoped deletion synchronization; acknowledgments attest client cleanup only."""

from ..operations.retention import RetentionError


async def synchronize(producer, scope, session, *, actor, after=0, limit=128):
    if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 128:
        raise RetentionError("invalid_purge_cursor")
    async with producer.receiver.repository.unit_of_work() as uow:
        row = await producer._check(uow, scope, session, actor, allow_revoked=True)
        head = await uow.purge_head(scope)
        if after > head or row.get("purged_through", 0) > head:
            raise RetentionError("purge_history_unavailable")
        entries = await uow.purge_page(scope, after, limit)
        if len(entries) != min(limit, head - after) or any(
            entry["cursor"] != index for index, entry in enumerate(entries, after + 1)
        ):
            raise RetentionError("purge_history_unavailable")
        through = entries[-1]["cursor"] if entries else after
        epoch = await uow.retention_epoch(scope)
        return {
            "schema": "producer-purge/1",
            "producer_id": session.producer_id,
            "session_epoch": session.epoch,
            "scope_epoch": epoch,
            "scope_key": scope.partition_key(),
            "after": after,
            "through": through,
            "head": head,
            "closed": through == head,
            "entries": list(entries),
            "scope_revoked": epoch != session.epoch,
        }


async def acknowledge(producer, scope, session, *, actor, through):
    if type(through) is not int or through < 0:
        raise RetentionError("invalid_purge_cursor")
    async with producer.receiver.repository.unit_of_work() as uow:
        row = await producer._check(uow, scope, session, actor, allow_revoked=True)
        head = await uow.purge_head(scope)
        if row.get("purged_through", 0) > head:
            raise RetentionError("purge_history_unavailable")
        if through > head:
            raise RetentionError("invalid_purge_cursor")
        row["purged_through"] = max(through, row.get("purged_through", 0))
        await uow.producer_put(scope, session.producer_id, row)
        return {
            "producer_id": session.producer_id,
            "session_epoch": session.epoch,
            "purged_through": row["purged_through"],
            "head": head,
            "closed": row["purged_through"] == head,
        }
