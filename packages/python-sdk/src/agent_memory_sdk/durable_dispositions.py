"""Resolve locally purged sequence holes without resending any source body."""

import json
from hashlib import sha256


async def settle(outbox, client, *, limit=128):
    with outbox._connection() as connection:
        outbox._check_live(connection)
        scope_key = connection.execute(
            "SELECT scope_key FROM durable_sessions WHERE session_key=?", (outbox.session_key,)
        ).fetchone()[0]
        rows = connection.execute(
            "SELECT sequence,event_id FROM durable_pending WHERE session_key=? "
            "AND purged=1 AND cancel_confirmed=0 ORDER BY sequence LIMIT ?",
            (outbox.session_key, limit + 1),
        ).fetchall()
    contracts = await client.durable_contracts(outbox.session)
    if (
        not isinstance(contracts, dict)
        or contracts.get("schema") != "durable-contracts/1"
        or contracts.get("producer_id") != outbox.session["producer_id"]
        or type(contracts.get("epoch")) is not int
        or contracts["epoch"] != outbox.session["epoch"]
        or contracts.get("scope_key") != scope_key
        or contracts.get("sequence_dispositions") is not True
        or contracts.get("sync_purges") is not True
        or contracts.get("cursor_schema") != "producer-disposition/1"
    ):
        raise ValueError("sequence dispositions were not negotiated")
    for sequence, event_id in rows[:limit]:
        source_id = (
            "source:"
            + sha256(json.dumps([scope_key, event_id], separators=(",", ":")).encode()).hexdigest()
        )
        response = await client.durable_cancel_sequence(outbox.session, sequence, source_id)
        if (
            not isinstance(response, dict)
            or response.get("schema") != "producer-cancellation/1"
            or response.get("producer_id") != outbox.session["producer_id"]
            or type(response.get("epoch")) is not int
            or response["epoch"] != outbox.session["epoch"]
            or response.get("scope_key") != scope_key
            or type(response.get("sequence")) is not int
            or response["sequence"] != sequence
            or response.get("source_event_id") != source_id
            or response.get("disposition") not in {"received", "cancelled"}
        ):
            raise ValueError("invalid sequence cancellation acknowledgment")
        with outbox._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            outbox._check_live(connection)
            connection.execute(
                "UPDATE durable_pending SET cancel_confirmed=1 "
                "WHERE session_key=? AND sequence=? AND purged=1",
                (outbox.session_key, sequence),
            )
    if len(rows) > limit:
        raise ValueError("sequence cancellation page limit exceeded; retry to continue")
