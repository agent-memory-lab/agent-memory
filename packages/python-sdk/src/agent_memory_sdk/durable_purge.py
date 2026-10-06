"""Apply remote deletion pages before delivery; local cleanup and cursor commit together."""

import json
from hashlib import sha256


def identity_hash(event_id):
    return sha256(event_id.encode()).hexdigest()


def purge_rows(connection, rows):
    for key, sequence, event_id in rows:
        connection.execute(
            "INSERT OR IGNORE INTO durable_purged_ids VALUES (?)", (identity_hash(event_id),)
        )
        connection.execute(
            "UPDATE durable_pending SET acknowledged=1,event_json='null',revision_json=NULL "
            "WHERE session_key=? AND sequence=?",
            (key, sequence),
        )


def purge_session(outbox):
    with outbox._connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        rows = connection.execute(
            "SELECT session_key,sequence,event_id FROM durable_pending WHERE session_key=?",
            (outbox.session_key,),
        ).fetchall()
        purge_rows(connection, rows)
        connection.execute(
            "UPDATE durable_sessions SET revoked=1 WHERE session_key=?", (outbox.session_key,)
        )


def validate_page(outbox, response, after):
    if not isinstance(response, dict):
        raise ValueError("invalid purge response")
    numbers = [
        response.get(k) for k in ("session_epoch", "scope_epoch", "after", "through", "head")
    ]
    if any(type(n) is not int or n < 0 for n in numbers):
        raise ValueError("invalid purge coordinates")
    epoch, current, start, through, head = numbers
    entries = response.get("entries")
    if (
        response.get("schema") != "producer-purge/1"
        or response.get("producer_id") != outbox.session["producer_id"]
        or epoch != outbox.session["epoch"]
        or current < epoch
        or start != after
        or not start <= through <= head
        or type(response.get("scope_key")) is not str
        or not response["scope_key"]
        or type(response.get("closed")) is not bool
        or type(response.get("scope_revoked")) is not bool
        or response["closed"] != (through == head)
        or response["scope_revoked"] != (epoch != current)
        or not isinstance(entries, list)
        or len(entries) > 128
        or through != start + len(entries)
    ):
        raise ValueError("invalid purge response binding")
    for index, entry in enumerate(entries, start + 1):
        if (
            not isinstance(entry, dict)
            or type(entry.get("cursor")) is not int
            or entry["cursor"] != index
            or type(entry.get("epoch")) is not int
            or not 0 <= entry["epoch"] <= current
            or type(entry.get("all_in_scope")) is not bool
            or type(entry.get("source_event_id")) is not str
            or entry.get("mode") not in {"erase", "archive"}
            or (entry["all_in_scope"] and entry["source_event_id"] != "")
            or (not entry["all_in_scope"] and not entry["source_event_id"])
        ):
            raise ValueError("invalid purge journal entry")


def apply_page(outbox, response):
    with outbox._connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT scope_key,purge_cursor FROM durable_sessions WHERE session_key=?",
            (outbox.session_key,),
        ).fetchone()
        if row[1] != response["after"]:
            raise ValueError("local purge cursor changed; retry synchronization")
        if row[0] is not None and row[0] != response["scope_key"]:
            raise ValueError("purge response scope mismatch")
        key = response["scope_key"]
        connection.execute(
            "UPDATE durable_sessions SET scope_key=? WHERE session_key=?", (key, outbox.session_key)
        )
        pending = connection.execute(
            "SELECT p.session_key,p.sequence,p.event_id,s.epoch FROM durable_pending p "
            "JOIN durable_sessions s USING(session_key) WHERE s.scope_key=?",
            (key,),
        ).fetchall()
        targets = {e["source_event_id"] for e in response["entries"] if not e["all_in_scope"]}
        revoked_before = max(
            [response["scope_epoch"] if response["scope_revoked"] else 0]
            + [e["epoch"] for e in response["entries"] if e["all_in_scope"]]
        )
        selected = []
        for session_key, sequence, event_id, epoch in pending:
            source_id = (
                "source:"
                + sha256(json.dumps([key, event_id], separators=(",", ":")).encode()).hexdigest()
            )
            revoked = revoked_before > 0 and (epoch is None or epoch < revoked_before)
            if revoked or source_id in targets:
                selected.append((session_key, sequence, event_id))
        purge_rows(connection, selected)
        if revoked_before > 0:
            connection.execute(
                "UPDATE durable_sessions SET revoked=1 WHERE scope_key=? AND "
                "(epoch IS NULL OR epoch<?)",
                (key, revoked_before),
            )
        connection.execute(
            "UPDATE durable_sessions SET purge_cursor=? WHERE session_key=?",
            (response["through"], outbox.session_key),
        )


async def synchronize(outbox, client, *, max_pages=32):
    if type(max_pages) is not int or not 1 <= max_pages <= 1000:
        raise ValueError("max_pages must be between 1 and 1000")
    for _ in range(max_pages):
        with outbox._connection() as connection:
            after = connection.execute(
                "SELECT purge_cursor FROM durable_sessions WHERE session_key=?",
                (outbox.session_key,),
            ).fetchone()[0]
        response = await client.durable_purge_sync(outbox.session, after=after, limit=128)
        validate_page(outbox, response, after)
        apply_page(outbox, response)
        # A lost acknowledgment leaves the local cursor and tombstones committed.
        ack = await client.durable_purge_ack(outbox.session, through=response["through"])
        if (
            not isinstance(ack, dict)
            or ack.get("producer_id") != outbox.session["producer_id"]
            or ack.get("session_epoch") != outbox.session["epoch"]
            or type(ack.get("purged_through")) is not int
            or ack["purged_through"] < response["through"]
            or type(ack.get("head")) is not int
            or ack["head"] < ack["purged_through"]
            or type(ack.get("closed")) is not bool
            or ack["closed"] != (ack["purged_through"] == ack["head"])
        ):
            raise ValueError("invalid purge acknowledgment")
        if response["closed"]:
            return response
    raise ValueError("purge synchronization page limit exceeded")
