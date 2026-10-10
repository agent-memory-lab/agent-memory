"""PostgreSQL equivalents of the bounded exact-scope verification selectors."""

from .admission import _record

PENDING = """
NOT payload_json @> '{"deleted": true}'::jsonb
AND payload_json ->> 'action' IN ('PENDING_VERIFICATION', 'CONTESTED')
AND COALESCE(payload_json -> 'qualification', 'null'::jsonb) IN ('null'::jsonb, '{}'::jsonb)
AND COALESCE(payload_json #> '{project_candidate,review}', 'null'::jsonb)
    IN ('null'::jsonb, '{}'::jsonb)
"""
ACTIVE = """
kind = 'domain_verification_task'
AND payload_json ->> 'state' IN ('pending', 'retry', 'running')
"""


async def candidates(connection, scope, *, after=None, limit=128):
    if type(limit) is not int or not 1 <= limit <= 128:
        raise ValueError("verification discovery page must be between 1 and 128")
    if after is not None and (type(after) is not str or not 1 <= len(after) <= 512):
        raise ValueError("invalid verification discovery cursor")
    cursor = await connection.execute(
        f"SELECT * FROM agent_memory_admission_records WHERE {PENDING} AND partition_key=%s "
        + ("AND record_id>%s " if after is not None else "")
        + "ORDER BY record_id LIMIT %s",
        (scope.partition_key(), *((after,) if after is not None else ()), limit),
    )
    return tuple(_record(row) for row in await cursor.fetchall())


async def active(connection, scope, *, limit=4096):
    if type(limit) is not int or not 1 <= limit <= 4096:
        raise ValueError("verification active limit must be between 1 and 4096")
    cursor = await connection.execute(
        f"SELECT identity,payload_json FROM agent_memory_derived_entries WHERE {ACTIVE} "
        "AND partition_key=%s ORDER BY identity LIMIT %s", (scope.partition_key(), limit),
    )
    return tuple(dict(identity=r["identity"], payload=r["payload_json"])
                 for r in await cursor.fetchall())


async def erase(connection, scope, parents, *, all_in_scope):
    """Erase receipt bodies in bounded pages inside the namespace-locked transaction."""
    from agent_memory.derived.model import erase_rows

    from .derived import put

    after = None
    while True:
        cursor = await connection.execute(
            "SELECT identity,payload_json FROM agent_memory_derived_entries "
            "WHERE partition_key=%s AND kind='domain_verification_task' "
            + ("AND identity>%s " if after is not None else "")
            + "ORDER BY identity LIMIT 128",
            (scope.partition_key(), *((after,) if after is not None else ())),
        )
        rows = await cursor.fetchall()
        if not rows:
            return
        items = [dict(kind="domain_verification_task", identity=r["identity"],
                      payload=r["payload_json"]) for r in rows]
        changes, _ = erase_rows(items, parents, all_in_scope)
        for kind, key, payload in changes:
            await put(connection, scope, kind, key, payload)
        after = rows[-1]["identity"]
