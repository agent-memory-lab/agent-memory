"""Indexed verification discovery and bounded-page erasure on the caller transaction."""

import json

PENDING = """
COALESCE(json_extract(payload_json, '$.deleted'), 0) = 0
AND json_extract(payload_json, '$.action') IN ('PENDING_VERIFICATION', 'CONTESTED')
AND COALESCE(json_extract(payload_json, '$.qualification'), '{}') = '{}'
AND COALESCE(json_extract(payload_json, '$.project_candidate.review'), '{}') = '{}'
"""
ACTIVE = """
kind = 'domain_verification_task'
AND json_extract(payload_json, '$.state') IN ('pending', 'retry', 'running')
"""
SCHEMA = f"""
CREATE INDEX IF NOT EXISTS admission_verification_pending_idx
 ON admission_records(partition_key, record_id) WHERE {PENDING};
CREATE INDEX IF NOT EXISTS derived_verification_active_idx
 ON derived_entries(partition_key, kind, identity) WHERE {ACTIVE};
"""


def candidates(connection, scope, *, after=None, limit=128):
    if type(limit) is not int or not 1 <= limit <= 128:
        raise ValueError("verification discovery page must be between 1 and 128")
    if after is not None and (type(after) is not str or not 1 <= len(after) <= 512):
        raise ValueError("invalid verification discovery cursor")
    return connection.execute(
        "SELECT * FROM admission_records INDEXED BY admission_verification_pending_idx "
        f"WHERE {PENDING} AND partition_key=? "
        + ("AND record_id>? " if after is not None else "")
        + "ORDER BY record_id LIMIT ?",
        (scope.partition_key(), *((after,) if after is not None else ()), limit),
    ).fetchall()


def active(connection, scope, *, limit=4096):
    if type(limit) is not int or not 1 <= limit <= 4096:
        raise ValueError("verification active limit must be between 1 and 4096")
    rows = connection.execute(
        "SELECT identity,payload_json FROM derived_entries "
        f"INDEXED BY derived_verification_active_idx WHERE {ACTIVE} "
        "AND partition_key=? ORDER BY identity LIMIT ?", (scope.partition_key(), limit),
    ).fetchall()
    return tuple(dict(identity=r[0], payload=json.loads(r[1])) for r in rows)


def erase(connection, scope, parents, *, all_in_scope):
    """Keep source deletion available beyond the generic ledger read limit.

    The enclosing erasure transaction excludes concurrent writers. Page the finite
    receipt history instead of loading it all or losing terminal deduplication.
    """
    from ..derived.model import erase_rows
    from .sqlite_derived import put

    after = None
    while True:
        rows = connection.execute(
            "SELECT identity,payload_json FROM derived_entries WHERE partition_key=? "
            "AND kind='domain_verification_task' "
            + ("AND identity>? " if after is not None else "")
            + "ORDER BY identity LIMIT 128",
            (scope.partition_key(), *((after,) if after is not None else ())),
        ).fetchall()
        if not rows:
            return
        items = [dict(kind="domain_verification_task", identity=r[0], payload=json.loads(r[1]))
                 for r in rows]
        changes, _ = erase_rows(items, parents, all_in_scope)
        for kind, key, payload in changes:
            put(connection, scope, kind, key, payload)
        after = rows[-1][0]
