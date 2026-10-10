"""Exact-scope admission version lookup on the existing writer transaction."""

import json

from ..conditions import instant
from ..derived.model import DerivedError


def record_at(connection, scope, record_id, known_at, *, metadata_only=False):
    current = connection.execute(
        "SELECT event_id,slot_key,version FROM admission_records "
        "WHERE partition_key=? AND record_id=? "
        "AND COALESCE(json_extract(payload_json,'$.deleted'),0)!=1",
        (scope.partition_key(), record_id),
    ).fetchone()
    if current is None:
        return None
    coverage = connection.execute(
        "SELECT COUNT(*) AS n,MIN(version) AS lo,MAX(version) AS hi FROM admission_versions "
        "WHERE record_id=?",
        (record_id,),
    ).fetchone()
    if (
        coverage["n"] > 256
        or coverage["lo"] != 1
        or coverage["hi"] != current["version"]
        or coverage["n"] != coverage["hi"]
    ):
        raise DerivedError("project_history_version_coverage_unavailable")
    columns = "payload_json"
    if metadata_only:
        columns = (
            "json_object('project_candidate',json_extract(payload_json,'$.project_candidate'),"
            "'draft',json_object('predicate',json_extract(payload_json,'$.draft.predicate'))) "
            "AS routing,"
            "(SELECT json_group_array(value) FROM json_tree(v.payload_json) "
            "WHERE key='source_event_id' AND type='text') AS source_ids,"
            "(SELECT json_group_array(json(value)) FROM json_tree(v.payload_json) "
            "WHERE key='source_event_ids' AND type='array') AS original_sources"
        )
    row = connection.execute(
        "SELECT version,recorded_at," + columns + " FROM admission_versions v "
        "WHERE record_id=? AND recorded_at<=? ORDER BY recorded_at DESC,version DESC LIMIT 1",
        (record_id, instant(known_at).isoformat()),
    ).fetchone()
    if row is None:
        return None
    result = dict(
        id=record_id,
        event_id=current["event_id"],
        slot_key=current["slot_key"],
        version=row["version"],
        recorded_at=row["recorded_at"],
    )
    from ..serialization import to_jsonable

    result["scope"] = to_jsonable(scope)
    if metadata_only:
        from ..derived.project_index import routing
        from ..derived.subscriptions import HEADER_SCHEMA

        payload = json.loads(row["routing"])
        if payload["project_candidate"] is None:
            payload.pop("project_candidate")
        payload["source_event_ids"] = sorted(
            set(
                [
                    current["event_id"],
                    *json.loads(row["source_ids"]),
                    *(key for group in json.loads(row["original_sources"]) for key in group),
                ]
            )
        )
        result["header"] = dict(
            schema=HEADER_SCHEMA,
            id=record_id,
            event_id=current["event_id"],
            slot_key=current["slot_key"],
            version=row["version"],
            generation=row["version"],
            project=routing(payload),
            source_ids=payload["source_event_ids"],
            claim_id=None,
        )
    else:
        result["payload"] = json.loads(row["payload_json"])
    return result


def source_revision_metadata(connection, scope, event_id):
    row = connection.execute(
        "SELECT json_extract(metadata_json,'$._retention') AS retained FROM events "
        "WHERE partition_key=? AND id=? AND archived_at IS NULL",
        (scope.partition_key(), event_id),
    ).fetchone()
    return json.loads(row["retained"]) if row and row["retained"] else None
