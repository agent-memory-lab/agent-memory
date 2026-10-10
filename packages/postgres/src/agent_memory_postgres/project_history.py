"""Exact-scope admission version lookup on the existing writer transaction."""

from agent_memory.conditions import instant
from agent_memory.derived.model import DerivedError
from agent_memory.serialization import to_jsonable


async def record_at(connection, scope, record_id, known_at, *, metadata_only=False):
    cursor = await connection.execute(
        "SELECT event_id,slot_key,version FROM agent_memory_admission_records "
        "WHERE partition_key=%s AND record_id=%s "
        "AND NOT payload_json @> '{\"deleted\":true}'::jsonb",
        (scope.partition_key(), record_id),
    )
    current = await cursor.fetchone()
    if current is None:
        return None
    cursor = await connection.execute(
        "SELECT COUNT(*) AS n,MIN(version) AS lo,MAX(version) AS hi "
        "FROM agent_memory_admission_versions WHERE record_id=%s",
        (record_id,),
    )
    coverage = await cursor.fetchone()
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
            "jsonb_build_object('project_candidate',payload_json->'project_candidate',"
            "'draft',jsonb_build_object('predicate',payload_json#>'{draft,predicate}')) AS routing,"
            "jsonb_path_query_array(payload_json,'$.**.source_event_id') AS source_ids,"
            "jsonb_path_query_array(payload_json,'$.**.source_event_ids[*]') AS original_sources"
        )
    cursor = await connection.execute(
        "SELECT version,recorded_at," + columns + " FROM agent_memory_admission_versions "
        "WHERE record_id=%s AND recorded_at<=%s ORDER BY recorded_at DESC,version DESC LIMIT 1",
        (record_id, instant(known_at)),
    )
    row = await cursor.fetchone()
    if row is None:
        return None
    result = dict(
        id=record_id,
        event_id=current["event_id"],
        slot_key=current["slot_key"],
        scope=to_jsonable(scope),
        version=row["version"],
        recorded_at=row["recorded_at"].isoformat(),
    )
    if metadata_only:
        from agent_memory.derived.project_index import routing
        from agent_memory.derived.subscriptions import HEADER_SCHEMA

        payload = row["routing"]
        if payload["project_candidate"] is None:
            payload.pop("project_candidate")
        payload["source_event_ids"] = sorted(
            set([current["event_id"], *row["source_ids"], *(row["original_sources"] or [])])
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
        result["payload"] = row["payload_json"]
    return result


async def source_revision_metadata(connection, scope, event_id):
    cursor = await connection.execute(
        "SELECT metadata_json->'_retention' AS retained FROM agent_memory_events "
        "WHERE partition_key=%s AND id=%s AND archived_at IS NULL",
        (scope.partition_key(), event_id),
    )
    row = await cursor.fetchone()
    return row["retained"] if row else None
