"""Durable schedule selectors and reservations on the namespace-locked SQL UoW."""

from agent_memory.derived.model import DerivedError
from agent_memory.operations.refresh_schedule_contract import (
    KINDS,
    checked_record,
    encoded,
    limits_payload,
    projection,
    query_args,
    stamp,
)

from . import admission


async def lock(connection, scope):
    # Every caller, including deletion, takes the namespace lock FIRST. No later
    # namespace may be acquired while holding this common accounting lock.
    await admission.lock_scope(connection, scope)
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        ("agent-memory-refresh-scheduler/1",),
    )


async def clock(connection):
    cursor = await connection.execute(
        "SELECT version,observed_clock FROM agent_memory_refresh_schedule_contract "
        "WHERE singleton=1 FOR UPDATE"
    )
    row = await cursor.fetchone()
    if not row or row["version"] != 1:
        raise DerivedError("refresh_scheduler_schema_unsupported")
    return stamp(row["observed_clock"])


async def observe_clock(connection, *, now):
    now = stamp(now)
    if now is None:
        raise DerivedError("refresh_scheduler_timezone_required")
    previous = await clock(connection)
    if previous is not None and now < previous:
        return False
    await connection.execute(
        "UPDATE agent_memory_refresh_schedule_contract SET observed_clock=%s WHERE singleton=1",
        (now,),
    )
    return True


async def config(connection, limits):
    value = limits_payload(limits)
    cursor = await connection.execute(
        "SELECT version,limits_json FROM agent_memory_refresh_schedule_contract "
        "WHERE singleton=1 FOR UPDATE"
    )
    row = await cursor.fetchone()
    if not row or row["version"] != 1:
        raise DerivedError("refresh_scheduler_schema_unsupported")
    if row["limits_json"] is not None and row["limits_json"] != value:
        raise DerivedError("refresh_scheduler_config_mismatch")
    if row["limits_json"] is None:
        await connection.execute(
            "UPDATE agent_memory_refresh_schedule_contract SET limits_json=%s WHERE singleton=1",
            (value,),
        )


async def project(connection, scope, identity, payload):
    values = projection(scope, identity, payload)
    await connection.execute(
        "INSERT INTO agent_memory_refresh_schedule_due "
        "VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT(partition_key,identity) DO UPDATE SET scope_json=excluded.scope_json, "
        "tenant_id=excluded.tenant_id,instance_key=excluded.instance_key, "
        "adapter_key=excluded.adapter_key,status=excluded.status,due_at=excluded.due_at, "
        "lease_until=excluded.lease_until,priority=excluded.priority, "
        "aging_seconds=excluded.aging_seconds,created_at=excluded.created_at", values,
    )


def due_sql(*, expired=False):
    column = "lease_until" if expired else "due_at"
    return f"""WITH eligible AS (
        SELECT d.*, COALESCE(f.last_turn,0) AS last_turn,
          ROW_NUMBER() OVER (PARTITION BY d.tenant_id ORDER BY
            (d.priority + FLOOR(EXTRACT(EPOCH FROM (%s::timestamptz-d.created_at))
              / d.aging_seconds)) DESC, d.{column}, d.identity) AS tenant_rank
        FROM agent_memory_refresh_schedule_due d
          LEFT JOIN agent_memory_refresh_schedule_fairness f ON f.tenant_id=d.tenant_id
        WHERE d.adapter_key=ANY(%s) AND d.{column} IS NOT NULL AND d.{column}<=%s
          {"AND d.status='running'" if expired else "AND d.status != 'running'"}
    ) SELECT d.partition_key,d.identity,d.scope_json,e.payload_json
      FROM eligible d JOIN agent_memory_derived_entries e ON e.partition_key=d.partition_key
        AND e.kind='refresh_demand' AND e.identity=d.identity
      ORDER BY d.tenant_rank,d.last_turn,d.{column},d.identity LIMIT %s"""


async def due(connection, *, now, adapter_keys, limit=128, expired=False):
    now, keys, limit = query_args(now, adapter_keys, limit)
    if not keys:
        return ()
    cursor = await connection.execute(due_sql(expired=expired), (now, list(keys), now, limit))
    return tuple({"partition_key": row["partition_key"], "identity": row["identity"],
                  "scope": row["scope_json"],
                  "payload": checked_record("refresh_demand", row["payload_json"])}
                 for row in await cursor.fetchall())


async def usage(connection, *, now, tenant_id, instance_key):
    _ = now
    cursor = await connection.execute(
        "SELECT count(*) AS global_pending,"
        "count(*) FILTER (WHERE tenant_id=%s) AS tenant_pending,"
        "count(*) FILTER (WHERE tenant_id=%s AND instance_key=%s) AS instance_pending "
        "FROM agent_memory_refresh_schedule_due WHERE status IN ('pending','retry')",
        (tenant_id, tenant_id, instance_key),
    )
    result = dict(await cursor.fetchone())
    cursor = await connection.execute(
        "SELECT COALESCE(sum(units),0) AS global_running,"
        "COALESCE(sum(units) FILTER (WHERE tenant_id=%s),0) AS tenant_running,"
        "COALESCE(sum(units) FILTER (WHERE tenant_id=%s AND instance_key=%s),0) "
        "AS instance_running FROM agent_memory_refresh_schedule_reservations",
        (tenant_id, tenant_id, instance_key),
    )
    return {**result, **dict(await cursor.fetchone())}


async def reserve(connection, scope, execution_id, *, instance_key, units=1, limits):
    await config(connection, limits)
    if type(units) is not int or units < 1:
        raise DerivedError("invalid_refresh_scheduler_units")
    cursor = await connection.execute(
        "SELECT instance_key,units FROM agent_memory_refresh_schedule_reservations "
        "WHERE partition_key=%s AND execution_id=%s", (scope.partition_key(), execution_id),
    )
    old = await cursor.fetchone()
    if old:
        if (old["instance_key"], old["units"]) != (instance_key, units):
            raise DerivedError("refresh_reservation_conflict")
        return True
    current = await usage(
        connection, now=None, tenant_id=scope.tenant_id, instance_key=instance_key
    )
    if any(current[key]+units > limits[key] for key in
           ("global_running", "tenant_running", "instance_running")):
        return False
    await connection.execute(
        "INSERT INTO agent_memory_refresh_schedule_reservations VALUES (%s,%s,%s,%s,%s)",
        (scope.partition_key(), execution_id, scope.tenant_id, instance_key, units),
    )
    return True


async def release(connection, scope, execution_id):
    await connection.execute(
        "DELETE FROM agent_memory_refresh_schedule_reservations "
        "WHERE partition_key=%s AND execution_id=%s", (scope.partition_key(), execution_id),
    )


async def turn(connection, scope):
    await connection.execute(
        "UPDATE agent_memory_refresh_schedule_contract SET turn=turn+1 WHERE singleton=1"
    )
    await connection.execute(
        "INSERT INTO agent_memory_refresh_schedule_fairness "
        "SELECT %s,turn FROM agent_memory_refresh_schedule_contract WHERE singleton=1 "
        "ON CONFLICT(tenant_id) DO UPDATE SET last_turn=excluded.last_turn", (scope.tenant_id,),
    )


async def forget(connection, request):
    """Scrub the affected exact scope while retaining authoritative deletion barriers."""
    if not request.all_in_scope and not request.memory_ids:
        return
    await lock(connection, request.scope)
    key = request.scope.partition_key()
    for table in ("refresh_schedule_due", "refresh_schedule_reservations"):
        await connection.execute(
            f"DELETE FROM agent_memory_{table} WHERE partition_key=%s", (key,)
        )
    await connection.execute(
        "DELETE FROM agent_memory_refresh_schedule_fairness WHERE tenant_id=%s",
        (request.scope.tenant_id,),
    )
    await connection.execute(
        "UPDATE agent_memory_derived_entries SET payload_json=%s::jsonb "
        "WHERE partition_key=%s AND kind='coverage_request'",
        (encoded({"schema": "coverage-receipt/1", "state": "erased"}), key),
    )
    await connection.execute(
        "DELETE FROM agent_memory_derived_entries WHERE partition_key=%s AND kind=ANY(%s)",
        (key, [kind for kind in KINDS if kind != "coverage_request"]),
    )
