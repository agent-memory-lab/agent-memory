"""Indexed durable scheduler state, using the caller's BEGIN IMMEDIATE transaction."""

import json

from ..derived.model import DerivedError
from .refresh_schedule_contract import (
    KINDS,
    checked_record,
    encoded,
    limits_payload,
    projection,
    query_args,
    stamp,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS refresh_schedule_contract (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1),
 limits_json TEXT, turn INTEGER NOT NULL DEFAULT 0, observed_clock TEXT
);
INSERT OR IGNORE INTO refresh_schedule_contract(singleton,version) VALUES (1,1);
CREATE TABLE IF NOT EXISTS refresh_schedule_due (
 partition_key TEXT NOT NULL, identity TEXT NOT NULL, scope_json TEXT NOT NULL,
 tenant_id TEXT NOT NULL, instance_key TEXT NOT NULL, adapter_key TEXT NOT NULL,
 status TEXT NOT NULL, due_at TEXT, lease_until TEXT, priority INTEGER NOT NULL,
 aging_seconds INTEGER NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(partition_key,identity)
);
CREATE INDEX IF NOT EXISTS refresh_schedule_due_idx
 ON refresh_schedule_due(adapter_key,due_at,tenant_id) WHERE due_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS refresh_schedule_expired_idx
 ON refresh_schedule_due(adapter_key,lease_until) WHERE lease_until IS NOT NULL;
CREATE INDEX IF NOT EXISTS refresh_schedule_usage_idx
 ON refresh_schedule_due(status,tenant_id,instance_key);
CREATE TABLE IF NOT EXISTS refresh_schedule_fairness (
 tenant_id TEXT PRIMARY KEY, last_turn INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS refresh_schedule_reservations (
 partition_key TEXT NOT NULL, execution_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
 instance_key TEXT NOT NULL, units INTEGER NOT NULL CHECK(units > 0),
 PRIMARY KEY(partition_key,execution_id)
);
CREATE INDEX IF NOT EXISTS refresh_schedule_reservation_usage_idx
 ON refresh_schedule_reservations(tenant_id,instance_key);
"""


def initialize(connection):
    # SQLite lacks ADD COLUMN IF NOT EXISTS. BEGIN IMMEDIATE serializes the
    # forward migration of pre-clock development/backup tables across processes.
    connection.execute("BEGIN IMMEDIATE")
    try:
        columns = {row[1] for row in connection.execute(
            "PRAGMA table_info(refresh_schedule_contract)"
        )}
        if "observed_clock" not in columns:
            connection.execute(
                "ALTER TABLE refresh_schedule_contract ADD COLUMN observed_clock TEXT"
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def clock(connection):
    row = connection.execute(
        "SELECT version,observed_clock FROM refresh_schedule_contract WHERE singleton=1"
    ).fetchone()
    if not row or row[0] != 1:
        raise DerivedError("refresh_scheduler_schema_unsupported")
    return stamp(row[1])


def observe_clock(connection, *, now):
    now = stamp(now)
    if now is None:
        raise DerivedError("refresh_scheduler_timezone_required")
    previous = clock(connection)
    if previous is not None and now < previous:
        return False
    connection.execute(
        "UPDATE refresh_schedule_contract SET observed_clock=? WHERE singleton=1", (now,)
    )
    return True


def config(connection, limits):
    value = limits_payload(limits)
    row = connection.execute(
        "SELECT version,limits_json FROM refresh_schedule_contract WHERE singleton=1"
    ).fetchone()
    if not row or row[0] != 1:
        raise DerivedError("refresh_scheduler_schema_unsupported")
    if row[1] is not None and row[1] != value:
        raise DerivedError("refresh_scheduler_config_mismatch")
    if row[1] is None:
        connection.execute(
            "UPDATE refresh_schedule_contract SET limits_json=? WHERE singleton=1", (value,)
        )


def project(connection, scope, identity, payload):
    values = projection(scope, identity, payload)
    connection.execute(
        "INSERT INTO refresh_schedule_due VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(partition_key,identity) DO UPDATE SET scope_json=excluded.scope_json, "
        "tenant_id=excluded.tenant_id,instance_key=excluded.instance_key, "
        "adapter_key=excluded.adapter_key,status=excluded.status,due_at=excluded.due_at, "
        "lease_until=excluded.lease_until,priority=excluded.priority, "
        "aging_seconds=excluded.aging_seconds,created_at=excluded.created_at", values,
    )


def due_sql(adapter_count, *, expired=False):
    marks = ",".join("?" for _ in range(adapter_count))
    column = "lease_until" if expired else "due_at"
    # Round-robin ranks a tenant's first candidate before another tenant's second.
    # The WHERE clause is index-backed and capability-filtered before any LIMIT.
    return f"""WITH eligible AS (
        SELECT d.*, COALESCE(f.last_turn,0) AS last_turn,
          ROW_NUMBER() OVER (PARTITION BY d.tenant_id ORDER BY
            (d.priority + CAST((julianday(?) - julianday(d.created_at))*86400
              / d.aging_seconds AS INTEGER)) DESC, d.{column}, d.identity) AS tenant_rank
        FROM refresh_schedule_due d LEFT JOIN refresh_schedule_fairness f
          ON f.tenant_id=d.tenant_id
        WHERE d.adapter_key IN ({marks}) AND d.{column} IS NOT NULL AND d.{column}<=?
          {"AND d.status='running'" if expired else "AND d.status != 'running'"}
    ) SELECT d.partition_key,d.identity,d.scope_json,e.payload_json
      FROM eligible d JOIN derived_entries e ON e.partition_key=d.partition_key
        AND e.kind='refresh_demand' AND e.identity=d.identity
      ORDER BY d.tenant_rank,d.last_turn,d.{column},d.identity LIMIT ?"""


def due(connection, *, now, adapter_keys, limit=128, expired=False):
    now, keys, limit = query_args(now, adapter_keys, limit)
    if not keys:
        return ()
    rows = connection.execute(due_sql(len(keys), expired=expired), (now, *keys, now, limit))
    return tuple({"partition_key": row[0], "identity": row[1], "scope": json.loads(row[2]),
                  "payload": checked_record("refresh_demand", json.loads(row[3]))} for row in rows)


def usage(connection, *, now, tenant_id, instance_key):
    # Leases do not release accounting. Recovery must explicitly reconcile the execution.
    _ = now
    pending = connection.execute(
        "SELECT count(*),COALESCE(sum(tenant_id=?),0),"
        "COALESCE(sum(tenant_id=? AND instance_key=?),0) FROM refresh_schedule_due "
        "WHERE status IN ('pending','retry')", (tenant_id, tenant_id, instance_key),
    ).fetchone()
    running = connection.execute(
        "SELECT COALESCE(sum(units),0),"
        "COALESCE(sum(CASE WHEN tenant_id=? THEN units ELSE 0 END),0),"
        "COALESCE(sum(CASE WHEN tenant_id=? AND instance_key=? THEN units ELSE 0 END),0) "
        "FROM refresh_schedule_reservations", (tenant_id, tenant_id, instance_key),
    ).fetchone()
    return dict(zip(("global_pending", "tenant_pending", "instance_pending", "global_running",
                     "tenant_running", "instance_running"), (*pending, *running), strict=True))


def reserve(connection, scope, execution_id, *, instance_key, units=1, limits):
    config(connection, limits)
    if type(units) is not int or units < 1:
        raise DerivedError("invalid_refresh_scheduler_units")
    old = connection.execute(
        "SELECT instance_key,units FROM refresh_schedule_reservations "
        "WHERE partition_key=? AND execution_id=?", (scope.partition_key(), execution_id),
    ).fetchone()
    if old:
        if (old[0], old[1]) != (instance_key, units):
            raise DerivedError("refresh_reservation_conflict")
        return True
    current = usage(connection, now=None, tenant_id=scope.tenant_id, instance_key=instance_key)
    if any(current[key]+units > limits[key] for key in
           ("global_running", "tenant_running", "instance_running")):
        return False
    connection.execute(
        "INSERT INTO refresh_schedule_reservations VALUES (?,?,?,?,?)",
        (scope.partition_key(), execution_id, scope.tenant_id, instance_key, units),
    )
    return True


def release(connection, scope, execution_id):
    connection.execute(
        "DELETE FROM refresh_schedule_reservations WHERE partition_key=? AND execution_id=?",
        (scope.partition_key(), execution_id),
    )


def turn(connection, scope):
    connection.execute("UPDATE refresh_schedule_contract SET turn=turn+1 WHERE singleton=1")
    connection.execute(
        "INSERT INTO refresh_schedule_fairness SELECT ?,turn FROM refresh_schedule_contract "
        "WHERE singleton=1 ON CONFLICT(tenant_id) DO UPDATE SET last_turn=excluded.last_turn",
        (scope.tenant_id,),
    )


def forget(connection, request):
    """Conservative exact-scope scrubbing; authoritative deletion barriers are separate."""
    if not request.all_in_scope and not request.memory_ids:
        return
    key = request.scope.partition_key()
    for table in ("refresh_schedule_due", "refresh_schedule_reservations"):
        connection.execute(f"DELETE FROM {table} WHERE partition_key=?", (key,))
    connection.execute(
        "DELETE FROM refresh_schedule_fairness WHERE tenant_id=?", (request.scope.tenant_id,)
    )
    connection.execute(
        "UPDATE derived_entries SET payload_json=? "
        "WHERE partition_key=? AND kind='coverage_request'",
        (encoded({"schema": "coverage-receipt/1", "state": "erased"}), key),
    )
    scrubbed = tuple(kind for kind in KINDS if kind != "coverage_request")
    connection.execute(
        "DELETE FROM derived_entries WHERE partition_key=? AND kind IN ("
        + ",".join("?" for _ in scrubbed) + ")", (key, *scrubbed),
    )
