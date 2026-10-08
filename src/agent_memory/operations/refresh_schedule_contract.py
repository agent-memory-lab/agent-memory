"""Shared storage validation for the opt-in durable refresh scheduler.

These are deterministic resource-unit limits, not monetary/model reservations.
The schema contract requires a coordinated upgrade of all lifecycle writers.
"""

import json
from dataclasses import asdict
from datetime import UTC, datetime

from ..derived.model import DerivedError

CONTRACT = "durable-coalescing/1"
KINDS = (
    "refresh_demand", "refresh_execution", "coverage_request", "refresh_policy",
    "refresh_publication",
)
SCHEMAS = dict(zip(KINDS, (
    "refresh-demand/1", "refresh-execution/1", "coverage-receipt/1",
    "refresh-policy-binding/1", "refresh-publication/1",
), strict=True))
LIMIT_KEYS = (
    "global_pending", "tenant_pending", "instance_pending", "global_running",
    "tenant_running", "instance_running",
)
IMMUTABLE_EXECUTION = (
    "id", "demand_id", "facet_id", "adapter_key", "epoch", "compatibility", "claimed",
    "unit_id", "unit", "created_at", "policy",
)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def snapshot(value):
    return json.loads(encoded(value))


def checked_record(kind, payload):
    if kind in SCHEMAS and payload is not None and (
        not isinstance(payload, dict) or payload.get("schema") != SCHEMAS[kind]
    ):
        raise DerivedError("refresh_scheduler_record_unsupported")
    return payload


def stamp(value):
    if value is None:
        return None
    at = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(at, datetime) or at.utcoffset() is None:
        raise DerivedError("refresh_scheduler_timezone_required")
    return at.astimezone(UTC).isoformat(timespec="microseconds")


def limits_payload(limits):
    data = snapshot(limits)
    if not isinstance(data, dict) or any(
        type(data.get(key)) is not int or not 1 <= data[key] <= 1_000_000
        for key in LIMIT_KEYS
    ):
        raise DerivedError("invalid_refresh_scheduler_limits")
    return encoded(data)


def immutable(old, payload):
    if old is not None and any(old.get(key) != payload.get(key) for key in IMMUTABLE_EXECUTION):
        raise DerivedError("refresh_execution_immutable")


def projection(scope, identity, payload):
    """Extract bounded SQL selectors. Never select runnable work by scanning JSON."""
    data = snapshot(payload)
    if data.get("tenant_id", scope.tenant_id) != scope.tenant_id:
        raise DerivedError("refresh_scheduler_scope_mismatch")
    status = data["status"]
    terminal = status in {"erased", "superseded", "dead", "cancelled"}
    if status in {"deferred", "retry"} and data.get("runnable_at") is not None:
        # Semantic boundaries remain mandatory read guards, but cannot bypass
        # a persisted retry delay and crowd runnable work out of a bounded page.
        times = [stamp(data["runnable_at"])]
    else:
        times = [stamp(data.get("runnable_at") or data.get("due_at")),
                 stamp(data.get("next_transition_at"))]
    times = [value for value in times if value is not None]
    due = min(times) if times and not terminal else None
    priority = data.get("priority", 0)
    aging = data.get("aging_seconds", 60)
    if type(priority) is not int or not -1_000_000 <= priority <= 1_000_000:
        raise DerivedError("invalid_refresh_scheduler_priority")
    if type(aging) is not int or not 1 <= aging <= 86400:
        raise DerivedError("invalid_refresh_scheduler_aging")
    return (
        scope.partition_key(), identity, encoded(asdict(scope)), scope.tenant_id,
        data.get("instance_key", identity), data["adapter_key"], status, due,
        stamp(data.get("lease_until")), priority, aging,
        stamp(data.get("first_dirty_at") or data.get("created_at")),
    )


def query_args(now, adapter_keys, limit):
    if type(limit) is not int or not 1 <= limit <= 1024:
        raise DerivedError("invalid_refresh_scheduler_limit")
    if not isinstance(adapter_keys, (list, tuple)) or len(adapter_keys) > 128 or any(
        not isinstance(key, str) or not key or len(key) > 256 for key in adapter_keys
    ):
        raise DerivedError("invalid_refresh_scheduler_capabilities")
    return stamp(now), tuple(sorted(set(adapter_keys))), limit
