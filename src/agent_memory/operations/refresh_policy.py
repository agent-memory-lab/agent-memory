"""Host-owned refresh policy and deterministic, non-monetary admission limits.

These are deployment controls, never authorization or semantic freshness rules.
All front/background demand shares one persisted limits configuration.
"""

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from ..derived.model import DerivedError, timestamp


def _integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise DerivedError("invalid_refresh_policy")


@dataclass(frozen=True, slots=True)
class RefreshPolicy:
    mode: str = "on_change"
    debounce_seconds: int = 0
    max_wait_seconds: int = 60
    schedule_seconds: int | None = None
    priority: int = 0
    max_age_seconds: int = 86400
    max_no_progress_seconds: int = 3600
    max_attempts: int = 3
    retry_seconds: int = 2
    aging_seconds: int = 60
    allow_fallback: bool = False
    hotness_window_seconds: int = 3600
    min_residence_seconds: int = 3600
    minimum_samples: int = 10
    promote_reuses: int = 8
    demote_reuses: int = 2
    revision: str = "1"
    schema: str = "refresh-policy/1"

    def __post_init__(self):
        if self.schema != "refresh-policy/1" or self.mode not in {
            "on_change",
            "on_demand",
            "scheduled",
            "hybrid",
        }:
            raise DerivedError("invalid_refresh_policy")
        for key in ("debounce_seconds", "priority", "min_residence_seconds", "demote_reuses"):
            _integer(getattr(self, key), 0, 86400)
        for key in (
            "max_wait_seconds",
            "max_age_seconds",
            "max_no_progress_seconds",
            "retry_seconds",
            "aging_seconds",
            "hotness_window_seconds",
            "minimum_samples",
            "promote_reuses",
        ):
            _integer(getattr(self, key), 1, 86400)
        _integer(self.max_attempts, 1, 100)
        if self.schedule_seconds is not None:
            _integer(self.schedule_seconds, 1, 86400)
        if self.mode == "scheduled" and self.schedule_seconds is None:
            raise DerivedError("refresh_schedule_required")
        if (
            type(self.allow_fallback) is not bool
            or type(self.revision) is not str
            or not self.revision
            or len(self.revision) > 128
            or self.demote_reuses >= self.promote_reuses
        ):
            raise DerivedError("invalid_refresh_policy")

    def payload(self):
        return asdict(self)

    @classmethod
    def from_payload(cls, value):
        try:
            return cls(**value)
        except (TypeError, KeyError) as error:
            raise DerivedError("invalid_refresh_policy") from error

    def due(self, first_dirty_at, last_dirty_at, *, boundary=None, deadline=None):
        first, last = (
            timestamp(value).astimezone(UTC) for value in (first_dirty_at, last_dirty_at)
        )
        # A regressing writer clock cannot postpone already recorded responsibility.
        last = max(first, last)
        values = [
            last + timedelta(seconds=self.debounce_seconds),
            first + timedelta(seconds=self.max_wait_seconds),
        ]
        values.extend(timestamp(v).astimezone(UTC) for v in (boundary, deadline) if v is not None)
        return min(values)


@dataclass(frozen=True, slots=True)
class RefreshLimits:
    global_pending: int = 4096
    tenant_pending: int = 128
    instance_pending: int = 2
    global_running: int = 64
    tenant_running: int = 8
    instance_running: int = 1
    revision: str = "1"

    def __post_init__(self):
        for key in (
            "global_pending",
            "tenant_pending",
            "instance_pending",
            "global_running",
            "tenant_running",
            "instance_running",
        ):
            _integer(getattr(self, key), 1, 1_000_000)
        if (
            self.tenant_pending > self.global_pending
            or self.instance_pending > self.tenant_pending
            or self.tenant_running > self.global_running
            or self.instance_running != 1
            or type(self.revision) is not str
            or not self.revision
            or len(self.revision) > 128
        ):
            raise DerivedError("invalid_refresh_limits")

    def payload(self):
        return asdict(self)


def temperature_decision(policy, stats, *, now, budget_available):
    """Recommend a mode from one complete authorized observation window.

    Cost is a measured common work unit, never a synthesized dollar amount. Unknown
    counterfactual cost cannot justify automatic promotion. Hysteresis and residency
    constrain both directions. The caller explicitly applies a new policy revision.
    """
    now = timestamp(now)
    if not isinstance(policy, RefreshPolicy) or type(budget_available) is not bool:
        raise DerivedError("invalid_refresh_policy")
    if (
        now
        < datetime.fromisoformat(stats["window_started_at"])
        + timedelta(seconds=policy.hotness_window_seconds)
        or now
        < datetime.fromisoformat(stats["changed_at"])
        + timedelta(seconds=policy.min_residence_seconds)
        or stats.get("authorized_reads", 0) < policy.minimum_samples
    ):
        return policy.mode
    reuses = stats.get("valid_reuses", 0)
    benefit = stats.get("measured_net_work_saved")
    if policy.mode == "on_demand" and (
        budget_available
        and reuses >= policy.promote_reuses
        and type(benefit) in (int, float)
        and benefit > 0
    ):
        return "on_change"
    if policy.mode in {"on_change", "hybrid"} and reuses <= policy.demote_reuses:
        return "on_demand"
    return policy.mode
