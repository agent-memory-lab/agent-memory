"""Bounded host-reported measurements, never an automatic reward definition."""
from dataclasses import dataclass

from .recovery import _text
from .recovery_store import _refs


@dataclass(frozen=True, slots=True)
class CompressionFeedback:
    feedback_id: str
    summary_id: str
    evaluator_id: str
    outcome: str
    before_units: int
    after_units: int
    measurement_unit: str
    counter_id: str
    failure_reason: str = ""
    outcome_event_ids: tuple[str, ...] = ()

    def __post_init__(self):
        for value in (self.feedback_id, self.summary_id, self.evaluator_id, self.counter_id):
            _text(value, limit=256)
        if self.outcome not in ("succeeded", "failed", "unknown"):
            raise ValueError("feedback outcome must be host supplied")
        if self.measurement_unit not in ("tokens", "utf8_bytes"):
            raise ValueError("feedback units must identify the measurement method")
        for value in (self.before_units, self.after_units):
            if type(value) is not int or not 0 <= value <= 1000000000:
                raise ValueError("feedback counts must be bounded nonnegative integers")
        _text(self.failure_reason, limit=2048, empty=True)
        if not isinstance(self.outcome_event_ids, tuple):
            raise ValueError("outcome evidence IDs must be immutable")
        _refs(self.outcome_event_ids, empty=True)
