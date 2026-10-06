"""Bounded fact qualifiers and field-to-source evidence expressions.

An alternative is an AND group of spans; alternatives are OR branches. Exact
spans establish provenance only. Semantic truth remains a reviewer/host decision.
The current scalar state projector cannot execute conditions or negation.
"""

from dataclasses import dataclass

EVIDENCE_FIELDS = frozenset(
    {
        "subject_id",
        "predicate",
        "value",
        "valid_from",
        "valid_to",
        "conditions",
        "exceptions",
        "negated",
    }
)


@dataclass(frozen=True, slots=True)
class SourceSpan:
    source_event_id: str
    start: int
    end: int
    quote: str

    def __post_init__(self):
        if not isinstance(self.source_event_id, str) or not 1 <= len(self.source_event_id) <= 256:
            raise ValueError("source_event_id must be bounded")
        if (
            type(self.start) is not int
            or type(self.end) is not int
            or not 0 <= self.start < self.end
        ):
            raise ValueError("source span must be a nonempty half-open interval")
        if not isinstance(self.quote, str) or not 1 <= len(self.quote) <= 16_384:
            raise ValueError("source quote must be bounded")


@dataclass(frozen=True, slots=True)
class FieldEvidence:
    field: str
    alternatives: tuple[tuple[SourceSpan, ...], ...]

    def __post_init__(self):
        if self.field not in EVIDENCE_FIELDS:
            raise ValueError("unknown evidence field")
        if not isinstance(self.alternatives, (tuple, list)) or not 1 <= len(self.alternatives) <= 8:
            raise ValueError("field evidence requires 1 to 8 alternatives")
        groups = []
        for group in self.alternatives:
            if not isinstance(group, (tuple, list)) or not 1 <= len(group) <= 8:
                raise ValueError("each alternative requires 1 to 8 spans")
            groups.append(
                tuple(
                    span if isinstance(span, SourceSpan) else SourceSpan(**span) for span in group
                )
            )
        object.__setattr__(self, "alternatives", tuple(groups))

    def supported_by(self, event):
        # This version admits one source revision. Cross-source branches cannot
        # borrow unknown content, authority or audience from another event.
        return any(
            all(
                span.source_event_id == event.id
                and span.end <= len(event.content)
                and event.content[span.start : span.end] == span.quote
                for span in group
            )
            for group in self.alternatives
        )


def normalize_qualifiers(draft):
    for name in ("conditions", "exceptions"):
        values = getattr(draft, name)
        if not isinstance(values, (tuple, list)) or len(values) > 16:
            raise ValueError(f"{name} must be a bounded sequence")
        if any(not isinstance(v, str) or not v.strip() or len(v) > 1024 for v in values):
            raise ValueError(f"{name} contains an invalid qualifier")
        object.__setattr__(draft, name, tuple(values))
    if type(draft.negated) is not bool:
        raise ValueError("negated must be a boolean")
    values = draft.field_evidence
    if not isinstance(values, (tuple, list)) or len(values) > len(EVIDENCE_FIELDS):
        raise ValueError("field_evidence must be a bounded sequence")
    values = tuple(v if isinstance(v, FieldEvidence) else FieldEvidence(**v) for v in values)
    if len({v.field for v in values}) != len(values):
        raise ValueError("field evidence must have one expression per field")
    object.__setattr__(draft, "field_evidence", values)


def qualification_reasons(event, draft, required_fields=()):
    reasons = []
    if draft.conditions or draft.exceptions or draft.negated:
        reasons.append("qualified_fact_requires_contextual_projection")
    supported = {e.field for e in draft.field_evidence if e.supported_by(event)}
    if any(e.field not in supported for e in draft.field_evidence):
        reasons.append("field_evidence_not_supported")
    if not set(required_fields).issubset(supported):
        reasons.append("required_field_evidence_missing")
    return reasons
