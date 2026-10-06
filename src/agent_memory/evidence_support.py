"""Field support with bounded AND/OR proofs and exact temporal set operations."""

from dataclasses import dataclass
from datetime import datetime
from itertools import product

from .conditions import identifier, instant
from .domain import SourceAuthority
from .fact_qualification import EVIDENCE_FIELDS, SourceSpan


@dataclass(frozen=True, slots=True)
class SupportRange:
    start: datetime
    end: datetime | None = None
    kind: str = "interval"

    def __post_init__(self):
        object.__setattr__(self, "start", instant(self.start))
        if self.end is not None:
            object.__setattr__(self, "end", instant(self.end))
        if self.kind not in {"interval", "point"} or (
            self.kind == "point" and self.end is not None
        ):
            raise ValueError("invalid support range")
        if self.end is not None and self.end <= self.start:
            raise ValueError("support interval must be nonempty")

    def contains(self, at):
        return (
            at == self.start
            if self.kind == "point"
            else self.start <= at and (self.end is None or at < self.end)
        )

    @classmethod
    def from_payload(cls, payload):
        return cls(
            datetime.fromisoformat(payload["start"]),
            datetime.fromisoformat(payload["end"]) if payload.get("end") else None,
            payload.get("kind", "interval"),
        )


def intersect(a, b):
    if a.kind == "point":
        return a if b.contains(a.start) else None
    if b.kind == "point":
        return b if a.contains(b.start) else None
    start = max(a.start, b.start)
    ends = [v for v in (a.end, b.end) if v is not None]
    end = min(ends) if ends else None
    return SupportRange(start, end) if end is None or start < end else None


def union(ranges):
    intervals = sorted((r for r in ranges if r.kind == "interval"), key=lambda r: r.start)
    merged = []
    for item in intervals:
        if merged and (merged[-1].end is None or item.start <= merged[-1].end):
            previous = merged.pop()
            end = None if previous.end is None or item.end is None else max(previous.end, item.end)
            merged.append(SupportRange(previous.start, end))
        else:
            merged.append(item)
    points = {
        r for r in ranges if r.kind == "point" and not any(i.contains(r.start) for i in merged)
    }
    return tuple(sorted([*merged, *points], key=lambda r: (r.start, r.kind)))


@dataclass(frozen=True, slots=True)
class EvidenceLink:
    id: str
    fields: tuple[str, ...]
    target_sha256: str
    span: SourceSpan
    authority: SourceAuthority
    support: SupportRange | None  # None means unknown time, never an unbounded proof.
    source_family: str | None = None
    domain_revision: str | None = None
    method: str = "host-reviewed/1"

    def __post_init__(self):
        identifier(self.id)
        identifier(self.method)
        if len(self.target_sha256) != 64 or any(
            c not in "0123456789abcdef" for c in self.target_sha256
        ):
            raise ValueError("evidence target must be a candidate fingerprint")
        if (
            not self.fields
            or len(self.fields) > len(EVIDENCE_FIELDS)
            or not set(self.fields) <= EVIDENCE_FIELDS
        ):
            raise ValueError("invalid supported fields")
        object.__setattr__(self, "fields", tuple(sorted(set(self.fields))))
        if not isinstance(self.span, SourceSpan) or not isinstance(self.authority, SourceAuthority):
            raise ValueError("evidence requires source locator and trusted authority")
        if self.support is not None and not isinstance(self.support, SupportRange):
            raise ValueError("invalid support range")
        for item in (self.source_family, self.domain_revision):
            if item is not None:
                identifier(item)


@dataclass(frozen=True, slots=True)
class FieldSupport:
    field: str
    alternatives: tuple[tuple[str, ...], ...]

    def __post_init__(self):
        if (
            self.field not in EVIDENCE_FIELDS
            or not isinstance(self.alternatives, (tuple, list))
            or not 1 <= len(self.alternatives) <= 8
        ):
            raise ValueError("field support requires bounded OR branches")
        groups = []
        for group in self.alternatives:
            if not isinstance(group, (tuple, list)) or not 1 <= len(group) <= 8:
                raise ValueError("field support requires bounded AND leaves")
            for identity in group:
                identifier(identity)
            groups.append(tuple(sorted(set(group))))
        object.__setattr__(self, "alternatives", tuple(groups))


def evaluate_support(qualification, available_sources):
    """Return every complete proof's support domain; fail on combinatorial excess."""
    links = {
        evidence["id"]: evidence
        for evidence in qualification["links"]
        if evidence["span"]["source_event_id"] in available_sources
        and evidence["support"] is not None
    }
    fields = qualification["field_support"]
    if not fields:
        return (), ()
    options = []
    policy = qualification["policy"]
    for field in fields:
        groups = [
            g
            for g in field["alternatives"]
            if g and all(i in links and field["field"] in links[i]["fields"] for i in g)
        ]
        if policy["require_independent_sources"]:
            groups = [
                g
                for g in groups
                if None not in {links[i]["source_family"] for i in g}
                and len({links[i]["source_family"] for i in g}) >= 2
                and len({links[i]["source_family"] for i in g})
                == len({links[i]["span"]["source_event_id"] for i in g})
            ]
        options.append(groups)
    combinations = 1
    for values in options:
        combinations *= len(values)
    if combinations > 256:
        raise ValueError("support proof budget exceeded")
    proofs = []
    for chosen in product(*options):
        selected = [links[i] for i in sorted({i for group in chosen for i in group})]
        revisions = {evidence["domain_revision"] for evidence in selected}
        if policy["require_domain_revision"] and (None in revisions or len(revisions) != 1):
            continue
        sources = {evidence["span"]["source_event_id"] for evidence in selected}
        families = {evidence["source_family"] for evidence in selected}
        if policy["require_independent_sources"] and (
            None in families or len(families) < 2 or len(families) != len(sources)
        ):
            continue
        support = SupportRange.from_payload(selected[0]["support"])
        for link in selected[1:]:
            support = intersect(support, SupportRange.from_payload(link["support"]))
            if support is None:
                break
        if support is not None:
            proofs.append((support, tuple(evidence["id"] for evidence in selected)))
    return union([r for r, _ in proofs]), tuple(proofs)


def scrub_support(payload, source_ids):
    """Erase auxiliary evidence from current and historical snapshots, keeping OR survivors.

    The primary candidate source is handled by ordinary deletion. All conditional
    results stay out of the unconditional Claim table, including after proof loss.
    """
    qualification = payload.get("qualification")
    if qualification is None:
        return False
    removed = {
        link["id"]
        for link in qualification["links"]
        if link["span"]["source_event_id"] in source_ids
    }
    if not removed:
        return False
    qualification["links"] = [
        evidence for evidence in qualification["links"] if evidence["id"] not in removed
    ]
    for field in qualification["field_support"]:
        field["alternatives"] = [g for g in field["alternatives"] if not (set(g) & removed)]
    return True
