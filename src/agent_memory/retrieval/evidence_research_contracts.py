"""Bounded host contracts for opt-in evidence research.

Reader output can select requirement IDs and search routes. It cannot author a
scope, time axis, search expression, complete-coverage receipt or time policy.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
from typing import Any

from ..domain import MemoryBundle, MemoryQuery, MemoryScope, canonical_json


def digest(value: Any) -> str:
    return sha256(canonical_json(value).encode()).hexdigest()


def bounded_text(value: str, name: str, maximum: int, *, empty: bool = False) -> None:
    if type(value) is not str or (not empty and not value.strip()) or len(value.encode()) > maximum:
        raise ValueError(f"invalid_{name}")


def bounded_tuple(values, cls, maximum, name):
    if (
        type(values) is not tuple
        or len(values) > maximum
        or any(type(v) is not cls for v in values)
    ):
        raise ValueError(f"invalid_{name}")


def validate_usage(input_tokens, output_tokens):
    for value in (input_tokens, output_tokens):
        if value is not None and (type(value) is not int or not 0 <= value <= 1048576):
            raise ValueError("invalid_host_reported_model_usage")


@dataclass(frozen=True, slots=True)
class ResearchLimits:
    """Host work limits; max_reserved_tokens is a transport upper reservation.

    It sums UTF-8 input byte units and provider output-token caps, not measured
    tokenizer usage. ResearchResult keeps the basis and actual/unknown usage.
    """

    max_rounds: int = 3
    max_calls: int = 24
    max_model_calls: int = 8
    max_reserved_tokens: int = 65536
    max_elapsed_ms: int = 5000
    reader_output_tokens: int = 512
    max_evidence_items: int = 100

    def __post_init__(self):
        for name, low, high in (
            ("max_rounds", 1, 8),
            ("max_calls", 2, 128),
            ("max_model_calls", 0, 32),
            ("max_reserved_tokens", 64, 1048576),
            ("max_elapsed_ms", 1, 300000),
            ("reader_output_tokens", 1, 4096),
            ("max_evidence_items", 1, 100),
        ):
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"invalid_{name}")


@dataclass(frozen=True, slots=True)
class EvidenceRequirement:
    id: str
    search_text: str
    kind: str = "fact"
    allowed_routes: tuple[str, ...] = ("native",)
    coverage_domain: str | None = None
    precision: str = "unspecified"

    def __post_init__(self):
        bounded_text(self.id, "requirement_id", 128)
        bounded_text(self.search_text, "search_text", 2048)
        if self.kind not in ("fact", "temporal", "enumeration", "absence"):
            raise ValueError("invalid_requirement_kind")
        if (
            type(self.allowed_routes) is not tuple
            or not self.allowed_routes
            or len(self.allowed_routes) > 3
            or len(set(self.allowed_routes)) != len(self.allowed_routes)
            or any(v not in ("native", "semantic", "census") for v in self.allowed_routes)
        ):
            raise ValueError("invalid_allowed_routes")
        if self.kind in ("enumeration", "absence"):
            bounded_text(self.coverage_domain, "coverage_domain", 128)
            if "census" not in self.allowed_routes:
                raise ValueError("complete_requirement_needs_census_route")
        elif self.coverage_domain is not None:
            raise ValueError("coverage_domain_requires_closed_requirement")
        if self.precision not in ("unspecified", "day", "month", "year"):
            raise ValueError("invalid_precision")
        if (self.kind == "temporal") != (self.precision != "unspecified"):
            raise ValueError("temporal_requirement_needs_host_precision")


@dataclass(frozen=True, slots=True)
class QueryContract:
    query: MemoryQuery
    requirements: tuple[EvidenceRequirement, ...]
    actor: str = "host"
    allow_approximate: bool = False
    limits: ResearchLimits = field(default_factory=ResearchLimits)
    registered_question: str | None = None

    def __post_init__(self):
        if type(self.query) is not MemoryQuery or type(self.limits) is not ResearchLimits:
            raise ValueError("typed_query_and_limits_required")
        if (
            type(self.query.scope) is not MemoryScope
            or type(self.query.limit) is not int
            or type(self.query.token_budget) is not int
            or type(self.query.include_current_state) is not bool
        ):
            raise ValueError("typed_query_coordinates_required")
        bounded_text(self.query.text, "query_text", 4096)
        bounded_text(self.query.request_id, "request_id", 128)
        bounded_text(self.actor, "actor", 128)
        bounded_tuple(self.requirements, EvidenceRequirement, 16, "requirements")
        if not self.requirements or len({r.id for r in self.requirements}) != len(
            self.requirements
        ):
            raise ValueError("unique_requirements_required")
        if type(self.allow_approximate) is not bool:
            raise ValueError("invalid_approximation_policy")
        if self.query.token_budget > 32768:
            raise ValueError("research_context_budget_exceeded")
        if self.registered_question is not None:
            bounded_text(self.registered_question, "registered_question", 512)

    @property
    def fingerprint(self):
        return digest(asdict(self))


@dataclass(frozen=True, slots=True)
class EvidenceGap:
    requirement_id: str
    route: str = "native"

    def __post_init__(self):
        bounded_text(self.requirement_id, "gap_requirement", 128)
        if self.route not in ("native", "semantic", "census"):
            raise ValueError("invalid_gap_route")


@dataclass(frozen=True, slots=True)
class EvidenceUse:
    requirement_id: str
    evidence_keys: tuple[str, ...]

    def __post_init__(self):
        bounded_text(self.requirement_id, "support_requirement", 128)
        bounded_tuple(self.evidence_keys, str, 100, "evidence_keys")
        if len(set(self.evidence_keys)) != len(self.evidence_keys):
            raise ValueError("duplicate_evidence_keys")
        for key in self.evidence_keys:
            bounded_text(key, "evidence_key", 128)


@dataclass(frozen=True, slots=True)
class ReaderAssessment:
    """Untrusted reader proposal; text and references are not semantic proof."""

    answer: str = ""
    supports: tuple[EvidenceUse, ...] = ()
    gaps: tuple[EvidenceGap, ...] = ()
    model_calls: int = 0
    input_tokens: int | None = None
    output_tokens: int | None = None

    def __post_init__(self):
        bounded_text(self.answer, "answer", 8192, empty=True)
        bounded_tuple(self.supports, EvidenceUse, 16, "supports")
        bounded_tuple(self.gaps, EvidenceGap, 16, "gaps")
        if type(self.model_calls) is not int or not 0 <= self.model_calls <= 1:
            raise ValueError("reader_must_make_at_most_one_accounted_model_call")
        validate_usage(self.input_tokens, self.output_tokens)


@dataclass(frozen=True, slots=True)
class ResearchPlan:
    gaps: tuple[EvidenceGap, ...]
    model_calls: int = 0
    input_tokens: int | None = None
    output_tokens: int | None = None

    def __post_init__(self):
        bounded_tuple(self.gaps, EvidenceGap, 16, "planned_gaps")
        if type(self.model_calls) is not int or not 0 <= self.model_calls <= 1:
            raise ValueError("planner_must_make_at_most_one_accounted_model_call")
        validate_usage(self.input_tokens, self.output_tokens)


@dataclass(frozen=True, slots=True)
class CallAllowance:
    """Per-invocation upper reservation, retained on failure and unknown usage.

    Input uses UTF-8 byte units as a conservative transport reservation; output
    is a provider max-token cap. Adapters must enforce the output cap and may
    make at most one model call. This is not a currency/full-resource ledger.
    """

    call_id: str
    input_tokens: int
    max_output_tokens: int
    max_model_calls: int
    remaining_ms: int


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    key: str
    source_id: str
    text: str
    text_sha256: str
    source_revision: str | None
    occurred_at: str | None = None


@dataclass(frozen=True, slots=True)
class CoverageReceipt:
    """Only a backend validation can authorize this data as complete coverage."""

    requirement_id: str
    contract_fingerprint: str
    domain: str
    snapshot_sha256: str
    source_versions: tuple[tuple[str, str], ...]
    evidence_keys: tuple[str, ...]
    authority_id: str
    signature: str

    def __post_init__(self):
        for name in ("requirement_id", "domain", "authority_id"):
            bounded_text(getattr(self, name), name, 128)
        for value in (self.contract_fingerprint, self.snapshot_sha256, self.signature):
            if value != "" and (
                type(value) is not str
                or len(value) != 64
                or any(c not in "0123456789abcdef" for c in value)
            ):
                raise ValueError("invalid_coverage_digest")
        bounded_tuple(self.source_versions, tuple, 100, "coverage_sources")
        for member in self.source_versions:
            if len(member) != 2:
                raise ValueError("invalid_coverage_source_version")
            bounded_text(member[0], "coverage_source_id", 128)
            bounded_text(member[1], "coverage_source_version", 64)
        bounded_tuple(self.evidence_keys, str, 100, "coverage_evidence")
        if len(set(self.evidence_keys)) != len(self.evidence_keys) or len(
            {s[0] for s in self.source_versions}
        ) != len(self.source_versions):
            raise ValueError("duplicate_coverage_members")
        for key in self.evidence_keys:
            bounded_text(key, "coverage_evidence_key", 64)


@dataclass(frozen=True, slots=True)
class TemporalResolution:
    """Trusted host extraction/computation, never parsed from reader JSON.

    Bind to the exact delivered evidence text hashes. A source timestamp alone
    does not establish event time. Hosts remain responsible for event semantics.
    """

    requirement_id: str
    value: str
    precision: str
    approximate: bool
    evidence_keys: tuple[str, ...]

    def __post_init__(self):
        bounded_text(self.requirement_id, "temporal_requirement", 128)
        bounded_text(self.value, "temporal_value", 1024)
        if self.precision not in ("day", "month", "year") or type(self.approximate) is not bool:
            raise ValueError("invalid_temporal_resolution")
        bounded_tuple(self.evidence_keys, str, 32, "temporal_evidence")
        if not self.evidence_keys:
            raise ValueError("temporal_evidence_required")


@dataclass(frozen=True, slots=True)
class RetrievalBatch:
    bundle: MemoryBundle
    receipts: tuple[CoverageReceipt, ...] = ()
    partial_reason: str | None = None
    model_calls: int = 0
    input_tokens: int | None = None
    output_tokens: int | None = None

    def __post_init__(self):
        if type(self.model_calls) is not int or not 0 <= self.model_calls <= 1:
            raise ValueError("retrieval_must_make_at_most_one_accounted_model_call")
        validate_usage(self.input_tokens, self.output_tokens)


@dataclass(frozen=True, slots=True)
class FinalizedEvidence:
    bundles: tuple[MemoryBundle, ...]
    valid_receipts: tuple[CoverageReceipt, ...] = ()


@dataclass(frozen=True, slots=True)
class ResearchEvent:
    stage: str
    outcome: str
    call_id: str | None = None
    reserved_tokens: int = 0
    evidence_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ResearchResult:
    status: str
    answer: str | None
    reason: str
    evidence: tuple[EvidenceRecord, ...] = ()
    coverage_receipts: tuple[CoverageReceipt, ...] = ()
    missing_requirements: tuple[str, ...] = ()
    events: tuple[ResearchEvent, ...] = ()
    calls: int = 0
    reserved_tokens: int = 0
    model_calls: int = 0
    model_call_reservations: int = 0
    unknown_model_usage_calls: int = 0
    known_input_tokens: int = 0
    known_output_tokens: int = 0
    reservation_basis: str = "utf8_input_bytes_plus_output_token_cap"
    entailment_verified: bool = False
    view_response: Any = None
