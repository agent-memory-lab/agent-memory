"""Versioned evidence evaluation over trusted, frozen gold annotations.

An alternative is an AND of exact evidence spans; alternatives are OR-ed.
This scores supplied annotations, not natural-language truth or live ACLs.
The V4 flat-source benchmark remains unchanged.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256

from ..domain import MemoryScope
from ..serialization import to_jsonable

SCORER_VERSION = "evidence-support/1"


def _text(value: str, name: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be a nonempty bounded string")
    return value


def _strings(values: Sequence[str], name: str, maximum: int = 256) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError(f"{name} must be a sequence")
    result = tuple(_text(value, name) for value in values)
    if len(result) > maximum or len(set(result)) != len(result):
        raise ValueError(f"{name} must be bounded and unique")
    return result


def _time(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(UTC)


def _finite(value: float, name: str) -> float:
    if type(value) not in (float, int) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return float(value)


def _digest(value: object) -> str:
    return sha256(
        json.dumps(
            to_jsonable(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class EvidenceSpan:
    """A gold span's identity, semantic fields and two temporal coordinates.

    readable is the fixture's CURRENT safety decision, not a historical ACL.
    source_revision_id alone never counts as a hit for an unrelated span.
    """

    evidence_id: str
    source_revision_id: str
    source_family: str
    scope: MemoryScope
    start: int
    end: int
    quote_sha256: str
    fields: tuple[str, ...]
    valid_from: datetime
    known_from: datetime
    valid_to: datetime | None = None
    known_to: datetime | None = None
    readable: bool = True

    def __post_init__(self) -> None:
        for name in ("evidence_id", "source_revision_id", "source_family"):
            _text(getattr(self, name), name)
        if not isinstance(self.scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope")
        if (
            type(self.start) is not int
            or type(self.end) is not int
            or not 0 <= self.start < self.end
        ):
            raise ValueError("span must use nonempty Unicode code point offsets")
        if (
            not isinstance(self.quote_sha256, str)
            or len(self.quote_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.quote_sha256)
        ):
            raise ValueError("quote_sha256 must be a lowercase SHA-256")
        fields = _strings(self.fields, "fields", 64)
        if not fields:
            raise ValueError("a gold span must support at least one field")
        object.__setattr__(self, "fields", fields)
        for start, end in (("valid_from", "valid_to"), ("known_from", "known_to")):
            object.__setattr__(self, start, _time(getattr(self, start), start))
            finish = getattr(self, end)
            if finish is not None:
                finish = _time(finish, end)
                if finish <= getattr(self, start):
                    raise ValueError(f"{end} must follow {start}")
                object.__setattr__(self, end, finish)
        if type(self.readable) is not bool:
            raise ValueError("readable must be a trusted boolean annotation")

    def eligible(self, scope: MemoryScope, valid_at: datetime, known_at: datetime) -> bool:
        # Gold scopes are already resolved by the fixture author. Broad scopes
        # may be visible to a matching narrower principal, never the reverse.
        return (
            self.readable
            and self.scope.tenant_id == scope.tenant_id
            and self.scope.namespace == scope.namespace
            and all(
                getattr(self.scope, name) in (None, getattr(scope, name))
                for name in ("user_id", "agent_id", "workspace_id", "session_id")
            )
            and self.valid_from <= valid_at
            and (self.valid_to is None or valid_at < self.valid_to)
            and self.known_from <= known_at
            and (self.known_to is None or known_at < self.known_to)
        )


@dataclass(frozen=True, slots=True)
class SupportAlternative:
    evidence_ids: tuple[str, ...]
    minimum_source_families: int = 1

    def __post_init__(self) -> None:
        ids = _strings(self.evidence_ids, "evidence_ids", 128)
        if not ids:
            raise ValueError("support cannot contain an empty AND branch")
        if type(
            self.minimum_source_families
        ) is not int or not 1 <= self.minimum_source_families <= len(ids):
            raise ValueError("minimum_source_families exceeds the branch size")
        object.__setattr__(self, "evidence_ids", ids)


@dataclass(frozen=True, slots=True)
class EvidenceEvalCase:
    case_id: str
    scope: MemoryScope
    query: str
    valid_at: datetime
    known_at: datetime
    answerable: bool
    evidence: tuple[EvidenceSpan, ...]
    alternatives: tuple[SupportAlternative, ...]
    required_fields: tuple[str, ...] = ()
    forbidden_evidence_ids: tuple[str, ...] = ()
    diagnostic_only: bool = False

    def __post_init__(self) -> None:
        _text(self.case_id, "case_id")
        _text(self.query, "query", 4096)
        if not isinstance(self.scope, MemoryScope):
            raise TypeError("scope must be a MemoryScope")
        for name in ("valid_at", "known_at"):
            object.__setattr__(self, name, _time(getattr(self, name), name))
        if type(self.answerable) is not bool or type(self.diagnostic_only) is not bool:
            raise ValueError("answerability and diagnostic_only must be booleans")
        for name, kind, maximum in (
            ("evidence", EvidenceSpan, 256),
            ("alternatives", SupportAlternative, 32),
        ):
            values = getattr(self, name)
            if not isinstance(values, (tuple, list)) or len(values) > maximum:
                raise ValueError(f"{name} must be a bounded sequence")
            if any(not isinstance(value, kind) for value in values):
                raise TypeError(f"{name} contains an invalid value")
            object.__setattr__(self, name, tuple(values))
        by_id = {item.evidence_id: item for item in self.evidence}
        if len(by_id) != len(self.evidence):
            raise ValueError("evidence IDs must be unique")
        required = _strings(self.required_fields, "required_fields", 64)
        forbidden = _strings(self.forbidden_evidence_ids, "forbidden_evidence_ids")
        object.__setattr__(self, "required_fields", required)
        object.__setattr__(self, "forbidden_evidence_ids", forbidden)
        if not set(forbidden) <= by_id.keys():
            raise ValueError("forbidden evidence must be registered in the gold universe")
        signatures = set()
        for branch in self.alternatives:
            refs = set(branch.evidence_ids)
            if not refs <= by_id.keys() or refs & set(forbidden):
                raise ValueError("support references missing or forbidden evidence")
            signature = (frozenset(refs), branch.minimum_source_families)
            if signature in signatures:
                raise ValueError("duplicate support alternative")
            signatures.add(signature)
            fields = {field for ref in refs for field in by_id[ref].fields}
            if not set(required) <= fields:
                raise ValueError("support alternative omits necessary fields")
        has_support = any(self.branch_eligible(branch) for branch in self.alternatives)
        if not self.diagnostic_only and self.answerable != has_support:
            raise ValueError("answerability contradicts qualified support at the query coordinates")

    def branch_eligible(self, branch: SupportAlternative) -> bool:
        by_id = {item.evidence_id: item for item in self.evidence}
        refs = [by_id[ref] for ref in branch.evidence_ids]
        return (
            all(ref.eligible(self.scope, self.valid_at, self.known_at) for ref in refs)
            and len({ref.source_family for ref in refs}) >= branch.minimum_source_families
        )


@dataclass(frozen=True, slots=True)
class EvidenceDataset:
    dataset_id: str
    version: str
    cases: tuple[EvidenceEvalCase, ...]
    annotation_version: str
    synthetic: bool = True

    def __post_init__(self) -> None:
        for name in ("dataset_id", "version", "annotation_version"):
            _text(getattr(self, name), name)
        if not isinstance(self.cases, (tuple, list)) or not 1 <= len(self.cases) <= 10_000:
            raise ValueError("dataset must contain between 1 and 10000 cases")
        if any(not isinstance(case, EvidenceEvalCase) for case in self.cases):
            raise TypeError("invalid dataset case")
        if len({case.case_id for case in self.cases}) != len(self.cases):
            raise ValueError("duplicate case IDs")
        if type(self.synthetic) is not bool:
            raise ValueError("synthetic must be a boolean")
        object.__setattr__(self, "cases", tuple(self.cases))

    @property
    def fingerprint(self) -> str:
        return _digest(self)


@dataclass(frozen=True, slots=True)
class EvidenceObservation:
    """Adapter-normalized output; answer_correct is an independent judge result."""

    evidence_ids: tuple[str, ...]
    status: str
    answer_correct: bool | None = None
    tokens: int = 0
    cost: float = 0.0
    latency_ms: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence_ids", _strings(self.evidence_ids, "evidence_ids", 1024))
        if self.status not in {"answered", "abstained", "error"}:
            raise ValueError("status must be answered, abstained or error")
        if self.answer_correct is not None and type(self.answer_correct) is not bool:
            raise ValueError("answer_correct must be a judge boolean or None")
        if self.status != "answered" and self.answer_correct is not None:
            raise ValueError("only answered observations can have an answer judgment")
        if type(self.tokens) is not int or not 0 <= self.tokens <= 10_000_000:
            raise ValueError("tokens must be a bounded nonnegative integer")
        for name in ("cost", "latency_ms"):
            object.__setattr__(self, name, _finite(getattr(self, name), name))


@dataclass(frozen=True, slots=True)
class EvidenceCaseResult:
    case_id: str
    answerable: bool
    diagnostic_only: bool
    support_complete: bool
    evidence_recall: float | None
    outcome: str
    forbidden_hits: tuple[str, ...]
    unknown_evidence_ids: tuple[str, ...]
    tokens: int
    cost: float
    latency_ms: float

    def __post_init__(self) -> None:
        _text(self.case_id, "case_id")
        for name in ("answerable", "diagnostic_only", "support_complete"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a boolean")
        if self.evidence_recall is not None:
            recall = _finite(self.evidence_recall, "evidence_recall")
            if recall > 1:
                raise ValueError("evidence_recall cannot exceed one")
        if self.support_complete != (self.evidence_recall == 1.0):
            raise ValueError("support_complete disagrees with evidence recall")
        if self.outcome not in {
            "system_error",
            "incorrect_abstention",
            "correct_abstention",
            "incorrect_answer",
            "unjudged_answer",
            "correct_answer",
        }:
            raise ValueError("invalid evaluation outcome")
        if self.outcome in {"correct_answer", "unjudged_answer"} and (
            not self.answerable or not self.support_complete
        ):
            raise ValueError("a supported answer requires an answerable case")
        for name in ("forbidden_hits", "unknown_evidence_ids"):
            object.__setattr__(self, name, _strings(getattr(self, name), name, 1024))
        if type(self.tokens) is not int or not 0 <= self.tokens <= 10_000_000:
            raise ValueError("tokens must be a bounded nonnegative integer")
        for name in ("cost", "latency_ms"):
            object.__setattr__(self, name, _finite(getattr(self, name), name))


def score_evidence_case(
    case: EvidenceEvalCase, observation: EvidenceObservation
) -> EvidenceCaseResult:
    if not isinstance(case, EvidenceEvalCase) or not isinstance(observation, EvidenceObservation):
        raise TypeError("case and observation must use the evidence evaluation contract")
    observed = set(observation.evidence_ids)
    universe = {ref.evidence_id: ref for ref in case.evidence}
    eligible_branches = [branch for branch in case.alternatives if case.branch_eligible(branch)]
    # Fractional recall uses the BEST eligible alternative, never the flattened
    # union that would penalize returning C for (A AND B) OR C.
    recalls = [
        len(observed & set(branch.evidence_ids)) / len(branch.evidence_ids)
        for branch in eligible_branches
    ]
    recall = max(recalls) if recalls else None
    complete = recall == 1.0
    forbidden = set(case.forbidden_evidence_ids)
    forbidden.update(
        ref.evidence_id
        for ref in case.evidence
        if not ref.eligible(
            case.scope,
            case.valid_at,
            case.known_at,
        )
    )
    forbidden_hits = tuple(sorted(observed & forbidden))
    unknown = tuple(sorted(observed - universe.keys()))
    if observation.status == "error":
        outcome = "system_error"
    elif observation.status == "abstained":
        outcome = "incorrect_abstention" if case.answerable else "correct_abstention"
    elif not case.answerable or not complete or observation.answer_correct is False:
        outcome = "incorrect_answer"
    elif observation.answer_correct is None:
        outcome = "unjudged_answer"
    else:
        outcome = "correct_answer"
    return EvidenceCaseResult(
        case.case_id,
        case.answerable,
        case.diagnostic_only,
        complete,
        recall,
        outcome,
        forbidden_hits,
        unknown,
        observation.tokens,
        observation.cost,
        observation.latency_ms,
    )


@dataclass(frozen=True, slots=True)
class EvidenceEvaluationReport:
    dataset_sha256: str
    scorer_version: str
    run_configuration_sha256: str
    results: tuple[EvidenceCaseResult, ...]

    def __post_init__(self) -> None:
        for name in ("dataset_sha256", "run_configuration_sha256"):
            value = getattr(self, name)
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(c not in "0123456789abcdef" for c in value)
            ):
                raise ValueError(f"{name} must be a lowercase SHA-256")
        _text(self.scorer_version, "scorer_version")
        if not isinstance(self.results, (tuple, list)) or not 1 <= len(self.results) <= 10_000:
            raise ValueError("report results must be a nonempty bounded sequence")
        if any(not isinstance(value, EvidenceCaseResult) for value in self.results):
            raise TypeError("invalid report result")
        if len({value.case_id for value in self.results}) != len(self.results):
            raise ValueError("duplicate report case IDs")
        object.__setattr__(self, "results", tuple(self.results))

    @property
    def fingerprint(self) -> str:
        return _digest(self)


def evaluate_evidence(
    dataset: EvidenceDataset,
    observations: Mapping[str, EvidenceObservation],
    *,
    run_configuration_sha256: str,
) -> EvidenceEvaluationReport:
    if not isinstance(dataset, EvidenceDataset) or not isinstance(observations, Mapping):
        raise TypeError("dataset and observations must use the evaluation contract")
    if (
        not isinstance(run_configuration_sha256, str)
        or len(run_configuration_sha256) != 64
        or any(c not in "0123456789abcdef" for c in run_configuration_sha256)
    ):
        raise ValueError("run_configuration_sha256 must be a lowercase SHA-256")
    ids = {case.case_id for case in dataset.cases}
    if set(observations) - ids:
        raise ValueError("observations contain unknown case IDs")
    # Missing execution stays in the denominator, as a system error.
    results = tuple(
        score_evidence_case(
            case,
            observations.get(case.case_id, EvidenceObservation((), "error")),
        )
        for case in sorted(dataset.cases, key=lambda item: item.case_id)
    )
    return EvidenceEvaluationReport(
        dataset.fingerprint, SCORER_VERSION, run_configuration_sha256, results
    )


def evidence_dataset_from_dict(data: Mapping) -> EvidenceDataset:
    """Load the explicit evidence/1 schema; unknown fields are never dropped."""

    def checked(value, required, optional=()):
        if not isinstance(value, Mapping):
            raise ValueError("evaluation document must contain objects")
        if set(value) - set(required) - set(optional) or set(required) - set(value):
            raise ValueError("unknown or missing evaluation fields")
        return dict(value)

    def timestamp(value):
        if not isinstance(value, str):
            raise ValueError("JSON timestamps must be ISO 8601 strings")
        return _time(datetime.fromisoformat(value.replace("Z", "+00:00")), "timestamp")

    def scope(value):
        return MemoryScope(
            **checked(
                value,
                ("tenant_id",),
                (
                    "namespace",
                    "user_id",
                    "agent_id",
                    "workspace_id",
                    "session_id",
                ),
            )
        )

    payload = checked(
        data, ("dataset_id", "version", "cases", "annotation_version"), ("synthetic",)
    )
    if not isinstance(payload["cases"], list) or len(payload["cases"]) > 10_000:
        raise ValueError("cases must be a bounded JSON array")
    cases = []
    for raw_case in payload["cases"]:
        case = checked(
            raw_case,
            (
                "case_id",
                "scope",
                "query",
                "valid_at",
                "known_at",
                "answerable",
                "evidence",
                "alternatives",
            ),
            ("required_fields", "forbidden_evidence_ids", "diagnostic_only"),
        )
        case["scope"] = scope(case["scope"])
        for name in ("valid_at", "known_at"):
            case[name] = timestamp(case[name])
        for name, maximum in (("evidence", 256), ("alternatives", 32)):
            if not isinstance(case[name], list) or len(case[name]) > maximum:
                raise ValueError(f"{name} must be a bounded JSON array")
        spans = []
        for raw_span in case["evidence"]:
            span = checked(
                raw_span,
                (
                    "evidence_id",
                    "source_revision_id",
                    "source_family",
                    "scope",
                    "start",
                    "end",
                    "quote_sha256",
                    "fields",
                    "valid_from",
                    "known_from",
                ),
                ("valid_to", "known_to", "readable"),
            )
            span["scope"] = scope(span["scope"])
            for name in ("valid_from", "valid_to", "known_from", "known_to"):
                if span.get(name) is not None:
                    span[name] = timestamp(span[name])
            spans.append(EvidenceSpan(**span))
        case["evidence"] = tuple(spans)
        case["alternatives"] = tuple(
            SupportAlternative(
                **checked(
                    value,
                    ("evidence_ids",),
                    ("minimum_source_families",),
                )
            )
            for value in case["alternatives"]
        )
        cases.append(EvidenceEvalCase(**case))
    payload["cases"] = tuple(cases)
    return EvidenceDataset(**payload)


def evidence_report_from_dict(data: Mapping) -> EvidenceEvaluationReport:
    if not isinstance(data, Mapping) or set(data) != {
        "dataset_sha256",
        "scorer_version",
        "run_configuration_sha256",
        "results",
    }:
        raise ValueError("unknown or missing report fields")
    if not isinstance(data["results"], list) or not 1 <= len(data["results"]) <= 10_000:
        raise ValueError("results must be a bounded JSON array")
    if any(not isinstance(value, Mapping) for value in data["results"]):
        raise ValueError("result must be an object")
    return EvidenceEvaluationReport(
        **{
            **data,
            "results": tuple(EvidenceCaseResult(**value) for value in data["results"]),
        }
    )
