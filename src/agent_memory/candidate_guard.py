"""Final, fail-closed policy boundary for fused retrieval candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Sequence

from .candidate_fusion import FusedCandidate
from .domain import MemoryScope
from .scoped_lexical_retrieval import ScopeIsolationError


class CandidateRejection(StrEnum):
    UNTRUSTED = "untrusted"
    ARCHIVED = "archived"
    DELETED = "deleted"
    NOT_YET_VALID = "not_yet_valid"
    EXPIRED = "expired"
    MISSING_PROVENANCE = "missing_provenance"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class GovernedCandidate:
    """Candidate plus authoritative policy fields supplied by its storage adapter."""

    scope: MemoryScope
    candidate: FusedCandidate
    trusted: bool = True
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    archived: bool = False
    deleted: bool = False


@dataclass(frozen=True, slots=True)
class CandidateRejectionRecord:
    memory_id: str
    reason: CandidateRejection


@dataclass(frozen=True, slots=True)
class CandidateGuardResult:
    accepted: tuple[FusedCandidate, ...]
    rejected: tuple[CandidateRejectionRecord, ...]
    input_count: int


def guard_candidates(
    scope: MemoryScope,
    records: Sequence[GovernedCandidate],
    *,
    now: datetime,
    max_candidates: int = 256,
) -> CandidateGuardResult:
    """Apply final policy checks before diversity selection or bundle packing.

    Cross-scope and malformed records fail the whole request. Ordinary policy
    exclusions are recorded and omitted so callers can expose degradation traces.
    """
    if not isinstance(scope, MemoryScope):
        raise TypeError("scope must be a MemoryScope")
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise TypeError("records must be a bounded sequence")
    if type(max_candidates) is not int or not 1 <= max_candidates <= 256:
        raise ValueError("max_candidates must be between 1 and 256")
    if len(records) > max_candidates:
        raise ValueError("candidate input exceeds max_candidates")

    grouped: dict[str, list[GovernedCandidate]] = {}
    for record in records:
        if not isinstance(record, GovernedCandidate):
            raise ScopeIsolationError("candidate is missing authoritative policy fields")
        if record.scope != scope:
            raise ScopeIsolationError("candidate belongs to another scope")
        if not isinstance(record.candidate, FusedCandidate):
            raise TypeError("candidate must be a FusedCandidate")
        for field, value in (("valid_from", record.valid_from), ("valid_to", record.valid_to)):
            if value is not None and (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset() is None
            ):
                raise ValueError(f"{field} must be timezone-aware")
        grouped.setdefault(record.candidate.item.id, []).append(record)

    accepted: list[FusedCandidate] = []
    rejected: list[CandidateRejectionRecord] = []
    for memory_id in sorted(grouped):
        versions = grouped[memory_id]
        first = versions[0]
        if any(
            value.candidate.item != first.candidate.item
            or value.candidate.source_event_ids != first.candidate.source_event_ids
            for value in versions[1:]
        ):
            rejected.append(CandidateRejectionRecord(memory_id, CandidateRejection.CONFLICT))
            continue
        record = max(versions, key=lambda value: value.candidate.score)
        reason: CandidateRejection | None = None
        if record.deleted:
            reason = CandidateRejection.DELETED
        elif record.archived:
            reason = CandidateRejection.ARCHIVED
        elif not record.trusted:
            reason = CandidateRejection.UNTRUSTED
        elif not record.candidate.source_event_ids:
            reason = CandidateRejection.MISSING_PROVENANCE
        elif record.valid_from is not None and now < record.valid_from:
            reason = CandidateRejection.NOT_YET_VALID
        elif record.valid_to is not None and now >= record.valid_to:
            reason = CandidateRejection.EXPIRED
        if reason is None:
            accepted.append(record.candidate)
        else:
            rejected.append(CandidateRejectionRecord(memory_id, reason))

    accepted.sort(key=lambda value: (-value.score, value.item.id))
    rejected.sort(key=lambda value: (value.memory_id, value.reason.value))
    return CandidateGuardResult(tuple(accepted), tuple(rejected), len(records))
