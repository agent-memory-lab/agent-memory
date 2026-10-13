"""Immutable extraction outcomes; a confidence score is not factual verification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .domain import ClaimDraft


@dataclass(frozen=True, slots=True)
class ExtractionRejection:
    index: int | None
    reason: str
    count: int = 1

    def __post_init__(self):
        if self.index is not None and (type(self.index) is not int or self.index < 0):
            raise ValueError("rejection index must be a nonnegative integer or unknown")
        if (
            not isinstance(self.reason, str)
            or not 1 <= len(self.reason) <= 128
            or not self.reason.isidentifier()
        ):
            raise ValueError("rejection reason must be a bounded code")
        if type(self.count) is not int or self.count < 1:
            raise ValueError("rejection count must be positive")


@dataclass(frozen=True, slots=True)
class ExtractionDiagnostic:
    status: str
    candidate_count: int | None = None
    accepted_count: int = 0
    rejections: tuple[ExtractionRejection, ...] = ()
    error_type: str | None = None
    components: tuple[ExtractionDiagnostic, ...] = ()

    def __post_init__(self):
        if self.status not in ("skipped", "empty", "completed", "rejected", "degraded", "error"):
            raise ValueError("invalid extraction status")
        if self.candidate_count is not None and (
            type(self.candidate_count) is not int or self.candidate_count < 0
        ):
            raise ValueError("candidate_count must be a nonnegative integer or unknown")
        if type(self.accepted_count) is not int or self.accepted_count < 0:
            raise ValueError("accepted_count must be a nonnegative integer")
        if self.candidate_count is not None and self.accepted_count > self.candidate_count:
            raise ValueError("accepted count cannot exceed candidate count")
        if self.error_type is not None and (
            not isinstance(self.error_type, str)
            or not 1 <= len(self.error_type) <= 128
            or not self.error_type.isidentifier()
        ):
            raise ValueError("error_type must be a bounded type code")
        if not isinstance(self.rejections, tuple) or not all(
            isinstance(item, ExtractionRejection) for item in self.rejections
        ):
            raise ValueError("rejections must be immutable diagnostic records")
        if not isinstance(self.components, tuple) or not all(
            isinstance(item, ExtractionDiagnostic) for item in self.components
        ):
            raise ValueError("components must be immutable diagnostic records")

    @property
    def successful(self):
        """Completed validation, including genuine empty or quality-rejected output."""
        return self.status in ("empty", "completed", "rejected")

    @property
    def rejected_count(self):
        return sum(rejection.count for rejection in self.rejections)

    @classmethod
    def from_dict(cls, value):
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise ValueError("invalid persisted extraction diagnostic")
        return cls(
            status=value["status"],
            candidate_count=value.get("candidate_count"),
            accepted_count=value.get("accepted_count", 0),
            rejections=tuple(ExtractionRejection(**item) for item in value.get("rejections", ())),
            error_type=value.get("error_type"),
            components=tuple(cls.from_dict(item) for item in value.get("components", ())),
        )


@dataclass(frozen=True, slots=True)
class ExtractionReport:
    drafts: tuple[ClaimDraft, ...]
    diagnostic: ExtractionDiagnostic

    def __post_init__(self):
        from .domain import ClaimDraft

        if not isinstance(self.drafts, tuple) or not all(
            isinstance(draft, ClaimDraft) for draft in self.drafts
        ):
            raise ValueError("extraction drafts must be immutable ClaimDraft records")
        if not isinstance(self.diagnostic, ExtractionDiagnostic):
            raise ValueError("extraction report requires a typed diagnostic")
        if self.diagnostic.accepted_count != len(self.drafts):
            raise ValueError("extraction accepted count must match its drafts")


class ExtractionError(Exception):
    def __init__(self, diagnostic: ExtractionDiagnostic):
        super().__init__("claim extraction did not complete validation")
        self.diagnostic = diagnostic


class ExtractionFormatError(ExtractionError, ValueError):
    pass


class ExtractionProviderError(ExtractionError, RuntimeError):
    pass
