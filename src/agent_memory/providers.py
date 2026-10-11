from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from math import isfinite, sqrt

from .domain import ClaimDraft, MemoryEvent, MemoryItem, MemoryQuery, Provenance, ScopeLevel
from .extraction_result import (
    ExtractionDiagnostic,
    ExtractionError,
    ExtractionFormatError,
    ExtractionProviderError,
    ExtractionRejection,
    ExtractionReport,
)
from .ports import ClaimExtractor, ClaimGenerator, EmbeddingProvider, Reranker


def _claim_time(value: object, fallback=None):
    if value is None:
        return fallback
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("claim times must include a timezone")
    return value


_CLAIM_FIELDS = frozenset(
    {
        "key",
        "value",
        "text",
        "confidence",
        "importance",
        "scope",
        "scope_level",
        "valid_from",
        "valid_to",
        "corrects_id",
    }
)


def _parse_claim(raw, *, generated):
    if not isinstance(raw, Mapping):
        return None, "invalid_candidate"
    # Conditions and provenance quotations require a typed, evidence-bound
    # admission path. A flat claim must never drop them and become unconditional.
    if set(raw) - _CLAIM_FIELDS:
        return None, "unsupported_fields"
    if not isinstance(raw.get("key"), str) or not isinstance(raw.get("text"), str):
        return None, "invalid_candidate"
    if "scope" in raw and "scope_level" in raw and str(raw["scope"]) != str(raw["scope_level"]):
        return None, "conflicting_scope"
    if generated:
        if "value" not in raw:
            return None, "missing_value"
        if "confidence" not in raw:
            return None, "missing_confidence"
        if type(raw["confidence"]) not in (int, float) or not 0 <= raw["confidence"] <= 1:
            return None, "invalid_confidence"
    try:
        draft = ClaimDraft(
            key=raw["key"],
            value=raw.get("value"),
            text=raw["text"],
            confidence=float(raw.get("confidence", 1.0)),
            importance=float(raw.get("importance", 0.5)),
            scope_level=ScopeLevel(
                str(raw.get("scope", raw.get("scope_level", ScopeLevel.SESSION)))
            ),
            valid_from=_claim_time(raw.get("valid_from")),
            valid_to=_claim_time(raw.get("valid_to")),
            corrects_id=raw.get("corrects_id"),
        )
    except (TypeError, ValueError, OverflowError):
        return None, "invalid_candidate"
    return draft, None


def _aborted_diagnostic(diagnostic, *, prior=(), prior_drafts=0):
    """A strict batch admits nothing, even if individual rows validated."""
    rejected = tuple(item for component in prior for item in component.rejections)
    rejected += diagnostic.rejections
    aborted = prior_drafts + diagnostic.accepted_count
    if aborted:
        rejected += (ExtractionRejection(None, "batch_aborted", aborted),)
    components = (*prior, diagnostic) if prior else diagnostic.components
    counts = [component.candidate_count for component in (*prior, diagnostic)]
    return ExtractionDiagnostic(
        "error",
        sum(counts) if all(count is not None for count in counts) else None,
        0,
        rejected,
        diagnostic.error_type,
        tuple(components),
    )


class MetadataClaimExtractor:
    """Trusted deterministic extractor for SDK and adapter integrations."""

    async def extract(self, event: MemoryEvent) -> Sequence[ClaimDraft]:
        return (await self.extract_with_report(event)).drafts

    async def extract_with_report(self, event: MemoryEvent) -> ExtractionReport:
        if "claims" not in event.metadata:
            return ExtractionReport((), ExtractionDiagnostic("skipped", candidate_count=0))
        raw_claims = event.metadata.get("claims", ())
        if not isinstance(raw_claims, Sequence) or isinstance(raw_claims, (str, bytes)):
            return ExtractionReport(
                (),
                ExtractionDiagnostic(
                    "error",
                    rejections=(ExtractionRejection(None, "invalid_output"),),
                    error_type="ExtractionFormatError",
                ),
            )
        claims: list[ClaimDraft] = []
        rejected = []
        for index, raw in enumerate(raw_claims):
            draft, reason = _parse_claim(raw, generated=False)
            if reason is not None:
                rejected.append(ExtractionRejection(index, reason))
            else:
                claims.append(draft)
        status = (
            ("degraded" if claims else "error") if rejected else "completed" if claims else "empty"
        )
        return ExtractionReport(
            tuple(claims),
            ExtractionDiagnostic(
                status,
                len(raw_claims),
                len(claims),
                tuple(rejected),
                "ExtractionFormatError" if rejected else None,
            ),
        )


class CompositeClaimExtractor:
    """Combine extractors in priority order and optionally isolate provider failures."""

    def __init__(self, *extractors: ClaimExtractor, fail_open: bool = True) -> None:
        if not extractors:
            raise ValueError("at least one claim extractor is required")
        self._extractors = extractors
        self._fail_open = fail_open

    async def extract(self, event: MemoryEvent) -> Sequence[ClaimDraft]:
        return (await self.extract_with_report(event)).drafts

    async def extract_with_report(self, event: MemoryEvent) -> ExtractionReport:
        drafts: list[ClaimDraft] = []
        components = []
        for extractor in self._extractors:
            try:
                with_report = getattr(extractor, "extract_with_report", None)
                if callable(with_report):
                    report = await with_report(event)
                    if not isinstance(report, ExtractionReport):
                        raise ExtractionFormatError(
                            ExtractionDiagnostic("error", error_type="ExtractionFormatError")
                        )
                else:
                    values = await extractor.extract(event)
                    if (
                        not isinstance(values, Sequence)
                        or isinstance(values, (str, bytes))
                        or not all(isinstance(value, ClaimDraft) for value in values)
                    ):
                        raise ExtractionFormatError(
                            ExtractionDiagnostic("error", error_type="ExtractionFormatError")
                        )
                    report = ExtractionReport(
                        tuple(values),
                        ExtractionDiagnostic(
                            "completed" if values else "empty",
                            len(values),
                            len(values),
                        ),
                    )
                if not self._fail_open and report.diagnostic.status in ("error", "degraded"):
                    raise ExtractionFormatError(report.diagnostic)
                drafts.extend(report.drafts)
                components.append(report.diagnostic)
            except Exception as error:
                if not self._fail_open:
                    diagnostic = (
                        error.diagnostic
                        if isinstance(error, ExtractionError)
                        else ExtractionDiagnostic("error", error_type=type(error).__name__)
                    )
                    aborted = _aborted_diagnostic(
                        diagnostic, prior=components, prior_drafts=len(drafts)
                    )
                    failure = (
                        ExtractionFormatError
                        if isinstance(error, ExtractionFormatError)
                        else ExtractionProviderError
                    )
                    raise failure(aborted) from error
                components.append(
                    error.diagnostic
                    if isinstance(error, ExtractionError)
                    else ExtractionDiagnostic("error", error_type=type(error).__name__)
                )
        rejected = tuple(item for component in components for item in component.rejections)
        degraded = any(component.status in ("error", "degraded") for component in components)
        count = (
            sum(component.candidate_count for component in components)
            if all(component.candidate_count is not None for component in components)
            else None
        )
        status = (
            ("degraded" if drafts else "error")
            if degraded
            else "completed"
            if drafts
            else "rejected"
            if rejected
            else "skipped"
            if all(component.status == "skipped" for component in components)
            else "empty"
        )
        return ExtractionReport(
            tuple(drafts),
            ExtractionDiagnostic(
                status,
                count,
                len(drafts),
                rejected,
                next(
                    (component.error_type for component in components if component.error_type), None
                ),
                tuple(components),
            ),
        )


class GeneratedTrajectoryClaimExtractor:
    """Validate structured model output before it enters the memory write policy."""

    DEFAULT_EVENT_TYPES = frozenset(
        {
            "user.message",
            "agent.model.completed",
            "agent.tool.completed",
        }
    )

    def __init__(
        self,
        generator: ClaimGenerator,
        *,
        provider: str = "external-generator",
        model: str | None = None,
        prompt_version: str = "trajectory-claims-v1",
        minimum_confidence: float = 0.8,
        max_claims: int = 8,
        event_types: Sequence[str] | None = None,
        fail_open: bool = True,
    ) -> None:
        if type(minimum_confidence) not in (int, float) or not 0.0 <= minimum_confidence <= 1.0:
            raise ValueError("minimum_confidence must be between 0 and 1")
        if type(max_claims) is not int or max_claims < 1:
            raise ValueError("max_claims must be positive")
        self._generator = generator
        self._provider = provider
        self._model = model
        self._prompt_version = prompt_version
        self._minimum_confidence = minimum_confidence
        self._max_claims = max_claims
        self._event_types = frozenset(
            self.DEFAULT_EVENT_TYPES if event_types is None else event_types
        )
        self._fail_open = fail_open

    async def extract(self, event: MemoryEvent) -> Sequence[ClaimDraft]:
        return (await self.extract_with_report(event)).drafts

    async def extract_with_report(self, event: MemoryEvent) -> ExtractionReport:
        if event.event_type not in self._event_types:
            return ExtractionReport((), ExtractionDiagnostic("skipped", candidate_count=0))
        try:
            generated = await self._generator.generate_claims(event)
        except Exception as error:
            diagnostic = ExtractionDiagnostic("error", error_type=type(error).__name__)
            if self._fail_open:
                return ExtractionReport((), diagnostic)
            raise ExtractionProviderError(diagnostic) from error
        if not isinstance(generated, Sequence) or isinstance(generated, (str, bytes)):
            diagnostic = ExtractionDiagnostic(
                "error",
                rejections=(ExtractionRejection(None, "invalid_output"),),
                error_type="ExtractionFormatError",
            )
            if not self._fail_open:
                raise ExtractionFormatError(diagnostic)
            return ExtractionReport((), diagnostic)
        accepted: list[ClaimDraft] = []
        rejected = []
        malformed = False
        for index, raw in enumerate(generated[: self._max_claims]):
            draft, reason = _parse_claim(raw, generated=True)
            if reason is not None:
                rejected.append(ExtractionRejection(index, reason))
                malformed = True
                continue
            if draft.confidence < self._minimum_confidence:
                rejected.append(ExtractionRejection(index, "low_confidence"))
                continue
            try:
                event.scope.project(draft.scope_level)
            except ValueError:
                rejected.append(ExtractionRejection(index, "scope_unavailable"))
                continue
            accepted.append(
                replace(
                    draft,
                    provenance=Provenance(
                        source_event_ids=(event.id,),
                        extractor=type(self).__name__,
                        provider=self._provider,
                        model=self._model,
                        prompt_version=self._prompt_version,
                        source_uri=event.source_uri,
                    ),
                )
            )
        if len(generated) > self._max_claims:
            rejected.append(
                ExtractionRejection(
                    self._max_claims, "candidate_limit", len(generated) - self._max_claims
                )
            )
        diagnostic = ExtractionDiagnostic(
            ("degraded" if accepted else "error")
            if malformed
            else "completed"
            if accepted
            else "rejected"
            if rejected
            else "empty",
            len(generated),
            len(accepted),
            tuple(rejected),
            "ExtractionFormatError" if malformed else None,
        )
        if malformed and not self._fail_open:
            raise ExtractionFormatError(_aborted_diagnostic(diagnostic))
        return ExtractionReport(tuple(accepted), diagnostic)


def build_trajectory_extractor(
    generator: ClaimGenerator,
    **config: object,
) -> CompositeClaimExtractor:
    """Preserve trusted explicit claims while adding automatic trajectory extraction."""

    return CompositeClaimExtractor(
        MetadataClaimExtractor(),
        GeneratedTrajectoryClaimExtractor(generator, **config),
        fail_open=config.get("fail_open", True),
    )


class TrustedMemoryPolicy:
    async def should_extract(self, event: MemoryEvent) -> bool:
        return True

    async def accept_claim(self, event: MemoryEvent, claim: ClaimDraft) -> bool:
        return claim.confidence >= 0.5 and bool(claim.text.strip())


class ReciprocalRankFusionReranker:
    """Provider-independent rank fusion that avoids incomparable raw score scales."""

    def __init__(self, k: int = 60) -> None:
        self._k = k

    async def rerank(
        self, query: MemoryQuery, candidates: Sequence[MemoryItem]
    ) -> Sequence[MemoryItem]:
        by_channel: dict[str, list[MemoryItem]] = defaultdict(list)
        for candidate in candidates:
            by_channel[str(candidate.metadata.get("channel", "semantic"))].append(candidate)

        fused: dict[str, float] = defaultdict(float)
        canonical: dict[str, MemoryItem] = {}
        for channel_candidates in by_channel.values():
            ranked = sorted(channel_candidates, key=lambda item: item.score, reverse=True)
            for rank, candidate in enumerate(ranked, start=1):
                fused[candidate.id] += 1.0 / (self._k + rank)
                canonical[candidate.id] = candidate

        return tuple(
            MemoryItem(
                id=canonical[item_id].id,
                kind=canonical[item_id].kind,
                text=canonical[item_id].text,
                score=score,
                occurred_at=canonical[item_id].occurred_at,
                metadata={**canonical[item_id].metadata, "fusion": "rrf"},
            )
            for item_id, score in sorted(fused.items(), key=lambda pair: pair[1], reverse=True)
        )


class EmbeddingReranker:
    """Bounded hybrid reranking over a caller-provided embedding provider.

    Vectors are used only for the active request and are never retained by the core.
    """

    def __init__(
        self,
        embedding_provider: EmbeddingProvider,
        *,
        fallback: Reranker | None = None,
        lexical_weight: float = 0.35,
        semantic_weight: float = 0.65,
        max_candidates: int = 48,
        fail_open: bool = True,
    ) -> None:
        if lexical_weight < 0.0 or semantic_weight < 0.0:
            raise ValueError("reranking weights must be non-negative")
        if lexical_weight + semantic_weight <= 0.0:
            raise ValueError("at least one reranking weight must be positive")
        if not 1 <= max_candidates <= 500:
            raise ValueError("max_candidates must be between 1 and 500")
        self._embedding_provider = embedding_provider
        self._fallback = fallback or ReciprocalRankFusionReranker()
        self._lexical_weight = lexical_weight
        self._semantic_weight = semantic_weight
        self._max_candidates = max_candidates
        self._fail_open = fail_open

    async def rerank(
        self, query: MemoryQuery, candidates: Sequence[MemoryItem]
    ) -> Sequence[MemoryItem]:
        baseline = tuple(await self._fallback.rerank(query, candidates))
        selected = baseline[: self._max_candidates]
        if not query.text.strip() or len(selected) < 2:
            return baseline

        try:
            vectors = await self._embedding_provider.embed(
                (query.text, *(candidate.text for candidate in selected))
            )
            if len(vectors) != len(selected) + 1:
                raise ValueError("embedding provider returned an unexpected vector count")
            query_vector = vectors[0]
            reranked: list[MemoryItem] = []
            denominator = max(1, len(selected) - 1)
            total_weight = self._lexical_weight + self._semantic_weight
            for index, (candidate, vector) in enumerate(zip(selected, vectors[1:], strict=True)):
                lexical_score = 1.0 - (index / denominator)
                semantic_score = (self._cosine_similarity(query_vector, vector) + 1.0) / 2.0
                score = (
                    self._lexical_weight * lexical_score
                    + self._semantic_weight * semantic_score
                ) / total_weight
                reranked.append(
                    MemoryItem(
                        id=candidate.id,
                        kind=candidate.kind,
                        text=candidate.text,
                        score=score,
                        occurred_at=candidate.occurred_at,
                        metadata={
                            **candidate.metadata,
                            "semantic_score": semantic_score,
                            "semantic_dimensions": len(query_vector),
                            "semantic_reranker": type(self).__name__,
                        },
                    )
                )
            return tuple(sorted(reranked, key=lambda item: item.score, reverse=True)) + baseline[
                self._max_candidates :
            ]
        except (TypeError, ValueError, ArithmeticError):
            if self._fail_open:
                return baseline
            raise
        except Exception:
            if self._fail_open:
                return baseline
            raise

    @staticmethod
    def _cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
        if not left or len(left) != len(right):
            raise ValueError("embedding vectors must be non-empty and have equal dimensions")
        left_values = tuple(float(value) for value in left)
        right_values = tuple(float(value) for value in right)
        if not all(isfinite(value) for value in (*left_values, *right_values)):
            raise ValueError("embedding vectors must contain finite values")
        left_norm = sqrt(sum(value * value for value in left_values))
        right_norm = sqrt(sum(value * value for value in right_values))
        if left_norm == 0.0 or right_norm == 0.0:
            raise ValueError("embedding vectors must not be zero vectors")
        similarity = sum(
            left * right for left, right in zip(left_values, right_values, strict=True)
        )
        return max(-1.0, min(1.0, similarity / (left_norm * right_norm)))
