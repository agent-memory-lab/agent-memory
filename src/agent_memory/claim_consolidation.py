from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from hashlib import sha256

from .domain import (
    Claim,
    ClaimEvidenceUpdate,
    ClaimProposalOperation,
    ClaimStatus,
    MemoryEvent,
    MemoryProposal,
    MemoryScope,
    ScopeLevel,
    canonical_json,
)
from .plugin_protocol import (
    ConsolidationRequest,
    ConsolidationResult,
    PluginContext,
    PluginHealth,
    PluginHealthStatus,
)
from .plugins import (
    PluginError,
    PluginErrorCode,
    PluginFailureMode,
    PluginKind,
    PluginManifest,
    PluginResourceLimits,
)


class EvidenceTrust(StrEnum):
    UNTRUSTED = "untrusted"
    DERIVED = "derived"
    OBSERVED = "observed"
    TRUSTED = "trusted"
    AUTHORITATIVE = "authoritative"


_TRUST_RANK = {
    EvidenceTrust.UNTRUSTED: 0,
    EvidenceTrust.DERIVED: 1,
    EvidenceTrust.OBSERVED: 2,
    EvidenceTrust.TRUSTED: 3,
    EvidenceTrust.AUTHORITATIVE: 4,
}


@dataclass(frozen=True, slots=True)
class ClaimConsolidationLimits:
    max_claims: int = 256
    max_sources_per_claim: int = 128
    max_proposals: int = 100

    def __post_init__(self) -> None:
        bounds = {
            "max_claims": (self.max_claims, 1, 10_000),
            "max_sources_per_claim": (self.max_sources_per_claim, 1, 10_000),
            "max_proposals": (self.max_proposals, 1, 1_000),
        }
        for name, (value, minimum, maximum) in bounds.items():
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(f"{name} must be between {minimum} and {maximum}")


class ClaimConsolidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ClaimConsolidationPlan:
    proposals: tuple[MemoryProposal, ...]
    evidence_updates: tuple[ClaimEvidenceUpdate, ...]


@dataclass(frozen=True, slots=True)
class _ClaimView:
    claim: Claim
    source_event_ids: tuple[str, ...]
    trust: EvidenceTrust


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


def _trust_label(event: MemoryEvent) -> EvidenceTrust:
    explicit = event.metadata.get("trust_label") if isinstance(event.metadata, Mapping) else None
    if isinstance(explicit, str):
        try:
            return EvidenceTrust(explicit)
        except ValueError as error:
            raise ClaimConsolidationError(f"unknown trust label: {explicit}") from error
    lifecycle = event.metadata.get("lifecycle") if isinstance(event.metadata, Mapping) else None
    origin = lifecycle.get("origin") if isinstance(lifecycle, Mapping) else None
    if origin == "host" or event.actor in {"host", "human"}:
        return EvidenceTrust.TRUSTED
    if origin == "model":
        return EvidenceTrust.DERIVED
    return EvidenceTrust.OBSERVED


def _scope_level(root: MemoryScope, target: MemoryScope) -> ScopeLevel:
    for level in ScopeLevel:
        try:
            if root.project(level) == target:
                return level
        except ValueError:
            continue
    raise ClaimConsolidationError("claim scope is outside the consolidation scope")


def _proposal_id(
    claim_scope: MemoryScope,
    key: str,
    operation: ClaimProposalOperation,
    value: object,
    source_event_ids: tuple[str, ...],
    expected_version: int,
) -> str:
    payload = (
        claim_scope.partition_key(),
        key,
        operation.value,
        value,
        source_event_ids,
        expected_version,
    )
    return f"claim-proposal-{sha256(canonical_json(payload).encode()).hexdigest()[:32]}"


def _rank(view: _ClaimView) -> tuple[int, datetime, datetime, str]:
    return (
        _TRUST_RANK[view.trust],
        view.claim.valid_from,
        view.claim.created_at,
        view.claim.id,
    )


def _same_decision_rank(left: _ClaimView, right: _ClaimView) -> bool:
    return (
        _TRUST_RANK[left.trust],
        left.claim.valid_from,
    ) == (
        _TRUST_RANK[right.trust],
        right.claim.valid_from,
    )


def _make_proposal(
    request_scope: MemoryScope,
    view: _ClaimView,
    *,
    operation: ClaimProposalOperation,
    expected_version: int,
    source_event_ids: tuple[str, ...],
    reason: str,
) -> MemoryProposal:
    claim = view.claim
    return MemoryProposal(
        id=_proposal_id(
            claim.scope,
            claim.key,
            operation,
            claim.value,
            source_event_ids,
            expected_version,
        ),
        scope=request_scope,
        key=claim.key,
        value=claim.value,
        text=claim.text,
        source_event_ids=source_event_ids,
        expected_version=expected_version,
        scope_level=_scope_level(request_scope, claim.scope),
        confidence=claim.confidence,
        importance=claim.importance,
        operation=operation,
        valid_from=claim.valid_from,
        reason=reason,
        created_at=claim.created_at,
    )


def consolidate_claim_proposals(
    request: ConsolidationRequest,
    *,
    limits: ClaimConsolidationLimits = ClaimConsolidationLimits(),
) -> ClaimConsolidationPlan:
    """Create bounded Claim proposals without mutating Current State."""

    if len(request.claims) > limits.max_claims:
        raise ClaimConsolidationError("claim input exceeds max_claims")
    deleted = set(request.deleted_event_ids)
    if any(not value for value in deleted):
        raise ClaimConsolidationError("deleted event IDs must not be empty")
    events = {event.id: event for event in request.events}
    if any(event.scope != request.scope for event in events.values()):
        raise ClaimConsolidationError("event scope does not match consolidation scope")

    groups: dict[tuple[str, str], list[_ClaimView]] = {}
    evidence_updates: list[ClaimEvidenceUpdate] = []
    for claim in request.claims:
        _scope_level(request.scope, claim.scope)
        if claim.status not in {ClaimStatus.ACTIVE, ClaimStatus.CANDIDATE}:
            continue
        sources = _unique(claim.provenance.source_event_ids)
        if len(sources) > limits.max_sources_per_claim:
            raise ClaimConsolidationError("claim source input exceeds max_sources_per_claim")
        removed = tuple(source for source in sources if source in deleted)
        untrusted = tuple(
            source
            for source in sources
            if source not in deleted
            and source in events
            and _trust_label(events[source]) is EvidenceTrust.UNTRUSTED
        )
        retained = tuple(
            source for source in sources if source not in deleted and source not in untrusted
        )
        if removed or untrusted:
            evidence_updates.append(
                ClaimEvidenceUpdate(
                    scope=claim.scope,
                    claim_id=claim.id,
                    expected_version=claim.version,
                    retained_source_event_ids=retained,
                    removed_source_event_ids=removed,
                    excluded_untrusted_event_ids=untrusted,
                )
            )
        if not retained:
            continue
        labels = [_trust_label(events[source]) for source in retained if source in events]
        trust = max(labels, key=_TRUST_RANK.__getitem__) if labels else EvidenceTrust.OBSERVED
        groups.setdefault((claim.scope.partition_key(), claim.key), []).append(
            _ClaimView(claim, retained, trust)
        )

    proposals: list[MemoryProposal] = []
    for group_key in sorted(groups):
        views = groups[group_key]
        active = [view for view in views if view.claim.status is ClaimStatus.ACTIVE]
        if len(active) > 1:
            raise ClaimConsolidationError("multiple active claims found for one scope and key")
        current = active[0] if active else None
        candidates = [view for view in views if view.claim.status is ClaimStatus.CANDIDATE]

        if current is not None:
            same_value = [
                view for view in candidates if canonical_json(view.claim.value) == canonical_json(current.claim.value)
            ]
            merged_sources = _unique(
                source
                for view in (current, *same_value)
                for source in view.source_event_ids
            )
            if merged_sources != current.source_event_ids:
                proposals.append(
                    _make_proposal(
                        request.scope,
                        current,
                        operation=ClaimProposalOperation.MERGE,
                        expected_version=current.claim.version,
                        source_event_ids=merged_sources,
                        reason="merge duplicate evidence for the current claim",
                    )
                )
            candidates = [view for view in candidates if view not in same_value]

        if not candidates:
            continue
        candidates.sort(key=_rank, reverse=True)
        best = candidates[0]
        tied = [
            view
            for view in candidates[1:]
            if _same_decision_rank(view, best)
            and canonical_json(view.claim.value) != canonical_json(best.claim.value)
        ]
        expected_version = current.claim.version if current else 0
        conflict_sources = _unique(
            source
            for view in ((current,) if current else ()) + (best, *tied)
            for source in view.source_event_ids
        )
        if tied:
            proposals.append(
                _make_proposal(
                    request.scope,
                    best,
                    operation=ClaimProposalOperation.CONFLICT,
                    expected_version=expected_version,
                    source_event_ids=conflict_sources,
                    reason="equally trusted evidence conflicts at the same valid time",
                )
            )
            continue

        if current is None:
            operation = ClaimProposalOperation.UPSERT
            reason = "create a claim from the strongest evidence"
        elif best.claim.valid_from < current.claim.valid_from:
            operation = ClaimProposalOperation.CONFLICT
            reason = "older valid-time evidence cannot replace the current claim"
        elif _TRUST_RANK[best.trust] < _TRUST_RANK[current.trust]:
            operation = ClaimProposalOperation.CONFLICT
            reason = "lower-trust evidence cannot replace the current claim"
        elif (
            best.claim.valid_from == current.claim.valid_from
            and _TRUST_RANK[best.trust] == _TRUST_RANK[current.trust]
        ):
            operation = ClaimProposalOperation.CONFLICT
            reason = "equally trusted evidence conflicts at the same valid time"
        else:
            operation = ClaimProposalOperation.SUPERSEDE
            reason = "newer or more trusted evidence supersedes the current claim"
        sources = _unique(
            source
            for view in ((current,) if current else ()) + (best,)
            for source in view.source_event_ids
        )
        proposals.append(
            _make_proposal(
                request.scope,
                best,
                operation=operation,
                expected_version=expected_version,
                source_event_ids=sources,
                reason=reason,
            )
        )

    if len(proposals) > limits.max_proposals:
        raise ClaimConsolidationError("claim proposals exceed max_proposals")
    return ClaimConsolidationPlan(tuple(proposals), tuple(evidence_updates))


class DeterministicClaimConsolidator:
    """Zero-dependency Claim consolidator that only emits governed proposals."""

    def __init__(self, limits: ClaimConsolidationLimits | None = None) -> None:
        self._limits = limits or ClaimConsolidationLimits()
        self._context: PluginContext | None = None
        self._closed = False

    def plugin_manifest(self) -> PluginManifest:
        return PluginManifest(
            name="deterministic-claim-consolidator",
            version="0.1.0",
            kind=PluginKind.CONSOLIDATOR,
            capabilities=("memory.consolidate", "memory.claim.consolidate"),
            requires={"core": ">=0.1,<1.0"},
            config_schema={"type": "object", "additionalProperties": False},
            resource_limits=PluginResourceLimits(max_candidates=100, max_batch_size=10_000),
            failure_mode=PluginFailureMode.FALLBACK,
        )

    async def initialize(self, context: PluginContext) -> None:
        if self._context is not None and not self._closed:
            raise PluginError(
                "claim consolidator is already initialized",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        self._context = context
        self._closed = False

    async def health(self) -> PluginHealth:
        if self._context is None or self._closed:
            return PluginHealth(PluginHealthStatus.UNAVAILABLE, "plugin is not active")
        return PluginHealth(PluginHealthStatus.READY)

    async def close(self) -> None:
        self._closed = True

    async def consolidate(
        self, request: ConsolidationRequest, context: PluginContext
    ) -> ConsolidationResult:
        self._require_context(context)
        if request.scope != context.scope:
            raise PluginError(
                "consolidation request is outside the trusted plugin scope",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="scope",
            )
        if len(request.claims) > context.resource_limits.max_batch_size:
            raise PluginError(
                "consolidation request exceeds the plugin batch limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="claims",
            )
        try:
            plan = consolidate_claim_proposals(request, limits=self._limits)
        except ClaimConsolidationError as error:
            raise PluginError(
                str(error), code=PluginErrorCode.INVALID_IMPLEMENTATION
            ) from error
        if len(plan.proposals) > context.resource_limits.max_candidates:
            raise PluginError(
                "claim proposals exceed the plugin candidate limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="claims",
            )
        return ConsolidationResult(
            claims=plan.proposals,
            claim_evidence_updates=plan.evidence_updates,
        )

    def _require_context(self, context: PluginContext) -> None:
        if self._closed or self._context is None:
            raise PluginError("claim consolidator is not active")
        if context is not self._context:
            raise PluginError(
                "plugin operation used a context not issued during initialization",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if context.cancelled or context.expired:
            raise PluginError("claim consolidation was cancelled or expired")
