from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256

from .domain import (
    ArtifactStatus,
    Episode,
    FeedbackStatus,
    OutcomeStatus,
    Procedure,
    ProcedureInductionRejection,
    Provenance,
    RewardSignal,
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


EXTRACTOR_VERSION = "deterministic-procedure-inducer-v1"


@dataclass(frozen=True, slots=True)
class ProcedureInductionLimits:
    minimum_successful_episodes: int = 3
    minimum_quality: float = 0.75
    max_episodes: int = 256
    max_groups: int = 100
    max_conditions: int = 4
    max_failure_patterns: int = 4
    max_sources: int = 256
    max_text_chars: int = 512

    def __post_init__(self) -> None:
        if type(self.minimum_successful_episodes) is not int or not 2 <= self.minimum_successful_episodes <= 100:
            raise ValueError("minimum_successful_episodes must be between 2 and 100")
        if not 0.0 <= self.minimum_quality <= 1.0:
            raise ValueError("minimum_quality must be between zero and one")
        bounds = {
            "max_episodes": (self.max_episodes, 2, 10_000),
            "max_groups": (self.max_groups, 1, 1_000),
            "max_conditions": (self.max_conditions, 1, 32),
            "max_failure_patterns": (self.max_failure_patterns, 1, 32),
            "max_sources": (self.max_sources, 1, 10_000),
            "max_text_chars": (self.max_text_chars, 32, 8_192),
        }
        for name, (value, minimum, maximum) in bounds.items():
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(f"{name} must be between {minimum} and {maximum}")


class ProcedureInductionError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ProcedureInductionPlan:
    procedures: tuple[Procedure, ...]
    rejections: tuple[ProcedureInductionRejection, ...]


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


def _clip(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(1, limit - 1)].rstrip() + "…"


def _active_rewards(
    rewards: Sequence[RewardSignal], now: datetime
) -> tuple[RewardSignal, ...]:
    accepted = [
        reward
        for reward in rewards
        if reward.feedback_status is FeedbackStatus.ACCEPTED
        and (reward.expires_at is None or reward.expires_at > now)
    ]
    corrected = {reward.corrects_id for reward in accepted if reward.corrects_id}
    return tuple(reward for reward in accepted if reward.id not in corrected)


def _evaluation_key(
    episodes: Sequence[Episode], rewards_by_outcome: Mapping[str, RewardSignal]
) -> tuple[str, str] | None | str:
    if not rewards_by_outcome:
        return None
    keys: set[tuple[str, str]] = set()
    for episode in episodes:
        if not episode.outcome_ids:
            return "incomplete"
        matched = [
            rewards_by_outcome[outcome_id]
            for outcome_id in episode.outcome_ids
            if outcome_id in rewards_by_outcome
        ]
        if len(matched) != 1:
            return "incomplete"
        reward = matched[0]
        keys.add((reward.reward_definition_id, reward.formula_version))
    if len(keys) != 1:
        return "incompatible"
    return next(iter(keys))


def _procedure_id(
    partition_key: str,
    group_key: str,
    episode_ids: tuple[str, ...],
    evaluation_key: tuple[str, str] | None,
) -> str:
    payload = (partition_key, group_key, episode_ids, evaluation_key, EXTRACTOR_VERSION)
    return f"procedure-{sha256(canonical_json(payload).encode()).hexdigest()[:32]}"


def induce_procedure_candidates(
    request: ConsolidationRequest,
    *,
    limits: ProcedureInductionLimits = ProcedureInductionLimits(),
    now: datetime,
) -> ProcedureInductionPlan:
    """Induce bounded candidate Procedures from trusted, outcome-labelled Episodes."""

    if len(request.episodes) > limits.max_episodes:
        raise ProcedureInductionError("episode input exceeds max_episodes")
    if any(episode.scope != request.scope for episode in request.episodes):
        raise ProcedureInductionError("episode scope does not match consolidation scope")
    if any(reward.scope != request.scope for reward in request.rewards):
        raise ProcedureInductionError("reward scope does not match consolidation scope")

    deleted = set(request.deleted_event_ids)
    eligible = [
        episode
        for episode in request.episodes
        if episode.status in {ArtifactStatus.CANDIDATE, ArtifactStatus.ACTIVE}
        and episode.provenance.source_event_ids
        and not deleted.intersection(episode.provenance.source_event_ids)
    ]
    groups: dict[str, list[Episode]] = {}
    for episode in eligible:
        key = " ".join(episode.action.casefold().split())
        if key:
            groups.setdefault(key, []).append(episode)
    if len(groups) > limits.max_groups:
        raise ProcedureInductionError("episode groups exceed max_groups")

    rewards = _active_rewards(request.rewards, now)
    rewards_by_outcome: dict[str, RewardSignal] = {}
    for reward in sorted(rewards, key=lambda item: (item.created_at, item.id)):
        if reward.outcome_id in rewards_by_outcome:
            raise ProcedureInductionError("multiple active rewards found for one outcome")
        rewards_by_outcome[reward.outcome_id] = reward

    procedures: list[Procedure] = []
    rejections: list[ProcedureInductionRejection] = []
    for group_key in sorted(groups):
        members = sorted(groups[group_key], key=lambda item: (item.occurred_at, item.id))
        member_ids = tuple(item.id for item in members)
        if any(item.outcome_status is OutcomeStatus.UNKNOWN for item in members):
            rejections.append(
                ProcedureInductionRejection(group_key, "unknown_outcome", member_ids)
            )
            continue
        evaluation_key = _evaluation_key(members, rewards_by_outcome)
        if evaluation_key in {"incomplete", "incompatible"}:
            rejections.append(
                ProcedureInductionRejection(
                    group_key,
                    f"{evaluation_key}_evaluation",
                    member_ids,
                )
            )
            continue
        successful = [
            item
            for item in members
            if item.outcome_status is OutcomeStatus.SUCCEEDED
            and item.quality >= limits.minimum_quality
        ]
        if len(successful) < limits.minimum_successful_episodes:
            rejections.append(
                ProcedureInductionRejection(group_key, "insufficient_support", member_ids)
            )
            continue
        counterexamples = [
            item for item in members if item.outcome_status is not OutcomeStatus.SUCCEEDED
        ]
        source_event_ids = _unique(
            event_id
            for episode in members
            for event_id in episode.provenance.source_event_ids
        )
        if not source_event_ids:
            rejections.append(
                ProcedureInductionRejection(group_key, "missing_evidence", member_ids)
            )
            continue
        if len(source_event_ids) > limits.max_sources:
            raise ProcedureInductionError("procedure evidence exceeds max_sources")

        representative = max(successful, key=lambda item: (item.quality, item.id))
        applicability = _unique(item.observation for item in successful)[
            : limits.max_conditions
        ]
        success_conditions = _unique(item.outcome for item in successful)[
            : limits.max_conditions
        ]
        failure_patterns = _unique(
            item.lesson or item.outcome for item in counterexamples
        )[: limits.max_failure_patterns]
        source_episode_ids = tuple(sorted(member_ids))
        reward_definition_id = evaluation_key[0] if isinstance(evaluation_key, tuple) else None
        reward_formula_version = evaluation_key[1] if isinstance(evaluation_key, tuple) else None
        procedures.append(
            Procedure(
                id=_procedure_id(
                    request.scope.partition_key(),
                    group_key,
                    source_episode_ids,
                    evaluation_key if isinstance(evaluation_key, tuple) else None,
                ),
                scope=request.scope,
                name=_clip(f"Candidate procedure: {representative.action}", limits.max_text_chars),
                trigger=_clip(representative.observation, limits.max_text_chars),
                steps=(_clip(representative.action, limits.max_text_chars),),
                success_conditions=tuple(
                    _clip(value, limits.max_text_chars) for value in success_conditions
                ),
                failure_patterns=tuple(
                    _clip(value, limits.max_text_chars) for value in failure_patterns
                ),
                applicability_conditions=tuple(
                    _clip(value, limits.max_text_chars) for value in applicability
                ),
                counterexample_episode_ids=tuple(item.id for item in counterexamples),
                source_episode_ids=source_episode_ids,
                extractor_version=EXTRACTOR_VERSION,
                evidence_start_at=members[0].occurred_at,
                evidence_end_at=members[-1].occurred_at,
                reward_definition_id=reward_definition_id,
                reward_formula_version=reward_formula_version,
                status=ArtifactStatus.CANDIDATE,
                provenance=Provenance(
                    source_event_ids=source_event_ids,
                    extractor="DeterministicProcedureInducer",
                    provider="agent-memory-core",
                    prompt_version=EXTRACTOR_VERSION,
                    created_at=members[-1].occurred_at,
                ),
                created_at=members[-1].occurred_at,
            )
        )
    return ProcedureInductionPlan(tuple(procedures), tuple(rejections))


class DeterministicProcedureInducer:
    """Consolidator plugin that emits Procedure candidates and never promotes them."""

    def __init__(self, limits: ProcedureInductionLimits | None = None) -> None:
        self._limits = limits or ProcedureInductionLimits()
        self._context: PluginContext | None = None
        self._closed = False

    def plugin_manifest(self) -> PluginManifest:
        return PluginManifest(
            name="deterministic-procedure-inducer",
            version="0.1.0",
            kind=PluginKind.CONSOLIDATOR,
            capabilities=("memory.consolidate", "memory.procedure.induce"),
            requires={"core": ">=0.1,<1.0"},
            config_schema={"type": "object", "additionalProperties": False},
            resource_limits=PluginResourceLimits(max_candidates=100, max_batch_size=10_000),
            failure_mode=PluginFailureMode.FALLBACK,
        )

    async def initialize(self, context: PluginContext) -> None:
        if self._context is not None and not self._closed:
            raise PluginError(
                "procedure inducer is already initialized",
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
        if len(request.episodes) > context.resource_limits.max_batch_size:
            raise PluginError(
                "consolidation request exceeds the plugin batch limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="episodes",
            )
        try:
            plan = induce_procedure_candidates(
                request,
                limits=self._limits,
                now=context.clock.now(),
            )
        except ProcedureInductionError as error:
            raise PluginError(
                str(error), code=PluginErrorCode.INVALID_IMPLEMENTATION
            ) from error
        if len(plan.procedures) > context.resource_limits.max_candidates:
            raise PluginError(
                "procedure candidates exceed the plugin candidate limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="episodes",
            )
        return ConsolidationResult(
            procedures=plan.procedures,
            procedure_rejections=plan.rejections,
        )

    def _require_context(self, context: PluginContext) -> None:
        if self._closed or self._context is None:
            raise PluginError("procedure inducer is not active")
        if context is not self._context:
            raise PluginError(
                "plugin operation used a context not issued during initialization",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if context.cancelled or context.expired:
            raise PluginError("procedure induction was cancelled or expired")
