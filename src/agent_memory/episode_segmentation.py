from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256

from .domain import (
    ArtifactStatus,
    DecisionRecord,
    Episode,
    FeedbackStatus,
    MemoryEvent,
    MemoryUsage,
    OutcomeEvent,
    OutcomeStatus,
    Provenance,
    RetrievalTrace,
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


@dataclass(frozen=True, slots=True)
class EpisodeSegmentationLimits:
    max_events_per_episode: int = 256
    max_summary_chars: int = 512
    max_used_memory_ids: int = 128

    def __post_init__(self) -> None:
        bounds = {
            "max_events_per_episode": (self.max_events_per_episode, 1, 10_000),
            "max_summary_chars": (self.max_summary_chars, 32, 8_192),
            "max_used_memory_ids": (self.max_used_memory_ids, 1, 10_000),
        }
        for name, (value, minimum, maximum) in bounds.items():
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(f"{name} must be between {minimum} and {maximum}")


class EpisodeSegmentationError(ValueError):
    pass


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


def _clip(value: str, limit: int) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(1, limit - 1)].rstrip() + "…"


def _lifecycle(event: MemoryEvent) -> Mapping[str, object]:
    value = event.metadata.get("lifecycle") if isinstance(event.metadata, Mapping) else None
    return value if isinstance(value, Mapping) else {}


def _run_key(event: MemoryEvent) -> tuple[str, str]:
    lifecycle = _lifecycle(event)
    run_id = lifecycle.get("run_id")
    if isinstance(run_id, str) and run_id.strip():
        return "run", run_id
    metadata_run_id = event.metadata.get("run_id")
    if isinstance(metadata_run_id, str) and metadata_run_id.strip():
        return "run", metadata_run_id
    if event.scope.session_id:
        return "session", event.scope.session_id
    return "event", event.id


def _active_records(records: Sequence[DecisionRecord | OutcomeEvent], now: datetime):
    accepted = [
        record
        for record in records
        if record.feedback_status is FeedbackStatus.ACCEPTED
        and (getattr(record, "expires_at", None) is None or record.expires_at > now)
    ]
    corrected = {record.corrects_id for record in accepted if record.corrects_id}
    return tuple(record for record in accepted if record.id not in corrected)


def _group_decisions(
    decisions: Sequence[DecisionRecord], run_id: str | None, *, single_group: bool
) -> tuple[DecisionRecord, ...]:
    selected = [
        decision
        for decision in decisions
        if decision.run_id == run_id
        or (single_group and decision.run_id is None)
    ]
    selected.sort(key=lambda value: (value.created_at, value.id))
    return tuple(selected)


def _group_outcomes(
    outcomes: Sequence[OutcomeEvent],
    decisions: Sequence[DecisionRecord],
    run_id: str | None,
    *,
    single_group: bool,
    now: datetime,
) -> tuple[OutcomeEvent, ...]:
    decision_ids = {decision.id for decision in decisions}
    relevant = [
        outcome
        for outcome in outcomes
        if outcome.run_id == run_id
        or outcome.decision_id in decision_ids
        or (single_group and outcome.run_id is None)
    ]
    active = list(_active_records(relevant, now))
    active.sort(key=lambda value: (value.occurred_at, value.id))
    return tuple(active)


def _group_traces(
    traces: Sequence[RetrievalTrace],
    decisions: Sequence[DecisionRecord],
    run_id: str | None,
    *,
    single_group: bool,
) -> tuple[RetrievalTrace, ...]:
    bundle_ids = {decision.bundle_id for decision in decisions if decision.bundle_id}
    selected = [
        trace
        for trace in traces
        if trace.run_id == run_id
        or trace.bundle_id in bundle_ids
        or (single_group and trace.run_id is None and trace.bundle_id in bundle_ids)
    ]
    selected.sort(key=lambda value: (value.created_at, value.id))
    return tuple(selected)


def _event_text(
    events: Sequence[MemoryEvent], preferred_types: frozenset[str], fallback: str
) -> str:
    values = [event.content for event in events if event.event_type in preferred_types and event.content]
    if not values:
        values = [event.content for event in events if event.content]
    return " | ".join(values) if values else fallback


def _lesson(events: Sequence[MemoryEvent], status: OutcomeStatus, outcome: str) -> str:
    for event in reversed(events):
        payload = _lifecycle(event).get("payload")
        if not isinstance(payload, Mapping):
            continue
        for key in ("lesson", "summary"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
    return f"Recorded {status.value} outcome: {outcome}"


def _episode_id(scope_key: str, group_key: tuple[str, str]) -> str:
    digest = sha256(canonical_json((scope_key, *group_key)).encode("utf-8")).hexdigest()
    return f"episode-{digest[:32]}"


def segment_episode_proposals(
    request: ConsolidationRequest,
    *,
    limits: EpisodeSegmentationLimits = EpisodeSegmentationLimits(),
    now: datetime | None = None,
) -> tuple[Episode, ...]:
    """Build deterministic candidate Episodes without publishing or activating them."""

    current_time = now or datetime.now(timezone.utc)
    groups: dict[tuple[str, str], list[MemoryEvent]] = {}
    for event in request.events:
        if event.scope != request.scope:
            raise EpisodeSegmentationError("event scope does not match consolidation scope")
        groups.setdefault(_run_key(event), []).append(event)

    lineage = (*request.decisions, *request.outcomes, *request.retrieval_traces)
    if any(record.scope != request.scope for record in lineage):
        raise EpisodeSegmentationError("lineage scope does not match consolidation scope")

    existing = {episode.id: episode for episode in request.episodes}
    proposals: list[Episode] = []
    single_group = len(groups) == 1
    active_decisions = _active_records(request.decisions, current_time)

    for group_key in sorted(groups):
        events = sorted(groups[group_key], key=lambda value: (value.occurred_at, value.id))
        if len(events) > limits.max_events_per_episode:
            raise EpisodeSegmentationError("episode event limit exceeded")

        run_id = group_key[1] if group_key[0] == "run" else None
        decisions = _group_decisions(active_decisions, run_id, single_group=single_group)
        outcomes = _group_outcomes(
            request.outcomes,
            decisions,
            run_id,
            single_group=single_group,
            now=current_time,
        )
        traces = _group_traces(
            request.retrieval_traces,
            decisions,
            run_id,
            single_group=single_group,
        )
        terminal = outcomes[-1] if outcomes else None
        status = terminal.outcome_status if terminal else OutcomeStatus.UNKNOWN
        outcome_text = terminal.outcome if terminal else "Outcome unknown."
        actions = " | ".join(decision.action for decision in decisions)
        action_text = actions or _event_text(
            events,
            frozenset({"agent.decision.made", "agent.tool.called"}),
            "No recorded action.",
        )
        observation = _event_text(
            events,
            frozenset({"user.message", "agent.message.received", "agent.turn.started"}),
            "No recorded observation.",
        )
        used_memory_ids = _unique(
            memory_id
            for decision in decisions
            if decision.memory_usage is MemoryUsage.CONFIRMED
            for memory_id in (*decision.memory_ids, *decision.procedure_ids)
        )
        if len(used_memory_ids) > limits.max_used_memory_ids:
            raise EpisodeSegmentationError("used memory ID limit exceeded")

        episode_id = _episode_id(request.scope.partition_key(), group_key)
        prior = existing.get(episode_id)
        source_ids = _unique(event.id for event in events)
        proposal = Episode(
            id=episode_id,
            scope=request.scope,
            observation=_clip(observation, limits.max_summary_chars),
            action=_clip(action_text, limits.max_summary_chars),
            outcome=_clip(outcome_text, limits.max_summary_chars),
            lesson=_clip(_lesson(events, status, outcome_text), limits.max_summary_chars),
            quality=terminal.score if terminal and terminal.score is not None else 0.5,
            status=ArtifactStatus.CANDIDATE,
            provenance=Provenance(
                source_event_ids=source_ids,
                extractor="deterministic-episode-segmenter-v1",
                provider="agent-memory-core",
                created_at=max(event.ingested_at for event in events),
            ),
            outcome_status=status,
            run_id=run_id,
            decision_ids=_unique(decision.id for decision in decisions),
            outcome_ids=_unique(outcome.id for outcome in outcomes),
            retrieval_trace_ids=_unique(trace.id for trace in traces),
            used_memory_ids=used_memory_ids,
            occurred_at=terminal.occurred_at if terminal else events[-1].occurred_at,
            version=(prior.version + 1) if prior else 1,
        )
        if prior is None or _episode_signature(prior) != _episode_signature(proposal):
            proposals.append(proposal)

    return tuple(proposals)


def _episode_signature(episode: Episode) -> tuple[object, ...]:
    return (
        episode.observation,
        episode.action,
        episode.outcome,
        episode.lesson,
        episode.quality,
        episode.status,
        episode.provenance.source_event_ids,
        episode.outcome_status,
        episode.run_id,
        episode.decision_ids,
        episode.outcome_ids,
        episode.retrieval_trace_ids,
        episode.used_memory_ids,
        episode.occurred_at,
    )


class DeterministicEpisodeSegmenter:
    """Zero-dependency consolidator plugin for evidence-backed Episode proposals."""

    def __init__(self, limits: EpisodeSegmentationLimits | None = None) -> None:
        self._limits = limits or EpisodeSegmentationLimits()
        self._context: PluginContext | None = None
        self._closed = False

    def plugin_manifest(self) -> PluginManifest:
        return PluginManifest(
            name="deterministic-episode-segmenter",
            version="0.1.0",
            kind=PluginKind.CONSOLIDATOR,
            capabilities=("memory.consolidate", "memory.episode.segment"),
            requires={"core": ">=0.1,<1.0"},
            config_schema={"type": "object", "additionalProperties": False},
            resource_limits=PluginResourceLimits(max_candidates=100, max_batch_size=10_000),
            failure_mode=PluginFailureMode.FALLBACK,
        )

    async def initialize(self, context: PluginContext) -> None:
        if self._context is not None and not self._closed:
            raise PluginError(
                "episode segmenter is already initialized",
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
        if len(request.events) > context.resource_limits.max_batch_size:
            raise PluginError(
                "consolidation request exceeds the plugin batch limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="events",
            )
        try:
            episodes = segment_episode_proposals(
                request,
                limits=self._limits,
                now=context.clock.now(),
            )
        except EpisodeSegmentationError as error:
            raise PluginError(
                str(error),
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            ) from error
        if len(episodes) > context.resource_limits.max_candidates:
            raise PluginError(
                "episode proposals exceed the plugin candidate limit",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
                field="events",
            )
        return ConsolidationResult(episodes=episodes)

    def _require_context(self, context: PluginContext) -> None:
        if self._closed or self._context is None:
            raise PluginError("episode segmenter is not active")
        if context is not self._context:
            raise PluginError(
                "plugin operation used a context not issued during initialization",
                code=PluginErrorCode.INVALID_IMPLEMENTATION,
            )
        if context.cancelled or context.expired:
            raise PluginError("episode segmentation was cancelled or expired")
