"""T36 acceptance tests for deterministic Episode segmentation."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from agent_memory import (
    ArtifactStatus,
    ConsolidationRequest,
    DecisionRecord,
    DeterministicEpisodeSegmenter,
    EpisodeSegmentationError,
    EpisodeSegmentationLimits,
    FeedbackStatus,
    MemoryScope,
    MemoryUsage,
    OutcomeEvent,
    OutcomeStatus,
    PluginContext,
    PluginError,
    PluginResourceLimits,
    RetrievalTrace,
    segment_episode_proposals,
)
from agent_memory.lifecycle import LifecycleEvent, LifecycleEventType, LifecycleOrigin


WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)
SCOPE = MemoryScope("tenant", user_id="user", session_id="session")


def event(
    event_id: str,
    event_type: LifecycleEventType,
    content: str,
    *,
    offset: int,
    payload: dict[str, object] | None = None,
):
    origin = LifecycleOrigin.USER if event_type is LifecycleEventType.MESSAGE_RECEIVED else LifecycleOrigin.HOST
    if event_type in {LifecycleEventType.TOOL_CALLED, LifecycleEventType.TOOL_COMPLETED}:
        origin = LifecycleOrigin.TOOL
    return LifecycleEvent(
        scope=SCOPE,
        event_id=event_id,
        event_type=event_type,
        origin=origin,
        occurred_at=WHEN + timedelta(seconds=offset),
        run_id="run-1",
        content=content,
        payload=payload or {},
    ).to_memory_event()


def test_out_of_order_events_form_bounded_evidence_backed_episode():
    events = (
        event("finished", LifecycleEventType.TURN_COMPLETED, "Finished", offset=4),
        event("input", LifecycleEventType.MESSAGE_RECEIVED, "Summarize the report", offset=1),
        event("tool", LifecycleEventType.TOOL_COMPLETED, "Loaded report", offset=3),
        event("started", LifecycleEventType.TURN_STARTED, "Started", offset=0),
    )
    decision = DecisionRecord(
        scope=SCOPE,
        id="decision-1",
        action="Summarize with the report evidence",
        memory_ids=("memory-1",),
        procedure_ids=("procedure-1",),
        memory_usage=MemoryUsage.CONFIRMED,
        run_id="run-1",
        bundle_id="bundle-1",
        created_at=WHEN + timedelta(seconds=2),
    )
    outcome = OutcomeEvent(
        scope=SCOPE,
        id="outcome-1",
        decision_id=decision.id,
        outcome="Summary accepted",
        success=True,
        score=0.9,
        run_id="run-1",
        occurred_at=WHEN + timedelta(seconds=5),
    )
    trace = RetrievalTrace(
        scope=SCOPE,
        request_id="request-1",
        bundle_id="bundle-1",
        returned_memory_ids=("memory-1",),
        returned_versions={"memory-1": 1},
        policy_version="policy-1",
        token_budget=256,
        token_estimate=64,
        candidate_count=2,
        selected_count=1,
        truncated=False,
        run_id="run-1",
        created_at=WHEN + timedelta(seconds=2),
    )
    request = ConsolidationRequest(
        SCOPE,
        events=events,
        decisions=(decision,),
        outcomes=(outcome,),
        retrieval_traces=(trace,),
    )

    proposal = segment_episode_proposals(
        request,
        limits=EpisodeSegmentationLimits(max_summary_chars=64),
        now=WHEN + timedelta(minutes=1),
    )[0]

    assert proposal.status is ArtifactStatus.CANDIDATE
    assert proposal.outcome_status is OutcomeStatus.SUCCEEDED
    assert proposal.run_id == "run-1"
    assert proposal.decision_ids == ("decision-1",)
    assert proposal.retrieval_trace_ids == ("bundle-1",)
    assert proposal.used_memory_ids == ("memory-1", "procedure-1")
    expected_source_ids = tuple(
        item.id for item in sorted(events, key=lambda value: (value.occurred_at, value.id))
    )
    assert proposal.provenance.source_event_ids == expected_source_ids
    assert max(map(len, (proposal.observation, proposal.action, proposal.outcome, proposal.lesson))) <= 64


def test_corrected_outcome_replaces_failure_and_increments_stable_episode():
    events = (event("input", LifecycleEventType.MESSAGE_RECEIVED, "Do work", offset=0),)
    decision = DecisionRecord(SCOPE, "Act", (), id="decision", run_id="run-1")
    failed = OutcomeEvent(
        SCOPE,
        decision.id,
        "Initial failure",
        False,
        id="failed",
        run_id="run-1",
        occurred_at=WHEN + timedelta(seconds=1),
    )
    corrected = OutcomeEvent(
        SCOPE,
        decision.id,
        "Host could not determine the result",
        None,
        id="corrected",
        run_id="run-1",
        outcome_status=OutcomeStatus.UNKNOWN,
        corrects_id=failed.id,
        occurred_at=WHEN + timedelta(seconds=2),
    )
    first_request = ConsolidationRequest(
        SCOPE, events=events, decisions=(decision,), outcomes=(failed,)
    )
    first = segment_episode_proposals(first_request, now=WHEN + timedelta(minutes=1))[0]
    assert first.outcome_status is OutcomeStatus.FAILED

    corrected_request = ConsolidationRequest(
        SCOPE,
        events=events,
        episodes=(first,),
        decisions=(decision,),
        outcomes=(corrected, failed),
    )
    second = segment_episode_proposals(corrected_request, now=WHEN + timedelta(minutes=1))[0]
    assert second.id == first.id
    assert second.version == 2
    assert second.outcome_status is OutcomeStatus.UNKNOWN
    assert second.outcome == "Host could not determine the result"


def test_session_fallback_is_unknown_and_idempotent():
    plain = replace(
        event("plain", LifecycleEventType.MESSAGE_RECEIVED, "Session evidence", offset=0),
        metadata={},
    )
    request = ConsolidationRequest(SCOPE, events=(plain,))
    first = segment_episode_proposals(request, now=WHEN)[0]
    assert first.run_id is None
    assert first.outcome_status is OutcomeStatus.UNKNOWN

    replay = segment_episode_proposals(
        replace(request, episodes=(first,)), now=WHEN
    )
    assert replay == ()


@pytest.mark.parametrize("status", (OutcomeStatus.CANCELLED, OutcomeStatus.TIMED_OUT))
def test_non_binary_terminal_outcomes_remain_distinct(status):
    source = event("input", LifecycleEventType.MESSAGE_RECEIVED, "Do work", offset=0)
    decision = DecisionRecord(SCOPE, "Act", (), id="decision", run_id="run-1")
    outcome = OutcomeEvent(
        SCOPE,
        decision.id,
        status.value,
        None,
        run_id="run-1",
        outcome_status=status,
        occurred_at=WHEN + timedelta(seconds=1),
    )
    proposal = segment_episode_proposals(
        ConsolidationRequest(
            SCOPE,
            events=(source,),
            decisions=(decision,),
            outcomes=(outcome,),
        ),
        now=WHEN + timedelta(minutes=1),
    )[0]

    assert proposal.outcome_status is status
    assert proposal.outcome_status not in {OutcomeStatus.SUCCEEDED, OutcomeStatus.FAILED}


def test_plugin_enforces_scope_limits_and_contract_lifecycle():
    async def scenario():
        context = PluginContext(SCOPE, PluginResourceLimits(max_batch_size=10))
        plugin = DeterministicEpisodeSegmenter(EpisodeSegmentationLimits(max_events_per_episode=1))
        await plugin.initialize(context)
        assert (await plugin.health()).status.value == "ready"

        request = ConsolidationRequest(
            SCOPE,
            events=(event("one", LifecycleEventType.TURN_STARTED, "one", offset=0),),
        )
        result = await plugin.consolidate(request, context)
        assert len(result.episodes) == 1

        foreign = MemoryScope("other", session_id="session")
        with pytest.raises(PluginError):
            await plugin.consolidate(ConsolidationRequest(foreign), context)
        await plugin.close()
        assert (await plugin.health()).status.value == "unavailable"

    asyncio.run(scenario())


def test_episode_limits_reject_partial_source_truncation():
    request = ConsolidationRequest(
        SCOPE,
        events=(
            event("one", LifecycleEventType.TURN_STARTED, "one", offset=0),
            event("two", LifecycleEventType.TURN_COMPLETED, "two", offset=1),
        ),
    )
    with pytest.raises(EpisodeSegmentationError, match="event limit"):
        segment_episode_proposals(
            request,
            limits=EpisodeSegmentationLimits(max_events_per_episode=1),
            now=WHEN,
        )
