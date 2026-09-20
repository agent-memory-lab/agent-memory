"""T37 acceptance tests for governed Claim consolidation."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from agent_memory import (
    Claim,
    ClaimConsolidationError,
    ClaimProposalOperation,
    ClaimStatus,
    ConsolidationRequest,
    DeterministicClaimConsolidator,
    MemoryEvent,
    MemoryProposal,
    MemoryScope,
    PluginContext,
    PluginResourceLimits,
    ProposalStatus,
    Provenance,
    ScopeLevel,
    build_local_kernel,
    consolidate_claim_proposals,
)


NOW = datetime(2026, 9, 20, tzinfo=timezone.utc)
SCOPE = MemoryScope("tenant", user_id="user", session_id="session")
USER_SCOPE = SCOPE.project(ScopeLevel.USER)


def evidence(event_id: str, trust_label: str = "observed") -> MemoryEvent:
    return MemoryEvent(
        SCOPE,
        "agent.observation",
        event_id,
        id=event_id,
        metadata={"trust_label": trust_label},
        occurred_at=NOW,
        ingested_at=NOW,
    )


def claim(
    claim_id: str,
    value: str,
    source_ids: tuple[str, ...],
    *,
    status: ClaimStatus,
    valid_from: datetime = NOW,
    version: int = 1,
) -> Claim:
    return Claim(
        id=claim_id,
        scope=USER_SCOPE,
        key="contact.preference",
        value=value,
        text=f"Use {value}",
        confidence=0.9,
        importance=0.7,
        status=status,
        provenance=Provenance(source_event_ids=source_ids),
        valid_from=valid_from,
        created_at=valid_from,
        version=version,
    )


def test_duplicate_claims_merge_sources_without_copying_claim():
    first, second = evidence("event-1"), evidence("event-2")
    current = claim("active", "email", (first.id,), status=ClaimStatus.ACTIVE, version=3)
    duplicate = claim("candidate", "email", (second.id,), status=ClaimStatus.CANDIDATE)

    plan = consolidate_claim_proposals(
        ConsolidationRequest(SCOPE, events=(first, second), claims=(duplicate, current))
    )

    assert len(plan.proposals) == 1
    proposal = plan.proposals[0]
    assert proposal.operation is ClaimProposalOperation.MERGE
    assert proposal.expected_version == 3
    assert proposal.source_event_ids == (first.id, second.id)
    assert proposal.value == "email"


def test_bitemporal_trust_rules_supersede_or_report_conflict():
    old = evidence("old", "observed")
    newer = evidence("newer", "trusted")
    stale = evidence("stale", "derived")
    current = claim(
        "active", "email", (old.id,), status=ClaimStatus.ACTIVE, valid_from=NOW, version=2
    )
    replacement = claim(
        "replacement",
        "sms",
        (newer.id,),
        status=ClaimStatus.CANDIDATE,
        valid_from=NOW + timedelta(days=1),
    )
    supersede = consolidate_claim_proposals(
        ConsolidationRequest(SCOPE, events=(old, newer), claims=(current, replacement))
    ).proposals[0]
    assert supersede.operation is ClaimProposalOperation.SUPERSEDE
    assert supersede.valid_from == replacement.valid_from

    older_lower_trust = claim(
        "stale-candidate",
        "push",
        (stale.id,),
        status=ClaimStatus.CANDIDATE,
        valid_from=NOW - timedelta(days=1),
    )
    conflict = consolidate_claim_proposals(
        ConsolidationRequest(SCOPE, events=(old, stale), claims=(current, older_lower_trust))
    ).proposals[0]
    assert conflict.operation is ClaimProposalOperation.CONFLICT
    assert "older" in conflict.reason


def test_equal_trust_same_valid_time_is_conflict_not_arbitrary_winner():
    left, right = evidence("left"), evidence("right")
    current = claim("active", "email", (left.id,), status=ClaimStatus.ACTIVE)
    candidate = claim("candidate", "sms", (right.id,), status=ClaimStatus.CANDIDATE)
    proposal = consolidate_claim_proposals(
        ConsolidationRequest(SCOPE, events=(left, right), claims=(current, candidate))
    ).proposals[0]
    assert proposal.operation is ClaimProposalOperation.CONFLICT


def test_deleted_and_untrusted_sources_are_recomputed_or_invalidated():
    kept = evidence("kept")
    deleted = evidence("deleted")
    untrusted = evidence("untrusted", "untrusted")
    partial = claim(
        "partial",
        "email",
        (kept.id, deleted.id, untrusted.id),
        status=ClaimStatus.ACTIVE,
    )
    orphan = claim("orphan", "sms", (deleted.id,), status=ClaimStatus.CANDIDATE)
    plan = consolidate_claim_proposals(
        ConsolidationRequest(
            SCOPE,
            events=(kept, deleted, untrusted),
            claims=(partial, orphan),
            deleted_event_ids=(deleted.id,),
        )
    )

    updates = {update.claim_id: update for update in plan.evidence_updates}
    assert updates["partial"].retained_source_event_ids == (kept.id,)
    assert updates["partial"].removed_source_event_ids == (deleted.id,)
    assert updates["partial"].excluded_untrusted_event_ids == (untrusted.id,)
    assert not updates["partial"].invalidated
    assert updates["orphan"].invalidated


def test_foreign_scope_and_multiple_active_claims_fail_closed():
    source = evidence("event")
    foreign = MemoryScope("other", user_id="user")
    foreign_claim = replace(
        claim("foreign", "email", (source.id,), status=ClaimStatus.ACTIVE),
        scope=foreign,
    )
    with pytest.raises(ClaimConsolidationError, match="outside"):
        consolidate_claim_proposals(
            ConsolidationRequest(SCOPE, events=(source,), claims=(foreign_claim,))
        )

    duplicate_active = claim("other-active", "sms", (source.id,), status=ClaimStatus.ACTIVE)
    with pytest.raises(ClaimConsolidationError, match="multiple active"):
        consolidate_claim_proposals(
            ConsolidationRequest(
                SCOPE,
                events=(source,),
                claims=(claim("active", "email", (source.id,), status=ClaimStatus.ACTIVE), duplicate_active),
            )
        )


def test_kernel_applies_merge_and_rejects_conflict_operation(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        first = MemoryEvent(
            SCOPE,
            "preference",
            "Email",
            id="first",
            metadata={
                "claims": [{
                    "key": "contact.preference",
                    "value": "email",
                    "text": "Use email",
                    "scope": "user",
                }]
            },
        )
        second = MemoryEvent(SCOPE, "preference.confirmed", "Still email", id="second")
        await kernel.ingest_event(first)
        await kernel.ingest_event(second)
        current = (await kernel.get_state(SCOPE))[0]
        candidate = claim(
            "candidate", "email", (second.id,), status=ClaimStatus.CANDIDATE
        )
        proposal = consolidate_claim_proposals(
            ConsolidationRequest(SCOPE, events=(first, second), claims=(current, candidate))
        ).proposals[0]
        accepted = await kernel.propose(proposal)
        assert accepted.status is ProposalStatus.ACCEPTED
        state = await kernel.get_state(SCOPE)
        assert len(state) == 1
        assert {first.id, second.id}.issubset(state[0].provenance.source_event_ids)

        conflicting = MemoryProposal(
            scope=SCOPE,
            key=current.key,
            value="sms",
            text="Use sms",
            source_event_ids=(second.id,),
            expected_version=current.version,
            scope_level=ScopeLevel.USER,
            operation=ClaimProposalOperation.CONFLICT,
            reason="equal evidence",
        )
        result = await kernel.propose(conflicting)
        assert result.status is ProposalStatus.CONFLICT
        assert (await kernel.get_state(SCOPE))[0].value == "email"

    asyncio.run(scenario())


def test_claim_consolidator_plugin_is_bounded_and_candidate_only():
    async def scenario():
        source = evidence("event")
        current = claim("active", "email", (source.id,), status=ClaimStatus.ACTIVE)
        context = PluginContext(SCOPE, PluginResourceLimits(max_batch_size=4))
        plugin = DeterministicClaimConsolidator()
        await plugin.initialize(context)
        result = await plugin.consolidate(
            ConsolidationRequest(SCOPE, events=(source,), claims=(current,)), context
        )
        assert result.claims == ()
        assert (await plugin.health()).status.value == "ready"
        await plugin.close()

    asyncio.run(scenario())
