"""Native provider replay ownership and independently relearned legacy claims."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta, timezone
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from agent_memory import (
    ClaimProposalOperation,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryProposal,
    MemoryQuery,
    MemoryScope,
    ProposalStatus,
    ScopeLevel,
    build_local_kernel,
)
from agent_memory.consolidation.proposal_identity import validate_proposal_replay
from agent_memory.domain import canonical_json
from agent_memory.serialization import to_jsonable


@pytest.fixture(params=["sqlite", "postgres"])
def store(request, tmp_path):
    if request.param == "postgres":
        pytest.importorskip("agent_memory_postgres")
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN")
        if not dsn:
            pytest.skip("proposal PostgreSQL contract tests require a test DSN")
        if "test" not in urlsplit(dsn).path.casefold():
            pytest.fail("proposal PostgreSQL tests require a database named test")
        from agent_memory_postgres import build_postgres_kernel

        kernel = build_postgres_kernel(dsn)
    else:
        kernel = build_local_kernel(tmp_path / "memory.db")
    scope = MemoryScope(f"proposal-{uuid4().hex}", user_id="user", agent_id="agent",
                        workspace_id="workspace", session_id="session")

    @asynccontextmanager
    async def open_store():
        await kernel.initialize()
        try:
            yield kernel, scope
        finally:
            await kernel.close()

    return open_store


async def seed(kernel, scope, **kwargs):
    event = MemoryEvent(scope, "user.message", "Use email")
    await kernel.ingest_event(event)
    proposal = MemoryProposal(scope, "contact", "email", "Use email", (event.id,), 0,
                              **kwargs)
    return event, proposal, await kernel.propose(proposal)


@pytest.mark.parametrize("field", ["tenant_id", "namespace", "user_id", "agent_id",
                                    "workspace_id", "session_id"])
def test_replay_rejects_each_changed_owner_dimension(store, field):
    async def run():
        async with store() as (kernel, scope):
            _, proposal, accepted = await seed(kernel, scope)
            other = replace(proposal, scope=replace(scope, **{field: "other"}))
            with pytest.raises(ValueError, match="different request"):
                await kernel.propose(other)
            assert await kernel.propose(proposal) == accepted
            assert (await kernel.get_state(scope))[0].id == accepted.claim_id
    asyncio.run(run())


@pytest.mark.parametrize("change", [
    {"key": "other"}, {"value": "sms"}, {"text": "Different evidence"},
    {"source_event_ids": ("missing",)}, {"expected_version": 1},
    {"scope_level": ScopeLevel.USER}, {"confidence": .5}, {"importance": .9},
    {"actor": "other"}, {"operation": ClaimProposalOperation.MERGE},
    {"valid_from": datetime(2026, 1, 1, tzinfo=UTC)}, {"reason": "private reason"},
])
def test_replay_rejects_changed_payload_without_overwriting_result(store, change):
    async def run():
        async with store() as (kernel, scope):
            _, proposal, accepted = await seed(kernel, scope)
            with pytest.raises(ValueError, match="different request"):
                await kernel.propose(replace(proposal, **change))
            assert await kernel.propose(proposal) == accepted
            assert len(await kernel.get_state(scope)) == 1
    asyncio.run(run())


@pytest.mark.parametrize("status", ["accepted", "conflict", "rejected"])
def test_exact_retries_keep_original_result_with_new_created_at(store, status):
    async def run():
        async with store() as (kernel, scope):
            event = MemoryEvent(scope, "user.message", "Use email")
            await kernel.ingest_event(event)
            proposal = MemoryProposal(
                scope, "contact", "email", "Use email",
                ("missing",) if status == "rejected" else (event.id,), 0,
                operation=(ClaimProposalOperation.CONFLICT if status == "conflict"
                           else ClaimProposalOperation.UPSERT),
                reason="Private reason ORCHID-624" if status == "conflict" else None,
            )
            result = await kernel.propose(proposal)
            assert result.status == status
            assert await kernel.propose(replace(
                proposal, created_at=proposal.created_at + timedelta(days=1),
            )) == result
            with pytest.raises(ValueError, match="different request") as error:
                await kernel.propose(replace(proposal, scope=replace(scope, tenant_id="other")))
            assert "ORCHID-624" not in str(error.value)
    asyncio.run(run())


@pytest.mark.parametrize("mode", [ForgetMode.ERASE, ForgetMode.ARCHIVE])
@pytest.mark.parametrize("level", [ScopeLevel.SESSION, ScopeLevel.USER, ScopeLevel.WORKSPACE])
def test_independent_same_value_is_visible_after_proposal_source_forget(store, mode, level):
    async def run():
        async with store() as (kernel, scope):
            event, _, accepted = await seed(kernel, scope, scope_level=level)
            assert [claim.value for claim in await kernel.get_state(scope)] == ["email"]
            await kernel.forget(ForgetRequest(scope, memory_ids=(event.id,), mode=mode))
            assert await kernel.get_state(scope) == ()
            first = None
            for repeat in range(2):
                evidence = MemoryEvent(scope, "user.message", f"Use email again {repeat}", metadata={
                    "claims": [{"key": "contact", "value": "email", "text": "Use email again",
                                "scope_level": level}],
                })
                result = await kernel.ingest_event(evidence)
                assert result.extraction.accepted_count == 1
                assert result.claim_ids != (accepted.claim_id,)
                if first is None:
                    first = result.claim_ids
                    assert len(result.state_delta_ids) == 1
                else:
                    assert result.claim_ids == first
                    assert result.state_delta_ids == ()
                for state in (await kernel.get_state(scope),
                              await kernel.get_state_bounded(scope, limit=10),
                              (await kernel.retrieve(MemoryQuery(scope, "email"))).current_state):
                    assert [claim.value for claim in state] == ["email"]
                    assert state[0].id == first[0]
                    assert event.id not in state[0].provenance.source_event_ids
                    assert evidence.id in state[0].provenance.source_event_ids
            # Durability and migration/reinitialization must not revive the old claim.
            await kernel.close()
            await kernel.initialize()
            assert (await kernel.get_state(scope))[0].id == first[0]
    asyncio.run(run())


def test_healthy_same_value_still_corroborates_existing_proposal(store):
    async def run():
        async with store() as (kernel, scope):
            _, _, accepted = await seed(kernel, scope)
            event = MemoryEvent(scope, "user.message", "Still email", metadata={
                "claims": [{"key": "contact", "value": "email", "text": "Still email"}],
            })
            result = await kernel.ingest_event(event)
            assert result.claim_ids == (accepted.claim_id,)
            assert result.state_delta_ids == ()
            assert (await kernel.get_state(scope))[0].id == accepted.claim_id
    asyncio.run(run())


def test_merge_does_not_accept_new_sources_into_invisible_claim(store):
    async def run():
        async with store() as (kernel, scope):
            event, proposal, _ = await seed(kernel, scope)
            await kernel.forget(ForgetRequest(scope, memory_ids=(event.id,), mode=ForgetMode.ARCHIVE))
            other = MemoryEvent(scope, "user.message", "Independent email evidence")
            await kernel.ingest_event(other)
            result = await kernel.propose(replace(
                proposal, id=str(uuid4()), source_event_ids=(other.id,), expected_version=1,
                operation=ClaimProposalOperation.MERGE,
            ))
            assert result.status == ProposalStatus.CONFLICT
            assert await kernel.get_state(scope) == ()
    asyncio.run(run())


def test_replay_identity_accepts_legacy_defaults_and_timestamp_serializations():
    scope = MemoryScope("tenant", session_id="session")
    proposal = MemoryProposal(scope, "contact", {"b": 2, "a": 1}, "Use email", ("event",), 0)
    legacy = json.loads(canonical_json(asdict(proposal)))
    for field in ("operation", "valid_from", "reason"):
        del legacy[field]
    validate_proposal_replay(proposal, scope, legacy)
    dated = replace(proposal, valid_from=datetime(2026, 1, 1, tzinfo=UTC))
    equivalent = replace(dated, valid_from=dated.valid_from.astimezone(timezone(timedelta(hours=8))))
    for payload in (json.loads(canonical_json(asdict(dated))), to_jsonable(dated)):
        validate_proposal_replay(equivalent, scope, payload)
    with pytest.raises(ValueError, match="forgotten"):
        validate_proposal_replay(proposal, scope, {"erased": True})
    with pytest.raises(ValueError, match="different request"):
        validate_proposal_replay(proposal, scope, {})


def test_erased_minimal_tombstone_cannot_replay_cached_result(tmp_path):
    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            scope = MemoryScope("tenant", session_id="session")
            _, proposal, _ = await seed(kernel, scope)
            async with kernel._repository.unit_of_work() as uow:
                await uow.run_sync(lambda db: db.execute(
                    "UPDATE proposals SET payload_json = ? WHERE id = ?",
                    (json.dumps({"erased": True}), proposal.id),
                ))
            with pytest.raises(ValueError, match="forgotten"):
                await kernel.propose(proposal)
        finally:
            await kernel.close()
    asyncio.run(run())


def test_legacy_uow_supports_new_proposals_but_fails_closed_on_replay(tmp_path):
    class LegacyUow:
        find_proposal_result_for = None

        def __init__(self, underlying):
            self.underlying = underlying

        async def __aenter__(self):
            await self.underlying.__aenter__()
            return self

        async def __aexit__(self, *args):
            return await self.underlying.__aexit__(*args)

        def __getattr__(self, name):
            return getattr(self.underlying, name)

    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        original = kernel._repository.unit_of_work
        kernel._repository.unit_of_work = lambda: LegacyUow(original())
        try:
            scope = MemoryScope("tenant", session_id="session")
            _, proposal, result = await seed(kernel, scope)
            assert result.status == ProposalStatus.ACCEPTED
            for replay in (proposal, replace(proposal, scope=replace(scope, tenant_id="other"))):
                with pytest.raises(ValueError, match="cannot validate proposal replay identity"):
                    await kernel.propose(replay)
        finally:
            await kernel.close()
    asyncio.run(run())


def test_concurrent_identical_replays_share_one_result(store):
    async def run():
        async with store() as (kernel, scope):
            event = MemoryEvent(scope, "user.message", "Use email")
            await kernel.ingest_event(event)
            proposal = MemoryProposal(scope, "contact", "email", "Use email", (event.id,), 0)
            first, second = await asyncio.gather(kernel.propose(proposal), kernel.propose(proposal))
            assert first == second
            assert first.status == ProposalStatus.ACCEPTED
            assert len(await kernel.get_state(scope)) == 1
    asyncio.run(run())
