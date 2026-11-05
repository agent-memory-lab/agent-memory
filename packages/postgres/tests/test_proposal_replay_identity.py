"""PostgreSQL replay ordering and validation without a server dependency."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agent_memory.consolidation.proposal_identity import validate_proposal_replay
from agent_memory.domain import MemoryProposal, MemoryScope, ProposalStatus
from agent_memory.serialization import to_jsonable
from agent_memory_postgres.repository import PostgresMemoryRepository, PostgresMemoryUnitOfWork


def test_postgres_lookup_serializes_and_validates_before_returning_cached_result():
    async def run():
        scope = MemoryScope("tenant", user_id="user", session_id="session")
        proposal = MemoryProposal(scope, "contact", {"at": datetime(2026, 1, 1, tzinfo=UTC)},
                                  "Use email", ("event",), 0)
        row = {
            **to_jsonable(scope), "id": proposal.id, "payload_json": to_jsonable(proposal),
            "status": "conflict", "claim_id": "private-claim", "state_delta_id": None,
            "superseded_claim_id": None, "reason": "Private reason ORCHID-624",
        }
        calls = []

        class Cursor:
            async def fetchone(self):
                return row

        class Connection:
            async def execute(self, sql, params):
                calls.append(("query", sql, params))
                return Cursor()

        repository = SimpleNamespace(_scope_from_row=PostgresMemoryRepository._scope_from_row)
        uow = PostgresMemoryUnitOfWork(repository)
        uow.connection = Connection()

        async def namespace_lock(owner):
            calls.append(("namespace", owner))

        uow.lock_admission_scope = namespace_lock
        result = await uow.find_proposal_result_for(proposal)
        assert result.status == ProposalStatus.CONFLICT
        assert calls[0] == ("namespace", scope)
        assert "pg_advisory_xact_lock" in calls[1][1]
        assert calls[1][2] == (f"agent-memory:proposal::{proposal.id}",)
        assert calls[2][2] == (proposal.id,)
        assert result.reason == row["reason"]

        for changed in (replace(proposal, value="sms"),
                        replace(proposal, scope=replace(scope, tenant_id="other"))):
            with pytest.raises(ValueError, match="different request") as error:
                await uow.find_proposal_result_for(changed)
            assert "ORCHID-624" not in str(error.value)
        row["payload_json"] = {"erased": True}
        with pytest.raises(ValueError, match="forgotten"):
            await uow.find_proposal_result_for(proposal)
    asyncio.run(run())


def test_postgres_legacy_payload_timestamp_matches_shared_identity():
    scope = MemoryScope("tenant", session_id="session")
    proposal = MemoryProposal(scope, "contact", "email", "Use email", ("event",), 0)
    payload = to_jsonable(proposal)
    for field in ("operation", "valid_from", "reason"):
        del payload[field]
    validate_proposal_replay(proposal, scope, payload)
