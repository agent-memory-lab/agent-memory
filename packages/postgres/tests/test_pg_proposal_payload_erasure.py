"""Live PostgreSQL parity for proposal bodies and derived audit-event erasure."""
import asyncio
from dataclasses import replace
import os
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from psycopg import AsyncConnection, sql
from agent_memory_postgres import build_postgres_kernel

from agent_memory import ForgetMode, ForgetRequest, MemoryEvent, MemoryProposal, MemoryScope
from agent_memory.domain import ClaimProposalOperation


@pytest.mark.parametrize("status", ["accepted", "rejected", "conflict"])
@pytest.mark.parametrize("target", ["scope", "source", "proposal"])
@pytest.mark.parametrize("archive_first", [False, True])
def test_live_pg_proposal_erasure(status, target, archive_first):
    async def run():
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN", "")
        if not dsn:
            pytest.skip("live PostgreSQL proposal erasure requires an explicit test DSN")
        parsed = urlsplit(dsn)
        if parsed.scheme not in {"postgres", "postgresql"} or "test" not in unquote(parsed.path).casefold():
            pytest.fail("proposal erasure requires a PostgreSQL test database")
        schema = "proposal_erasure_" + uuid4().hex
        async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
            await connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        query = dict(parse_qsl(parsed.query))
        query["options"] = "-csearch_path=" + schema
        scoped = urlunsplit(parsed._replace(query=urlencode(query)))
        kernel = build_postgres_kernel(scoped)
        try:
            await kernel.initialize()
            scope = MemoryScope("tenant", user_id="owner", session_id="s")
            event = MemoryEvent(scope, "user.message", "sensitive ORCHID-824")
            await kernel.ingest_event(event)
            proposal = MemoryProposal(scope, "key", "ORCHID-824", "sensitive ORCHID-824",
                ("missing" if status == "rejected" else event.id,), 0,
                operation=ClaimProposalOperation.CONFLICT if status == "conflict"
                else ClaimProposalOperation.UPSERT, reason="sensitive ORCHID-824")
            result = await kernel.propose(proposal)
            assert result.status == status
            foreign = replace(scope, user_id="control")
            control_event = MemoryEvent(foreign, "user.message", "independent control")
            await kernel.ingest_event(control_event)
            control = replace(proposal, id="control", scope=foreign,
                source_event_ids=(control_event.id,), operation=ClaimProposalOperation.UPSERT)
            await kernel.propose(control)
            ids = {"scope": (), "source": proposal.source_event_ids,
                   "proposal": (proposal.id,)}[target]
            request = ForgetRequest(scope, ids, target == "scope", ForgetMode.ERASE)
            if archive_first:
                await kernel.forget(replace(request, mode=ForgetMode.ARCHIVE))
            await kernel.forget(request)
            async with kernel._repository.pool.connection() as connection:
                cursor = await connection.execute(
                    "SELECT payload_json,status,reason,claim_id FROM agent_memory_proposals WHERE id=%s",
                    (proposal.id,))
                row = await cursor.fetchone()
                assert row == {"payload_json": {"erased": True}, "status": status,
                               "reason": None, "claim_id": result.claim_id}
                cursor = await connection.execute(
                    "SELECT payload_json FROM agent_memory_proposals WHERE id=%s", (control.id,))
                assert (await cursor.fetchone())["payload_json"]["text"] == "sensitive ORCHID-824"
                cursor = await connection.execute("SELECT id FROM agent_memory_events WHERE id=%s",
                    (str(uuid5(NAMESPACE_URL, f"proposal-event:{proposal.id}")),))
                assert await cursor.fetchone() is None
                if status == "accepted" and target in ("scope", "source"):
                    cursor = await connection.execute("SELECT id FROM agent_memory_claims WHERE id=%s",
                                                       (result.claim_id,))
                    assert await cursor.fetchone() is None
            assert (await kernel.forget(request)).affected_events == 0
        finally:
            await kernel.close()
            async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
                await connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    asyncio.run(run())


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_live_pg_preupgrade_projected_claim_proposal_reerase(monkeypatch, all_in_scope):
    from agent_memory.domain import ScopeLevel

    async def run():
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN", "")
        if not dsn:
            pytest.skip("live PostgreSQL proposal erasure requires an explicit test DSN")
        parsed = urlsplit(dsn)
        if parsed.scheme not in {"postgres", "postgresql"} or "test" not in unquote(parsed.path).casefold():
            pytest.fail("proposal erasure requires a PostgreSQL test database")
        schema = "claim_reerase_" + uuid4().hex
        async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
            await connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        query = dict(parse_qsl(parsed.query))
        query["options"] = "-csearch_path=" + schema
        kernel = build_postgres_kernel(urlunsplit(parsed._replace(query=urlencode(query))))
        try:
            await kernel.initialize()
            session = MemoryScope("tenant", user_id="owner", session_id="s")
            owner = session.project(ScopeLevel.USER)
            source = MemoryEvent(session, "user.message", "SECRET projected claim")
            await kernel.ingest_event(source)
            proposal = MemoryProposal(session, "projected", "SECRET", "SECRET projected claim",
                (source.id,), 0, scope_level=ScopeLevel.USER)
            result = await kernel.propose(proposal)
            assert result.status == "accepted"
            request = ForgetRequest(owner, () if all_in_scope else (result.claim_id,),
                                    all_in_scope, ForgetMode.ERASE)
            async def legacy_proposal_cleanup(*_args):
                return []
            with monkeypatch.context() as patch:
                patch.setattr(kernel._repository, "_erase_proposals", legacy_proposal_cleanup)
                await kernel.forget(request)
            async with kernel._repository.pool.connection() as connection:
                cursor = await connection.execute("SELECT id FROM agent_memory_claims WHERE id=%s",
                                                   (result.claim_id,))
                assert await cursor.fetchone() is None
                cursor = await connection.execute("SELECT id FROM agent_memory_memory_tombstones "
                    "WHERE partition_key=%s AND memory_table='claims' AND id=%s",
                    (owner.partition_key(), result.claim_id))
                assert await cursor.fetchone() is not None
            await kernel.forget(replace(request, scope=replace(owner, user_id="foreign")))
            async with kernel._repository.pool.connection() as connection:
                cursor = await connection.execute("SELECT payload_json FROM agent_memory_proposals WHERE id=%s",
                                                   (proposal.id,))
                assert (await cursor.fetchone())["payload_json"]["value"] == "SECRET"
            await kernel.forget(request)
            async with kernel._repository.pool.connection() as connection:
                cursor = await connection.execute("SELECT payload_json FROM agent_memory_proposals WHERE id=%s",
                                                   (proposal.id,))
                assert (await cursor.fetchone())["payload_json"] == {"erased": True}
                cursor = await connection.execute("SELECT id FROM agent_memory_events WHERE id=%s",
                    (str(uuid5(NAMESPACE_URL, f"proposal-event:{proposal.id}")),))
                assert await cursor.fetchone() is None
                cursor = await connection.execute("SELECT content FROM agent_memory_events WHERE id=%s", (source.id,))
                assert (await cursor.fetchone())["content"] == source.content
        finally:
            await kernel.close()
            async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
                await connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    asyncio.run(run())
