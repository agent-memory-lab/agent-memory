"""ERASE removes proposal bodies as well as their accepted audit-event copies."""
import asyncio
from contextlib import closing
from dataclasses import replace
import json
import sqlite3
from uuid import NAMESPACE_URL, uuid5

import pytest

from agent_memory import (
    DeletionAuditService, ForgetMode, ForgetRequest, MCPMemoryTools,
    MCPRequestContext, MemoryEvent, MemoryProposal, MemoryScope, SQLiteDeletionAuditSink,
    build_local_kernel,
)
from agent_memory.domain import ClaimProposalOperation
from agent_memory.mcp import MCPToolError


@pytest.mark.parametrize("status", ["accepted", "rejected", "conflict"])
@pytest.mark.parametrize("target", ["scope", "source", "proposal"])
@pytest.mark.parametrize("archive_first", [False, True])
def test_erasure_removes_payload_but_preserves_terminal_identity(
    tmp_path, status, target, archive_first,
):
    async def run():
        path = tmp_path / "core.db"
        kernel = build_local_kernel(path)
        await kernel.initialize()
        scope = MemoryScope("tenant", user_id="owner", session_id="s")
        foreign = replace(scope, session_id="foreign")
        secret = "ORCHID-824 proposal body"
        try:
            event = MemoryEvent(scope, "user.message", secret)
            await kernel.ingest_event(event)
            proposal = MemoryProposal(scope, "private.code", secret, secret,
                ("missing" if status == "rejected" else event.id,), 0,
                operation=ClaimProposalOperation.CONFLICT if status == "conflict"
                else ClaimProposalOperation.UPSERT, reason=secret)
            result = await kernel.propose(proposal)
            assert result.status == status
            control_event = MemoryEvent(foreign, "user.message", "independent control")
            await kernel.ingest_event(control_event)
            control = replace(proposal, id="foreign-proposal", scope=foreign,
                key="foreign", source_event_ids=(control_event.id,),
                operation=ClaimProposalOperation.UPSERT)
            await kernel.propose(control)
            ids = {"scope": (), "source": proposal.source_event_ids,
                   "proposal": (proposal.id,)}[target]
            request = ForgetRequest(scope, ids, target == "scope", ForgetMode.ERASE)
            if archive_first:
                await kernel.forget(replace(request, mode=ForgetMode.ARCHIVE))
                with closing(sqlite3.connect(path)) as db:
                    assert secret in db.execute("SELECT payload_json FROM proposals WHERE id=?",
                                               (proposal.id,)).fetchone()[0]
            auditor = DeletionAuditService(SQLiteDeletionAuditSink(tmp_path / "audit.db"),
                secret=b"synthetic-deletion-audit-secret-at-least-32-bytes")
            receipt = await auditor.forget(kernel, request, actor="trusted-host")
            assert receipt.status == "succeeded" and auditor.verify(receipt)
            with closing(sqlite3.connect(path)) as db:
                row = db.execute("SELECT payload_json,status,reason,claim_id FROM proposals WHERE id=?",
                                 (proposal.id,)).fetchone()
                assert json.loads(row[0]) == {"erased": True}
                assert row[1:] == (status, None, result.claim_id)
                assert secret in db.execute("SELECT payload_json FROM proposals WHERE id=?",
                                            (control.id,)).fetchone()[0]
                assert db.execute("SELECT content FROM events WHERE id=?",
                                  (control_event.id,)).fetchone()[0] == "independent control"
                audit_id = str(uuid5(NAMESPACE_URL, f"proposal-event:{proposal.id}"))
                assert db.execute("SELECT 1 FROM events WHERE id=?", (audit_id,)).fetchone() is None
                if status == "accepted" and target in ("source", "scope"):
                    assert db.execute("SELECT 1 FROM claims WHERE id=?",
                                      (result.claim_id,)).fetchone() is None
            # The same erasure is idempotent and does not remove the identity fence.
            again = await kernel.forget(request)
            assert again.affected_events == 0
            with closing(sqlite3.connect(path)) as db:
                assert db.execute("SELECT count(*) FROM proposals WHERE id=?",
                                  (proposal.id,)).fetchone()[0] == 1
        finally:
            await kernel.close()
    asyncio.run(run())


def test_erase_authorization_still_precedes_proposal_cleanup(tmp_path):
    async def run():
        path = tmp_path / "core.db"
        kernel = build_local_kernel(path)
        await kernel.initialize()
        scope = MemoryScope("tenant", session_id="s")
        tools = MCPMemoryTools(kernel)
        try:
            event = MemoryEvent(scope, "user.message", "private sentinel")
            await kernel.ingest_event(event)
            proposal = MemoryProposal(scope, "key", "sentinel", "private sentinel", (event.id,), 0)
            await kernel.propose(proposal)
            with pytest.raises(MCPToolError, match="authorized request context"):
                await tools.call_tool("memory_forget", {"all_in_scope": True, "mode": "erase"},
                                      MCPRequestContext(scope))
            with closing(sqlite3.connect(path)) as db:
                assert "private sentinel" in db.execute(
                    "SELECT payload_json FROM proposals WHERE id=?", (proposal.id,),
                ).fetchone()[0]
        finally:
            await kernel.close()
    asyncio.run(run())


def test_erasure_propagates_through_proposal_audit_chain_atomically(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "core.db"
        kernel = build_local_kernel(path)
        await kernel.initialize()
        scope = MemoryScope("tenant", session_id="s")
        try:
            source = MemoryEvent(scope, "user.message", "chain sentinel")
            await kernel.ingest_event(source)
            refs = (source.id,)
            proposals = []
            for index in range(4):
                proposal = MemoryProposal(scope, f"key-{index}", "chain sentinel", "chain sentinel",
                                          refs, 0)
                await kernel.propose(proposal)
                proposals.append(proposal)
                refs = (str(uuid5(NAMESPACE_URL, f"proposal-event:{proposal.id}")),)
            original = kernel._repository._forget_once
            calls = 0
            def fail_second(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError("injected failure")
                return original(*args, **kwargs)
            monkeypatch.setattr(kernel._repository, "_forget_once", fail_second)
            request = ForgetRequest(scope, (source.id,), mode=ForgetMode.ERASE)
            with pytest.raises(RuntimeError, match="injected failure"):
                await kernel.forget(request)
            with closing(sqlite3.connect(path)) as db:
                assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 5
                assert db.execute("SELECT count(*) FROM proposals WHERE payload_json LIKE '%chain sentinel%'").fetchone()[0] == 4
            monkeypatch.setattr(kernel._repository, "_forget_once", original)
            result = await kernel.forget(request)
            assert result.affected_events == 5
            assert result.affected_claims == 4
            with closing(sqlite3.connect(path)) as db:
                assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 0
                assert db.execute("SELECT count(*) FROM claims").fetchone()[0] == 0
                assert all(json.loads(row[0]) == {"erased": True}
                           for row in db.execute("SELECT payload_json FROM proposals"))
        finally:
            await kernel.close()
    asyncio.run(run())


@pytest.mark.parametrize("previous_erasure", [False, True])
def test_source_erasure_propagates_only_to_dependent_scopes(tmp_path, monkeypatch, previous_erasure):
    async def run():
        path = tmp_path / "core.db"
        kernel = build_local_kernel(path)
        await kernel.initialize()
        parent = MemoryScope("tenant", user_id="owner")
        child = replace(parent, session_id="child")
        other = replace(parent, user_id="other", session_id="child")
        try:
            source = MemoryEvent(parent, "user.message", "parent sensitive value")
            await kernel.ingest_event(source)
            dependent = MemoryProposal(child, "dependent", "sensitive", "parent sensitive value",
                                       (source.id,), 0)
            result = await kernel.propose(dependent)
            assert result.status == "accepted"
            independent_event = MemoryEvent(other, "user.message", "independent value")
            await kernel.ingest_event(independent_event)
            independent = MemoryProposal(other, "independent", "independent", "independent value",
                                         (independent_event.id,), 0)
            await kernel.propose(independent)
            # A foreign source ID in an exact-scope request is not authorization
            # to erase that source or proposals in another user's scope.
            await kernel.forget(ForgetRequest(other, (source.id,), mode=ForgetMode.ERASE))
            with closing(sqlite3.connect(path)) as db:
                assert "sensitive" in db.execute("SELECT payload_json FROM proposals WHERE id=?",
                                                 (dependent.id,)).fetchone()[0]
            if previous_erasure:
                # Persist the pre-fix layout: original source deleted, but its
                # proposal and accepted audit copy left behind in a child scope.
                with monkeypatch.context() as patch:
                    patch.setattr(kernel._repository, "_erase_proposals", lambda *_args: [])
                    await kernel.forget(ForgetRequest(parent, (source.id,), mode=ForgetMode.ERASE))
                with closing(sqlite3.connect(path)) as db:
                    assert db.execute("SELECT 1 FROM events WHERE id=?", (source.id,)).fetchone() is None
                    assert "sensitive" in db.execute("SELECT payload_json FROM proposals WHERE id=?",
                                                     (dependent.id,)).fetchone()[0]
            await kernel.forget(ForgetRequest(parent, (source.id,), mode=ForgetMode.ERASE))
            with closing(sqlite3.connect(path)) as db:
                assert json.loads(db.execute("SELECT payload_json FROM proposals WHERE id=?",
                                             (dependent.id,)).fetchone()[0]) == {"erased": True}
                assert db.execute("SELECT 1 FROM claims WHERE id=?", (result.claim_id,)).fetchone() is None
                assert "independent" in db.execute("SELECT payload_json FROM proposals WHERE id=?",
                                                   (independent.id,)).fetchone()[0]
        finally:
            await kernel.close()
    asyncio.run(run())


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_preupgrade_projected_claim_erasure_can_scrub_session_proposal(
    tmp_path, monkeypatch, all_in_scope,
):
    from agent_memory.domain import ScopeLevel

    async def run():
        path = tmp_path / "core.db"
        kernel = build_local_kernel(path)
        await kernel.initialize()
        session = MemoryScope("tenant", user_id="owner", session_id="s")
        owner = session.project(ScopeLevel.USER)
        try:
            source = MemoryEvent(session, "user.message", "SECRET projected claim")
            await kernel.ingest_event(source)
            proposal = MemoryProposal(session, "projected", "SECRET", "SECRET projected claim",
                (source.id,), 0, scope_level=ScopeLevel.USER)
            result = await kernel.propose(proposal)
            assert result.status == "accepted"
            request = ForgetRequest(owner, () if all_in_scope else (result.claim_id,),
                                    all_in_scope, ForgetMode.ERASE)
            with monkeypatch.context() as patch:
                patch.setattr(kernel._repository, "_erase_proposals", lambda *_args: [])
                await kernel.forget(request)
            with closing(sqlite3.connect(path)) as db:
                assert db.execute("SELECT 1 FROM claims WHERE id=?", (result.claim_id,)).fetchone() is None
                assert db.execute("SELECT 1 FROM memory_tombstones WHERE partition_key=? "
                    "AND memory_table='claims' AND id=?",
                    (owner.partition_key(), result.claim_id)).fetchone()
                assert "SECRET" in db.execute("SELECT payload_json FROM proposals WHERE id=?",
                                               (proposal.id,)).fetchone()[0]
            # A tombstone from another scope cannot authorize this cleanup,
            # even when the caller supplies the correct deleted Claim ID.
            await kernel.forget(replace(request, scope=replace(owner, user_id="foreign")))
            with closing(sqlite3.connect(path)) as db:
                assert "SECRET" in db.execute("SELECT payload_json FROM proposals WHERE id=?",
                                               (proposal.id,)).fetchone()[0]
            await kernel.forget(request)
            with closing(sqlite3.connect(path)) as db:
                assert json.loads(db.execute("SELECT payload_json FROM proposals WHERE id=?",
                                             (proposal.id,)).fetchone()[0]) == {"erased": True}
                assert db.execute("SELECT 1 FROM events WHERE id=?",
                    (str(uuid5(NAMESPACE_URL, f"proposal-event:{proposal.id}")),)).fetchone() is None
                # Claim deletion is not permission to erase its original source.
                assert db.execute("SELECT content FROM events WHERE id=?", (source.id,)).fetchone()[0] == source.content
        finally:
            await kernel.close()
    asyncio.run(run())
