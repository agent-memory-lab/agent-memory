"""Admission persistence, version isolation and deletion barriers in SQLite."""

import asyncio
import json
from dataclasses import asdict, replace
from datetime import UTC, datetime

import pytest

from agent_memory.domain import (
    Claim,
    ClaimStatus,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryScope,
    Provenance,
    ScopeLevel,
)
from agent_memory.sqlite import SQLiteMemoryRepository

SCOPE = MemoryScope("tenant", user_id="user", workspace_id="work", session_id="session")


def event(identity="source", **kwargs):
    return MemoryEvent(SCOPE, "message", "source text", id=identity, **kwargs)


def claim(scope, source_ids, identity="claim"):
    now = datetime.now(UTC)
    return Claim(
        identity, scope, "city", "Hangzhou", "Lives in Hangzhou", 1.0, 0.5,
        ClaimStatus.ACTIVE, Provenance(tuple(source_ids)), now, now,
    )


def test_admission_cas_history_and_transaction_rollback(tmp_path, monkeypatch):
    async def run():
        store = SQLiteMemoryRepository(tmp_path / "memory.db")
        await store.initialize()
        await store.initialize()
        monkeypatch.setattr(
            "agent_memory.sqlite.utc_now", lambda: datetime(2026, 10, 6, tzinfo=UTC)
        )
        async with store.unit_of_work() as uow:
            await uow.lock_admission_scope(SCOPE)
            await uow.append_event(event())
            assert await uow.save_admission_record(
                SCOPE, "candidate", "source", "city", {"action": "pending"}, 0
            ) == 1
        original = await store.admission_record(SCOPE, "candidate")
        async with store.unit_of_work() as uow:
            assert await uow.save_admission_record(
                SCOPE, "candidate", "source", "city", {"action": "accepted"}, 1
            ) == 2
        versions = await store.admission_record_versions(SCOPE, "candidate")
        assert [item["payload"]["action"] for item in versions] == ["pending", "accepted"]
        assert versions[0]["recorded_at"] < versions[1]["recorded_at"]
        assert original["scope"] == asdict(SCOPE)
        with pytest.raises(ValueError, match="conflict"):
            async with store.unit_of_work() as uow:
                await uow.append_event(event("rolled-back"))
                await uow.save_admission_record(
                    SCOPE, "candidate", "source", "city", {"action": "bad"}, 1
                )
        assert (await store.admission_record(SCOPE, "candidate"))["version"] == 2
        with store._connection() as connection:
            row = connection.execute("SELECT 1 FROM events WHERE id='rolled-back'").fetchone()
            assert row is None

    asyncio.run(run())


def test_admission_exact_writes_and_visible_reads(tmp_path):
    async def run():
        store = SQLiteMemoryRepository(tmp_path / "memory.db")
        await store.initialize()
        user_scope = SCOPE.project(ScopeLevel.USER)
        sibling = replace(SCOPE, session_id="sibling")
        async with store.unit_of_work() as uow:
            await uow.append_event(event())
            await uow.save_admission_record(user_scope, "b-user", "source", "city", {}, 0)
            await uow.save_admission_record(SCOPE, "a-session", "source", "city", {}, 0)
            assert await uow.get_admission_record(SCOPE, "b-user") is None
            assert [r["id"] for r in await uow.list_admission_records(SCOPE)] == ["a-session"]
            with pytest.raises(ValueError, match="scope"):
                await uow.save_admission_record(sibling, "wrong-session", "source", "city", {}, 0)
            with pytest.raises(ValueError, match="scope"):
                await uow.save_admission_record(
                    replace(SCOPE, tenant_id="other"), "wrong-tenant", "source", "city", {}, 0
                )
        assert [r["id"] for r in await store.admission_records(SCOPE)] == ["a-session", "b-user"]
        assert [r["id"] for r in await store.admission_records(sibling)] == ["b-user"]
        assert await store.admission_record(user_scope, "a-session") is None
        assert await store.admission_record_versions(sibling, "a-session") == ()
        assert await store.admission_records(SCOPE, slot_key="missing") == ()

    asyncio.run(run())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_admission_source_forget_scrubs_history_and_prevents_replay(tmp_path, mode):
    async def run():
        path = tmp_path / "memory.db"
        store = SQLiteMemoryRepository(path)
        await store.initialize()
        async with store.unit_of_work() as uow:
            await uow.append_event(event(idempotency_key="original"))
            await uow.save_admission_record(
                SCOPE, "candidate", "source", "city", {"draft": {"text": "private content"}}, 0
            )
        await store.forget(ForgetRequest(SCOPE, ("source",), mode=mode))
        store = SQLiteMemoryRepository(path)
        await store.initialize()
        assert await store.admission_record(SCOPE, "candidate") is None
        assert await store.admission_record_versions(SCOPE, "candidate") == ()
        assert await store.admission_records(SCOPE) == ()
        for replay in (event(), event("new-id", idempotency_key="original")):
            with pytest.raises(ValueError, match="forgotten"):
                async with store.unit_of_work() as uow:
                    await uow.append_event(replay)
        with store._connection() as connection:
            row = connection.execute("SELECT payload_json FROM admission_records").fetchone()
            assert json.loads(row["payload_json"]) == {"deleted": True}
            assert connection.execute("SELECT count(*) FROM admission_versions").fetchone()[0] == 0

    asyncio.run(run())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_forgetting_secondary_historical_evidence_removes_published_claim(tmp_path, mode):
    async def run():
        store = SQLiteMemoryRepository(tmp_path / "memory.db")
        await store.initialize()
        user_scope = SCOPE.project(ScopeLevel.USER)
        async with store.unit_of_work() as uow:
            await uow.append_event(event())
            await uow.append_event(event("verification"))
            await uow.save_claim(claim(user_scope, ["source", "verification"]))
            await uow.save_admission_record(
                user_scope, "candidate", "source", "city",
                {"claim_id": "claim", "evidence": [{"source_event_id": "verification"}]}, 0,
            )
            await uow.save_admission_record(
                user_scope, "candidate", "source", "city", {"claim_id": "claim"}, 1,
            )
        result = await store.forget(ForgetRequest(SCOPE, ("verification",), mode=mode))
        assert result.affected_claims == 1
        assert await store.admission_record(SCOPE, "candidate") is None
        assert await store.current_claims(SCOPE) == ()
        assert await store.admission_record_versions(SCOPE, "candidate") == ()
        with store._connection() as connection:
            assert connection.execute("SELECT 1 FROM events WHERE id='source'").fetchone()

    asyncio.run(run())


def test_deleting_claim_prevents_candidate_recreation_without_deleting_source(tmp_path):
    async def run():
        store = SQLiteMemoryRepository(tmp_path / "memory.db")
        await store.initialize()
        async with store.unit_of_work() as uow:
            await uow.append_event(event())
            await uow.save_claim(claim(SCOPE, ["source"]))
            await uow.save_admission_record(
                SCOPE, "candidate", "source", "city", {"claim_id": "claim"}, 0
            )
        await store.forget(ForgetRequest(SCOPE, ("claim",), mode=ForgetMode.ERASE))
        async with store.unit_of_work() as uow:
            assert await uow.events_exist(SCOPE, ["source"])
            assert await uow.get_admission_record(SCOPE, "candidate") is None
            for expected_version in (0, 1, 2):
                with pytest.raises(ValueError, match="deleted"):
                    await uow.save_admission_record(
                        SCOPE, "candidate", "source", "city", {}, expected_version
                    )

    asyncio.run(run())


def test_admission_list_limit_never_returns_partial_candidates(tmp_path):
    async def run():
        store = SQLiteMemoryRepository(tmp_path / "memory.db")
        await store.initialize()
        async with store.unit_of_work() as uow:
            await uow.append_event(event())
            for index in range(1025):
                await uow.save_admission_record(
                    SCOPE, f"record-{index:04}", "source", f"slot-{index}", {}, 0
                )
        with pytest.raises(ValueError, match="limit"):
            await store.admission_records(SCOPE)
        async with store.unit_of_work() as uow:
            with pytest.raises(ValueError, match="limit"):
                await uow.list_admission_records(SCOPE)
            assert len(await uow.list_admission_records(SCOPE, "slot-0")) == 1

    asyncio.run(run())


def test_deleting_later_state_withdraws_slot_without_reviving_old_value(tmp_path):
    async def run():
        store = SQLiteMemoryRepository(tmp_path / "memory.db")
        await store.initialize()
        first = claim(SCOPE, ["source"], "first-claim")
        second = replace(
            claim(SCOPE, ["later"], "second-claim"), value="Shanghai", text="Lives in Shanghai"
        )
        async with store.unit_of_work() as uow:
            await uow.append_event(event())
            await uow.append_event(event("later"))
            await uow.append_event(event("unrelated"))
            await uow.save_claim(first)
            await uow.save_admission_record(
                SCOPE, "first", "source", "city", {"claim_id": first.id}, 0
            )
            await uow.replace_current_claim(first, second)
            await uow.save_admission_record(
                SCOPE, "second", "later", "city", {"claim_id": second.id}, 0
            )
            await uow.save_admission_record(SCOPE, "other", "unrelated", "language", {}, 0)
        await store.forget(ForgetRequest(SCOPE, ("later",), mode=ForgetMode.ERASE))
        assert await store.current_claims(SCOPE) == ()
        assert [row["id"] for row in await store.admission_records(SCOPE)] == ["other"]
        assert await store.admission_record_versions(SCOPE, "first") == ()
        assert await store.admission_record_versions(SCOPE, "second") == ()
        with store._connection() as connection:
            assert connection.execute("SELECT 1 FROM events WHERE id='source'").fetchone()

    asyncio.run(run())
