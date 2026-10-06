import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import pytest
from agent_memory_postgres import PostgresMemoryRepository
from psycopg import AsyncConnection, sql

from agent_memory.domain import (
    Claim,
    ClaimStatus,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryScope,
    Provenance,
    ScopeLevel,
    utc_now,
)


@asynccontextmanager
async def repository():
    dsn = os.getenv("AGENT_MEMORY_TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.skip("AGENT_MEMORY_TEST_POSTGRES_DSN is not configured")
    parsed = urlsplit(dsn)
    if parsed.scheme not in {"postgres", "postgresql"} or "test" not in parsed.path.casefold():
        pytest.fail("admission tests require a PostgreSQL test database")
    schema = f"admission_{uuid4().hex}"
    async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
        await connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    query = dict(parse_qsl(parsed.query))
    query["options"] = f"-csearch_path={schema}"
    repo = PostgresMemoryRepository.from_dsn(
        urlunsplit(parsed._replace(query=urlencode(query))), max_size=4
    )
    try:
        await repo.initialize()
        yield repo
    finally:
        await repo.close()
        async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
            await connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )


def _scope():
    return MemoryScope("admission-tests", user_id="alice", session_id="session")


def _payload(event, *, claim_id=None):
    return {
        "source_event_ids": [event.id],
        "claim_id": claim_id,
        "evidence": [],
        "action": "pending_verification",
        "draft": {"text": "original assertion"},
    }


async def _seed(repo, *, projected=False, claim=False):
    source_scope = _scope()
    scope = source_scope.project(ScopeLevel.USER) if projected else source_scope
    event = MemoryEvent(
        source_scope, "user.message", "original evidence", idempotency_key="original"
    )
    claim_id = str(uuid4()) if claim else None
    async with repo.unit_of_work() as uow:
        await uow.append_event(event)
        if claim:
            now = utc_now()
            await uow.save_claim(
                Claim(
                    claim_id,
                    scope,
                    "location",
                    "Shanghai",
                    "Lives in Shanghai",
                    1.0,
                    0.5,
                    ClaimStatus.ACTIVE,
                    Provenance((event.id,)),
                    now,
                    now,
                )
            )
        await uow.save_admission_record(
            scope, "record", event.id, "location", _payload(event, claim_id=claim_id), 0
        )
    return event, scope, claim_id


def test_live_admission_versions_cas_and_scope_visibility():
    async def scenario():
        async with repository() as repo:
            event, target, _ = await _seed(repo, projected=True)
            rows = await repo.admission_records(event.scope)
            assert len(rows) == 1 and rows[0]["version"] == 1
            assert rows[0]["scope"]["session_id"] is None
            assert isinstance(rows[0]["recorded_at"], str)
            async with repo.unit_of_work() as uow:
                assert await uow.list_admission_records(event.scope) == ()
                assert await uow.get_admission_record(target, "record") is not None
            other_user = replace(event.scope, user_id="bob")
            assert await repo.admission_record(other_user, "record") is None
            assert await repo.admission_record_versions(other_user, "record") == ()

            async def update(action):
                async with repo.unit_of_work() as uow:
                    return await uow.save_admission_record(
                        target,
                        "record",
                        event.id,
                        "location",
                        {**_payload(event), "action": action},
                        1,
                    )

            outcomes = await asyncio.gather(
                update("accept"), update("contested"), return_exceptions=True
            )
            assert sum(item == 2 for item in outcomes) == 1
            assert sum(isinstance(item, ValueError) for item in outcomes) == 1
            versions = await repo.admission_record_versions(event.scope, "record")
            assert [item["version"] for item in versions] == [1, 2]
            assert versions[1]["recorded_at"] > versions[0]["recorded_at"]
            async with repo.unit_of_work() as uow:
                with pytest.raises(ValueError, match="authorized scope"):
                    await uow.save_admission_record(
                        other_user, "other", event.id, "location", _payload(event), 0
                    )

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_live_admission_source_deletion_fences_projected_records_and_replays(mode):
    async def scenario():
        async with repository() as repo:
            event, target, claim_id = await _seed(repo, projected=True, claim=True)
            result = await repo.forget(ForgetRequest(event.scope, (event.id,), mode=mode))
            assert result.affected_claims == 1
            assert await repo.current_claims(target) == ()
            assert await repo.admission_record(event.scope, "record") is None
            assert await repo.admission_record_versions(event.scope, "record") == ()
            async with repo.pool.connection() as connection:
                cursor = await connection.execute(
                    "SELECT payload_json FROM agent_memory_admission_records"
                )
                assert (await cursor.fetchone())["payload_json"] == {"deleted": True}
                cursor = await connection.execute(
                    "SELECT count(*) AS n FROM agent_memory_admission_versions"
                )
                assert (await cursor.fetchone())["n"] == 0
            async with repo.unit_of_work() as uow:
                with pytest.raises(ValueError, match="identity is unavailable"):
                    await uow.save_admission_record(
                        target, "record", event.id, "location", _payload(event), 0
                    )
            for replay in (event, replace(event, id=str(uuid4()))):
                async with repo.unit_of_work() as uow:
                    with pytest.raises(ValueError, match="deleted source"):
                        await uow.append_event(replay)
            async with repo.unit_of_work() as uow:
                with pytest.raises(ValueError, match="deleted source"):
                    await uow.find_event_by_idempotency(event.scope, event.idempotency_key)

    asyncio.run(scenario())


def test_live_admission_historical_source_and_claim_deletion_scrub_all_snapshots():
    async def scenario():
        async with repository() as repo:
            event, scope, claim_id = await _seed(repo, claim=True)
            verification = MemoryEvent(scope, "tool.result", "verification evidence")
            async with repo.unit_of_work() as uow:
                await uow.append_event(verification)
                payload = {
                    **_payload(event, claim_id=claim_id),
                    "evidence": [{"source_event_id": verification.id}],
                }
                await uow.save_admission_record(scope, "record", event.id, "location", payload, 1)
                await uow.save_admission_record(
                    scope, "record", event.id, "location", _payload(event, claim_id=claim_id), 2
                )
            # This source survives only in a historical admission snapshot.
            await repo.forget(ForgetRequest(scope, (verification.id,), mode=ForgetMode.ERASE))
            assert await repo.admission_records(scope) == ()
            assert await repo.current_claims(scope) == ()
        async with repository() as repo:
            event, scope, claim_id = await _seed(repo, claim=True)
            await repo.forget(ForgetRequest(scope, (claim_id,), mode=ForgetMode.ERASE))
            assert await repo.admission_record(scope, "record") is None
            assert await repo.admission_record_versions(scope, "record") == ()
            async with repo.unit_of_work() as uow:
                assert await uow.events_exist(scope, (event.id,))
                with pytest.raises(ValueError, match="identity is unavailable"):
                    await uow.save_admission_record(
                        scope, "record", event.id, "location", _payload(event), 0
                    )

    asyncio.run(scenario())


def test_live_admission_delete_waits_for_publication_and_prevents_late_save():
    async def scenario():
        async with repository() as repo:
            event, scope, _ = await _seed(repo)
            started, release = asyncio.Event(), asyncio.Event()

            async def publish():
                async with repo.unit_of_work() as uow:
                    await uow.lock_admission_scope(scope)
                    started.set()
                    await release.wait()
                    await uow.save_admission_record(
                        scope, "record", event.id, "location", _payload(event), 1
                    )

            publication = asyncio.create_task(publish())
            await started.wait()
            deletion = asyncio.create_task(
                repo.forget(ForgetRequest(scope, (event.id,), mode=ForgetMode.ERASE))
            )
            release.set()
            await asyncio.gather(publication, deletion)
            assert await repo.admission_records(scope) == ()
            async with repo.unit_of_work() as uow:
                with pytest.raises(ValueError):
                    await uow.save_admission_record(
                        scope, "late-record", event.id, "location", _payload(event), 0
                    )

    asyncio.run(scenario())


def test_live_admission_record_limit_fails_closed():
    async def scenario():
        async with repository() as repo:
            event, scope, _ = await _seed(repo)
            async with repo.pool.connection() as connection:
                await connection.execute(
                    """INSERT INTO agent_memory_admission_records
                        SELECT 'bulk-' || n, partition_key, event_id, slot_key,
                               scope_json, payload_json, version, recorded_at
                        FROM agent_memory_admission_records CROSS JOIN generate_series(1,1024) n
                        WHERE record_id='record'"""
                )
            with pytest.raises(ValueError, match="limit exceeded"):
                await repo.admission_records(scope)
            async with repo.unit_of_work() as uow:
                with pytest.raises(ValueError, match="limit exceeded"):
                    await uow.list_admission_records(scope)
            assert await repo.admission_record(scope, "record") is not None

    asyncio.run(scenario())


def test_live_admission_deleting_new_value_withdraws_entire_slot_without_resurrection():
    async def scenario():
        async with repository() as repo:
            first, scope, _ = await _seed(repo, claim=True)
            second = MemoryEvent(scope, "user.message", "Moved to Hangzhou")
            async with repo.unit_of_work() as uow:
                await uow.append_event(second)
                old_claim = await uow.find_current_claim(scope, "location")
                now = utc_now()
                new_claim = replace(
                    old_claim,
                    id=str(uuid4()),
                    value="Hangzhou",
                    text="Lives in Hangzhou",
                    provenance=Provenance((second.id,)),
                    valid_from=now,
                    created_at=now,
                    version=old_claim.version + 1,
                    supersedes=old_claim.id,
                )
                await uow.replace_current_claim(old_claim, new_claim)
                await uow.save_admission_record(
                    scope,
                    "second-record",
                    second.id,
                    "location",
                    _payload(second, claim_id=new_claim.id),
                    0,
                )
                await uow.save_admission_record(
                    scope, "unrelated-record", first.id, "preference", _payload(first), 0
                )
            assert (await repo.current_claims(scope))[0].value == "Hangzhou"
            await repo.forget(ForgetRequest(scope, (second.id,), mode=ForgetMode.ERASE))
            assert await repo.current_claims(scope) == ()
            assert await repo.admission_records(scope, slot_key="location") == ()
            assert await repo.admission_record_versions(scope, "record") == ()
            assert await repo.admission_record(scope, "unrelated-record") is not None

    asyncio.run(scenario())
