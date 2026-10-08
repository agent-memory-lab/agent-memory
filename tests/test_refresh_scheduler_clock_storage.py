"""Durable UTC high-water and independently pinned backup restoration."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_purge_restore import backup_copy, replay, restorer

from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.operations.refresh_schedule_contract import stamp
from agent_memory.operations.retention import RetentionError

store = base.store


def test_clock_floor_survives_connections_and_repository_reopen(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            async with repo.unit_of_work() as uow:
                assert await uow.refresh_scheduler_observe_clock(scope, now=clock[0].isoformat())
            if hasattr(repo, "pool"):
                from agent_memory_postgres.repository import PostgresMemoryRepository
                peer = PostgresMemoryRepository.from_dsn(repo.pool.conninfo, max_size=2)
            else:
                from agent_memory.sqlite import SQLiteMemoryRepository
                peer = SQLiteMemoryRepository(repo._path)
            await peer.initialize()
            later = clock[0] + timedelta(seconds=5)
            try:
                async with peer.unit_of_work() as uow:
                    assert await uow.refresh_scheduler_observe_clock(
                        replace(scope, tenant_id="second-tenant"), now=later.isoformat())
                async with repo.unit_of_work() as uow:
                    assert not await uow.refresh_scheduler_observe_clock(
                        scope, now=clock[0].isoformat())
                    assert await uow.refresh_scheduler_clock(scope) == stamp(later)
                    assert await uow.refresh_scheduler_observe_clock(scope, now=later.isoformat())
            finally:
                if hasattr(peer, "close"):
                    await peer.close()
    asyncio.run(run())


def test_clock_floor_preserved_by_erasure_and_observation_rollback(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            async with repo.unit_of_work() as uow:
                assert await uow.refresh_scheduler_observe_clock(scope, now=clock[0].isoformat())
            await repo.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            with pytest.raises(RuntimeError):
                async with repo.unit_of_work() as uow:
                    assert await uow.refresh_scheduler_observe_clock(
                        scope, now=(clock[0] + timedelta(days=1)).isoformat())
                    raise RuntimeError("transaction aborted")
            async with repo.unit_of_work() as uow:
                assert await uow.refresh_scheduler_clock(scope) == stamp(clock[0])
                assert not await uow.refresh_scheduler_observe_clock(
                    scope, now=(clock[0] - timedelta(seconds=1)).isoformat())
    asyncio.run(run())


@pytest.mark.parametrize("missing_table", [False, True])
def test_signed_current_checkpoint_raises_old_backup_clock_floor(store, tmp_path, missing_table):
    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            legacy = await restorer(repo, scope, clock).export()
            assert legacy["schema"] == "purge-restore-journal/1"
            assert set(legacy) == {"schema", "checkpoint", "entries", "signature"}
            assert set(legacy["checkpoint"]) == {
                "schema", "authority_id", "scope_key", "head", "scope_epoch", "entries_sha256",
            }
            async with repo.unit_of_work() as uow:
                await uow.refresh_scheduler_observe_clock(scope, now=clock[0].isoformat())
            async with backup_copy(repo, tmp_path) as (backup, _):
                if missing_table:
                    async with backup.unit_of_work() as uow:
                        if hasattr(backup, "pool"):
                            await uow.connection.execute(
                                "DROP TABLE agent_memory_refresh_schedule_contract")
                        else:
                            uow.connection.execute("DROP TABLE refresh_schedule_contract")
                    await backup.initialize()
                later = clock[0] + timedelta(seconds=20)
                async with repo.unit_of_work() as uow:
                    await uow.refresh_scheduler_observe_clock(scope, now=later.isoformat())
                current = await restorer(repo, scope, clock).export()
                assert current["schema"] == "purge-restore-journal/2"
                assert current["checkpoint"]["scheduler_clock_floor"] == stamp(later)
                operator = restorer(backup, scope, clock)
                await replay(operator, current)
                async with backup.unit_of_work() as uow:
                    assert await uow.refresh_scheduler_clock(scope) == stamp(later)
                    assert not await uow.refresh_scheduler_observe_clock(
                        scope, now=clock[0].isoformat())
                    assert await uow.refresh_scheduler_observe_clock(
                        scope, now=(later + timedelta(seconds=1)).isoformat())
                await replay(operator, current)
                async with backup.unit_of_work() as uow:
                    assert await uow.refresh_scheduler_clock(scope) == stamp(
                        later + timedelta(seconds=1))
                with pytest.raises(RetentionError, match="scheduler_clock_missing"):
                    await operator.replay(
                        legacy, expected_checkpoint=legacy["checkpoint"], restore_id="legacy",
                        reason="missing independent clock floor")
    asyncio.run(run())


def test_sqlite_initialization_adds_clock_column_to_old_contract_table(tmp_path):
    import sqlite3

    from agent_memory.sqlite import SQLiteMemoryRepository

    path = tmp_path / "pre-clock.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE refresh_schedule_contract ("
                       "singleton INTEGER PRIMARY KEY, version INTEGER NOT NULL, "
                       "limits_json TEXT, turn INTEGER NOT NULL DEFAULT 0)")
    connection.execute("INSERT INTO refresh_schedule_contract VALUES (1,1,NULL,7)")
    connection.commit()
    connection.close()

    async def run():
        repo = SQLiteMemoryRepository(path)
        await repo.initialize()
        scope = base.MemoryScope("old-backup")
        async with repo.unit_of_work() as uow:
            assert await uow.refresh_scheduler_clock(scope) is None
            assert await uow.refresh_scheduler_observe_clock(scope, now=base.at(1).isoformat())
            assert uow.connection.execute(
                "SELECT turn FROM refresh_schedule_contract WHERE singleton=1").fetchone()[0] == 7
    asyncio.run(run())


def test_restore_export_rejects_partial_scheduler_clock_backend(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            cls = type(engine.repository.unit_of_work())
            with monkeypatch.context() as patch:
                patch.setattr(cls, "refresh_scheduler_observe_clock", None)
                with pytest.raises(RetentionError, match="purge_restore_scheduler_unsupported"):
                    await restorer(engine.repository, scope, clock).export()
    asyncio.run(run())
