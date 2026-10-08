"""Shared SQL scheduler accounting, index selection, ownership and lifecycle contracts."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base

from agent_memory.derived.model import DerivedError
from agent_memory.domain import ForgetMode, ForgetRequest

store = base.store
LIMITS = dict(global_pending=8, tenant_pending=4, instance_pending=1,
              global_running=1, tenant_running=1, instance_running=1, revision="storage-test/1")


def demand(scope, now, key="d1", **changes):
    return dict(schema="refresh-demand/1", id=key, facet_id=key,
                tenant_id=scope.tenant_id, instance_key=key, adapter_key="test/1", epoch=0,
                requested=[key], covered=[], status="pending", priority=0,
                first_dirty_at=now.isoformat(), created_at=now.isoformat(),
                due_at=now.isoformat(), **changes)


async def put(repo, scope, now, key="d1"):
    async with repo.unit_of_work() as uow:
        await uow.refresh_scheduler_lock(scope)
        await uow.refresh_scheduler_config(LIMITS)
        await uow.derived_put(scope, "refresh_demand", key, demand(scope, now, key))


def test_durable_fairness_capability_filter_and_mutable_input_ownership(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            other = replace(scope, tenant_id="other")
            for key in ("a1", "a2", "a3"):
                await put(repo, scope, clock[0], key)
            await put(repo, other, clock[0], "b1")
            async with repo.unit_of_work() as uow:
                rows = await uow.refresh_scheduler_due(
                    now=clock[0].isoformat(), adapter_keys=("test/1",), limit=2)
                assert len({row["scope"]["tenant_id"] for row in rows}) == 2
                assert not await uow.refresh_scheduler_due(
                    now=clock[0].isoformat(), adapter_keys=("unsupported/1",))
                rows[0]["payload"]["requested"].append("caller-owned")
            async with repo.unit_of_work() as uow:
                await uow.refresh_scheduler_lock(scope)
                await uow.refresh_scheduler_turn(scope)
            async with repo.unit_of_work() as uow:
                rows = await uow.refresh_scheduler_due(
                    now=clock[0].isoformat(), adapter_keys=("test/1",), limit=1)
                assert rows[0]["scope"]["tenant_id"] == other.tenant_id
                original = await uow.derived_get(scope, "refresh_demand", "a1")
                assert original["requested"] == ["a1"]
    asyncio.run(run())


def test_shared_last_slot_config_mismatch_expiry_and_rollback(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            other = replace(scope, tenant_id="other")

            async def reserve(s, identity):
                async with repo.unit_of_work() as uow:
                    return await uow.refresh_scheduler_reserve(
                        s, identity, instance_key=identity, units=1, limits=LIMITS)

            results = await asyncio.gather(reserve(scope, "a"), reserve(other, "b"))
            assert sum(results) == 1
            winner, loser = (scope, other) if results[0] else (other, scope)
            winner_id = "a" if results[0] else "b"
            clock[0] += timedelta(days=20)
            async with repo.unit_of_work() as uow:
                usage = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(), tenant_id=scope.tenant_id, instance_key="a")
                assert usage["global_running"] == 1
            with pytest.raises(DerivedError, match="config_mismatch"):
                async with repo.unit_of_work() as uow:
                    await uow.refresh_scheduler_reserve(
                        loser, "c", instance_key="c", units=1,
                        limits={**LIMITS, "global_running": 2})
            with pytest.raises(RuntimeError):
                async with repo.unit_of_work() as uow:
                    await uow.refresh_scheduler_release(winner, winner_id)
                    raise RuntimeError("rollback release")
            assert not await reserve(loser, "c")
            async with repo.unit_of_work() as uow:
                await uow.refresh_scheduler_release(winner, winner_id)
            assert await reserve(loser, "c")
    asyncio.run(run())


def test_immutable_execution_and_expired_selector(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            row = demand(scope, clock[0])
            row.update(status="running", lease_until=clock[0].isoformat())
            execution = dict(schema="refresh-execution/1", id="e", demand_id="d1",
                             facet_id="d1", adapter_key="test/1",
                             epoch=0, compatibility="x", claimed=["one"], unit_id="unit",
                             unit={"fixed": ["one"]}, created_at=clock[0].isoformat(),
                             status="running", generation=1)
            async with repo.unit_of_work() as uow:
                await uow.derived_put(scope, "refresh_demand", "d1", row)
                await uow.derived_put(scope, "refresh_execution", "e", execution)
            execution["claimed"].append("two")
            with pytest.raises(DerivedError, match="immutable"):
                async with repo.unit_of_work() as uow:
                    await uow.derived_put(scope, "refresh_execution", "e", execution)
            async with repo.unit_of_work() as uow:
                assert not await uow.refresh_scheduler_due(
                    now=clock[0].isoformat(), adapter_keys=("test/1",))
                expired = await uow.refresh_scheduler_expired(
                    now=clock[0].isoformat(), adapter_keys=("test/1",))
                assert [row["identity"] for row in expired] == ["d1"]
                saved = await uow.derived_get(scope, "refresh_execution", "e")
                saved.update(status="retry", generation=2)
                await uow.derived_put(scope, "refresh_execution", "e", saved)
    asyncio.run(run())


def test_erasure_scrubs_scheduler_but_preserves_other_scope_and_barrier(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            other = replace(scope, session_id="other-session")
            await put(repo, scope, clock[0], "private-selector")
            await put(repo, other, clock[0], "other")
            async with repo.unit_of_work() as uow:
                await uow.refresh_scheduler_reserve(
                    scope, "private-execution", instance_key="private-selector", limits=LIMITS)
                await uow.refresh_scheduler_turn(scope)
                await uow.derived_put(scope, "barrier", "authoritative", {"generation": 9})
            await repo.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            async with repo.unit_of_work() as uow:
                assert await uow.derived_get(scope, "refresh_demand", "private-selector") is None
                assert await uow.derived_get(other, "refresh_demand", "other") is not None
                assert (await uow.derived_get(scope, "barrier", "authoritative"))["generation"] >= 9
                usage = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(), tenant_id=scope.tenant_id,
                    instance_key="private-selector")
                assert usage["global_running"] == 0
                assert usage["global_pending"] == 1
    asyncio.run(run())


def test_sqlite_due_and_expired_indexes_are_used(tmp_path):
    import sqlite3

    from agent_memory.operations import sqlite_derived
    from agent_memory.operations import sqlite_refresh_schedule as schedule
    connection = sqlite3.connect(tmp_path / "indexes.db")
    try:
        connection.executescript(sqlite_derived.SCHEMA + schedule.SCHEMA)
        for expired, expected in ((False, "refresh_schedule_due_idx"),
                                  (True, "refresh_schedule_expired_idx")):
            rows = connection.execute("EXPLAIN QUERY PLAN " + schedule.due_sql(1, expired=expired),
                                      (base.at(1).isoformat(), "test/1", base.at(1).isoformat(), 2))
            assert expected in " ".join(str(row) for row in rows)
    finally:
        connection.close()


def _reserve_process(path, scope_data, gate, out):
    """Spawn-safe worker proving independent SQLite connections share the hard cap."""
    from agent_memory.domain import MemoryScope
    from agent_memory.sqlite import SQLiteMemoryRepository

    async def run():
        repo = SQLiteMemoryRepository(path)
        scope = MemoryScope(**scope_data)
        gate.wait(20)
        async with repo.unit_of_work() as uow:
            return await uow.refresh_scheduler_reserve(
                scope, "execution", instance_key="instance", units=1, limits=LIMITS)
    try:
        out.put(("ok", asyncio.run(run())))
    except BaseException as error:
        out.put(("error", repr(error)))


def test_sqlite_independent_processes_compete_for_last_global_slot(tmp_path):
    import multiprocessing
    from dataclasses import asdict

    from agent_memory.domain import MemoryScope
    from agent_memory.sqlite import SQLiteMemoryRepository

    path = str(tmp_path / "shared-accounting.db")
    asyncio.run(SQLiteMemoryRepository(path).initialize())
    context = multiprocessing.get_context("spawn")
    gate, out = context.Event(), context.Queue()
    processes = [context.Process(target=_reserve_process,
                                 args=(path, asdict(MemoryScope(tenant)), gate, out))
                 for tenant in ("one", "two")]
    try:
        for process in processes:
            process.start()
        gate.set()
        results = [out.get(timeout=30) for _ in processes]
        assert all(status == "ok" for status, _ in results), results
        assert sum(result for _, result in results) == 1
        for process in processes:
            process.join(timeout=20)
            assert process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join()
        out.close()
        out.join_thread()


def test_actual_backup_restore_scrubs_scheduler_and_keeps_common_limits(store, tmp_path):
    from test_purge_restore import backup_copy, replay, restorer

    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            other = replace(scope, session_id="unrelated")
            await put(repo, scope, clock[0], "private")
            await put(repo, other, clock[0], "survivor")
            async with repo.unit_of_work() as uow:
                await uow.refresh_scheduler_reserve(
                    scope, "private-execution", instance_key="private", limits=LIMITS)
                await uow.derived_put(scope, "coverage_request", "receipt", {
                    "schema": "coverage-receipt/1", "required": ["private"], "selector": "secret",
                })
            async with backup_copy(repo, tmp_path) as (backup, _):
                await repo.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
                journal = await restorer(repo, scope, clock).export()
                operator = restorer(backup, scope, clock)
                await replay(operator, journal)
                async with backup.unit_of_work() as uow:
                    assert await uow.derived_get(scope, "refresh_demand", "private") is None
                    assert await uow.derived_get(other, "refresh_demand", "survivor") is not None
                    assert await uow.derived_get(scope, "coverage_request", "receipt") == {
                        "schema": "coverage-receipt/1", "state": "erased",
                    }
                    rows = await uow.refresh_scheduler_due(
                        now=clock[0].isoformat(), adapter_keys=("test/1",))
                    assert [row["identity"] for row in rows] == ["survivor"]
                    usage = await uow.refresh_scheduler_usage(
                        now=clock[0].isoformat(), tenant_id=scope.tenant_id, instance_key="private")
                    assert usage["global_running"] == 0
                    assert await uow.retention_epoch(scope) == 1
                    await uow.refresh_scheduler_lock(scope)
                    await uow.refresh_scheduler_config(LIMITS)
                with pytest.raises(DerivedError, match="config_mismatch"):
                    async with backup.unit_of_work() as uow:
                        await uow.refresh_scheduler_lock(scope)
                        await uow.refresh_scheduler_config({**LIMITS, "revision": "unauthorized"})
    asyncio.run(run())


@pytest.mark.parametrize("restore", [False, True])
def test_projected_source_erasure_scrubs_affected_scheduler_scopes_only(store, restore):
    from test_v7_backend_lifecycle import seed

    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            projected = replace(scope, session_id=None)
            unrelated = replace(scope, session_id="unrelated")
            private = base.source(scope, identity="private-source-session")
            await seed(repo, projected, suffix="surviving-projection")
            await seed(repo, unrelated, suffix="unrelated")
            async with repo.unit_of_work() as uow:
                await uow.append_event(private)
                await uow.save_admission_record(
                    projected, "promoted-private", private.id, "private-slot",
                    {"source_event_ids": [private.id]}, 0)
            for target, key in ((scope, "original"), (projected, "projected"),
                                (unrelated, "unrelated")):
                await put(repo, target, clock[0], key)
            async with repo.unit_of_work() as uow:
                await uow.refresh_scheduler_reserve(
                    projected, "promoted-running", instance_key="projected", limits=LIMITS)
            request = ForgetRequest(scope, memory_ids=(private.id,), mode=ForgetMode.ERASE)
            if restore:
                async with repo.unit_of_work() as uow:
                    await uow.forget_for_restore(request)
            else:
                await repo.forget(request)
            async with repo.unit_of_work() as uow:
                assert await uow.derived_get(scope, "refresh_demand", "original") is None
                assert await uow.derived_get(projected, "refresh_demand", "projected") is None
                assert await uow.derived_get(unrelated, "refresh_demand", "unrelated") is not None
                assert await uow.get_admission_record(projected, "candidate-surviving-projection")
                assert await uow.get_source_event(scope, private.id) is None
                if not restore:
                    assert await uow.source_erased(scope, private.id)
                usage = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(), tenant_id=scope.tenant_id, instance_key="projected")
                assert usage["global_running"] == 0
                assert usage["global_pending"] == 1
    asyncio.run(run())


def test_scheduler_erasure_and_reservation_roll_back_with_authoritative_transaction(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            await put(repo, scope, clock[0], "retained")
            async with repo.unit_of_work() as uow:
                await uow.refresh_scheduler_reserve(
                    scope, "retained-execution", instance_key="retained", limits=LIMITS)
            with pytest.raises(RuntimeError, match="after scheduler scrub"):
                async with repo.unit_of_work() as uow:
                    await uow.forget_for_restore(ForgetRequest(
                        scope, all_in_scope=True, mode=ForgetMode.ERASE))
                    raise RuntimeError("after scheduler scrub")
            async with repo.unit_of_work() as uow:
                assert await uow.retention_epoch(scope) == 0
                assert await uow.derived_get(scope, "refresh_demand", "retained")
                usage = await uow.refresh_scheduler_usage(
                    now=clock[0].isoformat(), tenant_id=scope.tenant_id, instance_key="retained")
                assert usage["global_running"] == usage["global_pending"] == 1
    asyncio.run(run())


def test_postgres_due_and_expired_indexes_are_available(store):
    async def run():
        async with store() as (engine, _, _, clock):
            if not hasattr(engine.repository, "pool"):
                pytest.skip("PostgreSQL query-plan contract")
            from agent_memory_postgres import refresh_schedule as schedule

            async with engine.repository.unit_of_work() as uow:
                # A tiny fixture legitimately prefers sequential scans. Force index
                # consideration to verify each range+capability query has its index.
                await uow.connection.execute("SET LOCAL enable_seqscan=off")
                for expired, expected in ((False, "refresh_schedule_due_idx"),
                                          (True, "refresh_schedule_expired_idx")):
                    cursor = await uow.connection.execute(
                        "EXPLAIN " + schedule.due_sql(expired=expired),
                        (clock[0].isoformat(), ["test/1"], clock[0].isoformat(), 2))
                    assert expected in " ".join(str(row) for row in await cursor.fetchall())
    asyncio.run(run())


def test_unknown_record_schema_fails_closed_but_does_not_block_erasure(store):
    import json

    async def run():
        async with store() as (engine, _, scope, clock):
            repo = engine.repository
            row = demand(scope, clock[0])
            row["schema"] = "refresh-demand/99"
            with pytest.raises(DerivedError, match="record_unsupported"):
                async with repo.unit_of_work() as uow:
                    await uow.derived_put(scope, "refresh_demand", "d1", row)
            await put(repo, scope, clock[0])
            async with repo.unit_of_work() as uow:
                if hasattr(repo, "pool"):
                    await uow.connection.execute(
                        "UPDATE agent_memory_derived_entries SET payload_json=%s::jsonb "
                        "WHERE partition_key=%s AND kind='refresh_demand'",
                        (json.dumps(row), scope.partition_key()))
                else:
                    uow.connection.execute(
                        "UPDATE derived_entries SET payload_json=? "
                        "WHERE partition_key=? AND kind='refresh_demand'",
                        (json.dumps(row), scope.partition_key()))
            with pytest.raises(DerivedError, match="record_unsupported"):
                async with repo.unit_of_work() as uow:
                    await uow.derived_get(scope, "refresh_demand", "d1")
            with pytest.raises(DerivedError, match="record_unsupported"):
                async with repo.unit_of_work() as uow:
                    await uow.refresh_scheduler_due(
                        now=clock[0].isoformat(), adapter_keys=("test/1",))
            await repo.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            async with repo.unit_of_work() as uow:
                assert await uow.derived_get(scope, "refresh_demand", "d1") is None
    asyncio.run(run())


@pytest.mark.parametrize("status", ["deferred", "retry"])
def test_retry_projection_preserves_backoff_over_past_semantic_boundary(status):
    from agent_memory.operations.refresh_schedule_contract import projection, stamp

    scope, now = base.MemoryScope("projection-tests"), base.at(1)
    row = demand(scope, now)
    row.update(status=status, runnable_at=(now + timedelta(hours=1)).isoformat(),
               next_transition_at=(now - timedelta(seconds=1)).isoformat())
    assert projection(scope, "d1", row)[7] == stamp(row["runnable_at"])
    assert row["next_transition_at"] == (now - timedelta(seconds=1)).isoformat()


def test_execution_policy_snapshot_is_immutable(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            row = dict(schema="refresh-execution/1", id="execution", claimed=["fixed"],
                       policy={"revision": "one", "max_attempts": 3},
                       created_at=clock[0].isoformat(), status="running")
            async with engine.repository.unit_of_work() as uow:
                await uow.derived_put(scope, "refresh_execution", "execution", row)
            row["policy"]["max_attempts"] = 99
            with pytest.raises(DerivedError, match="refresh_execution_immutable"):
                async with engine.repository.unit_of_work() as uow:
                    await uow.derived_put(scope, "refresh_execution", "execution", row)
            async with engine.repository.unit_of_work() as uow:
                saved = await uow.derived_get(scope, "refresh_execution", "execution")
                assert saved["policy"] == {"revision": "one", "max_attempts": 3}
    asyncio.run(run())
