"""Bounded discovery and stage progress on both admission backends."""

import asyncio
from dataclasses import replace

import pytest
import test_atom_admission as base

from agent_memory.consolidation.admission import AdmissionPolicy, draft_to_payload
from agent_memory.consolidation.atom_extraction import AtomExtractionPipeline
from agent_memory.consolidation.extraction_rules import RuleBasedAtomAdapter
from agent_memory.domain import PredicateSpec, ScopeLevel
from agent_memory.operations.domain_verification import (
    DomainVerificationQueue,
    VerificationFinding,
    VerificationToolSpec,
)
from agent_memory.operations.memory_host import MemoryHost

store = base.store


async def seed(repository, scope, *, terminal=0, pending=1, prefix="candidate"):
    event = base.source(scope)
    async with repository.unit_of_work() as uow:
        await uow.lock_admission_scope(scope)
        await uow.append_event(event)
        for index in range(terminal + pending):
            await uow.save_admission_record(
                scope,
                f"{prefix}:{index:04}",
                event.id,
                f"slot:{prefix}:{index:04}",
                {
                    "action": "REJECT" if index < terminal else "PENDING_VERIFICATION",
                    "draft": draft_to_payload(base.atom()),
                    "source_event_ids": [event.id],
                },
                0,
            )
    return tuple(f"{prefix}:{index:04}" for index in range(terminal, terminal + pending))


def host_for(engine, scope, clock, *, capacity=128):
    class Tool:
        spec = VerificationToolSpec("registry", "1", base.TOOL, ("alice",), ("city",))

        async def verify(self, candidate):
            return VerificationFinding("unknown")

    async def allowed(*_):
        return True

    async def publish(*_):
        raise AssertionError("unknown findings must not publish")

    queue = DomainVerificationQueue(
        engine.repository,
        scope,
        (Tool(),),
        authorize=allowed,
        publisher=publish,
        clock=lambda: clock[0],
        timeout_seconds=1,
        capacity=capacity,
    )
    rules = RuleBasedAtomAdapter("alice")
    return MemoryHost(
        engine.repository,
        scope,
        AtomExtractionPipeline(rules, rules),
        AdmissionPolicy([PredicateSpec("city")]),
        base.SELF,
        on_accept=allowed,
        verification=queue,
        clock=lambda: clock[0],
    )


def test_terminal_and_ancestor_history_does_not_block_verification(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            await seed(engine.repository, scope, terminal=140, pending=2)
            await seed(
                engine.repository, scope.project(ScopeLevel.USER), pending=140, prefix="ancestor"
            )
            await seed(
                engine.repository,
                replace(scope, user_id="someone-else"),
                pending=140,
                prefix="other",
            )
            host = host_for(engine, scope, clock)
            cycle = await host.run_once()
            assert cycle["verification_scheduled"] == 2
            assert cycle["verification"] == "unknown"

    asyncio.run(run())


def test_pending_keyset_pages_resume_after_restart_and_wrap(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            ids = await seed(engine.repository, scope, pending=130)
            host = host_for(engine, scope, clock, capacity=200)

            async def forbidden_scan(*_, **__):
                raise AssertionError("visible admission scans are not host discovery")

            monkeypatch.setattr(engine.repository, "admission_records", forbidden_scan)
            assert await host._schedule_verification() == 128
            async with engine.repository.unit_of_work() as uow:
                cursor = await uow.derived_get(scope, "verification_discovery", "scope")
                assert cursor["after"] == ids[127]
            restarted = host_for(engine, scope, clock, capacity=200)
            assert await restarted._schedule_verification() == 2
            assert (await restarted.verification.backlog())["active"] == 130
            # Insert behind the cursor: a complete sweep wraps and eventually sees it.
            await seed(engine.repository, scope, pending=1, prefix="a-late")
            assert await restarted._schedule_verification() == 128
            assert (await restarted.verification.backlog())["active"] == 131

    asyncio.run(run())


def test_terminal_receipts_never_exhaust_active_capacity_or_repeat_unknown(store, monkeypatch):
    from agent_memory.operations.domain_verification import KIND

    async def run():
        async with store() as (engine, _, scope, clock):
            await seed(engine.repository, scope, pending=1)
            host = host_for(engine, scope, clock, capacity=1)
            async with engine.repository.unit_of_work() as uow:
                for index in range(4200):
                    await uow.derived_put(
                        scope,
                        KIND,
                        f"history:{index:04}",
                        {"state": ("completed", "cancelled", "dead", "erased")[index % 4]},
                    )
                cls = type(uow)
            original = cls.derived_records

            async def without_task_scan(uow, requested_scope, kind):
                assert kind != KIND, "terminal history must not be materialized"
                return await original(uow, requested_scope, kind)

            monkeypatch.setattr(cls, "derived_records", without_task_scan)
            assert (await host.run_once())["verification"] == "unknown"
            assert (await host.verification.backlog())["active"] == 0
            # Same candidate/version/tool remains pending, but its request is terminal.
            restarted = host_for(engine, scope, clock, capacity=1)
            assert (await restarted.run_once())["verification"] == "idle"
            await seed(engine.repository, scope, pending=1, prefix="next")
            assert (await restarted.run_once())["verification"] == "unknown"
            assert (await restarted.metrics())["verification"] == {}

    asyncio.run(run())


def test_capacity_backpressure_drains_and_reports_age_without_dropping_facts(store):
    from datetime import timedelta

    async def run():
        async with store() as (engine, _, scope, clock):
            ids = await seed(engine.repository, scope, pending=4)
            host = host_for(engine, scope, clock, capacity=2)
            assert await host._schedule_verification() == 2
            assert host._verification_backpressured
            async with engine.repository.unit_of_work() as uow:
                cursor = await uow.derived_get(scope, "verification_discovery", "scope")
                assert cursor["after"] == ids[1]
            clock[0] += timedelta(days=2)
            metrics = await host.metrics()
            assert metrics["schema"] == "memory-host-metrics/2"
            assert metrics["verification_backlog"]["active"] == 2
            assert metrics["verification_backlog"]["backpressured"]
            assert metrics["verification_backlog"]["oldest_age_seconds"] == 172800
            # Full discovery must not prevent an already queued verification from completing.
            cycle = await host.run_once()
            assert cycle["verification_backpressured"] and cycle["verification"] == "unknown"
            restarted = host_for(engine, scope, clock, capacity=2)
            for _ in range(3):
                assert (await restarted.run_once())["verification"] == "unknown"
            assert (await restarted.verification.backlog())["active"] == 0
            assert (await restarted.verification.backlog())["oldest_age_seconds"] == 0
            for identity in ids:
                row = await engine.repository.admission_record(scope, identity)
                assert row["version"] == 1 and row["payload"]["action"] == "PENDING_VERIFICATION"

    asyncio.run(run())


def test_failed_candidates_do_not_starve_later_discovery(store):
    from agent_memory.retrieval.model_contracts import ModelError

    async def run():
        async with store() as (engine, _, scope, clock):
            ids = await seed(engine.repository, scope, pending=3)
            host = host_for(engine, scope, clock)
            schedule = host.verification.schedule

            async def fail_one(candidate_id, *args, **kwargs):
                if candidate_id == ids[0]:
                    raise ModelError("private_candidate_marker")
                return await schedule(candidate_id, *args, **kwargs)

            host.verification.schedule = fail_one
            result = await host.run_once()
            assert result["verification_scheduled"] == 2
            assert result["verification"] == "unknown"
            assert result["stage_errors"] == {
                "verification_discovery": "memory_host_verification_discovery_failed"
            }
            assert "private_candidate_marker" not in str(await host.metrics())
            # Repair and restart: the failed candidate is retried on a later sweep.
            host.verification.schedule = schedule
            result = await host.run_once()
            assert result["stage_errors"] == {}
            assert (await host.metrics())["last_error_code"] is None

    asyncio.run(run())


@pytest.mark.parametrize(
    "failed_stage", ["extraction", "verification_discovery", "verification", "refresh"]
)
def test_each_failed_stage_leaves_independent_stages_runnable(store, failed_stage):
    from types import SimpleNamespace

    from agent_memory.operations.worker_runtime import WorkerBatchResult

    async def run():
        async with store() as (engine, _, scope, clock):
            host = host_for(engine, scope, clock)
            calls = []
            fail = [True]

            def callback(name, result):
                async def invoke(*_, **__):
                    calls.append(name)
                    if fail[0] and name == failed_stage:
                        raise RuntimeError("private authority body")
                    return result

                return invoke

            host.worker.run_batch = callback("extraction", WorkerBatchResult(1, 1, 0, False))
            host._schedule_verification = callback("verification_discovery", 1)
            host.verification.run_once = callback("verification", "unknown")
            host.refresh = SimpleNamespace(
                run_once=callback("refresh", WorkerBatchResult(1, 1, 0, False))
            )
            result = await host.run_once()
            assert calls == ["extraction", "verification_discovery", "verification", "refresh"]
            assert result["stage_errors"] == {failed_stage: f"memory_host_{failed_stage}_failed"}
            assert "private authority body" not in str(result)
            metrics = await host.metrics()
            assert metrics["stage_errors"] == result["stage_errors"]
            assert metrics["last_error_code"] == "memory_host_stage_failed"
            fail[0] = False
            assert (await host.run_once())["stage_errors"] == {}
            assert (await host.metrics())["last_error_code"] is None

    asyncio.run(run())


def test_discovery_selectors_are_indexed_bounded_and_exclude_qualified(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            ids = await seed(engine.repository, scope, terminal=140, pending=4)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.get_admission_record(scope, ids[0])
                await uow.save_admission_record(
                    scope,
                    row["id"],
                    row["event_id"],
                    row["slot_key"],
                    {**row["payload"], "action": "CONTESTED"},
                    row["version"],
                )
                row = await uow.get_admission_record(scope, ids[1])
                await uow.save_admission_record(
                    scope,
                    row["id"],
                    row["event_id"],
                    row["slot_key"],
                    {**row["payload"], "qualification": {"status": "qualified"}},
                    row["version"],
                )
                page = await uow.verification_candidates(scope, limit=2)
                assert [row["id"] for row in page] == [ids[0], ids[2]]
                last = await uow.verification_candidates(scope, after=ids[2])
                assert [row["id"] for row in last] == [ids[3]]
                for invalid in (0, 129, True):
                    with pytest.raises(ValueError):
                        await uow.verification_candidates(scope, limit=invalid)
                with pytest.raises(ValueError):
                    await uow.verification_active(scope, limit=4097)
                if type(engine.repository).__name__ == "SQLiteMemoryRepository":
                    from agent_memory.operations.sqlite_verification import ACTIVE, PENDING

                    for table, where, column, index in (
                        (
                            "admission_records",
                            PENDING,
                            "record_id",
                            "admission_verification_pending_idx",
                        ),
                        ("derived_entries", ACTIVE, "identity", "derived_verification_active_idx"),
                    ):
                        plan = uow.connection.execute(
                            f"EXPLAIN QUERY PLAN SELECT * FROM {table} WHERE {where} "
                            f"AND partition_key=? AND {column}>? ORDER BY {column} LIMIT 2",
                            (scope.partition_key(), ""),
                        ).fetchall()
                        assert index in str([tuple(row) for row in plan])
                else:
                    from agent_memory_postgres.verification import ACTIVE, PENDING

                    await uow.connection.execute("SET LOCAL enable_seqscan=off")
                    for table, where, column, index in (
                        (
                            "agent_memory_admission_records",
                            PENDING,
                            "record_id",
                            "agent_memory_admission_verification_pending_idx",
                        ),
                        (
                            "agent_memory_derived_entries",
                            ACTIVE,
                            "identity",
                            "agent_memory_derived_verification_active_idx",
                        ),
                    ):
                        result = await uow.connection.execute(
                            f"EXPLAIN (FORMAT JSON) SELECT * FROM {table} WHERE {where} "
                            f"AND partition_key=%s AND {column}>%s ORDER BY {column} LIMIT 2",
                            (scope.partition_key(), ""),
                        )
                        assert index in str(await result.fetchall())

    asyncio.run(run())


def test_receipt_erasure_stays_available_after_large_terminal_history(store):
    from agent_memory.domain import ForgetMode, ForgetRequest
    from agent_memory.operations.domain_verification import KIND

    async def run():
        async with store() as (engine, kernel, scope, clock):
            ids = await seed(engine.repository, scope, pending=1)
            host = host_for(engine, scope, clock)
            await host._schedule_verification()
            row = await engine.repository.admission_record(scope, ids[0])
            async with engine.repository.unit_of_work() as uow:
                for index in range(4200):
                    await uow.derived_put(
                        scope,
                        KIND,
                        f"history:{index:04}",
                        {
                            "state": "completed",
                            "sources": [row["event_id"]],
                            "parents": ["atom:" + ids[0]],
                        },
                    )
            await kernel.forget(ForgetRequest(scope, (row["event_id"],), mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                cursor = await uow.derived_get(scope, "verification_discovery", "scope")
                assert cursor == {"state": "erased"}
                assert await uow.derived_get(scope, KIND, "history:0000") == {"state": "erased"}
                assert await uow.derived_get(scope, KIND, "history:4199") == {"state": "erased"}
                assert await uow.verification_active(scope) == ()
            assert (await host.run_once())["verification"] == "idle"

    asyncio.run(run())


def test_discovery_cursor_cannot_resurrect_an_erased_candidate(store):
    from agent_memory.domain import ForgetMode, ForgetRequest

    async def run():
        async with store() as (engine, kernel, scope, clock):
            ids = await seed(engine.repository, scope, pending=1)
            row = await engine.repository.admission_record(scope, ids[0])
            host = host_for(engine, scope, clock)
            original = host.verification.schedule

            async def erase_after_schedule(*args, **kwargs):
                result = await original(*args, **kwargs)
                await kernel.forget(ForgetRequest(scope, (row["event_id"],), mode=ForgetMode.ERASE))
                return result

            host.verification.schedule = erase_after_schedule
            assert await host._schedule_verification() == 1
            async with engine.repository.unit_of_work() as uow:
                cursor = await uow.derived_get(scope, "verification_discovery", "scope")
                assert cursor == {"after": None, "parents": []}
                assert await uow.verification_candidates(scope) == ()
            assert await host.verification.run_once("worker") == "idle"

    asyncio.run(run())


@pytest.mark.parametrize("failed_stage", ["extraction_initialization", "discovery"])
def test_failed_stage_preserves_real_verification_and_question_refresh(store, failed_stage):
    from datetime import timedelta

    from test_question_runtime_v7 import ACTOR, register, runtime

    from agent_memory.operations.refresh_host import RefreshHost

    async def run():
        async with store() as (engine, _, scope, clock):
            await seed(engine.repository, scope, pending=1)
            host = host_for(engine, scope, clock)
            assert await host._schedule_verification() == 1
            questions = runtime(engine, scope, clock)
            await register(questions)
            await questions.request("project-a:owner", actor=ACTOR, dedupe_key="refresh")
            clock[0] += timedelta(seconds=2)
            host.questions = questions
            host.refresh = RefreshHost(questions.queue, worker_id="independent-refresh")

            async def fail():
                raise RuntimeError("private failure detail")

            if failed_stage == "extraction_initialization":
                host.queue.initialize = fail
                error_stage = "extraction"
            else:
                host._schedule_verification = fail
                error_stage = "verification_discovery"
            cycle = await host.run_once()
            assert cycle["verification"] == "unknown"
            assert cycle["refresh"]["completed"] == 1
            assert cycle["stage_errors"] == {error_stage: f"memory_host_{error_stage}_failed"}
            result = await questions.read("project-a:owner", actor=ACTOR)
            assert result["availability_status"] == "valid"
            assert result["answer_status"] == "unknown"

    asyncio.run(run())


@pytest.mark.parametrize("cursor_state", ["empty", "live", "erased"])
def test_discovery_cursor_is_known_retained_question_gc_root(store, cursor_state):
    import test_question_gc_v7 as gc

    from agent_memory.derived.question_gc import COLLECTIBLE_KINDS, QuestionRetentionPolicy

    async def run():
        async with store() as (engine, _, scope, clock):
            service, _, _ = await gc.seed(engine, scope, clock)
            host = host_for(engine, scope, clock)
            if cursor_state == "live":
                await seed(engine.repository, scope, pending=1, prefix="cursor-live")
            assert await host._schedule_verification() == (1 if cursor_state == "live" else 0)
            async with engine.repository.unit_of_work() as uow:
                if cursor_state == "erased":
                    await uow.derived_put(
                        scope, "verification_discovery", "scope", {"state": "erased"}
                    )
                before = await uow.derived_get(scope, "verification_discovery", "scope")
            result = await service.collect_garbage(QuestionRetentionPolicy("host-cursor-check"))
            assert result["reason"] != "question_gc_kind_unsupported"
            assert "verification_discovery" not in COLLECTIBLE_KINDS
            assert result["remaining"]["verification_discovery"] == 1
            assert all(kind != "verification_discovery" for kind, _ in result["deleted"])
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "verification_discovery", "scope") == before

    asyncio.run(run())
