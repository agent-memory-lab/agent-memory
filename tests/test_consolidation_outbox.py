"""Transaction, recovery and deletion contracts for generic scheduler handoff."""

import asyncio
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from agent_memory.sqlite_worker_queue import SQLiteWorkerQueue

from agent_memory import ForgetMode, ForgetRequest, MemoryEvent, MemoryScope, build_local_kernel
from agent_memory.domain import IngestResult
from agent_memory.extraction_result import ExtractionDiagnostic, ExtractionRejection
from agent_memory.operations import sqlite_consolidation_outbox as storage
from agent_memory.operations.consolidation_outbox import (
    drain_consolidation_cancellations,
    drain_consolidation_outbox,
)
from agent_memory.sqlite import SQLiteMemoryRepository

SCOPE = MemoryScope("outbox", session_id="one")


async def store(repository, event, *, result=None):
    result = result or IngestResult(event.id, (), (), False, ())
    async with repository.unit_of_work() as uow:
        await uow.append_event(event)
        await uow.save_consolidation_intent(event, result)
    return result


class Scheduler:
    def __init__(self, *, failing=False):
        self.failing = failing
        self.events = []
        self.cancelled = []

    async def enqueue_event(self, event, result):
        if self.failing:
            raise RuntimeError("sensitive source must not enter errors")
        self.events.append((event, result))
        return f"event:{event.id}:consolidate"

    async def cancel_forget(self, request):
        self.cancelled.append(request)
        return 1


def test_intent_commits_with_event_and_rollback_keeps_neither(tmp_path):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "memory.db")
        await repository.initialize()
        event = MemoryEvent(SCOPE, "event", "source", id="one")
        with pytest.raises(RuntimeError, match="rollback"):
            async with repository.unit_of_work() as uow:
                await uow.append_event(event)
                await uow.save_consolidation_intent(
                    event, IngestResult(event.id, (), (), False, ())
                )
                raise RuntimeError("rollback")
        assert (await repository.consolidation_outbox_status())["pending"] == 0
        async with repository.unit_of_work() as uow:
            assert not await uow.events_exist(SCOPE, (event.id,))
        await store(repository, event)
        assert (await repository.consolidation_outbox_status())["pending"] == 1

    asyncio.run(scenario())


def test_existing_outbox_upgrades_without_losing_pending_intents(tmp_path):
    async def scenario():
        path = tmp_path / "memory.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        event = MemoryEvent(SCOPE, "event", "source", id="upgrade")
        await store(repository, event)
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("DROP INDEX consolidation_outbox_retry_idx")
            db.execute("ALTER TABLE consolidation_outbox DROP COLUMN next_attempt_at")
        restarted = SQLiteMemoryRepository(path)
        await restarted.initialize()
        await restarted.initialize()
        delivery = await restarted.claim_consolidation_intent()
        assert delivery.event.id == event.id
        assert delivery.attempts == 1

    asyncio.run(scenario())


def test_failure_retains_intent_and_restart_delivers_without_payload_duplication(
    tmp_path, monkeypatch,
):
    async def scenario():
        clock = [datetime(2026, 10, 12, tzinfo=UTC)]
        monkeypatch.setattr(storage, "utc_now", lambda: clock[0])
        path = tmp_path / "memory.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        event = MemoryEvent(SCOPE, "event", "private source text", id="source")
        await store(repository, event)
        failed = await drain_consolidation_outbox(repository, Scheduler(failing=True))
        assert failed == dict(claimed=1, delivered=0, failed=1, skipped=0)
        with closing(sqlite3.connect(path)) as db, db:
            row = db.execute(
                "SELECT result_json, last_error_code FROM consolidation_outbox"
            ).fetchone()
            assert event.content not in row[0]
            assert row[1] == "RuntimeError"
        restarted = SQLiteMemoryRepository(path)
        await restarted.initialize()
        scheduler = Scheduler()
        assert (await drain_consolidation_outbox(restarted, scheduler))["claimed"] == 0
        clock[0] += timedelta(seconds=1)
        result = await drain_consolidation_outbox(restarted, scheduler)
        assert result["delivered"] == 1
        assert scheduler.events[0][0].content == event.content
        assert await restarted.claim_consolidation_intent() is None
        with closing(sqlite3.connect(path)) as db, db:
            payload = db.execute("SELECT result_json FROM consolidation_outbox").fetchone()[0]
            assert json.loads(payload) == {"claim_ids": []}

    asyncio.run(scenario())


def test_crash_after_enqueue_recovers_expired_lease_with_scheduler_dedup(tmp_path, monkeypatch):
    async def scenario():
        clock = [datetime(2026, 10, 11, tzinfo=UTC)]
        monkeypatch.setattr(storage, "utc_now", lambda: clock[0])
        path = tmp_path / "memory.db"
        repository = SQLiteMemoryRepository(path)
        queue = SQLiteWorkerQueue(tmp_path / "queue.db")
        await repository.initialize()
        await queue.initialize()
        event = MemoryEvent(SCOPE, "event", "source", id="source")
        await store(repository, event)
        leased = await repository.claim_consolidation_intent(lease_seconds=5)
        assert leased.idempotency_key == f"event:{event.id}:consolidate"
        await queue.enqueue_event(leased.event, leased.result)
        restarted = SQLiteMemoryRepository(path)
        await restarted.initialize()
        assert await restarted.claim_consolidation_intent() is None
        clock[0] += timedelta(seconds=6)
        recovered = await restarted.claim_consolidation_intent(lease_seconds=5)
        assert recovered.token != leased.token
        assert not await repository.complete_consolidation_intent(leased)
        assert not await repository.fail_consolidation_intent(leased, "stale")
        await queue.enqueue_event(recovered.event, recovered.result)
        assert await restarted.complete_consolidation_intent(recovered)
        assert (await queue.stats()).pending == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
@pytest.mark.parametrize("all_in_scope", [False, True])
def test_forget_discards_pending_handoff_and_preserves_foreign_scope(tmp_path, mode, all_in_scope):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "memory.db")
        await repository.initialize()
        source = MemoryEvent(SCOPE, "event", "sensitive", id="source")
        foreign = MemoryEvent(replace(SCOPE, session_id="other"), "event", "foreign", id="other")
        await store(repository, source)
        await store(repository, foreign)
        await repository.forget(
            ForgetRequest(SCOPE, () if all_in_scope else (source.id,), all_in_scope, mode)
        )
        assert await repository.claim_consolidation_intent(scope=SCOPE) is None
        scheduler = Scheduler()
        await drain_consolidation_outbox(repository, scheduler)
        assert [event.id for event, _ in scheduler.events] == [foreign.id]

    asyncio.run(scenario())


def test_claim_forget_cancels_pending_result_reference(tmp_path):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "memory.db")
        await repository.initialize()
        event = MemoryEvent(SCOPE, "event", "source", id="source")
        await store(repository, event, result=IngestResult(event.id, ("claim",), (), False, ()))
        await repository.forget(ForgetRequest(SCOPE, ("claim",), mode=ForgetMode.ERASE))
        assert await repository.claim_consolidation_intent() is None

    asyncio.run(scenario())


def test_two_repository_instances_cannot_claim_one_live_lease(tmp_path):
    async def scenario():
        path = tmp_path / "memory.db"
        repositories = [SQLiteMemoryRepository(path) for _ in range(2)]
        await repositories[0].initialize()
        await store(repositories[0], MemoryEvent(SCOPE, "event", "source", id="source"))
        deliveries = await asyncio.gather(*(r.claim_consolidation_intent() for r in repositories))
        assert sum(d is not None for d in deliveries) == 1

    asyncio.run(scenario())


def test_deletion_racing_enqueue_cancels_delayed_queue_task(tmp_path):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "memory.db")
        await repository.initialize()
        event = MemoryEvent(SCOPE, "event", "sensitive", id="source")
        await store(repository, event)
        started, release = asyncio.Event(), asyncio.Event()

        class Delayed(Scheduler):
            async def enqueue_event(self, event, result):
                started.set()
                await release.wait()
                return await super().enqueue_event(event, result)

        scheduler = Delayed()
        running = asyncio.create_task(drain_consolidation_outbox(repository, scheduler))
        await started.wait()
        await repository.forget(ForgetRequest(SCOPE, (event.id,), mode=ForgetMode.ERASE))
        release.set()
        result = await running
        assert result["skipped"] == 1 and result["delivered"] == 0
        assert scheduler.cancelled[0].memory_ids == (event.id,)
        assert await repository.claim_consolidation_intent() is None

    asyncio.run(scenario())


def test_out_of_band_source_archive_is_not_hydrated_for_retry(tmp_path):
    async def scenario():
        path = tmp_path / "memory.db"
        repository = SQLiteMemoryRepository(path)
        await repository.initialize()
        event = MemoryEvent(SCOPE, "event", "sensitive", id="source")
        await store(repository, event)
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("UPDATE events SET archived_at='2026-10-11T00:00:00+00:00'")
        scheduler = Scheduler()
        assert (await drain_consolidation_outbox(repository, scheduler))["claimed"] == 0
        assert scheduler.events == []

    asyncio.run(scenario())


def test_kernel_without_scheduler_creates_no_outbox_intent(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        await kernel.ingest_event(MemoryEvent(SCOPE, "event", "source"))
        assert await kernel._repository.consolidation_outbox_status() == dict(
            pending=0, delivering=0, delivered=0, cancelled=0
        )

    asyncio.run(scenario())


def test_kernel_enqueue_failure_returns_committed_pending_and_duplicate_retries(tmp_path):
    async def scenario():
        scheduler = Scheduler(failing=True)
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=scheduler)
        await kernel.initialize()
        event = MemoryEvent(SCOPE, "event", "source", id="source", idempotency_key="same")
        first = await kernel.ingest_event(event)
        assert not first.duplicate and first.consolidation_status == "pending"
        assert await kernel._repository.consolidation_intent_state(SCOPE, event.id) == "pending"
        scheduler.failing = False
        repeated = await kernel.ingest_event(replace(event, id="ignored-on-duplicate"))
        assert repeated.duplicate and repeated.event_id == event.id
        assert repeated.consolidation_status == "delivered"
        assert [source.id for source, _ in scheduler.events] == [event.id]

    asyncio.run(scenario())


def test_kernel_restart_drains_committed_intent_and_keeps_typed_diagnostic(tmp_path, monkeypatch):
    async def scenario():
        clock = [datetime(2026, 10, 12, tzinfo=UTC)]
        monkeypatch.setattr(storage, "utc_now", lambda: clock[0])
        path = tmp_path / "memory.db"
        kernel = build_local_kernel(path, consolidation_scheduler=Scheduler(failing=True))
        await kernel.initialize()
        first = await kernel.ingest_event(MemoryEvent(SCOPE, "event", "source", id="source"))
        assert first.consolidation_status == "pending"
        assert first.extraction.status == "skipped"
        clock[0] += timedelta(seconds=1)
        scheduler = Scheduler()
        restarted = build_local_kernel(path, consolidation_scheduler=scheduler)
        await restarted.initialize()
        handed_off = scheduler.events[0][1]
        assert handed_off.extraction == first.extraction
        assert isinstance(handed_off.extraction, ExtractionDiagnostic)
        assert await restarted._repository.consolidation_intent_state(SCOPE, first.event_id) == (
            "delivered"
        )

    asyncio.run(scenario())


def test_nested_extraction_diagnostic_round_trips_in_intent(tmp_path):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "memory.db")
        await repository.initialize()
        source = MemoryEvent(SCOPE, "event", "source", id="source")
        nested = ExtractionDiagnostic(
            "rejected",
            candidate_count=1,
            rejections=(ExtractionRejection(0, "missing_confidence"),),
        )
        diagnostic = ExtractionDiagnostic("degraded", components=(nested,))
        result = IngestResult(source.id, (), (), False, (), extraction=diagnostic)
        await store(repository, source, result=result)
        delivery = await repository.claim_consolidation_intent()
        assert delivery.result == result
        assert isinstance(delivery.result.extraction.components[0], ExtractionDiagnostic)
        assert isinstance(
            delivery.result.extraction.components[0].rejections[0], ExtractionRejection
        )

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "failure_method", ["consolidation_delivery_is_current", "complete_consolidation_intent"]
)
def test_validation_or_ack_failure_keeps_retryable_intent(tmp_path, monkeypatch, failure_method):
    async def scenario():
        clock = [datetime(2026, 10, 12, tzinfo=UTC)]
        monkeypatch.setattr(storage, "utc_now", lambda: clock[0])
        repository = SQLiteMemoryRepository(tmp_path / "memory.db")
        await repository.initialize()
        source = MemoryEvent(SCOPE, "event", "source", id="source")
        await store(repository, source)
        original = getattr(repository, failure_method)

        async def failing(delivery):
            raise RuntimeError("temporary database fault")

        monkeypatch.setattr(repository, failure_method, failing)
        queue = SQLiteWorkerQueue(tmp_path / "queue.db")
        await queue.initialize()
        assert (await drain_consolidation_outbox(repository, queue))["failed"] == 1
        assert await repository.consolidation_intent_state(SCOPE, source.id) == "pending"
        monkeypatch.setattr(repository, failure_method, original)
        clock[0] += timedelta(seconds=1)
        assert (await drain_consolidation_outbox(repository, queue))["delivered"] == 1
        assert (await queue.stats()).pending == 1

    asyncio.run(scenario())


def test_exact_state_tracks_lease_delivery_and_deletion_without_other_scope_counts(tmp_path):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "memory.db")
        await repository.initialize()
        event = MemoryEvent(SCOPE, "event", "source", id="source")
        await store(repository, event)
        assert await repository.consolidation_intent_state(SCOPE, event.id) == "pending"
        assert (
            await repository.consolidation_intent_state(
                replace(SCOPE, session_id="other"), event.id
            )
            is None
        )
        delivery = await repository.claim_consolidation_intent(scope=SCOPE, event_id=event.id)
        assert await repository.consolidation_intent_state(SCOPE, event.id) == "leased"
        assert await repository.complete_consolidation_intent(delivery)
        assert await repository.consolidation_intent_state(SCOPE, event.id) == "delivered"
        await repository.forget(ForgetRequest(SCOPE, (event.id,), mode=ForgetMode.ARCHIVE))
        assert await repository.consolidation_intent_state(SCOPE, event.id) is None

    asyncio.run(scenario())


def test_outbox_save_failure_rolls_back_kernel_source_and_claims(tmp_path, monkeypatch):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=Scheduler())
        await kernel.initialize()

        def fail(connection, event, result):
            raise RuntimeError("intent persistence failed")

        monkeypatch.setattr(storage, "save", fail)
        event = MemoryEvent(SCOPE, "event", "source", id="source")
        with pytest.raises(RuntimeError, match="intent persistence"):
            await kernel.ingest_event(event)
        async with kernel._repository.unit_of_work() as uow:
            assert not await uow.events_exist(SCOPE, (event.id,))
        assert (await kernel._repository.consolidation_outbox_status())["pending"] == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("forget_target", ["source", "claim"])
def test_kernel_reports_cancelled_after_deletion_races_handoff(tmp_path, forget_target):
    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()

        class Delayed(Scheduler):
            async def enqueue_event(self, event, result):
                started.set()
                await release.wait()
                return await super().enqueue_event(event, result)

        scheduler = Delayed()
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=scheduler)
        await kernel.initialize()
        event = MemoryEvent(
            SCOPE,
            "event",
            "source",
            id="source",
            metadata={"claims": [{"key": "preference", "value": "tea", "text": "prefers tea"}]},
        )
        ingestion = asyncio.create_task(kernel.ingest_event(event))
        await started.wait()
        target = event.id if forget_target == "source" else (await kernel.get_state(SCOPE))[0].id
        await kernel.forget(ForgetRequest(SCOPE, (target,), mode=ForgetMode.ERASE))
        release.set()
        result = await ingestion
        assert result.consolidation_status == "cancelled"
        assert await kernel._repository.consolidation_intent_state(SCOPE, event.id) == (
            None if forget_target == "source" else "cancelled"
        )
        assert len(scheduler.cancelled) >= 2

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_duplicate_after_claim_forget_does_not_recreate_cancelled_handoff(tmp_path, mode):
    async def scenario():
        scheduler = Scheduler(failing=True)
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=scheduler)
        await kernel.initialize()
        event = MemoryEvent(
            SCOPE,
            "event",
            "source",
            id="source",
            idempotency_key="same",
            metadata={"claims": [{"key": "preference", "value": "tea", "text": "prefers tea"}]},
        )
        first = await kernel.ingest_event(event)
        assert first.claim_ids and first.consolidation_status == "pending"
        await kernel.forget(ForgetRequest(SCOPE, first.claim_ids, mode=mode))
        assert await kernel._repository.consolidation_intent_state(SCOPE, event.id) == "cancelled"
        scheduler.failing = False
        duplicate = await kernel.ingest_event(event)
        assert duplicate.duplicate and duplicate.consolidation_status == "cancelled"
        assert scheduler.events == []
        with closing(sqlite3.connect(tmp_path / "memory.db")) as db:
            assert db.execute("SELECT result_json FROM consolidation_outbox").fetchone()[0] == "{}"

    asyncio.run(scenario())


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_broader_claim_forget_cancels_narrow_source_handoff_without_cross_scope_effect(
    tmp_path, all_in_scope
):
    async def scenario():
        scheduler = Scheduler(failing=True)
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=scheduler)
        await kernel.initialize()
        source_scope = replace(SCOPE, user_id="user")
        source = MemoryEvent(
            source_scope,
            "event",
            "source",
            id="source",
            idempotency_key="same",
            metadata={
                "claims": [
                    {"key": "preference", "value": "tea", "text": "prefers tea", "scope": "user"}
                ]
            },
        )
        first = await kernel.ingest_event(source)
        claim = (await kernel.get_state(source_scope))[0]
        assert claim.id == first.claim_ids[0] and claim.scope != source_scope
        unrelated = MemoryEvent(
            replace(source_scope, user_id="other"), "event", "other", id="other"
        )
        await kernel.ingest_event(unrelated)
        await kernel.forget(
            ForgetRequest(
                claim.scope, () if all_in_scope else (claim.id,), all_in_scope, ForgetMode.ERASE
            )
        )
        assert await kernel._repository.consolidation_intent_state(source_scope, source.id) == (
            "cancelled"
        )
        unrelated_state = await kernel._repository.consolidation_intent_state(
            unrelated.scope, unrelated.id
        )
        assert unrelated_state == "pending"
        scheduler.failing = False
        duplicate = await kernel.ingest_event(source)
        assert duplicate.consolidation_status == "cancelled" and scheduler.events == []

    asyncio.run(scenario())


@pytest.mark.parametrize("forget_target", ["source", "claim"])
def test_enqueue_commit_then_raise_after_forget_still_cancels_late_job(tmp_path, forget_target):
    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()

        class UncertainQueue(Scheduler):
            async def enqueue_event(self, event, result):
                started.set()
                await release.wait()
                await super().enqueue_event(event, result)
                raise RuntimeError("response lost after queue commit")

        scheduler = UncertainQueue()
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=scheduler)
        await kernel.initialize()
        source = MemoryEvent(
            SCOPE,
            "event",
            "source",
            id="source",
            metadata={"claims": [{"key": "preference", "value": "tea", "text": "prefers tea"}]},
        )
        ingestion = asyncio.create_task(kernel.ingest_event(source))
        await started.wait()
        target = source.id if forget_target == "source" else (await kernel.get_state(SCOPE))[0].id
        await kernel.forget(ForgetRequest(SCOPE, (target,), mode=ForgetMode.ERASE))
        release.set()
        result = await ingestion
        assert result.consolidation_status == "cancelled"
        assert len(scheduler.events) == 1
        assert len(scheduler.cancelled) >= 2
        assert scheduler.cancelled[-1].memory_ids == (source.id,)

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
@pytest.mark.parametrize("all_in_scope", [False, True])
def test_delivered_wider_claim_forget_cancels_real_narrow_scope_queue_task(
    tmp_path, mode, all_in_scope
):
    async def scenario():
        queue = SQLiteWorkerQueue(tmp_path / "queue.db")
        await queue.initialize()
        kernel = build_local_kernel(tmp_path / "memory.db", consolidation_scheduler=queue)
        await kernel.initialize()
        source_scope = replace(SCOPE, user_id="user")
        source = MemoryEvent(
            source_scope,
            "event",
            "source",
            id="source",
            idempotency_key="same",
            metadata={
                "claims": [
                    {"key": "preference", "value": "tea", "text": "prefers tea", "scope": "user"}
                ]
            },
        )
        first = await kernel.ingest_event(source)
        assert first.consolidation_status == "delivered"
        claim = (await kernel.get_state(source_scope))[0]
        await kernel.ingest_event(
            MemoryEvent(replace(source_scope, user_id="other"), "event", "other", id="other")
        )
        assert (await queue.stats()).pending == 2
        await kernel.forget(
            ForgetRequest(claim.scope, () if all_in_scope else (claim.id,), all_in_scope, mode)
        )
        stats = await queue.stats()
        assert stats.pending == 1 and stats.cancelled == 1
        assert await kernel._repository.consolidation_intent_state(source_scope, source.id) == (
            "cancelled"
        )
        assert not await kernel._repository.pending_consolidation_cancellations()
        repeated = await kernel.ingest_event(source)
        assert repeated.duplicate and repeated.consolidation_status == "cancelled"
        assert (await queue.stats()).pending == 1

    asyncio.run(scenario())


def test_failed_queue_cancellation_restarts_without_reenqueuing_claim_source(tmp_path):
    async def scenario():
        queue = SQLiteWorkerQueue(tmp_path / "queue.db")
        await queue.initialize()

        class OfflineCancellation:
            async def enqueue_event(self, event, result):
                return await queue.enqueue_event(event, result)

            async def cancel_forget(self, request):
                raise RuntimeError("queue cancellation unavailable")

        path = tmp_path / "memory.db"
        kernel = build_local_kernel(path, consolidation_scheduler=OfflineCancellation())
        await kernel.initialize()
        source = MemoryEvent(
            replace(SCOPE, user_id="user"),
            "event",
            "source",
            id="source",
            metadata={
                "claims": [
                    {"key": "preference", "value": "tea", "text": "prefers tea", "scope": "user"}
                ]
            },
        )
        first = await kernel.ingest_event(source)
        claim = (await kernel.get_state(source.scope))[0]
        await kernel.forget(ForgetRequest(claim.scope, (claim.id,), mode=ForgetMode.ERASE))
        assert (await queue.stats()).pending == 1
        pending = await kernel._repository.pending_consolidation_cancellations()
        assert len(pending) == 1 and pending[0].scope == source.scope
        restarted = build_local_kernel(path, consolidation_scheduler=queue)
        await restarted.initialize()
        assert (await queue.stats()).pending == 0 and (await queue.stats()).cancelled == 1
        assert await restarted._repository.consolidation_intent_state(
            source.scope, first.event_id
        ) == ("cancelled")

    asyncio.run(scenario())


def test_cancellation_ack_generation_preserves_new_late_enqueue_intent(tmp_path):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "memory.db")
        await repository.initialize()
        source = MemoryEvent(SCOPE, "event", "private source", id="source")
        await store(repository, source, result=IngestResult(source.id, ("claim",), (), False, ()))
        await repository.forget(ForgetRequest(SCOPE, ("claim",), mode=ForgetMode.ERASE))
        old = (await repository.pending_consolidation_cancellations())[0]
        newer = await repository.request_consolidation_cancellation(SCOPE, source.id)
        assert newer.generation > old.generation
        assert not await repository.ack_consolidation_cancellation(old)
        assert await repository.pending_consolidation_cancellations() == (newer,)
        assert await repository.ack_consolidation_cancellation(newer)
        assert not await repository.pending_consolidation_cancellations()

    asyncio.run(scenario())


def test_cancellation_drain_is_bounded_exact_scope_and_does_not_read_body(tmp_path):
    async def scenario():
        repository = SQLiteMemoryRepository(tmp_path / "memory.db")
        await repository.initialize()
        other_scope = replace(SCOPE, session_id="other")
        for index, scope in enumerate((SCOPE, other_scope)):
            source = MemoryEvent(scope, "event", "private source text", id=f"source-{index}")
            await store(
                repository, source, result=IngestResult(source.id, ("claim",), (), False, ())
            )
            await repository.forget(ForgetRequest(scope, ("claim",), mode=ForgetMode.ERASE))
        assert len(await repository.pending_consolidation_cancellations(limit=1)) == 1
        scheduler = Scheduler()
        result = await drain_consolidation_cancellations(repository, scheduler, scope=SCOPE)
        assert result == dict(claimed=1, cancelled=1, failed=0, skipped=0)
        assert scheduler.events == []
        pending = await repository.pending_consolidation_cancellations()
        assert len(pending) == 1 and pending[0].scope == other_scope
        assert not hasattr(pending[0], "content")

    asyncio.run(scenario())
