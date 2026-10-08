"""Prove continuous unchanged intervals; never interpolate across a mutation gap."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_history import archive_spy, checkpoint, historical, refresh
from test_derived_history import configured as points
from test_derived_observations import build
from test_durable_purge import envelope, source_id

from agent_memory.derived import DerivedError, ObservationService, ProcessingGrant
from agent_memory.derived.model import HISTORY_INTERVAL
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.facet_refresh import FacetRefreshQueue

store = base.store


async def configured(engine, kernel, scope, clock, *, inputs=1):
    _, _, capture, authority, query, spec, legacy = await points(
        engine, kernel, scope, clock, inputs=inputs
    )
    service = ObservationService(
        engine.repository,
        scope,
        base.POLICY,
        clock=lambda: clock[0],
        authority_id=authority.id,
        authority_min_version=1,
        history_mode=HISTORY_INTERVAL,
    )
    spec = replace(spec, history_mode=HISTORY_INTERVAL)
    await service.register(spec, expected_generation=3)
    return service, FacetRefreshQueue(service), capture, authority, query, spec, legacy


async def ledger(repository, scope):
    async with repository.unit_of_work() as uow:
        return await uow.derived_get(scope, "history_interval", "language")


async def decision(repository, scope, *, predicate="locale", action="REJECT"):
    async with repository.unit_of_work() as uow:
        row = next(
            r
            for r in await uow.list_admission_records(scope)
            if r["payload"]["draft"]["predicate"] == predicate
        )
        changed = deepcopy(row["payload"])
        changed["action"] = action
        await uow.save_admission_record(
            scope, row["id"], row["event_id"], row["slot_key"], changed, row["version"]
        )


def test_open_and_checkpoint_sealed_intervals_are_half_open_and_bitemporal(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            for valid, state in (
                (base.at(1), "ready"),
                (base.at(1) - timedelta(seconds=1), "empty"),
            ):
                view = await historical(service, start + timedelta(seconds=5), valid)
                assert view["state"] == state
                assert view["coverage"]["kind"] == "interval"
                assert view["coverage"]["known_from"] == first
                assert view["coverage"]["known_to"] is None
            await refresh(service, queue, "unchanged")
            second = await checkpoint(service)
            spans = (await ledger(engine.repository, scope))["spans"]
            assert spans[0]["state"] == "sealed" and spans[0]["known_to"] == second
            assert spans[1]["state"] == "open"
            assert (await historical(service, start + timedelta(seconds=5), base.at(1)))[
                "coverage"
            ]["known_to"] == second
            assert (await historical(service, second, base.at(1)))["state"] == "ready"
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, start - timedelta(microseconds=1), base.at(1))

    asyncio.run(run())


def test_empty_interval_closes_on_new_member_and_gap_does_not_fake_absence(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, capture, _, _, _, _ = await configured(
                engine, kernel, scope, clock, inputs=0
            )
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            changed_at = clock[0]
            _, session, client, _, _, worker, _, _ = capture
            await client.durable_append(envelope(scope, "1", clock), session, 1)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            await service.grant(ProcessingGrant(source_id(scope, "1"), ("alice",)))
            row = await ledger(engine.repository, scope)
            assert row["spans"][0]["known_to"] == changed_at.isoformat()
            assert (await historical(service, start + timedelta(seconds=5), clock[0]))[
                "state"
            ] == "empty"
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, changed_at, clock[0])
            await refresh(service, queue, "member")
            clock[0] += timedelta(seconds=1)
            assert (await historical(service, clock[0], clock[0]))["state"] == "ready"
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, changed_at + timedelta(microseconds=1), clock[0])

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["candidate", "query", "definition", "interpretation", "document"])
def test_first_semantic_mutation_closes_original_interval_and_later_changes_never_extend_it(
    store, kind
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, query, spec, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            boundary = clock[0]
            if kind == "candidate":
                await decision(engine.repository, scope)
            elif kind == "query":
                await service.register_query(replace(query, version="2"), expected_generation=1)
            elif kind == "definition":
                await service.register(replace(spec, version="2"), expected_generation=4)
            elif kind == "interpretation":
                from agent_memory.operations.source_revisions import withdraw_revision

                async with engine.repository.unit_of_work() as uow:
                    source = await uow.get_source_event(scope, source_id(scope, "1"))
                    await withdraw_revision(uow, source, "withdraw")
            else:
                async with engine.repository.unit_of_work() as uow:
                    source = await uow.get_source_event(scope, source_id(scope, "1"))
                    key = source.metadata["_retention"]["document_id"]
                    head = await uow.retention_head_get(scope, "document", key)
                    await uow.retention_head_put(
                        scope, "document", key, head["payload"], head["generation"]
                    )
            row = await ledger(engine.repository, scope)
            assert row["spans"][0]["known_to"] == boundary.isoformat()
            clock[0] += timedelta(seconds=10)
            await service.register_query(
                replace(query, version="3"), expected_generation=2 if kind == "query" else 1
            )
            assert (await ledger(engine.repository, scope))["spans"][0][
                "known_to"
            ] == boundary.isoformat()
            old = await historical(service, start + timedelta(seconds=5), base.at(1))
            assert old["body"]["blocks"][0]["value"] == "zh-CN"
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, boundary, base.at(1))

    asyncio.run(run())


def test_policy_migration_closes_old_contract_without_reinterpreting_its_interval(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, query, spec, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            from agent_memory.consolidation.admission import AdmissionPolicy

            policy = AdmissionPolicy([base.PredicateSpec("locale", allow_self_report=False)])
            policy.version = "policy/2"
            newer = ObservationService(
                engine.repository,
                scope,
                policy,
                clock=lambda: clock[0],
                authority_id="local-host",
                authority_min_version=1,
                history_mode=HISTORY_INTERVAL,
            )
            await newer.register_query(replace(query, version="2"), expected_generation=1)
            await newer.register(replace(spec, version="2"), expected_generation=4)
            view = await historical(newer, start + timedelta(seconds=5), base.at(1))
            assert view["body"]["blocks"][0]["value"] == "zh-CN"
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(newer, clock[0], base.at(1))
            async with engine.repository.unit_of_work() as uow:
                old = await uow.derived_get(scope, "revision", view["revision_id"])
                assert old["history"]["policy"] == base.POLICY.config_payload()

    asyncio.run(run())


def test_unrelated_slot_and_permission_updates_do_not_close_semantic_interval(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            await decision(engine.repository, scope, predicate="city")
            await service.set_authority(authority, expected_version=1)
            await service.grant(
                ProcessingGrant(source_id(scope, "1"), ("alice",)), expected_version=2
            )
            assert (await ledger(engine.repository, scope))["spans"][0]["state"] == "open"
            assert (await historical(service, start + timedelta(seconds=5), base.at(1)))[
                "state"
            ] == "ready"

    asyncio.run(run())


def test_valid_time_boundary_and_running_refresh_do_not_change_historical_knowledge(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            end = clock[0] + timedelta(seconds=10)
            async with engine.repository.unit_of_work() as uow:
                atom = next(
                    r
                    for r in await uow.list_admission_records(scope)
                    if r["payload"]["draft"]["predicate"] == "locale"
                )
                payload = deepcopy(atom["payload"])
                payload["valid_to"] = end.isoformat()
                payload["draft"]["valid_to"] = end.isoformat()
                await uow.save_admission_record(
                    scope, atom["id"], atom["event_id"], atom["slot_key"], payload, atom["version"]
                )
            await build(queue)
            clock[0] = end
            lease = await queue.claim("transition", lease_seconds=60)
            assert lease and lease.task.payload["unit"]["time_generation"] > 0
            assert (await ledger(engine.repository, scope))["spans"][0]["state"] == "open"
            assert (await historical(service, end, base.at(1)))["state"] == "ready"
            assert (await historical(service, end, end))["state"] == "empty"
            await service.apply(lease.task)

    asyncio.run(run())


def test_backend_must_declare_transactional_coverage_hooks_before_enabling_interval(
    store, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            cls = type(engine.repository.unit_of_work())
            monkeypatch.delattr(cls, "derived_coverage_contract")
            with pytest.raises(DerivedError, match="derived_history_coverage_backend_unsupported"):
                await service.call("capabilities", {}, MCPRequestContext(scope, actor="alice"))

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["grant", "authority", "expiry", "floor"])
def test_current_security_blocks_interval_before_archived_body(store, monkeypatch, problem):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _, _ = await configured(
                engine, kernel, scope, clock, inputs=2
            )
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            if problem == "grant":
                await service.grant(
                    ProcessingGrant(source_id(scope, "2"), ("alice",), revoked=True),
                    expected_version=2,
                )
            elif problem == "authority":
                await service.set_authority(replace(authority, revoked=True), expected_version=1)
            elif problem == "expiry":
                clock[0] = authority.expires_at
            else:
                service.authority_min_version = 2
            calls = archive_spy(monkeypatch, engine.repository)
            with pytest.raises(DerivedError):
                await historical(service, start + timedelta(seconds=5), base.at(1))
            assert not calls

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["corrupt", "untracked_barrier", "missing"])
def test_incomplete_interval_proof_cannot_interpolate_or_fallback_current(
    store, monkeypatch, problem
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(scope, "history_interval", "language")
                if problem == "corrupt":
                    row["spans"][0]["known_from"] = base.at(1).isoformat()
                    await uow.derived_put(scope, "history_interval", "language", row)
                elif problem == "missing":
                    await uow.derived_put(scope, "history_interval", "language", None)
                else:
                    key = row["slots"][0]
                    old = await uow.derived_get(scope, "barrier", key)
                    await uow.derived_put(
                        scope, "barrier", key, {"generation": old["generation"] + 1}
                    )
            calls = archive_spy(monkeypatch, engine.repository)
            with pytest.raises(DerivedError):
                await historical(service, start + timedelta(seconds=5), base.at(1))
            assert not calls

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["document", "candidate"])
def test_untracked_change_never_becomes_sealed_coverage_after_next_publication(
    store, monkeypatch, kind
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            if kind == "document":
                async with engine.repository.unit_of_work() as uow:
                    source = await uow.get_source_event(scope, source_id(scope, "1"))
                    key = source.metadata["_retention"]["document_id"]
                    head = await uow.retention_head_get(scope, "document", key)
                    if hasattr(engine.repository, "pool"):
                        from agent_memory_postgres import retention

                        await retention.head_put(
                            uow.connection,
                            scope,
                            "document",
                            key,
                            head["payload"],
                            head["generation"],
                        )
                    else:
                        from agent_memory.operations import sqlite_retention

                        sqlite_retention.head_put(
                            uow.connection,
                            scope,
                            "document",
                            key,
                            head["payload"],
                            head["generation"],
                        )
            else:
                import agent_memory.derived.subscriptions as writer

                original = writer.close_coverage

                async def old_writer(*args, **kwargs):
                    pass

                with monkeypatch.context() as patch:
                    patch.setattr(writer, "close_coverage", old_writer)
                    await decision(engine.repository, scope)
                assert writer.close_coverage is original
            with monkeypatch.context() as patch:
                calls = archive_spy(patch, engine.repository)
                with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                    await historical(service, start + timedelta(seconds=5), base.at(1))
                assert not calls
            clock[0] += timedelta(seconds=1)
            await refresh(service, queue, "new")
            assert (await ledger(engine.repository, scope))["spans"][0]["state"] == "uncertain"
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, start + timedelta(seconds=5), base.at(1))
            assert (await historical(service, start, base.at(1)))["state"] == "ready"

    asyncio.run(run())


def test_backdated_clock_invalidates_interval_proofs_until_new_checkpoint(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            await refresh(service, queue, "second")
            clock[0] = start + timedelta(seconds=5)
            await decision(engine.repository, scope)
            assert all(
                s["state"] == "uncertain" for s in (await ledger(engine.repository, scope))["spans"]
            )
            clock[0] = start + timedelta(seconds=20)
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, start + timedelta(seconds=2), base.at(1))
            assert (await historical(service, start, base.at(1)))["state"] == "ready"
            await refresh(service, queue, "recovered")
            clock[0] += timedelta(seconds=1)
            assert (await historical(service, clock[0], base.at(1)))["state"] == "empty"

    asyncio.run(run())


def test_reader_behind_writer_frontier_rejects_sealed_proof_before_archive(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            await decision(engine.repository, scope)
            clock[0] = start + timedelta(seconds=5)
            calls = archive_spy(monkeypatch, engine.repository)
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, start + timedelta(seconds=2), base.at(1))
            assert not calls

    asyncio.run(run())


def test_publication_before_control_frontier_rolls_back_every_new_certificate(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, query, _, _ = await configured(
                engine, kernel, scope, clock, inputs=0
            )
            await build(queue)
            start = clock[0]
            async with engine.repository.unit_of_work() as uow:
                original_head = await uow.derived_get(scope, "head", "language")
            clock[0] += timedelta(seconds=10)
            await service.register_query(replace(query, version="2"), expected_generation=1)
            clock[0] = start + timedelta(seconds=5)
            receipt = await queue.request("language", dedupe_key="backdated", force=True)
            lease = await queue.claim("backdated", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_history_clock_unordered"):
                await service.apply(lease.task)
            assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "history_point")) == 1
                assert len(await uow.derived_records(scope, "revision")) == 1
                assert await uow.derived_get(scope, "head", "language") == original_head

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["candidate", "erase"])
def test_publication_serializes_with_independent_semantic_or_delete_connection(
    store, monkeypatch, mutation
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            pg = hasattr(engine.repository, "pool")
            if pg:
                from agent_memory_postgres.repository import PostgresMemoryRepository

                other = PostgresMemoryRepository.from_dsn(
                    engine.repository.pool.conninfo, max_size=2
                )
            else:
                from agent_memory.sqlite import SQLiteMemoryRepository

                other = SQLiteMemoryRepository(engine.repository._path)
            await other.initialize()
            lease = await queue.claim("publication", lease_seconds=60)
            entered, release = asyncio.Event(), asyncio.Event()
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_put

            async def paused(self, *args):
                await original(self, *args)
                if self._repository is engine.repository and args[1] == "history_interval":
                    entered.set()
                    await release.wait()

            async def change():
                if mutation == "candidate":
                    await decision(other, scope)
                else:
                    await other.forget(
                        ForgetRequest(scope, (source_id(scope, "1"),), mode=ForgetMode.ERASE)
                    )

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "derived_put", paused)
                    publication = asyncio.create_task(service.apply(lease.task))
                    await asyncio.wait_for(entered.wait(), timeout=10)
                    published_at = clock[0]
                    clock[0] += timedelta(seconds=2)
                    writer = asyncio.create_task(
                        change() if pg else asyncio.to_thread(lambda: asyncio.run(change()))
                    )
                    await asyncio.sleep(0.05)
                    assert not writer.done()
                    release.set()
                    await asyncio.wait_for(publication, timeout=10)
                    await asyncio.wait_for(writer, timeout=10)
                if mutation == "candidate":
                    span = (await ledger(other, scope))["spans"][0]
                    assert span["state"] == "sealed" and span["known_to"] == clock[0].isoformat()
                    assert (
                        await historical(service, published_at + timedelta(seconds=1), base.at(1))
                    )["state"] == "ready"
                else:
                    assert (await ledger(other, scope))["state"] == "erased"
                    with pytest.raises(DerivedError):
                        await historical(service, published_at + timedelta(seconds=1), base.at(1))
            finally:
                release.set()
                if pg:
                    await other.close()

    asyncio.run(run())


def test_deleting_later_member_also_scrubs_earlier_empty_interval(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, capture, _, _, _, _ = await configured(
                engine, kernel, scope, clock, inputs=0
            )
            await build(queue)
            clock[0] += timedelta(seconds=10)
            _, session, client, _, _, worker, _, _ = capture
            await client.durable_append(envelope(scope, "1", clock), session, 1)
            assert await worker.run_once()
            await kernel.forget(
                ForgetRequest(scope, (source_id(scope, "1"),), mode=ForgetMode.ERASE)
            )
            assert (await ledger(engine.repository, scope))["state"] == "erased"
            assert (await service.history_points("language", actor="alice"))["points"] == []

    asyncio.run(run())


def test_point_upgrade_never_backfills_old_point_gaps(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            old, queue, _, authority, _, spec, _ = await points(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            service = ObservationService(
                engine.repository,
                scope,
                base.POLICY,
                clock=lambda: clock[0],
                authority_id=authority.id,
                authority_min_version=1,
                history_mode=HISTORY_INTERVAL,
            )
            await service.register(
                replace(spec, history_mode=HISTORY_INTERVAL), expected_generation=3
            )
            await refresh(service, FacetRefreshQueue(service), "upgrade")
            assert (await historical(service, start, base.at(1)))["state"] == "ready"
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, start + timedelta(seconds=5), base.at(1))
            with pytest.raises(DerivedError, match="derived_history_configuration_mismatch"):
                await old.read("language", actor="alice")

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["candidate", "query", "publication"])
@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_real_sigkill_keeps_coverage_with_semantic_write_and_publication_atomic(
    store, tmp_path, kind, boundary
):
    from test_durable_process_recovery import kill_at_boundary

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            if kind != "publication":
                await build(queue)
                start = clock[0]
                clock[0] += timedelta(seconds=10)
            await kill_at_boundary(
                engine,
                scope,
                clock,
                tmp_path,
                "interval_" + boundary
                if kind == "publication"
                else "coverage_" + kind + "_" + boundary,
            )
            row = await ledger(engine.repository, scope)
            committed = boundary == "after_commit"
            if kind == "publication":
                async with engine.repository.unit_of_work() as uow:
                    history = await uow.derived_records(scope, "history_point")
                    revisions = await uow.derived_records(scope, "revision")
                    head = await uow.derived_get(scope, "head", "language")
                    assert bool(row) == bool(history) == bool(revisions) == bool(head) == committed
                clock[0] += timedelta(seconds=6)
                if not committed:
                    await refresh(service, queue, "recover")
                assert (
                    await historical(
                        service,
                        clock[0] - timedelta(seconds=1) if committed else await checkpoint(service),
                        base.at(1),
                    )
                )["state"] == "ready"
            else:
                assert row["spans"][0]["state"] == ("sealed" if committed else "open")
                async with engine.repository.unit_of_work() as uow:
                    if kind == "query":
                        query = await uow.derived_get(scope, "query", "language-inputs")
                        assert query["generation"] == (2 if committed else 1)
                    else:
                        atom = next(
                            r
                            for r in await uow.list_admission_records(scope)
                            if r["payload"]["draft"]["predicate"] == "locale"
                        )
                        assert atom["payload"]["action"] == ("REJECT" if committed else "ACCEPT")
                assert (await historical(service, start + timedelta(seconds=5), base.at(1)))[
                    "state"
                ] == "ready"
                if committed:
                    with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                        await historical(service, clock[0], base.at(1))

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["object", "scope"])
def test_live_and_real_backup_replay_erase_all_interval_certificates(store, tmp_path, mode):
    from test_purge_restore import backup_copy, replay, restorer

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                clone = ObservationService(
                    backup,
                    scope,
                    base.POLICY,
                    clock=lambda: clock[0],
                    authority_id="local-host",
                    authority_min_version=1,
                    history_mode=HISTORY_INTERVAL,
                )
                assert (await historical(clone, start + timedelta(seconds=5), base.at(1)))[
                    "state"
                ] == "ready"
                await kernel.forget(
                    ForgetRequest(
                        scope,
                        (source_id(scope, "1"),) if mode == "object" else (),
                        all_in_scope=mode == "scope",
                        mode=ForgetMode.ERASE,
                    )
                )
                snapshot = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), snapshot)
                for repository in (engine.repository, backup):
                    assert await ledger(repository, scope) == {
                        "id": "language",
                        "facet_id": "language",
                        "state": "erased",
                    }
                    async with repository.unit_of_work() as uow:
                        assert all(
                            "history" not in r["payload"]
                            for r in await uow.derived_records(scope, "revision")
                        )
                with pytest.raises(DerivedError):
                    await historical(clone, start + timedelta(seconds=5), base.at(1))

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
@pytest.mark.parametrize("change", ["revoke", "semantic"])
def test_readonly_sdk_final_delivery_preserves_interval_times_and_rechecks_state(
    store, monkeypatch, transport, change
):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            start = clock[0]
            clock[0] += timedelta(seconds=10)
            known_at, valid_at = (start + timedelta(seconds=5)).isoformat(), base.at(1).isoformat()
            original, calls = service.read, []

            async def intervening(*args, **kwargs):
                calls.append(kwargs)
                value = await original(*args, **kwargs)
                if len(calls) == 1:
                    if change == "revoke":
                        await service.set_authority(
                            replace(authority, revoked=True), expected_version=1
                        )
                    else:
                        await decision(engine.repository, scope)
                return value

            monkeypatch.setattr(service, "read", intervening)
            context = MCPRequestContext(scope, actor="alice")

            async def exercise(client):
                assert (await client.derived_capabilities())["historical_mode"] == HISTORY_INTERVAL
                assert (await client.derived_history_points("language"))["points"][0]["coverage"][
                    "state"
                ] == "open"
                if change == "revoke":
                    with pytest.raises(
                        sdk.MemoryClientError, match="derived_authority_unavailable"
                    ):
                        await client.derived_context(
                            "language", known_at=known_at, valid_at=valid_at
                        )
                else:
                    result = await client.derived_context(
                        "language", known_at=known_at, valid_at=valid_at
                    )
                    assert result["observations"][0]["body"]["blocks"][0]["value"] == "zh-CN"
                    assert result["observations"][0]["coverage"]["known_to"] == clock[0].isoformat()
                assert len(calls) == 2 and all(
                    c["known_at"] == known_at and c["valid_at"] == valid_at for c in calls
                )

            if transport == "embedded":
                await exercise(sdk.EmbeddedMemoryClient(kernel, context, derived=service))
            else:
                mcp = pytest.importorskip("agent_memory_mcp")
                server = mcp.create_server(
                    kernel, mcp.StaticIdentityResolver(context), derived=service
                )
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())
