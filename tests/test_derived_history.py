"""Certified knowledge-time points; independent valid time and current security."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta, timezone

import pytest
import test_atom_admission as base
from test_derived_controls import configured as controls
from test_derived_observations import build
from test_durable_purge import envelope, source_id

from agent_memory.derived import (
    DerivedError,
    FacetDefinition,
    HistoricalQuery,
    ObservationService,
    ProcessingGrant,
)
from agent_memory.derived.history import MODE
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.facet_refresh import FacetRefreshQueue

store = base.store


async def configured(engine, kernel, scope, clock, *, inputs=1):
    _, _, capture, authority, query, legacy = await controls(
        engine, kernel, scope, clock, inputs=inputs
    )
    service = ObservationService(
        engine.repository,
        scope,
        base.POLICY,
        clock=lambda: clock[0],
        authority_id=authority.id,
        authority_min_version=1,
        history_mode=MODE,
    )
    spec = FacetDefinition(
        "language", "alice", query_id=query.id, authority_id=authority.id, history_mode=MODE
    )
    await service.register(spec, expected_generation=2)
    return service, FacetRefreshQueue(service), capture, authority, query, spec, legacy


async def checkpoint(service):
    points = (await service.history_points("language", actor="alice"))["points"]
    return points[-1]["known_at"]


async def historical(service, known_at, valid_at, **kwargs):
    return await service.read(
        "language", actor="alice", known_at=known_at, valid_at=valid_at, **kwargs
    )


async def refresh(service, queue, key):
    receipt = await queue.request("language", dedupe_key=key, force=True)
    lease = await queue.claim("history", lease_seconds=60)
    assert lease
    await service.apply(lease.task)
    assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
    return receipt


def archive_spy(monkeypatch, repository):
    calls, cls = [], type(repository.unit_of_work())
    original = cls.derived_get

    async def spy(self, scope, kind, key):
        if kind == "revision":
            calls.append(key)
        return await original(self, scope, kind, key)

    monkeypatch.setattr(cls, "derived_get", spy)
    return calls


def test_late_arrival_has_two_independent_times_and_no_gap_interpolation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, capture, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            clock[0] += timedelta(minutes=20)
            _, session, client, _, generator, worker, _, _ = capture
            generator.values = (("locale", "en-US"),)
            original = generator.generate_atoms

            async def late(event):
                return [
                    {**r, "valid_from": (base.at(1) + timedelta(minutes=5)).isoformat()}
                    for r in await original(event)
                ]

            generator.generate_atoms = late
            await client.durable_append(envelope(scope, "2", clock), session, 2)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            await service.grant(ProcessingGrant(source_id(scope, "2"), ("alice",)))
            await refresh(service, queue, "late")
            second = await checkpoint(service)
            valid = base.at(1) + timedelta(minutes=10)
            old = await historical(service, first, valid)
            new = await historical(service, second, valid)
            earlier = await historical(service, second, base.at(1))
            assert [v["body"]["blocks"][0]["value"] for v in (old, new, earlier)] == [
                "zh-CN",
                "en-US",
                "zh-CN",
            ]
            assert old["coverage"] == dict(known_at=first, kind="point", query_complete=True)
            equivalent = base.at(1) + timedelta(seconds=1)
            assert HistoricalQuery.parse(first, valid).known_at == equivalent
            offset = equivalent.astimezone(timezone(timedelta(hours=8)))
            assert (await historical(service, offset, valid))["known_at"] == first
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, base.at(1) + timedelta(minutes=1), valid)

    asyncio.run(run())


def test_empty_checkpoint_stays_empty_after_new_member_and_is_erased_with_query(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, capture, _, _, _, _ = await configured(
                engine, kernel, scope, clock, inputs=0
            )
            await build(queue)
            first = await checkpoint(service)
            clock[0] += timedelta(seconds=1)
            _, session, client, _, _, worker, _, _ = capture
            await client.durable_append(envelope(scope, "1", clock), session, 1)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            await service.grant(ProcessingGrant(source_id(scope, "1"), ("alice",)))
            await refresh(service, queue, "member")
            assert (await historical(service, first, clock[0]))["state"] == "empty"
            assert (await historical(service, await checkpoint(service), clock[0]))[
                "state"
            ] == "ready"
            await kernel.forget(
                ForgetRequest(scope, (source_id(scope, "1"),), mode=ForgetMode.ERASE)
            )
            assert (await service.history_points("language", actor="alice"))["points"] == []
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, first, clock[0])
            async with engine.repository.unit_of_work() as uow:
                assert all(
                    "history" not in r["payload"]
                    for r in await uow.derived_records(scope, "revision")
                )

    asyncio.run(run())


def test_checkpoint_time_and_definition_metadata_come_from_publish_transaction(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock, inputs=0)
            lease = await queue.claim("delayed", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            alleged = base.at(1) - timedelta(days=1)
            snapshot["at"] = alleged
            snapshot["definition"]["generation"] = 999
            snapshot["definition"]["slots"] = ["forged"]
            clock[0] += timedelta(seconds=1)
            await service.publish(lease.task, snapshot, service.prepare(snapshot))
            actual = await checkpoint(service)
            assert actual == clock[0].isoformat()
            assert (await historical(service, actual, base.at(1)))["state"] == "empty"
            with pytest.raises(DerivedError, match="derived_history_coverage_unavailable"):
                await historical(service, alleged, base.at(1))

    asyncio.run(run())


def test_past_policy_query_definition_and_l1_version_are_frozen(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, query, spec, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.list_admission_records(scope)
                row = next(r for r in rows if r["payload"]["draft"]["predicate"] == "locale")
                changed = deepcopy(row["payload"])
                changed["action"] = "REJECT"
                await uow.save_admission_record(
                    scope, row["id"], row["event_id"], row["slot_key"], changed, row["version"]
                )
            clock[0] += timedelta(seconds=1)
            await service.register_query(replace(query, version="2"), expected_generation=1)
            await service.register(replace(spec, version="2"), expected_generation=3)
            before = await historical(service, first, base.at(1))
            assert before["body"]["blocks"][0]["value"] == "zh-CN"
            async with engine.repository.unit_of_work() as uow:
                revision = await uow.derived_get(scope, "revision", before["revision_id"])
                assert revision["history"]["query"]["generation"] == 1
                assert revision["history"]["definition"]["version"] == "1"
                assert revision["history"]["policy"] == base.POLICY.config_payload()
                assert revision["history"]["records"][0]["version"] == row["version"]
            await refresh(service, queue, "reject")
            assert (await historical(service, await checkpoint(service), base.at(1)))[
                "state"
            ] == "empty"

    asyncio.run(run())


def test_policy_migration_does_not_reinterpret_old_certified_checkpoint(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, query, spec, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            from agent_memory.consolidation.admission import AdmissionPolicy

            policy = AdmissionPolicy([base.PredicateSpec("locale", allow_self_report=False)])
            policy.version = "changed-policy/2"
            newer = ObservationService(
                engine.repository,
                scope,
                policy,
                clock=lambda: clock[0],
                authority_id="local-host",
                authority_min_version=1,
                history_mode=MODE,
            )
            await newer.register_query(replace(query, version="2"), expected_generation=1)
            await newer.register(replace(spec, version="2"), expected_generation=3)
            result = await historical(newer, first, base.at(1))
            assert result["body"]["blocks"][0]["value"] == "zh-CN"
            async with engine.repository.unit_of_work() as uow:
                revision = await uow.derived_get(scope, "revision", result["revision_id"])
                assert revision["history"]["policy"] == base.POLICY.config_payload()
                assert revision["history"]["policy"] != newer.policy

    asyncio.run(run())


@pytest.mark.parametrize(
    "problem", ["grant", "authority", "expiry", "floor", "audience", "purpose"]
)
def test_current_security_checked_before_archived_bodies(store, monkeypatch, problem):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, spec, _ = await configured(
                engine, kernel, scope, clock, inputs=2
            )
            await build(queue)
            first = await checkpoint(service)
            if problem == "grant":
                await service.grant(
                    ProcessingGrant(source_id(scope, "2"), ("alice",), revoked=True),
                    expected_version=2,
                )
            elif problem == "authority":
                await service.set_authority(replace(authority, revoked=True), expected_version=1)
            elif problem == "expiry":
                clock[0] = authority.expires_at
            elif problem == "floor":
                service.authority_min_version = 2
            elif problem == "audience":
                await service.set_authority(
                    replace(authority, readers=("alice", "bob")), expected_version=1
                )
                await service.register(replace(spec, readers=("bob",)), expected_generation=3)
            calls = archive_spy(monkeypatch, engine.repository)
            with pytest.raises(DerivedError):
                await historical(
                    service,
                    first,
                    base.at(1),
                    purpose="other" if problem == "purpose" else "agent_context",
                )
            assert not calls
            with pytest.raises(DerivedError):
                await service.history_points(
                    "language",
                    actor="alice",
                    purpose="other" if problem == "purpose" else "agent_context",
                )
            assert not calls

    asyncio.run(run())


def test_authority_renewal_regrant_allows_old_history_with_current_auth_summary(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            await service.set_authority(authority, expected_version=1)
            with pytest.raises(DerivedError, match="derived_grant_authority_changed"):
                await historical(service, first, base.at(1))
            await service.grant(
                ProcessingGrant(source_id(scope, "1"), ("alice",)), expected_version=2
            )
            result = await historical(service, first, base.at(1))
            assert result["state"] == "ready"
            assert result["authorization"]["readers"] == ["alice"]

    asyncio.run(run())


@pytest.mark.parametrize("part", ["point", "archive", "manifest", "body", "certificate"])
def test_history_integrity_rejects_corrupt_metadata_bodies_and_completion(store, part):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            receipt = await build(queue)
            first = await checkpoint(service)
            async with engine.repository.unit_of_work() as uow:
                point = (await uow.derived_records(scope, "history_point"))[0]["payload"]
                if part == "point":
                    point["sources"] = []
                    await uow.derived_put(scope, "history_point", point["id"], point)
                elif part == "certificate":
                    job = await uow.derived_get(scope, "job", receipt["unit_id"])
                    job["commit_token"] = "forged"
                    await uow.derived_put(scope, "job", receipt["unit_id"], job)
                else:
                    revision = await uow.derived_get(scope, "revision", point["revision_id"])
                    if part == "archive":
                        revision["history"]["records"][0]["payload"]["draft"]["value"] = "injected"
                    elif part == "manifest":
                        revision["manifest"]["query_complete"] = False
                    else:
                        revision["body"]["blocks"][0]["value"] = "injected"
                    await uow.derived_put(scope, "revision", point["revision_id"], revision)
            with pytest.raises(DerivedError, match="derived_history_integrity_failed"):
                await historical(service, first, base.at(1))

    asyncio.run(run())


def test_same_time_is_immutable_dedup_or_atomic_conflict(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, query, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            await refresh(service, queue, "same")
            assert len((await service.history_points("language", actor="alice"))["points"]) == 1
            await service.register_query(replace(query, version="2"), expected_generation=1)
            receipt = await queue.request("language", dedupe_key="conflict", force=True)
            lease = await queue.claim("conflict", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_history_point_conflict"):
                await service.apply(lease.task)
            assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            assert (await historical(service, first, base.at(1)))["state"] == "ready"
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "revision")) == 2

    asyncio.run(run())


def test_history_capacity_rolls_back_revision_head_and_completion(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            async with engine.repository.unit_of_work() as uow:
                old_head = await uow.derived_get(scope, "head", "language")
                for i in range(127):
                    key = "erased-point-" + str(i)
                    await uow.derived_put(
                        scope,
                        "history_point",
                        key,
                        dict(id=key, facet_id="language", state="erased"),
                    )
            clock[0] += timedelta(seconds=1)
            receipt = await queue.request("language", dedupe_key="capacity", force=True)
            lease = await queue.claim("capacity", lease_seconds=60)
            with pytest.raises(DerivedError, match="derived_history_capacity"):
                await service.apply(lease.task)
            assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "head", "language") == old_head
                assert len(await uow.derived_records(scope, "revision")) == 1

    asyncio.run(run())


def test_publication_and_deletion_serialize_on_independent_connections(store, monkeypatch):
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
            lease = await queue.claim("publish", lease_seconds=60)
            entered, release = asyncio.Event(), asyncio.Event()
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_put

            async def paused(self, *args):
                await original(self, *args)
                if self._repository is engine.repository and args[1] == "history_point":
                    entered.set()
                    await release.wait()

            async def erase():
                return await other.forget(
                    ForgetRequest(scope, (source_id(scope, "1"),), mode=ForgetMode.ERASE)
                )

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "derived_put", paused)
                    publication = asyncio.create_task(service.apply(lease.task))
                    await asyncio.wait_for(entered.wait(), timeout=10)
                    deletion = asyncio.create_task(
                        erase() if pg else asyncio.to_thread(lambda: asyncio.run(erase()))
                    )
                    await asyncio.sleep(0.05)
                    assert not deletion.done()
                    release.set()
                    await asyncio.wait_for(publication, timeout=10)
                    await asyncio.wait_for(deletion, timeout=10)
                assert (await service.history_points("language", actor="alice"))["points"] == []
                async with other.unit_of_work() as uow:
                    assert all(
                        "history" not in r["payload"]
                        for r in await uow.derived_records(scope, "revision")
                    )
            finally:
                release.set()
                if pg:
                    await other.close()

    asyncio.run(run())


def test_restored_old_authority_floor_blocks_history_before_archive(store, tmp_path, monkeypatch):
    from test_purge_restore import backup_copy

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await service.set_authority(replace(authority, revoked=True), expected_version=1)
                clone = ObservationService(
                    backup,
                    scope,
                    base.POLICY,
                    clock=lambda: clock[0],
                    authority_id="local-host",
                    authority_min_version=2,
                    history_mode=MODE,
                )
                calls = archive_spy(monkeypatch, backup)
                with pytest.raises(DerivedError, match="derived_authority_rollback"):
                    await historical(clone, first, base.at(1))
                assert not calls

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_actual_sigkill_keeps_history_revision_head_and_certificate_atomic(
    store, tmp_path, boundary
):
    from test_durable_process_recovery import kill_at_boundary

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await kill_at_boundary(engine, scope, clock, tmp_path, "history_" + boundary)
            async with engine.repository.unit_of_work() as uow:
                points = await uow.derived_records(scope, "history_point")
                revisions = await uow.derived_records(scope, "revision")
                head = await uow.derived_get(scope, "head", "language")
                assert bool(points) == bool(revisions) == bool(head) == (boundary == "after_commit")
            clock[0] += timedelta(seconds=6)
            if boundary == "before_commit":
                await refresh(service, queue, "recover")
            assert (await historical(service, await checkpoint(service), base.at(1)))[
                "state"
            ] == "ready"

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["object", "scope"])
def test_real_backup_replay_scrubs_historical_archive_and_coverage(store, tmp_path, mode):
    from test_purge_restore import backup_copy, replay, restorer

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                clone = ObservationService(
                    backup,
                    scope,
                    base.POLICY,
                    clock=lambda: clock[0],
                    authority_id="local-host",
                    authority_min_version=1,
                    history_mode=MODE,
                )
                assert (await historical(clone, first, base.at(1)))["state"] == "ready"
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
                    async with repository.unit_of_work() as uow:
                        assert all(
                            "history" not in r["payload"]
                            for r in await uow.derived_records(scope, "revision")
                        )
                        assert all(
                            r["payload"]["state"] == "erased"
                            for r in await uow.derived_records(scope, "history_point")
                        )
                with pytest.raises(DerivedError):
                    await historical(clone, first, base.at(1))

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
@pytest.mark.parametrize("revoke", [False, True])
def test_sdk_delivery_keeps_fixed_times_and_rechecks_current_authority(
    store, monkeypatch, transport, revoke
):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            valid = (base.at(1) - timedelta(seconds=1)).isoformat()
            context = MCPRequestContext(scope, actor="alice")
            original, calls = service.read, []

            async def intervening(*args, **kwargs):
                calls.append(kwargs)
                result = await original(*args, **kwargs)
                if revoke and len(calls) == 1:
                    await service.set_authority(
                        replace(authority, revoked=True), expected_version=1
                    )
                return result

            monkeypatch.setattr(service, "read", intervening)

            async def exercise(client):
                assert (await client.derived_capabilities())["historical_mode"] == MODE
                assert (await client.derived_history_points("language"))["points"][0][
                    "known_at"
                ] == first
                if revoke:
                    with pytest.raises(
                        sdk.MemoryClientError, match="derived_authority_unavailable"
                    ):
                        await client.derived_context("language", known_at=first, valid_at=valid)
                else:
                    result = await client.derived_context(
                        "language", known_at=first, valid_at=valid
                    )
                    assert result["observations"] == []  # Current body would be ready: no fallback.
                assert len(calls) == 2
                assert all(c["known_at"] == first and c["valid_at"] == valid for c in calls)

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


@pytest.mark.parametrize(
    "problem",
    ["partial", "naive", "malformed", "future_known", "future_valid", "unknown_operation"],
)
def test_invalid_history_request_is_explicit_and_readonly(store, problem):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            first = await checkpoint(service)
            context = MCPRequestContext(scope, actor="alice")
            payload = dict(facet_id="language", known_at=first, valid_at=base.at(1).isoformat())
            operation = "read"
            if problem == "partial":
                del payload["valid_at"]
            elif problem == "naive":
                payload["valid_at"] = "2026-10-01"
            elif problem == "malformed":
                payload["known_at"] = "invalid"
            elif problem == "future_known":
                payload["known_at"] = (clock[0] + timedelta(seconds=1)).isoformat()
            elif problem == "future_valid":
                payload["valid_at"] = (clock[0] + timedelta(seconds=1)).isoformat()
            else:
                operation = "publish_history"
            with pytest.raises(DerivedError):
                await service.call(operation, payload, context)
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "history_point")) == 1

    asyncio.run(run())


def test_history_is_host_optin_and_legacy_workers_cannot_claim_it(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, legacy = await configured(engine, kernel, scope, clock)
            assert await FacetRefreshQueue(legacy).claim("plain", lease_seconds=60) is None
            await build(queue)
            with pytest.raises(DerivedError):
                await legacy.read(
                    "language",
                    actor="alice",
                    known_at=await checkpoint(service),
                    valid_at=base.at(1),
                )
            with pytest.raises(DerivedError, match="invalid_derived_request"):
                await service.call(
                    "read",
                    {"facet_id": "language", "history_mode": MODE},
                    MCPRequestContext(scope, actor="alice"),
                )

    asyncio.run(run())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"history_mode": MODE},
        {"history_mode": "continuous/1", "query_id": "q", "authority_id": "a"},
        {
            "history_mode": MODE,
            "query_id": "q",
            "authority_id": "a",
            "template_version": "locale-context/1",
        },
    ],
)
def test_incomplete_or_unsupported_history_contract_rejected(kwargs):
    with pytest.raises(DerivedError):
        FacetDefinition("language", "alice", **kwargs)
