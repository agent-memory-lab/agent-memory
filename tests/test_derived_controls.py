"""Stage 14A: current query census and expiring host authority on both real backends."""

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_observations import build, setup
from test_durable_purge import envelope, source_id

from agent_memory.derived import (
    DerivedError,
    FacetDefinition,
    HostGrantAuthority,
    ObservationService,
    ProcessingGrant,
    QueryDefinition,
)
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.facet_refresh import FacetRefreshQueue

store = base.store


async def configured(engine, kernel, scope, clock, *, inputs=1):
    legacy, _, capture = await setup(engine, kernel, scope, clock, inputs=inputs)
    service = ObservationService(
        engine.repository,
        scope,
        base.POLICY,
        clock=lambda: clock[0],
        authority_id="local-host",
        authority_min_version=0,
    )
    authority = HostGrantAuthority("local-host", ("alice",), clock[0] + timedelta(hours=1))
    query = QueryDefinition("language-inputs", scope, "alice", ("locale",))
    await service.set_authority(authority)
    await service.register_query(query)
    await service.register(
        FacetDefinition("language", "alice", query_id=query.id, authority_id=authority.id),
        expected_generation=1,
    )
    for i in range(1, inputs + 1):
        await service.grant(
            ProcessingGrant(source_id(scope, str(i)), ("alice",)), expected_version=1
        )
    return service, FacetRefreshQueue(service), capture, authority, query, legacy


def body_spy(monkeypatch, repository):
    calls = []
    cls = type(repository.unit_of_work())
    for name in ("get_source_event", "get_admission_record"):
        original = getattr(cls, name)

        async def spy(self, *args, _name=name, _original=original):
            calls.append(_name)
            return await _original(self, *args)

        monkeypatch.setattr(cls, name, spy)
    return calls


def test_bound_snapshot_manifest_and_expiry_are_proven(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, query, _ = await configured(
                engine, kernel, scope, clock, inputs=2
            )
            receipt = await build(queue)
            unit = receipt["unit"]
            assert unit["schema"] == "facet-refresh-unit/2"
            assert unit["bindings"]["query"]["id"] == query.id
            assert unit["bindings"]["authority"]["version"] == 1
            view = await service.read("language", actor="alice")
            assert view["state"] == "ready"
            async with engine.repository.unit_of_work() as uow:
                revision = await uow.derived_get(scope, "revision", view["revision_id"])
                assert len(revision["manifest"]["sources"]) == 2
                assert revision["manifest"]["unit"] == unit
                assert revision["next_transition_at"] == authority.expires_at.isoformat()

    asyncio.run(run())


def test_query_revision_invalidates_head_and_fixed_running_target(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, query, _ = await configured(engine, kernel, scope, clock)
            first = await build(queue)
            pending = await queue.request("language", dedupe_key="pending", force=True)
            lease = await queue.claim("old-query", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            await service.register_query(replace(query, version="2"), expected_generation=1)
            assert (await service.read("language", actor="alice"))["state"] == "stale"
            with pytest.raises(DerivedError, match="derived_snapshot_changed"):
                await service.publish(lease.task, snapshot, service.prepare(snapshot))
            assert not (await queue.status(pending["target_id"], actor="alice"))["complete"]
            # A finite completed target proves its original execution, never current freshness.
            assert (await queue.status(first["target_id"], actor="alice"))["complete"]
            await queue.fail(lease, DerivedError("derived_snapshot_changed"))
            current = await queue.claim("new-query", lease_seconds=60)
            assert current.task.payload["unit"]["bindings"]["query"]["generation"] == 2
            await service.apply(current.task)
            assert (await service.read("language", actor="alice"))["state"] == "ready"

    asyncio.run(run())


def test_query_contract_is_generic_but_consumer_must_support_whole_census(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _, _, query, _ = await configured(engine, kernel, scope, clock, inputs=0)
            multi = replace(query, id="all-slots", predicates=("city", "locale"))
            registered = await service.register_query(multi)
            assert len(registered["slots"]) == 2
            with pytest.raises(DerivedError, match="derived_query_consumer_unsupported"):
                await service.register(
                    FacetDefinition(
                        "unsupported", "alice", query_id=multi.id, authority_id="local-host"
                    )
                )
            with pytest.raises(DerivedError, match="derived_query_consumer_unsupported"):
                await service.register_query(
                    replace(query, predicates=("city",)), expected_generation=1
                )
            async with engine.repository.unit_of_work() as uow:
                unchanged = await uow.derived_get(scope, "query", query.id)
                assert unchanged["generation"] == 1
            with pytest.raises(DerivedError, match="derived_predicate_unregistered"):
                await service.register_query(replace(query, id="unknown", predicates=("secret",)))
            with pytest.raises(DerivedError, match="derived_query_scope_mismatch"):
                await service.register_query(
                    replace(query, scope=replace(scope, session_id="other"))
                )

    asyncio.run(run())


def test_query_and_authority_compare_and_swap_across_connections(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _, authority, query, _ = await configured(engine, kernel, scope, clock)
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
            second = ObservationService(
                other,
                scope,
                base.POLICY,
                clock=lambda: clock[0],
                authority_id=authority.id,
                authority_min_version=1,
            )
            try:
                for operations, code in (
                    (
                        [
                            s.register_query(replace(query, version="2"), expected_generation=1)
                            for s in (service, second)
                        ],
                        "derived_query_conflict",
                    ),
                    (
                        [s.set_authority(authority, expected_version=1) for s in (service, second)],
                        "derived_authority_conflict",
                    ),
                ):
                    results = await asyncio.gather(*operations, return_exceptions=True)
                    assert sum(isinstance(r, dict) for r in results) == 1
                    assert [r.code for r in results if isinstance(r, DerivedError)] == [code]
            finally:
                if pg:
                    await other.close()

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["read", "snapshot", "publish", "status"])
def test_revoked_authority_blocks_every_boundary_before_bodies(store, monkeypatch, operation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _ = await configured(engine, kernel, scope, clock)
            receipt = await build(queue)
            await queue.request("language", dedupe_key="inflight", force=True)
            lease = await queue.claim("inflight", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            prepared = service.prepare(snapshot)
            await service.set_authority(replace(authority, revoked=True), expected_version=1)
            calls = body_spy(monkeypatch, engine.repository)
            with pytest.raises(DerivedError, match="derived_authority_unavailable"):
                if operation == "read":
                    await service.read("language", actor="alice")
                elif operation == "snapshot":
                    await service.snapshot(lease.task)
                elif operation == "publish":
                    await service.publish(lease.task, snapshot, prepared)
                else:
                    await queue.status(receipt["target_id"], actor="alice")
            assert calls == []
            assert await queue.claim("revoked", lease_seconds=60) is None

    asyncio.run(run())


@pytest.mark.parametrize("inputs", [0, 1])
def test_authority_expiry_blocks_empty_and_ready_views(store, monkeypatch, inputs):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _ = await configured(
                engine, kernel, scope, clock, inputs=inputs
            )
            receipt = await build(queue)
            clock[0] = authority.expires_at
            calls = body_spy(monkeypatch, engine.repository)
            for action in (
                service.read("language", actor="alice"),
                queue.status(receipt["target_id"], actor="alice"),
                queue.request("language", dedupe_key="expired"),
            ):
                with pytest.raises(DerivedError, match="derived_authority_expired"):
                    await action
            assert await queue.claim("expired", lease_seconds=60) is None
            assert calls == []

    asyncio.run(run())


def test_authority_renewal_requires_fresh_grants_and_cannot_restore_old_head(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            await service.set_authority(authority, expected_version=1)
            assert (await service.read("language", actor="alice"))["body"] is None
            lease = await queue.claim("renewed", lease_seconds=60)
            with monkeypatch.context() as patch:
                calls = body_spy(patch, engine.repository)
                with pytest.raises(DerivedError, match="derived_grant_authority_changed"):
                    await service.snapshot(lease.task)
                assert calls == []
            await queue.fail(lease, DerivedError("derived_grant_authority_changed"))
            await service.grant(
                ProcessingGrant(source_id(scope, "1"), ("alice",)), expected_version=2
            )
            new = await queue.claim("fresh-grants", lease_seconds=60)
            await service.apply(new.task)
            assert (await service.read("language", actor="alice"))["state"] == "ready"

    asyncio.run(run())


def test_legacy_host_cannot_overwrite_protected_grant_or_claim_bound_work(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, legacy = await configured(engine, kernel, scope, clock)
            receipt = await queue.request("language", dedupe_key="bound")
            calls = body_spy(monkeypatch, engine.repository)
            with pytest.raises(DerivedError, match="derived_authority_mismatch"):
                await legacy.grant(
                    ProcessingGrant(source_id(scope, "1"), ("alice",)), expected_version=2
                )
            with pytest.raises(DerivedError, match="derived_authority_mismatch"):
                await legacy.read("language", actor="alice")
            with pytest.raises(DerivedError, match="derived_authority_mismatch"):
                await legacy.register(FacetDefinition("language", "alice"), expected_generation=2)
            with pytest.raises(DerivedError, match="derived_authority_mismatch"):
                await FacetRefreshQueue(legacy).status(receipt["target_id"], actor="alice")
            assert await FacetRefreshQueue(legacy).claim("plain", lease_seconds=60) is None
            assert calls == []
            assert await queue.claim("bound", lease_seconds=60) is not None
            wrong = ObservationService(
                engine.repository,
                scope,
                base.POLICY,
                clock=lambda: clock[0],
                authority_id="other",
                authority_min_version=1,
            )
            assert await FacetRefreshQueue(wrong).claim("wrong", lease_seconds=60) is None
            with pytest.raises(DerivedError, match="derived_authority_mismatch"):
                await wrong.read("language", actor="alice")

    asyncio.run(run())


@pytest.mark.parametrize("changes", [dict(readers=("bob",)), dict(purposes=("research",))])
def test_authority_intersection_rejects_broader_grants_and_facets(store, monkeypatch, changes):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            calls = body_spy(monkeypatch, engine.repository)
            grant = replace(ProcessingGrant(source_id(scope, "1"), ("alice",)), **changes)
            with pytest.raises(DerivedError, match="derived_authority_denied"):
                await service.grant(grant, expected_version=2)
            args = (
                {"readers": changes["readers"]}
                if "readers" in changes
                else {"purpose": changes["purposes"][0]}
            )
            with pytest.raises(DerivedError, match="derived_authority_denied"):
                await service.register(
                    FacetDefinition("broader", "alice", authority_id="local-host", **args)
                )
            assert calls == []

    asyncio.run(run())


def test_complete_census_includes_ungranted_counterexample_and_new_member(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, capture, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            _, session, client, _, generator, worker, _, _ = capture
            generator.verdict = "uncertain"
            generator.values = (("locale", "en-US"),)
            await client.durable_append(envelope(scope, "2", clock), session, 2)
            assert await worker.run_once()
            clock[0] += timedelta(seconds=1)
            assert (await service.read("language", actor="alice"))["state"] == "stale"
            lease = await queue.claim("new-member", lease_seconds=60)
            with monkeypatch.context() as patch:
                calls = body_spy(patch, engine.repository)
                with pytest.raises(DerivedError, match="derived_processing_denied"):
                    await service.snapshot(lease.task)
                assert calls == []
            await queue.fail(lease, DerivedError("derived_processing_denied"))
            await service.grant(ProcessingGrant(source_id(scope, "2"), ("alice",)))
            lease = await queue.claim("counterexample", lease_seconds=60)
            await service.apply(lease.task)
            view = await service.read("language", actor="alice")
            assert view["body"]["blocks"][0]["value"] == "zh-CN"
            async with engine.repository.unit_of_work() as uow:
                revision = await uow.derived_get(scope, "revision", view["revision_id"])
                assert len(revision["manifest"]["atoms"]) == 2
                assert len(revision["manifest"]["sources"]) == 2

    asyncio.run(run())


def test_publish_authorizes_actual_query_members_before_any_body(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _ = await configured(engine, kernel, scope, clock, inputs=2)
            lease = await queue.claim("full", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            denied = source_id(scope, "2")
            async with engine.repository.unit_of_work() as uow:
                grant = await uow.derived_get(scope, "grant", denied)
                grant["revoked"] = True  # Even a missing outbox invalidation must fail closed.
                await uow.derived_put(scope, "grant", denied, grant)
            del snapshot["manifest"]["sources"][denied]
            snapshot["records"] = [r for r in snapshot["records"] if r["event_id"] != denied]
            snapshot["manifest"]["atoms"] = {r["id"]: r["version"] for r in snapshot["records"]}
            del snapshot["sources"][denied]
            del snapshot["grants"][denied]
            prepared = service.prepare(snapshot)
            calls = body_spy(monkeypatch, engine.repository)
            with pytest.raises(DerivedError, match="derived_processing_denied"):
                await service.publish(lease.task, snapshot, prepared)
            assert calls == []

    asyncio.run(run())


def test_scope_erasure_tombstones_control_and_needs_explicit_new_epoch_registration(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, query, _ = await configured(engine, kernel, scope, clock)
            old = await build(queue)
            await kernel.forget(ForgetRequest(scope, all_in_scope=True, mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                a = await uow.derived_get(scope, "authority", authority.id)
                q = await uow.derived_get(scope, "query", query.id)
                assert a["spec"] == {"id": authority.id, "revoked": True}
                assert q["disabled"] and "spec" not in q
            with pytest.raises(DerivedError, match="derived_authority_unavailable"):
                await service.grant(ProcessingGrant("deleted", ("alice",)))
            await service.set_authority(authority, expected_version=2)
            await service.register_query(query, expected_generation=2)
            await service.register(
                FacetDefinition("language", "alice", query_id=query.id, authority_id=authority.id),
                expected_generation=2,
            )
            new = await queue.request("language", dedupe_key="first")
            assert new["target_id"] != old["target_id"]
            with pytest.raises(DerivedError, match="derived_target_unavailable"):
                await queue.status(old["target_id"], actor="alice")
            lease = await queue.claim("new-epoch", lease_seconds=60)
            await service.apply(lease.task)
            assert (await service.read("language", actor="alice"))["state"] == "empty"

    asyncio.run(run())


def test_host_controls_remain_unavailable_to_model_transport(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            context = MCPRequestContext(scope, actor="alice")
            capabilities = await service.call("capabilities", {}, context)
            assert capabilities["grant_authority"] == "host-grant-authority/1"
            assert not capabilities["historical"] and not capabilities["remote_acl"]
            for name in ("set_authority", "register_query", "grant"):
                with pytest.raises(DerivedError, match="unsupported_derived_operation"):
                    await service.call(name, {}, context)
            with pytest.raises(DerivedError, match="derived_history_unsupported"):
                await service.read("language", actor="alice", known_at=clock[0])

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["query", "authority"])
@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_real_sigkill_control_and_outbox_are_atomic(store, tmp_path, kind, boundary):
    from test_durable_process_recovery import kill_at_boundary

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, query, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            await kill_at_boundary(
                engine, scope, clock, tmp_path, "control_" + kind + "_" + boundary
            )
            committed = boundary == "after_commit"
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(
                    scope, kind, query.id if kind == "query" else authority.id
                )
                definition = await uow.derived_get(scope, "definition", "language")
                assert row["generation" if kind == "query" else "version"] == 1 + committed
                assert definition["dirty"] == committed
            if committed and kind == "authority":
                with pytest.raises(DerivedError, match="derived_authority_unavailable"):
                    await service.read("language", actor="alice")
            else:
                view = await service.read("language", actor="alice")
                assert view["state"] == ("stale" if committed else "ready")

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["object", "scope"])
def test_real_backup_purge_replay_erases_controls_and_protected_history(store, tmp_path, mode):
    from test_purge_restore import backup_copy, replay, restorer

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, query, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                clone = ObservationService(
                    backup,
                    scope,
                    base.POLICY,
                    clock=lambda: clock[0],
                    authority_id=authority.id,
                    authority_min_version=1,
                )
                assert (await clone.read("language", actor="alice"))["state"] == "ready"
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
                async with backup.unit_of_work() as uow:
                    assert all(
                        "body" not in r["payload"] and "manifest" not in r["payload"]
                        for r in await uow.derived_records(scope, "revision")
                    )
                    if mode == "scope":
                        a = await uow.derived_get(scope, "authority", authority.id)
                        q = await uow.derived_get(scope, "query", query.id)
                        assert a["spec"] == {"id": authority.id, "revoked": True}
                        assert q["disabled"] and "spec" not in q
                if mode == "scope":
                    with pytest.raises(DerivedError, match="derived_definition_unavailable"):
                        await clone.read("language", actor="alice")
                else:
                    assert (await clone.read("language", actor="alice"))["body"] is None

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_transport_final_delivery_rechecks_authority(store, monkeypatch, transport):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            context = MCPRequestContext(scope, actor="alice")
            original, calls = service.read, []

            async def intervening(*args, **kwargs):
                result = await original(*args, **kwargs)
                calls.append(result)
                if len(calls) == 1:
                    await service.set_authority(
                        replace(authority, revoked=True), expected_version=1
                    )
                return result

            monkeypatch.setattr(service, "read", intervening)

            async def exercise(client):
                assert (await client.derived_capabilities())["remote_acl"] is False
                with pytest.raises(sdk.MemoryClientError, match="derived_authority_unavailable"):
                    await client.derived_context("language")
                assert len(calls) == 1  # Second read fails before returning any cached body.

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


def test_qualified_context_generation_composes_with_query_and_authority(store):
    import test_derived_contextual as qualified

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, _, _, rows = await qualified.setup(engine, kernel, scope, clock)
            await qualified.qualify(engine, scope, clock, rows[0])
            service = ObservationService(
                engine.repository,
                scope,
                base.POLICY,
                clock=lambda: clock[0],
                authority_id="local-host",
                authority_min_version=0,
                context_token="route-A",
            )
            await service.set_authority(
                HostGrantAuthority("local-host", ("alice",), clock[0] + timedelta(hours=1))
            )
            await service.register_query(QueryDefinition("q", scope, "alice", ("locale",)))
            definition = replace(
                qualified.definition(scope, clock), query_id="q", authority_id="local-host"
            )
            await service.register(definition, expected_generation=1)
            await service.grant(
                ProcessingGrant(rows[0]["event_id"], ("alice",)), expected_version=1
            )
            queue = FacetRefreshQueue(service)
            view, _ = await qualified.build(service, queue)
            assert view["state"] == "ready" and view["body"]["projection_status"] == "resolved"
            await queue.request("language", dedupe_key="old-context", force=True)
            lease = await queue.claim("old-context", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            revised = replace(
                qualified.definition(scope, clock, project="B"),
                query_id="q",
                authority_id="local-host",
            )
            await service.register(revised, expected_generation=2)
            with pytest.raises(DerivedError, match="derived_snapshot_changed"):
                await service.publish(lease.task, snapshot, service.prepare(snapshot))
            await queue.fail(lease, DerivedError("derived_snapshot_changed"))
            view, _ = await qualified.build(service, queue, key="new-context")
            assert view["state"] == "empty"

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["query", "authority"])
def test_control_update_serializes_with_publication_on_other_connection(store, monkeypatch, kind):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, query, _ = await configured(engine, kernel, scope, clock)
            lease = await queue.claim("publish", lease_seconds=60)
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
            second = ObservationService(
                other,
                scope,
                base.POLICY,
                clock=lambda: clock[0],
                authority_id=authority.id,
                authority_min_version=1,
            )
            entered, release, changing = asyncio.Event(), asyncio.Event(), asyncio.Event()
            cls, original = (
                type(engine.repository.unit_of_work()),
                type(engine.repository.unit_of_work()).derived_put,
            )

            async def paused(self, *args):
                await original(self, *args)
                if self._repository is engine.repository and args[1] == "revision":
                    entered.set()
                    await release.wait()

            async def update():
                if kind == "query":
                    return await second.register_query(
                        replace(query, version="2"), expected_generation=1
                    )
                return await second.set_authority(
                    replace(authority, revoked=True), expected_version=1
                )

            async def independent_update():
                changing.set()
                if pg:
                    return await update()
                # SQLite's synchronous BEGIN must wait on an independent thread,
                # so it cannot block the event loop that releases the other writer.
                return await asyncio.to_thread(lambda: asyncio.run(update()))

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "derived_put", paused)
                    publication = asyncio.create_task(service.apply(lease.task))
                    await asyncio.wait_for(entered.wait(), timeout=10)
                    change = asyncio.create_task(independent_update())
                    await asyncio.wait_for(changing.wait(), timeout=10)
                    await asyncio.sleep(0.03)
                    assert not change.done()
                    release.set()
                    await asyncio.wait_for(publication, timeout=10)
                    await asyncio.wait_for(change, timeout=10)
                if kind == "query":
                    assert (await service.read("language", actor="alice"))["body"] is None
                else:
                    with pytest.raises(DerivedError, match="derived_authority_unavailable"):
                        await service.read("language", actor="alice")
            finally:
                release.set()
                if pg:
                    await other.close()

    asyncio.run(run())


def test_independent_authority_floor_rejects_old_backup_before_bodies(store, tmp_path, monkeypatch):
    from test_purge_restore import backup_copy

    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _ = await configured(engine, kernel, scope, clock)
            receipt = await build(queue)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await service.set_authority(replace(authority, revoked=True), expected_version=1)
                assert service.authority_min_version == 2
                restored = ObservationService(
                    backup,
                    scope,
                    base.POLICY,
                    clock=lambda: clock[0],
                    authority_id=authority.id,
                    authority_min_version=service.authority_min_version,
                )
                calls = body_spy(monkeypatch, backup)
                actions = [
                    restored.read("language", actor="alice"),
                    FacetRefreshQueue(restored).status(receipt["target_id"], actor="alice"),
                    restored.grant(
                        ProcessingGrant(source_id(scope, "1"), ("alice",)), expected_version=2
                    ),
                    restored.set_authority(authority, expected_version=1),
                ]
                for action in actions:
                    with pytest.raises(DerivedError, match="derived_authority_rollback"):
                        await action
                assert calls == []

    asyncio.run(run())


def test_bootstrap_floor_cannot_open_existing_authority(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            unpinned = ObservationService(
                engine.repository,
                scope,
                base.POLICY,
                clock=lambda: clock[0],
                authority_id=authority.id,
                authority_min_version=0,
            )
            calls = body_spy(monkeypatch, engine.repository)
            for action in (
                unpinned.read("language", actor="alice"),
                unpinned.set_authority(authority, expected_version=1),
            ):
                with pytest.raises(DerivedError, match="trusted_authority_version_required"):
                    await action
            assert calls == []

    asyncio.run(run())


def test_failed_authority_transaction_does_not_advance_host_floor(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, _, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_put

            async def fail(self, *args):
                await original(self, *args)
                if args[1] == "definition":
                    raise RuntimeError("injected control transaction failure")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_put", fail)
                with pytest.raises(RuntimeError, match="injected control"):
                    await service.set_authority(
                        replace(authority, revoked=True), expected_version=1
                    )
            assert service.authority_min_version == 1
            assert (await service.read("language", actor="alice"))["state"] == "ready"

    asyncio.run(run())


@pytest.mark.parametrize("floor", [None, -1, True, "1"])
def test_authority_configuration_requires_trusted_floor(floor):
    with pytest.raises(DerivedError, match="trusted_authority_version_required"):
        ObservationService(
            object(),
            base.MemoryScope("t", user_id="alice"),
            base.POLICY,
            authority_id="host",
            authority_min_version=floor,
        )


@pytest.mark.parametrize(
    "changes",
    [
        dict(time_mode="historical"),
        dict(membership="accepted_only"),
        dict(predicates=("locale", "locale")),
        dict(predicates=()),
        dict(predicates=["locale"]),
    ],
)
def test_query_rejects_partial_or_unsupported_contract(changes):
    query = QueryDefinition("q", base.MemoryScope("t", user_id="alice"), "alice", ("locale",))
    with pytest.raises(DerivedError):
        replace(query, **changes)


def test_domain_and_registry_dependency_boundaries():
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "src" / "agent_memory" / "derived"
    contracts = ast.parse((root / "contracts.py").read_text())
    assert all(
        node.module not in {"registry", "service", "observation"}
        for node in ast.walk(contracts)
        if isinstance(node, ast.ImportFrom)
    )
    registry = ast.parse((root / "registry.py").read_text())
    assert all(
        node.module not in {"sqlite", "agent_memory_postgres", "observation", "contextual"}
        for node in ast.walk(registry)
        if isinstance(node, ast.ImportFrom)
    )
