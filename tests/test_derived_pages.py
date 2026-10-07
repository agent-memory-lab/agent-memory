"""L2 full rebuild with real storage, authorization, crashes and deletion replay."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_observations import build, setup
from test_derived_parents import definition, publish
from test_durable_purge import envelope, source_id

from agent_memory.derived import (
    DerivedError,
    FacetDefinition,
    ObservationService,
    PageBlockRevision,
    PageDefinition,
    ProcessingGrant,
    ScenarioDefinition,
)
from agent_memory.derived.model import digest
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.mcp import MCPRequestContext
from agent_memory.operations.facet_refresh import FacetRefreshQueue

store = base.store


def page(scope, parents=("language",), **changes):
    return PageDefinition(
        "language-page", ScenarioDefinition("scenario", scope, "alice", "Language context"),
        tuple(parents), **changes,
    )


async def ready(engine, kernel, scope, clock, *, inputs=2, nested=False):
    service, queue, capture = await setup(engine, kernel, scope, clock, inputs=inputs)
    await build(queue)
    if nested:
        await service.register(definition("parent-view", ("language",)))
        await publish(service, queue, "parent-view")
    await service.register_page(page(scope, ("parent-view",) if nested else ("language",)))
    receipt = await publish(service, queue, "language-page")
    return service, queue, capture, receipt


@pytest.mark.parametrize("inputs", [0, 1, 2])
@pytest.mark.parametrize("nested", [False, True])
def test_fixed_complete_page_block_contract_and_empty_readiness(store, inputs, nested):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, receipt = await ready(
                engine, kernel, scope, clock, inputs=inputs, nested=nested
            )
            view = await service.pages.read("language-page", actor="alice")
            assert view["state"] == ("ready" if inputs else "empty")
            status = await service.pages.status(receipt["target_id"], actor="alice")
            assert status["complete"] and status["page_complete"]
            assert status["page_ready"] == bool(inputs)
            async with engine.repository.unit_of_work() as uow:
                revision = await uow.derived_get(scope, "revision", view["revision_id"])
                assert revision["manifest"]["atoms"] == revision["manifest"]["sources"] == {}
                assert revision["manifest"]["parents"] == revision["unit"]["parents"]
                assert set(revision["manifest"]["lineage"]) == (
                    {"language", "parent-view"} if nested else {"language"}
                )
                assert len(revision["parents"]) == 1  # includes an empty parent
                rows = await uow.derived_records(scope, "page_block")
                assert len(rows) == int(bool(inputs))
                if inputs:
                    block = rows[0]["payload"]
                    assert block["manifest"] == revision["manifest"]
                    assert block["page_revision_id"] == revision["id"]
                    parent = await service._read(
                        uow, "parent-view" if nested else "language", "alice", "agent_context"
                    )
                    assert view["body"]["blocks"][0]["body"]["content"] == parent["body"]["blocks"]
                    assert view["body"]["rebuild"] == "full"
                    assert revision["id"].startswith("page-version:")
                else:
                    assert revision["id"].startswith("page-empty:")
                    assert not rows
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


def test_full_rebuild_stable_block_identity_new_versions_no_old_page_input(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, first = await ready(engine, kernel, scope, clock)
            before = await service.pages.read("language-page", actor="alice")
            await service.register_page(replace(page(scope), version="2"), expected_generation=1)
            assert not (await service.pages.status(first["target_id"], actor="alice"))["page_ready"]
            receipt = await queue.request("language-page", dedupe_key="rebuild")
            lease = await queue.claim("page", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            assert set(snapshot["parents"]) == {"language"}
            assert before["revision_id"] not in str(snapshot["manifest"])
            prepared = service.prepare(snapshot)
            await service.publish(lease.task, snapshot, prepared)
            await queue.complete(lease)
            after = await service.pages.read("language-page", actor="alice")
            old, new = before["body"]["blocks"][0], after["body"]["blocks"][0]
            assert old["block_id"] == new["block_id"] and old["body"] == new["body"]
            assert old["revision_id"] != new["revision_id"]
            assert before["revision_id"] != after["revision_id"]
            assert (await service.pages.status(receipt["target_id"], actor="alice"))["page_ready"]
            old_status = await service.pages.status(first["target_id"], actor="alice")
            assert old_status["complete"] and not old_status["page_complete"]

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["snapshot", "delivery"])
@pytest.mark.parametrize("problem", ["private", "revoked", "expired", "version", "missing"])
def test_uncited_chain_source_denied_before_page_or_parent_body(
    store, boundary, problem, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _ = await ready(engine, kernel, scope, clock, nested=True)
            if boundary == "snapshot":
                await queue.request("language-page", dedupe_key="guard", force=True)
                lease = await queue.claim("guard", lease_seconds=60)
            async with engine.repository.unit_of_work() as uow:
                key = source_id(scope, "1")  # processed, but not selected
                grant = await uow.derived_get(scope, "grant", key)
                if problem == "private":
                    grant["readers"] = ["bob"]
                elif problem == "revoked":
                    grant["revoked"] = True
                elif problem == "expired":
                    grant["expires_at"] = clock[0].isoformat()
                elif problem == "version":
                    grant["version"] += 1
                else:
                    grant = None
                await uow.derived_put(scope, "grant", key, grant)
            cls, calls = type(engine.repository.unit_of_work()), []
            original = cls.derived_get

            async def tracked(self, *args):
                if args[1] in {"revision", "page_block"}:
                    calls.append(args[1])
                return await original(self, *args)

            async def forbidden(*args):
                pytest.fail("L0/L1 body loaded before full chain authorization")

            monkeypatch.setattr(cls, "derived_get", tracked)
            monkeypatch.setattr(cls, "get_source_event", forbidden)
            monkeypatch.setattr(cls, "get_admission_record", forbidden)
            if boundary == "snapshot":
                with pytest.raises(DerivedError):
                    await service.snapshot(lease.task)
            else:
                assert (await service.pages.read("language-page", actor="alice"))["body"] is None
            assert calls == []

    asyncio.run(run())


@pytest.mark.parametrize("change", ["source", "head", "definition", "grant", "time"])
def test_parent_change_invalidates_fixed_page_publish_and_orders_refresh(store, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, capture, first = await ready(engine, kernel, scope, clock)
            await queue.request("language-page", dedupe_key="pending", force=True)
            lease = await queue.claim("pending", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            if change == "source":
                _, session, client, _, _, worker, _, _ = capture
                await client.durable_append(envelope(scope, "3", clock), session, 3)
                assert await worker.run_once()
                await service.grant(ProcessingGrant(source_id(scope, "3"), ("alice",)))
            elif change == "head":
                await publish(service, queue, "language", "new", force=True)
            elif change == "definition":
                await service.register(replace(FacetDefinition("language", "alice"), version="2"),
                                       expected_generation=1)
            elif change == "grant":
                await service.grant(
                    ProcessingGrant(source_id(scope, "1"), ("alice",), revoked=True),
                    expected_version=1,
                )
            else:
                async with engine.repository.unit_of_work() as uow:
                    head = await uow.derived_get(scope, "head", "language")
                    head["next_transition_at"] = clock[0].isoformat()
                    await uow.derived_put(scope, "head", "language", head)
            with pytest.raises(DerivedError):
                await service.publish(lease.task, snapshot, service.prepare(snapshot))
            view = await service.pages.read("language-page", actor="alice")
            assert view["body"] is None
            assert not (await service.pages.status(first["target_id"], actor="alice"))["page_ready"]
            if change == "source":
                clock[0] += timedelta(seconds=61)  # release the obsolete page lease
                order = []
                for _ in range(2):
                    next_lease = await queue.claim("ordered", lease_seconds=60)
                    order.append(next_lease.task.payload["unit"]["facet_id"])
                    await service.apply(next_lease.task)
                    await queue.complete(next_lease)
                assert order == ["language", "language-page"]

    asyncio.run(run())


@pytest.mark.parametrize("fault", ["block_write", "head_write", "certificate", "capacity"])
def test_page_block_head_certificate_failure_rolls_back_entire_publication(
    store, fault, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, first = await ready(engine, kernel, scope, clock)
            before = await service.pages.read("language-page", actor="alice")
            receipt = await queue.request("language-page", dedupe_key="atomic", force=True)
            lease = await queue.claim("atomic", lease_seconds=60)
            cls = type(engine.repository.unit_of_work())
            original, records = cls.derived_put, cls.derived_records

            async def fail(self, *args):
                await original(self, *args)
                kind, row = args[1], args[3]
                if (fault == "block_write" and kind == "page_block") or (
                    fault == "head_write" and kind == "head"
                ) or (fault == "certificate" and kind == "job" and row["status"] == "completed"):
                    raise RuntimeError("injected page transaction failure")

            async def full(self, scope, kind):
                rows = await records(self, scope, kind)
                return rows * 4096 if kind == "page_block" else rows

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_put", fail)
                if fault == "capacity":
                    patch.setattr(cls, "derived_records", full)
                with pytest.raises((RuntimeError, DerivedError)):
                    await service.apply(lease.task)
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, "head", "language-page"))[
                    "revision_id"
                ] == before["revision_id"]
                assert len(await uow.derived_records(scope, "page_block")) == 1
                assert len([r for r in await uow.derived_records(scope, "revision")
                            if r["payload"]["facet_id"] == "language-page"]) == 1
            assert not (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            assert (await queue.status(first["target_id"], actor="alice"))["complete"]
            # Old immutable content survives, but is not delivered as current.
            assert (await service.pages.read("language-page", actor="alice"))["body"] is None
            await queue.fail(lease, DerivedError("page_output_capacity"))
            failed = await service.pages.read("language-page", actor="alice")
            assert failed["refresh_state"] == "retry"

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["manifest", "body", "block", "parents"])
def test_forged_complete_input_or_block_cannot_publish(store, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _ = await ready(engine, kernel, scope, clock)
            await queue.request("language-page", dedupe_key="forge", force=True)
            lease = await queue.claim("forge", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            prepared = service.prepare(snapshot)
            if mutation == "manifest":
                snapshot["manifest"]["lineage"] = {}
                prepared = service.prepare(snapshot)
            elif mutation == "parents":
                snapshot["parents"]["language"]["body"]["blocks"] = []
                prepared = service.prepare(snapshot)
            elif mutation == "body":
                prepared["body"]["scenario"]["title"] = "Forged"
            else:
                prepared["block_revisions"][0]["manifest"] = {}
            with pytest.raises(DerivedError):
                await service.publish(lease.task, snapshot, prepared)

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["missing", "body", "manifest", "version"])
def test_block_integrity_fail_closed_after_chain_authorization(store, mutation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _, receipt = await ready(engine, kernel, scope, clock)
            view = await service.pages.read("language-page", actor="alice")
            key = view["body"]["blocks"][0]["revision_id"]
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(scope, "page_block", key)
                if mutation == "missing":
                    row = None
                elif mutation == "body":
                    row["body"]["content"] = []
                elif mutation == "manifest":
                    row["manifest"] = {}
                else:
                    row["page_revision_id"] = "other"
                await uow.derived_put(scope, "page_block", key, row)
            bad = await service.pages.read("language-page", actor="alice")
            assert bad["body"] is None and bad["reason"] == "page_block_integrity_failed"
            status = await service.pages.status(receipt["target_id"], actor="alice")
            assert not status["page_ready"]

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["source", "scope", "page", "revision", "block", "block_revision"])
def test_all_page_versions_physical_erase_and_real_backup_replay(store, tmp_path, mode):
    async def run():
        from test_purge_restore import backup_copy, replay, restorer

        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _ = await ready(engine, kernel, scope, clock)
            await publish(service, queue, "language-page", "second", force=True)
            view = await service.pages.read("language-page", actor="alice")
            block = view["body"]["blocks"][0]
            keys = dict(source=source_id(scope, "1"), page="language-page",
                        revision=view["revision_id"], block=block["block_id"],
                        block_revision=block["revision_id"])
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await kernel.forget(ForgetRequest(
                    scope, () if mode == "scope" else (keys[mode],),
                    all_in_scope=mode == "scope", mode=ForgetMode.ERASE,
                ))
                journal = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), journal)
                for repo in (engine.repository, backup):
                    clone = ObservationService(repo, scope, base.POLICY, clock=lambda: clock[0])
                    if mode in {"scope", "page"}:
                        with pytest.raises(DerivedError):
                            await clone.pages.read("language-page", actor="alice")
                    else:
                        result = await clone.pages.read("language-page", actor="alice")
                        assert result["body"] is None
                    async with repo.unit_of_work() as uow:
                        rows = await uow.derived_records(scope, "page_block")
                        assert len(rows) == 2
                        assert all(r["payload"]["state"] == "erased" for r in rows)
                        assert all("body" not in r["payload"] and "manifest" not in r["payload"]
                                   for r in rows)
                        if mode in {"scope", "page"}:
                            definition = await uow.derived_get(scope, "definition", "language-page")
                            assert "scenario" not in definition["spec"]
                            assert definition["disabled"]

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_actual_sigkill_page_block_version_head_and_completion(store, tmp_path, boundary):
    async def run():
        from test_durable_process_recovery import kill_at_boundary

        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _ = await ready(engine, kernel, scope, clock)
            before = await service.pages.read("language-page", actor="alice")
            receipt = await queue.request("language-page", dedupe_key="crash", force=True)
            await kill_at_boundary(engine, scope, clock, tmp_path, "derived_" + boundary)
            committed = boundary == "after_commit"
            fixed = await queue.status(receipt["target_id"], actor="alice")
            assert fixed["complete"] == committed
            async with engine.repository.unit_of_work() as uow:
                head = await uow.derived_get(scope, "head", "language-page")
                assert (head["revision_id"] != before["revision_id"]) == committed
                blocks = await uow.derived_records(scope, "page_block")
                assert len(blocks) == 1 + int(committed)
                header = await uow.derived_get(scope, "revision_header", head["audit_revision_id"])
                assert digest(header) == head["input_header_sha256"]
            if not committed:
                clock[0] += timedelta(seconds=6)
                lease = await queue.claim("recover", lease_seconds=60)
                await service.apply(lease.task)
                await queue.complete(lease)
            assert (await service.pages.status(receipt["target_id"], actor="alice"))["page_ready"]

    asyncio.run(run())


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_readonly_sdk_wire_final_guard_and_capabilities(store, transport, monkeypatch):
    async def run():
        import agent_memory_sdk as sdk

        async with store() as (engine, kernel, scope, clock):
            service, _, _, receipt = await ready(engine, kernel, scope, clock)
            context = MCPRequestContext(scope, actor="alice")
            original, calls = service.pages.read, []

            async def intervening(*args, **kwargs):
                view = await original(*args, **kwargs)
                calls.append(view)
                if len(calls) == 1:
                    await service.grant(ProcessingGrant(
                        source_id(scope, "1"), ("alice",), revoked=True
                    ), expected_version=1)
                return view

            async def exercise(client):
                assert (await client.page_capabilities())["rebuild_modes"] == ["full"]
                assert (await client.derived_capabilities())["pages"]
                assert (await client.page_status(receipt["target_id"]))["page_ready"]
                monkeypatch.setattr(service.pages, "read", intervening)
                assert (await client.page_context("language-page"))["pages"] == []
                assert len(calls) == 2 and calls[0]["state"] == "ready"
                with pytest.raises(sdk.MemoryClientError):
                    await client.derived_read("language-page")
                with pytest.raises(DerivedError, match="unsupported_derived_operation"):
                    await service.call("page_register", {}, context)
                with pytest.raises(DerivedError, match="invalid_page_request"):
                    await service.call("page_read", {"page_id": "language-page", "valid_at": "x"},
                                       context)

            if transport == "embedded":
                await exercise(sdk.EmbeddedMemoryClient(kernel, context, derived=service))
            else:
                import agent_memory_mcp as mcp

                server = mcp.create_server(kernel, mcp.StaticIdentityResolver(context),
                                           derived=service)
                async with sdk.MCPMemoryClient(server) as client:
                    await exercise(client)

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["kind", "page_parent", "scope", "history", "old_backend"])
def test_explicit_contract_boundaries_and_no_new_scheduler(store, problem, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _ = await ready(engine, kernel, scope, clock)
            if problem == "kind":
                invalid = replace(page(scope), id="language")
            elif problem == "page_parent":
                invalid = replace(page(scope), id="next-page", parent_facets=("language-page",))
            elif problem == "scope":
                invalid = replace(page(scope), scenario=ScenarioDefinition(
                    "other", replace(scope, session_id="other"), "alice", "Other scope"
                ))
            elif problem == "history":
                service.history_mode = "published-point/1"
                invalid = page(scope)
            else:
                cls = type(engine.repository.unit_of_work())
                monkeypatch.setattr(cls, "derived_page_contract", None)
                context = MCPRequestContext(scope, actor="alice")
                assert not (await service.call("page_capabilities", {}, context))["enabled"]
                invalid = page(scope)
            with pytest.raises(DerivedError):
                await service.register_page(invalid)
            if problem == "page_parent":
                with pytest.raises(DerivedError, match="derived_parent_page_unsupported"):
                    await service.register(definition("obs-child", ("language-page",)))
            assert isinstance(queue, FacetRefreshQueue)

    asyncio.run(run())


@pytest.mark.parametrize("change", ["revoke", "erase"])
def test_independent_connection_page_publication_serializes_with_privacy_change(
    store, change, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _ = await ready(engine, kernel, scope, clock)
            await queue.request("language-page", dedupe_key="race", force=True)
            lease = await queue.claim("race", lease_seconds=60)
            pg = hasattr(engine.repository, "pool")
            if pg:
                from agent_memory_postgres.repository import PostgresMemoryRepository

                other = PostgresMemoryRepository.from_dsn(engine.repository.pool.conninfo,
                                                          max_size=2)
            else:
                from agent_memory.sqlite import SQLiteMemoryRepository

                other = SQLiteMemoryRepository(engine.repository._path)
            await other.initialize()
            clone = ObservationService(other, scope, base.POLICY, clock=lambda: clock[0])
            entered, release, changing = asyncio.Event(), asyncio.Event(), asyncio.Event()
            cls, original = type(engine.repository.unit_of_work()), None
            original = cls.derived_put

            async def paused(self, *args):
                await original(self, *args)
                if self._repository is engine.repository and args[1] == "page_block":
                    entered.set()
                    await release.wait()

            async def commit():
                if change == "revoke":
                    return await clone.grant(ProcessingGrant(
                        source_id(scope, "1"), ("alice",), revoked=True
                    ), expected_version=1)
                from agent_memory.kernel import MemoryKernel
                from agent_memory.providers import (
                    MetadataClaimExtractor,
                    ReciprocalRankFusionReranker,
                    TrustedMemoryPolicy,
                )

                other_kernel = MemoryKernel(other, MetadataClaimExtractor(), TrustedMemoryPolicy(),
                                            ReciprocalRankFusionReranker())
                return await other_kernel.forget(ForgetRequest(
                    scope, (source_id(scope, "1"),), mode=ForgetMode.ERASE
                ))

            async def privacy_change():
                if pg:
                    changing.set()
                    return await commit()
                loop = asyncio.get_running_loop()

                def independent_thread():
                    loop.call_soon_threadsafe(changing.set)
                    return asyncio.run(commit())

                return await asyncio.to_thread(independent_thread)

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "derived_put", paused)
                    publication = asyncio.create_task(service.apply(lease.task))
                    await asyncio.wait_for(entered.wait(), 10)
                    privacy = asyncio.create_task(privacy_change())
                    await asyncio.wait_for(changing.wait(), 10)
                    await asyncio.sleep(0.03)
                    assert not privacy.done()
                    release.set()
                    await asyncio.wait_for(publication, 10)
                    await asyncio.wait_for(privacy, 10)
                assert (await clone.pages.read("language-page", actor="alice"))["body"] is None
                if change == "erase":
                    async with other.unit_of_work() as uow:
                        assert all(r["payload"]["state"] == "erased"
                                   for r in await uow.derived_records(scope, "page_block"))
            finally:
                release.set()
                if pg:
                    await other.close()

    asyncio.run(run())


@pytest.mark.parametrize("change", ["query", "authority", "expiry", "readers"])
def test_page_current_control_changes_block_delivery(store, change):
    async def run():
        from test_derived_controls import configured

        async with store() as (engine, kernel, scope, clock):
            if change == "readers":
                service, _, _, _ = await ready(engine, kernel, scope, clock)
                await service.register(FacetDefinition("language", "alice", readers=("bob",)),
                                       expected_generation=1)
                assert (await service.pages.read("language-page", actor="alice"))["body"] is None
                return
            service, queue, _, authority, query, _ = await configured(engine, kernel, scope, clock)
            await build(queue)
            await service.register_page(page(scope, authority_id=authority.id))
            await publish(service, queue, "language-page")
            if change == "query":
                await service.register_query(replace(query, version="2"), expected_generation=1)
            elif change == "authority":
                await service.set_authority(replace(authority, revoked=True), expected_version=1)
            elif change == "expiry":
                clock[0] = authority.expires_at
            if change in {"authority", "expiry"}:
                with pytest.raises(DerivedError):
                    await service.pages.read("language-page", actor="alice")
            else:
                assert (await service.pages.read("language-page", actor="alice"))["body"] is None

    asyncio.run(run())


def test_multiple_parents_bind_all_generation_inputs_to_every_block(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            await service.register(definition("parent-view", ("language",)))
            await publish(service, queue, "parent-view")
            await service.register_page(page(scope, ("language", "parent-view")))
            await publish(service, queue, "language-page")
            before = await service.pages.read("language-page", actor="alice")
            await service.register(replace(definition("parent-view", ("language",)), version="2"),
                                   expected_generation=1)
            await publish(service, queue, "parent-view", "update")
            await publish(service, queue, "language-page", "update")
            after = await service.pages.read("language-page", actor="alice")
            old, new = before["body"]["blocks"][0], after["body"]["blocks"][0]
            assert old["body"] == new["body"] and old["block_id"] == new["block_id"]
            assert old["revision_id"] != new["revision_id"]  # the OTHER input changed
            async with engine.repository.unit_of_work() as uow:
                for ref in after["body"]["blocks"]:
                    block = await uow.derived_get(scope, "page_block", ref["revision_id"])
                    assert set(block["manifest"]["parents"]) == {"language", "parent-view"}

    asyncio.run(run())


def test_unbuilt_building_and_failed_page_are_distinct_from_empty(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            await service.register_page(page(scope))
            unbuilt = await service.pages.read("language-page", actor="alice")
            assert unbuilt["reason"] == "not_built"
            receipt = await queue.request("language-page", dedupe_key="failure")
            queue.max_attempts = 1
            lease = await queue.claim("fail", lease_seconds=60)
            assert (await service.pages.read("language-page", actor="alice"))["state"] == "building"
            await queue.fail(lease, DerivedError("page_output_capacity"))
            assert (await service.pages.read("language-page", actor="alice"))["state"] == "failed"
            status = await service.pages.status(receipt["target_id"], actor="alice")
            assert not status["page_complete"]

    asyncio.run(run())


def test_page_contract_validation_and_output_bound():
    from agent_memory.derived.pages import compose_page

    scope = base.MemoryScope("pure", user_id="alice")
    spec = page(scope)
    for values in ({"parent_facets": ()}, {"readers": ("alice", "alice")},
                   {"parent_facets": tuple(str(i) for i in range(5))},
                   {"template_version": "llm/1"}):
        with pytest.raises(DerivedError):
            replace(spec, **values)
    with pytest.raises(DerivedError):
        ScenarioDefinition("scenario", scope, "alice", "x" * 257)
    with pytest.raises(DerivedError):
        PageBlockRevision("x", "block", spec.id, {}, "x", {}, "x")
    snapshot = dict(definition={"spec": spec.payload()}, manifest={}, parents={
        "language": dict(id="parent", state="ready", body={"blocks": ["x" * 32768]})
    })
    with pytest.raises(DerivedError, match="page_output_capacity"):
        compose_page(snapshot, scope)
    broken = deepcopy(snapshot)
    broken["parents"] = {}
    with pytest.raises(DerivedError):
        compose_page(broken, scope)
