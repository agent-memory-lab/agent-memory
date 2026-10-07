"""Actual parent revisions, transitive access and erasure on both real backends."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_observations import build, setup
from test_durable_purge import envelope, source_id

from agent_memory.derived import DerivedError, FacetDefinition, ProcessingGrant
from agent_memory.derived.model import FacetRefreshUnit, digest
from agent_memory.domain import ForgetMode, ForgetRequest

store = base.store


def definition(key, parents, **changes):
    return FacetDefinition(
        key, "alice", template_version="locale-parents/1", parent_facets=tuple(parents), **changes
    )


async def publish(service, queue, key, dedupe="first", **changes):
    receipt = await queue.request(key, dedupe_key=key + ":" + dedupe, **changes)
    for _ in range(10):
        lease = await queue.claim("graph", lease_seconds=60)
        if lease is None:
            break
        await service.apply(lease.task)
        await queue.complete(lease)
        if lease.task.payload["unit"]["facet_id"] == key:
            return receipt
    pytest.fail("requested graph facet was not scheduled")


async def chain(engine, kernel, scope, clock, *, inputs=2):
    service, queue, capture = await setup(engine, kernel, scope, clock, inputs=inputs)
    await build(queue)
    await service.register(definition("scenario", ("language",)))
    await publish(service, queue, "scenario")
    await service.register(definition("core", ("scenario",)))
    await publish(service, queue, "core")
    return service, queue, capture


@pytest.mark.parametrize("inputs", [0, 1, 2])
def test_fixed_actual_parent_manifest_edges_and_preserved_blocks(store, inputs):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await chain(engine, kernel, scope, clock, inputs=inputs)
            view = await service.read("core", actor="alice")
            assert view["state"] == ("ready" if inputs else "empty")
            async with engine.repository.unit_of_work() as uow:
                head = await uow.derived_get(scope, "head", "core")
                revision = await uow.derived_get(scope, "revision", head["audit_revision_id"])
                unit = revision["unit"]
                assert unit["schema"] == "facet-refresh-unit/3"
                assert FacetRefreshUnit(**unit).payload() == unit
                manifest = revision["manifest"]
                assert manifest["schema"] == "derived-input-manifest/2"
                assert manifest["parents"] == unit["parents"]
                assert set(manifest["lineage"]) == {"scenario", "language"}
                assert manifest["atoms"] == manifest["sources"] == {}
                assert manifest["authorization"]["readers"] == ["alice"]
                parent_id = unit["parents"]["scenario"]["revision_id"]
                assert revision["id"] in await uow.derived_reverse(scope, "derived:" + parent_id)
                for key, proof in manifest["lineage"].items():
                    header = await uow.derived_get(scope, "revision_header", proof["revision_id"])
                    assert digest(header) == proof["sha256"]
                    assert header["facet_id"] == key and "body" not in header
                if inputs:
                    parent = await service._read(uow, "scenario", "alice", "agent_context")
                    assert view["body"]["blocks"][0]["blocks"] == parent["body"]["blocks"]
                else:
                    assert revision["parents"] == ["derived:" + parent_id]

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["private", "revoked", "expired", "version", "missing"])
@pytest.mark.parametrize("boundary", ["snapshot", "delivery"])
def test_uncited_transitive_source_denied_before_any_body(store, problem, boundary, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await chain(engine, kernel, scope, clock)
            if boundary == "snapshot":
                await queue.request("core", dedupe_key="fresh", force=True)
                lease = await queue.claim("guard", lease_seconds=60)
                assert lease.task.payload["unit"]["facet_id"] == "core"
            # Source 1 is not the selected fact candidate but was processed.
            key = source_id(scope, "1")
            async with engine.repository.unit_of_work() as uow:
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
            calls = []
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_get

            async def tracked(self, scope, kind, identity):
                if kind == "revision":
                    calls.append(kind)
                return await original(self, scope, kind, identity)

            async def forbidden(self, *args):
                calls.append("source_or_atom")
                pytest.fail("body fetched before complete lineage authorization")

            monkeypatch.setattr(cls, "derived_get", tracked)
            monkeypatch.setattr(cls, "get_source_event", forbidden)
            monkeypatch.setattr(cls, "get_admission_record", forbidden)
            if boundary == "snapshot":
                with pytest.raises(DerivedError):
                    await service.snapshot(lease.task)
            else:
                assert (await service.read("core", actor="alice"))["body"] is None
            assert calls == []

    asyncio.run(run())


@pytest.mark.parametrize("change", ["head", "source", "grant", "definition", "time"])
def test_parent_changes_block_fixed_publish_and_dirty_descendants(store, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, capture = await chain(engine, kernel, scope, clock)
            await queue.request("core", dedupe_key="pending", force=True)
            lease = await queue.claim("before", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            prepared = service.prepare(snapshot)
            before = snapshot["manifest"]["lineage"]["language"]["revision_id"]
            if change == "head":
                await publish(service, queue, "language", "replace", force=True)
            elif change == "source":
                _, session, client, _, _, worker, _, _ = capture
                await client.durable_append(envelope(scope, "3", clock), session, 3)
                assert await worker.run_once()
                await service.grant(ProcessingGrant(source_id(scope, "3"), ("alice",)))
            elif change == "grant":
                await service.grant(
                    ProcessingGrant(source_id(scope, "1"), ("alice",), revoked=True),
                    expected_version=1,
                )
            elif change == "definition":
                await service.register(
                    replace(FacetDefinition("language", "alice"), version="2"),
                    expected_generation=1,
                )
            else:
                async with engine.repository.unit_of_work() as uow:
                    head = await uow.derived_get(scope, "head", "language")
                    head["next_transition_at"] = clock[0].isoformat()
                    await uow.derived_put(scope, "head", "language", head)
            assert (await service.read("core", actor="alice"))["body"] is None
            with pytest.raises(DerivedError):
                await service.publish(lease.task, snapshot, prepared)
            assert snapshot["manifest"]["lineage"]["language"]["revision_id"] == before
            if change != "time":
                async with engine.repository.unit_of_work() as uow:
                    assert (await uow.derived_get(scope, "definition", "scenario"))["dirty"]
                    assert (await uow.derived_get(scope, "definition", "core"))["dirty"]

    asyncio.run(run())


@pytest.mark.parametrize("problem", ["unknown", "self", "cycle", "depth", "private", "scope"])
def test_registration_rejects_invalid_graph_atomically(store, problem):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await chain(engine, kernel, scope, clock)
            key, expected = "invalid", 0
            if problem == "unknown":
                new = definition(key, ("absent",))
            elif problem == "self":
                new = definition(key, (key,))
            elif problem == "cycle":
                key, expected = "language", 1
                new = definition(key, ("core",))
            elif problem == "depth":
                await service.register(definition("four", ("core",)))
                new = definition(key, ("four",))
            elif problem == "private":
                new = definition(key, ("language",), readers=("alice", "bob"))
            else:
                new = definition(key, ("language",), purpose="other")
            async with engine.repository.unit_of_work() as uow:
                old = await uow.derived_get(scope, "definition", key)
            with pytest.raises(DerivedError):
                await service.register(new, expected_generation=expected)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "definition", key) == old

    asyncio.run(run())


@pytest.mark.parametrize("mutated", ["body", "parents", "lineage", "authorization", "manifest"])
def test_caller_cannot_forge_actual_parent_processing_inputs(store, mutated):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await chain(engine, kernel, scope, clock)
            await queue.request("core", dedupe_key="forge", force=True)
            lease = await queue.claim("forge", lease_seconds=60)
            snapshot = deepcopy(await service.snapshot(lease.task))
            if mutated == "body":
                snapshot["parents"]["scenario"]["body"]["blocks"] = []
            elif mutated == "parents":
                snapshot["parents"]["scenario"]["id"] = "fake"
            elif mutated == "lineage":
                snapshot["manifest"]["lineage"].pop("language")
            elif mutated == "authorization":
                snapshot["manifest"]["authorization"]["sensitivity"] = "public"
            else:
                snapshot["parent_headers"]["language"]["manifest"]["sources"] = {}
            with pytest.raises(DerivedError):
                await service.publish(lease.task, snapshot, service.prepare(snapshot))

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["object", "scope", "derived"])
def test_transitive_physical_erase_and_actual_old_backup_replay(store, tmp_path, mode):
    async def run():
        from test_purge_restore import backup_copy, replay, restorer

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await chain(engine, kernel, scope, clock)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                async with engine.repository.unit_of_work() as uow:
                    root = await uow.derived_get(scope, "head", "language")
                ids = () if mode == "scope" else (
                    root["revision_id"] if mode == "derived" else source_id(scope, "1"),
                )
                await kernel.forget(ForgetRequest(
                    scope, ids, all_in_scope=mode == "scope", mode=ForgetMode.ERASE
                ))
                journal = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), journal)
                for repo in (engine.repository, backup):
                    clone = type(service)(repo, scope, base.POLICY, clock=lambda: clock[0])
                    if mode == "scope":
                        with pytest.raises(DerivedError):
                            await clone.read("core", actor="alice")
                    else:
                        assert (await clone.read("core", actor="alice"))["body"] is None
                    async with repo.unit_of_work() as uow:
                        for kind in ("revision", "revision_header"):
                            rows = await uow.derived_records(scope, kind)
                            assert rows and all(r["payload"]["state"] == "erased" for r in rows)
                            assert all("manifest" not in r["payload"] and "body" not in
                                       r["payload"] for r in rows)
                        assert not await uow.derived_reverse(
                            scope, "derived:" + root["revision_id"]
                        )

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_actual_sigkill_parent_publication_and_descendant_outbox_atomic(store, tmp_path, boundary):
    async def run():
        from test_durable_process_recovery import kill_at_boundary

        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await chain(engine, kernel, scope, clock)
            receipt = await queue.request("language", dedupe_key="crash-root", force=True)
            await kill_at_boundary(engine, scope, clock, tmp_path, "derived_" + boundary)
            committed = boundary == "after_commit"
            status = await queue.status(receipt["target_id"], actor="alice")
            assert status["complete"] == committed
            async with engine.repository.unit_of_work() as uow:
                for key in ("scenario", "core"):
                    assert (await uow.derived_get(scope, "definition", key))["dirty"] == committed
                root = await uow.derived_get(scope, "head", "language")
                header = await uow.derived_get(scope, "revision_header", root["audit_revision_id"])
                assert digest(header) == root["input_header_sha256"]
            if not committed:
                clock[0] += timedelta(seconds=6)
                lease = await queue.claim("recover", lease_seconds=60)
                await service.apply(lease.task)
            assert (await service.read("core", actor="alice"))["body"] is None

    asyncio.run(run())


def test_legacy_header_requires_explicit_republication_and_readonly_sdk(store):
    async def run():
        from agent_memory_sdk import EmbeddedMemoryClient

        from agent_memory.mcp import MCPRequestContext

        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            async with engine.repository.unit_of_work() as uow:
                head = await uow.derived_get(scope, "head", "language")
                head.pop("input_header_sha256")
                await uow.derived_put(scope, "head", "language", head)
            await service.register(definition("scenario", ("language",)))
            with pytest.raises(DerivedError, match="derived_parent_proof_missing"):
                await queue.request("scenario", dedupe_key="legacy")
            await publish(service, queue, "language", "republish", force=True)
            await publish(service, queue, "scenario", "new")
            client = EmbeddedMemoryClient(kernel, MCPRequestContext(scope, actor="alice"),
                                          derived=service)
            capabilities = await client.derived_capabilities()
            assert capabilities["derived_parents"] and capabilities["readonly"]
            assert capabilities["derived_parent_history"] is False
            assert (await client.derived_context("scenario"))["observations"]
            with pytest.raises(DerivedError, match="unsupported_derived_operation"):
                await service.call("register", {}, MCPRequestContext(scope, actor="alice"))
            await service.grant(
                ProcessingGrant(source_id(scope, "1"), ("alice",), revoked=True), expected_version=1
            )
            assert (await client.derived_context("scenario"))["observations"] == []

    asyncio.run(run())


def test_dirty_graph_refreshes_ancestors_before_children_and_keeps_old_receipts(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, capture = await chain(engine, kernel, scope, clock)
            old = await queue.request("core", dedupe_key="before-change")
            _, session, client, _, _, worker, _, _ = capture
            await client.durable_append(envelope(scope, "3", clock), session, 3)
            assert await worker.run_once()
            await service.grant(ProcessingGrant(source_id(scope, "3"), ("alice",)))
            order = []
            for _ in range(3):
                lease = await queue.claim("ordered", lease_seconds=60)
                assert lease
                order.append(lease.task.payload["unit"]["facet_id"])
                await service.apply(lease.task)
                await queue.complete(lease)
            assert order == ["language", "scenario", "core"]
            assert await queue.claim("done", lease_seconds=60) is None
            assert (await service.read("core", actor="alice"))["state"] == "ready"
            assert (await queue.status(old["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


@pytest.mark.parametrize("change", ["query", "authority", "readers", "expiry"])
def test_control_changes_propagate_without_losing_current_permissions(store, change):
    async def run():
        from test_derived_controls import configured

        async with store() as (engine, kernel, scope, clock):
            if change == "readers":
                service, _, _ = await chain(engine, kernel, scope, clock)
                await service.register(
                    FacetDefinition("language", "alice", readers=("bob",)), expected_generation=1
                )
                assert (await service.read("core", actor="alice"))["body"] is None
                return
            service, queue, _, authority, query, _ = await configured(
                engine, kernel, scope, clock, inputs=2
            )
            await build(queue)
            await service.register(definition(
                "scenario", ("language",), authority_id=authority.id
            ))
            await publish(service, queue, "scenario")
            await service.register(definition("core", ("scenario",), authority_id=authority.id))
            await publish(service, queue, "core")
            if change == "query":
                await service.register_query(replace(query, version="2"), expected_generation=1)
            elif change == "authority":
                await service.set_authority(replace(authority, revoked=True), expected_version=1)
            else:
                clock[0] = authority.expires_at
            if change in {"authority", "expiry"}:
                with pytest.raises(DerivedError, match="derived_authority"):
                    await service.read("core", actor="alice")
            else:
                assert (await service.read("core", actor="alice"))["body"] is None

    asyncio.run(run())


def test_independent_connection_revoke_serializes_with_child_publication(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await chain(engine, kernel, scope, clock)
            await queue.request("core", dedupe_key="race", force=True)
            lease = await queue.claim("race", lease_seconds=60)
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
            clone = type(service)(other, scope, base.POLICY, clock=lambda: clock[0])
            entered, release, revoking = asyncio.Event(), asyncio.Event(), asyncio.Event()
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_put

            async def paused(self, *args):
                await original(self, *args)
                if self._repository is engine.repository and args[1] == "revision_header":
                    entered.set()
                    await release.wait()

            async def revoke():
                async def commit():
                    return await clone.grant(
                        ProcessingGrant(source_id(scope, "1"), ("alice",), revoked=True),
                        expected_version=1,
                    )

                if pg:
                    revoking.set()
                    return await commit()
                loop = asyncio.get_running_loop()

                def independent_thread():
                    # SQLite's synchronous lock wait cannot run on the loop
                    # whose task holds that lock. Exercise independent writers.
                    loop.call_soon_threadsafe(revoking.set)
                    return asyncio.run(commit())

                return await asyncio.to_thread(independent_thread)

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "derived_put", paused)
                    publication = asyncio.create_task(service.apply(lease.task))
                    await asyncio.wait_for(entered.wait(), 10)
                    revocation = asyncio.create_task(revoke())
                    await asyncio.wait_for(revoking.wait(), 10)
                    await asyncio.sleep(0.03)
                    assert not revocation.done()
                    release.set()
                    await asyncio.wait_for(publication, 10)
                    await asyncio.wait_for(revocation, 10)
                assert (await service.read("core", actor="alice"))["body"] is None
                async with other.unit_of_work() as uow:
                    assert (await uow.derived_get(scope, "definition", "core"))["dirty"]
                    head = await uow.derived_get(scope, "head", "core")
                    header = await uow.derived_get(
                        scope, "revision_header", head["audit_revision_id"]
                    )
                    assert digest(header) == head["input_header_sha256"]
            finally:
                release.set()
                if pg:
                    await other.close()

    asyncio.run(run())


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_actual_sigkill_child_fixed_manifest_header_head_and_certificate(store, tmp_path, boundary):
    async def run():
        from test_durable_process_recovery import kill_at_boundary

        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await chain(engine, kernel, scope, clock)
            async with engine.repository.unit_of_work() as uow:
                old = await uow.derived_get(scope, "head", "core")
            receipt = await queue.request("core", dedupe_key="crash-child", force=True)
            await kill_at_boundary(engine, scope, clock, tmp_path, "derived_" + boundary)
            committed = boundary == "after_commit"
            status = await queue.status(receipt["target_id"], actor="alice")
            assert status["complete"] == committed
            async with engine.repository.unit_of_work() as uow:
                head = await uow.derived_get(scope, "head", "core")
                assert (head != old) == committed
                header = await uow.derived_get(scope, "revision_header", head["audit_revision_id"])
                assert digest(header) == head["input_header_sha256"]
                assert header["manifest"]["parents"] == head["unit"]["parents"]
            if not committed:
                clock[0] += timedelta(seconds=6)
                lease = await queue.claim("recover", lease_seconds=60)
                await service.apply(lease.task)
                await queue.complete(lease)
            assert (await service.read("core", actor="alice"))["state"] == "ready"
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]

    asyncio.run(run())


@pytest.mark.parametrize("capacity", ["parents", "nodes", "output"])
def test_explicit_graph_and_output_budgets(capacity):
    from agent_memory.derived.parents import compose_parents, validate_graph

    if capacity == "parents":
        with pytest.raises(DerivedError, match="invalid_derived_parents"):
            definition("view", tuple(str(i) for i in range(5)))
    elif capacity == "nodes":
        leaves = {f"l{i}": FacetDefinition(f"l{i}", "alice").payload() for i in range(24)}
        mid = {f"m{i}": definition(f"m{i}", tuple(f"l{4*i+j}" for j in range(4))).payload()
               for i in range(6)}
        high = {"h1": definition("h1", ("m0", "m1", "m2")).payload(),
                "h2": definition("h2", ("m3", "m4", "m5")).payload()}
        root = definition("view", ("h1", "h2")).payload()
        with pytest.raises(DerivedError, match="derived_parent_capacity"):
            validate_graph({**leaves, **mid, **high}, root)
    else:
        spec = definition("view", ("large",)).payload()
        with pytest.raises(DerivedError, match="derived_output_capacity"):
            compose_parents(spec, {"large": dict(id="fixed", state="ready",
                                                body={"blocks": [{"value": "x" * 32768}]})})


def test_multiple_actual_parents_keep_provenance_without_new_independent_evidence(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock, inputs=2)
            await build(queue)
            for key in ("mirror", "third", "fourth"):
                await service.register(FacetDefinition(key, "alice"))
                await publish(service, queue, key)
            parents = ("language", "mirror", "third", "fourth")
            await service.register(definition("view", parents))
            await publish(service, queue, "view")
            view = await service.read("view", actor="alice")
            assert [b["facet_id"] for b in view["body"]["blocks"]] == list(parents)
            assert all(b["kind"] == "derived_view" for b in view["body"]["blocks"])
            assert all(b["blocks"] == view["body"]["blocks"][0]["blocks"]
                       for b in view["body"]["blocks"])
            async with engine.repository.unit_of_work() as uow:
                revision = await uow.derived_get(scope, "revision", view["revision_id"])
                assert set(revision["manifest"]["lineage"]) == set(parents)
                assert len(revision["parents"]) == 4

    asyncio.run(run())


@pytest.mark.parametrize("parent_kind", ["qualified", "history"])
def test_parent_qualification_or_history_cannot_be_silently_flattened(store, parent_kind):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            if parent_kind == "qualified":
                from test_derived_contextual import setup as qualified_setup

                service, _, _, _ = await qualified_setup(engine, kernel, scope, clock)
            else:
                from test_derived_history import configured

                service, _, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            from agent_memory.derived import ObservationService

            current = ObservationService(
                engine.repository, scope, base.POLICY, clock=lambda: clock[0],
                authority_id=service.authority_id,
                authority_min_version=service.authority_min_version,
            )
            new = definition("unsupported", ("language",), authority_id=service.authority_id)
            with pytest.raises(DerivedError, match="derived_parent_template_unsupported"):
                await current.register(new)

    asyncio.run(run())


def test_final_sdk_delivery_rechecks_entire_processing_lineage(store, monkeypatch):
    async def run():
        from agent_memory_sdk import EmbeddedMemoryClient

        from agent_memory.mcp import MCPRequestContext

        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await chain(engine, kernel, scope, clock)
            original, calls = service.read, []

            async def changed(*args, **kwargs):
                view = await original(*args, **kwargs)
                calls.append(view)
                if len(calls) == 1:
                    await service.grant(
                        ProcessingGrant(source_id(scope, "1"), ("alice",), revoked=True),
                        expected_version=1,
                    )
                return view

            monkeypatch.setattr(service, "read", changed)
            client = EmbeddedMemoryClient(
                kernel, MCPRequestContext(scope, actor="alice"), derived=service
            )
            result = await client.derived_context("core")
            assert calls[0]["state"] == "ready" and len(calls) == 2
            assert result["observations"] == []

    asyncio.run(run())


def test_backend_must_attest_transitive_erasure_contract_before_enabling_parents(
    store, monkeypatch
):
    async def run():
        from agent_memory.mcp import MCPRequestContext

        async with store() as (engine, kernel, scope, clock):
            cls = type(engine.repository.unit_of_work())
            monkeypatch.setattr(cls, "derived_parent_contract", "older-backend")
            service, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            assert (await service.read("language", actor="alice"))["state"] == "ready"
            capabilities = await service.call("capabilities", {}, MCPRequestContext(scope))
            assert capabilities["derived_parents"] is False
            with pytest.raises(DerivedError, match="derived_parent_backend_unsupported"):
                await service.register(definition("scenario", ("language",)))
            async with engine.repository.unit_of_work() as uow:
                assert not await uow.derived_records(scope, "revision_header")
                head = await uow.derived_get(scope, "head", "language")
                assert "input_header_sha256" not in head

    asyncio.run(run())


def test_input_byte_budget_is_checked_from_headers_before_any_body(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await chain(engine, kernel, scope, clock)
            async with engine.repository.unit_of_work() as uow:
                head = await uow.derived_get(scope, "head", "language")
                header = await uow.derived_get(scope, "revision_header", head["audit_revision_id"])
                header["body_bytes"] = 262145
                head["input_header_sha256"] = digest(header)
                await uow.derived_put(scope, "revision_header", header["id"], header)
                await uow.derived_put(scope, "head", "language", head)
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_get

            async def metadata_only(self, scope, kind, identity):
                assert kind != "revision", "overflow must reject before revision body"
                return await original(self, scope, kind, identity)

            monkeypatch.setattr(cls, "derived_get", metadata_only)
            with pytest.raises(DerivedError, match="derived_parent_input_capacity"):
                await queue.request("scenario", dedupe_key="capacity", force=True)

    asyncio.run(run())
