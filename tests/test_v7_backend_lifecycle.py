"""V7-B1 transaction ownership and exact-scope routing lifecycle evidence."""

import asyncio
from copy import deepcopy
from dataclasses import replace

import pytest
import test_atom_admission as base

from agent_memory.derived.subscriptions import (
    HEADER_SCHEMA,
    candidate_owner,
    ensure_index,
    source_key,
    subscription_owner,
)
from agent_memory.domain import ForgetMode, ForgetRequest, MemoryEvent, canonical_json

store = base.store


async def seed(repository, scope, *, suffix="one"):
    event = base.source(scope, identity="event-" + suffix)
    payload = {"draft": {"value": "original"}, "source_event_ids": [event.id]}
    async with repository.unit_of_work() as uow:
        await uow.append_event(event)
        await uow.save_admission_record(
            scope, "candidate-" + suffix, event.id, "slot-" + suffix, payload, 0
        )
    return event, payload


async def route_rows(uow, scope):
    if hasattr(uow._repository, "pool"):
        cursor = await uow.connection.execute(
            "SELECT revision_id,parent_id,edge_kind FROM agent_memory_derived_dependencies "
            "WHERE partition_key=%s AND revision_id LIKE 'route:%%' ORDER BY revision_id,parent_id",
            (scope.partition_key(),),
        )
        return tuple(dict(row) for row in await cursor.fetchall())
    return tuple(
        dict(row)
        for row in uow.connection.execute(
            "SELECT revision_id,parent_id,edge_kind FROM derived_dependencies "
            "WHERE partition_key=? AND revision_id LIKE 'route:%' ORDER BY revision_id,parent_id",
            (scope.partition_key(),),
        ).fetchall()
    )


def test_headers_replace_source_routes_and_preserve_admission_identity(store):
    async def run():
        async with store() as (engine, _, scope, _):
            repository = engine.repository
            first, payload = await seed(repository, scope)
            second = base.source(scope, identity="event-two")
            async with repository.unit_of_work() as uow:
                await uow.append_event(second)
                changed = deepcopy(payload)
                changed["source_event_ids"] = [second.id]
                await uow.save_admission_record(
                    scope, "candidate-one", first.id, "slot-one", changed, 1
                )
                header = await uow.derived_header(scope, "candidate-one")
                assert header["schema"] == HEADER_SCHEMA and header["generation"] == 2
                assert header["source_ids"] == sorted([first.id, second.id])
                assert await uow.derived_reverse(scope, source_key(second.id)) == (
                    candidate_owner("candidate-one"),
                )
                await uow.save_admission_record(
                    scope, "candidate-one", first.id, "slot-one", payload, 2
                )
                assert await uow.derived_reverse(scope, source_key(second.id)) == ()
                assert await uow.derived_headers(scope) == (
                    await uow.derived_header(scope, "candidate-one"),
                )
            for identity in ("event", "slot", "scope"):
                async with repository.unit_of_work() as uow:
                    with pytest.raises(ValueError):
                        await uow.save_admission_record(
                            replace(scope, session_id="different")
                            if identity == "scope"
                            else scope,
                            "candidate-one",
                            second.id if identity == "event" else first.id,
                            "new-slot" if identity == "slot" else "slot-one",
                            payload,
                            3,
                        )

    asyncio.run(run())


@pytest.mark.parametrize("restore", [False, True])
@pytest.mark.parametrize("mode", [ForgetMode.ERASE, ForgetMode.ARCHIVE])
def test_deletion_and_restore_scrub_routing_after_reconciliation_in_exact_scope(
    store, restore, mode
):
    async def run():
        async with store() as (engine, kernel, scope, _):
            repository = engine.repository
            event, _ = await seed(repository, scope)
            # This survivor is rewritten by reconciliation; it must not recreate routes.
            survivor, _ = await seed(repository, scope, suffix="survivor")
            other = replace(scope, session_id="other")
            await seed(repository, other, suffix="other")
            async with repository.unit_of_work() as uow:
                for target in (scope, other):
                    await uow.derived_put(target, "subscription", "view", {"keys": ["sensitive"]})
                    await uow.derived_edges(
                        target, subscription_owner("view"), [("query", "route:sensitive-selector")]
                    )
                    await uow.derived_edges(
                        target, "route:future-owner", [("query", "route:sensitive-selector")]
                    )
                old_index = await uow.derived_get(scope, "subscription_index", "scope")
                old_barrier = await uow.derived_get(scope, "barrier", "route:fallback")
                other_routes = await route_rows(uow, other)
                other_index = await uow.derived_get(other, "subscription_index", "scope")
            request = ForgetRequest(scope, (event.id,), mode=mode)
            if restore:
                async with repository.unit_of_work() as uow:
                    await uow.forget_for_restore(request)
            else:
                await kernel.forget(request)
            async with repository.unit_of_work() as uow:
                assert await route_rows(uow, scope) == ()
                assert await uow.derived_records(scope, "subscription") == ()
                index = await uow.derived_get(scope, "subscription_index", "scope")
                assert index == {
                    "schema": "derived-subscription-index/1",
                    "state": "needs_backfill",
                    "generation": old_index["generation"] + 1,
                }
                barrier = await uow.derived_get(scope, "barrier", "route:fallback")
                assert barrier["generation"] == old_barrier["generation"] + 1
                assert await uow.derived_header(scope, "candidate-one") is None
                assert await uow.derived_header(scope, "candidate-survivor") is not None
                assert await route_rows(uow, other) == other_routes
                assert await uow.derived_get(other, "subscription_index", "scope") == other_index
                assert await uow.derived_get(other, "subscription", "view") is not None
                # The gated rebuild revives only the survivor's non-erased selectors.
                await ensure_index(uow, scope)
                assert await uow.derived_reverse(scope, source_key(event.id)) == ()
                assert await uow.derived_reverse(scope, source_key(survivor.id)) == (
                    candidate_owner("candidate-survivor"),
                )

    asyncio.run(run())


def test_migration_rebuilds_header_from_authoritative_payload_under_gate(store):
    async def run():
        async with store() as (engine, _, scope, _):
            repository = engine.repository
            event, _ = await seed(repository, scope)
            async with repository.unit_of_work() as uow:
                old = await uow.derived_header(scope, "candidate-one")
                old.pop("schema")
                old.pop("generation")
                old["source_ids"] = ["obsolete-untrusted-metadata"]
                if hasattr(repository, "pool"):
                    await uow.connection.execute(
                        "UPDATE agent_memory_derived_atom_headers SET payload_json=%s::jsonb "
                        "WHERE partition_key=%s AND identity=%s",
                        (canonical_json(old), scope.partition_key(), "candidate-one"),
                    )
                else:
                    uow.connection.execute(
                        "UPDATE derived_atom_headers SET payload_json=? "
                        "WHERE partition_key=? AND identity=?",
                        (canonical_json(old), scope.partition_key(), "candidate-one"),
                    )
                await uow.derived_edges(scope, candidate_owner("candidate-one"), ())
                await uow.derived_put(
                    scope,
                    "subscription_index",
                    "scope",
                    {
                        "schema": "derived-subscription-index/1",
                        "state": "needs_backfill",
                        "generation": 1,
                    },
                )
                await ensure_index(uow, scope)
                upgraded = await uow.derived_header(scope, "candidate-one")
                assert upgraded["schema"] == HEADER_SCHEMA
                assert upgraded["generation"] == 1
                assert upgraded["source_ids"] == [event.id]
                assert await uow.derived_reverse(scope, source_key(event.id)) == (
                    candidate_owner("candidate-one"),
                )
                assert (
                    await uow.derived_reverse(scope, source_key("obsolete-untrusted-metadata"))
                    == ()
                )

    asyncio.run(run())


@pytest.mark.parametrize(
    "operation", ["append_event", "retention_head_put", "save_admission_record"]
)
def test_postgres_write_owns_input_before_waiting_on_real_namespace_lock(
    store, operation, monkeypatch
):
    async def run():
        async with store() as (engine, _, scope, _):
            repository = engine.repository
            if not hasattr(repository, "pool"):
                pytest.skip("real asynchronous namespace-lock race is PostgreSQL-specific")
            source = base.source(scope, identity="source")
            async with repository.unit_of_work() as uow:
                await uow.append_event(source)
            payload = {"source_event_ids": [source.id], "draft": {"value": "original"}}
            event = MemoryEvent(scope, "message", "owned", metadata=payload, id="owned-event")
            expected = deepcopy(payload)
            entered = asyncio.Event()
            cls = type(repository.unit_of_work())
            original = cls.lock_admission_scope

            async def notified(self, *args):
                if asyncio.current_task().get_name() == "blocked-input-writer":
                    entered.set()
                await original(self, *args)

            async def write():
                async with repository.unit_of_work() as uow:
                    if operation == "append_event":
                        await uow.append_event(event)
                    elif operation == "retention_head_put":
                        await uow.retention_head_put(scope, "document", "document", payload, 0)
                    else:
                        await uow.save_admission_record(
                            scope, "candidate", source.id, "slot", payload, 0
                        )

            with monkeypatch.context() as patch:
                patch.setattr(cls, "lock_admission_scope", notified)
                async with repository.unit_of_work() as blocker:
                    await blocker.lock_admission_scope(scope)
                    task = asyncio.create_task(write(), name="blocked-input-writer")
                    await asyncio.wait_for(entered.wait(), 10)
                    assert not task.done()
                    payload["draft"]["value"] = "caller-mutation"
                    payload["source_event_ids"].append("missing-after-await")
                await asyncio.wait_for(task, 10)
            async with repository.unit_of_work() as uow:
                if operation == "append_event":
                    persisted = await uow.get_source_event(scope, event.id)
                    assert persisted.metadata == expected
                    assert persisted.content_hash == event.content_hash
                elif operation == "retention_head_put":
                    persisted = await uow.retention_head_get(scope, "document", "document")
                    assert persisted["payload"] == expected
                else:
                    persisted = await uow.get_admission_record(scope, "candidate")
                    assert persisted["payload"] == expected
                    header = await uow.derived_header(scope, "candidate")
                    assert header["source_ids"] == [source.id]
                    assert await uow.derived_reverse(scope, source_key("missing-after-await")) == ()

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["derived_put", "derived_edges"])
def test_postgres_derived_writes_own_mutable_values_before_first_await(
    store, operation, monkeypatch
):
    async def run():
        async with store() as (engine, _, scope, _):
            repository = engine.repository
            if not hasattr(repository, "pool"):
                pytest.skip("asynchronous SQL helper ownership is PostgreSQL-specific")
            from agent_memory_postgres import derived

            method = "put" if operation == "derived_put" else "edges"
            original = getattr(derived, method)
            entered, release = asyncio.Event(), asyncio.Event()
            payload = {"nested": {"value": "original"}}
            values = [["query", "original-parent"]]

            async def paused(*args):
                entered.set()
                await release.wait()
                return await original(*args)

            async def write():
                async with repository.unit_of_work() as uow:
                    if operation == "derived_put":
                        await uow.derived_put(scope, "test", "owned", payload)
                    else:
                        await uow.derived_edges(scope, "owned", values)

            with monkeypatch.context() as patch:
                patch.setattr(derived, method, paused)
                task = asyncio.create_task(write())
                await asyncio.wait_for(entered.wait(), 10)
                payload["nested"]["value"] = "mutated"
                values[0][1] = "mutated-parent"
                release.set()
                await asyncio.wait_for(task, 10)
            async with repository.unit_of_work() as uow:
                if operation == "derived_put":
                    assert await uow.derived_get(scope, "test", "owned") == {
                        "nested": {"value": "original"},
                    }
                else:
                    assert await uow.derived_reverse(scope, "original-parent") == ("owned",)
                    assert await uow.derived_reverse(scope, "mutated-parent") == ()

    asyncio.run(run())


def test_header_census_stops_at_overflow_sentinel_and_selects_scope_fallback(store):
    async def run():
        async with store() as (engine, _, scope, _):
            repository = engine.repository
            async with repository.unit_of_work() as uow:
                payloads = []
                for index in range(4098):
                    identity = f"candidate-{index:04d}"
                    payloads.append(
                        (
                            scope.partition_key(),
                            identity,
                            "slot",
                            canonical_json(
                                {
                                    "schema": HEADER_SCHEMA,
                                    "generation": 1,
                                    "id": identity,
                                    "event_id": identity,
                                    "source_ids": [identity],
                                    "version": 1,
                                    "claim_id": None,
                                    "slot_key": "slot",
                                }
                            ),
                        )
                    )
                if hasattr(repository, "pool"):
                    async with uow.connection.cursor() as cursor:
                        await cursor.executemany(
                            "INSERT INTO agent_memory_derived_atom_headers "
                            "VALUES (%s,%s,%s,%s::jsonb)",
                            payloads,
                        )
                else:
                    uow.connection.executemany(
                        "INSERT INTO derived_atom_headers VALUES (?,?,?,?)", payloads
                    )
                headers = await uow.derived_headers(scope)
                assert len(headers) == 4097
                assert headers[0]["id"] == "candidate-0000"
                assert headers[-1]["id"] == "candidate-4096"
                gate = await ensure_index(uow, scope)
                assert gate["state"] == "ready" and gate["source_mode"] == "scope"
                assert await route_rows(uow, scope) == ()

    asyncio.run(run())


def test_startup_metadata_backfill_cannot_bypass_the_subscription_gate(store):
    async def run():
        async with store() as (engine, _, scope, _):
            repository = engine.repository
            event, _ = await seed(repository, scope)
            async with repository.unit_of_work() as uow:
                await uow.derived_edges(scope, candidate_owner("candidate-one"), ())
                gate = {
                    "schema": "derived-subscription-index/1",
                    "state": "needs_backfill",
                    "generation": 9,
                }
                await uow.derived_put(scope, "subscription_index", "scope", gate)
                if hasattr(repository, "pool"):
                    await uow.connection.execute(
                        "DELETE FROM agent_memory_derived_atom_headers WHERE partition_key=%s",
                        (scope.partition_key(),),
                    )
                else:
                    uow.connection.execute(
                        "DELETE FROM derived_atom_headers WHERE partition_key=?",
                        (scope.partition_key(),),
                    )
            if hasattr(repository, "pool"):
                fresh = type(repository).from_dsn(repository.pool.conninfo, max_size=2)
            else:
                fresh = type(repository)(repository._path)
            try:
                await fresh.initialize()
                async with fresh.unit_of_work() as uow:
                    assert await uow.derived_header(scope, "candidate-one") is not None
                    assert await uow.derived_get(scope, "subscription_index", "scope") == gate
                    assert await route_rows(uow, scope) == ()
                    await ensure_index(uow, scope)
                    assert await uow.derived_reverse(scope, source_key(event.id)) == (
                        candidate_owner("candidate-one"),
                    )
            finally:
                if hasattr(fresh, "close"):
                    await fresh.close()

    asyncio.run(run())


def test_candidate_header_uses_persisted_json_shape_for_tuple_source_membership(store):
    async def run():
        async with store() as (engine, _, scope, _):
            events = [base.source(scope, identity=f"source-{index}") for index in range(3)]
            payload = {
                "draft": {"value": "original"},
                "source_event_ids": (events[1].id,),
                "evidence": ({"source_event_id": events[2].id},),
            }
            async with engine.repository.unit_of_work() as uow:
                for event in events:
                    await uow.append_event(event)
                await uow.save_admission_record(
                    scope, "candidate", events[0].id, "slot", payload, 0
                )
                persisted = await uow.get_admission_record(scope, "candidate")
                assert persisted["payload"]["source_event_ids"] == [events[1].id]
                assert persisted["payload"]["evidence"] == [{"source_event_id": events[2].id}]
                header = await uow.derived_header(scope, "candidate")
                assert header["source_ids"] == sorted(event.id for event in events)
                for event in events:
                    assert await uow.derived_reverse(scope, source_key(event.id)) == (
                        candidate_owner("candidate"),
                    )

    asyncio.run(run())


@pytest.mark.parametrize("missing", ["derived_header", "derived_headers"])
def test_derived_adapter_missing_header_ports_fails_closed(store, missing, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, _):
            from agent_memory.derived import DerivedError
            from agent_memory.derived.service import open_derived

            async with engine.repository.unit_of_work() as uow:
                with monkeypatch.context() as patch:
                    patch.setattr(type(uow), missing, None)
                    with pytest.raises(DerivedError, match="derived_backend_unsupported"):
                        await open_derived(uow, scope)

    asyncio.run(run())


@pytest.mark.parametrize("restore", [False, True])
@pytest.mark.parametrize("all_in_scope", [False, True])
def test_projected_erasure_scrubs_only_actually_affected_scopes(store, restore, all_in_scope):
    async def run():
        from agent_memory.derived import FacetDefinition, ObservationService

        async with store() as (engine, kernel, scope, clock):
            repository = engine.repository
            projected = replace(scope, session_id=None)
            unrelated = replace(scope, session_id="unrelated")
            service = ObservationService(repository, projected, base.POLICY, clock=lambda: clock[0])
            await service.register(FacetDefinition("projected-view", "alice"))
            private = base.source(scope, identity="private-source-in-session")
            survivor, _ = await seed(repository, projected, suffix="projected-survivor")
            await seed(repository, unrelated, suffix="unrelated")
            async with repository.unit_of_work() as uow:
                definition = await uow.derived_get(projected, "definition", "projected-view")
                await uow.append_event(private)
                await uow.save_admission_record(
                    projected,
                    "promoted-candidate",
                    private.id,
                    definition["slots"][0],
                    {"source_event_ids": [private.id]},
                    0,
                )
                old_safety = definition["safety_generation"]
                projected_gate = await uow.derived_get(projected, "subscription_index", "scope")
                other_routes = await route_rows(uow, unrelated)
                other_gate = await uow.derived_get(unrelated, "subscription_index", "scope")
                assert await uow.derived_reverse(projected, source_key(private.id))
            request = ForgetRequest(
                scope,
                () if all_in_scope else (private.id,),
                mode=ForgetMode.ERASE,
                all_in_scope=all_in_scope,
            )
            if restore:
                async with repository.unit_of_work() as uow:
                    await uow.forget_for_restore(request)
            else:
                await kernel.forget(request)
            async with repository.unit_of_work() as uow:
                assert await uow.get_admission_record(projected, "promoted-candidate") is None
                assert await uow.derived_header(projected, "promoted-candidate") is None
                assert await uow.derived_reverse(projected, source_key(private.id)) == ()
                assert await route_rows(uow, projected) == ()
                assert await uow.derived_records(projected, "subscription") == ()
                changed = await uow.derived_get(projected, "subscription_index", "scope")
                assert changed["state"] == "needs_backfill"
                assert changed["generation"] == projected_gate["generation"] + 1
                definition = await uow.derived_get(projected, "definition", "projected-view")
                assert definition["dirty"] and not definition.get("disabled")
                assert definition["safety_generation"] == old_safety + 1
                assert await uow.get_source_event(projected, survivor.id) is not None
                assert await uow.get_admission_record(projected, "candidate-projected-survivor")
                assert await uow.derived_header(projected, "candidate-projected-survivor")
                assert await route_rows(uow, unrelated) == other_routes
                assert await uow.derived_get(unrelated, "subscription_index", "scope") == other_gate
                await ensure_index(uow, projected)
                assert await uow.derived_reverse(projected, source_key(private.id)) == ()
                assert await uow.derived_reverse(projected, source_key(survivor.id)) == (
                    candidate_owner("candidate-projected-survivor"),
                )

    asyncio.run(run())
