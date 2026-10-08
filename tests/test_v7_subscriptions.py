"""V7-B1 indexed admitted-L1 routing, shared SQLite/real PostgreSQL contracts."""

import asyncio
from copy import deepcopy
from dataclasses import replace

import pytest
import test_atom_admission as base
from test_derived_controls import configured
from test_derived_observations import build, setup
from test_durable_purge import source_id

from agent_memory.derived import DerivedError, FacetDefinition, ProcessingGrant
from agent_memory.derived import subscriptions as index
from agent_memory.derived.service import interpretation_changed, mark_slot_changed
from agent_memory.domain import ForgetMode, ForgetRequest

store = base.store


def forbid_definition_scan(monkeypatch, repository):
    cls = type(repository.unit_of_work())
    original = cls.derived_records

    async def indexed_only(self, scope, kind):
        if kind == "definition":
            raise AssertionError("normal write enumerated definitions")
        return await original(self, scope, kind)

    monkeypatch.setattr(cls, "derived_records", indexed_only)


def test_registered_empty_subscription_precedes_census_and_cas(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock, inputs=0)
            receipt = await queue.request("language", dedupe_key="empty-first")
            lease = await queue.claim("empty-first", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                subscription = await index.subscription(uow, scope, definition)
                assert subscription["schema"] == index.SUBSCRIPTION_SCHEMA
                assert receipt["unit"]["query_generation"][index.SUBSCRIPTION_BARRIER] == 1
                assert await uow.derived_reverse(scope, index.slot_key(definition["slots"][0])) == (
                    index.subscription_owner("language"),
                )
                source = base.source(scope)
                await uow.append_event(source)
                await uow.save_admission_record(
                    scope,
                    "late-pending",
                    source.id,
                    definition["slots"][0],
                    {"action": "PENDING", "source_event_ids": [source.id]},
                    0,
                )
            with pytest.raises(DerivedError, match="derived_snapshot_changed"):
                await service.publish(lease.task, snapshot, service.prepare(snapshot))

    asyncio.run(run())


def test_normal_candidate_grant_interpretation_control_and_parent_writes_use_index(
    store, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _, authority, query, _ = await configured(engine, kernel, scope, clock)
            await service.register(
                FacetDefinition(
                    "child",
                    "alice",
                    template_version="locale-parents/1",
                    parent_facets=("language",),
                    authority_id=authority.id,
                )
            )
            await build(queue)
            forbid_definition_scan(monkeypatch, engine.repository)
            async with engine.repository.unit_of_work() as uow:
                row = (await uow.list_admission_records(scope))[0]
                await uow.save_admission_record(
                    scope,
                    row["id"],
                    row["event_id"],
                    row["slot_key"],
                    row["payload"],
                    row["version"],
                )
                await interpretation_changed(uow, scope, row["event_id"], at=clock[0])
                child = await uow.derived_get(scope, "definition", "child")
                assert child["dirty"]
            await service.grant(
                ProcessingGrant(source_id(scope, "1"), ("alice",)), expected_version=2
            )
            await service.register_query(replace(query, version="2"), expected_generation=1)
            await service.set_authority(authority, expected_version=1)

    asyncio.run(run())


def test_unrelated_candidate_preserves_ready_result_and_query_barrier(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            before = await service.read("language", actor="alice")
            forbid_definition_scan(monkeypatch, engine.repository)
            await engine.admit(
                base.source(scope), [base.atom()], authority=base.SELF, policy=base.POLICY
            )
            after = await service.read("language", actor="alice")
            assert after == before
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(scope, "definition", "language")
                assert not row["dirty"]
                assert (await uow.derived_get(scope, "barrier", index.SCOPE_BARRIER))[
                    "generation"
                ] > 0

    asyncio.run(run())


@pytest.mark.parametrize("action", ["PENDING", "REJECT", "WITHDRAWN"])
def test_source_routes_track_old_and_new_memberships_for_every_candidate(store, action):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(scope, "definition", "language")
                slot = row["slots"][0]
                a, b = base.source(scope), base.source(scope)
                await uow.append_event(a)
                await uow.append_event(b)
                payload = {"action": action, "source_event_ids": [a.id, b.id]}
                await uow.save_admission_record(scope, "candidate", a.id, slot, payload, 0)
                assert await index.source_slots(uow, scope, b.id) == {slot}
                owner = index.candidate_owner("candidate")
                assert await uow.derived_reverse(scope, index.source_key(b.id)) == (owner,)
                # The routing edge is not actual processing provenance.
                assert await uow.derived_reverse(scope, "source:" + b.id) == ()
                previous = (await uow.derived_get(scope, "barrier", slot))["generation"]
                await interpretation_changed(uow, scope, b.id, at=clock[0])
                assert (await uow.derived_get(scope, "barrier", slot))["generation"] > previous
                payload["source_event_ids"] = [a.id]
                await uow.save_admission_record(scope, "candidate", a.id, slot, payload, 1)
                assert await uow.derived_reverse(scope, index.source_key(b.id)) == ()
                assert await index.source_slots(uow, scope, b.id) == set()

    asyncio.run(run())


def test_membership_delta_invalidates_both_old_and_new_slot_keys(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            async with engine.repository.unit_of_work() as uow:
                await mark_slot_changed(
                    uow,
                    scope,
                    "new-slot",
                    old_header={"slot_key": "old-slot"},
                    new_header={"slot_key": "new-slot"},
                    at=clock[0],
                )
                for key in ("old-slot", "new-slot"):
                    assert (await uow.derived_get(scope, "barrier", key))["generation"] == 1

    asyncio.run(run())


def test_scope_fallback_is_bounded_metered_and_invalidates_snapshot(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            forbid_definition_scan(monkeypatch, engine.repository)
            async with engine.repository.unit_of_work() as uow:
                gate = await uow.derived_get(scope, "subscription_index", "scope")
                gate["source_mode"] = "scope"
                await uow.derived_put(scope, "subscription_index", "scope", gate)
                await interpretation_changed(uow, scope, "unindexed-source", at=clock[0])
                updated = await uow.derived_get(scope, "subscription_index", "scope")
                assert updated["fallback_count"] == gate["fallback_count"] + 1
                assert updated["last_fallback_reason"] == "interpretation"
            assert (await service.read("language", actor="alice"))["state"] == "stale"

    asyncio.run(run())


def test_backfill_is_atomic_and_old_targets_fail_closed(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            async with engine.repository.unit_of_work() as uow:
                gate = await uow.derived_get(scope, "subscription_index", "scope")
                gate["state"] = "needs_backfill"
                await uow.derived_put(scope, "subscription_index", "scope", gate)
                await uow.derived_edges(scope, index.subscription_owner("language"), [])
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_edges

            async def fail(self, *args):
                await original(self, *args)
                raise RuntimeError("index interrupted")

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_edges", fail)
                with pytest.raises(RuntimeError, match="index interrupted"):
                    await service.read("language", actor="alice")
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, "subscription_index", "scope"))[
                    "state"
                ] == "needs_backfill"
                assert await uow.derived_reverse(scope, index.SCOPE_KEY) == ()
            assert (await service.read("language", actor="alice"))["state"] == "stale"
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, "subscription_index", "scope"))[
                    "state"
                ] == "ready"
                header = (await uow.derived_headers(scope))[0]
                assert header["schema"] == index.HEADER_SCHEMA
                assert header["generation"] == header["version"]

    asyncio.run(run())


def test_erasure_physically_scrubs_subscription_and_source_routing(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            source = source_id(scope, "1")
            await kernel.forget(ForgetRequest(scope, (source,), mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_reverse(scope, index.source_key(source)) == ()
                assert await uow.derived_reverse(scope, index.SCOPE_KEY) == ()
                assert await uow.derived_records(scope, "subscription") == ()
                assert (await uow.derived_get(scope, "subscription_index", "scope"))[
                    "state"
                ] == "needs_backfill"
            assert (await service.read("language", actor="alice"))["body"] is None
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_reverse(scope, index.source_key(source)) == ()

    asyncio.run(run())


def test_subscription_proof_tamper_blocks_snapshot_before_bodies(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock)
            receipt = await queue.request("language", dedupe_key="proof")
            async with engine.repository.unit_of_work() as uow:
                proof = deepcopy(await uow.derived_get(scope, "subscription", "language"))
                proof["generation"] += 1
                await uow.derived_put(scope, "subscription", "language", proof)
                with pytest.raises(DerivedError, match="derived_subscription_unavailable"):
                    await service._check_unit(uow, receipt["unit"])

    asyncio.run(run())


def test_first_registration_serializes_with_candidate_writer(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            from agent_memory.derived import ObservationService

            service = ObservationService(
                engine.repository, scope, base.POLICY, clock=lambda: clock[0]
            )
            installed, resume, writer_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_edges

            async def install_then_wait(self, scope, owner, values):
                await original(self, scope, owner, values)
                if owner == index.subscription_owner("language"):
                    installed.set()
                    await resume.wait()

            async def write_candidate():
                writer_started.set()
                return await engine.admit(
                    base.source(scope, "Alice prefers zh-CN"),
                    [
                        base.atom(
                            "zh-CN",
                            predicate="locale",
                            text="Alice prefers zh-CN",
                            source_quote="Alice prefers zh-CN",
                        )
                    ],
                    authority=base.SELF,
                    policy=base.POLICY,
                )

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_edges", install_then_wait)
                registration = asyncio.create_task(
                    service.register(FacetDefinition("language", "alice"))
                )
                await installed.wait()
                writer = asyncio.create_task(write_candidate())
                await writer_started.wait()
                resume.set()
                await asyncio.gather(registration, writer)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                unit = (await service._unit(uow, definition)).payload()
                assert unit["query_generation"][definition["slots"][0]] > 0
                assert len(await uow.derived_candidates(scope, definition["slots"])) == 1
                assert definition["dirty"]

    asyncio.run(run())


def test_oversized_backfill_selects_scope_fallback_without_partial_index_claim(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            async with engine.repository.unit_of_work() as uow:
                gate = await uow.derived_get(scope, "subscription_index", "scope")
                gate["state"] = "needs_backfill"
                await uow.derived_put(scope, "subscription_index", "scope", gate)
            cls = type(engine.repository.unit_of_work())

            async def oversized(self, scope):
                return ({},) * (index.MAX_HEADERS + 1)

            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_headers", oversized)
                await service.read("language", actor="alice")
            forbid_definition_scan(monkeypatch, engine.repository)
            async with engine.repository.unit_of_work() as uow:
                gate = await uow.derived_get(scope, "subscription_index", "scope")
                assert gate["state"] == "ready" and gate["source_mode"] == "scope"
                await interpretation_changed(uow, scope, "never-indexed", at=clock[0])
                assert (await uow.derived_get(scope, "definition", "language"))["dirty"]

    asyncio.run(run())


def test_old_backup_purge_replay_cannot_resurrect_subscription_selectors(store, tmp_path):
    async def run():
        from test_purge_restore import backup_copy, replay, restorer

        async with store() as (engine, kernel, scope, clock):
            _, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            source = source_id(scope, "1")
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                async with backup.unit_of_work() as uow:
                    assert await uow.derived_reverse(scope, index.source_key(source))
                    assert await uow.derived_records(scope, "subscription")
                await kernel.forget(ForgetRequest(scope, (source,), mode=ForgetMode.ERASE))
                snapshot = await restorer(engine.repository, scope, clock).export()
                result = await replay(restorer(backup, scope, clock), snapshot)
                assert result["replayed_entries"] == 1
                async with backup.unit_of_work() as uow:
                    assert await uow.derived_reverse(scope, index.source_key(source)) == ()
                    assert await uow.derived_records(scope, "subscription") == ()
                    assert await uow.derived_reverse(scope, index.SCOPE_KEY) == ()
                    assert await uow.derived_headers(scope) == ()

    asyncio.run(run())


def test_source_invalidation_covers_candidates_beyond_snapshot_capacity(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "language")
                slot = definition["slots"][0]
                for number in range(66):
                    source = base.source(scope, identity=f"candidate-source-{number:03d}")
                    await uow.append_event(source)
                    await uow.save_admission_record(
                        scope,
                        f"candidate-{number:03d}",
                        source.id,
                        slot,
                        {"action": "REJECT", "source_event_ids": [source.id]},
                        0,
                    )
                before = (await uow.derived_get(scope, "barrier", slot))["generation"]
            forbid_definition_scan(monkeypatch, engine.repository)
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_candidates(scope, [slot])) == 65
                await interpretation_changed(uow, scope, source.id, at=clock[0])
                assert (await uow.derived_get(scope, "barrier", slot))["generation"] > before

    asyncio.run(run())


def test_backfill_retires_open_coverage_at_proven_host_boundary(store, monkeypatch):
    async def run():
        from datetime import timedelta

        from test_derived_coverage import configured, ledger
        from test_derived_history import refresh

        async with store() as (engine, kernel, scope, clock):
            service, queue, _, _, _, _, _ = await configured(engine, kernel, scope, clock)
            receipt = await build(queue)
            clock[0] += timedelta(seconds=10)
            await refresh(service, queue, "seal-one")
            before = await ledger(engine.repository, scope)
            assert [span["state"] for span in before["spans"]] == ["sealed", "open"]
            async with engine.repository.unit_of_work() as uow:
                gate = await uow.derived_get(scope, "subscription_index", "scope")
                gate["state"] = "needs_backfill"
                await uow.derived_put(scope, "subscription_index", "scope", gate)
            with monkeypatch.context() as patch:
                patch.setattr(index, "utc_now", lambda: clock[0] + timedelta(days=10))
                assert (await service.read("language", actor="alice"))["state"] == "stale"
            after = await ledger(engine.repository, scope)
            assert after["last_at"] == before["last_at"]
            assert after["spans"][0] == before["spans"][0]
            assert after["spans"][1]["state"] == "uncertain"
            assert after["spans"][1]["known_to"] == before["last_at"]
            assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            clock[0] += timedelta(seconds=1)
            await refresh(service, queue, "after-backfill")
            assert (await service.read("language", actor="alice"))["state"] == "ready"

    asyncio.run(run())
