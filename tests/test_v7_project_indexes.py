"""B3 real project censuses, write barriers and deletion/restore integration."""

import asyncio
import json
from dataclasses import replace

import pytest
import test_atom_admission as base
from test_project_admission_v7 import CONTRACT, grant, qualify, service, stage
from test_purge_restore import backup_copy, erase, replay, restorer

from agent_memory.derived.model import DerivedError
from agent_memory.derived.project_index import INDEX_SCHEMA, project_key, query_keys
from agent_memory.domain import ScopeLevel

store = base.store


async def generations(uow, scope, project_id):
    return {
        key: (await uow.derived_get(scope, "barrier", key) or {"generation": 0})["generation"]
        for key in query_keys(CONTRACT.fingerprint, project_id)
    }


def test_production_census_and_source_metadata_are_indexed_without_body_reads(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(svc, scope)
            await qualify(svc, *item)
            clock[0] = base.at(2)
            census = await svc.snapshot("project-a")
            assert census.candidate_count == 1
            cls = type(engine.repository.unit_of_work())

            async def forbidden(*args, **kwargs):
                raise AssertionError("steady-state census scanned candidates or fetched bodies")

            with monkeypatch.context() as patch:
                for name in (
                    "derived_headers",
                    "list_admission_records",
                    "get_source_event",
                    "get_admission_record",
                ):
                    patch.setattr(cls, name, forbidden)
                async with engine.repository.unit_of_work() as uow:
                    headers = await uow.derived_project_candidates(
                        scope, CONTRACT.fingerprint, "project-a"
                    )
                    assert [h["id"] for h in headers] == [item[2]]
                    proof = await uow.derived_project_source_proof(scope, item[0].id)
                    assert proof == {key: census.source_proofs[0][key] for key in proof}
                    assert (
                        await uow.derived_project_candidates(
                            scope, CONTRACT.fingerprint, "project-b"
                        )
                        == ()
                    )
            # Writes after cutover also avoid the bounded backfill path.
            with monkeypatch.context() as patch:
                patch.setattr(cls, "derived_headers", forbidden)
                patch.setattr(cls, "list_admission_records", forbidden)
                await svc.withdraw(
                    item[2], expected_version=4, review_id="withdraw", reasons=("retired",)
                )

    asyncio.run(run())


def test_project_barriers_exist_before_first_subscription_and_track_moves(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(
                svc,
                scope,
                membership="promise-a",
                subject="promise-1",
                predicate="commitment.action",
                value="ship",
            )
            async with engine.repository.unit_of_work() as uow:
                assert (await generations(uow, scope, "project-a"))[
                    project_key(CONTRACT.fingerprint, "project-a")
                ] > 0
                before_a = await generations(uow, scope, "project-a")
                before_b = await generations(uow, scope, "project-b")
            await svc.replace_membership(item[2], expected_version=2, membership_id="promise-b")
            async with engine.repository.unit_of_work() as uow:
                for project_id, before in (("project-a", before_a), ("project-b", before_b)):
                    after = await generations(uow, scope, project_id)
                    key = project_key(CONTRACT.fingerprint, project_id)
                    assert after[key] > before[key]
                    headers = await uow.derived_project_candidates(
                        scope, CONTRACT.fingerprint, project_id
                    )
                    assert (
                        len(headers) == 1
                        and headers[0]["project"]["current_project_id"] == "project-b"
                    )
            await grant(engine.repository, scope, item[0].id, revoked=True)
            clock[0] = base.at(2)
            moved = await svc.snapshot("project-a")
            assert moved.candidate_count == 1 and moved.source_ids == ()
            with pytest.raises(DerivedError, match="processing_denied"):
                await svc.snapshot("project-b")

    asyncio.run(run())


def test_every_disposition_and_wildcard_is_in_complete_exact_scope_census(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            pending = await stage(svc, scope)
            rejected = await stage(svc, scope, identity="rejected")
            withdrawn = await stage(svc, scope, identity="withdrawn")
            await svc.reject(
                rejected[2], expected_version=2, review_id="rejected", reasons=("incorrect",)
            )
            await svc.withdraw(
                withdrawn[2], expected_version=2, review_id="withdrawn", reasons=("retired",)
            )
            unknown = await stage(svc, scope, identity="unknown", membership=None)
            other = replace(scope, namespace="unrelated")
            other_svc = service(engine, other, clock)
            await stage(other_svc, other, identity="foreign")
            async with engine.repository.unit_of_work() as uow:
                headers = await uow.derived_project_candidates(
                    scope, CONTRACT.fingerprint, "project-a"
                )
                assert {h["id"] for h in headers} == {
                    pending[2],
                    rejected[2],
                    withdrawn[2],
                    unknown[2],
                }
                assert {
                    h["id"]
                    for h in await uow.derived_project_candidates(
                        scope, CONTRACT.fingerprint, "project-b"
                    )
                } == {unknown[2]}
                assert (
                    await uow.derived_project_candidates(scope, "another-contract", "project-a")
                    == ()
                )
                row = await uow.get_admission_record(scope, pending[2])
                row["payload"].pop("project_candidate")
                await svc._save(uow, row)
                wildcard = await uow.derived_project_candidates(
                    scope, "another-contract", "unrelated-project"
                )
                assert [h["id"] for h in wildcard] == [pending[2]]
                assert wildcard[0]["project"]["unreviewed"] is True

    asyncio.run(run())


def test_real_overflow_sentinel_precedes_all_body_reads(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            event = base.source(scope)
            async with engine.repository.unit_of_work() as uow:
                await uow.append_event(event)
                for number in range(65):
                    await uow.save_admission_record(
                        scope,
                        f"candidate-{number:02d}",
                        event.id,
                        "slot",
                        {"draft": {"predicate": "project.owner"}, "action": "REJECT"},
                        0,
                    )
            cls = type(engine.repository.unit_of_work())

            async def forbidden(*args, **kwargs):
                raise AssertionError("overflow fetched source/candidate body")

            monkeypatch.setattr(cls, "get_source_event", forbidden)
            monkeypatch.setattr(cls, "get_admission_record", forbidden)
            async with engine.repository.unit_of_work() as uow:
                assert (
                    len(
                        await uow.derived_project_candidates(
                            scope, CONTRACT.fingerprint, "project-a"
                        )
                    )
                    == 65
                )
            with pytest.raises(DerivedError, match="project_candidate_capacity"):
                await svc.snapshot("project-a")

    asyncio.run(run())


@pytest.mark.parametrize("damage", ["schema", "generation", "incomplete"])
def test_unknown_or_incomplete_index_fails_closed_before_bodies(store, monkeypatch, damage):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            await stage(svc, scope)
            async with engine.repository.unit_of_work() as uow:
                gate = await uow.derived_get(scope, "project_index", "scope")
                gate.update(
                    {"schema": "future/999"}
                    if damage == "schema"
                    else {"generation": True}
                    if damage == "generation"
                    else {"state": "incomplete"}
                )
                await uow.derived_put(scope, "project_index", "scope", gate)
            cls = type(engine.repository.unit_of_work())

            async def forbidden(*args, **kwargs):
                raise AssertionError("invalid index fetched source/candidate body")

            monkeypatch.setattr(cls, "get_source_event", forbidden)
            monkeypatch.setattr(cls, "get_admission_record", forbidden)
            with pytest.raises(DerivedError, match="project_candidate_index"):
                await svc.snapshot("project-a")

    asyncio.run(run())


def test_backfill_failure_rolls_back_cutover_and_retry_rebuilds_from_persisted_json(
    store, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(svc, scope)
            async with engine.repository.unit_of_work() as uow:
                gate = await uow.derived_get(scope, "project_index", "scope")
                gate["state"] = "needs_backfill"
                await uow.derived_put(scope, "project_index", "scope", gate)
            if hasattr(engine.repository, "pool"):
                from agent_memory_postgres import project_index as backend

                original = backend.replace

                async def fail(*args, **kwargs):
                    await original(*args, **kwargs)
                    raise RuntimeError("interrupted project backfill")
            else:
                from agent_memory.operations import sqlite_project_index as backend

                original = backend.replace

                def fail(*args, **kwargs):
                    original(*args, **kwargs)
                    raise RuntimeError("interrupted project backfill")

            with monkeypatch.context() as patch:
                patch.setattr(backend, "replace", fail)
                with pytest.raises(RuntimeError, match="interrupted project backfill"):
                    async with engine.repository.unit_of_work() as uow:
                        await uow.derived_project_candidates(
                            scope, CONTRACT.fingerprint, "project-a"
                        )
            async with engine.repository.unit_of_work() as uow:
                assert (await uow.derived_get(scope, "project_index", "scope"))[
                    "state"
                ] == "needs_backfill"
                headers = await uow.derived_project_candidates(
                    scope, CONTRACT.fingerprint, "project-a"
                )
                assert headers[0]["id"] == item[2]
                gate = await uow.derived_get(scope, "project_index", "scope")
                assert gate["schema"] == INDEX_SCHEMA and gate["state"] == "ready"

    asyncio.run(run())


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_project_routes_erased_in_all_affected_projections_and_real_backup(
    store, tmp_path, all_in_scope
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(svc, scope)
            projected = scope.project(ScopeLevel.USER)
            unrelated = replace(scope, user_id="bob", session_id="another-session")
            other = await stage(
                service(engine, unrelated, clock), unrelated, identity="unrelated-source"
            )
            async with engine.repository.unit_of_work() as uow:
                row = await uow.get_admission_record(scope, item[2])
                await uow.save_admission_record(
                    projected,
                    "private-projected-candidate",
                    row["event_id"],
                    row["slot_key"],
                    row["payload"],
                    0,
                )
                for target in (scope, projected, unrelated):
                    assert await uow.derived_project_candidates(
                        target, CONTRACT.fingerprint, "project-a"
                    )
            async with backup_copy(engine.repository, tmp_path) as (backup, restored_kernel):
                await erase(kernel, scope, item[0].id, all_in_scope=all_in_scope)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        if hasattr(repository, "pool"):
                            rows = await (
                                await uow.connection.execute(
                                    "SELECT * FROM agent_memory_derived_project_routes"
                                )
                            ).fetchall()
                        else:
                            rows = [
                                dict(row)
                                for row in uow.connection.execute(
                                    "SELECT * FROM derived_project_routes"
                                ).fetchall()
                            ]
                        serialized = json.dumps(rows)
                        assert (
                            item[2] not in serialized
                            and "private-projected-candidate" not in serialized
                        )
                        assert other[2] in serialized
                        for target in (scope, projected):
                            assert (
                                await uow.derived_project_candidates(
                                    target, CONTRACT.fingerprint, "project-a"
                                )
                                == ()
                            )
                        remaining = await uow.derived_project_candidates(
                            unrelated, CONTRACT.fingerprint, "project-a"
                        )
                        assert [header["id"] for header in remaining] == [other[2]]

    asyncio.run(run())


def test_first_query_lock_fences_later_writer_and_subscription_dirties_atomically(store):
    async def run():
        from agent_memory.derived import subscriptions
        from agent_memory.derived.model import digest

        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            entered, release, writer_started = asyncio.Event(), asyncio.Event(), asyncio.Event()

            async def reader():
                async with engine.repository.unit_of_work() as uow:
                    await uow.lock_admission_scope(scope)
                    assert (
                        await uow.derived_project_candidates(
                            scope, CONTRACT.fingerprint, "project-a"
                        )
                        == ()
                    )
                    before = await generations(uow, scope, "project-a")
                    definition = dict(
                        facet_id="project-view",
                        fingerprint=digest("view"),
                        generation=1,
                        epoch=0,
                        safety_generation=0,
                        time_generation=0,
                        slots=[],
                        disabled=False,
                        dirty=False,
                        spec={
                            "schema": "question-instance-registration/1",
                            "id": "project-view",
                            "project_id": "project-a",
                            "contract_fingerprint": CONTRACT.fingerprint,
                            "parent_facets": [],
                        },
                    )
                    await uow.derived_put(scope, "definition", "project-view", definition)
                    await subscriptions.install(uow, scope, definition)
                    entered.set()
                    await release.wait()
                    assert await generations(uow, scope, "project-a") == before
                    assert not (await uow.derived_get(scope, "definition", "project-view"))["dirty"]

            async def writer():
                await entered.wait()
                writer_started.set()
                return await stage(svc, scope)

            read_task, write_task = asyncio.create_task(reader()), asyncio.create_task(writer())
            await entered.wait()
            await writer_started.wait()
            await asyncio.sleep(0.02)
            assert not write_task.done()
            release.set()
            await read_task
            item = await write_task
            async with engine.repository.unit_of_work() as uow:
                definition = await uow.derived_get(scope, "definition", "project-view")
                assert definition["dirty"]
                assert [
                    h["id"]
                    for h in await uow.derived_project_candidates(
                        scope, CONTRACT.fingerprint, "project-a"
                    )
                ] == [item[2]]
                assert (await generations(uow, scope, "project-a"))[
                    project_key(CONTRACT.fingerprint, "project-a")
                ] > 0

    asyncio.run(run())


def test_sqlite_wildcard_probe_is_bounded_before_deduplication():
    import sqlite3

    from agent_memory.derived.project_index import WILDCARD_KEY
    from agent_memory.derived.subscriptions import HEADER_SCHEMA
    from agent_memory.domain import MemoryScope, canonical_json
    from agent_memory.operations import sqlite_derived, sqlite_project_index

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(sqlite_derived.SCHEMA + sqlite_project_index.SCHEMA)
    scope = MemoryScope("bounded-project-census")
    sqlite_derived.put(
        connection,
        scope,
        "project_index",
        "scope",
        {
            "schema": INDEX_SCHEMA,
            "state": "ready",
            "generation": 1,
        },
    )
    connection.executemany(
        "INSERT INTO derived_project_routes VALUES (?,?,?)",
        [(scope.partition_key(), WILDCARD_KEY, f"candidate-{i:06d}") for i in range(10_000)],
    )
    for i in range(65):
        header = {
            "schema": HEADER_SCHEMA,
            "generation": 1,
            "version": 1,
            "id": f"candidate-{i:06d}",
            "source_ids": ["source"],
            "project": {
                "schema": "project-candidate/1",
                "contract_fingerprint": None,
                "current_project_id": None,
                "project_ids": [],
                "was_unbound": True,
                "unreviewed": True,
            },
        }
        connection.execute(
            "INSERT INTO derived_atom_headers VALUES (?,?,?,?)",
            (
                scope.partition_key(),
                header["id"],
                "slot",
                canonical_json(header),
            ),
        )
    steps = [0]

    def count():
        steps[0] += 1000
        return steps[0] > 10_000

    connection.set_progress_handler(count, 1000)
    assert (
        len(sqlite_project_index.candidates(connection, scope, CONTRACT.fingerprint, "project-a"))
        == 65
    )
    assert steps[0] < 10_000
    connection.close()


def test_unknown_project_schema_write_rolls_back_candidate_header_and_barriers(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(svc, scope)
            async with engine.repository.unit_of_work() as uow:
                before = await generations(uow, scope, "project-a")
            with pytest.raises(DerivedError, match="project_candidate_schema_unsupported"):
                async with engine.repository.unit_of_work() as uow:
                    row = await uow.get_admission_record(scope, item[2])
                    row["payload"]["project_candidate"]["schema"] = "future-project/999"
                    await svc._save(uow, row)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.get_admission_record(scope, item[2])
                assert row["version"] == 2
                assert row["payload"]["project_candidate"]["schema"] == "project-candidate/1"
                assert await generations(uow, scope, "project-a") == before
                assert (
                    await uow.derived_project_candidates(scope, CONTRACT.fingerprint, "project-a")
                )[0]["version"] == 2

    asyncio.run(run())


def test_oversized_legacy_cutover_stays_explicitly_fenced_without_repeated_scan(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc = service(engine, scope, clock)
            item = await stage(svc, scope)
            if hasattr(engine.repository, "pool"):
                from agent_memory_postgres import project_index as backend
            else:
                from agent_memory.operations import sqlite_project_index as backend
            with monkeypatch.context() as patch:
                patch.setattr(backend, "MAX_BACKFILL", 0)
                async with engine.repository.unit_of_work() as uow:
                    gate = await uow.derived_get(scope, "project_index", "scope")
                    gate["state"] = "needs_backfill"
                    await uow.derived_put(scope, "project_index", "scope", gate)
                    await uow.derived_project_index(scope)
            async with engine.repository.unit_of_work() as uow:
                gate = await uow.derived_get(scope, "project_index", "scope")
                assert gate["state"] == "scope"
                before = await uow.derived_get(scope, "barrier", "route:fallback")
            await svc.withdraw(
                item[2], expected_version=2, review_id="dispose", reasons=("retired",)
            )
            async with engine.repository.unit_of_work() as uow:
                after = await uow.derived_get(scope, "barrier", "route:fallback")
                assert after["generation"] > before["generation"]
            with pytest.raises(DerivedError, match="project_candidate_index_incomplete"):
                await svc.snapshot("project-a")

    asyncio.run(run())


def test_unknown_project_routing_header_cannot_block_source_erasure_or_backup_replay(
    store, tmp_path
):
    async def run():
        from test_question_runtime_v7 import ACTOR, register, runtime

        from agent_memory.domain import canonical_json

        async with store() as (engine, kernel, scope, clock):
            questions = runtime(engine, scope, clock)
            registration = await register(questions)
            item = await stage(questions.admission, scope)
            async with engine.repository.unit_of_work() as uow:
                header = await uow.derived_header(scope, item[2])
                header["project"]["schema"] = "project-candidate/999"
                if hasattr(engine.repository, "pool"):
                    from psycopg.types.json import Jsonb

                    await uow.connection.execute(
                        "UPDATE agent_memory_derived_atom_headers SET payload_json=%s "
                        "WHERE partition_key=%s AND identity=%s",
                        (Jsonb(header), scope.partition_key(), item[2]),
                    )
                else:
                    uow.connection.execute(
                        "UPDATE derived_atom_headers SET payload_json=? "
                        "WHERE partition_key=? AND identity=?",
                        (canonical_json(header), scope.partition_key(), item[2]),
                    )
            with pytest.raises(DerivedError, match="project_candidate_header_invalid"):
                await questions.admission.snapshot("project-a")
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, item[0].id)
                deletion = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), deletion)
                for repository in (engine.repository, backup):
                    async with repository.unit_of_work() as uow:
                        assert await uow.get_source_event(scope, item[0].id) is None
                        definition = await uow.derived_get(
                            scope, "definition", registration["instance_id"]
                        )
                        assert definition["disabled"]
                        assert definition["spec"]["schema"] == "question-erased/1"
                        assert await uow.derived_project_candidates(
                            scope, CONTRACT.fingerprint, "project-a"
                        ) == ()
                with pytest.raises(DerivedError, match="question_erased"):
                    await questions.read("project-a:owner", actor=ACTOR)

    asyncio.run(run())
