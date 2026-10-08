"""Regression evidence for mutable publication inputs and capability boundaries."""

import asyncio
from copy import deepcopy

import pytest
import test_atom_admission as base
from test_derived_observations import build, setup
from test_derived_pages import page, ready

from agent_memory.derived import DerivedError, FacetDefinition
from agent_memory.derived.model import digest
from agent_memory.domain import canonical_json

store = base.store


@pytest.mark.parametrize("resource", ["observation", "page", "history_point", "history_interval"])
@pytest.mark.parametrize("changed", ["prepared", "snapshot", "task"])
def test_publish_owns_checked_output_before_first_await(store, resource, changed, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            if resource == "page":
                service, queue, _, _ = await ready(engine, kernel, scope, clock)
                key = "language-page"
            elif resource.startswith("history_"):
                if resource == "history_point":
                    from test_derived_history import configured
                else:
                    from test_derived_coverage import configured

                service, queue, *_ = await configured(engine, kernel, scope, clock)
                await build(queue)
                key = "language"
            else:
                service, queue, _ = await setup(engine, kernel, scope, clock)
                await build(queue)
                key = "language"
            receipt = await queue.request(key, dedupe_key="mutable-output", force=True)
            lease = await queue.claim("audit", lease_seconds=60)
            snapshot = await service.snapshot(lease.task)
            prepared = service.prepare(snapshot)
            expected = deepcopy(prepared["body"])
            expected_unit = deepcopy(lease.task.payload["unit"])
            cls = type(engine.repository.unit_of_work())
            original = cls.lock_admission_scope
            entered, release = asyncio.Event(), asyncio.Event()

            async def paused(self, *args):
                await original(self, *args)
                if asyncio.current_task().get_name() == "audit-publication":
                    entered.set()
                    await release.wait()

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(cls, "lock_admission_scope", paused)
                    publication = asyncio.create_task(
                        service.publish(lease.task, snapshot, prepared), name="audit-publication"
                    )
                    await asyncio.wait_for(entered.wait(), 10)
                    if changed == "task":
                        lease.task.payload["unit"]["time_generation"] += 1
                    elif changed == "snapshot":
                        if resource == "page":
                            snapshot["parents"]["language"]["body"]["blocks"] = []
                        else:
                            snapshot["records"][0]["payload"]["draft"]["value"] = "changed"
                    elif resource == "page":
                        prepared["body"]["scenario"]["title"] = "Caller mutation after validation"
                    else:
                        prepared["body"]["blocks"][0]["value"] = "fabricated-locale"
                    prepared["body_sha256"] = digest(prepared["body"])
                    release.set()
                    await asyncio.wait_for(publication, 10)
                async with engine.repository.unit_of_work() as uow:
                    head = await uow.derived_get(scope, "head", key)
                    revision = await uow.derived_get(scope, "revision", head["revision_id"])
                    assert revision["body"] == expected
                    assert revision["unit"] == expected_unit
                assert (await queue.status(receipt["target_id"], actor="alice"))["complete"]
            finally:
                release.set()

    asyncio.run(run())


@pytest.mark.parametrize("scheduled", [False, True])
def test_unsupported_page_worker_preserves_refresh_responsibility(store, scheduled, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, queue, _ = await setup(engine, kernel, scope, clock)
            await build(queue)
            await service.register_page(page(scope))
            if scheduled:
                receipt = await queue.request("language-page", dedupe_key="supported-worker")
            async with engine.repository.unit_of_work() as uow:
                before = await uow.derived_get(scope, "definition", "language-page")
                jobs = await uow.derived_records(scope, "job")
            with monkeypatch.context() as patch:
                patch.setattr(type(engine.repository.unit_of_work()), "derived_page_contract", None)
                assert await queue.claim("unsupported-worker", lease_seconds=60) is None
            async with engine.repository.unit_of_work() as uow:
                after = await uow.derived_get(scope, "definition", "language-page")
                assert after == before
                assert await uow.derived_records(scope, "job") == jobs
            lease = await queue.claim("supported-worker", lease_seconds=60)
            assert lease and lease.task.payload["unit"]["facet_id"] == "language-page"
            await service.apply(lease.task)
            await queue.complete(lease)
            if scheduled:
                status = await service.pages.status(receipt["target_id"], actor="alice")
                assert status["page_ready"]

    asyncio.run(run())


@pytest.mark.parametrize("generation", [False, True, 0.0, -1])
@pytest.mark.parametrize("resource", ["observation", "page"])
def test_definition_generation_requires_nonnegative_integer(store, resource, generation):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, _, _ = await setup(engine, kernel, scope, clock, inputs=0)
            if resource == "page":
                operation = service.register_page
                definition = page(scope)
            else:
                operation = service.register
                definition = FacetDefinition("language", "alice")
            with pytest.raises(DerivedError, match="invalid_derived_control_version"):
                await operation(definition, expected_generation=generation)

    asyncio.run(run())


@pytest.mark.parametrize("force", [0, 1, None])
def test_duplicate_request_still_validates_force_type(store, force):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, queue, _ = await setup(engine, kernel, scope, clock, inputs=0)
            await queue.request("language", dedupe_key="duplicate", force=bool(force))
            with pytest.raises(DerivedError, match="invalid_derived_request"):
                await queue.request("language", dedupe_key="duplicate", force=force)

    asyncio.run(run())


@pytest.mark.parametrize("parents", [1, 4])
def test_page_output_limit_measures_delivered_materialized_body(parents):
    from agent_memory.derived.pages import compose_page

    scope = base.MemoryScope("audit", user_id="alice")
    keys = tuple(["language", *["parent-" + str(i) for i in range(1, parents)]])
    spec = page(scope, keys)
    snapshot = dict(definition={"spec": spec.payload()}, manifest={}, parents={
        key: dict(id="parent-" + key, state="ready", body={"blocks": [""]}) for key in keys
    })
    first = compose_page(snapshot, scope)
    materialized = {**first["body"], "blocks": [
        {**ref, "body": block["body"]}
        for ref, block in zip(first["body"]["blocks"], first["block_revisions"], strict=True)
    ]}
    overhead = len(canonical_json(materialized).encode())
    snapshot["parents"]["language"]["body"]["blocks"] = ["x" * (32768 - overhead)]
    output = compose_page(snapshot, scope)
    actual = {**output["body"], "blocks": [
        {**ref, "body": block["body"]}
        for ref, block in zip(output["body"]["blocks"], output["block_revisions"], strict=True)
    ]}
    assert len(canonical_json(actual).encode()) == 32768
    snapshot["parents"]["language"]["body"]["blocks"][0] += "x"
    with pytest.raises(DerivedError, match="page_output_capacity"):
        compose_page(snapshot, scope)
