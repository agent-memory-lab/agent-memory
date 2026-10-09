"""Durable pre-dispatch cache slots and live finance status; synthetic providers only."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_governed_models_v7 import setup
from test_question_models_v7 import configured_question
from test_question_runtime_v7 import ACTOR

from agent_memory.derived.model import ProcessingGrant
from agent_memory.retrieval.model_answers import GovernedModelAnswers
from agent_memory.retrieval.model_contracts import ModelError, digest

store = project.store


def runtime(authority, port, answers):
    return GovernedModelAnswers(
        authority,
        port,
        account_keys=answers.account_keys,
        validate_output=lambda text, _: text == "zh-CN",
        max_cache_entries=1,
    )


async def other_input(authority, sealed, identity):
    return await authority.prepare(
        replace(sealed.coordinates, task_intent=identity),
        tuple(json.loads(sealed.manifest_json)["sources"]),
    )


def test_question_full_cache_retries_never_dispatch_or_reserve_money(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock)
            models.answers.max_cache_entries = 1
            first = await models.answer("project-a:owner", actor=ACTOR)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)),
                expected_version=2,
            )
            clock[0] += timedelta(microseconds=100)
            await svc.answer("project-a:owner", actor=ACTOR, dedupe_key="new-proof")
            for _ in range(2):
                with pytest.raises(ModelError, match="model_cache_capacity"):
                    await models.answer("project-a:owner", actor=ACTOR)
            assert len(port.calls) == 1
            (call,) = await ledger.snapshot()
            assert call["call_id"] == first["call_id"]
            assert call["outcome"] == "completed"
            async with engine.repository.unit_of_work() as uow:
                assert len(await uow.derived_records(scope, "model_cache_header")) == 1

    asyncio.run(run())


@pytest.mark.parametrize("legacy_header", [False, True])
def test_settlement_updates_cached_answer_without_mutating_provider_receipt(store, legacy_header):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, models, ledger, port = await configured_question(engine, scope, clock)
            first = await models.answer("project-a:owner", actor=ACTOR)
            assert first["cost_status"] == "unknown"
            (call,) = await ledger.snapshot()
            key = port.calls[0].key
            async with engine.repository.unit_of_work() as uow:
                header = await uow.derived_get(scope, "model_cache_header", key)
                body = await uow.derived_get(scope, "model_cache_body", key)
                assert header["reservation_key"] == call["key"]
                if legacy_header:
                    header.pop("reservation_key")
                    header.pop("delivery_reservation")
                    await uow.derived_put(scope, "model_cache_header", key, header)
            await ledger.settle(
                call["key"], actual_microunits=100, receipt_id="later-verified-receipt"
            )
            second = await models.answer("project-a:owner", actor=ACTOR)
            assert second["cache_hit"] and second["cost_status"] == "measured"
            assert second["call_id"] == first["call_id"]
            assert len(port.calls) == 1
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "model_cache_body", key) == body
                delivery = await uow.derived_get(
                    scope, "model_authorization", second["delivery_id"]
                )
                assert delivery["payload_sha256"] == digest(second)

    asyncio.run(run())


def test_distinct_runtime_keys_compete_for_one_durable_cache_slot(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            entered, release = asyncio.Event(), asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()

            authority, sealed, ledger, port, answers, _ = await setup(
                engine, kernel, scope, clock, callback=slow
            )
            answers.max_cache_entries = 1
            other = runtime(authority, port, answers)
            inputs = [await other_input(authority, sealed, str(i)) for i in range(4)]
            first = asyncio.create_task(answers.answer(sealed))
            try:
                await asyncio.wait_for(entered.wait(), 5)
                async with engine.repository.unit_of_work() as uow:
                    header = await uow.derived_get(scope, "model_cache_header", sealed.key)
                    assert header["state"] == "reserved"
                    assert await uow.derived_records(scope, "model_cache_body") == ()
                results = await asyncio.gather(
                    *(other.answer(item) for item in inputs), return_exceptions=True
                )
                assert all(
                    isinstance(error, ModelError) and error.code == "model_cache_capacity"
                    for error in results
                )
                assert len(port.calls) == 1 and len(await ledger.snapshot()) == 1
            finally:
                release.set()
                await first
            assert (await other.answer(sealed)).cache_hit
            assert len(port.calls) == 1

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["budget", "provider", "validation"])
def test_failed_execution_releases_its_cache_slot_but_preserves_cost_debt(store, failure):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            answers.max_cache_entries = 1
            reserve, validate = answers.budget.reserve, answers.validate_output

            async def rejected(**kwargs):
                raise ModelError("model_budget_exhausted")

            async def failed():
                raise RuntimeError("synthetic provider failure")

            if failure == "budget":
                answers.budget.reserve = rejected
            elif failure == "provider":
                port.before_return = failed
            else:
                answers.validate_output = lambda *_: False
            with pytest.raises(ModelError):
                await answers.answer(sealed)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "model_cache_header", sealed.key) == {
                    "state": "expired"
                }
                assert await uow.derived_records(scope, "model_cache_body") == ()
            before = await ledger.snapshot()
            if failure == "budget":
                assert before == () and port.calls == []
            else:
                assert len(before) == 1 and before[0]["state"] == "reconciliation_pending"
            answers.budget.reserve, answers.validate_output = reserve, validate
            port.before_return = None
            updated = await other_input(authority, sealed, "retry-after-failure")
            assert (await answers.answer(updated)).text == "zh-CN"
            assert len(await ledger.snapshot()) == len(before) + 1
            if before:
                old = next(row for row in await ledger.snapshot() if row["key"] == before[0]["key"])
                assert old["state"] == "reconciliation_pending"

    asyncio.run(run())


def test_expired_leader_cannot_clear_reclaimed_slot_or_release_unknown_debt(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            answers.max_cache_entries = 1
            token = await answers._claim(sealed)
            await answers._reserve_cache(sealed, token)
            call = await ledger.reserve(
                operation_id=sealed.key,
                attempt_id=token,
                request_sha256=sealed.payload_sha256,
                account_keys=answers.account_keys,
                maximum_microunits=None,
            )
            async with engine.repository.unit_of_work() as uow:
                await ledger.intent(uow, call["key"], sealed.payload_sha256)
            restarted = runtime(authority, port, answers)
            clock[0] += timedelta(seconds=sealed.configuration.timeout_seconds * 8 + 2)
            entered, release = asyncio.Event(), asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()

            port.before_return = slow
            current = asyncio.create_task(restarted.answer(sealed))
            try:
                await asyncio.wait_for(entered.wait(), 5)
                await answers._complete_flight(sealed, token, "finished")
                async with engine.repository.unit_of_work() as uow:
                    slot = await uow.derived_get(scope, "model_cache_header", sealed.key)
                    assert slot["state"] == "reserved" and slot["token"] != token
                assert await restarted.sweep_expired() == 0
            finally:
                release.set()
                await current
            old = next(row for row in await ledger.snapshot() if row["key"] == call["key"])
            assert old["state"] == "dispatch_intent" and old["actual_microunits"] is None
            assert len(await ledger.snapshot()) == 2
            assert (await restarted.answer(sealed)).cache_hit

    asyncio.run(run())


def test_sweeping_abandoned_cache_reservation_allows_another_key(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            answers.max_cache_entries = 1
            token = await answers._claim(sealed)
            await answers._reserve_cache(sealed, token)
            updated = await other_input(authority, sealed, "another-key")
            restarted = runtime(authority, port, answers)
            with pytest.raises(ModelError, match="model_cache_capacity"):
                await restarted.answer(updated)
            assert port.calls == [] and await ledger.snapshot() == ()
            clock[0] += timedelta(seconds=sealed.configuration.timeout_seconds * 8 + 2)
            assert await restarted.sweep_expired() == 1
            assert await restarted.sweep_expired() == 0
            assert (await restarted.answer(updated)).text == "zh-CN"
            assert len(port.calls) == 1

    asyncio.run(run())


def test_expired_inflight_slot_cannot_publish_and_keeps_unknown_debt(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)

            async def expire():
                clock[0] += timedelta(seconds=sealed.configuration.timeout_seconds * 8 + 2)

            port.before_return = expire
            with pytest.raises(ModelError, match="model_execution_fenced"):
                await answers.answer(sealed)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_records(scope, "model_cache_body") == ()
                assert await uow.derived_get(scope, "model_cache_header", sealed.key) == {
                    "state": "expired"
                }
            (call,) = await ledger.snapshot()
            assert call["state"] == "reconciliation_pending" and call["outcome"] == "failed"
            assert len(port.calls) == 1
            port.before_return = None
            assert (await answers.answer(sealed)).text == "zh-CN"
            assert len(port.calls) == 2 and len(await ledger.snapshot()) == 2

    asyncio.run(run())


def test_full_cache_audit_census_rejects_before_provider_or_money_reservation(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            async with engine.repository.unit_of_work() as uow:
                await uow.lock_admission_scope(scope)
                for index in range(4096):
                    await uow.derived_put(
                        scope, "model_cache_header", str(index), {"state": "expired"}
                    )
            with pytest.raises(ModelError, match="model_cache_audit_capacity"):
                await answers.answer(sealed)
            assert port.calls == [] and await ledger.snapshot() == ()

    asyncio.run(run())


async def fill_authorizations(engine, scope, count):
    async with engine.repository.unit_of_work() as uow:
        await uow.lock_admission_scope(scope)
        for index in range(count):
            await uow.derived_put(
                scope, "model_authorization", f"retained-audit-{index}", {"state": "erased"}
            )


@pytest.mark.parametrize("existing", [4094, 4095, 4096])
def test_dispatch_reserves_both_authorizations_at_exact_audit_boundary(store, existing):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            await fill_authorizations(engine, scope, existing)
            if existing == 4094:
                assert (await answers.answer(sealed)).text == "zh-CN"
                assert len(port.calls) == 1
            else:
                for _ in range(2):
                    with pytest.raises(ModelError, match="model_authorization_capacity"):
                        await answers.answer(sealed)
                assert port.calls == []
                assert await ledger.snapshot() == ()
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.derived_records(scope, "model_authorization")
                assert len(rows) == (4096 if existing == 4094 else existing)
                assert all(row["payload"].get("state") != "reserved" for row in rows)

    asyncio.run(run())


def test_other_key_dispatch_and_cache_hit_cannot_take_held_delivery_slot(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            await fill_authorizations(engine, scope, 4092)
            updated = await other_input(authority, sealed, "new-dispatch")
            competing = await other_input(authority, sealed, "competing-dispatch")
            entered, release = asyncio.Event(), asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()

            port.before_return = slow
            current = asyncio.create_task(answers.answer(updated))
            try:
                await asyncio.wait_for(entered.wait(), 5)
                for item in (sealed, competing):
                    with pytest.raises(ModelError, match="model_authorization_capacity"):
                        await answers.answer(item)
                async with engine.repository.unit_of_work() as uow:
                    rows = await uow.derived_records(scope, "model_authorization")
                    assert len(rows) == 4096
                    assert sum(row["payload"].get("state") == "reserved" for row in rows) == 1
                assert len(port.calls) == 2
            finally:
                release.set()
                assert (await current).text == "zh-CN"
            assert len(port.calls) == 2

    asyncio.run(run())


@pytest.mark.parametrize("separate_runtime", [False, True])
def test_concurrent_waiters_consume_first_delivery_reservation_exactly_once(
    store, separate_runtime
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            await fill_authorizations(engine, scope, 4094)
            entered, release = asyncio.Event(), asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()

            port.before_return = slow
            other = runtime(authority, port, answers) if separate_runtime else answers
            first = asyncio.create_task(answers.answer(sealed))
            await asyncio.wait_for(entered.wait(), 5)
            second = asyncio.create_task(other.answer(sealed))
            await asyncio.sleep(0.05)
            release.set()
            results = await asyncio.gather(first, second, return_exceptions=True)
            assert sum(not isinstance(result, BaseException) for result in results) == 1
            errors = [result for result in results if isinstance(result, BaseException)]
            assert len(errors) == 1 and isinstance(errors[0], ModelError)
            assert errors[0].code == "model_authorization_capacity"
            assert len(port.calls) == 1 and len(await ledger.snapshot()) == 1
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.derived_records(scope, "model_authorization")
                assert len(rows) == 4096
                assert sum(row["payload"].get("stage") == "delivery" for row in rows) == 1
                assert all(row["payload"].get("state") != "reserved" for row in rows)

    asyncio.run(run())


def test_cancelled_original_waiter_keeps_delivery_slot_for_restarted_cache_reader(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            await fill_authorizations(engine, scope, 4094)
            entered, release = asyncio.Event(), asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()

            port.before_return = slow
            first = asyncio.create_task(answers.answer(sealed))
            await asyncio.wait_for(entered.wait(), 5)
            execution = answers._tasks[sealed.key]
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            release.set()
            await execution
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.derived_records(scope, "model_authorization")
                assert sum(row["payload"].get("state") == "reserved" for row in rows) == 1
            restarted = runtime(authority, port, answers)
            answer = await restarted.answer(sealed)
            assert answer.cache_hit and answer.text == "zh-CN"
            assert len(port.calls) == 1

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["provider", "serializer"])
def test_failure_preserves_audit_and_holds_published_slot_for_other_waiters(store, failure):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            await fill_authorizations(engine, scope, 4094)

            async def broken_provider():
                raise RuntimeError("synthetic failure")

            def broken_serializer(answer):
                raise ValueError("synthetic serialization failure")

            if failure == "provider":
                port.before_return = broken_provider
            with pytest.raises((ModelError, ValueError)):
                await answers.answer(
                    sealed, serialize=broken_serializer if failure == "serializer" else None
                )
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.derived_records(scope, "model_authorization")
                assert len(rows) == (4095 if failure == "provider" else 4096)
                assert sum(row["payload"].get("stage") == "dispatch" for row in rows) == 1
                assert sum(row["payload"].get("state") == "reserved" for row in rows) == (
                    0 if failure == "provider" else 1
                )
            assert len(port.calls) == 1
            assert (await ledger.snapshot())[0]["state"] == "reconciliation_pending"
            if failure == "serializer":
                assert (await answers.answer(sealed)).cache_hit
                assert len(port.calls) == 1

    asyncio.run(run())


def test_expiry_reclaims_only_unused_audit_reservation_without_money_release(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            await fill_authorizations(engine, scope, 4093)
            result = await answers._execute(sealed)
            reservation = result[3]
            clock[0] += timedelta(
                seconds=sealed.configuration.timeout_seconds * 8 + answers.cache_seconds + 2
            )
            updated = await other_input(authority, sealed, "after-expiry")
            assert (await answers.answer(updated)).text == "zh-CN"
            async with engine.repository.unit_of_work() as uow:
                assert (
                    await uow.derived_get(scope, "model_authorization", reservation["id"]) is None
                )
                rows = await uow.derived_records(scope, "model_authorization")
                assert len(rows) == 4096
                assert sum(row["payload"].get("stage") == "dispatch" for row in rows) == 2
            assert len(port.calls) == 2
            assert all(row["state"] == "reconciliation_pending" for row in await ledger.snapshot())

    asyncio.run(run())


def test_failed_serializer_cannot_free_shared_slot_for_an_unrelated_cache_hit(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, _, port, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            await fill_authorizations(engine, scope, 4092)
            updated = await other_input(authority, sealed, "held-for-valid-waiter")

            def broken(answer):
                raise ValueError("synthetic failed waiter")

            with pytest.raises(ValueError, match="synthetic failed waiter"):
                await answers.answer(updated, serialize=broken)
            with pytest.raises(ModelError, match="model_authorization_capacity"):
                await answers.answer(sealed)
            valid = await answers.answer(updated)
            assert valid.cache_hit and valid.text == "zh-CN"
            assert len(port.calls) == 2

    asyncio.run(run())


def test_publication_rechecks_lease_after_last_storage_await(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_edges

            async def expire_after_write(self, scope, revision_id, values):
                await original(self, scope, revision_id, values)
                if revision_id == "model-cache:" + sealed.key and values:
                    clock[0] += timedelta(seconds=sealed.configuration.timeout_seconds * 8 + 2)

            monkeypatch.setattr(cls, "derived_edges", expire_after_write)
            with pytest.raises(ModelError, match="model_execution_fenced"):
                await answers.answer(sealed)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_records(scope, "model_cache_body") == ()
                rows = await uow.derived_records(scope, "model_authorization")
                assert len(rows) == 1 and rows[0]["payload"]["stage"] == "dispatch"
            assert len(port.calls) == 1
            assert (await ledger.snapshot())[0]["state"] == "reconciliation_pending"

    asyncio.run(run())


def test_dispatch_rechecks_lease_after_persisting_intent(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            original = answers.budget.intent

            async def expire_after_intent(uow, key, request_sha256):
                await original(uow, key, request_sha256)
                clock[0] += timedelta(seconds=sealed.configuration.timeout_seconds * 8 + 2)

            answers.budget.intent = expire_after_intent
            with pytest.raises(ModelError, match="model_execution_fenced"):
                await answers.answer(sealed)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_records(scope, "model_authorization") == ()
            assert port.calls == []
            assert (await ledger.snapshot())[0]["state"] == "released"

    asyncio.run(run())


def test_uncertain_publication_commit_keeps_its_durable_first_delivery_slot(store, monkeypatch):
    from contextlib import asynccontextmanager

    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            await fill_authorizations(engine, scope, 4094)
            original = engine.repository.unit_of_work
            armed = True

            @asynccontextmanager
            async def commit_then_fail():
                nonlocal armed
                async with original() as uow:
                    before = await uow.derived_get(scope, "model_cache_header", sealed.key)
                    yield uow
                    after = await uow.derived_get(scope, "model_cache_header", sealed.key)
                    fail = (
                        armed and not (before or {}).get("call_id") and (after or {}).get("call_id")
                    )
                if fail:
                    armed = False
                    raise RuntimeError("synthetic lost publication acknowledgment")

            monkeypatch.setattr(engine.repository, "unit_of_work", commit_then_fail)
            with pytest.raises(ModelError, match="model_execution_failed"):
                await answers.answer(sealed)
            async with original() as uow:
                rows = await uow.derived_records(scope, "model_authorization")
                assert len(rows) == 4096
                slots = [r for r in rows if r["payload"].get("state") == "reserved"]
                assert len(slots) == 1
            answer = await answers.answer(sealed)
            assert answer.cache_hit and answer.delivery_id == slots[0]["identity"]
            assert len(port.calls) == 1 and len(await ledger.snapshot()) == 1

    asyncio.run(run())
