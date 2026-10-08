"""Synthetic transport and governance contracts; never real model-quality evidence."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest
import test_atom_admission as base
from test_derived_controls import configured
from test_durable_purge import source_id

from agent_memory.derived import DerivedError
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
from agent_memory.retrieval.model_answers import GovernedModelAnswers
from agent_memory.retrieval.model_authority import SourceModelAuthority
from agent_memory.retrieval.model_contracts import (
    ModelConfiguration,
    ModelCoordinates,
    ModelError,
    ModelResponse,
    canonical,
    digest,
)

store = base.store
TEMPLATE = "Return only a supported language code from the numbered sources. Preserve unknowns."


def configuration(**changes):
    cfg = ModelConfiguration(
        provider="ollama",
        endpoint="http://localhost:11434",
        account="local-host",
        region="host-local",
        processing_policy="test-only/1",
        model="qwen3.5:9b",
        model_revision="a" * 64,
        runtime_manifest_sha256="b" * 64,
        overflow_guard_sha256="f" * 64,
        tokenizer_revision="unavailable-byte-bound/1",
        prompt_revision="1",
        template_sha256=digest(TEMPLATE),
        options_json=canonical({"num_ctx": 8192, "num_predict": 128}),
        output_schema_json="{}",
        output_revision="text/1",
        language="en",
        max_input_bytes=32768,
        max_output_bytes=4096,
        timeout_seconds=5,
    )
    return replace(cfg, **changes)


class StubPort:
    def __init__(self, cfg, before_return=None):
        self.configuration = cfg
        self.calls = []
        self.before_return = before_return

    async def generate(self, sealed):
        self.calls.append(sealed)
        if self.before_return:
            await self.before_return()
        return ModelResponse("zh-CN", 80, 4, 1000)


async def setup(engine, kernel, scope, clock, *, inputs=2, callback=None):
    service, *_ = await configured(engine, kernel, scope, clock, inputs=inputs)
    cfg = configuration()
    proof_current = [True]

    async def query_guard(uow, coordinates):
        return proof_current[0]

    authority = SourceModelAuthority(
        service, public_template=TEMPLATE, configuration=cfg, verify_coordinates=query_guard
    )
    ids = tuple(source_id(scope, str(i)) for i in range(1, inputs + 1))
    for key in ids:
        await authority.allow_processing(
            key,
            readers=("alice",),
            purposes=("agent_context",),
            expires_at=clock[0] + timedelta(hours=1),
        )
    coordinates = ModelCoordinates(
        scope.partition_key(),
        "alice",
        "project-a",
        "agent_context",
        "alice",
        "language",
        "1",
        "{}",
        "explain_language",
        "c" * 64,
        "d" * 64,
        "e" * 64,
        clock[0].isoformat(),
        clock[0].isoformat(),
    )
    sealed = await authority.prepare(coordinates, ids)
    ledger = ModelBudget(engine.repository)
    accounts = await ledger.configure((BudgetAccount("shared", "run", "USD", "1", None),))
    port = StubPort(cfg, callback)
    answers = GovernedModelAnswers(
        authority, port, account_keys=accounts, validate_output=lambda value, _: value == "zh-CN"
    )
    return authority, sealed, ledger, port, answers, proof_current


def test_cached_answer_keeps_full_input_lineage_and_unknown_local_cost(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            first = await answers.answer(sealed)
            second = await answers.answer(sealed)
            assert first.text == second.text == "zh-CN"
            assert first.call_id == second.call_id and first.delivery_id != second.delivery_id
            assert not first.cache_hit and second.cache_hit
            assert first.cost_status == "unknown"
            assert len(port.calls) == 1
            manifest = json.loads(sealed.manifest_json)
            assert len(manifest["sources"]) == 2
            assert all(s in sealed.payload_json for s in manifest["sources"])
            calls = await ledger.snapshot()
            assert len(calls) == 1 and calls[0]["state"] == "reconciliation_pending"
            assert calls[0]["actual_microunits"] is None
            assert calls[0]["input_tokens"] == 80
            async with engine.repository.unit_of_work() as uow:
                headers = await uow.derived_records(scope, "model_cache_header")
                assert headers[0]["payload"]["generation_manifest"] == manifest
                auth = await uow.derived_records(scope, "model_authorization")
                assert [r["payload"]["stage"] for r in auth].count("dispatch") == 1
                assert [r["payload"]["stage"] for r in auth].count("delivery") == 2

    asyncio.run(run())


@pytest.mark.parametrize(
    "change",
    [
        "principal",
        "project",
        "purpose",
        "task_intent",
        "question_version",
        "parameters_json",
        "context_sha256",
        "policy_sha256",
        "query_proof_sha256",
        "valid_at",
        "known_at",
    ],
)
def test_exact_coordinates_never_use_semantic_similarity(change):
    values = dict(
        scope_key="scope",
        principal="alice",
        project="p",
        purpose="context",
        audience="alice",
        question_definition="q",
        question_version="1",
        parameters_json="{}",
        task_intent="why",
        context_sha256="a" * 64,
        policy_sha256="a" * 64,
        query_proof_sha256="a" * 64,
        valid_at="2026-10-01T00:00:00+00:00",
        known_at="2026-10-01T00:00:00+00:00",
    )
    old = ModelCoordinates(**values)
    value = (
        "b" * 64
        if change.endswith("sha256")
        else '{"project":"other"}'
        if change.endswith("json")
        else "2026-10-02T00:00:00+00:00"
        if change.endswith("_at")
        else "other"
    )
    assert digest(old.payload()) != digest(replace(old, **{change: value}).payload())


@pytest.mark.parametrize(
    "change,value",
    [
        ("endpoint", "http://localhost:11435"),
        ("model", "other:9b"),
        ("model_revision", "c" * 64),
        ("prompt_revision", "2"),
        ("tokenizer_revision", "2"),
        ("language", "zh"),
        ("options_json", '{"num_ctx":8192,"num_predict":128,"temperature":0}'),
        ("output_schema_json", '{"type":"object"}'),
        ("output_revision", "2"),
        ("runtime_manifest_sha256", "c" * 64),
        ("max_input_bytes", 40000),
        ("think", True),
    ],
)
def test_exact_configuration_change_always_misses(change, value):
    assert configuration().fingerprint != configuration(**{change: value}).fingerprint


def test_no_read_permission_implies_provider_processing_permission(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            service, *_ = await configured(engine, kernel, scope, clock)
            cfg = configuration()

            async def guard(uow, coords):
                return True

            authority = SourceModelAuthority(
                service, public_template=TEMPLATE, configuration=cfg, verify_coordinates=guard
            )
            coordinates = ModelCoordinates(
                scope.partition_key(),
                "alice",
                "p",
                "agent_context",
                "alice",
                "q",
                "1",
                "{}",
                "why",
                "a" * 64,
                "b" * 64,
                "c" * 64,
                clock[0].isoformat(),
                clock[0].isoformat(),
            )
            calls = []
            cls = type(engine.repository.unit_of_work())
            original = cls.get_source_event

            async def spy(self, *args):
                calls.append(True)
                return await original(self, *args)

            monkeypatch.setattr(cls, "get_source_event", spy)
            with pytest.raises(ModelError, match="processing_unauthorized"):
                await authority.prepare(coordinates, (source_id(scope, "1"),))
            assert calls == []

    asyncio.run(run())


@pytest.mark.parametrize("when", ["before_dispatch", "inflight", "cache_hit"])
def test_erasure_prevents_all_new_delivery_and_scrubs_cached_bodies(store, when):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)

            async def erase():
                await kernel.forget(
                    ForgetRequest(scope, (source_id(scope, "2"),), mode=ForgetMode.ERASE)
                )

            if when == "cache_hit":
                await answers.answer(sealed)
                await erase()
            elif when == "inflight":
                port.before_return = erase
            else:
                await erase()
            with pytest.raises((ModelError, DerivedError)):
                await answers.answer(sealed)
            assert len(port.calls) == (0 if when == "before_dispatch" else 1)
            async with engine.repository.unit_of_work() as uow:
                for kind in (
                    "model_cache_body",
                    "model_cache_header",
                    "model_authorization",
                    "model_flight",
                ):
                    assert all(
                        r["payload"].get("state") == "erased"
                        for r in await uow.derived_records(scope, kind)
                    )
            calls = await ledger.snapshot()
            if when != "before_dispatch":
                assert len(calls) == 1 and calls[0]["actual_microunits"] is None

    asyncio.run(run())


def test_query_changed_after_model_start_refuses_publication(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, proof = await setup(
                engine, kernel, scope, clock
            )

            async def changed():
                proof[0] = False

            port.before_return = changed
            with pytest.raises(ModelError, match="query_proof_unavailable"):
                await answers.answer(sealed)
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_records(scope, "model_cache_body") == ()

    asyncio.run(run())


def test_tampered_or_omitted_manifest_and_payload_cannot_dispatch(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            manifest = json.loads(sealed.manifest_json)
            manifest["sources"].pop(source_id(scope, "2"))
            bad = replace(sealed, manifest_json=canonical(manifest))
            with pytest.raises(ModelError, match="model_input_changed"):
                await answers.answer(bad)
            assert not port.calls

    asyncio.run(run())


def test_cancel_one_waiter_keeps_one_call_and_other_waiters(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            entered, release = asyncio.Event(), asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()

            authority, sealed, ledger, port, answers, _ = await setup(
                engine, kernel, scope, clock, callback=slow
            )
            first = asyncio.create_task(answers.answer(sealed))
            await entered.wait()
            second = asyncio.create_task(answers.answer(sealed))
            await asyncio.sleep(0)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            release.set()
            assert (await second).text == "zh-CN"
            assert len(port.calls) == 1

    asyncio.run(run())


def test_two_runtime_instances_share_a_durable_flight(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            entered, release = asyncio.Event(), asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()

            authority, sealed, ledger, port, answers, _ = await setup(
                engine, kernel, scope, clock, callback=slow
            )
            other = GovernedModelAnswers(
                authority,
                port,
                account_keys=answers.account_keys,
                validate_output=lambda value, _: True,
            )
            first = asyncio.create_task(answers.answer(sealed))
            await entered.wait()
            second = asyncio.create_task(other.answer(sealed))
            await asyncio.sleep(0.05)
            release.set()
            assert (await first).text == (await second).text
            assert len(port.calls) == 1

    asyncio.run(run())


def test_old_backup_erasure_scrubs_model_outputs_but_keeps_minimal_cost_debt(store, tmp_path):
    from test_purge_restore import backup_copy, erase, replay, restorer

    from agent_memory.derived import ObservationService

    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await erase(kernel, scope, source_id(scope, "2"))
                checkpoint = await restorer(engine.repository, scope, clock).export()
                await replay(restorer(backup, scope, clock), checkpoint)
                service = ObservationService(
                    backup,
                    scope,
                    base.POLICY,
                    clock=lambda: clock[0],
                    authority_id="local-host",
                    authority_min_version=1,
                )

                async def proof(uow, coordinates):
                    return True

                restored = SourceModelAuthority(
                    service,
                    public_template=TEMPLATE,
                    configuration=sealed.configuration,
                    verify_coordinates=proof,
                )
                stopped = GovernedModelAnswers(
                    restored,
                    port,
                    account_keys=answers.account_keys,
                    validate_output=lambda *_: True,
                )
                with pytest.raises((ModelError, DerivedError)):
                    await stopped.answer(sealed)
                assert len(port.calls) == 1
                async with backup.unit_of_work() as uow:
                    for kind in (
                        "model_cache_header",
                        "model_cache_body",
                        "model_authorization",
                        "model_flight",
                    ):
                        rows = await uow.derived_records(scope, kind)
                        assert rows and all(row["payload"] == {"state": "erased"} for row in rows)
                (debt,) = await ModelBudget(backup).snapshot()
                assert debt["state"] == "reconciliation_pending"
                assert debt["actual_microunits"] is None
                assert "source" not in debt and "response" not in debt

    asyncio.run(run())


def test_settled_output_validation_failure_remains_billed_and_records_failure(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)

            async def measured(sealed):
                return ModelResponse("bad", 8, 3, 10, actual_microunits=12, cost_evidence="invoice")

            port.generate = measured
            with pytest.raises(ModelError, match="output_validation_failed"):
                await answers.answer(sealed)
            (row,) = await ledger.snapshot()
            assert row["state"] == "settled" and row["actual_microunits"] == 12
            assert row["outcome"] == "failed"

    asyncio.run(run())


def test_private_untyped_provider_failure_is_sanitized(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)

            async def failure():
                raise RuntimeError("private-source-body must never reach the user")

            port.before_return = failure
            with pytest.raises(ModelError, match="model_execution_failed") as error:
                await answers.answer(sealed)
            assert "private-source" not in str(error.value)
            assert (await ledger.snapshot())[0]["actual_microunits"] is None

    asyncio.run(run())


def test_expired_cache_maintenance_scrubs_bodies_without_model_work(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(sealed)
            clock[0] += timedelta(seconds=301)
            assert await answers.sweep_expired() == 1
            assert await answers.sweep_expired() == 0
            async with engine.repository.unit_of_work() as uow:
                assert await uow.derived_get(scope, "model_cache_body", sealed.key) == {
                    "state": "expired"
                }
                assert await uow.derived_reverse(scope, "source:" + source_id(scope, "1")) == ()
            assert len(port.calls) == 1
            assert (await ledger.snapshot())[0]["actual_microunits"] is None

    asyncio.run(run())


def test_failed_model_delivery_keeps_expiry_floor_and_blocks_clock_rollback(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            old = clock[0]

            async def expire():
                clock[0] += timedelta(hours=2)

            port.before_return = expire
            with pytest.raises(DerivedError, match="expired"):
                await answers.answer(sealed)
            clock[0] = old
            with pytest.raises(DerivedError, match="clock_discontinuity"):
                await answers.answer(sealed)
            assert len(port.calls) == 1
            assert (await ledger.snapshot())[0]["actual_microunits"] is None

    asyncio.run(run())


def test_final_envelope_budget_includes_provenance_not_only_model_text(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, sealed, ledger, port, answers, _ = await setup(engine, kernel, scope, clock)
            # The text fits this limit, while the complete public envelope does not.
            cfg = replace(sealed.configuration, max_output_bytes=16)
            revised = SourceModelAuthority(
                authority.service,
                public_template=TEMPLATE,
                configuration=cfg,
                verify_coordinates=authority.verify_coordinates,
            )
            updated = await revised.prepare(
                sealed.coordinates, tuple(json.loads(sealed.manifest_json)["sources"])
            )
            port.configuration = cfg
            governor = GovernedModelAnswers(
                revised, port, account_keys=answers.account_keys, validate_output=lambda *_: True
            )
            with pytest.raises(ModelError, match="delivery_budget_exceeded"):
                await governor.answer(updated)
            async with engine.repository.unit_of_work() as uow:
                rows = await uow.derived_records(scope, "model_authorization")
                assert not any(r["payload"].get("stage") == "delivery" for r in rows)
            assert len(port.calls) == 1

    asyncio.run(run())
