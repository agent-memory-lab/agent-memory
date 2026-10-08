"""Actual QuestionService + embedded/MCP model route, synthetic inference only."""

import asyncio
import json
from datetime import timedelta

import pytest
import test_project_admission_v7 as project
from test_governed_models_v7 import TEMPLATE, StubPort, configuration
from test_question_runtime_v7 import ACTOR, register
from test_question_transport_v7 import client_for

from agent_memory.derived.contracts import HostGrantAuthority
from agent_memory.derived.model import DerivedError, ProcessingGrant, digest
from agent_memory.derived.question_model import QuestionContext
from agent_memory.derived.question_service import QuestionService
from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
from agent_memory.retrieval.model_contracts import ModelError
from agent_memory.retrieval.question_models import QuestionModelRuntime

store = project.store


async def configured_question(engine, scope, clock, *, grant_provider=True, second=False):
    admission = project.service(
        engine, scope, clock, authority_id="model-host", authority_min_version=1
    )
    authority = HostGrantAuthority(
        "model-host", (ACTOR,), clock[0] + timedelta(hours=1), purposes=("project_questions",)
    )
    # Test host seeds its authenticated current local authority, independent of
    # the model or transport request. Production host owns this registry.
    async with engine.repository.unit_of_work() as uow:
        epoch = await uow.retention_epoch(scope)
        await uow.derived_put(
            scope,
            "authority",
            authority.id,
            dict(
                spec=authority.payload(),
                version=1,
                epoch=epoch,
                fingerprint=digest(authority.payload()),
            ),
        )
    service = QuestionService(
        admission, QuestionContext("host", "context/1", {}, clock[0] + timedelta(hours=1))
    )
    for source in ("source", "private-unused") if second else ("source",):
        item = await project.stage(admission, scope, identity=source)
        await service.grant(
            ProcessingGrant(source, (ACTOR,), ("project_questions",)), expected_version=1
        )
        if source == "source":
            await project.qualify(admission, *item)
        else:
            await admission.reject(
                item[2], expected_version=2, review_id="reject", reasons=("unsupported",)
            )
    await register(service)
    clock[0] += timedelta(microseconds=100)
    await service.answer("project-a:owner", actor=ACTOR, dedupe_key="build")
    ledger = ModelBudget(engine.repository)
    accounts = await ledger.configure((BudgetAccount("global", "run", "USD", "1", None),))
    port = StubPort(configuration())
    models = QuestionModelRuntime(
        service,
        port,
        public_template=TEMPLATE,
        account_keys=accounts,
        validate_output=lambda value, _: value == "zh-CN",
    )
    if grant_provider:
        for source in ("source", "private-unused") if second else ("source",):
            await models.authority.allow_processing(
                source,
                readers=(ACTOR,),
                purposes=("project_questions",),
                expires_at=clock[0] + timedelta(minutes=30),
            )
    return service, models, ledger, port


@pytest.mark.parametrize("transport", ["embedded", "mcp"])
def test_explicit_model_route_uses_registered_real_question_and_exact_cache(store, transport):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock, second=True)
            async with client_for(kernel, scope, svc, transport) as client:
                capability = await client.question_capabilities()
                assert capability["models"] and "model_answer" in capability["operations"]
                assert capability["model_runtime"]["experimental"]
                assert not capability["model_runtime"]["strict_cost_budget"]
                assert (await client.question_answer("project-a:owner", dedupe_key="ordinary"))[
                    "model_calls"
                ] == 0
                assert not port.calls
                first = await client.question_model_answer("project-a:owner")
                second = await client.question_model_answer("project-a:owner")
                assert not first["cache_hit"] and second["cache_hit"]
                assert first["call_id"] == second["call_id"]
                assert first["delivery_id"] != second["delivery_id"]
                assert first["experimental"] and first["cost_status"] == "unknown"
                assert len(port.calls) == 1 and len(await ledger.snapshot()) == 1
                sealed = port.calls[0]
                manifest = json.loads(sealed.manifest_json)
                assert set(manifest["sources"]) == {"source", "private-unused"}
                assert manifest["inherited_generation_manifest"]
                result = json.loads(json.loads(sealed.payload_json)["messages"][1]["content"])
                assert result["result"]["rows"][0]["fields"][0]["known_values"] == ["Alice"]
                async with engine.repository.unit_of_work() as uow:
                    rows = await uow.derived_records(scope, "model_authorization")
                    assert len([r for r in rows if r["payload"]["stage"] == "delivery"]) == 2

    asyncio.run(run())


def test_provider_permission_precedes_all_question_bodies(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(
                engine, scope, clock, grant_provider=False
            )
            cls = type(engine.repository.unit_of_work())
            original = cls.derived_get
            bodies = []

            async def spy(self, scope, kind, identity):
                if kind in {"question_content", "question_certificate", "model_cache_body"}:
                    bodies.append(kind)
                return await original(self, scope, kind, identity)

            monkeypatch.setattr(cls, "derived_get", spy)
            with pytest.raises(ModelError, match="processing_unauthorized"):
                await models.answer("project-a:owner", actor=ACTOR)
            assert bodies == [] and port.calls == [] and await ledger.snapshot() == ()

    asyncio.run(run())


@pytest.mark.parametrize("when", ["before_dispatch", "inflight", "cache_hit"])
def test_question_provider_revocation_stops_new_call_or_delivery(store, when):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock)

            async def revoke():
                await models.authority.allow_processing(
                    "source",
                    readers=(ACTOR,),
                    purposes=("project_questions",),
                    expires_at=clock[0] + timedelta(minutes=30),
                    revoked=True,
                    expected_version=1,
                )

            if when == "cache_hit":
                await models.answer("project-a:owner", actor=ACTOR)
                await revoke()
            elif when == "inflight":
                port.before_return = revoke
            else:
                await revoke()
            with pytest.raises(ModelError, match="processing_unauthorized"):
                await models.answer("project-a:owner", actor=ACTOR)
            assert len(port.calls) == (0 if when == "before_dispatch" else 1)

    asyncio.run(run())


def test_model_request_cannot_supply_authority_or_context(store):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock)
            async with client_for(kernel, scope, svc, "embedded") as client:
                for field in (
                    "actor",
                    "purpose",
                    "model",
                    "prompt",
                    "source_ids",
                    "query_proof",
                    "context",
                ):
                    with pytest.raises(sdk.MemoryClientError, match="invalid_question_request"):
                        await client._call(
                            "memory_question",
                            {
                                "operation": "model_answer",
                                "payload": {"question_id": "project-a:owner", field: "injected"},
                            },
                        )
            assert not port.calls

    asyncio.run(run())


def test_noop_certificate_keeps_original_private_model_lineage_and_forces_exact_miss(
    store, monkeypatch
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock, second=True)
            first = await models.answer("project-a:owner", actor=ACTOR)
            original = json.loads(port.calls[0].manifest_json)
            await svc.grant(
                ProcessingGrant("source", (ACTOR,), ("project_questions",)), expected_version=2
            )
            clock[0] += timedelta(microseconds=100)
            view = await svc.answer("project-a:owner", actor=ACTOR, dedupe_key="proof-only")
            assert view["compute_mode"] == "proof_reuse"
            assert view["generation_manifest"] == original["inherited_generation_manifest"]
            second = await models.answer("project-a:owner", actor=ACTOR)
            assert not second["cache_hit"] and first["call_id"] != second["call_id"]
            assert len(port.calls) == 2
            assert "private-unused" in json.loads(port.calls[1].manifest_json)["sources"]
            await models.authority.allow_processing(
                "private-unused",
                readers=(ACTOR,),
                purposes=("project_questions",),
                expires_at=clock[0] + timedelta(minutes=30),
                revoked=True,
                expected_version=1,
            )
            cls = type(engine.repository.unit_of_work())
            read = cls.derived_get
            bodies = []

            async def spy(self, scope, kind, key):
                if kind in {"question_content", "model_cache_body"}:
                    bodies.append(kind)
                return await read(self, scope, kind, key)

            monkeypatch.setattr(cls, "derived_get", spy)
            with pytest.raises(ModelError, match="processing_unauthorized"):
                await models.answer("project-a:owner", actor=ACTOR)
            assert not bodies and len(port.calls) == 2

    asyncio.run(run())


def test_erasing_question_content_scrubs_dependent_model_cache(store):
    from agent_memory.domain import ForgetMode, ForgetRequest

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock)
            await models.answer("project-a:owner", actor=ACTOR)
            content = json.loads(port.calls[0].manifest_json)["question_content_revision"]
            await kernel.forget(ForgetRequest(scope, (content,), mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                for kind in (
                    "model_cache_header",
                    "model_cache_body",
                    "model_flight",
                    "model_authorization",
                ):
                    rows = await uow.derived_records(scope, kind)
                    assert rows and all(row["payload"] == {"state": "erased"} for row in rows)
            with pytest.raises((DerivedError, ModelError)):
                await models.answer("project-a:owner", actor=ACTOR)
            assert len(port.calls) == 1

    asyncio.run(run())


def test_erasing_an_inherited_atom_scrubs_question_model_output(store):
    from agent_memory.domain import ForgetMode, ForgetRequest

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock)
            await models.answer("project-a:owner", actor=ACTOR)
            manifest = json.loads(port.calls[0].manifest_json)["inherited_generation_manifest"]
            atom_id = next(item["id"] for item in manifest["inputs"] if item["kind"] == "atom")
            await kernel.forget(ForgetRequest(scope, (atom_id,), mode=ForgetMode.ERASE))
            async with engine.repository.unit_of_work() as uow:
                assert all(
                    row["payload"] == {"state": "erased"}
                    for row in await uow.derived_records(scope, "model_cache_body")
                )
            with pytest.raises((DerivedError, ModelError)):
                await models.answer("project-a:owner", actor=ACTOR)
            assert len(port.calls) == 1

    asyncio.run(run())


def test_transport_database_or_validator_exception_never_echoes_private_body(store, monkeypatch):
    sdk = pytest.importorskip("agent_memory_sdk")

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock)

            async def broken(*args, **kwargs):
                raise RuntimeError("private project source body from database failure")

            monkeypatch.setattr(models, "answer", broken)
            async with client_for(kernel, scope, svc, "embedded") as client:
                with pytest.raises(
                    sdk.MemoryClientError, match="model_execution_unavailable"
                ) as error:
                    await client.question_model_answer("project-a:owner")
                assert "private project" not in str(error.value)

    asyncio.run(run())
