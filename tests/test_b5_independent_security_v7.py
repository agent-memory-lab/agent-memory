"""Independent adversarial regressions for exact B5 dispatch and delivery binding.

Synthetic transports only. These assertions express the required safety contract,
not the vulnerable behavior, and are intentionally independent of existing tests.
"""

import asyncio
import threading
from dataclasses import asdict, replace
from hashlib import sha256

import pytest
import test_atom_admission as base
from test_governed_models_v7 import configuration, setup
from test_ollama_port_v7 import sealed, server

from agent_memory.retrieval.model_contracts import ModelError, canonical, digest
from agent_memory.retrieval.ollama import OllamaPort

store = base.store


def test_reconfiguration_during_preflight_never_sends_body_to_new_recipient():
    """A host changing port.configuration while generate awaits cannot retarget I/O."""

    async def run():
        with server() as (endpoint_a, received_a, runtime), server() as (endpoint_b, received_b, _):
            cfg = configuration(endpoint=endpoint_a, runtime_manifest_sha256=digest(runtime))
            port = OllamaPort(cfg)
            request = sealed(cfg)
            entered, release = threading.Event(), threading.Event()
            original = port._verify_installation
            calls = 0

            def pause_after_preflight():
                nonlocal calls
                original()
                calls += 1
                if calls == 1:
                    entered.set()
                    assert release.wait(10)

            port._verify_installation = pause_after_preflight
            task = asyncio.create_task(port.generate(request))
            try:
                assert await asyncio.to_thread(entered.wait, 10)
                port.configuration = replace(
                    cfg,
                    endpoint=endpoint_b,
                    account="different-account",
                    region="different-region",
                    processing_policy="different-policy/2",
                    model="different-model:1b",
                    model_revision="c" * 64,
                    runtime_manifest_sha256="d" * 64,
                    options_json=canonical({"num_ctx": 128, "num_predict": 1}),
                    max_output_bytes=1,
                    timeout_seconds=1,
                )
            finally:
                release.set()
            try:
                await task
            except ModelError:
                pass  # Rejecting an in-flight reconfiguration is also safe.
            assert not any(path == "/api/chat" for path, _ in received_b), (
                "Sealed input was authorized only for endpoint A but endpoint B received it"
            )
            assert all(
                body == request.payload_json.encode()
                for path, body in received_a
                if path == "/api/chat"
            )

    asyncio.run(run())


@pytest.mark.parametrize("mutation", ["top_level", "nested"])
def test_serializer_alias_cannot_change_delivery_after_hash_is_frozen(store, mutation):
    """The returned envelope must equal the bytes whose delivery was authorized."""

    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, request, _, _, answers, _ = await setup(engine, kernel, scope, clock)
            await answers.answer(request)  # Isolate final delivery from provider execution.
            shared = {}
            original = authority.record

            async def pause_before_delivery(uow, sealed_input, stage, **kwargs):
                if stage == "delivery":
                    # Represents another task retaining the serializer's dictionary.
                    await asyncio.sleep(0)
                    if mutation == "top_level":
                        shared["text"] = "unvalidated replacement after serialization"
                    else:
                        shared["nested"]["items"][0]["text"] = "unvalidated nested replacement"
                return await original(uow, sealed_input, stage, **kwargs)

            authority.record = pause_before_delivery

            def serialize(answer):
                shared.update(asdict(answer))
                shared["nested"] = {"items": [{"text": answer.text}]}
                return shared

            result = await answers.answer(request, serialize=serialize)
            async with engine.repository.unit_of_work() as uow:
                row = await uow.derived_get(scope, "model_authorization", result["delivery_id"])
            assert row["payload_sha256"] == sha256(canonical(result).encode()).hexdigest(), (
                "Returned payload differs from the authorized serialized delivery"
            )
            assert result["text"] == "zh-CN"
            assert result["nested"] == {"items": [{"text": "zh-CN"}]}

    asyncio.run(run())


def test_source_authority_rechecks_expiry_before_each_body_load(store, monkeypatch):
    from datetime import timedelta

    from agent_memory.derived import DerivedError

    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, request, _, _, _, _ = await setup(engine, kernel, scope, clock)
            import json

            source_ids = tuple(sorted(json.loads(request.manifest_json)["sources"]))
            loaded = []
            cls = type(engine.repository.unit_of_work())
            original = cls.get_source_event

            async def expire_after_first_read(self, requested_scope, source_id):
                loaded.append(source_id)
                result = await original(self, requested_scope, source_id)
                if len(loaded) == 1:
                    clock[0] += timedelta(hours=2)
                    await asyncio.sleep(0)
                return result

            monkeypatch.setattr(cls, "get_source_event", expire_after_first_read)
            try:
                await authority.prepare(request.coordinates, source_ids)
            except (ModelError, DerivedError):
                pass
            assert loaded == [source_ids[0]], (
                "The second protected source body was read after provider/read authority expired"
            )

    asyncio.run(run())


def test_settlement_cannot_replace_known_provider_request_identity(store):
    from test_model_budget_v7 import intent, reserve

    from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
    from agent_memory.retrieval.model_contracts import ModelResponse

    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            accounts = await budget.configure((BudgetAccount("global", "run", "USD", "1", 100),))
            row = await reserve(budget, accounts)
            await intent(budget, row)
            await budget.pending(
                row["key"],
                response=ModelResponse(
                    "answer", 20, 5, 10, provider_request_id="actual-dispatched-request"
                ),
            )
            try:
                await budget.settle(
                    row["key"],
                    actual_microunits=0,
                    receipt_id="unrelated-free-request-invoice",
                    provider_request_id="unrelated-request",
                )
            except ModelError:
                pass
            (current,) = await budget.snapshot()
            assert current["state"] == "reconciliation_pending", (
                "Unrelated provider request receipt released the known dispatched request's debt"
            )
            assert current["provider_request_sha256"] == digest("actual-dispatched-request")
            async with engine.repository.unit_of_work() as uow:
                account = await uow.model_budget_get("account", accounts[0])
            assert account["reserved"] == 100

    asyncio.run(run())


@pytest.mark.parametrize("revision", ["current", "historical"])
@pytest.mark.parametrize("restore", [False, True], ids=["live", "backup_replay"])
def test_erasing_question_certificate_scrubs_dependent_model_bodies(
    store, tmp_path, revision, restore
):
    import json
    from contextlib import asynccontextmanager
    from datetime import timedelta

    from test_purge_restore import backup_copy, replay, restorer
    from test_question_models_v7 import ACTOR, configured_question

    from agent_memory.derived import DerivedError, ProcessingGrant
    from agent_memory.domain import ForgetMode, ForgetRequest
    from agent_memory.operations.model_budget import ModelBudget

    @asynccontextmanager
    async def target_repository(repository):
        if restore:
            async with backup_copy(repository, tmp_path) as (backup, _):
                yield backup
        else:
            yield repository

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, ledger, port = await configured_question(engine, scope, clock)
            await models.answer("project-a:owner", actor=ACTOR)
            payload = json.loads(port.calls[0].payload_json)
            question = json.loads(payload["messages"][1]["content"])
            certificate_id = question["certificate_revision_id"]
            if revision == "historical":
                await svc.grant(
                    ProcessingGrant("source", (ACTOR,), ("project_questions",)),
                    expected_version=2,
                )
                clock[0] += timedelta(microseconds=100)
                await svc.answer("project-a:owner", actor=ACTOR, dedupe_key="proof-only")
                await models.answer("project-a:owner", actor=ACTOR)
                assert len(port.calls) == 2
                newer = json.loads(
                    json.loads(port.calls[-1].payload_json)["messages"][1]["content"]
                )
                assert newer["certificate_revision_id"] != certificate_id
            async with target_repository(engine.repository) as target:
                await kernel.forget(ForgetRequest(scope, (certificate_id,), mode=ForgetMode.ERASE))
                if restore:
                    deletion = await restorer(engine.repository, scope, clock).export()
                    await replay(restorer(target, scope, clock), deletion)
                    money = await ledger.export()
                    await ModelBudget(target).replay(money, expected_checkpoint=money["checkpoint"])
                async with target.unit_of_work() as uow:
                    for kind in (
                        "model_cache_body",
                        "model_cache_header",
                        "model_flight",
                        "model_authorization",
                    ):
                        rows = await uow.derived_records(scope, kind)
                        assert rows and all(
                            row["payload"] == {"state": "erased"} for row in rows
                        ), f"Erasing the input certificate left dependent {kind} alive"
                debts = await ModelBudget(target).snapshot()
                assert len(debts) == len(port.calls)
                assert all(row["actual_microunits"] is None for row in debts)
            with pytest.raises((ModelError, DerivedError)):
                await models.answer("project-a:owner", actor=ACTOR)

    asyncio.run(run())


def test_question_provider_expiry_is_checked_at_body_loading_boundary(store, monkeypatch):
    from datetime import timedelta

    from test_question_models_v7 import ACTOR, configured_question

    from agent_memory.derived import DerivedError

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, _, _ = await configured_question(engine, scope, clock)
            original_read = svc._read_in_uow
            cls = type(engine.repository.unit_of_work())
            original_get = cls.derived_get
            bodies = []

            async def delayed_read(*args, **kwargs):
                # Provider permission lasts 30 minutes, ordinary authority lasts 1 hour.
                clock[0] += timedelta(minutes=31)
                await asyncio.sleep(0)
                return await original_read(*args, **kwargs)

            async def spy(self, scope, kind, key):
                if kind in {"question_content", "question_certificate"}:
                    bodies.append(kind)
                return await original_get(self, scope, kind, key)

            monkeypatch.setattr(cls, "derived_get", spy)
            svc._read_in_uow = delayed_read
            try:
                await models.authority.prepare_question("project-a:owner", actor=ACTOR)
            except (ModelError, DerivedError):
                pass
            assert bodies == [], (
                "Question bodies were read for model processing after the provider grant expired"
            )

    asyncio.run(run())


@pytest.mark.parametrize("when", ["cached", "inflight"])
@pytest.mark.parametrize("permission", ["read", "provider"])
def test_other_actor_never_inherits_exact_cache_or_flight_authorization(store, when, permission):
    import json
    from datetime import timedelta

    from agent_memory.derived import DerivedError, HostGrantAuthority, ProcessingGrant

    async def run():
        async with store() as (engine, kernel, scope, clock):
            authority, initial, _, port, answers, _ = await setup(engine, kernel, scope, clock)
            ids = tuple(sorted(json.loads(initial.manifest_json)["sources"]))
            await authority.service.set_authority(
                HostGrantAuthority("local-host", ("alice", "bob"), clock[0] + timedelta(hours=1)),
                expected_version=1,
            )
            for key in ids:
                await authority.service.grant(
                    ProcessingGrant(key, ("alice", "bob")), expected_version=2
                )
                await authority.allow_processing(
                    key,
                    readers=("alice", "bob"),
                    purposes=("agent_context",),
                    expires_at=clock[0] + timedelta(hours=1),
                    expected_version=1,
                )
            alice = await authority.prepare(initial.coordinates, ids)
            bob = await authority.prepare(
                replace(initial.coordinates, principal="bob", audience="bob"), ids
            )
            assert alice.key != bob.key
            entered, release = asyncio.Event(), asyncio.Event()

            async def pause():
                entered.set()
                await release.wait()

            first = None
            if when == "cached":
                assert (await answers.answer(alice)).text == "zh-CN"
            else:
                port.before_return = pause
                first = asyncio.create_task(answers.answer(alice))
                await asyncio.wait_for(entered.wait(), 10)
            if permission == "read":
                await authority.service.grant(
                    ProcessingGrant(ids[0], ("alice",)), expected_version=3
                )
            else:
                await authority.allow_processing(
                    ids[0],
                    readers=("alice",),
                    purposes=("agent_context",),
                    expires_at=clock[0] + timedelta(hours=1),
                    expected_version=2,
                )
            try:
                with pytest.raises((ModelError, DerivedError)):
                    await answers.answer(bob)
                assert len(port.calls) == 1
            finally:
                release.set()
                if first:
                    with pytest.raises((ModelError, DerivedError)):
                        await first  # Alice's old proof also changed and must be renewed.
            async with engine.repository.unit_of_work() as uow:
                auth = await uow.derived_records(scope, "model_authorization")
                assert not any(r["payload"].get("key") == bob.key for r in auth)

    asyncio.run(run())


def test_erasing_one_question_certificate_preserves_unrelated_project_model_cache(store):
    import json
    from datetime import timedelta

    import test_project_admission_v7 as project
    from test_question_models_v7 import ACTOR, configured_question

    from agent_memory.derived import ProcessingGrant
    from agent_memory.domain import ForgetMode, ForgetRequest

    async def run():
        async with store() as (engine, kernel, scope, clock):
            svc, models, _, port = await configured_question(engine, scope, clock)
            other = await project.stage(
                svc.admission, scope, identity="other-source", subject="project-b", membership="b"
            )
            await svc.grant(
                ProcessingGrant("other-source", (ACTOR,), ("project_questions",)),
                expected_version=1,
            )
            await project.qualify(svc.admission, *other)
            await svc.register("project-b:owner", "project-b", "owner", readers=(ACTOR,))
            clock[0] += timedelta(microseconds=100)
            await svc.answer("project-b:owner", actor=ACTOR, dedupe_key="build-b")
            await svc.answer("project-a:owner", actor=ACTOR, dedupe_key="renew-a")
            await models.authority.allow_processing(
                "other-source",
                readers=(ACTOR,),
                purposes=("project_questions",),
                expires_at=clock[0] + timedelta(minutes=30),
            )
            await models.answer("project-a:owner", actor=ACTOR)
            unaffected = await models.answer("project-b:owner", actor=ACTOR)
            question = json.loads(json.loads(port.calls[0].payload_json)["messages"][1]["content"])
            await kernel.forget(
                ForgetRequest(scope, (question["certificate_revision_id"],), mode=ForgetMode.ERASE)
            )
            # Scope-wide deletion barriers can require a new query certificate;
            # the independent project's retained cache must not be physically erased.
            assert len(port.calls) == 2
            async with engine.repository.unit_of_work() as uow:
                body = await uow.derived_get(scope, "model_cache_body", unaffected["key"])
                assert body["response"]["text"] == "zh-CN"

    asyncio.run(run())


@pytest.mark.parametrize(
    "action", ["pending_conflict", "settle_missing", "replay_conflict", "replay_missing"]
)
def test_known_provider_identity_and_debt_survive_reconciliation_and_restore(store, action):
    from test_model_budget_v7 import intent, reserve

    from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
    from agent_memory.retrieval.model_contracts import ModelResponse

    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            accounts = await budget.configure((BudgetAccount("global", "run", "USD", "1", 300),))
            row = await reserve(budget, accounts)
            await intent(budget, row)
            await budget.pending(
                row["key"],
                response=ModelResponse(
                    "answer", 20, 5, 10, provider_request_id="actual-dispatched-request"
                ),
            )
            if action == "pending_conflict":
                with pytest.raises(ModelError):
                    await budget.pending(
                        row["key"],
                        response=ModelResponse(
                            "answer", 20, 5, 10, provider_request_id="unrelated-request"
                        ),
                    )
            elif action == "settle_missing":
                with pytest.raises(ModelError):
                    await budget.settle(
                        row["key"], actual_microunits=0, receipt_id="unmatched-free-request-invoice"
                    )
            else:
                checkpoint = await budget.export()
                external = checkpoint["calls"][0]
                external.update(
                    state="settled",
                    actual_microunits=0,
                    receipt_sha256=digest("external-receipt"),
                    provider_request_sha256=(
                        digest("unrelated-request") if action == "replay_conflict" else None
                    ),
                )
                checkpoint["checkpoint"] = digest(
                    {key: value for key, value in checkpoint.items() if key != "checkpoint"}
                )
                with pytest.raises(ModelError):
                    await budget.replay(checkpoint, expected_checkpoint=checkpoint["checkpoint"])
            (current,) = await budget.snapshot()
            assert current["state"] == "reconciliation_pending"
            assert current["provider_request_sha256"] == digest("actual-dispatched-request")
            async with engine.repository.unit_of_work() as uow:
                account = await uow.model_budget_get("account", accounts[0])
            assert account["reserved"] == 100 and account["settled"] == 0
            # Correct evidence can still settle, repeated exactly once across restores.
            for _ in range(2):
                await budget.settle(
                    row["key"],
                    actual_microunits=25,
                    receipt_id="invoice-1",
                    provider_request_id="actual-dispatched-request",
                )
            known = await budget.export()
            await budget.replay(known, expected_checkpoint=known["checkpoint"])
            second = await reserve(budget, accounts, attempt="second")
            await intent(budget, second)
            await budget.pending(
                second["key"],
                response=ModelResponse(
                    "answer", 20, 5, 10, provider_request_id="second-dispatched-request"
                ),
            )
            with pytest.raises(ModelError):
                await budget.settle(
                    second["key"],
                    actual_microunits=25,
                    receipt_id="invoice-1",
                    provider_request_id="actual-dispatched-request",
                )
            async with engine.repository.unit_of_work() as uow:
                account = await uow.model_budget_get("account", accounts[0])
            assert account["reserved"] == 100 and account["settled"] == 25

    asyncio.run(run())


@pytest.mark.parametrize("path", ["pending", "replay"])
def test_late_provider_identity_on_settled_call_cannot_be_reused_by_another_call(store, path):
    from test_model_budget_v7 import intent, reserve

    from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
    from agent_memory.retrieval.model_contracts import ModelResponse

    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            accounts = await budget.configure((BudgetAccount("global", "run", "USD", "1", 300),))
            first = await reserve(budget, accounts)
            await intent(budget, first)
            await budget.settle(first["key"], actual_microunits=25, receipt_id="early-invoice")
            late = ModelResponse("answer", 20, 5, 10, provider_request_id="late-request-id")
            before = await budget.export()
            try:
                if path == "pending":
                    await budget.pending(first["key"], response=late)
                else:
                    external = await budget.export()
                    external["calls"][0]["provider_request_sha256"] = digest("late-request-id")
                    external["checkpoint"] = digest(
                        {key: value for key, value in external.items() if key != "checkpoint"}
                    )
                    await budget.replay(external, expected_checkpoint=external["checkpoint"])
            except ModelError as error:
                assert error.code in {
                    "model_settled_identity_frozen",
                    "model_budget_restore_conflict",
                }
                assert await budget.export() == before
                return  # Refusing to rebind a settled identity is also safe.
            second = await reserve(budget, accounts, attempt="second")
            await intent(budget, second)
            with pytest.raises(ModelError):
                await budget.settle(
                    second["key"],
                    actual_microunits=25,
                    receipt_id="duplicate-provider-invoice",
                    provider_request_id="late-request-id",
                )

    asyncio.run(run())
