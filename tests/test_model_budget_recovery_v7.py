"""Real SIGKILL, real SQL/pg_dump restores, and monotonic money-authority replay."""

import asyncio
import json
import os
import signal
import sys
from pathlib import Path

import pytest
import test_atom_admission as base
from test_model_budget_v7 import intent, reserve
from test_purge_restore import backup_copy

from agent_memory.operations.model_budget import BudgetAccount, ModelBudget
from agent_memory.retrieval.model_contracts import ModelError

store = base.store


@pytest.mark.parametrize("phase", ["intent_before_commit", "intent_after_commit", "settled"])
def test_sigkill_does_not_release_accepted_or_unknown_cost(store, phase):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            repo = engine.repository
            budget = ModelBudget(repo)
            keys = await budget.configure((BudgetAccount("global", "run", "USD", "1", 100),))
            config = dict(
                backend="postgres" if hasattr(repo, "pool") else "sqlite",
                database=repo.pool.conninfo if hasattr(repo, "pool") else str(repo._path),
                accounts=keys,
                phase=phase,
            )
            child = Path(__file__).parent / "fixtures/model_budget_crash_child.py"
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(child),
                json.dumps(config),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=os.environ,
            )
            try:
                line = await asyncio.wait_for(process.stdout.readline(), 30)
                if line != b"ready\n":
                    pytest.fail((await process.stderr.read()).decode())
                process.kill()
                assert await asyncio.wait_for(process.wait(), 10) == -signal.SIGKILL
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            (row,) = await ModelBudget(repo).snapshot()
            assert (
                row["state"]
                == {
                    "intent_before_commit": "reserved",
                    "intent_after_commit": "dispatch_intent",
                    "settled": "settled",
                }[phase]
            )
            if phase == "intent_before_commit":
                await budget.release(row["key"])
                await reserve(budget, keys, attempt="new")
            else:
                with pytest.raises(ModelError, match="cannot_release"):
                    await budget.release(row["key"])
                with pytest.raises(ModelError, match="budget_exhausted"):
                    await reserve(budget, keys, attempt="new")
                if phase == "settled":
                    await budget.settle(
                        row["key"],
                        actual_microunits=70,
                        receipt_id="bill",
                        provider_request_id="provider-call",
                    )
                    assert (await budget.snapshot())[0]["actual_microunits"] == 70
                else:
                    assert row["actual_microunits"] is None

    asyncio.run(run())


def test_independently_pinned_money_replay_restores_postbackup_debt(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            budget = ModelBudget(engine.repository)
            keys = await budget.configure((BudgetAccount("global", "run", "USD", "1", 500),))
            early = await reserve(budget, keys)
            async with backup_copy(engine.repository, tmp_path) as (backup, _):
                await intent(budget, early)
                await budget.settle(
                    early["key"], actual_microunits=80, receipt_id="bill", provider_request_id="p1"
                )
                later = await reserve(budget, keys, attempt="later")
                await intent(budget, later)
                current = await budget.export()
                restored = ModelBudget(backup)
                assert len(await restored.snapshot()) == 1
                result = await restored.replay(current, expected_checkpoint=current["checkpoint"])
                assert result["calls"] == 2
                assert (
                    await restored.replay(current, expected_checkpoint=current["checkpoint"])
                    == result
                )
                states = {r["key"]: r for r in await restored.snapshot()}
                assert states[early["key"]]["actual_microunits"] == 80
                assert states[later["key"]]["state"] == "dispatch_intent"
                with pytest.raises(ModelError, match="checkpoint_mismatch"):
                    await restored.replay(current, expected_checkpoint="f" * 64)
                await restored.settle(
                    later["key"], actual_microunits=30, receipt_id="bill2", provider_request_id="p2"
                )
                # Replaying the older unresolved checkpoint cannot reopen/free a bill.
                await restored.replay(current, expected_checkpoint=current["checkpoint"])
                assert all(r["state"] == "settled" for r in await restored.snapshot())
                retry = await reserve(restored, keys, attempt="retry")
                await intent(restored, retry)
                with pytest.raises(ModelError, match="provider_request_reused"):
                    await restored.settle(
                        retry["key"],
                        actual_microunits=80,
                        receipt_id="different",
                        provider_request_id="p1",
                    )
                async with backup.unit_of_work() as uow:
                    account = await uow.model_budget_get("account", keys[0])
                    assert account["settled"] == 110 and account["reserved"] == 100

    asyncio.run(run())


@pytest.mark.parametrize("recovery", ["retry", "erase"])
def test_sigkill_after_recording_provider_accepts_body_keeps_debt_and_fences_delivery(
    store, recovery
):
    import threading
    from dataclasses import asdict
    from datetime import timedelta

    from test_durable_purge import source_id
    from test_governed_models_v7 import TEMPLATE, configuration, setup
    from test_ollama_port_v7 import server

    from agent_memory.derived import DerivedError
    from agent_memory.domain import ForgetMode, ForgetRequest
    from agent_memory.retrieval.model_answers import GovernedModelAnswers
    from agent_memory.retrieval.model_authority import SourceModelAuthority
    from agent_memory.retrieval.model_contracts import digest
    from agent_memory.retrieval.ollama import OllamaPort
    from agent_memory.serialization import to_jsonable

    async def run():
        async with store() as (engine, kernel, scope, clock):
            repo = engine.repository
            original, _, ledger, _, answers, _ = await setup(engine, kernel, scope, clock)
            pause = threading.Event()
            with server(pause=pause) as (endpoint, received, runtime):
                cfg = configuration(endpoint=endpoint, runtime_manifest_sha256=digest(runtime))
                authority = SourceModelAuthority(
                    original.service,
                    public_template=TEMPLATE,
                    configuration=cfg,
                    verify_coordinates=original.verify_coordinates,
                )
                ids = (source_id(scope, "1"), source_id(scope, "2"))
                for key in ids:
                    await authority.allow_processing(
                        key,
                        readers=("alice",),
                        purposes=("agent_context",),
                        expires_at=clock[0] + timedelta(hours=1),
                    )
                # Use the real governor to seal the request and persist flight/intent.
                from test_governed_models_v7 import ModelCoordinates

                coords = ModelCoordinates(
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
                sealed = await authority.prepare(coords, ids)
                config = dict(
                    backend="postgres" if hasattr(repo, "pool") else "sqlite",
                    database=repo.pool.conninfo if hasattr(repo, "pool") else str(repo._path),
                    phase="governed_http",
                    configuration=asdict(cfg),
                    sources=ids,
                    coordinates=asdict(coords),
                    scope=to_jsonable(scope),
                    clock=clock[0].isoformat(),
                    accounts=answers.account_keys,
                )
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(Path(__file__).parent / "fixtures/model_budget_crash_child.py"),
                    json.dumps(config),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env={**os.environ, "PYTHONPATH": str(Path(__file__).parent)},
                )
                try:

                    async def accepted():
                        while not any(path == "/api/chat" for path, _ in received):
                            if process.returncode is not None:
                                pytest.fail((await process.stderr.read()).decode())
                            await asyncio.sleep(0.01)

                    await asyncio.wait_for(accepted(), 30)
                    assert (
                        next(body for path, body in received if path == "/api/chat")
                        == sealed.payload_json.encode()
                    )
                    process.kill()
                    assert await asyncio.wait_for(process.wait(), 10) == -signal.SIGKILL
                finally:
                    if process.returncode is None:
                        process.kill()
                        await process.wait()
                    pause.set()
                (old,) = await ledger.snapshot()
                assert old["state"] == "dispatch_intent" and old["actual_microunits"] is None
                with pytest.raises(ModelError, match="cannot_release"):
                    await ledger.release(old["key"])
                async with repo.unit_of_work() as uow:
                    assert await uow.derived_records(scope, "model_cache_body") == ()
                    assert not any(
                        row["payload"]["stage"] == "delivery"
                        for row in await uow.derived_records(scope, "model_authorization")
                    )
                clock[0] += timedelta(seconds=42)
                recovered = GovernedModelAnswers(
                    authority,
                    OllamaPort(cfg),
                    account_keys=answers.account_keys,
                    validate_output=lambda value, _: value == "zh-CN",
                )
                if recovery == "erase":
                    await kernel.forget(ForgetRequest(scope, (ids[0],), mode=ForgetMode.ERASE))
                    with pytest.raises((ModelError, DerivedError)):
                        await recovered.answer(sealed)
                    assert len(await ledger.snapshot()) == 1
                else:
                    assert (await recovered.answer(sealed)).text == "zh-CN"
                    rows = await ledger.snapshot()
                    assert len(rows) == 2 and len({row["call_id"] for row in rows}) == 2
                    assert {row["state"] for row in rows} == {
                        "dispatch_intent",
                        "reconciliation_pending",
                    }
                    assert all(row["actual_microunits"] is None for row in rows)

    asyncio.run(run())


def test_even_pinned_malformed_budget_checkpoint_cannot_release_or_duplicate_debt(store):
    from copy import deepcopy

    from agent_memory.retrieval.model_contracts import digest

    async def run():
        async with store() as (engine, kernel, scope, clock):
            ledger = ModelBudget(engine.repository)
            keys = await ledger.configure((BudgetAccount("global", "run", "USD", "1", 100),))
            original = await reserve(ledger, keys)
            await intent(ledger, original)
            valid = await ledger.export()
            for field, value in (
                ("maximum_microunits", 0),
                ("accounts", [keys[0], keys[0]]),
                ("actual_microunits", 0),
            ):
                wrong = deepcopy(valid)
                wrong["calls"][0][field] = value
                wrong["checkpoint"] = digest({k: v for k, v in wrong.items() if k != "checkpoint"})
                with pytest.raises(ModelError, match="checkpoint_invalid"):
                    await ledger.replay(wrong, expected_checkpoint=wrong["checkpoint"])
            assert await ledger.export() == valid

    asyncio.run(run())
