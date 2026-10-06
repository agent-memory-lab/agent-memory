"""SIGKILL recovery boundaries; SQLite and a real PostgreSQL server, no mocked crash."""

import asyncio
import json
import os
import signal
import sys
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
import test_atom_admission as base
from test_durable_execution import setup

from agent_memory.capture.producer import DurableProducer
from agent_memory.domain import ForgetMode, ForgetRequest
from agent_memory.operations.retention import DurableReceiver
from agent_memory.serialization import to_jsonable

store = base.store
CHILD = Path(__file__).parent / "fixtures" / "durable_crash_child.py"
pytestmark = pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="requires real SIGKILL")


async def kill_at_boundary(engine, scope, clock, tmp_path, phase):
    repo = engine.repository
    config = {
        "backend": "postgres" if hasattr(repo, "pool") else "sqlite",
        "database": repo.pool.conninfo if hasattr(repo, "pool") else str(repo._path),
        "scope": to_jsonable(scope),
        "clock": clock[0].isoformat(),
        "phase": phase,
        "calls": str(tmp_path / "generation.log"),
    }
    path = tmp_path / "child.json"
    path.write_text(json.dumps(config))
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(CHILD),
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parent)},
    )
    try:
        line = await asyncio.wait_for(process.stdout.readline(), timeout=20)
        if line != b"ready\n":
            error = await asyncio.wait_for(process.stderr.read(), timeout=5)
            pytest.fail(f"child failed before boundary: {error.decode()}")
        process.kill()
        assert await asyncio.wait_for(process.wait(), timeout=10) == -signal.SIGKILL
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.parametrize("phase", ["receive_before_commit", "receive_after_commit"])
def test_kill_around_transaction_a_keeps_source_request_cursor_atomic(store, tmp_path, phase):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await kill_at_boundary(engine, scope, clock, tmp_path, phase)
            producer = DurableProducer(DurableReceiver(engine.repository, clock=lambda: clock[0]))
            session = await producer.open(
                scope, producer_id="crash-device", actor="alice", configuration_sha256="a" * 64
            )
            committed = phase == "receive_after_commit"
            assert (await producer.cursor(scope, session, actor="alice"))["acked_through"] == int(
                committed
            )
            event = replace(
                base.source(scope, identity="crash-receive", idempotency="crash-receive"),
                actor="alice",
            )
            async with engine.repository.unit_of_work() as uow:
                assert await uow.retention_count(scope, "request") == int(committed)
                assert (await uow.get_source_event(scope, event.id) is not None) == committed
            result = await producer.append(event, session, sequence=1, actor="alice")
            assert result["receipt"]["duplicate"] == committed
            assert result["acked_through"] == 1
            async with engine.repository.unit_of_work() as uow:
                assert await uow.retention_count(scope, "request") == 1

    asyncio.run(run())


@pytest.mark.parametrize(
    "phase",
    [
        "worker_after_claim",
        "worker_after_checkpoint",
        "worker_before_publication_commit",
        "worker_after_publication_commit",
    ],
)
def test_real_worker_kill_recovers_saved_stage_without_duplicate_publication(
    store, tmp_path, phase
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, generator, _, queue, handler = await setup(engine, scope, clock)
            await kill_at_boundary(engine, scope, clock, tmp_path, phase)
            published = phase == "worker_after_publication_commit"
            rows = await engine.repository.admission_records(scope)
            assert bool(rows) == published
            status = await queue.status("one")
            assert status["l1_decided"] == published
            if published:
                assert await queue.claim("recovered", lease_seconds=60) is None
                assert len(status["result"]["claim_ids"]) == 1
            else:
                assert await queue.claim("too-early", lease_seconds=60) is None
                clock[0] += timedelta(seconds=6)
                lease = await queue.claim("recovered", lease_seconds=60)
                assert lease.task.attempts == 2
                await handler(lease.task, lambda value: queue.checkpoint(lease, value))
                await queue.complete(lease)
                status = await queue.status("one")
                assert status["l1_decided"]
            assert generator.calls == int(phase == "worker_after_claim")
            assert len(await engine.repository.admission_records(scope)) == 1
            claims, _ = await engine.state(scope, valid_at=base.at(2), known_at=base.at(30))
            assert [c.value for c in claims] == ["Hangzhou"]
            calls = tmp_path / "generation.log"
            child_calls = len(calls.read_text().splitlines()) if calls.exists() else 0
            assert child_calls + generator.calls == 1

    asyncio.run(run())


def test_erase_after_killed_checkpoint_cancels_stage_and_prevents_restart_publish(store, tmp_path):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            source, generator, _, queue, _ = await setup(engine, scope, clock)
            await kill_at_boundary(engine, scope, clock, tmp_path, "worker_after_checkpoint")
            await kernel.forget(
                ForgetRequest(scope, memory_ids=(source.id,), mode=ForgetMode.ERASE)
            )
            clock[0] += timedelta(seconds=6)
            assert await queue.claim("recovered", lease_seconds=60) is None
            assert (await queue.status("one"))["status"] == "cancelled"
            async with engine.repository.unit_of_work() as uow:
                row = await uow.retention_get(scope, "request", "one")
                assert "prepared" not in row and "result" not in row
            assert not await engine.repository.admission_records(scope)
            assert generator.calls == 0

    asyncio.run(run())
