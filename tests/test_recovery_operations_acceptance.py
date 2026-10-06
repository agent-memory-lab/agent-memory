import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import sqlite3

import pytest

from agent_memory.domain import MemoryScope
from agent_memory.mcp import MCPRequestContext, MCPToolError
from agent_memory.recovery_store import RecoveryConflict, SQLiteRecoveryStore
from agent_memory.recovery_transport import RecoveryTransport
from agent_memory_sdk import EmbeddedMemoryClient, MemoryClientError
from test_recovery_context_acceptance import AT, CONTENT, SCOPE, memory_at, prepare


class Allow:
    async def authorize(self, context, operation):
        return context.actor == "trusted-host"


def test_sdk_and_transport_authorization(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        transport = RecoveryTransport(memory, Allow())
        context = MCPRequestContext(SCOPE, actor="trusted-host", can_erase=True)
        client = EmbeddedMemoryClient(memory.provider, context, recovery_tools=transport)
        await client.initialize()
        try:
            receipt, state, plan = await prepare(memory)
            assert (await client.load_recovery(state.run_id))["result"]["version"] == 1
            assert (await client.capture_receipt(receipt.event_id))["result"]["stage"] == "persisted"
            summary = (await client.propose_compression(plan))["result"]
            assert summary["accepted"]
            assert (await client.validate_compression(summary["summary_id"], plan))["result"]["accepted"]
            with pytest.raises(MemoryClientError):
                await client.ingest("user.message", "bypass")
            for forbidden in (MCPRequestContext(SCOPE, actor="untrusted"),
                              MCPRequestContext(MemoryScope("foreign"), actor="trusted-host")):
                with pytest.raises(MCPToolError):
                    await transport.call("stats", {}, forbidden)
            with pytest.raises(MCPToolError):
                await transport.call("forget", {"all_in_scope": True},
                    MCPRequestContext(SCOPE, actor="trusted-host", can_erase=False))
            await client.forget_sources((receipt.provider_event_id,))
            assert (await client.load_recovery("run-1"))["result"] is None
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_queue_restart_worker_and_receipt(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            result = await memory.enqueue_capture(event_id="q1", role="user", content=CONTENT,
                                                  run_id="queue-run", occurred_at=AT)
            assert result.stage == "received" and result.queue_status == "queued"
            assert not result.persisted
        finally:
            await memory.close()
        restarted = memory_at(tmp_path)
        await restarted.initialize()
        try:
            result = await restarted.process_next_capture()
            assert result.persisted and result.queue_status == "done" and result.attempts == 1
            assert await restarted.process_next_capture() is None
            duplicate = await restarted.enqueue_capture(event_id="q1", role="user", content=CONTENT,
                                                        run_id="queue-run", occurred_at=AT)
            assert duplicate.provider_event_id == result.provider_event_id
        finally:
            await restarted.close()
    asyncio.run(scenario())


def test_queue_failure_explicit_retry(tmp_path):
    class Generator:
        fail = True

        async def generate_claims(self, event):
            if self.fail:
                raise RuntimeError("private failure")
            return []

    async def scenario():
        generator = Generator()
        memory = memory_at(tmp_path, generator=generator)
        await memory.initialize()
        try:
            await memory.enqueue_capture(event_id="q1", role="user", content=CONTENT,
                                         run_id="queue-run", occurred_at=AT)
            failed = await memory.process_next_capture()
            assert failed.queue_status == "failed" and not failed.persisted
            assert failed.error_code == "capture_processing_failed"
            assert await memory.process_next_capture() is None
            generator.fail = False
            await memory.retry_capture("q1")
            done = await memory.process_next_capture()
            assert done.persisted and done.extracted and done.attempts == 2
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_completion_cleanup_frees_records_but_keeps_run_fence(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            _, _, plan = await prepare(memory)
            await memory.propose_compression(plan)
            with pytest.raises(RecoveryConflict):
                await memory.complete_recovery("run-1", expected_version=2)
            await memory.complete_recovery("run-1", expected_version=1)
            assert await memory.load_recovery("run-1") is None
            assert await memory.capture_receipt("event-1") is None
            assert (await memory.cleanup_recovery(limit=1))["removed_records"] == 1
            await memory.cleanup_recovery()
            stats = await memory.recovery_stats()
            assert stats["records"] == 0 and stats["run_fences"] == 1
            with pytest.raises(RecoveryConflict):
                await memory.enqueue_capture(event_id="new", run_id="run-1", role="user",
                                             content=CONTENT, occurred_at=AT)
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_expiry_hides_and_cleans_pending_jobs(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            await memory.enqueue_capture(event_id="pending", run_id="expire-run", role="user",
                                         content=CONTENT, occurred_at=AT)
            await memory.set_recovery_expiry("expire-run",
                expires_at=datetime.now(UTC) - timedelta(seconds=1))
            assert await memory.capture_receipt("pending") is None
            assert await memory.process_next_capture() is None
            assert (await memory.cleanup_recovery())["removed_records"] == 2
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_preflight_rejects_context_changes(tmp_path):
    async def scenario():
        memory = memory_at(tmp_path)
        await memory.initialize()
        try:
            receipt, _, plan = await prepare(memory)
            proposal = await memory.propose_compression(plan)
            changed = replace(plan, token_budget=plan.token_budget + 1)
            result = await memory.validate_compression(proposal.summary_id, changed)
            assert not result.accepted and result.replacement is None
            assert result.reason == "host_context_changed"
            await memory.forget_sources((receipt.provider_event_id,))
            assert not (await memory.validate_compression(proposal.summary_id, plan)).accepted
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_default_probe_confirms_real_claim_recall(tmp_path):
    class Generator:
        async def generate_claims(self, event):
            return [{"key": "report.constraint", "value": "do-not-publish",
                     "text": "Do not publish the report", "scope_level": "session",
                     "confidence": 0.95}]

    async def scenario():
        memory = memory_at(tmp_path, generator=Generator())
        await memory.initialize()
        try:
            receipt = await memory.capture_with_receipt(event_id="fact", run_id="run",
                role="user", content="Do not publish the report", occurred_at=AT)
            assert receipt.extracted and receipt.raw_readable
            assert receipt.facts_retrievable and receipt.retrievable
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_legacy_store_migration(tmp_path):
    path = tmp_path / "old-recovery.db"
    db = sqlite3.connect(path)
    try:
        db.execute("""CREATE TABLE recovery_records_v1 (
            scope TEXT NOT NULL, kind TEXT NOT NULL, identity TEXT NOT NULL,
            revision INTEGER NOT NULL, payload TEXT, sources TEXT NOT NULL,
            PRIMARY KEY(scope,kind,identity))""")
        db.execute("INSERT INTO recovery_records_v1 VALUES (?,?,?,?,?,?)",
                   (SCOPE.partition_key(), "receipt", "old", 1, '{"run_id":"old-run"}', '[]'))
        db.commit()
    finally:
        db.close()

    async def scenario():
        store = SQLiteRecoveryStore(path)
        await store.initialize()
        assert (await store.read(SCOPE, "receipt", "old")).payload["run_id"] == "old-run"
        await store.configure_run(SCOPE, "old-run", completed=True)
        assert await store.read(SCOPE, "receipt", "old") is None
        assert await store.cleanup(SCOPE) == 1
    asyncio.run(scenario())
