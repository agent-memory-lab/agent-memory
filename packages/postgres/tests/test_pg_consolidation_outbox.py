"""PostgreSQL generic handoff parity; live tests require an existing test DSN."""

import asyncio
import inspect
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import pytest
from agent_memory_postgres import build_postgres_kernel
from agent_memory_postgres import consolidation_outbox as storage
from psycopg import AsyncConnection, sql
from test_live_contract import _test_dsn

from agent_memory import ForgetMode, ForgetRequest, MemoryEvent, MemoryScope
from agent_memory.extraction_result import ExtractionDiagnostic, ExtractionRejection
from agent_memory.operations.consolidation_outbox import restore_ingest_result


def test_outbox_migration_is_additive_id_only_and_cascades_with_source_erasure():
    text = (Path(__file__).parents[1] / "migrations" / "021_consolidation_outbox.sql").read_text()
    assert "REFERENCES agent_memory_events(id) ON DELETE CASCADE" in text
    assert "result_json JSONB" in text and "source_hash TEXT" in text
    assert "lease_token TEXT" in text and "lease_expires_at TIMESTAMPTZ" in text
    assert "agent_memory_consolidation_outbox_scope_idx" in text
    assert "DROP TABLE" not in text
    assert "ALTER TABLE agent_memory_consolidation_outbox" in text
    assert "cancel_pending BOOLEAN" in text and "cancel_generation BIGINT" in text
    assert "content TEXT" not in text


def test_outbox_sql_uses_only_tables_declared_in_packaged_migrations():
    migrations = Path(__file__).parents[1] / "migrations"
    schema = "\n".join(path.read_text() for path in migrations.glob("*.sql"))
    declared = set(re.findall(r"CREATE TABLE IF NOT EXISTS (agent_memory_\w+)", schema, re.I))
    used = set(
        re.findall(
            r"\b(?:FROM|JOIN|UPDATE|INTO)\s+(agent_memory_\w+)", inspect.getsource(storage), re.I
        )
    )
    assert used <= declared
    assert "agent_memory_claim_sources" not in used
    assert "provenance_json->'source_event_ids'" in inspect.getsource(storage)


def test_pg_handoff_restore_keeps_nested_diagnostic_types():
    result = restore_ingest_result(
        {
            "event_id": "source",
            "claim_ids": ["claim"],
            "state_delta_ids": [],
            "superseded_claim_ids": [],
            "duplicate": False,
            "extraction": {
                "status": "degraded",
                "components": [
                    {
                        "status": "rejected",
                        "candidate_count": 1,
                        "rejections": [{"index": 0, "reason": "missing_confidence"}],
                    }
                ],
            },
        }
    )
    assert result.claim_ids == ("claim",)
    assert isinstance(result.extraction.components[0], ExtractionDiagnostic)
    assert result.extraction.components[0].rejections == (
        ExtractionRejection(0, "missing_confidence"),
    )


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_live_postgres_pending_retry_typed_receipt_and_forget(mode):
    async def scenario():
        dsn = _test_dsn()
        schema = "consolidation_outbox_" + uuid4().hex
        async with await AsyncConnection.connect(dsn, autocommit=True) as admin:
            await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            parsed = urlsplit(dsn)
            query = dict(parse_qsl(parsed.query))
            query["options"] = "-csearch_path=" + schema
            target = urlunsplit(parsed._replace(query=urlencode(query)))
            kernel = build_postgres_kernel(target, min_pool_size=1, max_pool_size=1)
            repository = kernel._repository
            source = MemoryEvent(
                MemoryScope("outbox", session_id="one"), "event", "private", id="source"
            )

            class FailedQueue:
                async def enqueue_event(self, event, result):
                    raise RuntimeError("queue unavailable")

                async def cancel_forget(self, request):
                    return 0

            queue = kernel._consolidation_scheduler
            kernel._consolidation_scheduler = FailedQueue()
            try:
                await asyncio.wait_for(kernel.initialize(), 15)
                first = await asyncio.wait_for(kernel.ingest_event(source), 5)
                assert first.consolidation_status == "pending"
                assert await repository.consolidation_intent_state(source.scope, source.id) == (
                    "pending"
                )
                delivery = await repository.claim_consolidation_intent(
                    scope=source.scope, event_id=source.id, lease_seconds=5
                )
                assert delivery.result.extraction == first.extraction
                assert isinstance(delivery.result.extraction, ExtractionDiagnostic)
                assert await repository.consolidation_intent_state(source.scope, source.id) == (
                    "leased"
                )
                assert await repository.fail_consolidation_intent(delivery, "retry")
                kernel._consolidation_scheduler = queue
                assert (await asyncio.wait_for(kernel.retry_consolidation(), 5))["delivered"] == 1
                assert await repository.consolidation_intent_state(source.scope, source.id) == (
                    "delivered"
                )
                await kernel.forget(ForgetRequest(source.scope, (source.id,), mode=mode))
                assert await repository.consolidation_intent_state(source.scope, source.id) is None
                assert await repository.claim_consolidation_intent(scope=source.scope) is None
            finally:
                await repository.close()
                await admin.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    asyncio.run(scenario())


def test_pg_outbox_validates_lease_before_database_access():
    async def scenario():
        for value in (True, 0, 4, 3601, "60"):
            with pytest.raises(ValueError, match="lease"):
                await storage.claim(None, None, lease_seconds=value)

    asyncio.run(scenario())
