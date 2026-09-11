import asyncio
import os
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import pytest
from agent_memory_postgres import (
    PostgresConsolidationQueue,
    PostgresMemoryRepository,
    QueueCapacityExceeded,
    build_postgres_kernel,
)
from psycopg import AsyncConnection, sql

from agent_memory import (
    AgentMemory,
    FeedbackStatus,
    ForgetMode,
    ForgetRequest,
    MemoryScope,
    MemoryUsage,
    OutcomeEvent,
)


def _test_dsn() -> str:
    dsn = os.getenv("AGENT_MEMORY_TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.skip("AGENT_MEMORY_TEST_POSTGRES_DSN is not configured")
    parsed = urlsplit(dsn)
    database = unquote(parsed.path.lstrip("/")).casefold()
    if parsed.scheme not in {"postgres", "postgresql"} or "test" not in database:
        pytest.fail(
            "AGENT_MEMORY_TEST_POSTGRES_DSN must be a PostgreSQL URI whose database name "
            "contains 'test'"
        )
    return dsn


def test_test_dsn_guard_rejects_non_test_database(monkeypatch) -> None:
    monkeypatch.setenv(
        "AGENT_MEMORY_TEST_POSTGRES_DSN",
        "postgresql://memory@localhost/production",
    )
    with pytest.raises(pytest.fail.Exception, match="contains 'test'"):
        _test_dsn()


def test_live_postgres_additive_v1_to_v2_migration() -> None:
    async def scenario() -> None:
        dsn = _test_dsn()
        schema = f"migration_{uuid4().hex}"
        async with await AsyncConnection.connect(dsn, autocommit=True) as connection:
            await connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

        parsed = urlsplit(dsn)
        query = dict(parse_qsl(parsed.query))
        query["options"] = f"-csearch_path={schema}"
        scoped_dsn = urlunsplit(parsed._replace(query=urlencode(query)))
        migration_path = Path(__file__).parents[1] / "migrations"
        async with await AsyncConnection.connect(scoped_dsn) as connection:
            await connection.execute(
                (migration_path / "001_core.sql").read_text(encoding="utf-8"),
                prepare=False,
            )
            await connection.commit()
            cursor = await connection.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = 'agent_memory_evolution_records'""",
                (schema,),
            )
            assert "feedback_status" not in {row[0] for row in await cursor.fetchall()}

        repository = PostgresMemoryRepository.from_dsn(
            scoped_dsn, migrations_path=migration_path
        )
        await repository.initialize()
        try:
            async with repository.pool.connection() as connection:
                cursor = await connection.execute(
                    """SELECT column_name FROM information_schema.columns
                       WHERE table_schema = %s
                         AND table_name = 'agent_memory_evolution_records'""",
                    (schema,),
                )
                columns = {row["column_name"] for row in await cursor.fetchall()}
                assert {"feedback_status", "parent_id", "idempotency_key"} <= columns
                cursor = await connection.execute(
                    "SELECT max(schema_version) AS version FROM agent_memory_schema"
                )
                assert (await cursor.fetchone())["version"] == 2
        finally:
            await repository.close()

    asyncio.run(scenario())


def test_live_postgres_feedback_contract() -> None:
    async def scenario() -> None:
        dsn = _test_dsn()
        suffix = uuid4().hex
        scope = MemoryScope(
            tenant_id=f"contract-{suffix}",
            namespace="live-test",
            session_id="session",
        )
        kernel = build_postgres_kernel(
            dsn,
            min_pool_size=1,
            max_pool_size=4,
            trusted_evaluator_ids=("trusted-evaluator",),
        )
        memory = AgentMemory(kernel, scope)
        await memory.initialize()
        try:
            async with kernel._repository.pool.connection() as connection:
                cursor = await connection.execute(
                    "SELECT max(schema_version) AS version FROM agent_memory_schema"
                )
                assert (await cursor.fetchone())["version"] == 2

            queue_scope = MemoryScope(
                tenant_id=f"queue-{suffix}", namespace="live-test", session_id="session"
            )
            bounded_queue = PostgresConsolidationQueue(
                kernel._repository.pool, max_pending_per_scope=2
            )
            first_job = await bounded_queue.enqueue(
                queue_scope, job_key=f"job-1-{suffix}", job_type="test", payload={}
            )
            assert (
                await bounded_queue.enqueue(
                    queue_scope, job_key=f"job-1-{suffix}", job_type="test", payload={}
                )
                == first_job
            )
            await bounded_queue.enqueue(
                queue_scope, job_key=f"job-2-{suffix}", job_type="test", payload={}
            )
            with pytest.raises(QueueCapacityExceeded, match="capacity reached"):
                await bounded_queue.enqueue(
                    queue_scope, job_key=f"job-3-{suffix}", job_type="test", payload={}
                )

            event = await memory.remember(
                "The host prefers bounded evidence.",
                claims=(
                    {
                        "key": "retrieval.preference",
                        "value": "bounded",
                        "text": "Use bounded evidence retrieval.",
                    },
                ),
                idempotency_key=f"event-{suffix}",
            )
            duplicate = await memory.remember(
                "The host prefers bounded evidence.",
                claims=(
                    {
                        "key": "retrieval.preference",
                        "value": "bounded",
                        "text": "Use bounded evidence retrieval.",
                    },
                ),
                idempotency_key=f"event-{suffix}",
            )
            assert duplicate.event_id == event.event_id
            assert duplicate.duplicate is True

            concurrent_key = f"concurrent-event-{suffix}"
            concurrent = await asyncio.gather(
                memory.remember("Concurrent evidence", idempotency_key=concurrent_key),
                memory.remember("Concurrent evidence", idempotency_key=concurrent_key),
            )
            assert concurrent[0].event_id == concurrent[1].event_id
            assert sum(item.duplicate for item in concurrent) == 1

            await asyncio.gather(
                memory.remember(
                    "First concurrent claim",
                    claims=(
                        {
                            "key": "concurrent.state",
                            "value": "first",
                            "text": "Concurrent state is first.",
                        },
                    ),
                    idempotency_key=f"claim-first-{suffix}",
                ),
                memory.remember(
                    "Second concurrent claim",
                    claims=(
                        {
                            "key": "concurrent.state",
                            "value": "second",
                            "text": "Concurrent state is second.",
                        },
                    ),
                    idempotency_key=f"claim-second-{suffix}",
                ),
            )
            concurrent_claims = [
                claim for claim in await memory.state() if claim.key == "concurrent.state"
            ]
            assert len(concurrent_claims) == 1

            bundle = await memory.recall("bounded evidence")
            returned_ids = tuple(claim.id for claim in bundle.current_state) + tuple(
                item.id for item in bundle.relevant_memories
            )
            decision, repeated_decision = await asyncio.gather(
                memory.record_decision(
                    "Use bounded evidence retrieval",
                    memory_ids=returned_ids,
                    bundle_id=bundle.bundle_id,
                    memory_usage=MemoryUsage.CONFIRMED,
                    idempotency_key=f"decision-{suffix}",
                ),
                memory.record_decision(
                    "Use bounded evidence retrieval",
                    memory_ids=returned_ids,
                    bundle_id=bundle.bundle_id,
                    memory_usage=MemoryUsage.CONFIRMED,
                    idempotency_key=f"decision-{suffix}",
                ),
            )
            assert repeated_decision.id == decision.id

            with pytest.raises(ValueError, match="scope|parent|decision"):
                await kernel.record_outcome(
                    OutcomeEvent(
                        scope=MemoryScope(
                            tenant_id=f"other-{suffix}",
                            namespace="live-test",
                            session_id="session",
                        ),
                        decision_id=decision.id,
                        outcome="cross scope",
                        success=True,
                    )
                )

            outcome = await memory.record_outcome(
                decision.id,
                "accepted",
                True,
                idempotency_key=f"outcome-{suffix}",
            )
            corrected_outcome = await memory.record_outcome(
                decision.id,
                "accepted after correction",
                True,
                corrects_id=outcome.id,
                idempotency_key=f"outcome-correction-{suffix}",
            )
            superseded = await memory.feedback_status(outcome.id, "outcome")
            assert superseded is not None
            assert superseded.status == FeedbackStatus.SUPERSEDED
            evaluation = await memory.record_evaluation(
                corrected_outcome.id,
                evaluator_id="trusted-evaluator",
                evaluator_version="1",
                rubric_id="task-result",
                rubric_version="1",
                metrics={"quality": 1.0},
                evidence_digest=f"sha256:{suffix}",
                idempotency_key=f"evaluation-{suffix}",
            )
            reward = await memory.record_reward(
                corrected_outcome.id,
                1.0,
                "1",
                evaluation_id=evaluation.id,
                reward_definition_id="task-result",
                idempotency_key=f"reward-{suffix}",
            )
            page = await memory.feedback_history("reward", limit=1)
            assert page.items[0].record_id == reward.id
        finally:
            await kernel.close()

        restarted_kernel = build_postgres_kernel(
            dsn,
            min_pool_size=1,
            max_pool_size=2,
            trusted_evaluator_ids=("trusted-evaluator",),
        )
        restarted = AgentMemory(restarted_kernel, scope)
        await restarted.initialize()
        try:
            receipt = await restarted.feedback_status(reward.id, "reward")
            assert receipt is not None
            assert receipt.status == FeedbackStatus.ACCEPTED
            await restarted.provider.forget(
                ForgetRequest(
                    scope=scope,
                    memory_ids=(event.event_id,),
                    mode=ForgetMode.ERASE,
                )
            )
            invalidated = await restarted.feedback_status(reward.id, "reward")
            assert invalidated is not None
            assert invalidated.status == FeedbackStatus.INVALIDATED
        finally:
            await restarted_kernel.close()

    asyncio.run(scenario())
