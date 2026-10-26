import asyncio
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import timedelta

import pytest
from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    EvaluationReport,
    EvaluationStage,
    EvolutionConflict,
    EvolutionEngine,
    EvolutionGateError,
    EvolutionState,
    EvolutionWithdrawalError,
    PromotionApproval,
    SQLiteEvolutionRegistry,
)

from agent_memory import MemoryScope, Procedure, Provenance
from agent_memory.domain import utc_now


class Deployment:
    def __init__(self):
        self.live = set()
        self.withdrawals = 0
        self.fail_withdrawal = False
        self.fail_activation = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    async def activate(self, candidate):
        self.entered.set()
        await self.release.wait()
        self.live.add(candidate.id)
        if self.fail_activation:
            raise OSError("partial activation")

    async def deactivate(self, candidate):
        self.withdrawals += 1
        if self.fail_withdrawal:
            raise OSError("external target unavailable")
        self.live.discard(candidate.id)


async def canary(path, deployment, scope=None):
    scope = scope or MemoryScope("tenant", session_id="one")
    registry = SQLiteEvolutionRegistry(path)
    engine = EvolutionEngine(registry, DeterministicPromotionPolicy(), deployment)
    await engine.initialize()
    candidate = await engine.register_procedure(
        Procedure(
            scope=scope,
            name="recover a failed build",
            trigger="build failure",
            steps=("inspect logs", "fix the cause", "rerun build"),
            success_conditions=("build passed",),
            provenance=Provenance(source_event_ids=("source",)),
        ),
        ("episode",),
    )
    for stage, count in (
        (EvaluationStage.OFFLINE, 30),
        (EvaluationStage.SHADOW, 50),
        (EvaluationStage.CANARY, 100),
    ):
        await engine.submit_evaluation(
            EvaluationReport(
                candidate_id=candidate.id,
                candidate_version=1,
                scope_partition_key=scope.partition_key(),
                stage=stage,
                dataset_id="offline-fixture",
                dataset_version="1",
                evaluator_id="evaluator",
                evaluator_version="1",
                rubric_id="build",
                rubric_version="1",
                sample_size=count,
                metrics={
                    "task_success_rate": 0.9,
                    "task_success_delta": 0.1,
                    "cross_scope_leakage": 0,
                    "safety_violation_rate": 0,
                },
                evidence_digest="fixture:1",
            ),
            actor="evaluator",
        )
        if stage != EvaluationStage.CANARY:
            await engine.promote(candidate.id, actor="host")
    approval = PromotionApproval(
        "reviewer",
        "offline-review",
        "reviewed",
        candidate_id=candidate.id,
        candidate_version=1,
        scope_partition_key=scope.partition_key(),
        expires_at=utc_now() + timedelta(hours=1),
    )
    return engine, registry, candidate, approval


@pytest.mark.parametrize("operation", ["transition", "invalidate"])
def test_audit_write_failure_rolls_back_state_pointer_and_source_tombstones(tmp_path, operation):
    async def scenario():
        path = tmp_path / "evolution.db"
        engine, registry, candidate, approval = await canary(path, Deployment())
        await engine.promote(candidate.id, actor="host", approval=approval)
        before = await registry.get(candidate.id)
        history = await registry.history(candidate.id)
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute(
                "CREATE TRIGGER reject_audit BEFORE INSERT ON evolution_promotions "
                "BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END"
            )
        with pytest.raises(sqlite3.IntegrityError, match="audit unavailable"):
            if operation == "transition":
                await registry.transition(
                    candidate.id, EvolutionState.ACTIVE, EvolutionState.ROLLING_BACK,
                    actor="host", reason="rollback",
                )
            else:
                await registry.invalidate_sources(
                    ("source",), scope=candidate.scope, actor="host", reason="erase", erase=True
                )
        assert await registry.get(candidate.id) == before
        assert await registry.active_candidate_id(candidate.scope) == candidate.id
        assert await registry.history(candidate.id) == history
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute(
                "SELECT count(*) FROM evolution_invalidated_sources"
            ).fetchone()[0] == 0

    asyncio.run(scenario())


def test_active_invalidation_denies_immediately_and_recovers_after_reopen(tmp_path):
    async def scenario():
        deployment = Deployment()
        path = tmp_path / "evolution.db"
        engine, registry, candidate, approval = await canary(path, deployment)
        await engine.promote(candidate.id, actor="host", approval=approval, idempotency_key="once")
        deployment.fail_withdrawal = True
        with pytest.raises(EvolutionWithdrawalError):
            await engine.invalidate_sources(
                ("source",), scope=candidate.scope, actor="host", reason="source erased"
            )
        assert await engine.active_candidate(candidate.scope) is None
        assert await registry.active_candidate_id(candidate.scope) is None
        assert (await registry.get(candidate.id)).withdrawal_pending
        with pytest.raises(EvolutionGateError, match="invalidated"):
            await engine.promote(
                candidate.id, actor="host", approval=approval, idempotency_key="once"
            )
        with pytest.raises(EvolutionConflict, match="invalidated"):
            await engine.register_procedure(replace(candidate.procedure, id="new-id"), ("episode",))
        deployment.fail_withdrawal = False
        reopened = EvolutionEngine(
            SQLiteEvolutionRegistry(path), DeterministicPromotionPolicy(), deployment
        )
        await reopened.initialize()
        await reopened.recover()
        assert not deployment.live
        assert not (await registry.get(candidate.id)).withdrawal_pending
        count = deployment.withdrawals
        assert await reopened.recover() == ()
        assert (
            await reopened.invalidate_sources(
                ("source",), scope=candidate.scope, actor="host", reason="retry"
            )
            == ()
        )
        assert deployment.withdrawals == count

    asyncio.run(scenario())


def test_invalidation_during_activation_repeats_compensation_after_late_effect(tmp_path):
    async def scenario():
        deployment = Deployment()
        deployment.release.clear()
        engine, registry, candidate, approval = await canary(tmp_path / "e.db", deployment)
        activation = asyncio.create_task(
            engine.promote(candidate.id, actor="host", approval=approval)
        )
        await deployment.entered.wait()
        await engine.invalidate_sources(
            ("source",), scope=candidate.scope, actor="host", reason="source revoked"
        )
        pending = await registry.get(candidate.id)
        assert pending.state == EvolutionState.INVALIDATED
        assert pending.activation_pending and pending.withdrawal_pending
        assert await engine.active_candidate(candidate.scope) is None
        deployment.release.set()
        with pytest.raises(EvolutionConflict):
            await activation
        assert deployment.withdrawals == 2
        assert not deployment.live
        finished = await registry.get(candidate.id)
        assert not finished.activation_pending and not finished.withdrawal_pending

    asyncio.run(scenario())


def test_crash_between_invalidation_and_withdrawal_is_recovered(tmp_path):
    async def scenario():
        deployment = Deployment()
        path = tmp_path / "e.db"
        _, registry, candidate, _ = await canary(path, deployment)
        await registry.transition(
            candidate.id,
            EvolutionState.CANARY,
            EvolutionState.ACTIVATING,
            actor="host",
            reason="simulate process before crash",
        )
        await deployment.activate(candidate)
        # The durable transaction committed, but no engine callback ran before crash.
        await registry.invalidate_sources(
            ("source",), scope=candidate.scope, actor="host", reason="erase"
        )
        engine = EvolutionEngine(
            SQLiteEvolutionRegistry(path), DeterministicPromotionPolicy(), deployment
        )
        await engine.initialize()
        await engine.recover()
        assert not deployment.live
        assert (await registry.get(candidate.id)).state == EvolutionState.INVALIDATED
        assert not (await registry.get(candidate.id)).withdrawal_pending

    asyncio.run(scenario())


def test_source_ids_do_not_cross_scopes_and_tombstones_prevent_new_candidates(tmp_path):
    async def scenario():
        deployment = Deployment()
        path = tmp_path / "e.db"
        engine, registry, first, _ = await canary(path, deployment)
        other_scope = MemoryScope("another-tenant", session_id="one")
        _, _, other, _ = await canary(path, deployment, other_scope)
        await engine.invalidate_sources(
            ("source",), scope=first.scope, actor="host", reason="erase"
        )
        assert (await registry.get(first.id)).state == EvolutionState.INVALIDATED
        assert (await registry.get(other.id)).state == EvolutionState.CANARY
        await engine.invalidate_sources(
            ("never-seen",), scope=first.scope, actor="host", reason="already erased"
        )
        with pytest.raises(EvolutionConflict, match="invalidated"):
            await engine.register_procedure(
                replace(
                    first.procedure,
                    id="unseen",
                    provenance=Provenance(source_event_ids=("never-seen",)),
                ),
                ("new-episode",),
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("activation_failure", [False, True])
def test_failed_withdrawal_never_restores_active_or_canary(tmp_path, activation_failure):
    async def scenario():
        deployment = Deployment()
        engine, registry, candidate, approval = await canary(tmp_path / "e.db", deployment)
        deployment.fail_activation = activation_failure
        if not activation_failure:
            await engine.promote(candidate.id, actor="host", approval=approval)
        deployment.fail_withdrawal = True
        with pytest.raises(OSError):
            if activation_failure:
                await engine.promote(candidate.id, actor="host", approval=approval)
            else:
                await engine.rollback(candidate.id, actor="host", reason="bad result")
        assert (await registry.get(candidate.id)).state == EvolutionState.ROLLING_BACK
        assert await engine.active_candidate(candidate.scope) is None
        deployment.fail_withdrawal = False
        await engine.recover()
        assert (await registry.get(candidate.id)).state == EvolutionState.ROLLED_BACK
        assert not deployment.live

    asyncio.run(scenario())


@pytest.mark.parametrize("pointer_replaced", [False, True])
def test_legacy_active_invalidation_is_backfilled_once(tmp_path, pointer_replaced):
    async def scenario():
        deployment = Deployment()
        path = tmp_path / "legacy.db"
        engine, _, candidate, approval = await canary(path, deployment)
        await engine.promote(candidate.id, actor="host", approval=approval)
        if pointer_replaced:
            _, _, replacement, replacement_approval = await canary(path, deployment)
            await engine.promote(replacement.id, actor="host", approval=replacement_approval)
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                "UPDATE evolution_candidates SET state = 'invalidated' WHERE id = ?",
                (candidate.id,),
            )
            connection.execute("ALTER TABLE evolution_candidates DROP COLUMN withdrawal_pending")
            connection.execute("ALTER TABLE evolution_candidates DROP COLUMN activation_pending")
            connection.commit()
        reopened = SQLiteEvolutionRegistry(path)
        await reopened.initialize()
        await reopened.initialize()
        assert (await reopened.get(candidate.id)).withdrawal_pending
        assert await reopened.active_candidate_id(candidate.scope) == (
            replacement.id if pointer_replaced else None
        )
        recovered = EvolutionEngine(reopened, DeterministicPromotionPolicy(), deployment)
        await recovered.recover()
        await reopened.initialize()
        assert not (await reopened.get(candidate.id)).withdrawal_pending
        assert deployment.live == ({replacement.id} if pointer_replaced else set())

    asyncio.run(scenario())


def test_procedure_lineage_survives_registry_roundtrip_and_identity_is_immutable(tmp_path):
    async def scenario():
        engine, registry, candidate, _ = await canary(tmp_path / "e.db", Deployment())
        enriched = replace(
            candidate.procedure,
            id="enriched",
            source_episode_ids=("ok", "bad"),
            counterexample_episode_ids=("bad",),
            applicability_conditions=("local build",),
            failure_patterns=("network unavailable",),
            extractor_version="multi-step/1",
            evidence_start_at=utc_now(),
            evidence_end_at=utc_now(),
            reward_definition_id="quality",
            reward_formula_version="2",
        )
        saved = await engine.register_procedure(enriched, ("ok", "bad"))
        assert (await registry.get(saved.id)).procedure == enriched
        with pytest.raises(EvolutionConflict, match="immutable"):
            await engine.register_procedure(replace(enriched, steps=("different",)), ("ok", "bad"))

    asyncio.run(scenario())
