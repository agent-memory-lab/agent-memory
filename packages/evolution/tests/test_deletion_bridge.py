import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from agent_memory_evolution import (
    DeterministicPromotionPolicy,
    EvaluationStage,
    EvolutionDeletionTarget,
    EvolutionEngine,
    EvolutionGateError,
    EvolutionState,
    EvolutionWithdrawalError,
    SQLiteEvolutionRegistry,
)
from test_withdrawal_compensation import Deployment, canary

from agent_memory import ForgetRequest, MemoryScope, Provenance
from agent_memory.unified_memory import PendingMemoryDeletion, UnifiedMemory


@pytest.mark.parametrize("all_in_scope", [False, True])
def test_real_core_erase_keeps_journal_pending_until_evolution_withdrawal(tmp_path, all_in_scope):
    async def scenario():
        deployment = Deployment()
        path = tmp_path / "evolution.db"
        engine, registry, example, _ = await canary(path, deployment)
        target = EvolutionDeletionTarget(engine, example.scope)
        memory = UnifiedMemory.local(
            tmp_path / "core.db", example.scope, targets={"evolution": target}
        )
        await memory.initialize()
        receipt = await memory.capture(
            event_id="observed",
            role="user",
            content="build success",
            run_id="run",
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        candidate = await engine.register_procedure(
            replace(
                example.procedure,
                id="linked",
                provenance=Provenance(source_event_ids=(receipt.provider_event_id,)),
            ),
            ("linked-episode",),
        )
        # Set up a previously approved deployment; the gate progression is covered
        # by the canary helper and Skill lifecycle integration tests.
        for expected, state in (
            (EvolutionState.CANDIDATE, EvolutionState.EVALUATED),
            (EvolutionState.EVALUATED, EvolutionState.SHADOW),
            (EvolutionState.SHADOW, EvolutionState.CANARY),
            (EvolutionState.CANARY, EvolutionState.ACTIVATING),
            (EvolutionState.ACTIVATING, EvolutionState.ACTIVE),
        ):
            await registry.transition(
                candidate.id, expected, state, actor="fixture", reason="setup"
            )
        await registry.finish_activation(candidate.id)
        await registry.set_active_pointer(await registry.get(candidate.id))
        await deployment.activate(candidate)
        deployment.fail_withdrawal = True
        with pytest.raises(EvolutionWithdrawalError):
            await memory.forget_sources(
                () if all_in_scope else (receipt.provider_event_id,), all_in_scope=all_in_scope
            )
        assert (await registry.get(candidate.id)).state == EvolutionState.INVALIDATED
        assert await engine.active_candidate(candidate.scope) is None
        assert await memory.journal.pending(candidate.scope) is not None
        with pytest.raises(PendingMemoryDeletion):
            await memory.recall("build")
        await memory.close()

        reopened_engine = EvolutionEngine(
            SQLiteEvolutionRegistry(path), DeterministicPromotionPolicy(), deployment
        )
        await reopened_engine.initialize()
        reopened = UnifiedMemory.local(
            tmp_path / "core.db",
            candidate.scope,
            targets={"evolution": EvolutionDeletionTarget(reopened_engine, candidate.scope)},
        )
        await reopened.initialize()
        deployment.fail_withdrawal = False
        await reopened.resume_deletion()
        assert not deployment.live
        tombstone = await registry.get(candidate.id)
        assert tombstone.erased and not tombstone.erasure_pending
        assert tombstone.procedure.id == candidate.procedure.id
        assert tombstone.procedure.steps == () and tombstone.evidence == ()
        assert "Recover" not in repr(tombstone)
        assert all(
            record.reason == "erased candidate" for record in await registry.history(candidate.id)
        )
        assert await reopened.journal.pending(candidate.scope) is None
        assert await reopened.resume_deletion() is None
        assert not (await reopened.recall("build")).relevant_memories
        await reopened.close()

    asyncio.run(scenario())


def test_archive_retains_history_but_erase_scrubs_it_after_pending_withdrawal(tmp_path):
    async def scenario():
        from agent_memory_evolution import export_skill

        from agent_memory import ForgetMode

        deployment = Deployment()
        path = tmp_path / "e.db"
        engine, registry, candidate, approval = await canary(path, deployment)
        await engine.promote(candidate.id, actor="host", approval=approval)
        deployment.fail_withdrawal = True
        target = EvolutionDeletionTarget(engine, candidate.scope)
        with pytest.raises(EvolutionWithdrawalError):
            await target.forget_sources(
                ForgetRequest(candidate.scope, ("source",), mode=ForgetMode.ARCHIVE)
            )
        archived = await registry.get(candidate.id)
        assert archived.procedure == candidate.procedure and not archived.erasure_pending
        with pytest.raises(EvolutionWithdrawalError):
            await target.forget_sources(
                ForgetRequest(candidate.scope, ("source",), mode=ForgetMode.ERASE)
            )
        pending = await registry.get(candidate.id)
        assert pending.erasure_pending and pending.withdrawal_pending
        assert pending.procedure.id == candidate.procedure.id
        with pytest.raises(EvolutionGateError, match="withdrawn"):
            export_skill(pending)
        deployment.fail_withdrawal = False
        reopened = EvolutionEngine(
            SQLiteEvolutionRegistry(path), DeterministicPromotionPolicy(), deployment
        )
        await reopened.initialize()
        await reopened.recover()
        erased = await registry.get(candidate.id)
        assert erased.erased and not erased.erasure_pending
        assert erased.procedure.steps == () and erased.procedure.provenance.source_event_ids == ()
        assert await registry.latest_evaluation(candidate.id, EvaluationStage.OFFLINE) is None
        await target.forget_sources(
            ForgetRequest(candidate.scope, ("source",), mode=ForgetMode.ERASE)
        )

    asyncio.run(scenario())


def test_bridge_rejects_other_scope_and_inflight_activation_cannot_acknowledge(tmp_path):
    async def scenario():
        deployment = Deployment()
        deployment.release.clear()
        engine, registry, candidate, approval = await canary(tmp_path / "e.db", deployment)
        target = EvolutionDeletionTarget(engine, candidate.scope)
        with pytest.raises(ValueError, match="scope"):
            await target.forget_sources(ForgetRequest(MemoryScope("other"), ("source",)))
        task = asyncio.create_task(engine.promote(candidate.id, actor="host", approval=approval))
        await deployment.entered.wait()
        with pytest.raises(EvolutionWithdrawalError):
            await target.forget_sources(ForgetRequest(candidate.scope, all_in_scope=True))
        deployment.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await target.forget_sources(ForgetRequest(candidate.scope, all_in_scope=True))
        assert not (await registry.get(candidate.id)).withdrawal_pending
        assert not deployment.live

    asyncio.run(scenario())
