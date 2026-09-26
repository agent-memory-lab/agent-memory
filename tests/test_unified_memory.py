import asyncio
from datetime import UTC, datetime
from dataclasses import replace

import pytest

from agent_memory import MemoryScope
from agent_memory.unified_memory import UnifiedMemory, PendingMemoryDeletion
from agent_memory.ontology_rules import DerivedMemory
from agent_memory.ontology_rule_candidates import SQLiteRuleCandidateStore


SCOPE = MemoryScope("unified-test", session_id="session")


class Generator:
    def __init__(self):
        self.events = []

    async def generate_claims(self, event):
        self.events.append(event)
        return [{"key": "preference", "value": event.content, "text": event.content,
                 "scope": "session", "confidence": 0.95}]


async def capture(memory, event_id="one", content="I prefer tea", role="user"):
    return await memory.capture(event_id=event_id, role=role, content=content,
                                run_id="run", occurred_at=datetime(2026, 9, 26, tzinfo=UTC))


@pytest.mark.parametrize("role", ["user", "assistant", "tool"])
def test_raw_capture_extract_recall_and_delete(tmp_path, role):
    async def scenario():
        generator = Generator()
        memory = UnifiedMemory.local(tmp_path / "core.db", SCOPE, generator=generator)
        await memory.initialize()
        try:
            first = await capture(memory, role=role)
            duplicate = await capture(memory, role=role)
            assert duplicate.duplicate and duplicate.provider_event_id == first.provider_event_id
            assert len(generator.events) == 1
            assert "claims" not in generator.events[0].metadata
            assert (await memory.recall("tea")).relevant_memories
            await memory.forget_sources((first.provider_event_id,))
            assert not (await memory.recall("tea")).relevant_memories
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_raw_capture_redacts_before_generator_and_rejects_scope_promotion(tmp_path):
    async def scenario():
        generator = Generator()
        memory = UnifiedMemory.local(tmp_path / "core.db", SCOPE, generator=generator)
        await memory.initialize()
        try:
            await capture(memory, content="contact demo@example.com")
            assert "demo@example.com" not in generator.events[0].content
            async def promoted(event):
                return [{"key": "bad", "value": "bad", "text": "bad", "scope": "tenant"}]
            generator.generate_claims = promoted
            with pytest.raises(ValueError):
                await capture(memory, "two")
        finally:
            await memory.close()
    asyncio.run(scenario())


def test_deletion_pending_blocks_reads_and_writes_across_restart(tmp_path):
    class Target:
        failing = True
        calls = 0
        async def forget_sources(self, request):
            self.calls += 1
            if self.failing:
                raise RuntimeError("temporary cleanup failure")

    async def scenario():
        target = Target()
        path = tmp_path / "core.db"
        memory = UnifiedMemory.local(path, SCOPE, targets={"target": target})
        await memory.initialize()
        try:
            receipt = await capture(memory)
            with pytest.raises(RuntimeError, match="cleanup"):
                await memory.forget_sources((receipt.provider_event_id,))
            with pytest.raises(PendingMemoryDeletion):
                await memory.recall("tea")
            with pytest.raises(PendingMemoryDeletion):
                await capture(memory, "two")
        finally:
            await memory.close()
        reopened = UnifiedMemory.local(path, SCOPE, targets={"target": target})
        await reopened.initialize()
        try:
            with pytest.raises(PendingMemoryDeletion):
                await reopened.recall("tea")
            target.failing = False
            await reopened.resume_deletion()
            assert target.calls == 2
            assert not (await reopened.recall("tea")).relevant_memories
            assert await reopened.resume_deletion() is None
        finally:
            await reopened.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("erase", [True, False])
def test_source_deletion_cascades_candidates_without_cross_scope_leak(tmp_path, erase):
    async def scenario():
        sink = SQLiteRuleCandidateStore(tmp_path / "candidates.db")
        await sink.initialize()
        memory = UnifiedMemory.local(tmp_path / "core.db", SCOPE, targets={"candidates": sink})
        await memory.initialize()
        try:
            receipt = await capture(memory)
            candidate = DerivedMemory("derived-test", SCOPE, "schema", "rules", "a", "knows", "c",
                ("claim-a", "claim-b"), "proof", (receipt.provider_event_id,), ("chain@1",),
                datetime(2026, 9, 26, tzinfo=UTC), None, 0.9)
            foreign = replace(candidate, scope=MemoryScope("foreign"))
            unrelated = replace(candidate, candidate_id="other", source_event_ids=("unrelated",))
            for value in (candidate, foreign, unrelated):
                await sink.put(value)
            await memory.forget_sources((receipt.provider_event_id,), erase=erase)
            assert await sink.get(SCOPE, candidate.candidate_id) is None
            assert await sink.get(foreign.scope, foreign.candidate_id) == foreign
            assert await sink.get(SCOPE, unrelated.candidate_id) == unrelated
            if erase:
                assert not await sink.erase(SCOPE, candidate.candidate_id)
            else:
                with pytest.raises(ValueError, match="archived"):
                    await sink.put(candidate)
            await memory.forget_sources(all_in_scope=True)
            assert await sink.get(SCOPE, unrelated.candidate_id) is None
            assert await sink.get(foreign.scope, foreign.candidate_id) == foreign
        finally:
            await memory.close()
    asyncio.run(scenario())
