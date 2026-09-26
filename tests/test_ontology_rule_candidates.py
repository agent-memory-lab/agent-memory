import asyncio
from dataclasses import replace
import subprocess
import sys

import pytest

from agent_memory import MemoryScope
from agent_memory.ontology_rule_candidates import DurableRuleCandidates, SQLiteRuleCandidateStore
from agent_memory.ontology_rules import OntologyRuleEngine, RelationRule
from test_ontology_queries import SCHEMA, SCOPE, prepare, projection, store


class Evidence:
    available = True
    error = None

    async def verify(self, scope, ids):
        if self.error:
            raise self.error
        return self.available


async def configured(store, tmp_path, **kwargs):
    await prepare(store)
    roots = (projection("person:a", "person:b"), projection("person:b", "person:c"))
    for value in roots:
        await store.upsert_projection(value)
    evidence = Evidence()
    rule = RelationRule("chain", "1", "knows", "knows", "knows")
    engine = OntologyRuleEngine(store, SCHEMA, evidence, (rule,))
    candidate = (await engine.derive(SCOPE, tuple(p.assertion.assertion_id for p in roots))).candidates[0]
    sink = SQLiteRuleCandidateStore(tmp_path / "candidates.db", **kwargs)
    await sink.initialize()
    return DurableRuleCandidates(SCOPE, engine, sink), candidate, evidence


def test_candidates_survive_new_process_and_revalidate(store, tmp_path):
    async def scenario():
        memory, candidate, _ = await configured(store, tmp_path)
        await memory.save(candidate)
        await memory.save(candidate)
        code = """
import asyncio, sys
from agent_memory import MemoryScope
from agent_memory.ontology_rule_candidates import SQLiteRuleCandidateStore
async def main():
    value = await SQLiteRuleCandidateStore(sys.argv[1]).get(
        MemoryScope('query-test', user_id='alice'), sys.argv[2])
    assert value is not None and value.object == 'person:c'
asyncio.run(main())
"""
        result = await asyncio.to_thread(subprocess.run,
            [sys.executable, "-c", code, str(memory.store.path), candidate.candidate_id],
            capture_output=True, text=True, timeout=15)
        assert result.returncode == 0, result.stderr
        reopened = DurableRuleCandidates(SCOPE, memory.engine, SQLiteRuleCandidateStore(memory.store.path))
        assert await reopened.get(candidate.candidate_id) == candidate
    asyncio.run(scenario())


def test_candidates_archive_invalid_proof_and_require_explicit_erasure(store, tmp_path):
    async def scenario():
        memory, candidate, evidence = await configured(store, tmp_path)
        await memory.save(candidate)
        evidence.available = False
        assert await memory.get(candidate.candidate_id) is None
        evidence.available = True
        with pytest.raises(ValueError, match="archived"):
            await memory.save(candidate)
        assert await memory.erase(candidate.candidate_id)
        assert not await memory.erase(candidate.candidate_id)
        await memory.save(candidate)
        assert await memory.get(candidate.candidate_id) == candidate
    asyncio.run(scenario())


def test_candidates_transient_error_does_not_archive(store, tmp_path):
    async def scenario():
        memory, candidate, evidence = await configured(store, tmp_path)
        await memory.save(candidate)
        evidence.error = TimeoutError("source unavailable")
        with pytest.raises(TimeoutError):
            await memory.get(candidate.candidate_id)
        evidence.error = None
        assert await memory.get(candidate.candidate_id) == candidate
    asyncio.run(scenario())


def test_candidates_scope_and_rule_version_isolation(store, tmp_path):
    async def scenario():
        memory, candidate, evidence = await configured(store, tmp_path)
        await memory.save(candidate)
        foreign = DurableRuleCandidates(MemoryScope("foreign"), memory.engine, memory.store)
        assert await foreign.get(candidate.candidate_id) is None
        assert not await foreign.archive(candidate.candidate_id)
        assert not await foreign.erase(candidate.candidate_id)
        with pytest.raises(ValueError, match="scope"):
            await foreign.save(candidate)
        next_engine = OntologyRuleEngine(store, SCHEMA, evidence,
            (RelationRule("chain", "2", "knows", "knows", "knows"),))
        next_memory = DurableRuleCandidates(SCOPE, next_engine, memory.store)
        assert await next_memory.get(candidate.candidate_id) is None
        assert await memory.get(candidate.candidate_id) == candidate
    asyncio.run(scenario())


def test_candidate_capacity_is_atomic_under_concurrent_writers(store, tmp_path):
    async def scenario():
        memory, candidate, _ = await configured(store, tmp_path, max_records=3)
        values = [replace(candidate, candidate_id=f"capacity-{i}") for i in range(12)]
        results = await asyncio.gather(*(memory.store.put(c) for c in values), return_exceptions=True)
        assert sum(r is None for r in results) == 3
        assert all(r is None or isinstance(r, ValueError) and "full" in str(r) for r in results)
        survivors = [c for c, r in zip(values, results) if r is None]
        await memory.store.put(survivors[0])
        await memory.store.archive(SCOPE, survivors[0].candidate_id)
        with pytest.raises(ValueError, match="full"):
            await memory.store.put(candidate)
        await memory.store.erase(SCOPE, survivors[0].candidate_id)
        await memory.save(candidate)
    asyncio.run(scenario())


def test_candidates_payload_limit_and_identity_collision(store, tmp_path):
    async def scenario():
        memory, candidate, _ = await configured(store, tmp_path)
        await memory.save(candidate)
        with pytest.raises(ValueError, match="different payload"):
            await memory.store.put(replace(candidate, confidence=0.1))
        with pytest.raises(ValueError, match="byte budget"):
            await memory.store.put(replace(candidate, candidate_id="large", object="x" * 65536))
        assert await memory.get(candidate.candidate_id) == candidate
    asyncio.run(scenario())


def test_candidate_erased_during_verification_is_not_returned(store, tmp_path):
    async def scenario():
        memory, candidate, _ = await configured(store, tmp_path)
        await memory.save(candidate)
        original = memory.engine.valid
        async def valid(value):
            accepted = await original(value)
            await memory.erase(value.candidate_id)
            return accepted
        memory.engine.valid = valid
        assert await memory.get(candidate.candidate_id) is None
    asyncio.run(scenario())
