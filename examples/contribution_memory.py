"""Separate state termination evidence from new-value evidence; then erase the latter."""

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.consolidation.contributions import ContributionMemory
from agent_memory.domain import (
    AtomDraft,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryScope,
    PredicateSpec,
    SourceAuthority,
    utc_now,
)
from agent_memory.sqlite import SQLiteMemoryRepository


def at(day):
    return datetime(2026, 10, day, tzinfo=UTC)


async def main():
    with TemporaryDirectory(prefix="agent-memory-contribution-demo-") as temporary:
        repository = SQLiteMemoryRepository(Path(temporary) / "memory.db")
        await repository.initialize()
        scope = MemoryScope("demo", user_id="alice", session_id="session")
        engine = AdmissionEngine(repository)
        service = ContributionMemory(engine, scope, principal="authenticated:alice")
        authority = SourceAuthority("user:alice", subjects=("alice",), predicates=("city",))
        policy = AdmissionPolicy([PredicateSpec("city")])
        candidates, sources = [], []
        for city, day in (("Hangzhou", 1), ("Shanghai", 5)):
            sentence = f"Alice lives in {city}"
            event = MemoryEvent(scope, "message", sentence, occurred_at=at(day))
            receipt = await engine.admit(
                event,
                [AtomDraft("alice", "city", city, sentence, sentence, valid_from=at(day))],
                authority=authority,
                policy=policy,
            )
            candidates.append(receipt.candidate_ids[0])
            sources.append(event.id)
        ending = MemoryEvent(scope, "message", "Alice left Hangzhou on October 5")
        # The host approves the end assertion independently of Shanghai's evidence.
        await service.transition(
            candidates[1],
            predecessor_ids=[candidates[0]],
            valid_from=at(5),
            event=ending,
            expected_versions=await service.snapshot(candidates[1]),
            authority=authority,
            policy=policy,
            source_quote=ending.content,
        )
        await repository.forget(
            ForgetRequest(scope, memory_ids=(sources[1],), mode=ForgetMode.ERASE)
        )
        for day in (2, 6):
            claims, _ = await engine.state(scope, valid_at=at(day), known_at=utc_now())
            values = [claim.value for claim in claims]
            assert values == (["Hangzhou"] if day == 2 else [])
            print(f"October {day}: {values or 'unknown'}")


if __name__ == "__main__":
    asyncio.run(main())
