"""Dispute barriers and equivalent-time boundaries for typed Atom history."""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta, timezone
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from agent_memory import AgentMemory, MemoryEvent, MemoryScope
from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.domain import AtomDraft, PredicateSpec, SourceAuthority

PREDICATE = "residence.city"
AUTHORITY = SourceAuthority("authenticated-user", subjects=("alice",), predicates=(PREDICATE,))
POLICY = AdmissionPolicy((PredicateSpec(PREDICATE),))


def at(day):
    return datetime(2026, 9, day, tzinfo=UTC)


@pytest.fixture(params=["sqlite", "postgres"])
def edge_store(request, tmp_path, monkeypatch):
    import agent_memory.consolidation.admission_runtime as admission
    import agent_memory.domain as domain
    import agent_memory.kernel as kernel
    import agent_memory.retrieval.temporal_history as history
    import agent_memory.sqlite as local

    clock = [at(1)]
    modules = [admission, domain, kernel, history, local]
    scope = MemoryScope(f"atom-edges-{uuid4().hex}", user_id="alice", session_id="session")
    if request.param == "postgres":
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN", "")
        if not dsn:
            pytest.skip("real PostgreSQL Atom edge tests require a test DSN")
        parsed = urlsplit(dsn)
        if parsed.scheme not in {"postgres", "postgresql"} or "test" not in parsed.path.casefold():
            pytest.fail("Atom edge PostgreSQL tests require a database named test")
        modules.extend(
            pytest.importorskip(f"agent_memory_postgres.{name}")
            for name in ("admission", "repository", "temporal_history")
        )
        from agent_memory_postgres import build_postgres_kernel

        memory = AgentMemory(build_postgres_kernel(dsn), scope)
    else:
        memory = AgentMemory.local(tmp_path / "atom-edges.db", scope=scope)
    for module in modules:
        monkeypatch.setattr(module, "utc_now", lambda: clock[0])

    @asynccontextmanager
    async def open_store():
        await memory.initialize()
        try:
            yield memory, clock
        finally:
            await memory.__aexit__(None, None, None)

    return open_store


async def admit(memory, clock, value, start, known, *, explicit=True, end=None, observed=None):
    clock[0] = at(known)
    text = f"Alice lives in {value}."
    event = MemoryEvent(memory.scope, "user.message", text, occurred_at=observed or at(start))
    draft = AtomDraft(
        "alice",
        PREDICATE,
        value,
        text,
        text,
        valid_from=at(start) if explicit else None,
        valid_to=at(end) if end else None,
    )
    return await memory.provider.admit_event(event, (draft,), authority=AUTHORITY, policy=POLICY)


async def state(memory, valid, known):
    return await memory.provider.get_state_at(memory.scope, valid_at=at(valid), known_at=at(known))


def test_equivalent_offset_observation_and_explicit_time_share_a_conflict_boundary(edge_store):
    async def scenario():
        async with edge_store() as (memory, clock):
            offset = datetime(2026, 9, 1, 8, tzinfo=timezone(timedelta(hours=8)))
            original = await admit(memory, clock, "Shanghai", 1, 1, explicit=False, observed=offset)
            assert original.decisions[0].action == "ACCEPT"
            competing = await admit(memory, clock, "Hangzhou", 1, 10)
            assert competing.decisions[0].action == "CONTESTED"
            assert competing.claim_ids == ()
            assert await state(memory, 2, 11) == ()
            assert [claim.value for claim in await state(memory, 2, 2)] == ["Shanghai"]

    asyncio.run(scenario())


def test_late_dispute_is_bounded_by_the_next_established_replacement(edge_store):
    async def scenario():
        async with edge_store() as (memory, clock):
            await admit(memory, clock, "Shanghai", 1, 1)
            await admit(memory, clock, "Beijing", 10, 10)
            disputed = await admit(memory, clock, "Hangzhou", 5, 20, explicit=False)
            assert disputed.decisions[0].action == "CONTESTED"
            assert await state(memory, 6, 21) == ()
            assert [claim.value for claim in await state(memory, 12, 21)] == ["Beijing"]
            before = (await state(memory, 4, 21))[0]
            assert before.value == "Shanghai"
            assert before.valid_to == at(5)
            assert [claim.value for claim in await state(memory, 6, 19)] == ["Shanghai"]
            assert [claim.value for claim in await memory.provider.get_state(memory.scope)] == [
                "Beijing"
            ]

    asyncio.run(scenario())


def test_expired_contested_replacement_stays_unknown_until_a_new_accepted_boundary(edge_store):
    async def scenario():
        async with edge_store() as (memory, clock):
            await admit(memory, clock, "Shanghai", 1, 1)
            disputed = await admit(memory, clock, "Hangzhou", 5, 20, explicit=False, end=8)
            assert disputed.decisions[0].action == "CONTESTED"
            assert await state(memory, 6, 21) == ()
            assert await state(memory, 9, 21) == ()
            assert (await state(memory, 4, 21))[0].valid_to == at(5)
            assert [claim.value for claim in await state(memory, 9, 19)] == ["Shanghai"]
            restored = await admit(memory, clock, "Beijing", 10, 25)
            assert restored.decisions[0].action == "ACCEPT"
            assert await state(memory, 9, 26) == ()
            assert [claim.value for claim in await state(memory, 10, 26)] == ["Beijing"]

    asyncio.run(scenario())


def test_resolving_duplicate_value_never_records_a_false_refutation(edge_store):
    async def scenario():
        async with edge_store() as (memory, clock):
            clock[0] = at(1)
            reports = (
                ("Shanghai", "First report: Shanghai."),
                ("Shanghai", "Repeated report: Shanghai."),
                ("Hangzhou", "Alternative report: Hangzhou."),
            )
            original = MemoryEvent(
                memory.scope,
                "user.message",
                " ".join(text for _, text in reports),
                occurred_at=at(1),
            )
            drafts = tuple(
                AtomDraft("alice", PREDICATE, value, text, text, valid_from=at(1))
                for value, text in reports
            )
            receipt = await memory.provider.admit_event(
                original, drafts, authority=AUTHORITY, policy=POLICY
            )
            assert len(set(receipt.candidate_ids)) == 3
            assert [decision.action for decision in receipt.decisions] == ["CONTESTED"] * 3
            chosen_id, duplicate_id, contrary_id = receipt.candidate_ids
            clock[0] = at(10)
            proof = MemoryEvent(
                memory.scope,
                "tool.result",
                "Confirmed: Alice lives in Shanghai.",
                occurred_at=at(10),
            )
            selected = await memory.provider.resolve_atom(
                memory.scope,
                chosen_id,
                event=proof,
                authority=SourceAuthority(
                    "authenticated-tool", "tool_observation", ("alice",), (PREDICATE,)
                ),
                policy=POLICY,
                expected_version=1,
                accept=True,
                source_quote=proof.content,
                support_from=at(1),
            )
            assert [decision.action for decision in selected.decisions] == ["ACCEPT"]
            duplicate = await memory.provider.admission_status(memory.scope, duplicate_id)
            contrary = await memory.provider.admission_status(memory.scope, contrary_id)
            assert duplicate["payload"]["action"] == "L0_ONLY"
            assert contrary["payload"]["action"] == "REJECT"
            for record, expected_relation in ((duplicate, "supports"), (contrary, "refutes")):
                proof_evidence = [
                    evidence
                    for evidence in record["payload"]["evidence"]
                    if evidence["source_event_id"] == proof.id
                ]
                assert len(proof_evidence) == 1
                assert proof_evidence[0]["relation"] == expected_relation
            current = await state(memory, 2, 11)
            assert [claim.value for claim in current] == ["Shanghai"]
            assert (current[0].id,) == selected.claim_ids
            assert await state(memory, 2, 2) == ()

    asyncio.run(scenario())
