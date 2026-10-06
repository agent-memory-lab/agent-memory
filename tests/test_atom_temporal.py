"""Typed Atom temporal behavior against SQLite and real PostgreSQL."""

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from agent_memory import AgentMemory, MemoryEvent, MemoryQuery, MemoryScope
from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.domain import AtomDraft, PredicateSpec, SourceAuthority

PREDICATE = "residence.city"
AUTHORITY = SourceAuthority("authenticated-user", subjects=("user:alice",), predicates=(PREDICATE,))
POLICY = AdmissionPolicy((PredicateSpec(PREDICATE),))


def at(day, hour=0):
    return datetime(2026, 9, day, hour, tzinfo=UTC)


@pytest.fixture(params=["sqlite", "postgres"])
def atom_store(request, tmp_path, monkeypatch):
    import agent_memory.consolidation.admission_runtime as admission
    import agent_memory.domain as domain
    import agent_memory.kernel as kernel
    import agent_memory.retrieval.temporal_history as history
    import agent_memory.sqlite as local

    clock = [at(1)]
    modules = [admission, domain, kernel, history, local]
    scope = MemoryScope(f"atom-temporal-{uuid4().hex}", user_id="alice", session_id="session")
    if request.param == "postgres":
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN", "")
        if not dsn:
            pytest.skip("real PostgreSQL Atom temporal tests require a test DSN")
        parsed = urlsplit(dsn)
        if parsed.scheme not in {"postgres", "postgresql"} or "test" not in parsed.path.casefold():
            pytest.fail("Atom temporal PostgreSQL tests require a database named test")
        modules.extend(
            pytest.importorskip(f"agent_memory_postgres.{name}")
            for name in ("admission", "repository", "temporal_history")
        )
        from agent_memory_postgres import build_postgres_kernel

        memory = AgentMemory(build_postgres_kernel(dsn), scope)
    else:
        memory = AgentMemory.local(tmp_path / "atoms.db", scope=scope)
    for module in modules:
        monkeypatch.setattr(module, "utc_now", lambda: clock[0])

    @asynccontextmanager
    async def open_store():
        await memory.initialize()
        try:
            yield memory, clock, AdmissionEngine(memory.provider._repository)
        finally:
            await memory.__aexit__(None, None, None)

    return open_store


async def admit(
    memory,
    clock,
    value,
    valid,
    known,
    *,
    end=None,
    change="replace",
    corrects=None,
    explicit=True,
    authority=AUTHORITY,
):
    clock[0] = at(known)
    text = f"Alice resides in {value}."
    draft = AtomDraft(
        "user:alice",
        PREDICATE,
        value,
        text,
        text,
        valid_from=at(valid) if explicit else None,
        valid_to=at(end) if end else None,
        change_kind=change,
        corrects_id=corrects,
    )
    event = MemoryEvent(memory.scope, "user.message", text, occurred_at=at(valid))
    receipt = await memory.provider.admit_event(event, (draft,), authority=authority, policy=POLICY)
    return receipt


async def state(memory, valid, known):
    return await memory.provider.get_state_at(memory.scope, valid_at=at(valid), known_at=at(known))


async def values(memory, valid, known):
    return [claim.value for claim in await state(memory, valid, known)]


def assert_accepted(receipt):
    assert [decision.action for decision in receipt.decisions] == ["ACCEPT"]
    assert len(receipt.claim_ids) == 1


def test_real_replacement_expiry_becomes_unknown_without_reviving_old_state(atom_store):
    async def scenario():
        async with atom_store() as (memory, clock, _):
            assert_accepted(await admit(memory, clock, "Shanghai", 1, 1))
            assert_accepted(await admit(memory, clock, "Hangzhou", 5, 10, end=8))
            assert await values(memory, 4, 11) == ["Shanghai"]
            assert await values(memory, 5, 11) == ["Hangzhou"]
            assert await values(memory, 8, 11) == []
            assert await memory.provider.get_state(memory.scope) == ()
            assert await values(memory, 9, 2) == ["Shanghai"]
            recalled = await memory.provider.retrieve(MemoryQuery(memory.scope, "resides"))
            assert recalled.current_state == ()
            assert not any(
                item.text == "Alice resides in Shanghai." for item in recalled.relevant_memories
            )

    asyncio.run(scenario())


def test_temporary_override_restores_live_base_and_reports_effective_segments(atom_store):
    async def scenario():
        async with atom_store() as (memory, clock, _):
            first = await admit(memory, clock, "Shanghai", 1, 1)
            override = await admit(
                memory, clock, "Hangzhou", 5, 10, end=8, change="temporary_override"
            )
            assert_accepted(override)
            before = (await state(memory, 4, 11))[0]
            during = (await state(memory, 5, 11))[0]
            after = (await state(memory, 8, 11))[0]
            assert before.value == after.value == "Shanghai"
            assert before.id == after.id == first.claim_ids[0]
            assert during.value == "Hangzhou"
            assert (before.valid_from, before.valid_to) == (at(1), at(5))
            assert (during.valid_from, during.valid_to) == (at(5), at(8))
            assert (after.valid_from, after.valid_to) == (at(8), None)

    asyncio.run(scenario())


def test_replacement_during_override_never_restores_superseded_base(atom_store):
    async def scenario():
        async with atom_store() as (memory, clock, _):
            await admit(memory, clock, "Shanghai", 1, 1)
            await admit(memory, clock, "Hangzhou", 5, 10, end=12, change="temporary_override")
            assert_accepted(await admit(memory, clock, "Beijing", 8, 15, end=10))
            assert await values(memory, 7, 16) == ["Hangzhou"]
            assert (await state(memory, 7, 16))[0].valid_to == at(8)
            assert await values(memory, 8, 16) == ["Beijing"]
            assert await values(memory, 10, 16) == []
            assert await values(memory, 12, 16) == []
            # Before the later replacement was learned, the override still applied.
            assert await values(memory, 9, 11) == ["Hangzhou"]
            assert await values(memory, 12, 11) == ["Shanghai"]

    asyncio.run(scenario())


def test_late_observation_fills_old_interval_without_overwriting_later_change(atom_store):
    async def scenario():
        async with atom_store() as (memory, clock, _):
            await admit(memory, clock, "Shanghai", 1, 1)
            await admit(memory, clock, "Beijing", 10, 10)
            assert_accepted(await admit(memory, clock, "Hangzhou", 3, 20))
            assert await values(memory, 4, 15) == ["Shanghai"]
            assert await values(memory, 4, 21) == ["Hangzhou"]
            assert (await state(memory, 4, 21))[0].valid_to == at(10)
            assert await values(memory, 12, 21) == ["Beijing"]
            assert [claim.value for claim in await memory.provider.get_state(memory.scope)] == [
                "Beijing"
            ]

    asyncio.run(scenario())


def test_retroactive_correction_changes_reality_interval_only_after_it_is_known(atom_store):
    async def scenario():
        async with atom_store() as (memory, clock, _):
            await admit(memory, clock, "Shanghai", 1, 1)
            moved = await admit(memory, clock, "Hangzhou", 5, 10)
            corrected = await admit(
                memory, clock, "Hangzhou", 8, 20, change="correct", corrects=moved.candidate_ids[0]
            )
            assert_accepted(corrected)
            assert await values(memory, 6, 15) == ["Hangzhou"]
            assert await values(memory, 6, 21) == ["Shanghai"]
            current = (await state(memory, 8, 21))[0]
            assert current.id == corrected.claim_ids[0]
            assert current.corrects_id == moved.claim_ids[0]
            assert current.system_from == at(20)
            assert (await state(memory, 6, 21))[0].valid_to == at(8)
            old_view = (await state(memory, 6, 15))[0]
            assert (old_view.system_from, old_view.system_to) == (at(10), at(20))

    asyncio.run(scenario())


def test_future_effective_change_is_visible_only_on_its_reality_boundary(atom_store):
    async def scenario():
        async with atom_store() as (memory, clock, _):
            await admit(memory, clock, "Shanghai", 1, 1)
            await admit(memory, clock, "Hangzhou", 20, 10)
            assert [claim.value for claim in await memory.provider.get_state(memory.scope)] == [
                "Shanghai"
            ]
            assert await values(memory, 20, 2) == ["Shanghai"]
            assert await values(memory, 19, 11) == ["Shanghai"]
            before_learning = (await state(memory, 19, 2))[0]
            after_learning = (await state(memory, 19, 11))[0]
            assert before_learning.valid_to is None
            assert (before_learning.system_from, before_learning.system_to) == (at(1), at(10))
            assert after_learning.valid_to == at(20)
            assert (after_learning.system_from, after_learning.system_to) == (at(10), None)
            assert await values(memory, 20, 11) == ["Hangzhou"]

    asyncio.run(scenario())


def test_point_observation_does_not_claim_interval_evidence_or_backfill_the_past(atom_store):
    async def scenario():
        async with atom_store() as (memory, clock, engine):
            first = await admit(memory, clock, "Shanghai", 5, 10, explicit=False)
            at_point, point_info = await engine.state(memory.scope, valid_at=at(5), known_at=at(11))
            support = point_info["atom_support"][at_point[0].id]
            assert support["basis"] == "source_assertion"
            assert [item["source_event_id"] for item in support["evidence"]] == [first.event_id]
            assert support["evidence"][0]["support_kind"] == "point"
            later, later_info = await engine.state(memory.scope, valid_at=at(6), known_at=at(11))
            assert later[0].value == "Shanghai"
            assert later_info["atom_support"][later[0].id] == {
                "candidate_id": first.candidate_ids[0],
                "evidence": [],
                "basis": "assumed_continuity",
            }
            assert await values(memory, 4, 11) == []
            second = await admit(memory, clock, "Shanghai", 12, 20, explicit=False)
            old, old_info = await engine.state(memory.scope, valid_at=at(6), known_at=at(21))
            assert old[0].id == first.claim_ids[0]
            assert old_info["atom_support"][old[0].id]["evidence"] == []
            now, now_info = await engine.state(memory.scope, valid_at=at(12), known_at=at(21))
            assert [
                item["source_event_id"] for item in now_info["atom_support"][now[0].id]["evidence"]
            ] == [second.event_id]

    asyncio.run(scenario())


def test_pending_point_verification_does_not_prove_earlier_validity(atom_store):
    async def scenario():
        async with atom_store() as (memory, clock, engine):
            untrusted = SourceAuthority(
                "model-output", "inferred", AUTHORITY.subjects, AUTHORITY.predicates
            )
            pending = await admit(
                memory, clock, "Shanghai", 1, 5, explicit=False, authority=untrusted
            )
            assert pending.decisions[0].action == "PENDING_VERIFICATION"
            clock[0] = at(20)
            event = MemoryEvent(
                memory.scope, "tool.result", "Alice resides in Shanghai.", occurred_at=at(10)
            )
            resolved = await engine.resolve(
                memory.scope,
                pending.candidate_ids[0],
                event=event,
                authority=SourceAuthority(
                    "authenticated-tool",
                    "tool_observation",
                    AUTHORITY.subjects,
                    AUTHORITY.predicates,
                ),
                policy=POLICY,
                expected_version=1,
                accept=True,
                source_quote=event.content,
            )
            assert_accepted(resolved)
            assert await values(memory, 10, 19) == []
            assert await values(memory, 9, 21) == []
            assert await values(memory, 10, 21) == ["Shanghai"]
            claims, metadata = await engine.state(memory.scope, valid_at=at(11), known_at=at(21))
            assert metadata["atom_support"][claims[0].id]["evidence"] == []
            assert metadata["atom_support"][claims[0].id]["basis"] == "assumed_continuity"

    asyncio.run(scenario())


def test_missing_quote_cannot_become_interval_support_after_point_verification(atom_store):
    async def scenario():
        async with atom_store() as (memory, clock, engine):
            clock[0] = at(5)
            original = MemoryEvent(
                memory.scope, "user.message", "Alice has not shared her city.", occurred_at=at(1)
            )
            draft = AtomDraft(
                "user:alice",
                PREDICATE,
                "Shanghai",
                "Alice resides in Shanghai.",
                "Alice resides in Shanghai.",
                valid_from=at(1),
            )
            pending = await memory.provider.admit_event(
                original, (draft,), authority=AUTHORITY, policy=POLICY
            )
            assert pending.decisions[0].action == "PENDING_VERIFICATION"
            assert "source_quote_not_found" in pending.decisions[0].reasons
            clock[0] = at(20)
            verified = MemoryEvent(
                memory.scope, "tool.result", "Alice resides in Shanghai.", occurred_at=at(10)
            )
            await engine.resolve(
                memory.scope,
                pending.candidate_ids[0],
                event=verified,
                authority=SourceAuthority(
                    "authenticated-tool",
                    "tool_observation",
                    AUTHORITY.subjects,
                    AUTHORITY.predicates,
                ),
                policy=POLICY,
                expected_version=1,
                accept=True,
                source_quote=verified.content,
            )
            claims, metadata = await engine.state(memory.scope, valid_at=at(11), known_at=at(21))
            support = metadata["atom_support"][claims[0].id]
            assert support["evidence"] == []
            assert support["basis"] == "assumed_continuity"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("original_explicit", "proof_interval"), [(True, True), (True, False), (False, False)]
)
def test_pending_verification_must_recheck_conflicts_before_publishing(
    atom_store, original_explicit, proof_interval
):
    async def scenario():
        async with atom_store() as (memory, clock, engine):
            await admit(memory, clock, "Shanghai", 1, 1)
            untrusted = SourceAuthority(
                "model-output", "inferred", AUTHORITY.subjects, AUTHORITY.predicates
            )
            pending = await admit(
                memory,
                clock,
                "Hangzhou",
                1,
                5,
                explicit=original_explicit,
                authority=untrusted,
            )
            assert pending.decisions[0].action == "PENDING_VERIFICATION"
            assert await values(memory, 12, 6) == ["Shanghai"]
            clock[0] = at(20)
            proof = MemoryEvent(
                memory.scope, "tool.result", "Alice resides in Hangzhou.", occurred_at=at(10)
            )
            tool = SourceAuthority(
                "authenticated-tool", "tool_observation", AUTHORITY.subjects, AUTHORITY.predicates
            )
            verified = await engine.resolve(
                memory.scope,
                pending.candidate_ids[0],
                event=proof,
                authority=tool,
                policy=POLICY,
                expected_version=1,
                accept=True,
                source_quote=proof.content,
                support_from=at(1) if proof_interval else None,
            )
            # A source check on a pending candidate has not selected a winner
            # between incompatible facts. Surface the new dispute first.
            assert verified.decisions[0].action == "CONTESTED"
            assert verified.claim_ids == ()
            assert verified.pending_ids == pending.candidate_ids
            assert await values(memory, 12, 21) == []
            assert await values(memory, 12, 19) == ["Shanghai"]
            _, metadata = await engine.state(memory.scope, valid_at=at(12), known_at=at(21))
            assert metadata["conflicts"]
            current = await memory.provider._repository.admission_record(
                memory.scope, pending.candidate_ids[0]
            )
            assert current["version"] == 2
            assert current["payload"]["claim_id"] is None

            # The separate, explicit review of that now-known dispute may pick
            # Hangzhou, provided it supplies exactly the contested interval.
            clock[0] = at(25)
            choice = MemoryEvent(
                memory.scope, "tool.result", "Alice resides in Hangzhou.", occurred_at=at(15)
            )
            resolved = await engine.resolve(
                memory.scope,
                pending.candidate_ids[0],
                event=choice,
                authority=tool,
                policy=POLICY,
                expected_version=2,
                accept=True,
                source_quote=choice.content,
                support_from=at(1) if proof_interval else at(10),
            )
            assert_accepted(resolved)
            assert await values(memory, 12, 26) == ["Hangzhou"]
            assert await values(memory, 12, 24) == []

    asyncio.run(scenario())


def _advance_writer_clock(monkeypatch, start):
    """Use a changing clock so a frozen fixture cannot conceal split publications."""
    ticks = [0]

    def now():
        ticks[0] += 1
        return start + timedelta(milliseconds=ticks[0])

    for name in (
        "agent_memory.domain",
        "agent_memory.kernel",
        "agent_memory.sqlite",
        "agent_memory.consolidation.admission_runtime",
        "agent_memory.retrieval.temporal_history",
        "agent_memory_postgres.admission",
        "agent_memory_postgres.repository",
        "agent_memory_postgres.temporal_history",
    ):
        module = sys.modules.get(name)
        if module is not None:
            monkeypatch.setattr(module, "utc_now", now)


def test_atomic_multi_slot_admission_has_one_system_boundary(atom_store, monkeypatch):
    async def scenario():
        async with atom_store() as (memory, _, _):
            _advance_writer_clock(monkeypatch, at(10))
            language = "response.language"
            authority = SourceAuthority(
                "authenticated-user",
                subjects=AUTHORITY.subjects,
                predicates=(PREDICATE, language),
            )
            policy = AdmissionPolicy((PredicateSpec(PREDICATE), PredicateSpec(language)))
            city_quote = "Alice resides in Shanghai."
            language_quote = "Alice prefers Chinese."
            receipt = await memory.remember_atoms(
                f"{city_quote} {language_quote}",
                (
                    AtomDraft("user:alice", PREDICATE, "Shanghai", city_quote, city_quote),
                    AtomDraft("user:alice", language, "Chinese", language_quote, language_quote),
                ),
                authority=authority,
                policy=policy,
                occurred_at=at(1),
            )
            assert len(receipt.claim_ids) == 2
            rows = [await memory.atom_status(identity) for identity in receipt.candidate_ids]
            boundaries = {datetime.fromisoformat(row["recorded_at"]) for row in rows}
            assert len(boundaries) == 1
            boundary = boundaries.pop()
            before = await memory.provider.get_state_at(
                memory.scope, valid_at=at(1), known_at=boundary - timedelta(microseconds=1)
            )
            after = await memory.provider.get_state_at(
                memory.scope, valid_at=at(1), known_at=boundary
            )
            assert before == ()
            assert {claim.value for claim in after} == {"Shanghai", "Chinese"}

    asyncio.run(scenario())


def test_conflict_resolution_and_peer_rejection_share_system_boundary(atom_store, monkeypatch):
    async def scenario():
        async with atom_store() as (memory, _, engine):
            _advance_writer_clock(monkeypatch, at(10))
            choices = ("Alice resides in Shanghai.", "Alice resides in Hangzhou.")
            receipt = await memory.remember_atoms(
                " ".join(choices),
                tuple(
                    AtomDraft("user:alice", PREDICATE, value, quote, quote, valid_from=at(1))
                    for value, quote in zip(("Shanghai", "Hangzhou"), choices, strict=True)
                ),
                authority=AUTHORITY,
                policy=POLICY,
                occurred_at=at(1),
            )
            assert [decision.action for decision in receipt.decisions] == ["CONTESTED"] * 2
            initial = [await memory.atom_status(identity) for identity in receipt.candidate_ids]
            assert len({row["recorded_at"] for row in initial}) == 1

            _advance_writer_clock(monkeypatch, at(20))
            resolved = await memory.resolve_atom(
                receipt.candidate_ids[0],
                choices[0],
                authority=AUTHORITY,
                policy=POLICY,
                expected_version=1,
                accept=True,
                source_quote=choices[0],
                support_from=at(1),
                occurred_at=at(20),
            )
            assert_accepted(resolved)
            rows = [await memory.atom_status(identity) for identity in receipt.candidate_ids]
            assert {row["version"] for row in rows} == {2}
            assert {row["payload"]["action"] for row in rows} == {"ACCEPT", "REJECT"}
            boundaries = {datetime.fromisoformat(row["recorded_at"]) for row in rows}
            assert len(boundaries) == 1
            boundary = boundaries.pop()
            before, old_metadata = await engine.state(
                memory.scope, valid_at=at(1), known_at=boundary - timedelta(microseconds=1)
            )
            after, new_metadata = await engine.state(
                memory.scope, valid_at=at(1), known_at=boundary
            )
            assert before == () and old_metadata["conflicts"]
            assert [claim.value for claim in after] == ["Shanghai"]
            assert new_metadata["conflicts"] == []

    asyncio.run(scenario())
