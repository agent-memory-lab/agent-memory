"""Output bounds follow authority/conflict checks; SQLite and PostgreSQL share semantics."""

import asyncio
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta

import pytest

from agent_memory import AgentMemory, MemoryLimits
from agent_memory.consolidation.admission import AdmissionPolicy, candidate_id, slot_key
from agent_memory.domain import AtomDraft, MemoryEvent, MemoryScope, PredicateSpec, SourceAuthority
from agent_memory.retrieval.atom_state import project_records, select_record_snapshots
from agent_memory.sqlite import SQLiteMemoryRepository
from test_atom_admission import POLICY, SELF, at, atom, source, store  # noqa: F401


async def legacy(kernel, scope, *, count=96, prefix="ordinary", **fields):
    return await kernel.ingest_event(MemoryEvent(
        scope, "fact", prefix, occurred_at=at(1), metadata={"claims": [
            {"key": f"{prefix}-{index:03d}", "value": index, "text": f"{prefix} {index}",
             "importance": (index % 5) / 5, "confidence": 0.5 + (index % 3) / 4, **fields}
            for index in range(count)
        ]},
    ))


@pytest.mark.parametrize("count", [96, 192])
def test_sqlite_state_reads_one_payload_without_source_n_plus_one(tmp_path, monkeypatch, count):
    async def run():
        import agent_memory.retrieval.temporal_history as history

        async with AgentMemory.local(tmp_path / "bounded.db", limits=MemoryLimits(
            max_state_claims=1,
        )) as memory:
            await legacy(memory.provider, memory.scope, count=count)
            expected = (await memory.provider.get_state(memory.scope))[:1]
            repository = memory.provider._repository
            statements, hydrated = [], []
            original, loader = repository._connection, history.load_claim

            @contextmanager
            def tracing():
                with original() as connection:
                    connection.set_trace_callback(statements.append)
                    yield connection

            def load(payload):
                hydrated.append(payload)
                return loader(payload)

            monkeypatch.setattr(repository, "_connection", tracing)
            monkeypatch.setattr(history, "load_claim", load)
            assert await memory.state() == expected
            assert len(hydrated) == 1
            assert not any("SELECT 1 FROM events WHERE id =" in sql for sql in statements)
            assert len(statements) == 14
            assert any("FROM claim_versions v" in sql and "LIMIT 1" in sql for sql in statements)

    asyncio.run(run())


def test_bounded_state_matches_full_order_and_filters_before_limit(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            scope = replace(scope, agent_id="agent", workspace_id="workspace")
            await legacy(kernel, scope)
            await legacy(kernel, scope, count=1, prefix="inherited", scope_level="tenant")
            for field in ("tenant_id", "namespace", "user_id", "agent_id", "workspace_id", "session_id"):
                await legacy(kernel, replace(scope, **{field: "foreign"}), count=1,
                             prefix=f"foreign-{field}", importance=1.0)
            # A higher-ranked legacy-looking claim backed by a governed
            # source must be removed before LIMIT.
            governed = await legacy(kernel, scope, count=1, prefix="protected", importance=1.0)
            async with engine.repository.unit_of_work() as uow:
                await uow.save_admission_record(
                    scope, "protected-record", governed.event_id, "guarded-slot",
                    {"action": "L0_ONLY"}, 0,
                )
            clock[0] = at(2)
            expected = await kernel.get_state(scope)
            assert len(expected) == 97
            assert not any(c.key.startswith(("foreign", "protected", "atom:")) for c in expected)
            for limit in (1, 64, 96, 97, 192):
                assert await kernel.get_state_bounded(scope, limit=limit) == expected[:limit]

    asyncio.run(run())


def test_atom_conflict_beyond_bound_is_evaluated_before_state_limit(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            predicates = tuple(f"field-{index:03d}" for index in range(96))
            authority = SourceAuthority("user:alice", subjects=("alice",), predicates=predicates)
            policy = AdmissionPolicy([PredicateSpec(predicate) for predicate in predicates])
            drafts = [AtomDraft("alice", predicate, "A", "A", "A", valid_from=at(1))
                      for predicate in predicates]
            for index in range(0, 96, 32):
                await engine.admit(source(scope, "A", identity=f"bounded-atoms-{index}"),
                                   drafts[index:index + 32],
                                   authority=authority, policy=policy)
            first = min(drafts, key=lambda draft: slot_key(scope, draft))
            disputed = replace(first, value="B", text="B", source_quote="B")
            rows = await engine.repository.admission_records(scope)
            # Choose a genuine conflicting candidate sorted after every existing
            # record, so a premature row limit would silently miss it.
            event = source(scope, "B", day=2)
            for index in range(100000):
                trial = replace(event, id=f"late-conflict-{index}")
                if candidate_id(trial, disputed) > max(row["id"] for row in rows):
                    event = trial
                    break
            else:
                raise AssertionError("late conflict fixture search exhausted")
            clock[0] = at(2)
            receipt = await engine.admit(event, [disputed], authority=authority, policy=policy)
            assert receipt.decisions[0].action == "CONTESTED"
            clock[0] = at(3)
            expected = await kernel.get_state(scope)
            assert len(expected) == 95 and all(c.key != slot_key(scope, first) for c in expected)
            assert await kernel.get_state_bounded(scope, limit=1) == expected[:1]
            old = await kernel.get_state_at(scope, valid_at=at(3), known_at=at(1) + timedelta(hours=1))
            assert len(old) == 96 and old[0].key == slot_key(scope, first)

    asyncio.run(run())


def test_current_admission_skips_history_but_historical_projection_matches(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, clock):
            receipt = await engine.admit(source(scope), [atom()], authority=SELF, policy=POLICY)
            identity = receipt.candidate_ids[0]
            # Many persisted versions make all-history reads observably expensive.
            for index in range(96):
                clock[0] = at(1) + timedelta(minutes=index + 1)
                row = await engine.repository.admission_record(scope, identity)
                async with engine.repository.unit_of_work() as uow:
                    await uow.save_admission_record(
                        scope, identity, row["event_id"], row["slot_key"], row["payload"], row["version"],
                    )
            repository = engine.repository
            full = await repository.admission_snapshot(scope)
            assert len(full[0]["versions"]) == 97
            if isinstance(repository, SQLiteMemoryRepository):
                statements, original = [], repository._connection

                @contextmanager
                def tracing():
                    with original() as connection:
                        connection.set_trace_callback(statements.append)
                        yield connection

                monkeypatch.setattr(repository, "_connection", tracing)
            current = await repository.admission_state_snapshot(scope, known_at=at(3))
            assert len(current[0]["versions"]) == 1
            if isinstance(repository, SQLiteMemoryRepository):
                assert not any("FROM admission_versions" in sql for sql in statements)
            for known in (at(1) - timedelta(microseconds=1), at(1),
                          at(1) + timedelta(minutes=50), at(3)):
                optimized = await repository.admission_state_snapshot(scope, known_at=known)
                assert project_records(optimized, at(2)) == project_records(
                    select_record_snapshots(full, known), at(2),
                )

    asyncio.run(run())


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_bounded_provider_rejects_invalid_limit(tmp_path, limit):
    async def run():
        async with AgentMemory.local(tmp_path / "invalid.db") as memory:
            with pytest.raises(ValueError, match="positive integer"):
                await memory.provider.get_state_bounded(memory.scope, limit=limit)
    asyncio.run(run())


def test_runtime_uses_optional_bounded_port_and_keeps_old_provider_fallback(tmp_path):
    async def run():
        async with AgentMemory.local(tmp_path / "compatibility.db") as seed:
            await legacy(seed.provider, seed.scope, count=3)
            claims = await seed.provider.get_state(seed.scope)

            class OldProvider:
                async def initialize(self):
                    pass

                async def get_state(self, scope):
                    return claims

            class BoundedProvider(OldProvider):
                async def get_state(self, scope):
                    raise AssertionError("runtime should prefer the optional bounded port")

                async def get_state_bounded(self, scope, *, limit):
                    assert limit == 1
                    return claims[:limit]

            for provider in (OldProvider(), BoundedProvider()):
                memory = AgentMemory(provider, seed.scope, limits=MemoryLimits(max_state_claims=1))
                await memory.initialize()
                assert await memory.state() == claims[:1]

    asyncio.run(run())


def test_pipeline_state_and_final_guard_only_hydrate_requested_claims(tmp_path, monkeypatch):
    async def run():
        import agent_memory.retrieval.temporal_history as history
        from agent_memory.domain import MemoryBundle

        async with AgentMemory.local(tmp_path / "pipeline.db") as seed:
            await legacy(seed.provider, seed.scope, count=192)
            expected = (await seed.provider.get_state(seed.scope))[:1]
            hydrated, loader = [], history.load_claim

            def load(payload):
                hydrated.append(payload)
                return loader(payload)

            monkeypatch.setattr(history, "load_claim", load)

            class Pipeline:
                async def retrieve(self, query, current):
                    assert current == expected
                    return MemoryBundle(
                        current, (), (), (), (), 1, {}, seed.provider.manifest().capabilities,
                    )

            memory = AgentMemory(seed.provider, seed.scope, limits=MemoryLimits(max_state_claims=1),
                                 recall_pipeline=Pipeline())
            await memory.initialize()
            assert (await memory.recall("ordinary")).current_state == expected
            assert len(hydrated) == 2  # one state payload, one final-authority recheck

    asyncio.run(run())


def test_kernel_retains_old_repository_fallback(tmp_path, monkeypatch):
    async def run():
        async with AgentMemory.local(tmp_path / "old-repository.db") as memory:
            await legacy(memory.provider, memory.scope, count=96)
            expected = (await memory.provider.get_state(memory.scope))[:1]
            monkeypatch.setattr(memory.provider._repository, "bounded_legacy_claims", None)
            assert await memory.provider.get_state_bounded(memory.scope, limit=1) == expected

    asyncio.run(run())
