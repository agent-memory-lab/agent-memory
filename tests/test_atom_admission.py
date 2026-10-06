"""Typed admission behavior shared by SQLite and a real PostgreSQL database."""

import asyncio
import os
import threading
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from urllib.parse import parse_qsl, urlencode, urlsplit
from uuid import uuid4

import pytest

from agent_memory.consolidation.admission import AdmissionPolicy
from agent_memory.consolidation.admission_runtime import AdmissionEngine
from agent_memory.domain import (
    AtomDraft,
    ForgetMode,
    ForgetRequest,
    MemoryBundle,
    MemoryEvent,
    MemoryItem,
    MemoryKind,
    MemoryProposal,
    MemoryQuery,
    MemoryScope,
    PredicateSpec,
    ProposalStatus,
    ScopeLevel,
    SourceAuthority,
)
from agent_memory.kernel import MemoryKernel
from agent_memory.providers import (
    MetadataClaimExtractor,
    ReciprocalRankFusionReranker,
    TrustedMemoryPolicy,
)
from agent_memory.runtime import AgentMemory
from agent_memory.sqlite import SQLiteMemoryRepository


def at(day):
    return datetime(2026, 10, day, tzinfo=UTC)


POLICY = AdmissionPolicy([
    PredicateSpec("city"), PredicateSpec("locale"),
    PredicateSpec("paid", value_type="boolean", allow_self_report=False),
])
SELF = SourceAuthority("user:alice", subjects=("alice",), predicates=("city", "locale", "paid"))
TOOL = replace(SELF, source_id="billing-api", kind="tool_observation")


def source(scope, text="Alice lives in Hangzhou", *, day=1, identity=None, idempotency=None):
    return MemoryEvent(
        scope, "message", text, id=identity or uuid4().hex,
        occurred_at=at(day), idempotency_key=idempotency,
    )


def atom(value="Hangzhou", **changes):
    draft = AtomDraft(
        "alice", "city", value, f"Alice lives in {value}", f"Alice lives in {value}",
        valid_from=at(1),
    )
    return replace(draft, **changes)


@pytest.fixture(params=["sqlite", "postgres"])
def store(request, tmp_path, monkeypatch):
    import agent_memory.consolidation.admission_runtime as runtime
    import agent_memory.domain as domain
    import agent_memory.kernel as kernel_module
    import agent_memory.retrieval.temporal_history as history
    import agent_memory.sqlite as local

    # Import optional adapters before patching their imported domain clock;
    # otherwise first import captures the fixture's clock beyond teardown.
    if request.param == "postgres":
        for name in ("repository", "admission", "temporal_history"):
            pytest.importorskip(f"agent_memory_postgres.{name}")

    clock = [at(1)]
    for module in (runtime, domain, kernel_module, history, local):
        monkeypatch.setattr(module, "utc_now", lambda: clock[0])
    scope = MemoryScope("atom-tests", user_id="alice", session_id="session")

    @asynccontextmanager
    async def open_store():
        schema = None
        dsn = None
        if request.param == "postgres":
            pg = pytest.importorskip("agent_memory_postgres.repository")
            admission = pytest.importorskip("agent_memory_postgres.admission")
            pg_history = pytest.importorskip("agent_memory_postgres.temporal_history")
            psycopg = pytest.importorskip("psycopg")
            for module in (admission, pg_history, pg):
                monkeypatch.setattr(module, "utc_now", lambda: clock[0])
            dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN")
            if not dsn:
                pytest.skip("set AGENT_MEMORY_TEST_POSTGRES_DSN for live PostgreSQL tests")
            parsed = urlsplit(dsn)
            if parsed.scheme not in {"postgres", "postgresql"} or "test" not in parsed.path:
                pytest.fail("admission tests require a PostgreSQL test database")
            schema = f"atom_behavior_{uuid4().hex}"
            connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
            async with connection:
                await connection.execute(psycopg.sql.SQL("CREATE SCHEMA {}").format(
                    psycopg.sql.Identifier(schema)
                ))
            query = dict(parse_qsl(parsed.query))
            query["options"] = f"-csearch_path={schema}"
            repo = pg.PostgresMemoryRepository.from_dsn(
                parsed._replace(query=urlencode(query)).geturl(), max_size=4
            )
        else:
            repo = SQLiteMemoryRepository(tmp_path / "admission.db")
        kernel = MemoryKernel(
            repo, MetadataClaimExtractor(), TrustedMemoryPolicy(), ReciprocalRankFusionReranker()
        )
        try:
            await kernel.initialize()
            yield AdmissionEngine(repo), kernel, scope, clock
        finally:
            await kernel.close()
            if schema:
                connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
                async with connection:
                    await connection.execute(psycopg.sql.SQL("DROP SCHEMA {} CASCADE").format(
                        psycopg.sql.Identifier(schema)
                    ))

    return open_store


async def state(engine, scope, *, valid=2, known=30):
    return await engine.state(scope, valid_at=at(valid), known_at=at(known))


def test_accept_and_pending_are_separate_on_default_recall(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            receipt = await engine.admit(source(scope), [atom()], authority=SELF, policy=POLICY)
            assert receipt.decisions[0].action == "ACCEPT"
            assert len(receipt.claim_ids) == 1 and receipt.pending_ids == ()
            pending = atom(
                True, predicate="paid", text="Invoice secret-marker is paid",
                source_quote="Invoice secret-marker is paid",
            )
            queued = await engine.admit(
                source(scope, pending.text), [pending], authority=SELF, policy=POLICY
            )
            assert queued.pending_ids == queued.candidate_ids and queued.claim_ids == ()
            claims, _ = await state(engine, scope)
            assert [c.value for c in claims] == ["Hangzhou"]
            clock[0] = at(2)
            bundle = await kernel.retrieve(MemoryQuery(scope, "secret-marker", token_budget=2048))
            assert all("secret-marker" not in item.text for item in bundle.relevant_memories)
            assert all(c.value is not True for c in bundle.current_state)

    asyncio.run(run())


def test_same_batch_conflicts_preserve_both_candidates_and_can_be_resolved(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            event = source(scope, "Alice lives in Hangzhou. Alice lives in Shanghai")
            receipt = await engine.admit(
                event, [atom(), atom("Shanghai")], authority=SELF, policy=POLICY
            )
            assert len(set(receipt.candidate_ids)) == 2
            assert {d.action for d in receipt.decisions} == {"CONTESTED"}
            assert receipt.claim_ids == ()
            claims, info = await state(engine, scope)
            assert claims == () and len(info["conflicts"]) == 1
            clock[0] = at(5)
            chosen = receipt.candidate_ids[1]
            resolved = await engine.resolve(
                scope, chosen, event=source(scope, "Alice lives in Shanghai", day=5),
                authority=TOOL, policy=POLICY, expected_version=1,
                accept=True, source_quote="Alice lives in Shanghai", support_from=at(1),
            )
            assert resolved.decisions[0].action == "ACCEPT"
            claims, info = await state(engine, scope, known=6)
            assert [c.value for c in claims] == ["Shanghai"] and info["conflicts"] == []
            historical, info = await state(engine, scope, known=3)
            assert historical == () and info["conflicts"]
            rejected = await engine.repository.admission_record(scope, receipt.candidate_ids[0])
            assert rejected["payload"]["action"] == "REJECT"

    asyncio.run(run())


def test_resolution_cas_and_initial_idempotent_receipt_are_stable(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            draft = atom(
                True, predicate="paid", text="Invoice is paid", source_quote="Invoice is paid"
            )
            event = source(scope, draft.text, idempotency="payment")
            initial = await engine.admit(event, [draft], authority=SELF, policy=POLICY)
            identity = initial.candidate_ids[0]
            clock[0] = at(10)
            verification = source(scope, draft.text, day=10)
            result = await engine.resolve(
                scope, identity, event=verification, authority=TOOL, policy=POLICY,
                expected_version=1, accept=True, source_quote=draft.text,
            )
            assert result.claim_ids and result.pending_ids == ()
            with pytest.raises(ValueError, match="version"):
                await engine.resolve(
                    scope, identity, event=source(scope, draft.text, day=10),
                    authority=TOOL, policy=POLICY, expected_version=1,
                    accept=False, source_quote=draft.text,
                )
            duplicate = await engine.admit(
                replace(event, id="retry-id"), [draft], authority=SELF, policy=POLICY
            )
            assert duplicate.duplicate is True
            assert replace(duplicate, duplicate=False) == initial
            status = await engine.repository.admission_record(scope, identity)
            assert status["version"] == 2 and status["payload"]["action"] == "ACCEPT"
            assert (await state(engine, scope, valid=11, known=9))[0] == ()
            assert [c.value for c in (await state(engine, scope, valid=11, known=11))[0]] == [True]
            assert (await state(engine, scope, valid=5, known=11))[0] == ()
            assert len(await engine.repository.admission_record_versions(scope, identity)) == 2

    asyncio.run(run())


def test_idempotency_rejects_changed_observation_time(store):
    async def run():
        async with store() as (engine, _, scope, _):
            event = source(scope, idempotency="time-sensitive")
            draft = atom(valid_from=None)
            await engine.admit(event, [draft], authority=SELF, policy=POLICY)
            with pytest.raises(ValueError, match="idempotency"):
                await engine.admit(
                    replace(event, occurred_at=at(2)), [draft], authority=SELF, policy=POLICY
                )

    asyncio.run(run())


def test_new_conflict_suppresses_prior_claim_but_preserves_prior_knowledge(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await engine.admit(source(scope), [atom()], authority=SELF, policy=POLICY)
            clock[0] = at(5)
            new = await engine.admit(
                source(scope, "Alice lives in Shanghai", day=5), [atom("Shanghai")],
                authority=SELF, policy=POLICY,
            )
            assert new.decisions[0].action == "CONTESTED"
            assert (await state(engine, scope, known=6))[0] == ()
            assert [c.value for c in (await state(engine, scope, known=3))[0]] == ["Hangzhou"]
            bundle = await kernel.retrieve(MemoryQuery(scope, "city", token_budget=2048))
            assert bundle.current_state == ()

    asyncio.run(run())


@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_delete_new_value_does_not_resurrect_old_state_or_allow_replay(store, mode):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            first = source(scope, idempotency="first")
            initial = await engine.admit(first, [atom()], authority=SELF, policy=POLICY)
            clock[0] = at(5)
            second = source(scope, "Alice lives in Shanghai", day=5, idempotency="second")
            update = atom("Shanghai", valid_from=at(5))
            latest = await engine.admit(second, [update], authority=SELF, policy=POLICY)
            assert [c.value for c in (await state(engine, scope, valid=6))[0]] == ["Shanghai"]
            await kernel.forget(ForgetRequest(scope, (second.id,), mode=mode))
            assert (await state(engine, scope, valid=6))[0] == ()
            assert (await state(engine, scope, valid=2, known=3))[0] == ()
            for identity in (*initial.candidate_ids, *latest.candidate_ids):
                assert await engine.repository.admission_record_versions(scope, identity) == ()
            with pytest.raises(ValueError):
                await engine.admit(second, [update], authority=SELF, policy=POLICY)
            with pytest.raises(ValueError):
                await engine.admit(first, [atom()], authority=SELF, policy=POLICY)

    asyncio.run(run())


def test_unknown_or_cross_scope_resolution_does_not_publish(store):
    async def run():
        async with store() as (engine, _, scope, _):
            draft = atom(source_quote="missing quotation")
            result = await engine.admit(source(scope), [draft], authority=SELF, policy=POLICY)
            other = replace(scope, user_id="bob")
            with pytest.raises(ValueError, match="scope"):
                await engine.resolve(
                    other, result.candidate_ids[0], event=source(other),
                    authority=TOOL, policy=POLICY, expected_version=1,
                    accept=True, source_quote="Alice lives in Hangzhou",
                )
            assert (await state(engine, scope))[0] == ()
            assert len(await engine.repository.admission_record_versions(
                scope, result.candidate_ids[0]
            )) == 1

    asyncio.run(run())


def test_facade_retries_implicit_observation_and_exposes_current_status(store):
    async def run():
        async with store() as (_, kernel, scope, clock):
            memory = AgentMemory(kernel, scope)
            await memory.initialize()
            draft = atom()
            first = await memory.remember_atoms(
                draft.text, [draft], authority=SELF, policy=POLICY, idempotency_key="facade"
            )
            clock[0] = at(3)
            retried = await memory.remember_atoms(
                draft.text, [draft], authority=SELF, policy=POLICY, idempotency_key="facade"
            )
            assert replace(retried, duplicate=False) == first
            status = await memory.atom_status(first.candidate_ids[0])
            assert status["payload"]["action"] == "ACCEPT"
            assert len(await memory.atom_history(first.candidate_ids[0])) == 1
            assert [c.value for c in (await memory.recall("city")).current_state] == ["Hangzhou"]

    asyncio.run(run())


def test_custom_pipeline_cannot_restore_a_withdrawn_atom_source(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            original = source(scope)
            await engine.admit(original, [atom()], authority=SELF, policy=POLICY)
            clock[0] = at(5)
            later = source(scope, "Alice lives in Shanghai", day=5)
            await engine.admit(
                later, [atom("Shanghai", valid_from=at(5))], authority=SELF, policy=POLICY
            )
            await kernel.forget(ForgetRequest(scope, (later.id,), mode=ForgetMode.ERASE))

            class CachedPipeline:
                async def retrieve(self, query, current_state):
                    item = MemoryItem(
                        original.id, MemoryKind.EVENT, original.content, 1.0, original.occurred_at,
                        metadata={"source_event_ids": (original.id,)},
                    )
                    return MemoryBundle(
                        current_state, (item,), (), (), (), 10, {}, kernel.manifest().capabilities
                    )

            memory = AgentMemory(kernel, scope, recall_pipeline=CachedPipeline())
            await memory.initialize()
            bundle = await memory.recall("Hangzhou")
            assert bundle.current_state == ()
            assert bundle.relevant_memories == ()

    asyncio.run(run())


def test_protected_source_ids_include_erased_and_inherited_sources_only(store):
    async def run():
        async with store() as (engine, kernel, scope, _):
            original = source(scope)
            await engine.admit(original, [atom()], authority=SELF, policy=POLICY)
            ancestor = replace(
                source(scope, "ancestor"), scope=scope.project(ScopeLevel.USER),
                event_type="memory.atom.verification",
            )
            foreign = replace(
                source(scope, "foreign"), scope=replace(scope, user_id="bob"),
                event_type="memory.atom",
            )
            async with engine.repository.unit_of_work() as uow:
                await uow.append_event(ancestor)
                await uow.append_event(foreign)
            await kernel.forget(ForgetRequest(scope, (original.id,), mode=ForgetMode.ERASE))
            ids = await engine.repository.admission_protected_sources(scope)
            assert set(ids) == {original.id, ancestor.id}
            assert ids == tuple(sorted(ids))
            assert await engine.repository.admission_protected_sources(
                replace(scope, tenant_id="other-tenant")
            ) == ()

    asyncio.run(run())


def test_legacy_proposal_cannot_promote_pending_typed_evidence(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            pending = source(scope, "Invoice secret-marker is paid")
            draft = atom(
                True, predicate="paid", text=pending.content, source_quote=pending.content
            )
            receipt = await engine.admit(pending, [draft], authority=SELF, policy=POLICY)
            assert receipt.pending_ids
            clock[0] = at(2)
            proposed = await kernel.propose(MemoryProposal(
                scope, "legacy-paid", True, pending.content, (pending.id,),
                expected_version=0, valid_from=at(1),
            ))
            assert proposed.status == ProposalStatus.ACCEPTED
            stored_claims = await engine.repository.current_claims(scope)
            legacy = next(item for item in stored_claims if item.id == proposed.claim_id)
            audit_id = next(identity for identity in legacy.provenance.source_event_ids
                            if identity != pending.id)
            derived = replace(
                source(scope, pending.content, day=2), event_type="derived.result",
                metadata={"source_event_ids": [audit_id]},
            )
            async with engine.repository.unit_of_work() as uow:
                await uow.append_event(derived)
            protected = await engine.repository.admission_protected_sources(scope)
            assert {pending.id, audit_id, derived.id}.issubset(protected)
            assert await kernel.get_state(scope) == ()
            assert await kernel.get_state_at(scope, valid_at=at(2), known_at=at(3)) == ()
            for query in (
                MemoryQuery(scope, "secret-marker", token_budget=2048),
                MemoryQuery(scope, "secret-marker", token_budget=2048,
                            valid_at=at(2), known_at=at(3)),
            ):
                bundle = await kernel.retrieve(query)
                assert bundle.current_state == ()
                assert all("secret-marker" not in item.text for item in bundle.relevant_memories)

    asyncio.run(run())


def test_storage_uses_one_namespace_boundary_per_uow_and_resets_on_reentry(store):
    async def run():
        async with store() as (engine, _, scope, clock):
            repository = engine.repository
            parent_scope = scope.project(ScopeLevel.USER)
            event = source(scope)
            uow = repository.unit_of_work()
            async with uow:
                await uow.append_event(event)
                await uow.save_admission_record(scope, "one", event.id, "slot-one", {}, 0)
                clock[0] = at(2)
                await uow.save_admission_record(parent_scope, "two", event.id, "slot-two", {}, 0)
                clock[0] = at(3)
                await uow.save_admission_record(scope, "one", event.id, "slot-one", {}, 1)
            first = await repository.admission_record(scope, "one")
            sibling = await repository.admission_record(scope, "two")
            history = await repository.admission_record_versions(scope, "one")
            assert first["recorded_at"] == sibling["recorded_at"]
            assert {item["recorded_at"] for item in history} == {first["recorded_at"]}
            assert [item["version"] for item in history] == [1, 2]
            snapshot = await repository.admission_snapshot(scope)
            by_id = {item["id"]: item for item in snapshot}
            assert [item["version"] for item in by_id["one"]["versions"]] == [1, 2]
            assert [item["version"] for item in by_id["two"]["versions"]] == [1]
            clock[0] = at(1)
            async with uow:
                await uow.save_admission_record(scope, "three", event.id, "slot-three", {}, 0)
            subsequent = await repository.admission_record(scope, "three")
            assert subsequent["recorded_at"] > first["recorded_at"]

    asyncio.run(run())


def test_concurrent_legacy_proposal_and_ingest_share_lock_order(store):
    async def run():
        async with store() as (_, kernel, scope, clock):
            baseline = replace(source(scope, "original state"), metadata={"claims": [{
                "key": "legacy-state", "value": "original", "text": "original state",
            }]})
            await kernel.ingest_event(baseline)
            incoming = replace(source(scope, "incoming state", day=3), metadata={"claims": [{
                "key": "legacy-state", "value": "incoming", "text": "incoming state",
            }]})
            proposal = MemoryProposal(
                scope, "legacy-state", "proposed", "proposed state", (baseline.id,),
                expected_version=1, valid_from=at(2),
            )
            proposed, ingested = await asyncio.wait_for(asyncio.gather(
                kernel.propose(proposal), kernel.ingest_event(incoming)
            ), timeout=10)
            assert proposed.status in {ProposalStatus.ACCEPTED, ProposalStatus.CONFLICT}
            assert ingested.claim_ids
            clock[0] = at(4)
            assert [item.value for item in await kernel.get_state(scope)] == ["incoming"]

    asyncio.run(run())


def test_snapshot_keeps_current_and_history_consistent_during_concurrent_commit(store, monkeypatch):
    async def run():
        async with store() as (engine, _, scope, _):
            repository = engine.repository
            event = source(scope)
            async with repository.unit_of_work() as uow:
                await uow.append_event(event)
                await uow.save_admission_record(
                    scope, "snapshot", event.id, "slot", {"value": "before"}, 0
                )
            if isinstance(repository, SQLiteMemoryRepository):
                captured, release = threading.Event(), threading.Event()
                original = repository._read_admission_records

                def pause(connection, read_scope, **kwargs):
                    rows = original(connection, read_scope, **kwargs)
                    captured.set()
                    if not release.wait(5):
                        raise TimeoutError("concurrent writer did not finish")
                    return rows

                monkeypatch.setattr(repository, "_read_admission_records", pause)

                async def wait_captured():
                    assert await asyncio.to_thread(captured.wait, 5)

            else:
                import agent_memory_postgres.admission as pg_admission

                captured, release = asyncio.Event(), asyncio.Event()
                original = pg_admission.read_records

                async def pause(connection, read_scope, **kwargs):
                    rows = await original(connection, read_scope, **kwargs)
                    captured.set()
                    await asyncio.wait_for(release.wait(), 5)
                    return rows

                monkeypatch.setattr(pg_admission, "read_records", pause)

                async def wait_captured():
                    await asyncio.wait_for(captured.wait(), 5)

            reading = asyncio.create_task(repository.admission_snapshot(scope))
            try:
                await wait_captured()
                async with repository.unit_of_work() as uow:
                    await uow.save_admission_record(
                        scope, "snapshot", event.id, "slot", {"value": "after"}, 1
                    )
            finally:
                release.set()
            snapshot = await reading
            assert snapshot[0]["version"] == 1
            assert snapshot[0]["payload"] == {"value": "before"}
            assert [item["version"] for item in snapshot[0]["versions"]] == [1]
            assert snapshot[0]["versions"][0]["payload"] == {"value": "before"}

    asyncio.run(run())
