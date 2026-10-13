"""Native delivery must reauthorize after rerank and the final trace await."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from test_atom_admission import POLICY, SELF, at, atom, source
from test_atom_admission import store as store

from agent_memory.domain import (
    ForgetMode,
    ForgetRequest,
    MemoryBlock,
    MemoryEvent,
    MemoryKind,
    MemoryQuery,
    ScopeLevel,
)
from agent_memory.retrieval.delivery_guard import bundle_values
from agent_memory.retrieval.packing import token_estimate
from agent_memory.serialization import to_jsonable


async def seed(engine, kernel, scope, kind):
    if kind == "atom":
        event = source(scope)
        receipt = await engine.admit(event, [atom()], authority=SELF, policy=POLICY)
        return event.id, receipt.claim_ids[0]
    event = MemoryEvent(scope, "fact", "The archive meeting is at Harbor Hall.", metadata={
        "claims": [{"key": "meeting", "value": "Harbor Hall",
                    "text": "The archive meeting is at Harbor Hall."}]
        if kind == "claim" else [],
    }, occurred_at=at(1))
    receipt = await kernel.ingest_event(event)
    return event.id, receipt.claim_ids[0] if receipt.claim_ids else event.id


def pause_at(kernel, phase, mutation, monkeypatch):
    calls = []
    if phase == "rerank":
        reranker = kernel._reranker

        class PausedReranker:
            async def rerank(self, query, candidates):
                ranked = await reranker.rerank(query, candidates)
                calls.append(phase)
                await mutation()
                return ranked

        kernel._reranker = PausedReranker()
    else:
        factory = kernel._repository.unit_of_work

        @asynccontextmanager
        async def unit_of_work():
            async with factory() as uow:
                yield uow
            # Run after the trace transaction commits, while retrieve is still
            # awaiting __aexit__. This is its last pre-delivery await window.
            calls.append(phase)
            await mutation()

        monkeypatch.setattr(kernel._repository, "unit_of_work", unit_of_work)
    return calls


@pytest.mark.parametrize("phase", ["rerank", "trace"])
@pytest.mark.parametrize("mode", [ForgetMode.ARCHIVE, ForgetMode.ERASE])
@pytest.mark.parametrize("kind,historical", [
    ("event", False), ("claim", False), ("claim", True), ("atom", False), ("atom", True),
])
def test_deleted_evidence_is_removed_after_last_await(store, monkeypatch, phase, mode,
                                                     kind, historical):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event_id, claim_id = await seed(engine, kernel, scope, kind)
            clock[0] = at(2)
            query = MemoryQuery(
                scope, "Hangzhou" if kind == "atom" else "archive meeting",
                trace_enabled=phase == "trace",
                **({"valid_at": at(2), "known_at": at(2)} if historical else {}),
            )
            before = await kernel.retrieve(replace(query, trace_enabled=False))
            assert claim_id in {value.id for value in bundle_values(before)}

            async def mutate():
                await kernel.forget(ForgetRequest(scope, (event_id,), mode=mode))

            calls = pause_at(kernel, phase, mutate, monkeypatch)
            bundle = await kernel.retrieve(query)
            assert calls == [phase], "delivery must not rerank or retry"
            assert bundle_values(bundle) == ()
            assert bundle.citations == () and bundle.token_estimate == 0
            assert bundle.retrieval_metadata["atom_support"] == {}
            assert bundle.retrieval_metadata["citation_spans"] == ()
            assert bundle.retrieval_metadata["packing_selection_trace"] == ()
            encoded = json.dumps(to_jsonable(bundle))
            assert event_id not in encoded and claim_id not in encoded
            guard = bundle.retrieval_metadata["delivery_guard"]
            assert guard["removed_count"] > 0 and guard["status"] == "validated"
            if historical:
                assert guard["valid_at"] == at(2).isoformat()
                assert guard["known_at"] == at(2).isoformat()
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["event", "claim", "atom"])
def test_unchanged_evidence_and_atom_classification_survive(store, monkeypatch, kind):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, identity = await seed(engine, kernel, scope, kind)
            clock[0] = at(2)
            calls = []

            async def unchanged():
                calls.append("unchanged")

            pause_at(kernel, "rerank", unchanged, monkeypatch)
            result = await kernel.retrieve(MemoryQuery(
                scope, "Hangzhou" if kind == "atom" else "archive meeting",
            ))
            assert calls == ["unchanged"]
            assert identity in {value.id for value in bundle_values(result)}
            assert result.retrieval_metadata["delivery_guard"]["removed_count"] == 0
            assert result.token_estimate == sum(
                token_estimate(v.text) for v in bundle_values(result)
            )
            if kind == "atom":
                assert identity in result.retrieval_metadata["atom_support"]
                assert all(getattr(v, "kind", MemoryKind.CLAIM) == MemoryKind.CLAIM
                           for v in bundle_values(result))
    asyncio.run(run())


@pytest.mark.parametrize("change", ["content", "scope"])
def test_event_replacement_and_authorization_change_reject_stale_packed_text(
    store, monkeypatch, change,
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event_id, _ = await seed(engine, kernel, scope, "event")
            clock[0] = at(2)

            async def mutate():
                repo = kernel._repository
                column, value = (("content", "Replacement that excludes the old assertion.")
                                 if change == "content" else ("user_id", "different-user"))
                if hasattr(repo, "pool"):
                    async with repo.pool.connection() as connection:
                        await connection.execute(
                            f"UPDATE agent_memory_events SET {column}=%s WHERE id=%s",
                            (value, event_id),
                        )
                else:
                    with repo._connection() as connection:
                        connection.execute(f"UPDATE events SET {column}=? WHERE id=?",
                                           (value, event_id))

            pause_at(kernel, "rerank", mutate, monkeypatch)
            result = await kernel.retrieve(MemoryQuery(scope, "archive meeting"))
            assert bundle_values(result) == ()
            assert result.retrieval_metadata["delivery_guard"]["removed_count"] > 0
    asyncio.run(run())


def test_current_claim_expiring_during_rerank_is_not_delivered(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event = source(scope)
            await engine.admit(event, [replace(atom(), valid_to=at(3))],
                               authority=SELF, policy=POLICY)
            clock[0] = at(2)

            async def mutate():
                clock[0] = at(4)

            pause_at(kernel, "rerank", mutate, monkeypatch)
            result = await kernel.retrieve(MemoryQuery(scope, "Hangzhou"))
            assert bundle_values(result) == ()
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["claim", "atom"])
def test_historical_claim_survives_later_observation_and_keeps_requested_coordinates(
    store, monkeypatch, kind,
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            _, identity = await seed(engine, kernel, scope, kind)
            clock[0] = at(2)

            async def mutate():
                clock[0] = at(3)
                if kind == "atom":
                    await engine.admit(source(scope, "Alice lives in Beijing", day=3),
                                       [replace(atom("Beijing"), valid_from=at(3))],
                                       authority=SELF, policy=POLICY)
                else:
                    await kernel.ingest_event(MemoryEvent(scope, "fact", "Meeting moved.",
                        occurred_at=at(3), metadata={"claims": [{"key": "meeting",
                            "value": "Sunrise Hall", "text": "The meeting is at Sunrise Hall.",
                            "valid_from": at(3).isoformat()}]}))

            pause_at(kernel, "rerank", mutate, monkeypatch)
            bundle = await kernel.retrieve(MemoryQuery(
                scope, "Hangzhou" if kind == "atom" else "archive meeting",
                valid_at=at(2), known_at=at(2),
            ))
            assert identity in {value.id for value in bundle_values(bundle)}
            assert bundle.retrieval_metadata["delivery_guard"]["removed_count"] == 0
            assert bundle.retrieval_metadata["valid_at"] == at(2).isoformat()
            assert bundle.retrieval_metadata["known_at"] == at(2).isoformat()
    asyncio.run(run())


@pytest.mark.parametrize("change", ["version", "source_erase"])
def test_artifact_revision_and_provenance_are_rechecked_after_rerank(store, monkeypatch, change):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            event_id, _ = await seed(engine, kernel, scope, "event")
            kernel._manifest = replace(kernel._manifest, capabilities=replace(
                kernel._manifest.capabilities, memory_blocks=True,
            ))
            block = await kernel.write_block(MemoryBlock(
                scope, "archive guide", "The archive meeting is at Harbor Hall.", (event_id,),
            ))
            clock[0] = at(2)
            original = kernel._reranker

            class BlockFirst:
                async def rerank(self, query, candidates):
                    candidates = await original.rerank(query, candidates)
                    return tuple(sorted(candidates, key=lambda item: item.id != block.id))

            kernel._reranker = BlockFirst()
            query = MemoryQuery(scope, "archive meeting", include_current_state=False,
                                trace_enabled=False)
            before = await kernel.retrieve(query)
            assert block.id in {item.id for item in before.relevant_memories}

            async def mutate():
                if change == "version":
                    await kernel.write_block(replace(block, title="updated archive guide"),
                                             expected_version=block.version)
                else:
                    await kernel.forget(ForgetRequest(scope, (event_id,), mode=ForgetMode.ERASE))

            pause_at(kernel, "rerank", mutate, monkeypatch)
            bundle = await kernel.retrieve(query)
            assert block.id not in {item.id for item in bundle.relevant_memories}
            assert bundle.retrieval_metadata["delivery_guard"]["removed_count"] > 0
    asyncio.run(run())


def test_persisted_trace_declares_pre_delivery_stage(store):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            await seed(engine, kernel, scope, "event")
            clock[0] = at(2)
            result = await kernel.retrieve(MemoryQuery(
                scope, "archive meeting", trace_enabled=True,
            ))
            traces = await kernel._repository.list_feedback(scope, "retrieval", 10)
            assert len(traces) == 1
            assert traces[0]["payload"]["delivery_stage"] == "pre_delivery"
            assert result.retrieval_metadata["delivery_guard"]["trace_stage"] == "pre_delivery"
    asyncio.run(run())


@pytest.mark.parametrize("complete", [True, False])
def test_complete_event_without_locator_is_validated_by_exact_body(store, complete):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            identity, _ = await seed(engine, kernel, scope, "event")
            clock[0] = at(2)
            query = MemoryQuery(scope, "archive meeting", include_current_state=False,
                                trace_enabled=False)
            bundle = await kernel.retrieve(query)
            original = next(item for item in bundle.relevant_memories if item.id == identity)
            item = replace(original, text=original.text if complete else "archive meeting",
                           metadata={"event_type": "fact", "source_event_ids": (identity,)})
            bundle = replace(bundle, relevant_memories=(item,))
            result = await kernel._repository.guard_retrieval_delivery(query, bundle)
            assert bool(result.relevant_memories) is complete
    asyncio.run(run())


def test_unselected_deleted_candidate_does_not_survive_in_diagnostics(store, monkeypatch):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            first, _ = await seed(engine, kernel, scope, "event")
            other = MemoryEvent(scope, "fact", "The archive meeting is at Sunset Hall.")
            await kernel.ingest_event(other)
            clock[0] = at(2)

            class FirstOnly:
                async def rerank(self, query, candidates):
                    return tuple(sorted((replace(item, score=1.0 if item.id == first else 0.0)
                                        for item in candidates), key=lambda item: -item.score))

            kernel._reranker = FirstOnly()

            async def mutate():
                await kernel.forget(ForgetRequest(scope, (other.id,), mode=ForgetMode.ERASE))

            pause_at(kernel, "trace", mutate, monkeypatch)
            bundle = await kernel.retrieve(MemoryQuery(
                scope, "archive meeting", trace_enabled=True, limit=1,
                include_current_state=False,
            ))
            assert tuple(item.id for item in bundle.relevant_memories) == (first,)
            assert bundle.retrieval_metadata["delivery_guard"]["removed_count"] == 0
            assert other.id not in json.dumps(to_jsonable(bundle))
            stages = bundle.retrieval_metadata["retrieval_stages"]
            assert stages["candidate_recall"]["count"] >= 2
            assert first in {row["id"] for row in stages["candidate_recall"]["items"]}
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["claim", "atom"])
@pytest.mark.parametrize("mode", [None, ForgetMode.ARCHIVE, ForgetMode.ERASE])
def test_user_scope_promoted_claim_is_visible_across_sessions_but_source_erasure_wins(
    store, monkeypatch, kind, mode,
):
    async def run():
        async with store() as (engine, kernel, scope, clock):
            if kind == "atom":
                event = source(scope)
                receipt = await engine.admit(
                    event, [replace(atom(), scope_level=ScopeLevel.USER)],
                    authority=SELF, policy=POLICY,
                )
            else:
                event = MemoryEvent(scope, "fact", "The archive meeting is at Harbor Hall.",
                    occurred_at=at(1), metadata={"claims": [{"key": "meeting",
                        "value": "Harbor Hall", "text": "The archive meeting is at Harbor Hall.",
                        "scope_level": "user"}]})
                receipt = await kernel.ingest_event(event)
            identity = receipt.claim_ids[0]
            clock[0] = at(2)
            target = replace(scope, session_id="another-session")
            query = MemoryQuery(target, "Hangzhou" if kind == "atom" else "archive meeting",
                                trace_enabled=False)
            before = await kernel.retrieve(query)
            assert identity in {value.id for value in bundle_values(before)}
            assert all(getattr(value, "kind", MemoryKind.CLAIM) == MemoryKind.CLAIM
                       for value in bundle_values(before))
            assert event.id not in {value.id for value in bundle_values(before)}
            if mode is None:
                assert before.retrieval_metadata["delivery_guard"]["removed_count"] == 0
                return

            async def mutate():
                await kernel.forget(ForgetRequest(scope, (event.id,), mode=mode))

            pause_at(kernel, "rerank", mutate, monkeypatch)
            result = await kernel.retrieve(query)
            assert bundle_values(result) == ()
            assert result.citations == ()
            assert result.retrieval_metadata["delivery_guard"]["removed_count"] > 0
    asyncio.run(run())
