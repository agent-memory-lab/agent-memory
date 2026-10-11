"""Answer-free queries must retain bounded source context before dense reranking."""

import asyncio
import json
import sqlite3
from collections import Counter
from contextlib import closing
from dataclasses import replace
from hashlib import sha256

import pytest

from agent_memory import ForgetMode, ForgetRequest, MemoryEvent, build_local_kernel
from agent_memory.domain import DecisionRecord, MemoryQuery, MemoryScope, MemoryUsage
from agent_memory.providers import ReciprocalRankFusionReranker
from agent_memory.retrieval import sqlite_lexical_index as index
from agent_memory.retrieval.candidate_identity import (
    MAX_SOURCE_WINDOW_CHARS,
    candidate_identity,
)
from agent_memory.retrieval.packing import pack_native_evidence

SCOPE = MemoryScope("source-context", user_id="one", session_id="session")
QUERY = "What degree did I graduate with?"
ANSWER = "Business Administration"
FRONT = (
    "Earlier, I wondered what degree I should graduate with and how to present my degree on a CV. "
)
TAIL = "Update: I completed university, with a qualification in Business Administration."


class RecordingTopicEmbeddings:
    """Local topic/intent stub; it never checks the answer literal or a gold label."""

    def __init__(self):
        self.inputs = []

    async def embed(self, texts):
        self.inputs.append(tuple(texts))
        vectors = []
        for text in texts:
            lowered = text.casefold()
            if "wondered" in lowered or "no choice" in lowered:
                vectors.append((0.0, 1.0))
            elif "degree" in lowered or "qualification" in lowered or "university" in lowered:
                vectors.append((1.0, 0.0))
            else:
                vectors.append((-1.0, 0.0))
        return tuple(vectors)


def run(coroutine):
    return asyncio.run(coroutine)


def query():
    assert ANSWER.casefold() not in QUERY.casefold()
    return MemoryQuery(SCOPE, QUERY, limit=8, token_budget=8192, include_current_state=False)


def assert_span_proof(item, source):
    span = item.metadata["source_span"]
    assert item.id == "long-source"
    assert item.text == source[span["start"] : span["end"]]
    assert item.metadata["source_event_ids"] == (item.id,)
    assert item.metadata["source_owner_id"] == item.id
    assert item.metadata["source_revision"] == sha256(source.encode()).hexdigest()
    assert candidate_identity(item) == item.metadata["candidate_identity"]


@pytest.mark.parametrize("placement", ("tail", "neighbor", "middle"))
def test_answer_free_query_delivers_late_source_before_embedding(tmp_path, placement):
    async def exercise():
        embeddings = RecordingTopicEmbeddings()
        kernel = build_local_kernel(tmp_path / "memory.db", embedding_provider=embeddings)
        await kernel.initialize()
        try:
            if placement == "tail":
                source = FRONT + "Routine maintenance notes. " * 120 + TAIL
            elif placement == "neighbor":
                prefix = FRONT + "Routine notes. " * 80
                prefix = prefix[:1005]
                source = prefix + TAIL + " Routine maintenance notes. " * 200
            else:
                source = (
                    FRONT * 6
                    + "Routine maintenance notes. " * 120
                    + "My degree is Business Administration."
                    + " Routine maintenance notes. " * 140
                )
            assert source.index(ANSWER) > 1024
            await kernel.ingest_event(MemoryEvent(SCOPE, "user.message", source, id="long-source"))
            await kernel.ingest_event(
                MemoryEvent(
                    SCOPE,
                    "user.message",
                    "A friend asked about a degree and how to graduate. No choice has been made.",
                    id="other-source",
                )
            )
            candidates = await kernel._repository.search(query(), 48)
            spans = [item for item in candidates if item.id == "long-source"]
            assert 2 <= len(spans) <= 4
            assert len({candidate_identity(item) for item in spans}) == len(spans)
            assert any(ANSWER in item.text for item in spans)
            if placement == "middle":
                middle = next(item for item in spans if ANSWER in item.text)
                assert middle.metadata["context_reason"] == "additional_match"
                assert middle.metadata["matched_source_span"]["start"] > 2048
                assert all(
                    ANSWER not in item.text
                    for item in spans
                    if item.metadata["context_reason"] in {"matched", "source_tail"}
                )
            for item in spans:
                assert_span_proof(item, source)
            bundle = await kernel.retrieve(query())
            assert embeddings.inputs
            assert any(ANSWER in text for batch in embeddings.inputs for text in batch)
            assert all(len(batch) <= 49 for batch in embeddings.inputs)
            assert all(
                len(text) <= MAX_SOURCE_WINDOW_CHARS
                for batch in embeddings.inputs for text in batch[1:]
            )
            assert any(len(item.text) > 1024 for item in spans)
            for item in spans:
                matched = item.metadata["matched_source_span"]
                window = item.metadata["source_span"]
                assert window["start"] <= matched["start"] < matched["end"] <= window["end"]
            delivered = [item for item in bundle.relevant_memories if item.id == "long-source"]
            assert any(ANSWER in item.text for item in delivered)
            assert len({candidate_identity(item) for item in delivered}) == len(delivered)
            assert bundle.token_estimate <= query().token_budget
            assert bundle.retrieval_metadata["coverage"] == "partial"
            assert bundle.retrieval_metadata["world_negative"] is False
            proofs = bundle.retrieval_metadata["citation_spans"]
            for item in delivered:
                assert_span_proof(item, source)
                proof = next(
                    value for value in proofs if value["candidate_id"] == candidate_identity(item)
                )
                assert proof["memory_id"] == item.id
                assert proof["source_span"] == item.metadata["source_span"]
                assert proof["source_revision"] == item.metadata["source_revision"]
                assert proof["source_event_ids"] == (item.id,)
            assert all(
                citation.memory_id in {item.id for item in delivered} | {"other-source"}
                for citation in bundle.citations
            )
        finally:
            await kernel.close()

    run(exercise())


def test_bounded_parent_preserves_earlier_entity_and_following_qualification(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            # The entity is two indexed boundaries before the matching action.
            # Multibyte filler also checks the character/UTF-8 span distinction.
            lead_in = "We enrolled through the Harbor Studio membership."
            action = "I redeemed a pottery voucher during our visit."
            qualifier = "The voucher belonged to that membership, not the guest account."
            source = "日" * 2000 + lead_in
            source += "月" * (3900 - len(source)) + action
            source += "星" * (4650 - len(source)) + qualifier + "云" * 1800
            await kernel.ingest_event(MemoryEvent(SCOPE, "user.message", source, id="long-source"))
            request = MemoryQuery(SCOPE, "Where did I redeem a pottery voucher?", limit=8,
                                  token_budget=8192, include_current_state=False)
            candidates = await kernel._repository.search(request, 48)
            primary = next(
                item for item in candidates if item.metadata["context_reason"] == "matched"
            )
            assert lead_in in primary.text and action in primary.text and qualifier in primary.text
            assert primary.metadata["matched_source_span"]["start"] == 3584
            assert primary.metadata["context_window"] == "bounded-parent/2"
            assert len(primary.text) <= MAX_SOURCE_WINDOW_CHARS
            assert_span_proof(primary, source)
            span = primary.metadata["source_span"]
            assert primary.text.encode() == source[span["start"]:span["end"]].encode()
        finally:
            await kernel.close()

    run(exercise())


def test_rrf_and_native_packing_keep_distinct_same_source_spans(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            source = FRONT + "Routine notes. " * 220 + TAIL
            await kernel.ingest_event(MemoryEvent(SCOPE, "user.message", source, id="long-source"))
            candidates = await kernel._repository.search(query(), 48)
            assert len(candidates) >= 2
            ranked = await ReciprocalRankFusionReranker().rerank(
                query(), (*candidates, candidates[0])
            )
            assert len(ranked) == len(candidates)
            assert {item.id for item in ranked} == {"long-source"}
            packed = pack_native_evidence(query(), (), ranked)
            # Distinct locators stay distinct through ranking, but a source
            # interval already fully delivered must not consume another slot.
            delivered = set()
            for item in packed.items:
                span = item.metadata["source_span"]
                delivered.update(range(span["start"], span["end"]))
            for item in candidates:
                span = item.metadata["source_span"]
                assert set(range(span["start"], span["end"])) <= delivered
            assert len(packed.metadata["citation_spans"]) == len(packed.items)
            assert any(ANSWER in item.text for item in packed.items)
            # Untrusted identity text cannot merge different validated locators.
            forged = tuple(
                replace(item, metadata={**item.metadata, "candidate_identity": "same"})
                for item in ranked
            )
            assert len(await ReciprocalRankFusionReranker().rerank(query(), forged)) == len(ranked)
            bundle = await kernel.retrieve(query())
            await kernel.record_decision(
                DecisionRecord(
                    SCOPE,
                    "answer",
                    ("long-source",),
                    bundle_id=bundle.bundle_id,
                    memory_usage=MemoryUsage.CONFIRMED,
                )
            )
            with closing(sqlite3.connect(tmp_path / "memory.db")) as db:
                payload = json.loads(
                    db.execute(
                        "SELECT payload_json FROM evolution_records WHERE id=?",
                        (bundle.bundle_id,),
                    ).fetchone()[0]
                )
            assert set(payload["returned_memory_ids"]) == {"long-source"}
            assert set(payload["returned_versions"]) == {"long-source"}
        finally:
            await kernel.close()

    run(exercise())


@pytest.mark.parametrize("mode", (ForgetMode.ARCHIVE, ForgetMode.ERASE))
def test_scope_and_forget_using_public_retrieved_id_cover_all_spans(tmp_path, mode):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            source = FRONT + "Routine notes. " * 220 + TAIL
            await kernel.ingest_event(MemoryEvent(SCOPE, "user.message", source, id="long-source"))
            hidden = MemoryScope(SCOPE.tenant_id, user_id="different", session_id=SCOPE.session_id)
            await kernel.ingest_event(
                MemoryEvent(hidden, "user.message", source, id="hidden-source")
            )
            bundle = await kernel.retrieve(query())
            delivered = bundle.relevant_memories
            assert delivered and {item.id for item in delivered} == {"long-source"}
            # Source-level forget accepts exactly the same public ID as before.
            await kernel.forget(ForgetRequest(SCOPE, (delivered[0].id,), mode=mode))
            assert not (await kernel.retrieve(query())).relevant_memories
            with closing(sqlite3.connect(tmp_path / "memory.db")) as db:
                assert (
                    db.execute(
                        "SELECT COUNT(*) FROM lexical_chunks WHERE owner_id='long-source'"
                    ).fetchone()[0]
                    == 0
                )
                assert (
                    db.execute(
                        "SELECT COUNT(*) FROM lexical_chunks WHERE owner_id='hidden-source'"
                    ).fetchone()[0]
                    > 0
                )
        finally:
            await kernel.close()

    run(exercise())


def test_expansion_bounds_and_short_source_breadth(tmp_path, monkeypatch):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            for number in range(40):
                await kernel.ingest_event(
                    MemoryEvent(
                        SCOPE,
                        "user.message",
                        f"degree graduate record {number}",
                        id=f"short-{number:03}",
                    )
                )
            found = await kernel._repository.search(query(), 48)
            assert len(found) == 40
            assert {item.id for item in found} == {f"short-{number:03}" for number in range(40)}
            for number in range(20):
                await kernel.ingest_event(
                    MemoryEvent(
                        SCOPE,
                        "user.message",
                        FRONT + "Routine notes. " * 300 + TAIL,
                        id=f"long-{number:03}",
                    )
                )
            observed = []
            original = index.hydrate_events

            def bounded(connection, locators):
                assert len(locators) <= 192
                observed.extend(locators)
                return original(connection, locators)

            monkeypatch.setattr(index, "hydrate_events", bounded)
            found = await kernel._repository.search(query(), 48)
            assert len(found) <= 48 and observed
            assert max(Counter(value["owner_id"] for value in observed).values()) <= 4
            assert all(len(item.text) <= MAX_SOURCE_WINDOW_CHARS for item in found)
        finally:
            await kernel.close()

    run(exercise())
