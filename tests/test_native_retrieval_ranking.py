"""Answer-independent contracts for native candidate recall and rank fusion."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from math import sqrt

import pytest

from agent_memory import (
    EmbeddingReranker,
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryItem,
    MemoryKind,
    MemoryQuery,
    MemoryScope,
    build_local_kernel,
)
from agent_memory.providers import ReciprocalRankFusionReranker
from agent_memory.retrieval.candidate_selection import balanced_candidates, stage_summary

SCOPE = MemoryScope("native-ranking", user_id="reader", session_id="session")
NOW = datetime(2026, 9, 15, tzinfo=UTC)


def candidate(identity, text, *, score=1.0, kind=MemoryKind.EVENT, **metadata):
    return MemoryItem(identity, kind, text, score, NOW, metadata)


def test_rare_query_terms_beat_long_common_word_noise_in_real_sqlite(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            for number in range(24):
                await kernel.ingest_event(MemoryEvent(
                    SCOPE, "user.message", "Where did I leave the documents? " * 60,
                    id=f"noise-{number}",
                ))
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "The zirconium certificate is in a sealed archive box.",
                id="specific-record",
            ))
            results = await kernel._repository.search(
                MemoryQuery(SCOPE, "Where did I leave the zirconium certificate?"), 8,
            )
            assert results[0].id == "specific-record"
            assert results[0].metadata["lexical_ranker"] == "scoped-bm25/1"
        finally:
            await kernel.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("mode", (ForgetMode.ARCHIVE, ForgetMode.ERASE))
def test_inaccessible_and_deleted_sources_do_not_influence_authorized_idf(tmp_path, mode):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        query = MemoryQuery(SCOPE, "zirconium certificate")

        async def scores():
            return {
                value.id: value.metadata["lexical_score"]
                for value in await kernel._repository.search(query, 12)
            }

        try:
            for number, text in enumerate((
                "zirconium certificate stored in a box",
                "certificate application awaiting review",
                "routine archive organization",
            )):
                await kernel.ingest_event(MemoryEvent(
                    SCOPE, "user.message", text, id=f"visible-{number}",
                ))
            baseline = await scores()
            for number in range(12):
                scope = replace(SCOPE, user_id="other") if number % 2 else replace(
                    SCOPE, tenant_id="another-tenant",
                )
                await kernel.ingest_event(MemoryEvent(
                    scope, "user.message", "zirconium certificate " * 100,
                    id=f"private-{number}",
                ))
            assert await scores() == baseline
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "zirconium certificate " * 100, id="deleted-source",
            ))
            await kernel.forget(ForgetRequest(SCOPE, ("deleted-source",), mode=mode))
            assert await scores() == baseline
        finally:
            await kernel.close()

    asyncio.run(exercise())


def test_candidate_budget_preserves_one_short_fact_among_many_event_matches():
    fact = candidate(
        "short-fact", "The reference number is 47, not 74.", kind=MemoryKind.CLAIM,
        score=0.01, lexical_score=0.01,
    )
    events = tuple(candidate(
        f"event-{number}", "reference number reference number", score=100 - number,
        lexical_score=100 - number,
    ) for number in range(80))
    selected = balanced_candidates((*events, fact), 6)
    assert len(selected) == 6
    assert any(item.id == fact.id and item.text == fact.text for item in selected)
    assert len({item.id for item in selected}) == 6
    assert sum(item.kind == MemoryKind.EVENT for item in selected) == 5


def test_real_repository_reserves_a_claim_before_the_reranker_budget(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "An authoritative record was imported.",
                metadata={"claims": [{"key": "record", "value": "47",
                                     "text": "The reference number is 47, not 74."}]},
            ))
            for number in range(64):
                await kernel.ingest_event(MemoryEvent(
                    SCOPE, "user.message", "What was the reference number and where was it stored?",
                    id=f"event-{number}",
                ))
            items = await kernel._repository.search(
                MemoryQuery(SCOPE, "What was the reference number and where was it stored?"), 6,
            )
            assert len(items) == 6
            facts = [item for item in items if item.kind == MemoryKind.CLAIM]
            assert len(facts) == 1
            assert facts[0].text == "The reference number is 47, not 74."
        finally:
            await kernel.close()

    asyncio.run(exercise())


class NarrowSimilarityEmbeddings:
    async def embed(self, texts):
        result = [(1.0, 0.0)]
        for text in texts[1:]:
            cosine = 0.802 if "automobile" in text else 0.800
            result.append((cosine, sqrt(1 - cosine * cosine)))
        return tuple(result)


def test_rank_fusion_can_use_narrow_semantic_gaps_without_linear_rank_domination():
    async def exercise():
        items = (
            candidate("literal", "vehicle transport schedules", score=1.0),
            candidate("meaning", "an automobile carries passengers", score=0.1),
        )
        ranked = await EmbeddingReranker(NarrowSimilarityEmbeddings()).rerank(
            MemoryQuery(SCOPE, "vehicle transport"), items,
        )
        assert ranked[0].id == "meaning"
        assert ranked[0].metadata["semantic_rank"] == 1
        assert ranked[0].metadata["lexical_fusion_rank"] == 2
        assert ranked[0].metadata["reranking_method"] == "weighted-rank-fusion/1"
        assert ranked[0].metadata["semantic_score"] - ranked[1].metadata["semantic_score"] < 0.01

    asyncio.run(exercise())


def test_branch_fusion_retains_equal_rank_and_tail_stays_below_reranked_evidence():
    async def exercise():
        items = (
            candidate("event", "vehicle transport schedules", retrieval_branch="native.event"),
            candidate("fact", "an automobile carries passengers", kind=MemoryKind.CLAIM,
                      retrieval_branch="native.claim"),
            candidate("tail", "old vehicle transport notes", score=0.1,
                      retrieval_branch="native.event"),
        )
        query = MemoryQuery(SCOPE, "vehicle transport")
        fused = await ReciprocalRankFusionReranker().rerank(query, items)
        assert fused[0].score == fused[1].score
        ranked = await EmbeddingReranker(
            NarrowSimilarityEmbeddings(), max_candidates=2,
        ).rerank(query, items)
        assert ranked[-1].id == "tail"
        assert ranked[-1].score < min(item.score for item in ranked[:-1])
        assert ranked[-1].metadata["semantic_reranker_status"] == "budget_excluded"
        assert (
            ranked[0].metadata["lexical_fusion_rank"]
            == ranked[1].metadata["lexical_fusion_rank"]
        )

    asyncio.run(exercise())


def test_stage_trace_never_serializes_the_evidence_body():
    item = candidate("item", "sensitive evidence body", kind=MemoryKind.CLAIM)
    trace = stage_summary((item,))
    assert trace["count"] == 1
    assert "sensitive" not in str(trace)
    assert "text" not in trace["items"][0]


def test_bm25_claim_candidates_respect_both_time_axes_and_exact_fact_text(tmp_path, monkeypatch):
    import agent_memory.domain as domain
    import agent_memory.kernel as kernel_module
    import agent_memory.retrieval.temporal_history as history
    import agent_memory.sqlite as repository_module

    def at(day):
        return datetime(2026, 9, day, tzinfo=UTC)

    clock = [at(1)]
    for module in (domain, kernel_module, history, repository_module):
        monkeypatch.setattr(module, "utc_now", lambda: clock[0])

    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            old_text = "The billing cap is €30.75, not $20.00."
            new_text = "The billing cap is €40.75, not $30.00."
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", old_text, occurred_at=at(1),
                metadata={"claims": [{"key": "billing-cap", "value": "€30.75",
                                     "text": old_text, "valid_from": at(1).isoformat(),
                                     "valid_to": at(10).isoformat()}]},
            ))
            clock[0] = at(10)
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", new_text, occurred_at=at(10),
                metadata={"claims": [{"key": "billing-cap", "value": "€40.75",
                                     "text": new_text, "valid_from": at(10).isoformat()}]},
            ))
            clock[0] = at(15)
            current = await kernel._repository.search(MemoryQuery(SCOPE, "billing cap"), 12)
            assert [item.text for item in current if item.kind == MemoryKind.CLAIM] == [new_text]
            before_boundary = await kernel._repository.search(MemoryQuery(
                SCOPE, "billing cap", valid_at=at(9), known_at=at(15),
            ), 12)
            assert [item.text for item in before_boundary] == [old_text]
            boundary = await kernel._repository.search(MemoryQuery(
                SCOPE, "billing cap", valid_at=at(10), known_at=at(15),
            ), 12)
            assert [item.text for item in boundary] == [new_text]
            not_yet_known = await kernel._repository.search(MemoryQuery(
                SCOPE, "billing cap", valid_at=at(10), known_at=at(9),
            ), 12)
            assert not_yet_known == ()
        finally:
            await kernel.close()

    asyncio.run(exercise())
