"""One lexical term contract for SQLite, BM25, and temporal claim scoring."""

import asyncio
from datetime import UTC, datetime

import pytest

from agent_memory.composition import build_local_kernel
from agent_memory.domain import (
    Claim,
    ClaimStatus,
    MemoryChannel,
    MemoryEvent,
    MemoryItem,
    MemoryKind,
    MemoryQuery,
    MemoryScope,
    Provenance,
)
from agent_memory.retrieval.analyzer import LEXICAL_ANALYZER_VERSION, lexical_terms
from agent_memory.retrieval.lexical import EvidenceItem, _terms, lexical_candidates
from agent_memory.retrieval.temporal_history import temporal_candidates
from agent_memory.sqlite import SQLiteMemoryRepository, _tokens


@pytest.mark.parametrize("separator", [" ", "，", ".", "\n", "-", "_", "API", "42", "🙂"])
def test_han_bigrams_never_cross_a_run_boundary(separator):
    text = f"上海{separator}杭州"
    assert "海杭" not in _tokens(text)
    assert _tokens(text) == set(_terms(text))
    assert {"上", "海", "上海", "杭", "州", "杭州"} <= _tokens(text)


@pytest.mark.parametrize("text", ["A B 7", "request_id-v2", "Straße API", "中文 API 中文"])
def test_sqlite_and_bm25_share_casefolded_ascii_and_han_terms(text):
    assert _tokens(text) == set(_terms(text))


def test_sqlite_overlap_does_not_reward_a_fabricated_bigram():
    query = _tokens("上海")
    assert SQLiteMemoryRepository._overlap(query, "上，海") == pytest.approx(2 / 3)
    assert SQLiteMemoryRepository._overlap(query, "上API海") == pytest.approx(2 / 3)
    assert SQLiteMemoryRepository._overlap(query, "上海") == 1


def _claim(text, *, value="", key="subject"):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Claim(
        id="claim", scope=MemoryScope("tenant"), key=key, value=value, text=text,
        confidence=0.8, importance=0.4, status=ClaimStatus.ACTIVE,
        provenance=Provenance(source_event_ids=("source",)), valid_from=now, created_at=now,
    )


@pytest.mark.parametrize(
    ("text", "query", "value", "overlap"),
    [
        ("迁移，回滚", "迁移回滚", "", 6 / 7),
        ("上 API 海", "上海", "", 2 / 3),
        ("Shanghai", "上海", "上海", 1),
        ("request_id-v2", "request id v2", "", 1),
        ("concatenate", "cat", "", 0),
    ],
)
def test_temporal_claim_scoring_uses_the_same_terms(text, query, value, overlap):
    claim = _claim(text, value=value)
    (item,) = temporal_candidates([claim], query, 1)
    assert item.score == pytest.approx(overlap * 0.55 + 0.4 * 0.25 + 0.8 * 0.2)
    assert item.metadata["source_event_ids"] == ("source",)
    assert item.metadata["valid_from"] == claim.valid_from.isoformat()


def test_sqlite_ranks_contiguous_han_above_split_runs_without_scope_drift(tmp_path):
    async def scenario():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        scope = MemoryScope("tenant", user_id="owner", session_id="session")
        events = [
            MemoryEvent(scope, "note", "上，海", id="split"),
            MemoryEvent(scope, "note", "上海", id="contiguous"),
            MemoryEvent(MemoryScope("foreign"), "note", "上海", id="foreign"),
        ]
        try:
            for event in events:
                await kernel.ingest_event(event)
            rows = await kernel._repository.search(
                MemoryQuery(scope, "上海", channels=(MemoryChannel.SEMANTIC,)), 8
            )
            assert [item.id for item in rows] == ["contiguous", "split"]
            assert rows[0].score > rows[1].score
            assert all(item.metadata["source_event_ids"] == (item.id,) for item in rows)
        finally:
            await kernel.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("", ()),
        ("，🙂---__", ()),
        ("A B 7", ("a", "b", "7")),
        ("request_id-v2", ("request", "id", "v2")),
        ("Straße API", ("strasse", "api")),
        ("上海杭州", ("上", "海", "杭", "州", "上海", "海杭", "杭州")),
        ("中中 API 中中", ("中", "中", "中中", "api", "中", "中", "中中")),
        ("上🙂海", ("上", "海")),
        ("\u3400\u9fff", ("\u3400", "\u9fff", "\u3400\u9fff")),
        # The first version intentionally preserves the optional BM25 alphabet.
        ("ＡＰＩ 𠀀", ()),
    ],
)
def test_version_one_golden_terms_preserve_frequency_and_order(text, expected):
    assert LEXICAL_ANALYZER_VERSION == "ascii-han/1"
    assert lexical_terms(text) == expected
    assert _terms(text) == expected
    assert _tokens(text) == set(expected)


def test_bm25_reports_analyzer_version_without_changing_evidence():
    now = datetime(2026, 1, 1, tzinfo=UTC)
    evidence = EvidenceItem(
        MemoryItem("item", MemoryKind.EVENT, "上海", 0.5, now),
        MemoryChannel.SEMANTIC, ("source",),
    )
    (candidate,) = lexical_candidates("上海", [evidence]).candidates
    assert candidate.metadata["lexical_analyzer_version"] == LEXICAL_ANALYZER_VERSION
    assert candidate.metadata["lexical_score"] > 0
    assert candidate.item is evidence.item
    assert candidate.source_event_ids == ("source",)


def test_temporal_terms_do_not_join_across_claim_fields():
    claim = _claim("海", key="上")
    (item,) = temporal_candidates([claim], "上海", 1)
    assert item.score == pytest.approx(2 / 3 * 0.55 + 0.4 * 0.25 + 0.8 * 0.2)


@pytest.mark.parametrize("query", ["", "unrelated", "，🙂"])
def test_temporal_no_hit_keeps_existing_nonlexical_ranking_policy(query):
    (item,) = temporal_candidates([_claim("上海")], query, 1)
    assert item.score == pytest.approx(0.4 * 0.25 + 0.8 * 0.2)


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_historical_claim_search_shares_terms_and_keeps_evidence_gates(tmp_path, backend):
    import os
    from dataclasses import replace
    from urllib.parse import urlsplit
    from uuid import uuid4

    from agent_memory.domain import ForgetMode, ForgetRequest, utc_now

    if backend == "postgres":
        dsn = os.environ.get("AGENT_MEMORY_TEST_POSTGRES_DSN", "")
        if not dsn:
            pytest.skip("historical analyzer parity requires a disposable PostgreSQL test DSN")
        parsed = urlsplit(dsn)
        if parsed.scheme not in {"postgres", "postgresql"} or "test" not in parsed.path.casefold():
            pytest.fail("historical analyzer check requires a PostgreSQL database named test")
        pg = pytest.importorskip("agent_memory_postgres")
        kernel = pg.build_postgres_kernel(dsn)
    else:
        kernel = build_local_kernel(tmp_path / "historical.db")

    async def scenario():
        await kernel.initialize()
        scope = MemoryScope(f"analyzer-{uuid4().hex}", user_id="owner", session_id="session")
        start = datetime(2026, 1, 1, tzinfo=UTC)
        events = [
            MemoryEvent(scope, "fact", "destination", metadata={"claims": [{
                "key": "contiguous", "text": "destination", "value": "上海",
                "valid_from": start.isoformat(), "confidence": 0.8, "importance": 0.4,
            }]}),
            MemoryEvent(scope, "fact", "上，海", metadata={"claims": [{
                "key": "split", "text": "上，海", "value": "",
                "valid_from": start.isoformat(), "confidence": 0.8, "importance": 0.4,
            }]}),
        ]
        try:
            for event in events:
                await kernel.ingest_event(event)
            query = MemoryQuery(scope, "上海", valid_at=start, known_at=utc_now())
            rows = await kernel._repository.search(query, 8)
            assert [item.metadata["key"] for item in rows] == ["contiguous", "split"]
            assert rows[0].score == pytest.approx(0.55 + 0.4 * 0.25 + 0.8 * 0.2)
            assert rows[1].score == pytest.approx(2 / 3 * 0.55 + 0.4 * 0.25 + 0.8 * 0.2)
            assert [item.metadata["source_event_ids"] for item in rows] == [
                (events[0].id,), (events[1].id,),
            ]
            assert await kernel._repository.search(
                replace(query, scope=replace(scope, user_id="other")), 8
            ) == ()
            await kernel.forget(ForgetRequest(scope, (events[0].id,), mode=ForgetMode.ARCHIVE))
            survivors = await kernel._repository.search(query, 8)
            assert [item.metadata["key"] for item in survivors] == ["split"]
        finally:
            await kernel.close()

    asyncio.run(scenario())
