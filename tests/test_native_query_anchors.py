"""Content admission anchors must not replace evidence, scoring, or governance."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_memory import (
    ForgetMode,
    ForgetRequest,
    MemoryEvent,
    MemoryKind,
    MemoryQuery,
    MemoryScope,
    build_local_kernel,
)
from agent_memory.retrieval import sqlite_lexical_index as index
from agent_memory.retrieval.analyzer import (
    LEXICAL_ANALYZER_VERSION,
    lexical_terms,
    query_content_anchors,
)

SCOPE = MemoryScope("query-anchors", user_id="reader", session_id="session")


@pytest.mark.parametrize(("query", "expected"), (
    ("Where did I buy the $5 microscope?", ("buy", "microscope")),
    ("Which samples are not below 0.5?", ("samples", "below")),
    ('"The Who"', ("who",)),
    ("‘It’", ("it",)),
    ("5", ()),
    ("not 0.5", ()),
    ("Where was I?", ()),
    ("How much was it?", ()),
))
def test_request_anchors_preserve_index_and_full_query_terms(query, expected):
    before = lexical_terms(query)
    assert query_content_anchors(query) == expected
    assert lexical_terms(query) == before
    assert LEXICAL_ANALYZER_VERSION == "ascii-han/1"


def test_low_document_frequency_pronoun_and_decimal_do_not_admit_unrelated_claims(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        query = MemoryQuery(SCOPE, "Where did I buy the $5 microscope?")
        try:
            noise_claim_ids = set()
            for number in range(24):
                text = "My partner and I enjoy walks." if number == 0 else "Routine sample record."
                result = await kernel.ingest_event(MemoryEvent(
                    SCOPE, "user.message", "Observation recorded.", id=f"noise-{number}",
                    metadata={"claims": [{"key": f"observation-{number}", "value": 0.5,
                                         "text": text}]},
                ))
                noise_claim_ids.update(result.claim_ids)
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "The microscope purchase was at Cedar Instruments.",
                id="topic-evidence",
            ))
            # Establish that ordinary full-query lexical retrieval really admits
            # these claims via I / the decimal's 5, without naming the answer.
            now = datetime.now(UTC)
            with kernel._repository._connection() as connection:
                index.prepare_read(connection)
                unrestricted = kernel._repository._lexical_claim_candidates(
                    connection, query, 48, now, now,
                )
            assert unrestricted
            assert {item.id for item in unrestricted} <= noise_claim_ids
            selected = await kernel._repository.search(query, 8)
            assert selected and {item.id for item in selected} == {"topic-evidence"}
            assert not any(item.kind == MemoryKind.CLAIM for item in selected)
            assert all(item.metadata["query_anchor_mode"] == "required" for item in selected)
            assert all(item.metadata["query_anchor_policy"] == "content-anchor/1"
                       for item in selected)
        finally:
            await kernel.close()

    asyncio.run(exercise())


def test_anchor_sql_keeps_negation_numbers_text_and_full_query_bm25_scores(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        query = "microscope not 5"
        text = "The microscope discount is not $5.00; it is €7.25."
        try:
            await kernel.ingest_event(MemoryEvent(SCOPE, "user.message", text, id="topic"))
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "The unrelated record is not 5.", id="number-only",
            ))
            with kernel._repository._connection() as connection:
                index.prepare_read(connection)
                kwargs = {"source_table": "events", "limit": 8,
                          "eligibility": "events.archived_at IS NULL"}
                unrestricted = index.candidates(connection, SCOPE, query, **kwargs)
                anchored = index.candidates(
                    connection, SCOPE, query, **kwargs, anchor_terms=query_content_anchors(query),
                )
            assert {row["owner_id"] for row in unrestricted} == {"topic", "number-only"}
            assert [row["owner_id"] for row in anchored] == ["topic"]
            original = next(row for row in unrestricted if row["owner_id"] == "topic")
            assert anchored[0]["rank_score"] == original["rank_score"]
            selected = await kernel._repository.search(MemoryQuery(SCOPE, query), 8)
            assert [item.text for item in selected] == [text]
        finally:
            await kernel.close()

    asyncio.run(exercise())


def test_quoted_function_word_name_remains_retrievable_without_article_noise(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "The Who released a recording.", id="named-group",
            ))
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "The workshop opened on time.", id="article-only",
            ))
            selected = await kernel._repository.search(MemoryQuery(SCOPE, '"The Who"'), 8)
            assert [item.id for item in selected] == ["named-group"]
            assert selected[0].metadata["query_anchor_count"] == 1
        finally:
            await kernel.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("query", ("5", "not", "Where was I?"))
def test_number_negation_and_function_only_queries_keep_original_recall(tmp_path, query):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            text = "I was not at desk 5."
            await kernel.ingest_event(MemoryEvent(SCOPE, "user.message", text, id="source"))
            selected = await kernel._repository.search(MemoryQuery(SCOPE, query), 8)
            assert [item.text for item in selected] == [text]
            assert selected[0].metadata["query_anchor_mode"] == "unrestricted"
            assert selected[0].metadata["query_anchor_count"] == 0
        finally:
            await kernel.close()

    asyncio.run(exercise())


def test_all_branch_miss_retries_once_without_cross_scope_or_deleted_evidence(
    tmp_path, monkeypatch,
):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        query = MemoryQuery(SCOPE, "Where is my xenolith?")
        calls = []
        collect = kernel._repository._native_lexical_candidates

        def record(connection, request, pool, now, anchors):
            calls.append(tuple(anchors))
            return collect(connection, request, pool, now, anchors)

        monkeypatch.setattr(kernel._repository, "_native_lexical_candidates", record)
        try:
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "My notes are filed.", id="visible",
            ))
            await kernel.ingest_event(MemoryEvent(
                replace(SCOPE, user_id="other"), "user.message", "My xenolith.", id="private",
            ))
            await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "My xenolith.", id="deleted",
            ))
            await kernel.forget(ForgetRequest(SCOPE, ("deleted",), mode=ForgetMode.ERASE))
            selected = await kernel._repository.search(query, 8)
            assert calls == [("xenolith",), ()]
            assert [item.id for item in selected] == ["visible"]
            assert selected[0].metadata["query_anchor_mode"] == "full_query_fallback"
            assert selected[0].metadata["query_anchor_count"] == 1
        finally:
            await kernel.close()

    asyncio.run(exercise())
