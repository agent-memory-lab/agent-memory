"""Post-ranking context expansion obeys authoritative scope and source revisions."""

import asyncio
import sqlite3
from collections import Counter
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256

import pytest

from agent_memory import MemoryEvent, build_local_kernel
from agent_memory.domain import MemoryChannel, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.retrieval.candidate_identity import candidate_identity

SCOPE = MemoryScope("context", user_id="u", agent_id="a", workspace_id="w", session_id="s")
NOW = datetime(2026, 10, 11, tzinfo=UTC)
QUERY = "Where is the archive meeting?"
FOLLOWING = "user: I cannot attend the archive meeting at Harbor Hall on Friday."


def source_text(*, middle=False):
    if middle:
        text = "Routine notes. " * 200
        text += "The archive meeting was discussed."
    else:
        text = "The archive meeting was discussed."
    text += " Routine notes." * 600
    return text[:3900] + "\n" + FOLLOWING + "\n" + text[3900:]


def request(**kwargs):
    return MemoryQuery(SCOPE, QUERY, include_current_state=False, token_budget=2042, **kwargs)


async def seed(kernel, count=1, *, middle=False):
    for number in range(count):
        await kernel.ingest_event(MemoryEvent(
            SCOPE, "user.message", source_text(middle=middle), id=f"source-{number}",
        ))
    rows = await kernel._repository.search(request(), 96)
    return tuple(row for row in rows if row.metadata.get("context_reason") == "matched")


def test_expansion_recovers_a_user_sentence_outside_the_ranked_primary_window(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            anchors = await seed(kernel)
            anchor = anchors[0]
            assert anchor.metadata["source_span"] == {
                "start": 0, "end": 3712, "unit": "characters",
            }
            assert FOLLOWING not in anchor.text
            expanded = await kernel._repository.expand_source_context(request(), anchors, 8)
            assert len(expanded) == 1
            item = expanded[0]
            assert FOLLOWING in item.text
            assert item.metadata["source_span"] == {
                "start": 896, "end": 4608, "unit": "characters",
            }
            assert item.score == anchor.score
            assert item.metadata["context_only"] is True
            assert item.metadata["relevance_origin"] == "inherited_context_anchor"
            assert item.metadata["context_anchor_candidate_id"] == candidate_identity(anchor)
            assert not {"semantic_score", "semantic_dimensions", "lexical_score",
                        "matched_source_span"}.intersection(item.metadata)
            assert item.metadata["source_revision"] == sha256(source_text().encode()).hexdigest()
            assert item.text == source_text()[896:4608]
        finally:
            await kernel.close()
    asyncio.run(exercise())


@pytest.mark.parametrize("field", (
    "tenant_id", "namespace", "user_id", "agent_id", "workspace_id", "session_id",
))
def test_forged_caller_scope_cannot_expand_a_known_private_source(tmp_path, field):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            anchors = await seed(kernel)
            foreign = replace(request(), scope=replace(SCOPE, **{field: "different"}))
            assert await kernel._repository.expand_source_context(foreign, anchors, 8) == ()
        finally:
            await kernel.close()
    asyncio.run(exercise())


@pytest.mark.parametrize("change", ("revision", "archive", "erase"))
def test_stale_ranked_source_cannot_authorize_reads_after_authoritative_change(tmp_path, change):
    async def exercise():
        path = tmp_path / "memory.db"
        kernel = build_local_kernel(path)
        await kernel.initialize()
        try:
            anchors = await seed(kernel)
            with closing(sqlite3.connect(path)) as connection, connection:
                if change == "revision":
                    connection.execute("UPDATE events SET content=? WHERE id=?", (
                        "A changed source must not inherit old relevance. " * 100, "source-0",
                    ))
                elif change == "archive":
                    connection.execute("UPDATE events SET archived_at=? WHERE id=?", (
                        NOW.isoformat(), "source-0",
                    ))
                else:
                    connection.execute("DELETE FROM events WHERE id=?", ("source-0",))
            assert await kernel._repository.expand_source_context(request(), anchors, 8) == ()
        finally:
            await kernel.close()
    asyncio.run(exercise())


@pytest.mark.parametrize("overrides", (
    {"valid_at": NOW}, {"known_at": NOW}, {"channels": ()},
    {"channels": (MemoryChannel.EPISODIC,)},
))
def test_historical_and_nonsemantic_requests_never_expand_current_source(tmp_path, overrides):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            anchors = await seed(kernel)
            assert await kernel._repository.expand_source_context(
                request(**overrides), anchors, 8,
            ) == ()
        finally:
            await kernel.close()
    asyncio.run(exercise())


def test_expansion_source_locator_and_body_budgets_are_independent_of_caller_limit(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            anchors = await seed(kernel, 6, middle=True)
            assert len(anchors) == 6
            statements = []
            connect = kernel._repository._connect

            def traced_connection():
                connection = connect()
                connection.set_trace_callback(statements.append)
                return connection

            kernel._repository._connect = traced_connection
            expanded = await kernel._repository.expand_source_context(request(), anchors, 10000)
            assert len(expanded) == 8
            assert len({item.id for item in expanded}) == 4
            assert max(Counter(item.id for item in expanded).values()) == 2
            assert all(len(item.text) <= 3712 for item in expanded)
            reads = [sql for sql in statements if " AS content " in sql]
            assert len(reads) == 8
            assert all("substr(CAST(e.content AS BLOB)" in sql for sql in reads)
            assert all("SELECT * FROM events" not in sql for sql in statements)
            assert len(await kernel._repository.expand_source_context(
                request(limit=1), anchors, 10000,
            )) == 2
            assert len(await kernel._repository.expand_source_context(request(), anchors, 1)) == 1
            # Existing candidate identities do not get re-hydrated as added windows.
            assert await kernel._repository.expand_source_context(
                request(), (*anchors, *expanded), 8,
            ) == ()
        finally:
            await kernel.close()
    asyncio.run(exercise())


def test_kernel_adds_context_after_fixed_embedding_batch_and_reguards_it(tmp_path):
    async def exercise():
        class Embeddings:
            def __init__(self):
                self.calls = []

            async def embed(self, texts):
                self.calls.append(tuple(texts))
                return tuple((1.0, 0.0) for _ in texts)

        embeddings = Embeddings()
        kernel = build_local_kernel(tmp_path / "memory.db", embedding_provider=embeddings)
        await kernel.initialize()
        try:
            anchors = await seed(kernel)

            noise = replace(
                anchors[0], id="other", kind=MemoryKind.CLAIM,
                text="No scheduling changes.", score=0.001, metadata={},
            )

            async def primary_only(query, limit):
                return (*anchors, noise)

            kernel._repository.search = primary_only
            original_guard = kernel._guard_atom_candidates
            guarded = []

            async def recording_guard(scope, items):
                guarded.append(tuple(items))
                return await original_guard(scope, items)

            kernel._guard_atom_candidates = recording_guard
            bundle = await kernel.retrieve(request())
            assert len(embeddings.calls) == 1
            assert len(embeddings.calls[0]) == 3
            assert FOLLOWING not in embeddings.calls[0][1]
            assert any(FOLLOWING in item.text for item in bundle.relevant_memories)
            assert len(guarded) == 2
            assert any(item.metadata.get("context_only") for item in guarded[1])
            assert len(bundle.relevant_memories) <= 8 and bundle.token_estimate <= 2042
            info = bundle.retrieval_metadata
            assert info["source_context_expansion"]["candidate_count"] == 1
            assert info["source_context_expansion"]["semantic_scoring"] is False
            assert info["retrieval_stages"]["reranked"]["count"] == 2
            assert info["retrieval_stages"]["packing_input"]["count"] == 3
            assert info["retrieval_stages"]["source_context"]["count"] == 1
        finally:
            await kernel.close()
    asyncio.run(exercise())


def test_full_48_document_embedding_budget_is_unchanged_by_context_expansion(tmp_path):
    async def exercise():
        class Embeddings:
            def __init__(self):
                self.batch_sizes = []

            async def embed(self, texts):
                self.batch_sizes.append(len(texts))
                return tuple((1.0, 0.0) for _ in texts)

        embeddings = Embeddings()
        kernel = build_local_kernel(tmp_path / "memory.db", embedding_provider=embeddings)
        await kernel.initialize()
        try:
            anchors = await seed(kernel, 48)
            assert len(anchors) == 48

            async def first_source_windows(query, limit):
                return anchors

            kernel._repository.search = first_source_windows
            bundle = await kernel.retrieve(request())
            assert embeddings.batch_sizes == [49]
            stages = bundle.retrieval_metadata["retrieval_stages"]
            assert stages["reranked"]["count"] == 48
            assert stages["source_context"]["count"] == 4
            assert stages["packing_input"]["count"] == 52
            assert len(bundle.relevant_memories) <= 8
            assert bundle.token_estimate <= 2042
        finally:
            await kernel.close()
    asyncio.run(exercise())


def test_optional_capability_absence_preserves_the_existing_retrieval_path(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            await seed(kernel)
            kernel._repository.expand_source_context = None
            bundle = await kernel.retrieve(request())
            assert bundle.relevant_memories
            assert bundle.retrieval_metadata["source_context_expansion"]["status"] == "unsupported"
            assert bundle.retrieval_metadata["source_context_expansion"]["candidate_count"] == 0
        finally:
            await kernel.close()
    asyncio.run(exercise())


def test_legacy_public_id_prefix_does_not_impersonate_a_validated_source_locator(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            anchors = await seed(kernel)
            malformed = replace(anchors[0], id="source-span:legacy-public-id", metadata={})
            assert await kernel._repository.expand_source_context(
                request(), (malformed,), 8,
            ) == ()
        finally:
            await kernel.close()
    asyncio.run(exercise())
