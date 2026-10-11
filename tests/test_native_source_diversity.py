"""Source opportunity is preserved before and after representation quotas."""

import asyncio
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from json import dumps

from agent_memory import MemoryEvent, MemoryItem, MemoryKind, MemoryQuery, MemoryScope
from agent_memory.composition import build_local_kernel
from agent_memory.retrieval import sqlite_lexical_index as index
from agent_memory.retrieval.candidate_identity import (
    MAX_SOURCE_WINDOW_CHARS,
    candidate_identity,
)
from agent_memory.retrieval.candidate_selection import balanced_candidates

NOW = datetime(2026, 10, 11, tzinfo=UTC)
SCOPE = MemoryScope("source-diversity", user_id="reader", session_id="session")


def window(owner, start, score, reason="matched"):
    end = start + 2816
    revision = sha256(owner.encode()).hexdigest()
    locator = sha256(dumps(["events", owner, revision, start, end]).encode()).hexdigest()
    return MemoryItem(owner, MemoryKind.EVENT, "x" * 2816, score, NOW, {
        "lexical_score": score,
        "source_event_ids": (owner,),
        "source_owner_id": owner,
        "source_revision": revision,
        "source_chars": 10000,
        "source_span": {"start": start, "end": end, "unit": "characters"},
        "lexical_chunk_id": locator,
        "context_reason": reason,
    })


def test_kind_budget_cannot_be_consumed_by_four_windows_of_only_six_sources():
    events = tuple(
        window(f"event-{owner:02}", start, 100 - owner, reason)
        for owner in range(20)
        for start, reason in ((0, "matched"), (896, "additional_match"),
                              (1792, "source_tail"), (2688, "adjacent"))
    )
    facts = tuple(MemoryItem(
        f"fact-{number}", MemoryKind.CLAIM, "reference record", 1.0, NOW,
        {"lexical_score": 100 - number, "source_event_ids": ("same-source",)},
    ) for number in range(30))
    selected = balanced_candidates((*events, *facts), 48)
    chosen_events = [item for item in selected if item.kind == MemoryKind.EVENT]
    assert len(selected) == 48
    assert len(chosen_events) == 24
    assert len({item.id for item in chosen_events}) == 20
    assert all(item.metadata["context_reason"] == "matched" for item in chosen_events[:20])
    assert max(Counter(item.id for item in chosen_events).values()) == 2
    assert sum(item.kind == MemoryKind.CLAIM for item in selected) == 24


def test_primary_hits_precede_inherited_scores_and_metadata_cannot_merge_owners():
    weak = window("weak", 0, 1.0)
    tail = window("weak", 1792, 100.0, "source_tail")
    strong = window("strong", 0, 10.0)
    # The public EVENT id, not a caller-provided owner label, defines the group.
    strong = replace(strong, metadata={**strong.metadata, "source_owner_id": "weak"})
    selected = balanced_candidates((tail, weak, strong), 2)
    assert [item.id for item in selected] == ["strong", "weak"]
    assert all(item.metadata["context_reason"] == "matched" for item in selected)
    complete = balanced_candidates((tail, weak, strong), 3)
    assert complete[-1].metadata["context_reason"] == "source_tail"


def test_spare_slots_are_borrowed_for_distinct_same_source_spans():
    items = tuple(
        window(owner, start, score, reason)
        for owner, score in (("a", 10), ("b", 5), ("c", 1))
        for start, reason in ((0, "matched"), (896, "additional_match"),
                              (1792, "source_tail"), (2688, "adjacent"))
    )
    selected = balanced_candidates(items, 10)
    assert len(selected) == 10
    assert [item.id for item in selected[:9]] == ["a", "b", "c"] * 3
    assert len({candidate_identity(item) for item in selected}) == 10
    assert set(Counter(item.id for item in selected).values()) == {3, 4}


def test_sqlite_locator_limit_reserves_one_primary_per_owner_before_expansion(
    tmp_path, monkeypatch,
):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        observed = []
        resolve = index._event_window

        def record_window(connection, locator):
            observed.append(locator["owner_id"])
            return resolve(connection, locator)

        monkeypatch.setattr(index, "_event_window", record_window)
        try:
            for number in range(15):
                await kernel.ingest_event(MemoryEvent(
                    SCOPE, "user.message",
                    "The archive certificate is filed. " + "Routine maintenance. " * 400,
                    id=f"visible-{number:02}",
                ))
            await kernel.ingest_event(MemoryEvent(
                replace(SCOPE, user_id="private"), "user.message",
                "archive certificate " * 500, id="hidden",
            ))
            # Exercise the total SQL ceiling without creating thousands of rows:
            # the second hit of one owner cannot displace another owner's first.
            monkeypatch.setattr(index, "MAX_CANDIDATES", 7)
            with kernel._repository._connection() as connection:
                index.prepare_read(connection)
                locators = index.candidates(
                    connection, SCOPE, "archive certificate", source_table="events", limit=7,
                    eligibility="events.archived_at IS NULL", context_chunks=4,
                )
                rows = index.hydrate_events(connection, locators)
            assert len(locators) == len(rows) == 7
            assert len({item["owner_id"] for item in locators}) == 7
            assert len(observed) == 7  # No deferred context windows were resolved.
            assert all(item["context_reason"] == "matched" for item in locators)
            assert all(item["owner_id"] != "hidden" for item in locators)
            assert all(len(row["content"]) <= MAX_SOURCE_WINDOW_CHARS for row, _ in rows)
        finally:
            await kernel.close()

    asyncio.run(exercise())


def test_real_repository_preserves_sources_after_claim_event_budget_split(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db")
        await kernel.initialize()
        try:
            for number in range(15):
                await kernel.ingest_event(MemoryEvent(
                    SCOPE, "user.message",
                    "The archive certificate is filed. " + "Routine maintenance. " * 400,
                    id=f"visible-{number:02}",
                    metadata={"claims": [{"key": f"archive-{number}", "value": str(number),
                                         "text": f"The archive reference is {number}."}]},
                ))
            selected = await kernel._repository.search(
                MemoryQuery(SCOPE, "archive certificate reference"), 12,
            )
            facts = [item for item in selected if item.kind == MemoryKind.CLAIM]
            events = [item for item in selected if item.kind == MemoryKind.EVENT]
            assert len(facts) == len(events) == 6
            assert len({item.id for item in events}) == 6
            assert all(item.metadata["context_reason"] == "matched" for item in events)
        finally:
            await kernel.close()

    asyncio.run(exercise())
