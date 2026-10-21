"""Answer-independent intent recall contracts against the real SQLite/core path."""

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
from agent_memory.retrieval.candidate_selection import balanced_candidates, rank_candidate_routes
from agent_memory.retrieval.query_facets import (
    MAX_PROBE_CANDIDATES,
    QUERY_FACET_POLICY,
    query_facets,
)
from agent_memory.sqlite import SQLiteMemoryRepository

SCOPE = MemoryScope("facet-regression", user_id="owner", session_id="conversation")
ADVICE = "Can you recommend a route for my bicycle trip?"


async def fact(kernel, identity, text, *, key=None, scope=SCOPE, **attributes):
    return await kernel.ingest_event(MemoryEvent(
        scope, "user.message", "An imported statement was recorded.", id=identity,
        metadata={"claims": [{"key": key or identity, "value": text, "text": text,
                              **attributes}]},
    ))


@pytest.mark.parametrize("factory", (build_local_kernel, SQLiteMemoryRepository))
@pytest.mark.parametrize("value", (0, 1, "true", "false", None, (), {}))
def test_experimental_opt_in_requires_a_real_bool(tmp_path, factory, value):
    path = tmp_path / "not-created.db"
    with pytest.raises(TypeError, match="enable_query_facets must be a bool"):
        factory(path, enable_query_facets=value)
    assert not path.exists()


@pytest.mark.parametrize("options", ({}, {"enable_query_facets": False}))
def test_default_and_explicit_disabled_recall_match_literal_without_probe(
    tmp_path, monkeypatch, options,
):
    import agent_memory.retrieval.query_facets as facets
    from agent_memory.retrieval.analyzer import query_content_anchors

    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db", **options)
        await kernel.initialize()
        try:
            ignored = await fact(kernel, "standing-preference", "Level terrain suits my knee.")
            for number in range(6):
                await fact(kernel, f"topic-{number}", "A bicycle route report was filed.")
            query = MemoryQuery(SCOPE, ADVICE)
            now = datetime.now(UTC)
            with kernel._repository._connection() as connection:
                index.prepare_read(connection)
                literal = kernel._repository._lexical_claim_candidates(
                    connection, query, 32, now, now,
                    anchor_terms=query_content_anchors(query.text),
                )
            expected = balanced_candidates(literal, 8)
            calls = []
            original = index.candidates

            def record(*args, **kwargs):
                calls.append(kwargs)
                return original(*args, **kwargs)

            monkeypatch.setattr(index, "candidates", record)
            monkeypatch.setattr(
                facets, "query_facets", lambda *args: pytest.fail("planned a probe"),
            )
            actual = await kernel._repository.search(query, 8)

            def shape(items):
                return [(item.id, item.text, item.score, item.metadata["source_span"],
                         item.metadata["lexical_rank"], item.metadata["retrieval_branch"])
                        for item in items]

            assert shape(actual) == shape(expected)
            assert not {item.id for item in actual}.intersection(ignored.claim_ids)
            assert not any("query_facet" in item.metadata for item in actual)
            assert sum(call["source_table"] == "claim_versions" for call in calls) == 1
            assert not any(call.get("required_term_groups") for call in calls)
        finally:
            await kernel.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("query", (
    "Where did I leave the camera?",
    "Explain how recommendation engines work.",
    "What recommendation did I make yesterday?",
    "Recommend a scheduling algorithm.",  # Not a personal request.
    "How many did I acquire last year?",  # No subject to constrain the action.
    "How many instruments were on the shelf?",  # No supported event action.
    "How many times did I attend?",
    "How many guests got sick?",  # An ambiguous verb is not an acquisition intent.
    "Can you help me locate the receipt?",
    "我的图书在哪里？",  # Unknown language keeps the existing path.
))
def test_unrecognized_or_ambiguous_queries_do_not_add_a_probe(query):
    assert query_facets(query) == ()


@pytest.mark.parametrize(("query", "name", "group_count"), (
    (ADVICE, "personal_context", 1),
    ("How should I organize my workshop?", "personal_context", 1),
    ("My afternoon routine is tiring. Any advice?", "personal_context", 1),
    ("How many instruments did I acquire last year?", "acquisition", 2),
    ("What is the number of tutorials I finished?", "completion", 2),
    ("How many workshops did we visit this month?", "attendance", 2),
))
def test_probe_terms_are_bounded_and_all_gates_belong_to_the_query(query, name, group_count):
    (probe,) = query_facets(query)
    assert probe.name == name
    assert len(probe.terms) <= 128
    assert len(probe.required_groups) == group_count
    assert all(set(group) <= set(probe.terms) for group in probe.required_groups)
    if name == "acquisition":
        assert "instrument" in probe.required_groups[1]
        assert not {"year", "last", "acquire"}.intersection(probe.required_groups[1])


def test_route_budget_is_shared_deduplicated_and_borrowed_before_hydration():
    literal = tuple(f"literal-{number}" for number in range(30))
    probe = ("extra", *literal[:8])
    rows = rank_candidate_routes(
        literal, probe, probe_name="facet", limit=12, identity=lambda item: item,
    )
    assert [row[0] for row in rows[:4]] == [*literal[:3], "extra"]
    assert len(rows) == len({row[0] for row in rows}) == 12
    assert rows[0][2] == ("literal", "facet")
    assert [row[1] for row in rows] == list(range(1, 13))
    # Duplicates and empty lanes never consume a slot that another lane can use.
    overlap = rank_candidate_routes(
        literal, literal[:8], probe_name="facet", limit=32, identity=lambda item: item,
    )
    assert len(overlap) == 30
    only_literal = rank_candidate_routes(
        literal, (), probe_name="facet", limit=12, identity=lambda item: item,
    )
    assert [row[0] for row in only_literal] == list(literal[:12])
    only_probe = rank_candidate_routes(
        (), probe, probe_name="facet", limit=12, identity=lambda item: item,
    )
    assert [row[0] for row in only_probe] == list(probe)


def test_same_owner_probe_cannot_replace_the_literal_window():
    literal = tuple({"owner_id": f"claim-{number}", "chunk_id": f"tail-{number}"}
                    for number in range(6))
    probe = ({"owner_id": "claim-5", "chunk_id": "first-5"},)
    selected = rank_candidate_routes(
        literal, probe, probe_name="facet", limit=6, identity=lambda row: row["owner_id"],
    )
    # The probe schedules this owner before literal would reach it, but only
    # contributes rank/provenance. The original topical window remains canonical.
    assert selected[3][0] is literal[5]
    assert selected[3][0]["chunk_id"] == "tail-5"
    assert selected[3][2] == ("literal", "facet")
    assert len({row[0]["owner_id"] for row in selected}) == 6


def test_long_claim_keeps_literal_tail_when_facet_matches_its_first_window(tmp_path):
    from agent_memory.retrieval.analyzer import query_content_anchors

    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db", enable_query_facets=True)
        await kernel.initialize()
        try:
            text = ("I prefer quiet roads. " + "Neutral archive background. " * 140
                    + "The bicycle trip route passes the blue bridge.")
            target = await kernel.ingest_event(MemoryEvent(
                SCOPE, "user.message", "A standing preference was recorded.", id="long-source",
                metadata={"claims": [{"key": "standing_preference", "value": "quiet roads",
                                      "text": text}]},
            ))
            for number in range(6):
                await fact(kernel, f"exact-{number}", ADVICE)
            query = MemoryQuery(SCOPE, ADVICE)
            now = datetime.now(UTC)
            with kernel._repository._connection() as connection:
                index.prepare_read(connection)
                baseline = kernel._repository._lexical_claim_candidates(
                    connection, query, 32, now, now,
                    anchor_terms=query_content_anchors(ADVICE),
                )
            original = next(item for item in baseline if item.id in target.claim_ids)
            assert original.metadata["source_span"]["start"] > 0
            assert "blue bridge" in original.text
            assert next(i for i, item in enumerate(baseline) if item.id == original.id) >= 3
            selected = await kernel._repository.search(query, 12)
            (actual,) = [item for item in selected if item.id == original.id]
            assert actual.metadata["candidate_routes"] == ("literal", "personal_context")
            assert actual.metadata["source_span"] == original.metadata["source_span"]
            assert actual.metadata["lexical_chunk_id"] == original.metadata["lexical_chunk_id"]
            assert actual.text == original.text and "blue bridge" in actual.text
        finally:
            await kernel.close()

    asyncio.run(exercise())


def test_personal_constraint_reaches_core_despite_many_literal_distractors(tmp_path, monkeypatch):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db", enable_query_facets=True)
        await kernel.initialize()
        try:
            text = "Level terrain is easier on my recovering knee."
            target = await fact(kernel, "health-note", text, key="travel_preference")
            for number in range(96):
                await fact(kernel, f"noise-{number}",
                           "A bicycle trip route recommendation document was filed.")
            query = MemoryQuery(SCOPE, ADVICE, include_current_state=False, token_budget=2048)
            # Fail if reads attempt to backfill/scan source bodies instead of
            # using transactional postings already produced during ingestion.
            monkeypatch.setattr(index, "_source", lambda *args: pytest.fail("read rebuilt index"))
            bundle = await kernel.retrieve(query)
            trace = bundle.retrieval_metadata["retrieval_stages"]["candidate_recall"]
            assert trace["count"] <= query.limit * 6
            diagnostic = trace["query_facets"]["personal_context"]
            assert diagnostic["admitted_count"] == diagnostic["locator_count"] == 1
            assert diagnostic["probe_limit"] == MAX_PROBE_CANDIDATES
            assert diagnostic["required_group_count"] == 1
            assert diagnostic["hydration_limit"] == query.limit * 6 * 4
            assert target.claim_ids[0] in {item["id"] for item in trace["items"]}
            claims = [item for item in bundle.relevant_memories
                      if item.id in target.claim_ids]
            assert len(claims) == 1 and claims[0].text == text
            assert claims[0].metadata["query_facet_policy"] == QUERY_FACET_POLICY
            assert claims[0].metadata["candidate_routes"] == ("personal_context",)
            assert claims[0].metadata["source_event_ids"] == ("health-note",)
        finally:
            await kernel.close()

    asyncio.run(exercise())


@pytest.mark.parametrize(("query_text", "key", "text", "noise", "off_topic_text", "facet"), (
    ("How many instruments did I acquire in the last year?", "instrument_inventory",
     "I bought a viola and an oboe together.", "The instruments need cleaning every year.",
     "I purchased a carpet for the house.", "acquisition"),
    ("How many courses did I finish this month?", "course_progress",
     "I completed the safety briefing and welding exercise.", "The courses use short exercises.",
     "I completed a donation to the shelter.", "completion"),
    ("How many workshops did I visit last week?", "workshop_attendance",
     "I attended the soldering class.", "The workshops provide tools and safety equipment.",
     "I attended a concert in town.", "attendance"),
))
def test_event_count_probe_requires_topic_and_action_and_preserves_original_text(
    tmp_path, query_text, key, text, noise, off_topic_text, facet,
):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db", enable_query_facets=True)
        await kernel.initialize()
        try:
            target = await fact(kernel, "inventory", text, key=key)
            for number in range(72):
                await fact(kernel, f"care-{number}", noise)
            off_topic = await fact(kernel, "other-purchase", off_topic_text)
            query = MemoryQuery(SCOPE, query_text)
            found = await kernel._repository.search(query, 12)
            assert len(found) == 12
            result = next(item for item in found if item.id in target.claim_ids)
            assert result.text == text and result.metadata["query_facet"] == facet
            assert result.metadata["candidate_routes"] == (facet,)
            assert not {item.id for item in found}.intersection(off_topic.claim_ids)
            assert all(item.kind == MemoryKind.CLAIM for item in found)
        finally:
            await kernel.close()

    asyncio.run(exercise())


def test_probe_sql_and_body_hydration_have_separate_explicit_bounds(tmp_path, monkeypatch):
    import agent_memory.retrieval.temporal_history as history

    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db", enable_query_facets=True)
        await kernel.initialize()
        try:
            for number in range(48):
                await fact(kernel, f"record-{number}",
                           "A bicycle trip route is documented.", key=f"route_preference_{number}")
            calls, hydrated = [], []
            original_candidates, original_load = index.candidates, history.load_claim

            def record(*args, **kwargs):
                result = original_candidates(*args, **kwargs)
                calls.append((kwargs, len(result)))
                return result

            def load(payload):
                hydrated.append(payload)
                return original_load(payload)

            monkeypatch.setattr(index, "candidates", record)
            monkeypatch.setattr(history, "load_claim", load)
            found = await kernel._repository.search(MemoryQuery(SCOPE, ADVICE), 6)
            assert len(found) == len({item.id for item in found}) == 6
            claims = [(kwargs, count) for kwargs, count in calls
                      if kwargs["source_table"] == "claim_versions"]
            assert len(claims) == 2
            assert claims[0][0]["limit"] == 32
            assert claims[1][0]["limit"] <= MAX_PROBE_CANDIDATES
            assert len(hydrated) == 32  # Overlapping routes hydrate each claim only once.
            assert all(count <= kwargs["limit"] for kwargs, count in calls)
            assert claims[0][0]["eligibility"] == claims[1][0]["eligibility"]
            assert claims[0][0]["params"] == claims[1][0]["params"]
            assert claims[1][0]["required_term_groups"]
        finally:
            await kernel.close()

    asyncio.run(exercise())


def test_non_advice_lookup_does_not_add_unrelated_preferences(tmp_path, monkeypatch):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db", enable_query_facets=True)
        await kernel.initialize()
        try:
            unwanted = await fact(kernel, "preference", "Gentle slopes suit me best.")
            source = MemoryEvent(SCOPE, "note", "The microscope is in a sealed cabinet.")
            await kernel.ingest_event(source)
            calls = []
            original = index.candidates

            def record(*args, **kwargs):
                calls.append(kwargs)
                return original(*args, **kwargs)

            monkeypatch.setattr(index, "candidates", record)
            found = await kernel._repository.search(
                MemoryQuery(SCOPE, "Where is the microscope?"), 8,
            )
            assert {item.id for item in found} == {source.id}
            assert not {item.id for item in found}.intersection(unwanted.claim_ids)
            assert sum(call["source_table"] == "claim_versions" for call in calls) == 1
            assert all(not call.get("required_term_groups") for call in calls)
        finally:
            await kernel.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("mode", (ForgetMode.ARCHIVE, ForgetMode.ERASE))
def test_facet_candidates_respect_scope_deletion_and_both_time_axes(tmp_path, monkeypatch, mode):
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
        kernel = build_local_kernel(tmp_path / "memory.db", enable_query_facets=True)
        await kernel.initialize()
        try:
            old = await fact(kernel, "old", "Level terrain helps.", key="old_preference",
                             valid_from=at(1).isoformat(), valid_to=at(5).isoformat())
            forbidden = set()
            for label, scope in (("tenant", replace(SCOPE, tenant_id="other")),
                                 ("user", replace(SCOPE, user_id="other"))):
                result = await fact(kernel, label, "Gentle slopes suit me.",
                                    key=f"{label}_preference", scope=scope)
                forbidden.update(result.claim_ids)
            removed = await fact(kernel, "removed", "No steep hills.", key="removed_preference")
            forbidden.update(removed.claim_ids)
            await kernel.forget(ForgetRequest(SCOPE, ("removed",), mode=mode))
            clock[0] = at(6)
            late = await fact(kernel, "late", "Quiet lanes are easier.", key="late_preference",
                              valid_from=at(1).isoformat())
            clock[0] = at(10)
            query = MemoryQuery(SCOPE, ADVICE)
            current = await kernel._repository.search(query, 12)
            assert {item.id for item in current} == set(late.claim_ids)
            past = await kernel._repository.search(
                replace(query, valid_at=at(4), known_at=at(4)), 12,
            )
            assert {item.id for item in past} == set(old.claim_ids)
            learned_later = await kernel._repository.search(
                replace(query, valid_at=at(4), known_at=at(7)), 12,
            )
            assert {item.id for item in learned_later} == {*old.claim_ids, *late.claim_ids}
            assert not forbidden.intersection(item.id for item in (*current, *past, *learned_later))
        finally:
            await kernel.close()

    asyncio.run(exercise())


def test_term_group_capacity_and_empty_groups_fail_closed(tmp_path):
    async def exercise():
        kernel = build_local_kernel(tmp_path / "memory.db", enable_query_facets=True)
        await kernel.initialize()
        try:
            await kernel.ingest_event(MemoryEvent(SCOPE, "note", "workshop visit"))
            with kernel._repository._connection() as connection:
                index.prepare_read(connection)
                kwargs = {"source_table": "events", "limit": 4}
                assert index.candidates(connection, SCOPE, "workshop", **kwargs,
                                        required_term_groups=(("visit",),)) == ()
                with pytest.raises(ValueError, match="bounded probe"):
                    index.candidates(connection, SCOPE, "workshop", **kwargs,
                                     required_term_groups=(("workshop",),) * 5)
        finally:
            await kernel.close()

    asyncio.run(exercise())
