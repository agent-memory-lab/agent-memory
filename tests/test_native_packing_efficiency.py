"""Synthetic evidence utility regressions; no benchmark IDs, gold or models."""

import asyncio
from dataclasses import replace
from functools import partial
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from test_evidence_coverage_packing import item
from test_indexed_event_packing import SCOPE, assert_source_lineage, indexed_item

from agent_memory import ForgetMode, ForgetRequest, MemoryEvent, build_local_kernel
from agent_memory.domain import MemoryKind, MemoryQuery
from agent_memory.retrieval.analyzer import lexical_terms, query_content_anchors
from agent_memory.retrieval.candidate_identity import candidate_identity
from agent_memory.retrieval.packing import pack_native_evidence, token_estimate


pack_v3 = partial(pack_native_evidence, version="v3")


@pytest.mark.parametrize(
    "left,right,unit",
    (("kiln drying", "pigment curing", "hours"),
     ("relay assembly", "sensor calibration", "minutes"),
     ("maple seedlings", "cedar seedlings", "trays")),
)
def test_compact_independent_facts_survive_long_same_topic_context(left, right, unit):
    question = f"What is the total for {left} and {right} in {unit}?"
    facts = (
        item("right-fact", f"The {right} total is 45 {unit}.",
             kind=MemoryKind.CLAIM, score=1.0, sources=("shared",)),
        item("left-fact", f"The {left} total is 30 {unit}.",
             kind=MemoryKind.CLAIM, score=0.91, sources=("shared",)),
    )
    text = (
        f"assistant: The {right} total in {unit} requires careful observation.\n"
        + "Routine observations must be documented before making a decision. " * 40
    )
    context = tuple(
        replace(indexed_item(text, 0, len(text), identity=f"context-{n}"), score=.99 - n / 100)
        for n in range(3)
    )
    distractors = tuple(
        item(f"unrelated-{n}", "The maintenance log has been archived.",
             kind=MemoryKind.CLAIM, score=.1, sources=(f"other-{n}",))
        for n in range(40)
    )
    packed = pack_v3(
        MemoryQuery(SCOPE, question, limit=6, token_budget=256), (),
        (facts[0], *context, facts[1], *distractors),
    )
    assert {v.id for v in packed.items} >= {"left-fact", "right-fact"}
    assert {v.text for v in packed.items if v.kind == MemoryKind.CLAIM} >= {
        fact.text for fact in facts
    }
    assert packed.tokens <= 256 and len(packed.items) <= 6
    assert packed.metadata["packing_budget_pressure"]
    for value in packed.items:
        if value.kind == MemoryKind.EVENT:
            original = next(candidate for candidate in context if candidate.id == value.id)
            assert_source_lineage(value, original, text)


def test_lost_content_anchor_cannot_keep_the_parent_score():
    source = (
        "user: The administration notes require careful review before proceeding. "
        + "Earlier meeting records contain background context. " * 10
        + "Zephyr admission closes at 17:45, not at 18:30."
    )
    parent = indexed_item(source, 0, len(source))
    fact = item("fact", "Zephyr admission closes at 17:45, not at 18:30.",
                kind=MemoryKind.CLAIM, score=.65, sources=("authoritative-record",))
    query = MemoryQuery(SCOPE, "When does Zephyr admission close?", limit=1, token_budget=64)
    packed = pack_v3(query, (), (parent, fact))
    assert packed.items == (fact,)
    # The parent is a real topical hit, but its only budget-feasible prefixes
    # lose every topic anchor. Do not fill the bundle with those prefixes.
    prefix = pack_v3(query, (), (parent,))
    assert prefix.items == ()
    assert prefix.metadata["packing_zero_support_variants"] > 0


def test_partially_retained_topic_gets_a_discount_and_preserves_its_role():
    source = (
        "user: Zephyr administration notes require careful review before proceeding. "
        + "Earlier meeting records contain background context. " * 10
        + "The admission closes at 17:45, not at 18:30."
    )
    parent = indexed_item(source, 0, len(source))
    prefix = pack_v3(
        MemoryQuery(SCOPE, "When does Zephyr admission close?", limit=1, token_budget=64),
        (), (parent,),
    )
    assert prefix.items and "Zephyr" in prefix.items[0].text
    assert "admission" not in prefix.items[0].text
    trace = prefix.metadata["packing_selection_trace"][0]
    assert trace["parent_relevance"] == 1
    assert 0 < trace["support_retention"] == trace["relevance"] < 1
    assert prefix.items[0].text.startswith("user:")
    assert_source_lineage(prefix.items[0], parent, source)


def test_semantic_only_parent_is_discounted_after_narrowing_not_discarded():
    sentence = "The vehicle battery was replenished at Haven Garage, never at home."
    source = sentence + " Maintenance records remain available." * 30
    parent = indexed_item(source, 0, len(source))
    packed = pack_v3(
        MemoryQuery(SCOPE, "Where did I recharge the electric car?", limit=1, token_budget=64),
        (), (parent,),
    )
    assert packed.items and sentence in packed.items[0].text
    trace = packed.metadata["packing_selection_trace"][0]
    assert 0 < trace["relevance"] < trace["parent_relevance"] == 1
    assert 0 < trace["support_retention"] < 1
    assert_source_lineage(packed.items[0], parent, source)


def test_legacy_initial_excerpt_also_rechecks_its_retained_query_support():
    source = "Aster permits are recorded here. " + "Background record. " * 120 + "Zephyr."
    parent = item("legacy", source, sources=("source",))
    fact = item("fact", "Aster permits include the Zephyr route.", kind=MemoryKind.CLAIM,
                score=.75, sources=("decision",))
    packed = pack_v3(
        MemoryQuery(SCOPE, "Aster permits Zephyr", limit=1, token_budget=64),
        (), (parent, fact),
    )
    assert packed.items == (fact,)


def test_cost_bonus_cannot_erase_a_large_semantic_relevance_gap():
    text = (
        "The vehicle battery was replenished at Haven Garage, never at home. "
        "The driver confirmed this entry against the dated receipt and the service record. "
        "The signed statement remains available for inspection by an authorized auditor. "
        "The original receipt is kept in the secure records room."
    )
    fact = item("semantic", text, kind=MemoryKind.CLAIM, score=.95, sources=("receipt",))
    noise = item("noise", "Where did I recharge the electric car?", kind=MemoryKind.CLAIM,
                 score=.5, sources=("unanswered",))
    budget = token_estimate(text) + 2
    packed = pack_v3(
        MemoryQuery(SCOPE, "Where did I recharge the electric car?", limit=2, token_budget=budget),
        (), (fact, noise),
    )
    assert packed.metadata["packing_budget_pressure"]
    assert packed.items == (fact,)
    trace = packed.metadata["packing_selection_trace"][0]
    assert trace["support_retention"] == trace["relevance"] == 1
    assert 0 <= trace["cost_penalty"] <= .10


def test_budget_pressure_keeps_role_and_hypothetical_negation_prefix():
    source = (
        "assistant: This is a hypothetical example, not an approved instruction. "
        "The archive gate should not open at 07:30; approval is still pending.\n"
        + "assistant: General archival background. " * 40
    )
    parent = indexed_item(source, 0, len(source))
    other = item("other", "Archive gate audit is pending.", kind=MemoryKind.CLAIM,
                 score=.9, sources=("audit",))
    packed = pack_v3(
        MemoryQuery(SCOPE, "archive gate open approval", limit=2, token_budget=128),
        (), (parent, other),
    )
    value = next(v for v in packed.items if v.id == parent.id)
    assert "hypothetical example, not an approved instruction" in value.text
    assert "should not open at 07:30" in value.text
    assert value.text.startswith("assistant:")
    assert_source_lineage(value, parent, source)


@pytest.mark.parametrize("packing_version", ["v3", "v4", "v5"])
def test_real_kernel_keeps_temporal_scope_and_erasure_guards(tmp_path, packing_version):
    async def run():
        kernel = build_local_kernel(tmp_path / "memory.db", packing_version=packing_version)
        await kernel.initialize()
        known = datetime.now(UTC) + timedelta(minutes=1)
        valid = datetime(2026, 2, 1, tzinfo=UTC)
        try:
            async def ingest(identity, text, *, scope=SCOPE, valid_from="2026-01-01T00:00:00+00:00"):
                await kernel.ingest_event(MemoryEvent(
                    scope, "user.message", text, id=identity,
                    metadata={"claims": [{"key": identity, "value": text, "text": text,
                                           "valid_from": valid_from}]},
                ))

            await ingest("left", "Kiln drying takes 11 hours.")
            await ingest("right", "Pigment curing takes 22 hours.")
            await ingest("future", "Kiln drying takes 999 hours.",
                         valid_from="2027-01-01T00:00:00+00:00")
            await ingest("foreign", "Pigment curing takes 888 hours.",
                         scope=replace(SCOPE, tenant_id="another-tenant"))
            query = MemoryQuery(
                SCOPE, "kiln drying pigment curing hours", limit=8, token_budget=64,
                include_current_state=False, trace_enabled=True, valid_at=valid, known_at=known,
            )
            before = await kernel.retrieve(query)
            text = " ".join(v.text for v in before.relevant_memories)
            assert "11 hours" in text and "22 hours" in text
            assert "999" not in text and "888" not in text
            assert before.token_estimate <= 64
            assert before.retrieval_metadata["source_context_expansion"]["status"] == "disabled_historical"
            traces = before.retrieval_metadata["packing_selection_trace"]
            assert all("cost_penalty" in t and "support_retention" in t for t in traces)
            anchors = set(query_content_anchors(query.text)) or set(lexical_terms(query.text))
            covered = {term for value in before.relevant_memories
                       for term in lexical_terms(value.text) if term in anchors}
            assert before.retrieval_metadata["packing_query_terms_total"] == len(anchors)
            assert before.retrieval_metadata["packing_query_terms_covered"] == len(covered)
            assert len(covered) <= len(anchors)
            await kernel.forget(ForgetRequest(SCOPE, ("left",), mode=ForgetMode.ERASE))
            after = await kernel.retrieve(replace(query, request_id=str(uuid4())))
            text = " ".join(v.text for v in after.relevant_memories)
            assert "11 hours" not in text and "22 hours" in text
        finally:
            await kernel.close()

    asyncio.run(run())


def test_item_cap_does_not_fill_with_one_topic_while_another_fact_is_available():
    repeated = tuple(
        item(f"detail-{n}", f"Amber modules have inventory annotation {n}.",
             kind=MemoryKind.CLAIM, score=1 - n * .008, sources=("inventory",))
        for n in range(12)
    )
    left = item("violet", "Violet modules total 70 units.", kind=MemoryKind.CLAIM,
                score=.91, sources=("inventory",))
    right = item("amber", "Amber modules total 30 units.", kind=MemoryKind.CLAIM,
                 score=.99, sources=("inventory",))
    packed = pack_v3(
        MemoryQuery(SCOPE, "What is the total for violet modules and amber modules?",
                    limit=8, token_budget=1024),
        (), (repeated[0], right, *repeated[1:], left),
    )
    assert {left.id, right.id} <= {v.id for v in packed.items}
    assert len(packed.items) <= 8 and packed.tokens <= 1024


def test_short_same_topic_details_do_not_displace_the_only_updated_source():
    old = item("earlier", "My inventory total is seven components.", kind=MemoryKind.CLAIM,
               sources=("old-record",))
    source = (
        "user: The latest inventory total is discussed in this record. "
        + "The audit includes supporting documentation. " * 16
        + "The latest inventory total is eleven components, not seven.\n"
        + "assistant: Thank you for the update. " * 14
    )
    later = replace(indexed_item(source, 0, len(source), identity="later-record"), score=.98)
    details = tuple(
        item(f"detail-{n}", "Inventory total is a topic in the audit plan.",
             kind=MemoryKind.CLAIM, score=.97 - .003 * n, sources=("old-record",))
        for n in range(10)
    )
    packed = pack_v3(
        MemoryQuery(SCOPE, "What is the latest inventory total?", limit=8, token_budget=512),
        (), (old, later, *details),
    )
    text = " ".join(v.text for v in packed.items)
    assert "seven components" in text
    assert "eleven components, not seven" in text
    assert packed.tokens <= 512 and len(packed.items) <= 8


def test_remaining_budget_expands_multiple_sources_without_removing_selected_facts():
    sources, parents = {}, []
    for topic, value in (("Alpha", "cobalt"), ("Beta", "ivory")):
        source = (
            f"user: {topic} service notes. "
            + "Routine documentation is available. " * 12
            + f"The recovery label is {value}, not gray. "
            + "Routine documentation is available. " * 24
        )
        sources[topic] = source
        parents.append(indexed_item(source, 0, len(source), identity=topic))
    packed = pack_v3(
        MemoryQuery(SCOPE, "Alpha Beta service notes", limit=2, token_budget=512),
        (), parents,
    )
    assert {v.id for v in packed.items} == {"Alpha", "Beta"}
    for value in packed.items:
        assert "The recovery label is" in value.text
        assert "not gray" in value.text
        assert_source_lineage(value, next(v for v in parents if v.id == value.id), sources[value.id])
    trace = packed.metadata["packing_selection_trace"]
    assert all(row["context_expansion_tokens"] > 0 for row in trace)
    assert sum(row["tokens"] for row in trace) == packed.tokens <= 512
    assert packed.metadata["packing_trace_score_stage"] == "compact_selection_before_context_growth"
    for row, value in zip(trace, packed.items, strict=True):
        assert row["candidate_id"] == candidate_identity(value)
        assert row["source_span"] == value.metadata["source_span"]
        assert row["tokens"] == token_estimate(value.text)
        assert row["tokens"] == row["initial_tokens"] + row["context_expansion_tokens"]
        assert row["source_span"]["start"] <= row["initial_source_span"]["start"]
        assert row["initial_source_span"]["end"] <= row["source_span"]["end"]
    assert all(row["source_span"] == value.metadata["source_span"]
               for row, value in zip(packed.metadata["citation_spans"], packed.items, strict=True))


def test_soft_topic_mmr_limits_repetition_even_for_a_broad_query():
    repeated = tuple(
        item(f"detail-{n}", f"Amber equipment has inventory annotation {n}.",
             kind=MemoryKind.CLAIM, score=1 - n * .008, sources=("inventory",))
        for n in range(12)
    )
    other = item("violet", "Violet equipment uses the isolated service bay.",
                 kind=MemoryKind.CLAIM, score=.91, sources=("inventory",))
    query = MemoryQuery(
        SCOPE, "Compare the inventory records, current planning, delivery, and storage "
        "of amber equipment and violet equipment.", limit=8, token_budget=1024,
    )
    packed = pack_v3(query, (), (*repeated, other))
    assert other.id in {value.id for value in packed.items}
    traces = packed.metadata["packing_selection_trace"]
    assert any(row["query_topic_repetition"] > 0 for row in traces)
    assert all(0 <= row["query_topic_repetition"] <= 1 for row in traces)
    assert len(packed.items) <= 8 and packed.tokens <= 1024


def test_topic_repetition_does_not_remove_independent_conflicting_claims():
    facts = (
        item("approved", "Violet equipment can use the service bay.",
             kind=MemoryKind.CLAIM, sources=("shared",)),
        item("revoked", "Violet equipment cannot use the service bay; access was revoked.",
             kind=MemoryKind.CLAIM, sources=("shared",), score=.98),
        item("schedule", "Violet equipment occupies the service bay on Tuesday.",
             kind=MemoryKind.CLAIM, sources=("shared",), score=.97),
    )
    packed = pack_v3(
        MemoryQuery(SCOPE, "Violet equipment service bay access", limit=3, token_budget=128),
        (), facts,
    )
    assert {value.id for value in packed.items} == {value.id for value in facts}
    assert len(packed.citations) == 3


@pytest.mark.parametrize("neighbour,expected_factor", (
    ("assistant: It is at Royal Gardens, never at home.", .8),
    ("assistant: The violet installation is at Royal Gardens, never at home.", .85),
))
def test_inherited_neighbour_score_is_bounded_without_rejecting_semantic_bridges(neighbour, expected_factor):
    first = "user: The violet workshop address is in the next message.\n"
    source = first + neighbour
    anchor = indexed_item(source, 0, len(first))
    context = indexed_item(source, len(first), len(source))
    context = replace(context, metadata={
        **context.metadata, "context_only": True,
        "relevance_origin": "inherited_context_anchor",
        "context_anchor_candidate_id": candidate_identity(anchor),
    })
    packed = pack_v3(
        MemoryQuery(SCOPE, "Where is the violet workshop?", limit=2, token_budget=128),
        (), (anchor, context),
    )
    assert len(packed.items) == 2
    trace = next(row for row in packed.metadata["packing_selection_trace"]
                 if row["candidate_id"] == candidate_identity(context))
    assert trace["context_score_factor"] == pytest.approx(expected_factor)
    assert trace["relevance"] == pytest.approx(expected_factor)
    assert trace["support_retention"] == 1
    assert neighbour in {value.text for value in packed.items}
