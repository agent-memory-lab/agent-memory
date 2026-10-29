"""Budget pressure must not trade independent facts for early long context."""

import asyncio
from dataclasses import replace

import pytest
from test_evidence_coverage_packing import item
from test_indexed_event_packing import NOW, SCOPE
from test_packing_group_duplicates import context_child, indexed_item

from agent_memory import MemoryEvent, build_local_kernel
from agent_memory.domain import MemoryBundle, MemoryCapabilities, MemoryKind, MemoryQuery
from agent_memory.retrieval.analyzer import lexical_terms
from agent_memory.retrieval.candidate_identity import candidate_identity
from agent_memory.retrieval.evidence_units import retain_complete_groups
from agent_memory.retrieval.packing import pack_native_evidence, token_estimate
from agent_memory.retrieval.packing_policy import packing_contract

BACKGROUND = "Routine documentation remains available in the archive. " * 15


def records(count):
    facts = [f"user: Cobalt count for parcel {i} is {12 + i}, not {21 + i}." for i in range(count)]
    sources = [fact + "\nassistant: " + BACKGROUND for fact in facts]
    candidates = tuple(
        replace(indexed_item(text, 0, len(text), identity=f"parcel-{i}"), score=1 - i / 100)
        for i, text in enumerate(sources)
    )
    return facts, sources, candidates


def request(limit, budget=256):
    return MemoryQuery(
        SCOPE,
        "cobalt count",
        limit=limit,
        token_budget=budget,
        include_current_state=False,
        trace_enabled=False,
    )


async def validate_delivery(tmp_path, sources, query, packed):
    from experiments.public_longmemeval.reader_context import core_reader_context

    kernel = build_local_kernel(tmp_path / "memory.db")
    await kernel.initialize()
    try:
        for identity, source in sources.items():
            await kernel.ingest_event(
                MemoryEvent(
                    SCOPE,
                    "conversation",
                    source,
                    id=identity,
                    occurred_at=NOW,
                )
            )
        bundle = MemoryBundle(
            (),
            packed.items,
            (),
            (),
            packed.citations,
            packed.tokens,
            packed.metadata,
            MemoryCapabilities(),
        )
        guarded = await kernel._repository.guard_retrieval_delivery(query, bundle)
        assert guarded.relevant_memories == packed.items
        assert guarded.retrieval_metadata["delivery_guard"]["removed_count"] == 0
        groups = guarded.retrieval_metadata.get("packing_dependency_groups", ())
        retained, complete = retain_complete_groups(guarded.relevant_memories, groups)
        assert retained == packed.items and complete == tuple(groups)
        reader = core_reader_context(
            guarded,
            {key: key for key in sources},
            token_budget=query.token_budget,
            item_limit=query.limit,
        )
        assert [value["text"] for value in reader.delivered] == [
            value.text for value in packed.items
        ]
        assert reader.diagnostics["evidence_estimated_tokens"] == packed.tokens
    finally:
        await kernel.close()


def test_default_v2_keeps_reservations_and_context_expansion_disabled():
    contract = packing_contract()
    assert contract == packing_contract("v2")
    assert contract["version"] == "v2"
    assert contract["strategy"] == "relevance_bounded_diversity_v2"
    assert contract["reserve_evidence_budget"] is False
    assert contract["expand_selected_context"] is False
    _, _, candidates = records(4)
    default = pack_native_evidence(request(4), (), candidates)
    explicit = pack_native_evidence(request(4), (), candidates, version="v2")
    assert default == explicit
    assert default.metadata["packing_context_expansion_tokens"] == 0


@pytest.mark.parametrize("version", ("v4", "v5"))
@pytest.mark.parametrize("count", (2, 4))
def test_independent_long_sources_keep_every_complete_fact_under_pressure(version, count):
    facts, sources, candidates = records(count)
    assert max(token_estimate(text) for text in sources) < 256
    assert sum(token_estimate(text) for text in sources) > 256
    packed = pack_native_evidence(request(count), (), candidates, version=version)

    assert {value.id for value in packed.items} == {value.id for value in candidates}
    for number, fact in enumerate(facts):
        delivered = next(value for value in packed.items if value.id == f"parcel-{number}")
        assert fact in delivered.text  # Includes the full role and negated value.
        span = delivered.metadata["source_span"]
        assert delivered.text == sources[number][span["start"] : span["end"]]
    assert packed.tokens == sum(token_estimate(value.text) for value in packed.items) <= 256


@pytest.mark.parametrize("version", ("v2", "v4", "v5"))
def test_no_pressure_preserves_full_source_context(version):
    _, sources, candidates = records(2)
    packed = pack_native_evidence(request(2, budget=2048), (), candidates, version=version)

    assert [value.text for value in packed.items] == sources
    assert packed.tokens == sum(token_estimate(text) for text in sources)


@pytest.mark.parametrize("version", ("v4", "v5"))
def test_soft_share_falls_back_to_indivisible_qualified_evidence(version):
    # One complete sentence has no smaller safe passage. It exceeds a nominal
    # quarter-share but fits the total budget together with compact other facts.
    sentence = (
        "user: Cobalt is valid only if "
        + "the written verification includes all mandated details " * 8
        + "and never otherwise."
    )
    assert 64 < token_estimate(sentence) < 256
    root = indexed_item(sentence, 0, len(sentence), identity="qualified-rule")
    facts, _, others = records(3)
    packed = pack_native_evidence(request(4), (), (root, *others), version=version)

    assert sentence in [value.text for value in packed.items]
    assert {value.id for value in packed.items} == {
        "qualified-rule",
        "parcel-0",
        "parcel-1",
        "parcel-2",
    }
    assert all(any(fact in value.text for value in packed.items) for fact in facts)
    assert packed.tokens <= 256


@pytest.mark.parametrize("version", ("v2", "v4", "v5"))
def test_budget_allocation_preserves_distinct_claims_with_shared_provenance(version):
    _, _, candidates = records(2)
    claims = (
        item(
            "observed",
            "Cobalt count was 12.",
            kind=MemoryKind.CLAIM,
            sources=("ledger",),
            score=1.0,
        ),
        item(
            "corrected",
            "Cobalt count is 14, not 12.",
            kind=MemoryKind.CLAIM,
            sources=("ledger",),
            score=0.99,
        ),
    )
    packed = pack_native_evidence(request(4), (), (*claims, *candidates), version=version)

    assert {value.id for value in packed.items} == {"observed", "corrected", "parcel-0", "parcel-1"}
    assert {value.text for value in packed.items if value.kind == MemoryKind.CLAIM} == {
        value.text for value in claims
    }
    assert packed.tokens <= 256


@pytest.mark.parametrize("version", ("v2", "v4", "v5"))
def test_high_ranked_semantic_evidence_keeps_fact_without_query_word_overlap(version):
    fact = "user: The vehicle battery was replenished at Haven Garage, never at home."
    source = fact + "\nassistant: " + BACKGROUND
    query = replace(request(3), text="Where did I recharge?")
    assert not set(lexical_terms(fact)).intersection(lexical_terms(query.text))
    semantic_hit = indexed_item(source, 0, len(source), identity="semantic-hit")
    _, _, others = records(2)
    others = tuple(replace(value, score=0.8 - i / 10) for i, value in enumerate(others))
    packed = pack_native_evidence(query, (), (semantic_hit, *others), version=version)

    hit = next(value for value in packed.items if value.id == "semantic-hit")
    assert fact in hit.text
    assert packed.tokens <= query.token_budget


@pytest.mark.parametrize("version", ("v4", "v5"))
@pytest.mark.parametrize("support", ("semantic", "separated_terms"))
def test_infeasible_reservations_keep_the_high_ranked_complete_evidence(version, support):
    # The leading complete evidence fits, but all minima together do not. The
    # reservation fallback must prefer relevance over an unproved short slice.
    if support == "semantic":
        fact = "user: The vehicle battery was replenished at Haven Garage, never at home."
        first = "assistant: " + BACKGROUND + "\n" + fact + "\n"
        query = replace(request(2), text="Where did I recharge?")
        assert not set(lexical_terms(first)).intersection(lexical_terms(query.text))
    else:
        fact = "user: Count is 12, never 21."
        first = (
            "user: Cobalt parcel is under review.\nassistant: " + BACKGROUND + "\n" + fact + "\n"
        )
        query = request(2)
    second = (
        "assistant: This rule "
        + "requires complete attached documentation " * 7
        + "and never otherwise."
    )
    source = first + second
    root = indexed_item(source, 0, len(first), identity="leading-evidence")
    other = item(
        "indivisible-rule",
        "A required rule " + "must remain completely attached " * 8 + "and never otherwise.",
        kind=MemoryKind.CLAIM,
        sources=("policy",),
        score=0.5,
    )
    child = context_child(
        root,
        indexed_item(source, len(first) - 1, len(source), identity=root.id),
    )
    candidates = (root, child, other)
    query = replace(query, limit=3)
    assert token_estimate(first) < query.token_budget
    assert token_estimate(first) + token_estimate(other.text) > query.token_budget

    packed = pack_native_evidence(query, (), candidates, version=version)

    assert root.text in [value.text for value in packed.items]
    assert any(fact in value.text for value in packed.items)
    assert packed.tokens <= query.token_budget


@pytest.mark.parametrize("version", ("v2", "v4", "v5"))
def test_duplicate_candidate_identity_does_not_change_delivery(version):
    _, _, candidates = records(4)
    baseline = pack_native_evidence(request(4), (), candidates, version=version)
    repeated = (candidates[0],) * 20 + candidates + candidates * 5
    duplicated = pack_native_evidence(request(4), (), repeated, version=version)

    assert duplicated.items == baseline.items
    assert duplicated.tokens == baseline.tokens
    assert duplicated.citations == baseline.citations


@pytest.mark.parametrize("version", ("v4", "v5"))
def test_same_minimum_span_does_not_reserve_a_second_independent_slot(version):
    source = "user: Cobalt count is 12, never 21.\nassistant: " + BACKGROUND
    root = indexed_item(source, 0, len(source), identity="same-source")
    duplicate = indexed_item(source, 0, len(source) - 1, identity=root.id)
    assert candidate_identity(root) != candidate_identity(duplicate)
    other = item(
        "independent-rule",
        "Cobalt count is valid only if "
        + "all paperwork accompanies it " * 5
        + "and never otherwise.",
        kind=MemoryKind.CLAIM,
        sources=("policy",),
        score=0.7,
    )
    query = request(2, budget=64)
    baseline = pack_native_evidence(query, (), (root, other), version=version)
    duplicated = pack_native_evidence(query, (), (root, duplicate, other), version=version)

    assert {value.id for value in baseline.items} == {root.id, other.id}
    assert duplicated.items == baseline.items
    assert duplicated.tokens == baseline.tokens
    assert duplicated.citations == baseline.citations


@pytest.mark.parametrize("version", ("v4", "v5"))
def test_allocated_context_groups_remain_source_bound_at_final_delivery(tmp_path, version):
    sources, candidates, answers = {}, [], []
    for number in range(2):
        question = f"user: Cobalt count for parcel {number} needs the following restriction.\n"
        first = question + "assistant: " + BACKGROUND + "\n"
        answer = f"assistant: Cobalt count is {12 + number}, never {21 + number}."
        second = answer + "\nuser: " + BACKGROUND
        source = first + second
        identity = f"parcel-{number}"
        anchor = replace(
            indexed_item(source, 0, len(first), identity=identity), score=1 - number / 100
        )
        # Include the preceding newline so the first child turn has a proven
        # boundary, rather than the existing conservative cut-edge ambiguity.
        neighbor = context_child(
            anchor, indexed_item(source, len(first) - 1, len(source), identity=identity)
        )
        sources[identity] = source
        answers.append(answer)
        candidates.extend((anchor, neighbor))
    query = request(4)
    packed = pack_native_evidence(query, (), tuple(candidates), version=version)

    assert all(any(answer in value.text for value in packed.items) for answer in answers)
    assert {value.id for value in packed.items} == set(sources)
    groups = packed.metadata["packing_dependency_groups"]
    retained, complete = retain_complete_groups(packed.items, groups)
    assert retained == packed.items and complete == tuple(groups)
    assert len(groups) == 2 and packed.tokens <= query.token_budget

    asyncio.run(validate_delivery(tmp_path, sources, query, packed))


@pytest.mark.parametrize("version", ("v4", "v5"))
def test_expanded_context_group_refreshes_proof_before_final_delivery(tmp_path, version):
    first = "user: Cobalt count needs its restriction.\nassistant: " + BACKGROUND + "\n"
    answer = "assistant: Cobalt count is 12, never 21."
    source = first + answer + "\nuser: " + BACKGROUND
    anchor = indexed_item(source, 0, len(first), identity="event")
    neighbor = context_child(
        anchor,
        indexed_item(source, len(first) - 1, len(source), identity="event"),
    )
    _, independent_sources, others = records(2)
    others = tuple(replace(value, score=0.8 - i / 10) for i, value in enumerate(others))
    query = request(3, budget=96)
    packed = pack_native_evidence(query, (), (anchor, neighbor, *others), version=version)

    trace = packed.metadata["packing_selection_trace"]
    grown = [
        row
        for row in trace
        if row.get("dependency_group_id") and row["context_expansion_tokens"] > 0
    ]
    assert grown  # Exercise proof rebuilding after actual member growth.
    assert all(row["tokens"] > row["initial_tokens"] for row in grown)
    assert any(answer in value.text for value in packed.items)
    groups = packed.metadata["packing_dependency_groups"]
    retained, complete = retain_complete_groups(packed.items, groups)
    assert retained == packed.items and complete == tuple(groups)
    assert len(groups) == 1
    assert packed.tokens == sum(token_estimate(value.text) for value in packed.items)
    assert packed.tokens <= query.token_budget
    sources = {
        "event": source,
        **{f"parcel-{i}": text for i, text in enumerate(independent_sources)},
    }
    asyncio.run(validate_delivery(tmp_path, sources, query, packed))


@pytest.mark.parametrize("version", ("v4", "v5"))
def test_group_refit_does_not_reselect_a_fully_covered_smaller_variant(version):
    # The broad candidate has useful new coordinates, but its cheaper variant
    # overlaps an earlier selection. Refitting must not create an empty unit.
    source = "Cobalt count remains documented. " * 30
    anchor = indexed_item(source, 0, 330, identity="shared")
    broad = replace(indexed_item(source, 0, len(source), identity="shared"), score=0.99)
    independent = (
        "Cobalt count follows " + "the required verification " * 10 + "and never otherwise."
    )
    other = replace(
        indexed_item(independent, 0, len(independent), identity="independent"), score=0.5
    )
    packed = pack_native_evidence(request(3), (), (anchor, broad, other), version=version)

    covered = set()
    for value in packed.items:
        if value.id != "shared":
            continue
        span = value.metadata["source_span"]
        coordinates = set(range(span["start"], span["end"]))
        assert coordinates - covered
        assert value.text == source[span["start"] : span["end"]]
        covered.update(coordinates)
    assert len(covered) > 330
    assert packed.tokens == sum(token_estimate(value.text) for value in packed.items) <= 256
