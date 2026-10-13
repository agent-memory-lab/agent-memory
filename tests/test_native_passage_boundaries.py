"""General passage-integrity regressions for bounded indexed EVENT evidence."""

from dataclasses import replace

import pytest
from test_indexed_event_packing import SCOPE, assert_source_lineage, indexed_item

from agent_memory.domain import MemoryKind, MemoryQuery
from agent_memory.retrieval.candidate_identity import candidate_identity
from agent_memory.retrieval.packing import _indexed_event_variants, pack_native_evidence


def test_remaining_budget_does_not_turn_source_into_word_fragments():
    full = "The archive badge is not reusable until the supervisor authorizes it."
    event = replace(indexed_item(full, 0, len(full)), score=0.8)
    first = replace(
        indexed_item("archive " + "x" * 240, 0, 248, identity="first"),
        kind=MemoryKind.CLAIM,
    )
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive badge", limit=8, token_budget=64), (), (first, event),
    )
    assert packed.items == (first,)
    assert packed.tokens == 62
    assert packed.metadata["packing_infeasible"] == 1


def test_a_whole_short_source_remains_valid_even_at_the_end_of_the_budget():
    first = replace(
        indexed_item("archive " + "x" * 244, 0, 252, identity="first"),
        kind=MemoryKind.CLAIM,
    )
    short = replace(indexed_item("No.", 0, 3), score=0.8)
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive", limit=8, token_budget=64), (), (first, short),
    )
    assert packed.items == (first, short)
    assert packed.tokens == 64


def test_complete_sentence_preserves_negation_decimals_symbols_and_abbreviation():
    target = "Dr. Ada did not approve $25.50; the invoice remains €30.75."
    source = "Routine calibration is complete. " * 30 + target + " Shipment is pending." * 30
    original = indexed_item(source, 0, len(source))
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "Ada invoice approve", token_budget=64), (), (original,),
    )
    assert packed.items
    assert target in packed.items[0].text
    assert packed.items[0].text.endswith(".")
    assert packed.items[0].metadata["passage_boundary"] == "sentence_or_line"
    assert_source_lineage(packed.items[0], original, source)


def test_dialogue_line_is_not_reduced_to_its_answer_token():
    source = (
        "assistant: " + "Routine calibration guidance. " * 55
        + "\nuser: My archive badge expires on Friday, not Monday.\n"
        + "assistant: " + "Please check the calendar. " * 30
    )
    original = indexed_item(source, 0, len(source))
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive badge expires", token_budget=64), (), (original,),
    )
    assert "user: My archive badge expires on Friday, not Monday." in packed.items[0].text
    assert_source_lineage(packed.items[0], original, source)


def test_partial_parent_edges_are_not_claimed_as_complete_sentence_boundaries():
    source = (
        "unrelated prefix is unfinished " * 8 + ". "
        + "The archive badge expires on Friday, not Monday. "
        + "An unfinished trailing description " * 20
    )
    start, end = 17, len(source) - 13
    original = indexed_item(source, start, end)
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive badge", token_budget=64), (), (original,),
    )
    assert packed.items[0].text == "The archive badge expires on Friday, not Monday."
    assert_source_lineage(packed.items[0], original, source)


@pytest.mark.parametrize("query", ("存放位置", "", "47"))
def test_unpunctuated_han_has_explicit_bounded_fallback(query):
    source = "设备档案存放位置需要核验" * 40 + "47"
    original = indexed_item(source, 0, len(source))
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, query, token_budget=64), (), (original,),
    )
    assert packed.items
    assert len(packed.items[0].text) >= 64
    assert packed.items[0].metadata["passage_boundary"] == "unbroken_min64"
    assert_source_lineage(packed.items[0], original, source)
    assert packed.tokens <= 64


def test_long_single_sentence_is_skipped_instead_of_emitting_a_suffix():
    source = "The archive badge " + "requires explicit authorization " * 30 + "."
    original = indexed_item(source, 0, len(source))
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive badge", token_budget=64), (), (original,),
    )
    assert packed.items == ()
    assert packed.tokens == 0


def test_unpunctuated_identifier_is_not_split_to_fill_a_budget():
    source = "archive_" * 100
    original = indexed_item(source, 0, len(source))
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive", token_budget=64), (), (original,),
    )
    assert packed.items == ()


def test_variants_are_bounded_contiguous_and_keep_candidate_identity():
    source = "The archive remains locked. 档案尚未获准开放。" * 70
    original = indexed_item(source, 0, len(source))
    variants = _indexed_event_variants(original, "archive 档案")
    assert 1 < len(variants) <= 13
    for value in variants:
        assert_source_lineage(value, original, source)
        assert candidate_identity(value) == candidate_identity(original)
        assert value.text.endswith((".", "。"))


def test_smallest_complete_sentence_is_not_skipped_by_geometric_sizes():
    target = (
        "The archive badge expires Friday; this authorization must not be treated as final."
    )
    source = "Note. " * 100 + target + " Note." * 100
    original = replace(indexed_item(source, 0, len(source)), score=0.8)
    first = replace(
        indexed_item("archive " + "x" * 164, 0, 172, identity="first"),
        kind=MemoryKind.CLAIM,
    )
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive badge expires", token_budget=64), (), (first, original),
    )
    assert packed.items[0] == first
    assert packed.items[1].text == target
    assert packed.tokens <= 64
    assert_source_lineage(packed.items[1], original, source)


def test_numbered_list_marker_cannot_fill_the_last_budget_token():
    source = (
        "Routine checks remain pending.\n"
        "8. The archive badge requires supervisor approval before it can be reused.\n"
        "9. The invoice total remains $30.75."
    )
    event = replace(indexed_item(source, 0, len(source)), score=0.8)
    first = replace(
        indexed_item("archive " + "x" * 244, 0, 252, identity="first"),
        kind=MemoryKind.CLAIM,
    )
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive 8", token_budget=64), (), (first, event),
    )
    assert packed.items == (first,)
    assert packed.tokens == 63
    variants = _indexed_event_variants(event, "archive 8")
    assert all(value.text.strip() != "8." for value in variants)
    assert any("8. The archive badge" in value.text for value in variants)


def test_a_standalone_numbered_line_is_not_a_narrowed_semantic_unit():
    source = "Routine checks remain pending.\n8.\nThe archive badge is locked."
    original = indexed_item(source, 0, len(source))
    assert all(
        value.text.strip() != "8."
        for value in _indexed_event_variants(original, "8")
    )
    short = indexed_item("8.", 0, 2)
    assert pack_native_evidence(
        MemoryQuery(SCOPE, "8", token_budget=64), (), (short,),
    ).items == (short,)


def test_narrowing_preserves_the_speaker_and_the_turns_hypothetical_qualification():
    source = (
        "user: I have not approved any archive access.\n"
        "assistant: This is an example only. I approved archive access yesterday. "
        + "Routine calibration remains pending. " * 60
    )
    event = replace(indexed_item(source, 0, len(source)), score=.8)
    text = "Archive " + "x" * 145
    first = replace(
        indexed_item(text, 0, len(text), identity="claim"), kind=MemoryKind.CLAIM,
    )
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "approved archive access", limit=2, token_budget=64),
        (), (first, event),
    )
    assert packed.items[0] == first
    for value in packed.items[1:]:
        assert value.text.startswith(("user:", "assistant:"))
        if "I approved archive access yesterday." in value.text:
            assert value.text.startswith("assistant: This is an example only.")
        assert_source_lineage(value, event, source)
    for value in _indexed_event_variants(event, "approved archive access"):
        assert value.text.startswith(("user:", "assistant:"))


@pytest.mark.parametrize("role", ("user", "assistant", "tool"))
def test_visible_turn_prefix_is_retained_even_without_sentence_punctuation(role):
    source = f"{role}: " + "archive audit " * 100
    original = indexed_item(source, 0, len(source))
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive audit", token_budget=64), (), (original,),
    )
    assert packed.items == ()


def test_timestamped_turn_prefix_is_preserved_without_inventing_a_new_quote():
    header = "[2026-10-01T09:00:00+00:00] user: "
    source = (
        header + "This example is fictional. The archive badge expires Friday. "
        + "Routine notes. " * 80
    )
    original = indexed_item(source, 0, len(source))
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive badge", token_budget=64), (), (original,),
    )
    assert packed.items and packed.items[0].text.startswith(header)
    assert "This example is fictional." in packed.items[0].text
    assert packed.items[0].metadata["passage_boundary"] == "dialogue_turn_prefix"
    assert_source_lineage(packed.items[0], original, source)


def test_inline_role_word_does_not_turn_unstructured_prose_into_a_dialogue():
    target = "The log mentions assistant: archive access is pending, not approved."
    source = "Routine calibration is complete. " * 30 + target + " Checks continue." * 30
    original = indexed_item(source, 0, len(source))
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive access approved", token_budget=64), (), (original,),
    )
    assert target in packed.items[0].text
    assert packed.items[0].metadata["passage_boundary"] == "sentence_or_line"


def test_a_bare_dialogue_header_is_not_used_to_fill_the_remaining_budget():
    source = "assistant:\n" + "This hypothetical archive authorization is not effective. " * 30
    original = replace(indexed_item(source, 0, len(source)), score=.8)
    text = "Archive " + "x" * 233
    first = replace(
        indexed_item(text, 0, len(text), identity="claim"), kind=MemoryKind.CLAIM,
    )
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive", token_budget=64), (), (first, original),
    )
    assert packed.items == (first,)


def test_legacy_complete_event_cannot_bypass_the_explicit_turn_prefix_rule():
    source = (
        "user: The archive is still locked.\n"
        "assistant: This is hypothetical. The archive access was approved yesterday. "
        + "Routine checks remain pending. " * 60
    )
    original = replace(
        indexed_item(source, 0, len(source)), metadata={"source_event_ids": ("event",)},
    )
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "archive access approved", token_budget=128), (), (original,),
    )
    assert packed.items
    value = packed.items[0]
    assert value.text.startswith(("user:", "assistant:"))
    if "The archive access was approved yesterday." in value.text:
        assert "assistant: This is hypothetical." in value.text
    assert value.metadata["passage_boundary"] == "dialogue_turn_prefix"
    span = value.metadata["source_span"]
    assert value.text == source[span["start"]:span["end"]]
