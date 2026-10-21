"""Independent facts and source-bound context compete under real item budgets."""
from dataclasses import replace
import asyncio
import json
from uuid import uuid4

import pytest
from test_evidence_coverage_packing import item
from test_indexed_event_packing import SCOPE, indexed_item

from agent_memory.domain import MemoryKind, MemoryQuery
from agent_memory.retrieval.candidate_identity import candidate_identity
from agent_memory.retrieval.packing import pack_native_evidence
from agent_memory import MemoryEvent, build_local_kernel
from agent_memory.retrieval.evidence_units import dependency_group, retain_complete_groups
from agent_memory.retrieval.evidence_units import context_relations


def dialogue(*, score=.01):
    first = "user: The obsidian depot location is in the next message.\n"
    second = "assistant: It is at Cedar Square, never at home."
    text = first + second
    anchor = indexed_item(text, 0, len(first), identity="dialogue")
    neighbor = replace(indexed_item(text, len(first), len(text), identity="dialogue"), score=score)
    neighbor = replace(neighbor, metadata={**neighbor.metadata, "context_only": True,
        "relevance_origin": "inherited_context_anchor",
        "context_anchor_candidate_id": candidate_identity(anchor)})
    return anchor, neighbor


def test_context_dependency_is_selected_with_its_anchor_ahead_of_topical_noise():
    anchor, neighbor = dialogue()
    noise = item("topic", "Obsidian depot location remains a subject of discussion.",
                 kind=MemoryKind.CLAIM, score=.99, sources=("other",))
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "Where is the obsidian depot?", limit=2, token_budget=128),
        (), (anchor, noise, neighbor), version="v4",
    )
    assert [value.text for value in packed.items] == [anchor.text, neighbor.text]
    assert packed.tokens <= 128 and len(packed.items) == 2
    assert len(packed.metadata["packing_dependency_groups"]) == 1
    assert packed.metadata["coverage"] == "partial"


def test_infeasible_dependency_does_not_deliver_an_unanchored_neighbor():
    anchor, neighbor = dialogue(score=999)
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "Where is the obsidian depot?", limit=1, token_budget=64),
        (), (anchor, neighbor), version="v4",
    )
    assert packed.items == (anchor,)
    assert packed.metadata["packing_dependency_deferred"] == 1


@pytest.mark.parametrize("change", [
    {"context_anchor_candidate_id": "missing"},
    {"source_revision": "a" * 64},
    {"source_event_ids": ("foreign",)},
    {"source_event_ids": None},
    {"context_anchor_candidate_id": []},
    {"relevance_origin": "unverified"},
])
def test_invalid_context_relationship_cannot_inherit_an_anchor_score(change):
    anchor, neighbor = dialogue(score=999)
    neighbor = replace(neighbor, metadata={**neighbor.metadata, **change})
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "obsidian depot", limit=2, token_budget=128),
        (), (anchor, neighbor), version="v4",
    )
    assert packed.items == (anchor,)
    assert packed.metadata["packing_dependency_rejected"] == 1


@pytest.mark.parametrize("topic", ["canal inspection", "苔藓观测", "lunar relay"])
def test_same_topic_different_events_and_corrections_survive_competition(topic):
    facts = (
        item("first", f"{topic}: first count was 14.", kind=MemoryKind.CLAIM),
        item("second", f"{topic}: second count was 14.", kind=MemoryKind.CLAIM, score=.99),
        item("correction", f"{topic}: correction, the first count was not 14 but 19.",
             kind=MemoryKind.CLAIM, score=.98),
    )
    noise = item("overview", f"{topic} count review and comparison are under discussion.",
                 kind=MemoryKind.CLAIM, score=.94)
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, f"Compare every {topic} count and correction", limit=3, token_budget=128),
        (), (*facts, noise), version="v4",
    )
    assert {value.id for value in packed.items} == {fact.id for fact in facts}
    assert len(packed.citations) == 3


def test_semantic_excerpt_does_not_receive_a_length_based_relevance_penalty():
    answer = "The parcel was dispatched through Willow Station, never from my house."
    text = answer + " The transaction ledger is archived." * 40
    source = indexed_item(text, 0, len(text))
    noise = item("question-only", "Where did I send the package? The notes need review.",
                 kind=MemoryKind.CLAIM, score=.6)
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "Where did I send the package?", limit=1, token_budget=64),
        (), (source, noise), version="v4",
    )
    assert packed.items and answer in packed.items[0].text
    trace = packed.metadata["packing_selection_trace"][0]
    assert trace["relevance_basis"] == "unverified_parent_bound"
    assert packed.metadata["packing_passage_relevance_verified"] is False


@pytest.mark.parametrize("lost_member", [0, 1])
def test_final_source_authority_loss_removes_the_whole_dependency_unit(tmp_path, monkeypatch, lost_member):
    async def run():
        anchor, neighbor = dialogue()
        anchor = replace(anchor, metadata={**anchor.metadata, "event_type": "user.message"})
        neighbor = replace(neighbor, metadata={**neighbor.metadata, "event_type": "user.message"})
        kernel = build_local_kernel(tmp_path / "memory.db", packing_version="v4")
        await kernel.initialize()
        try:
            await kernel.ingest_event(MemoryEvent(SCOPE, "user.message", anchor.text+neighbor.text, id="dialogue"))
            async def search(*args):
                return (anchor,)
            async def expand(*args):
                return (neighbor,)
            monkeypatch.setattr(kernel._repository, "search", search)
            monkeypatch.setattr(kernel._repository, "expand_source_context", expand)
            query = MemoryQuery(SCOPE, "obsidian depot", limit=2, token_budget=128,
                                include_current_state=False)
            bundle = await kernel.retrieve(query)
            assert len(bundle.relevant_memories) == 2
            members = list(bundle.relevant_memories)
            members[lost_member] = replace(members[lost_member], text="x"+members[lost_member].text[1:])
            changed = replace(bundle, relevant_memories=tuple(members))
            final = await kernel.guard_pipeline_delivery(replace(query, request_id=str(uuid4())), changed)
            assert not final.relevant_memories
            assert not final.citations and final.token_estimate == 0
            assert final.retrieval_metadata["packing_dependency_groups"] == ()
            assert final.retrieval_metadata["packing_dependency_delivery_dropped"] == 1
        finally:
            await kernel.close()
    asyncio.run(run())


def test_dependency_filter_closes_over_overlapping_groups_in_either_order():
    a, b = dialogue()
    c = replace(b, id="other")
    groups = (dependency_group((a, b)), dependency_group((b, c)))
    for order in (groups, groups[::-1]):
        retained, complete = retain_complete_groups((b, c), order)
        assert retained == () and complete == ()


def test_context_with_no_declared_group_is_not_delivered_standalone():
    anchor, neighbor = dialogue()
    retained, complete = retain_complete_groups((anchor, neighbor), ())
    assert retained == (anchor,) and complete == ()


def test_a_fitting_context_pair_is_not_blocked_by_a_second_neighbor():
    previous = "assistant: Earlier depot records remain available.\n"
    middle = "user: The obsidian depot location is in the next message.\n"
    following = "assistant: It is at Cedar Square."
    text = previous + middle + following
    anchor = indexed_item(text, len(previous), len(previous+middle), identity="three-turns")
    children = []
    for start, end in ((0, len(previous)), (len(previous+middle), len(text))):
        child = indexed_item(text, start, end, identity="three-turns")
        children.append(replace(child, metadata={**child.metadata, "context_only": True,
            "relevance_origin": "inherited_context_anchor",
            "context_anchor_candidate_id": candidate_identity(anchor)}))
    # An item cap of two cannot admit all three representations, even though
    # each separately fits. A complete anchor+neighbour pair remains feasible.
    packed = pack_native_evidence(
        MemoryQuery(SCOPE, "obsidian depot", limit=2, token_budget=128),
        (), (anchor, *children), version="v4",
    )
    assert len(packed.items) == 2 and len(packed.metadata["packing_dependency_groups"]) == 1
    assert packed.items[0] == anchor and packed.items[1] == children[0]
    assert packed.metadata["packing_dependency_deferred"] == 1


@pytest.mark.parametrize("overlap", [False, True])
def test_disconnected_or_inconsistent_source_windows_do_not_form_a_unit(overlap):
    source = "The depot address is listed. Earlier paperwork is archived. Cedar Square is the address."
    anchor = indexed_item(source, 0, 35)
    start = 25 if overlap else 45
    child = indexed_item(source, start, len(source))
    if overlap:
        child = replace(child, text="x"+child.text[1:])
    child = replace(child, metadata={**child.metadata, "context_only": True,
        "relevance_origin": "inherited_context_anchor",
        "context_anchor_candidate_id": candidate_identity(anchor)})
    relations = context_relations((anchor, child))
    assert not relations.parents and relations.rejected == 1


def test_empty_dependency_group_never_restores_or_invents_evidence():
    anchor, _ = dialogue()
    retained, complete = retain_complete_groups((anchor,), ({"members": ()},))
    assert retained == (anchor,) and complete == ()


def test_dependency_proofs_survive_json_serialization_without_losing_valid_evidence():
    anchor, neighbor = dialogue()
    groups = json.loads(json.dumps([dependency_group((anchor, neighbor))]))
    retained, complete = retain_complete_groups((anchor, neighbor), groups)
    assert retained == (anchor, neighbor) and len(complete) == 1
