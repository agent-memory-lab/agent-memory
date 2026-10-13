"""Dataset integrity and isolation tests; no credentials or model calls required."""

import json
import random
from dataclasses import asdict, fields, replace

import pytest

from experiments.evidence_obligations.dataset import (
    Episode,
    GoldAnswer,
    Query,
    SourceEvent,
    SourceRecord,
    apply_source_event,
    build_dataset,
    runtime_payload,
)


def all_sources(episodes):
    return [
        source
        for episode in episodes
        for source in (
            *episode.initial_sources,
            *(step.source for step in episode.steps if isinstance(step, SourceEvent)
              and step.source is not None),
        )
    ]


def test_seeded_build_is_reproducible_and_does_not_touch_global_random():
    original = random.getstate()
    first = build_dataset(17)
    assert first == build_dataset(17)
    assert first != build_dataset(18)
    assert random.getstate() == original
    assert json.dumps(asdict(first), sort_keys=True) == json.dumps(
        asdict(build_dataset(17)), sort_keys=True
    )


@pytest.mark.parametrize("bad_seed", [True, None, "17", 17.5])
def test_seed_must_be_integer(bad_seed):
    with pytest.raises(TypeError, match="integer"):
        build_dataset(bad_seed)


@pytest.mark.parametrize("seed", [0, 17, 991, -3])
def test_splits_sources_entities_and_query_ids_are_disjoint(seed):
    dataset = build_dataset(seed)
    assert len(dataset.dev) == 6
    assert len(dataset.heldout) == 8
    dev_sources = {source.source_id for source in all_sources(dataset.dev)}
    heldout_sources = {source.source_id for source in all_sources(dataset.heldout)}
    assert not dev_sources & heldout_sources
    assert len(dev_sources) == 6 * 6
    assert len(heldout_sources) == 8 * 6
    assert not ({episode.source_group for episode in dataset.dev}
                & {episode.source_group for episode in dataset.heldout})
    assert all(episode.split == "dev" for episode in dataset.dev)
    assert all(episode.split == "heldout" for episode in dataset.heldout)
    query_ids = [query.query_id for episode in dataset.episodes for query in episode.queries]
    assert len(query_ids) == len(set(query_ids)) == 140
    assert set(query_ids) == set(dataset.gold)
    assert len({episode.episode_id for episode in dataset.episodes}) == 14
    # Distinct alphabetic project names, not merely source ID namespaces.
    dev_text = " ".join(source.text for source in all_sources(dataset.dev))
    heldout_text = " ".join(source.text for source in all_sources(dataset.heldout))
    for name in ("Aster", "Birch", "Cedar", "Dahlia", "Elm", "Fennel"):
        assert name + "-" in dev_text and name + "-" not in heldout_text
    for name in ("Garnet", "Hazel", "Indigo", "Juniper", "Kestrel", "Linden", "Maple", "Nectar"):
        assert name + "-" in heldout_text and name + "-" not in dev_text


def test_runtime_schema_and_serialization_do_not_contain_gold_or_oracle_slots():
    assert {field.name for field in fields(Query)} == {"query_id", "text", "agent_id"}
    assert {field.name for field in fields(SourceRecord)} == {
        "source_id", "source_group", "revision", "text",
    }
    assert {field.name for field in fields(Episode)} == {
        "episode_id", "split", "source_group", "initial_sources", "steps",
    }
    dataset = build_dataset()
    forbidden = {
        "gold", "expected_values", "required_values", "required_qualifiers", "answerable",
        "supporting_source_ids", "supporting_source_revisions", "category", "task_key",
        "required_slots", "facts", "lexical_rank", "gap", "route",
    }

    def assert_clean(value):
        if isinstance(value, dict):
            assert not (set(value) & forbidden)
            for item in value.values():
                assert_clean(item)
        elif isinstance(value, list):
            for item in value:
                assert_clean(item)

    for episode in dataset.episodes:
        payload = json.loads(json.dumps(runtime_payload(episode)))
        assert_clean(payload)
        assert all(step["type"] in {"query", "source_event"} for step in payload["steps"])
    with pytest.raises(TypeError, match="Episode"):
        runtime_payload(dataset)


def test_gold_is_independent_from_runtime_payload_and_treatment_output():
    dataset = build_dataset()
    before = [runtime_payload(episode) for episode in dataset.episodes]
    for query_id in list(dataset.gold):
        dataset.gold[query_id] = GoldAnswer(query_id, ("POISON",), (), True, (), "POISON")
    assert before == [runtime_payload(episode) for episode in dataset.episodes]
    assert all("POISON" not in json.dumps(payload) for payload in before)


def test_gold_matches_current_accessible_sources_and_never_stale_revisions():
    dataset = build_dataset()
    for episode in dataset.episodes:
        corpus = {source.source_id: source for source in episode.initial_sources}
        for step in episode.steps:
            if isinstance(step, SourceEvent):
                apply_source_event(corpus, step)
                continue
            gold = dataset.gold[step.query_id]
            assert gold.query_id == step.query_id
            if not gold.answerable:
                assert gold.required_values == gold.required_qualifiers == ()
                assert gold.supporting_source_ids == gold.supporting_source_revisions == ()
                if gold.category.startswith(("source_deleted", "source_revoked")):
                    assert not any("bulletin, revision" in source.text
                                   for source in corpus.values())
                continue
            assert gold.required_values and gold.required_qualifiers
            assert tuple(corpus[source_id].revision for source_id in gold.supporting_source_ids)
            assert gold.supporting_source_revisions == tuple(
                (source_id, corpus[source_id].revision) for source_id in gold.supporting_source_ids
            )
            text = " ".join(corpus[source_id].text for source_id in gold.supporting_source_ids)
            assert all(value.casefold() in text.casefold()
                       for value in gold.required_values + gold.required_qualifiers)


def test_first_dev_episode_has_full_lifecycle_and_narrow_invalidation_control():
    dataset = build_dataset()
    expected_categories = [
        "initial_gap", "exact_repeat", "paraphrase", "cross_agent", "unsupported",
        "unsupported_repeat", "irrelevant_update", "source_update",
        "source_update_cross_agent", "source_deleted", "source_deleted_repeat",
        "source_restored", "source_revoked", "source_revoked_repeat",
    ]
    for episode in dataset.dev[:1]:
        queries = episode.queries
        gold = [dataset.gold[query.query_id] for query in queries]
        assert [answer.category for answer in gold] == expected_categories
        assert queries[0].text == queries[1].text == queries[6].text == queries[7].text
        assert queries[2].text != queries[0].text
        assert queries[2].text == queries[3].text
        assert queries[2].agent_id != queries[3].agent_id
        assert queries[4].text != queries[5].text
        assert gold[0].required_values == gold[6].required_values
        assert gold[0].required_qualifiers == gold[6].required_qualifiers
        assert gold[0].supporting_source_revisions == gold[6].supporting_source_revisions
        assert gold[0].required_values != gold[7].required_values != gold[11].required_values
        assert gold[0].required_qualifiers != gold[7].required_qualifiers
        assert [answer.answerable for answer in gold] == [
            True, True, True, True, False, False, True, True, True, False, False,
            True, False, False,
        ]
        events = [step for step in episode.steps if isinstance(step, SourceEvent)]
        assert [event.kind for event in events] == [
            "upsert", "upsert", "delete", "upsert", "revoke",
        ]
        assert events[0].source_id not in gold[0].supporting_source_ids
        assert all(event.source_id in gold[0].supporting_source_ids for event in events[1:])
        assert events[1].source.revision == 2
        assert events[3].source.revision == 3


def test_initial_corpus_has_partial_digest_and_complete_independent_bulletin():
    dataset = build_dataset()
    for episode in dataset.episodes:
        complete = next(source for source in episode.initial_sources
                        if "Current authoritative bulletin" in source.text)
        digest = next(source for source in episode.initial_sources
                      if "directory FAQ:" in source.text)
        assert complete.source_id != digest.source_id
        assert "static, nonbinding snapshot" in digest.text
        assert "detailed restrictions" in digest.text
        assert len(episode.initial_sources) == 6
        assert all(source.source_group == episode.source_group
                   for source in episode.initial_sources)
        # At least one supported gold state in revision 1 is exercised wherever
        # a query is asked before mutation; those states require the omitted qualifier.
        initial_gold = [dataset.gold[query.query_id] for query in episode.queries
                        if dataset.gold[query.query_id].supporting_source_revisions
                        == ((complete.source_id, 1),)]
        for gold in initial_gold:
            assert all(value in digest.text for value in gold.required_values)
            assert all(value in complete.text for value in gold.required_values)
            assert all(qualifier in complete.text for qualifier in gold.required_qualifiers)
            assert not any(qualifier in digest.text for qualifier in gold.required_qualifiers)


def test_varied_domains_are_not_owner_only():
    dataset = build_dataset()
    all_questions = " ".join(
        query.text for episode in dataset.episodes for query in episode.queries
    )
    for word in ("refund", "due", "retain", "budget", "escalated", "processing",
                 "pass rate", "reimbursement", "maintenance", "response-time", "tenure",
                 "reorder", "export"):
        assert word in all_questions


@pytest.mark.parametrize("kind", ["delete", "revoke"])
def test_remove_events_are_idempotent_and_do_not_carry_removed_text(kind):
    source = SourceRecord("s", "g", 1, "source contents")
    corpus = {source.source_id: source}
    event = SourceEvent(kind, source.source_id)
    apply_source_event(corpus, event)
    apply_source_event(corpus, event)
    assert corpus == {}
    assert "source contents" not in json.dumps(asdict(event))
    with pytest.raises(ValueError, match="must not carry"):
        SourceEvent(kind, source.source_id, source)


def test_upsert_validation_rejects_wrong_identity_non_increasing_revision_and_group_move():
    source = SourceRecord("s", "g", 1, "old")
    with pytest.raises(ValueError, match="matching"):
        SourceEvent("upsert", "s")
    with pytest.raises(ValueError, match="matching"):
        SourceEvent("upsert", "wrong", source)
    with pytest.raises(ValueError, match="Unknown"):
        SourceEvent("silently-forget", "s")
    corpus = {source.source_id: source}
    with pytest.raises(ValueError, match="increase"):
        apply_source_event(corpus, SourceEvent("upsert", "s", source))
    with pytest.raises(ValueError, match="groups"):
        apply_source_event(corpus, SourceEvent("upsert", "s", replace(source, source_group="h")))
    new = replace(source, revision=2, text="new")
    apply_source_event(corpus, SourceEvent("upsert", "s", new))
    assert corpus["s"] == new


def test_heldout_has_varied_causal_ordering_and_control_only_episodes():
    dataset = build_dataset()
    signatures = {
        tuple("q" if isinstance(step, Query) else step.kind for step in episode.steps)
        for episode in dataset.heldout
    }
    assert len(signatures) == 8
    assert all(isinstance(step, Query) for step in dataset.heldout[1].steps)
    irrelevant_only = [step for step in dataset.heldout[2].steps if isinstance(step, SourceEvent)]
    assert len(irrelevant_only) == 1
    assert irrelevant_only[0].source_id not in dataset.gold[
        dataset.heldout[2].queries[0].query_id
    ].supporting_source_ids
    assert isinstance(dataset.heldout[3].steps[1], SourceEvent)
    assert dataset.heldout[3].steps[1].kind == "delete"
    assert dataset.heldout[4].steps[0].kind == "upsert"
    assert dataset.heldout[5].steps[0].kind == "revoke"
    assert dataset.heldout[7].steps[0].kind == "delete"
    assert dataset.heldout[6].queries[0].agent_id == "colleague"
    assert len({len(episode.queries) for episode in dataset.heldout}) >= 4
