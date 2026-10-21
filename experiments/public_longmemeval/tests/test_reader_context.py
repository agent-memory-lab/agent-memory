"""Synthetic evidence only: validate native delivery without model or gold data."""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from json import dumps, loads
from types import SimpleNamespace

import pytest

from agent_memory import Citation, MemoryItem, MemoryKind, MemoryScope
from agent_memory.retrieval.candidate_identity import candidate_identity
from agent_memory.retrieval.packing import token_estimate
from experiments.public_longmemeval.reader_context import (
    CORE_READER_MODE, LEGACY_READER_MODE, ReaderContextError, core_reader_context,
    legacy_pack, legacy_reader_context, reader_contract,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)
SCOPE = MemoryScope('authored-reader-fixture', user_id='synthetic', session_id='one')


def item(identity, text, *, kind=MemoryKind.CLAIM, sources=None, score=1.0):
    return MemoryItem(identity, kind, text, score, NOW,
                      {'source_event_ids': sources or ('event-'+identity,)})


def selected_bundle(items, *, order=None):
    """A selected native bundle with independently authored citation proofs."""
    order = items if order is None else order
    proofs = []
    for value in order:
        proof = {'memory_id': value.id, 'candidate_id': candidate_identity(value),
                 'source_event_ids': value.metadata['source_event_ids']}
        proof.update({name: value.metadata[name] for name in ('source_span', 'source_revision', 'source_chars') if name in value.metadata})
        proofs.append(proof)
    return SimpleNamespace(
        current_state=(), relevant_memories=tuple(x for x in items if x.kind not in (MemoryKind.EPISODE, MemoryKind.PROCEDURE)),
        episodes=tuple(x for x in items if x.kind == MemoryKind.EPISODE),
        procedures=tuple(x for x in items if x.kind == MemoryKind.PROCEDURE),
        token_estimate=sum(token_estimate(x.text) for x in items),
        citations=tuple(Citation(x.id, x.metadata['source_event_ids']) for x in order),
        retrieval_metadata={'citation_spans': tuple(proofs)},
    )


def source_map(items):
    return {source: 'session-'+source for value in items for source in value.metadata['source_event_ids']}


@pytest.mark.parametrize('version', ['v2', 'v3'])
def test_native_late_update_survives_previously_clipped_context(version):
    values = [item('early', 'Fictional project record. '+'a'*3400, score=.9),
              item('middle', 'Historical project note. '+'b'*2600, score=.8),
              item('latest', 'Recent project update: the revised fictional budget is 73 units.', score=.7)]
    # This regression exercises serialization of an already selected bundle.
    # Fix its citation order independently of evolving retrieval/ranking policy:
    # the final update must occur after the historical 6000-byte prefix limit.
    bundle = selected_bundle(values, order=values)
    packed = core_reader_context(bundle, source_map(values), version=version)
    old_context, old_delivered = legacy_pack(packed.candidates)
    assert len(packed.context.encode()) > 6000
    assert '73 units' in packed.context and '73 units' not in old_context
    assert [r['id'] for r in packed.delivered] == ['early', 'middle', 'latest']
    assert [r['text'] for r in packed.candidates] == [value.text for value in values]
    assert [r['text'] for r in packed.delivered] == [value.text for value in values]
    if version == 'v3':
        assert [block['text'] for block in loads(packed.context)] == [value.text for value in values]
        assert packed.diagnostics['text_preservation'] == 'verbatim_after_json_decode'
    else:
        assert packed.context == ''.join('- '+value.text+'\n' for value in values)
        assert packed.diagnostics['text_preservation'] == 'verbatim'
    assert packed.diagnostics['partial_count'] == packed.diagnostics['omitted_count'] == 0
    assert [r['id'] for r in old_delivered] == ['early', 'middle']
    assert old_delivered[-1]['partial'] is True
    assert len(old_delivered) < len(packed.delivered)


@pytest.mark.parametrize('text', ['z'*8192, '🧪'*2048, '界'*2048, 'e\u0301'*1024])
def test_utf8_extremes_exactly_preserve_valid_core_budget(text):
    values = [item('unicode', text)]
    packed = core_reader_context(selected_bundle(values), source_map(values))
    assert packed.delivered[0]['text'] == text
    assert packed.context == '- '+text+'\n'
    assert packed.diagnostics['context_utf8_bytes'] <= 8216
    assert packed.diagnostics['evidence_estimated_tokens'] <= 2048
    assert packed.diagnostics['framing_utf8_bytes'] == 3


def test_whole_bundle_budget_exceeded_fails_without_clipping():
    values = [item('one', 'x'*8193)]
    bundle = selected_bundle(values)
    original = deepcopy(bundle)
    with pytest.raises(ReaderContextError, match='core_token_budget_exceeded'):
        core_reader_context(bundle, source_map(values))
    assert bundle == original


def test_more_than_requested_items_fails_without_dropping_tail():
    values = [item(str(i), 'short exact evidence') for i in range(9)]
    with pytest.raises(ReaderContextError, match='core_item_limit_exceeded'):
        core_reader_context(selected_bundle(values), source_map(values))


def test_cross_kind_selection_order_not_resorted_by_score():
    values = [item('first', 'selected first', score=.1),
              item('episode', 'selected second', kind=MemoryKind.EPISODE, score=.3),
              item('last', 'selected last', score=.9)]
    bundle = selected_bundle(values, order=values)
    packed = core_reader_context(bundle, source_map(values))
    assert [row['id'] for row in packed.delivered] == ['first', 'episode', 'last']


def event_window(identity, full, start, end):
    revision = sha256(full.encode()).hexdigest()
    locator = sha256(dumps(['events', identity, revision, start, end]).encode()).hexdigest()
    return MemoryItem(identity, MemoryKind.EVENT, full[start:end], 1.0, NOW, {
        'source_event_ids': (identity,), 'source_revision': revision,
        'source_chars': len(full), 'lexical_chunk_id': locator,
        'source_span': {'start': start, 'end': end, 'unit': 'characters'},
    })


def test_same_public_id_distinct_spans_keep_text_citations_and_occurrence_identity():
    full = 'Initial fictional setting: copper. Updated fictional setting: silver.'
    split = full.index('Updated')
    first, second = event_window('same-event', full, 0, split), event_window('same-event', full, split, len(full))
    bundle = selected_bundle([first, second], order=[second, first])
    packed = core_reader_context(bundle, {'same-event': 'session-synthetic'})
    assert [row['text'] for row in packed.delivered] == [second.text, first.text]
    assert [row['id'] for row in packed.delivered] == ['same-event', 'same-event']
    assert len(set(row['candidate_id'] for row in packed.delivered)) == 2
    assert [row['source_span'] for row in packed.delivered] == [second.metadata['source_span'], first.metadata['source_span']]
    assert all(row['source_event_ids'] == ['same-event'] and row['source_ids'] == ['session-synthetic'] for row in packed.delivered)


def test_duplicate_identity_without_distinct_span_is_rejected():
    values = [item('same', 'one'), item('same', 'two')]
    with pytest.raises(ReaderContextError, match='ambiguous_duplicate_candidate_identity'):
        core_reader_context(selected_bundle(values), source_map(values))


@pytest.mark.parametrize('mutation,code', [
    ('tokens', 'core_token_estimate_mismatch'), ('token_bool', 'core_token_estimate_mismatch'),
    ('proof_missing', 'native_citation_proofs_missing_or_extra'), ('proof_extra', 'native_citation_proofs_missing_or_extra'),
    ('proof_identity', 'native_citation_order_or_identity_mismatch'),
    ('proof_memory', 'native_citation_evidence_mismatch'), ('proof_source', 'native_citation_evidence_mismatch'),
    ('citation_missing', 'bundle_citations_do_not_match_delivered_evidence'),
    ('citation_extra', 'bundle_citations_do_not_match_delivered_evidence'),
    ('foreign_source', 'citation_outside_captured_history'),
    ('state', 'current_state_not_supported'),
])
def test_budget_and_citation_corruption_fail_closed(mutation, code):
    values = [item('one', 'synthetic content')]
    bundle = selected_bundle(values)
    sources = source_map(values)
    if mutation == 'tokens': bundle.token_estimate += 1
    if mutation == 'token_bool': bundle.token_estimate = True
    if mutation == 'proof_missing': bundle.retrieval_metadata['citation_spans'] = ()
    if mutation == 'proof_extra': bundle.retrieval_metadata['citation_spans'] *= 2
    if mutation == 'proof_identity': bundle.retrieval_metadata['citation_spans'][0]['candidate_id'] = 'unreturned'
    if mutation == 'proof_memory': bundle.retrieval_metadata['citation_spans'][0]['memory_id'] = 'unreturned'
    if mutation == 'proof_source': bundle.retrieval_metadata['citation_spans'][0]['source_event_ids'] = ('foreign',)
    if mutation == 'citation_missing': bundle.citations = ()
    if mutation == 'citation_extra': bundle.citations += (Citation('unreturned', ('event-one',)),)
    if mutation == 'foreign_source': sources = {}
    if mutation == 'state': bundle.current_state = (object(),)
    with pytest.raises(ReaderContextError, match=code):
        core_reader_context(bundle, sources)


def test_span_proof_must_match_delivered_text_geometry():
    value = event_window('event', 'synthetic original content', 0, 9)
    corrupted = replace(value, text='wrong-sized content')
    with pytest.raises(ReaderContextError, match='span_geometry_invalid'):
        core_reader_context(selected_bundle([corrupted]), {'event': 'session'})


def test_source_proof_not_overwritten_by_public_id_dictionary():
    full = 'one scene. two scenes.'
    first, second = event_window('one-event', full, 0, 10), event_window('one-event', full, 11, len(full))
    bundle = selected_bundle([first, second])
    bundle.retrieval_metadata['citation_spans'][1]['source_span'] = dict(first.metadata['source_span'])
    with pytest.raises(ReaderContextError, match='native_citation_span_mismatch'):
        core_reader_context(bundle, {'one-event': 'session'})


def test_legacy_utf8_partial_and_omission_are_explicit():
    rows = [{'id': 'a', 'text': '中'*10, 'source_ids': ['s-a']},
            {'id': 'b', 'text': 'not delivered', 'source_ids': ['s-b']}]
    packed = legacy_reader_context(rows, byte_budget=22)
    assert packed.context == '- '+'中'*6+'\n'
    assert len(packed.context.encode()) == 21
    assert packed.diagnostics['mode'] == LEGACY_READER_MODE
    assert packed.diagnostics['partial_count'] == 1 and packed.diagnostics['omitted_count'] == 1
    assert packed.diagnostics['selected'][0]['delivered_character_span']['end'] == 6
    assert packed.diagnostics['omitted_input_indices'] == [1]
    assert packed.delivered[0]['source_ids'] == ['s-a']
    assert rows[0]['text'] == '中'*10


def test_legacy_omission_indices_distinguish_repeated_public_ids():
    rows = [{'id': 'same', 'text': 'x'*17}, {'id': 'same', 'text': 'y'*17}]
    packed = legacy_reader_context(rows, byte_budget=22)
    assert packed.diagnostics['selected'][0]['input_index'] == 0
    assert packed.diagnostics['omitted_input_indices'] == [1]


def test_empty_bundle_is_explicit_and_never_fabricates_citations():
    result = core_reader_context(selected_bundle([]), {})
    assert result.context == '(no memory retrieved)'
    assert result.delivered == ()
    assert result.diagnostics['empty_context_marker'] is True
    assert result.diagnostics['evidence_estimated_tokens'] == 0


def test_contract_separates_raw_bytes_from_core_evidence_tokens():
    contract = reader_contract()
    assert contract['agent-memory']['mode'] == CORE_READER_MODE
    assert contract['agent-memory']['serialization_utf8_guard'] == 8216
    assert contract['raw-baselines']['context_utf8_byte_cap'] == 6000
    assert contract['cross_arm_budget_equivalence'] is False


def test_v3_labeled_source_dates_roles_and_same_event_windows_are_preserved():
    import json
    full = 'user: I used to repair clocks.\nassistant: Consider a new hobby.'
    first = event_window('same', full, 0, 29)
    second = event_window('same', full, 30, len(full))
    metadata = {'same':{'source_id':'session-synthetic','occurred_at':'2024-03-01T00:00:00+00:00',
                       'roles':['user','assistant']}}
    packed = core_reader_context(selected_bundle([second, first]), {'same':'session-synthetic'},
                                 version='v3', source_metadata=metadata)
    blocks = json.loads(packed.context)
    assert [b['text'] for b in blocks] == [second.text, first.text]
    assert [b['evidence_id'] for b in blocks] == ['E1','E2']
    assert blocks[0]['source_details'][0]['occurred_at'] == '2024-03-01T00:00:00+00:00'
    assert blocks[0]['source_details'][0]['speaker_scope'] == 'mixed_transcript'
    assert blocks[0]['source_span'] == second.metadata['source_span']
    assert packed.diagnostics['framing_in_core_budget'] is False
    assert packed.diagnostics['serialized_context_estimated_tokens'] == token_estimate(packed.context)
    assert packed.diagnostics['evidence_estimated_tokens'] == token_estimate(first.text) + token_estimate(second.text)
    assert packed.diagnostics['partial_count'] == packed.diagnostics['omitted_count'] == 0
    assert reader_contract('v3')['answer_reader']['schema_version'] == 'evidence-reader/3'
    assert reader_contract()['version'] == 'public-longmemeval-reader/2'


def test_v3_unknown_source_time_is_not_the_claim_creation_time():
    import json
    values = [item('claim', 'The user prefers quiet rooms.')]
    packed = core_reader_context(selected_bundle(values), source_map(values), version='v3')
    assert json.loads(packed.context)[0]['source_details'][0]['occurred_at'] == 'unknown'


def test_v3_source_annotations_must_match_authorized_citations():
    values = [item('claim', 'Synthetic statement')]
    with pytest.raises(ReaderContextError, match='source_metadata_identity_mismatch'):
        core_reader_context(selected_bundle(values), source_map(values), version='v3', source_metadata={
            'event-claim':{'source_id':'other-session','occurred_at':'2024-01-01T00:00:00+00:00','roles':['user']}})


@pytest.mark.parametrize('over_budget', [False, True])
def test_runner_delivers_large_native_bundle_without_legacy_second_pack(tmp_path, monkeypatch, over_budget):
    import asyncio
    import json
    from experiments.public_longmemeval import run_experiment as runner
    from experiments.public_longmemeval.tests.fixtures import write_smoke
    from agent_memory.unified_memory import UnifiedMemory

    write_smoke(tmp_path)
    monkeypatch.setattr(runner, 'ROOT', tmp_path)
    original_recall = UnifiedMemory.recall

    async def expanded_synthetic_recall(self, *args, **kwargs):
        actual = await original_recall(self, *args, **kwargs)
        sources = actual.citations[0].source_event_ids
        values = [item('first', 'x'*(8193 if over_budget else 3500), sources=sources),
                  item('second', 'y'*2600, sources=sources),
                  item('last', 'The latest fictional color is amber.', sources=sources)]
        return selected_bundle(values)

    monkeypatch.setattr(UnifiedMemory, 'recall', expanded_synthetic_recall)

    class MockClient:
        model = 'OFFLINE-MOCK-NOT-BENCHMARK'
        def __init__(self):
            self.receipts = []
            self.prompts = {}
        async def chat(self, prompt, phase, **kwargs):
            self.receipts.append({'phase': phase, 'mock': True})
            self.prompts[phase] = prompt
            if phase == 'extraction':
                return json.dumps({'claims': [{'key': 'fixture.robot.color', 'value': 'blue',
                                              'text': 'The fictional robot stores a blue cube.', 'confidence': .95}]})
            return 'Offline mock response, not benchmark evidence.'

    client = MockClient()
    if over_budget:
        with pytest.raises(ReaderContextError, match='core_token_budget_exceeded'):
            asyncio.run(runner.experiment(client, tmp_path/'run'))
        failed = json.loads((tmp_path/'run/result.json').read_text(encoding="utf-8"))
        assert failed['status'] == 'incomplete'
        assert failed['reader_contract_error'] == 'core_token_budget_exceeded'
        assert 'answer:agent-memory' not in client.prompts
        return
    result = asyncio.run(runner.experiment(client, tmp_path/'run'))
    native = result['arms']['agent-memory']
    assert native['context_utf8_bytes'] > 6000
    assert 'latest fictional color is amber' in client.prompts['answer:agent-memory']
    assert native['reader_delivery']['delivered_count'] == 3
    assert all(not row['partial'] for row in native['delivered_memories'])
    assert [r['text'] for r in native['retrieved_candidates']] == [r['text'] for r in native['delivered_memories']]
