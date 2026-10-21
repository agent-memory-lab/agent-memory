"""Synthetic local-runner regressions; never contact Ollama or load models."""
import argparse
import asyncio
from dataclasses import replace
from datetime import UTC, datetime
import json

import pytest

from agent_memory import Citation, MemoryItem, MemoryKind
from agent_memory.evaluation.comparison import AgentMemoryComparisonAdapter
from agent_memory.retrieval.packing import token_estimate
from agent_memory.unified_memory import UnifiedMemory
from experiments.public_longmemeval import mac_smoke
from experiments.public_longmemeval.reader_context import ReaderContextError
from experiments.public_longmemeval.tests.fixtures import write_smoke


@pytest.fixture(autouse=True)
def forbid_ollama(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('This offline test must never contact Ollama')
    monkeypatch.setattr(mac_smoke.LocalOllama, 'request', forbidden)
    monkeypatch.setattr(AgentMemoryComparisonAdapter, 'search', forbidden)


class MockClient:
    model = 'OFFLINE-MOCK-NOT-BENCHMARK'

    def __init__(self, output, *, fail_answer=False):
        self.output = output
        self.fail_answer = fail_answer
        self.receipts = []
        self.prompts = {}
        self.answer_checkpoint = None

    async def chat(self, prompt, phase, **kwargs):
        self.receipts.append({'phase': phase, 'status': 'completed', 'mock': True})
        self.prompts[phase] = prompt
        if phase == 'extraction':
            return json.dumps({'claims': [{'key': 'fictional.robot.color', 'value': 'blue',
                'text': 'The fictional robot stores a blue cube.', 'confidence': .95}]})
        if phase == 'answer:agent-memory':
            self.answer_checkpoint = json.loads((self.output/'result.json').read_text())
            if self.fail_answer:
                raise RuntimeError('Synthetic reader failure')
        return 'Offline mock answer, not a model benchmark result.'


def arguments(tmp_path):
    return argparse.Namespace(smoke=write_smoke(tmp_path), output=tmp_path/'run',
                              max_calls=160, max_seconds=1800)


def test_real_core_local_runner_preserves_native_citations_and_checkpoints(tmp_path):
    args = arguments(tmp_path)
    client = MockClient(args.output)
    result = asyncio.run(mac_smoke.run(args, client, {'embedding': None}))
    assert result['status'] == 'completed'
    assert len(client.receipts) == 5
    assert result['events_completed'] == 1
    assert list(result['provider_event_source_map'].values()) == ['session-0000']
    assert result['reader_contract']['cross_arm_budget_equivalence'] is False
    assert 'shared_reader_context_utf8_byte_cap' not in result
    native = result['arms']['agent-memory']
    assert native['reader_delivery']['mode'] == 'core-evidence-verbatim/v2'
    assert native['reader_delivery']['partial_count'] == 0
    assert [row['text'] for row in native['retrieved_memories']] == [row['text'] for row in native['delivered_memories']]
    assert all(row['source_ids'] == ['session-0000'] for row in native['delivered_memories'])
    assert all(set(row['source_event_ids']) <= set(result['provider_event_source_map']) for row in native['delivered_memories'])
    for name in ('no-memory', 'bm25-raw-chunk', 'recent-raw-chunk'):
        assert result['arms'][name]['reader_delivery']['mode'] == 'legacy-utf8-prefix/v1'
        assert result['arms'][name]['context_utf8_bytes'] <= 6000
    assert client.answer_checkpoint['arms']['agent-memory']['status'] == 'answering'
    assert client.answer_checkpoint['arms']['agent-memory']['reader_context'] == native['reader_context']
    saved = json.loads((args.output/'result.json').read_text())
    assert saved['status'] == 'completed' and saved['answer_accuracy'] is None


def mock_large_recall(monkeypatch, *, over_budget=False, foreign=False):
    original = UnifiedMemory.recall
    observed = []
    async def recall(self, *args, **kwargs):
        observed.append(kwargs)
        actual = await original(self, *args, **kwargs)
        sources = ('foreign-event',) if foreign else actual.citations[0].source_event_ids
        values = tuple(MemoryItem(ident, MemoryKind.CLAIM, text, 1.0, datetime(2026, 1, 1, tzinfo=UTC),
                                  {'source_event_ids': sources}) for ident, text in (
            ('first', 'x'*(8193 if over_budget else 3500)),
            ('second', 'y'*2600), ('latest', 'The latest fictional color is amber.')))
        proofs = tuple({'memory_id': value.id, 'candidate_id': value.id,
                        'source_event_ids': sources} for value in values)
        return replace(actual, current_state=(), relevant_memories=values, episodes=(), procedures=(),
                       citations=tuple(Citation(value.id, sources) for value in values),
                       token_estimate=sum(token_estimate(value.text) for value in values),
                       retrieval_metadata={'citation_spans': proofs})
    monkeypatch.setattr(UnifiedMemory, 'recall', recall)
    return observed


def test_local_runner_sends_late_update_beyond_6000_bytes_without_second_pack(tmp_path, monkeypatch):
    args = arguments(tmp_path)
    observed = mock_large_recall(monkeypatch)
    client = MockClient(args.output)
    result = asyncio.run(mac_smoke.run(args, client, {'embedding': None}))
    native = result['arms']['agent-memory']
    assert observed == [{'limit': 8, 'token_budget': 2048, 'include_current_state': False}]
    assert 6000 < native['context_utf8_bytes'] <= 8216
    assert 'latest fictional color is amber' in client.prompts['answer:agent-memory']
    assert native['reader_delivery']['delivered_count'] == 3
    assert native['reader_delivery']['omitted_count'] == 0
    assert all(not row['partial'] for row in native['delivered_memories'])


@pytest.mark.parametrize('over_budget,foreign,code', [
    (True, False, 'core_token_budget_exceeded'),
    (False, True, 'citation_outside_captured_history'),
])
def test_reader_contract_failure_saved_before_answer_call(tmp_path, monkeypatch, over_budget, foreign, code):
    args = arguments(tmp_path)
    mock_large_recall(monkeypatch, over_budget=over_budget, foreign=foreign)
    client = MockClient(args.output)
    with pytest.raises(ReaderContextError, match=code):
        asyncio.run(mac_smoke.run(args, client, {'embedding': None}))
    saved = json.loads((args.output/'result.json').read_text())
    assert saved['status'] == 'blocked_or_failed'
    assert saved['error_type'] == 'ReaderContextError'
    assert saved['reader_contract_error'] == code
    assert saved['events_completed'] == 1 and saved['provider_event_source_map']
    assert 'answer:agent-memory' not in client.prompts
    assert len(client.receipts) == 4


def test_failed_answer_keeps_context_and_source_evidence(tmp_path):
    args = arguments(tmp_path)
    client = MockClient(args.output, fail_answer=True)
    with pytest.raises(RuntimeError, match='Synthetic reader failure'):
        asyncio.run(mac_smoke.run(args, client, {'embedding': None}))
    saved = json.loads((args.output/'result.json').read_text())
    assert saved['status'] == 'blocked_or_failed'
    native = saved['arms']['agent-memory']
    assert native['status'] == 'answering'
    assert native['reader_context'] and native['delivered_memories']
    assert native['reader_delivery']['mode'] == 'core-evidence-verbatim/v2'
    assert 'hypothesis' not in native


def test_missing_capture_source_id_fails_without_claiming_ingest_completion(tmp_path, monkeypatch):
    async def broken_capture(self, event):
        return argparse.Namespace(status='done', provider_event_id=None)
    monkeypatch.setattr(AgentMemoryComparisonAdapter, 'add', broken_capture)
    args = arguments(tmp_path)
    client = MockClient(args.output)
    with pytest.raises(RuntimeError, match='provider event identifier'):
        asyncio.run(mac_smoke.run(args, client, {'embedding': None}))
    saved = json.loads((args.output/'result.json').read_text())
    assert saved['events_completed'] == 0
    assert saved['provider_event_source_map'] == {}
    assert saved['status'] == 'blocked_or_failed'
    assert len(client.receipts) == 3
