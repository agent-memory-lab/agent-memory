"""Actual local SQLite core with synthetic records and accountable mock outputs."""
import argparse
import asyncio
import json

import pytest

from examples.memory_evidence_reader import InvalidEvidenceReaderResponse, RESPONSE_SCHEMA_V3
from experiments.public_longmemeval import run_experiment, mac_smoke
from experiments.public_longmemeval.tests.fixtures import write_smoke


class MockClient:
    model = 'OFFLINE-SYNTHETIC-NOT-BENCHMARK'
    def __init__(self, invalid=False):
        self.invalid = invalid
        self.receipts, self.prompts, self.responses = [], {}, {}
    async def chat(self,prompt,phase,**kwargs):
        self.receipts.append({'phase':phase,'mock':True})
        self.prompts[phase] = prompt
        if phase == 'extraction':
            return json.dumps({'claims':[{'key':'synthetic.robot.color','value':'blue',
                'text':'The fictional robot stores a blue cube.','confidence':.95}]})
        assert kwargs['format'] == RESPONSE_SCHEMA_V3 and kwargs['limit'] == 768
        blocks = json.loads(prompt.split('Delivered evidence JSON:\n',1)[1])
        value = {'schema_version':'evidence-reader/3','answerable':bool(blocks),
            'answer':'Synthetic answer' if blocks else '',
            'supporting_quotes':[{'evidence_id':'E1','text':blocks[0]['text'][:100]}] if blocks else [],
            'missing_information':[] if blocks else ['The requested fact.']}
        if self.invalid and phase == 'answer:agent-memory':
            value['supporting_quotes'][0]['text'] = 'This quotation was never delivered.'
        self.responses[phase] = json.dumps(value)
        return self.responses[phase]


@pytest.mark.parametrize('entry', ['public','local'])
@pytest.mark.parametrize('invalid', [False,True])
@pytest.mark.parametrize('packing_version', ['v2','v3','v4','v5'])
def test_v3_real_sqlite_path_uses_one_shared_reader_and_saves_protocol_errors(tmp_path,monkeypatch,entry,invalid,packing_version):
    smoke = write_smoke(tmp_path)
    client = MockClient(invalid)
    if entry == 'public':
        monkeypatch.setattr(run_experiment,'ROOT',tmp_path)
        invocation = lambda:run_experiment.experiment(client,tmp_path/'run',reader_version='v3',packing_version=packing_version)
    else:
        args = argparse.Namespace(smoke=smoke,output=tmp_path/'run',max_calls=160,max_seconds=1800,reader_version='v3',packing_version=packing_version)
        invocation = lambda:mac_smoke.run(args,client,{'embedding':None})
    if invalid:
        with pytest.raises(InvalidEvidenceReaderResponse):
            asyncio.run(invocation())
    else:
        asyncio.run(invocation())
    saved = json.loads((tmp_path/'run/result.json').read_text())
    contract = saved.get('reader_contract') or saved['protocol']['reader_contract']
    assert contract['version'] == 'public-longmemeval-reader/3'
    assert len(contract['answer_reader']['shared_reader_sha256']) == 64
    native = saved['arms']['agent-memory']
    packing = saved.get('packing_contract') or saved['protocol']['packing_contract']
    assert packing['version'] == packing_version
    assert native['answer_reader_raw'] == client.responses['answer:agent-memory']
    assert native['answer_reader']['raw_response'] == native['answer_reader_raw']
    assert native['answer_reader']['status'] == ('invalid' if invalid else 'supported')
    if invalid:
        assert 'hypothesis' not in native
    assert len(client.receipts) == 5
    assert saved['persisted_claims'] > 0
    blocks = json.loads(native['reader_context'])
    assert all(block['source_details'][0]['occurred_at'] != 'unknown' for block in blocks)
    assert all(block['source_ids'] == ['session-0000'] for block in blocks)
    assert native['reader_delivery']['evidence_text_not_repacked'] is True
    assert native['reader_delivery']['partial_count'] == 0
    assert saved['arms']['no-memory']['answer_reader']['status'] == 'unsupported'
    assert 'Missing information:' in saved['arms']['no-memory']['hypothesis']


def test_public_cli_preflight_defaults_to_v3_and_v2_remains_explicit(tmp_path,monkeypatch,capsys):
    write_smoke(tmp_path)
    monkeypatch.setattr(run_experiment,'ROOT',tmp_path)
    monkeypatch.setattr('sys.argv',['run_experiment.py','preflight'])
    run_experiment.main()
    assert json.loads(capsys.readouterr().out)['reader_contract']['version'] == 'public-longmemeval-reader/3'
    monkeypatch.setattr('sys.argv',['run_experiment.py','preflight','--reader-version','v2'])
    run_experiment.main()
    assert json.loads(capsys.readouterr().out)['reader_contract']['version'] == 'public-longmemeval-reader/2'
    monkeypatch.setattr('sys.argv',['run_experiment.py','preflight','--packing-version','v3','--reader-version','v2'])
    run_experiment.main()
    plan = json.loads(capsys.readouterr().out)
    assert plan['packing_contract']['version'] == 'v3'
    assert plan['packing_contract']['experimental'] is True
    assert plan['reader_contract']['version'] == 'public-longmemeval-reader/2'
