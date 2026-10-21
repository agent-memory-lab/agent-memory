"""Offline validation only. All model answers in tests are mocks, never benchmark evidence."""
import asyncio, contextlib, io, json, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
from experiments.public_longmemeval.tests.fixtures import write_smoke
from experiments.public_longmemeval import run_experiment as runner
from experiments.public_longmemeval.extraction import ClaimGenerator, chunks
from experiments.public_longmemeval import evaluate

HAS_PUBLIC_SAMPLE = all((runner.ROOT / "smoke" / name).exists() for name in
                        ("corpus.json", "query.json", "gold.evaluator-only.json"))
PUBLIC_SKIP = "Optional full public data absent; run download_full then prepare_smoke explicitly"

class TestRunner(unittest.TestCase):
    @unittest.skipUnless(HAS_PUBLIC_SAMPLE, PUBLIC_SKIP)
    def test_real_data_integrity(self):
        record,query,plan=runner.verify()
        self.assertEqual(plan['sessions'],49);self.assertEqual(plan['turns'],527)
        self.assertEqual(plan['extraction_calls'],78)
        self.assertEqual(plan['max_calls_needed'],82)
        self.assertNotIn('answer',query)
    def test_context_tracks_only_delivered_sources(self):
        text,delivered=runner.pack([{'id':'a','text':'中'*9000,'source_ids':['s1']},{'id':'b','text':'secret','source_ids':['s2']}])
        self.assertLessEqual(len(text.encode()),6000)
        self.assertEqual(len(delivered),1);self.assertTrue(delivered[0]['partial'])
        self.assertEqual(delivered[0]['source_ids'],['s1'])
    def test_unicode_chunks_lossless(self):
        text='🙂中文abc'*10000;parts=list(chunks(text))
        self.assertEqual(''.join(parts),text);self.assertTrue(all(len(p.encode())<=9000 for p in parts))
    def test_confidence_required(self):
        class Client:
            async def chat(self,*a,**kw): return json.dumps({'claims':[{'key':'x','value':'x','text':'x'}]})
        with self.assertRaises(ValueError): asyncio.run(ClaimGenerator(Client()).generate_claims(type('E',(),{'content':'a'})()))
    @unittest.skipUnless(HAS_PUBLIC_SAMPLE, PUBLIC_SKIP)
    def test_actual_core_full_public_history_with_mock_model(self):
        self._exercise_core(82, 78)

    def test_actual_core_synthetic_history_with_mock_model(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_smoke(root)
            with patch.object(runner, "ROOT", root), patch.object(evaluate, "ROOT", root):
                self._exercise_core(5, 1)

    def _exercise_core(self, expected_calls, expected_events):
        class Client:
            model='OFFLINE-MOCK-NOT-BENCHMARK'
            def __init__(self): self.receipts=[]
            async def chat(self,prompt,phase,**kw):
                self.receipts.append({'phase':phase,'mock':True})
                if phase=='extraction':
                    n=len(self.receipts)
                    return json.dumps({'claims':[{'key':f'offline.test.{n}','value':str(n),'text':f'Offline mock fact {n}. '+runner.verify()[1]['question'],'confidence':0.95}]})
                return 'Offline mocked answer; never a measured model result.'
        with tempfile.TemporaryDirectory() as d:
            out=Path(d)/'run';client=Client()
            with contextlib.redirect_stdout(io.StringIO()): result=asyncio.run(runner.experiment(client,out))
            self.assertEqual(result['status'],'completed');self.assertEqual(len(client.receipts),expected_calls)
            self.assertEqual(result['events_completed'],expected_events);self.assertGreater(result['persisted_claims'],0)
            self.assertEqual(set(result['arms']),set(runner.ARMS))
            memories=result['arms']['agent-memory']['delivered_memories']
            self.assertTrue(memories)
            self.assertTrue(all(m['source_ids'] for m in memories))
            self.assertTrue(all(s.startswith('session-') and '-part-' not in s for m in memories for s in m['source_ids']))
            for name, arm in result['arms'].items():
                if name == 'agent-memory':
                    self.assertEqual(arm['reader_delivery']['mode'], 'core-evidence-verbatim/v2')
                    self.assertEqual([m['text'] for m in arm['retrieved_candidates']],
                                     [m['text'] for m in arm['delivered_memories']])
                    self.assertEqual(arm['reader_delivery']['partial_count'], 0)
                    self.assertLessEqual(arm['context_utf8_bytes'], 8216)
                else:
                    self.assertEqual(arm['reader_delivery']['mode'], 'legacy-utf8-prefix/v1')
                    self.assertLessEqual(arm['context_utf8_bytes'], 6000)
            self.assertFalse(result['protocol']['reader_contract']['cross_arm_budget_equivalence'])
            evidence=evaluate.evaluate(out)
            self.assertIsNone(evidence['qa_accuracy'])
            with self.assertRaises(ValueError): evaluate.summarize(out)
            with self.assertRaises(FileExistsError): evaluate.evaluate(out)
    def test_partial_not_scored(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);(p/'result.json').write_text(json.dumps({'status':'incomplete'}), encoding="utf-8", newline="\n")
            with self.assertRaises(ValueError):evaluate.evaluate(p)
if __name__=='__main__': unittest.main()
