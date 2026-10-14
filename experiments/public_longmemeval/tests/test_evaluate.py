"""Authored synthetic evaluation records only; no providers or public data."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.public_longmemeval import evaluate
from experiments.public_longmemeval.core import input_identity
from experiments.public_longmemeval.reader_context import reader_contract
from experiments.public_longmemeval.tests.fixtures import write_smoke


class ReaderProtocolEvaluation(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        write_smoke(self.root)
        self.run = self.root/'run'
        self.run.mkdir()
        self.patch = patch.object(evaluate, 'ROOT', self.root)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.temp.cleanup()

    def prepare(self, contract=None, *, include=False):
        result = {'status':'completed','case_id':'synthetic-test',
                  'question':'What color is the fictional cube?',
                  'input_artifacts':input_identity(self.root/'smoke'),
                  'arms':{name:{'status':'completed','hypothesis':'blue',
                               'delivered_memories':[{'source_ids':['session-0000']}]}
                          for name in ('no-memory','bm25-raw-chunk','recent-raw-chunk','agent-memory')}}
        if include:
            result['protocol'] = {'reader_contract':contract}
        (self.run/'result.json').write_text(json.dumps(result), encoding='utf-8')
        return evaluate.evaluate(self.run)

    def review(self):
        review = json.loads((self.run/'manual_review.json').read_text())
        review['reviewer'] = 'Synthetic fixture reviewer'
        for row in review['answers'].values():
            row.update(correct=1, reason='Authored fixture answer matches.')
        (self.run/'manual_review.json').write_text(json.dumps(review), encoding='utf-8')
        return review

    def test_new_reports_preserve_unequal_budget_contract_verbatim(self):
        contract = reader_contract()
        evidence = self.prepare(contract, include=True)
        review = self.review()
        summary = evaluate.summarize(self.run)
        for output in (evidence, review, summary):
            self.assertEqual(output['reader_contract'], contract)
            self.assertEqual(output['reader_contract_status'], 'recorded')
            self.assertIs(output['cross_arm_budget_equivalence'], False)
            self.assertIn('not an equal-context-budget comparison', output['reader_comparison_note'])
        self.assertIsNone(evidence['qa_accuracy'])

    def test_old_result_has_unknown_protocol_without_retroactive_v2_claim(self):
        evidence = self.prepare()
        review = self.review()
        summary = evaluate.summarize(self.run)
        for output in (evidence, review, summary):
            self.assertIsNone(output['reader_contract'])
            self.assertEqual(output['reader_contract_status'], 'unknown')
            self.assertIsNone(output['cross_arm_budget_equivalence'])
            self.assertIn('unknown', output['reader_comparison_note'])
            self.assertNotIn('public-longmemeval-reader/2', json.dumps(output))

    def test_preserves_recorded_version_instead_of_importing_latest_contract(self):
        original = {'version':'authored-previous-contract',
                    'cross_arm_budget_equivalence':False,
                    'comparison_note':'Authored historic mismatch.',
                    'custom_cap':123}
        self.prepare(original, include=True)
        self.review()
        summary = evaluate.summarize(self.run)
        self.assertEqual(summary['reader_contract'], original)
        self.assertEqual(summary['reader_comparison_note'], 'Authored historic mismatch.')

    def test_malformed_or_absent_equivalence_is_unknown_not_truth_coerced(self):
        for contract in ({'version':'fixture'}, {'cross_arm_budget_equivalence':'false'},
                         {'cross_arm_budget_equivalence':0}):
            with self.subTest(contract=contract):
                output = evaluate.reader_comparison({'protocol':{'reader_contract':contract}})
                self.assertEqual(output['reader_contract'], contract)
                self.assertIsNone(output['cross_arm_budget_equivalence'])
                self.assertIn('unknown', output['reader_comparison_note'])

    def test_summary_uses_run_evidence_not_edited_review_protocol(self):
        contract = reader_contract()
        self.prepare(contract, include=True)
        review = self.review()
        review['reader_contract']['cross_arm_budget_equivalence'] = True
        review['cross_arm_budget_equivalence'] = True
        (self.run/'manual_review.json').write_text(json.dumps(review), encoding='utf-8')
        summary = evaluate.summarize(self.run)
        self.assertEqual(summary['reader_contract'], contract)
        self.assertIs(summary['cross_arm_budget_equivalence'], False)

    def test_legacy_review_without_run_file_does_not_invent_protocol(self):
        self.prepare()
        self.review()
        (self.run/'result.json').unlink()
        summary = evaluate.summarize(self.run)
        self.assertIsNone(summary['reader_contract'])
        self.assertIsNone(summary['cross_arm_budget_equivalence'])


if __name__ == '__main__':
    unittest.main()
